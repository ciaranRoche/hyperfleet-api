package k8sfacade

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"github.com/gorilla/mux"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"

	"github.com/openshift-hyperfleet/hyperfleet-api/pkg/api"
	"github.com/openshift-hyperfleet/hyperfleet-api/pkg/k8sfacade/cache"
	"github.com/openshift-hyperfleet/hyperfleet-api/pkg/logger"
	"github.com/openshift-hyperfleet/hyperfleet-api/pkg/registry"
)

// defaultWatchTimeout caps watches that don't send timeoutSeconds; the
// clean EOF makes clients re-establish, kube-apiserver style.
const defaultWatchTimeout = 5 * time.Minute

// watchEvent is one frame of the newline-delimited watch stream.
type watchEvent struct {
	Object interface{} `json:"object"`
	Type   string      `json:"type"`
}

// watcher applies per-connection filtering to the raw event stream. It
// tracks what it has sent per namespace/name key so it can (a) suppress
// soft-deleted "ghosts" whose name was reclaimed by a newer live object and
// (b) emit DELETED when an object stops matching the watch selectors.
type watcher struct {
	labelSelector labels.Selector
	fieldSelector fields.Selector
	lastSent      map[string]sentEntry
	namespace     string
	descriptor    registry.EntityDescriptor
}

type sentEntry struct {
	object map[string]interface{}
	uid    string
}

func (w *watcher) key(r *api.Resource) string {
	ns := ""
	if r.OwnerID != nil {
		ns = *r.OwnerID
	}
	return ns + "/" + r.Name
}

func (w *watcher) inScope(r *api.Resource) bool {
	if w.namespace == "" {
		return true
	}
	return r.OwnerID != nil && *r.OwnerID == w.namespace
}

// process turns one raw feed event into zero or more frames for this watch.
func (w *watcher) process(event cache.Event) []watchEvent {
	r := event.Resource
	if !w.inScope(r) {
		return nil
	}
	key := w.key(r)
	current, had := w.lastSent[key]
	matches := matchesSelectors(r, w.labelSelector, w.fieldSelector)

	if event.Type == api.ResourceEventDeleted {
		if had && current.uid == r.ID {
			delete(w.lastSent, key)
			return []watchEvent{{Type: "DELETED", Object: ToK8sObject(w.descriptor, r)}}
		}
		// Ghost finalization after its name was reclaimed, or an object this
		// watch never surfaced — nothing to tell the client.
		return nil
	}

	// A soft-deleted object whose name is now held by a different live
	// object is invisible: the reclaim already emitted DELETED for it.
	if r.DeletedTime != nil && had && current.uid != r.ID {
		return nil
	}

	if !matches {
		if had {
			// Stopped matching the selector: to the client, it is gone.
			delete(w.lastSent, key)
			return []watchEvent{{Type: "DELETED", Object: current.object}}
		}
		return nil
	}

	obj := ToK8sObject(w.descriptor, r)
	var frames []watchEvent
	eventType := event.Type
	if had && current.uid != r.ID {
		// Name reclaimed by a new live object: close out the ghost first so
		// clients keyed by namespace/name never hold two objects.
		frames = append(frames, watchEvent{Type: "DELETED", Object: current.object})
		eventType = api.ResourceEventAdded
	}
	w.lastSent[key] = sentEntry{uid: r.ID, object: obj}
	return append(frames, watchEvent{Type: eventType, Object: obj})
}

// handleWatchStream implements GET ...?watch=true.
func (h *Handler) handleWatchStream(w http.ResponseWriter, r *http.Request, d registry.EntityDescriptor) {
	ctx := r.Context()

	if h.feed == nil || !h.feed.Ready() {
		writeInternalError(ctx, w, "watch is unavailable: event feed is not running")
		return
	}

	query := r.URL.Query()
	if query.Get("sendInitialEvents") != "" || query.Get("resourceVersionMatch") != "" {
		writeBadRequest(ctx, w, "sendInitialEvents and resourceVersionMatch are not supported")
		return
	}

	labelSelector, fieldSelector, err := requestSelectors(r)
	if err != nil {
		writeBadRequest(ctx, w, err.Error())
		return
	}

	var startSeq int64
	sendInitialState := false
	switch rvParam := query.Get("resourceVersion"); rvParam {
	case "", "0":
		sendInitialState = true
	default:
		rv, parseErr := strconv.ParseInt(rvParam, 10, 64)
		if parseErr != nil || rv < 0 {
			writeBadRequest(ctx, w, "invalid resourceVersion: "+rvParam)
			return
		}
		minRetained, minErr := h.feed.MinRetainedSeq(ctx)
		if minErr != nil {
			writeInternalError(ctx, w, "failed to check resource version floor: "+minErr.Error())
			return
		}
		// Replay needs every event with seq > rv; if the oldest retained
		// event is beyond rv+1 the history has a hole.
		if minRetained > rv+1 {
			writeExpired(ctx, w, "too old resource version: "+rvParam)
			return
		}
		startSeq = rv
	}

	timeout := defaultWatchTimeout
	if rawTimeout := query.Get("timeoutSeconds"); rawTimeout != "" {
		seconds, parseErr := strconv.ParseInt(rawTimeout, 10, 64)
		if parseErr != nil || seconds <= 0 {
			writeBadRequest(ctx, w, "invalid timeoutSeconds: "+rawTimeout)
			return
		}
		timeout = time.Duration(seconds) * time.Second
	}
	allowBookmarks := query.Get("allowWatchBookmarks") == "true"

	// Neutralize the server-wide write deadline for this long-lived
	// response; Flush support is required for streaming.
	controller := http.NewResponseController(w)
	if deadlineErr := controller.SetWriteDeadline(time.Time{}); deadlineErr != nil {
		logger.WithError(ctx, deadlineErr).Error("k8sfacade: failed to clear write deadline")
	}

	watchState := &watcher{
		descriptor:    d,
		namespace:     mux.Vars(r)["namespace"],
		labelSelector: labelSelector,
		fieldSelector: fieldSelector,
		lastSent:      make(map[string]sentEntry),
	}

	// Subscribe before reading state/replaying so no event can fall between
	// replay and the live stream; duplicates are skipped by seq.
	subID, events := h.feed.Subscribe(d.Kind)
	defer h.feed.Unsubscribe(subID)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	// Flush the headers now — clients block on them before reading frames,
	// and net/http buffers until the first write otherwise.
	if flushErr := controller.Flush(); flushErr != nil {
		logger.WithError(ctx, flushErr).Error("k8sfacade: streaming unsupported by response writer")
		return
	}

	encoder := json.NewEncoder(w)
	send := func(frame watchEvent) bool {
		if encodeErr := encoder.Encode(frame); encodeErr != nil {
			return false
		}
		if flushErr := controller.Flush(); flushErr != nil {
			return false
		}
		return true
	}

	if sendInitialState {
		// Watermark first, state second: anything committed in between is
		// replayed as a duplicate-but-harmless MODIFIED.
		startSeq = h.feed.Head()
		if !h.sendInitialAdds(ctx, watchState, send) {
			return
		}
	}

	// Replay committed history after startSeq straight from the outbox.
	lastDelivered := startSeq
	for {
		replay, replayErr := h.feed.ListSince(ctx, lastDelivered, 500)
		if replayErr != nil {
			logger.WithError(ctx, replayErr).Error("k8sfacade: watch replay failed")
			return
		}
		for i := range replay {
			for _, frame := range watchState.process(replay[i]) {
				if !send(frame) {
					return
				}
			}
			lastDelivered = replay[i].Seq
		}
		if len(replay) < 500 {
			break
		}
	}

	// Live stream until timeout, disconnect, or slow-consumer close.
	timeoutTimer := time.NewTimer(timeout)
	defer timeoutTimer.Stop()
	var bookmarkTick <-chan time.Time
	if allowBookmarks {
		bookmarkTicker := time.NewTicker(h.bookmarkInterval)
		defer bookmarkTicker.Stop()
		bookmarkTick = bookmarkTicker.C
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-timeoutTimer.C:
			// Clean EOF — clients re-establish from their last seen RV.
			return
		case <-bookmarkTick:
			if !send(bookmarkFrame(d, maxSeq(lastDelivered, h.feed.Head()))) {
				return
			}
		case event, ok := <-events:
			if !ok {
				// Feed closed us (slow consumer or shutdown).
				return
			}
			if event.Seq <= lastDelivered {
				continue
			}
			for _, frame := range watchState.process(event) {
				if !send(frame) {
					return
				}
			}
			lastDelivered = event.Seq
		}
	}
}

// sendInitialAdds streams synthetic ADDED events for the current state
// (watch with resourceVersion "" or "0").
func (h *Handler) sendInitialAdds(
	ctx context.Context, watchState *watcher, send func(watchEvent) bool,
) bool {
	var resources api.ResourceList
	var err error
	if watchState.namespace != "" {
		resources, err = h.resourceDao.FindByKindAndOwner(ctx, watchState.descriptor.Kind, watchState.namespace)
	} else {
		resources, err = h.resourceDao.FindByKind(ctx, watchState.descriptor.Kind)
	}
	if err != nil {
		logger.WithError(ctx, err).Error("k8sfacade: watch initial state read failed")
		return false
	}
	for _, resource := range dedupeByScopeName(resources) {
		if !matchesSelectors(resource, watchState.labelSelector, watchState.fieldSelector) {
			continue
		}
		obj := ToK8sObject(watchState.descriptor, resource)
		watchState.lastSent[watchState.key(resource)] = sentEntry{uid: resource.ID, object: obj}
		if !send(watchEvent{Type: api.ResourceEventAdded, Object: obj}) {
			return false
		}
	}
	return true
}

// bookmarkFrame builds a BOOKMARK event carrying only the resourceVersion.
func bookmarkFrame(d registry.EntityDescriptor, seq int64) watchEvent {
	return watchEvent{
		Type: "BOOKMARK",
		Object: map[string]interface{}{
			"apiVersion": GroupVersion,
			"kind":       d.Kind,
			"metadata": map[string]interface{}{
				"resourceVersion": strconv.FormatInt(seq, 10),
			},
		},
	}
}

func maxSeq(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
