package integration

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
	. "github.com/onsi/gomega"

	"github.com/openshift-hyperfleet/hyperfleet-api/pkg/api"
	"github.com/openshift-hyperfleet/hyperfleet-api/pkg/dao"
	"github.com/openshift-hyperfleet/hyperfleet-api/pkg/k8sfacade"
	"github.com/openshift-hyperfleet/hyperfleet-api/test"
)

// watchStream reads newline-delimited watch frames from a streaming
// response in a background goroutine.
type watchStream struct {
	resp   *http.Response
	frames chan map[string]interface{}
	closed chan struct{}
}

// openWatch starts a facade watch. The returned stream must be closed.
// For non-200 responses the stream is nil and the decoded error body is
// returned instead.
func openWatch(
	t *testing.T, h *test.Helper, path string, query map[string]string,
) (*watchStream, int, map[string]interface{}) {
	t.Helper()
	account := h.NewRandAccount()
	authCtx := h.NewAuthenticatedContext(account)
	token := test.GetAccessTokenFromContext(authCtx)

	values := url.Values{"watch": []string{"true"}}
	for k, v := range query {
		values.Set(k, v)
	}
	req, err := http.NewRequest(http.MethodGet, h.RootURL(path)+"?"+values.Encode(), nil)
	Expect(err).NotTo(HaveOccurred())
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token))

	client := &http.Client{} // no timeout: the watch is long-lived
	resp, err := client.Do(req)
	Expect(err).NotTo(HaveOccurred())

	if resp.StatusCode != http.StatusOK {
		defer func() { _ = resp.Body.Close() }()
		var body map[string]interface{}
		_ = json.NewDecoder(resp.Body).Decode(&body)
		return nil, resp.StatusCode, body
	}

	ws := &watchStream{
		resp:   resp,
		frames: make(chan map[string]interface{}, 64),
		closed: make(chan struct{}),
	}
	go func() {
		defer close(ws.closed)
		scanner := bufio.NewScanner(resp.Body)
		scanner.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
		for scanner.Scan() {
			line := scanner.Bytes()
			if len(line) == 0 {
				continue
			}
			var frame map[string]interface{}
			if json.Unmarshal(line, &frame) == nil {
				ws.frames <- frame
			}
		}
	}()
	return ws, resp.StatusCode, nil
}

func (ws *watchStream) close() {
	_ = ws.resp.Body.Close()
}

// next waits for one frame, failing the assertion on timeout.
func (ws *watchStream) next(timeout time.Duration) map[string]interface{} {
	select {
	case frame := <-ws.frames:
		return frame
	case <-time.After(timeout):
		Expect(false).To(BeTrue(), "timed out waiting for watch frame")
		return nil
	}
}

// nextOfType drains frames until one of the wanted type arrives.
func (ws *watchStream) nextOfType(eventType string, timeout time.Duration) map[string]interface{} {
	deadline := time.Now().Add(timeout)
	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			Expect(false).To(BeTrue(), "timed out waiting for %s frame", eventType)
			return nil
		}
		select {
		case frame := <-ws.frames:
			if frame["type"] == eventType {
				return frame
			}
		case <-time.After(remaining):
			Expect(false).To(BeTrue(), "timed out waiting for %s frame", eventType)
			return nil
		}
	}
}

// eof asserts the stream terminates within timeout.
func (ws *watchStream) eof(timeout time.Duration) {
	select {
	case <-ws.closed:
	case <-time.After(timeout):
		Expect(false).To(BeTrue(), "watch stream did not terminate")
	}
}

func frameObjectMeta(frame map[string]interface{}) map[string]interface{} {
	obj, ok := frame["object"].(map[string]interface{})
	Expect(ok).To(BeTrue(), "frame missing object: %v", frame)
	return metadataOf(obj)
}

const watchFrameTimeout = 10 * time.Second

func TestK8sFacadeWatch_LifecycleFromListRV(t *testing.T) {
	RegisterTestingT(t)
	svc, h := setupResourceTest(t)
	ctx := context.Background()

	// LIST to obtain the resourceVersion a reflector would watch from.
	code, list := facadeGet(t, h, "/apis/"+k8sfacade.GroupVersion+"/channels", nil)
	Expect(code).To(Equal(http.StatusOK))
	listRv := metadataOf(list)["resourceVersion"].(string)

	ws, code, _ := openWatch(t, h, "/apis/"+k8sfacade.GroupVersion+"/channels",
		map[string]string{"resourceVersion": listRv})
	Expect(code).To(Equal(http.StatusOK))
	defer ws.close()

	// ADDED on create.
	name := fmt.Sprintf("watch-ch-%s", uuid.NewString()[:8])
	created, svcErr := svc.Create(ctx, "Channel", newChannelResource(name), nil)
	Expect(svcErr).To(BeNil())

	frame := ws.next(watchFrameTimeout)
	Expect(frame["type"]).To(Equal("ADDED"))
	metadata := frameObjectMeta(frame)
	Expect(metadata["name"]).To(Equal(name))
	Expect(metadata["uid"]).To(Equal(created.ID))
	addedRv, err := strconv.ParseInt(metadata["resourceVersion"].(string), 10, 64)
	Expect(err).NotTo(HaveOccurred())
	listRvInt, _ := strconv.ParseInt(listRv, 10, 64)
	Expect(addedRv).To(BeNumerically(">", listRvInt))

	// MODIFIED on patch, with monotonically increasing resourceVersion.
	_, svcErr = svc.Patch(ctx, "Channel", created.ID, &api.ResourcePatch{
		Labels: map[string]string{"stage": "one"},
	})
	Expect(svcErr).To(BeNil())
	frame = ws.next(watchFrameTimeout)
	Expect(frame["type"]).To(Equal("MODIFIED"))
	modifiedRv, _ := strconv.ParseInt(frameObjectMeta(frame)["resourceVersion"].(string), 10, 64)
	Expect(modifiedRv).To(BeNumerically(">", addedRv))

	// DELETED tombstone on hard delete.
	_, svcErr = svc.Delete(ctx, "Channel", created.ID)
	Expect(svcErr).To(BeNil())
	frame = ws.next(watchFrameTimeout)
	Expect(frame["type"]).To(Equal("DELETED"))
	Expect(frameObjectMeta(frame)["uid"]).To(Equal(created.ID))
}

func TestK8sFacadeWatch_InitialState(t *testing.T) {
	RegisterTestingT(t)
	svc, h := setupResourceTest(t)
	ctx := context.Background()

	name := fmt.Sprintf("init-ch-%s", uuid.NewString()[:8])
	created, svcErr := svc.Create(ctx, "Channel", newChannelResource(name), nil)
	Expect(svcErr).To(BeNil())

	// resourceVersion=0 → synthetic ADDED for existing state, then live.
	ws, code, _ := openWatch(t, h, "/apis/"+k8sfacade.GroupVersion+"/channels",
		map[string]string{"resourceVersion": "0",
			"fieldSelector": "metadata.name=" + name})
	Expect(code).To(Equal(http.StatusOK))
	defer ws.close()

	frame := ws.next(watchFrameTimeout)
	Expect(frame["type"]).To(Equal("ADDED"))
	Expect(frameObjectMeta(frame)["uid"]).To(Equal(created.ID))

	// Live events continue after the initial dump.
	_, svcErr = svc.Patch(ctx, "Channel", created.ID, &api.ResourcePatch{
		Labels: map[string]string{"phase": "live"},
	})
	Expect(svcErr).To(BeNil())
	frame = ws.next(watchFrameTimeout)
	Expect(frame["type"]).To(Equal("MODIFIED"))
}

func TestK8sFacadeWatch_SoftDeleteLifecycle(t *testing.T) {
	RegisterTestingT(t)
	svc, h := setupResourceTest(t)
	ctx := context.Background()

	cluster, err := h.Factories.NewClusters(h.NewID())
	Expect(err).NotTo(HaveOccurred())

	ws, code, _ := openWatch(t, h, "/apis/"+k8sfacade.GroupVersion+"/clusters",
		map[string]string{"resourceVersion": "0",
			"fieldSelector": "metadata.name=" + cluster.Name})
	Expect(code).To(Equal(http.StatusOK))
	defer ws.close()

	frame := ws.next(watchFrameTimeout)
	Expect(frame["type"]).To(Equal("ADDED"))

	// Soft delete → MODIFIED carrying deletionTimestamp + finalizer.
	_, svcErr := svc.Delete(ctx, clusterKind, cluster.ID)
	Expect(svcErr).To(BeNil())
	frame = ws.next(watchFrameTimeout)
	Expect(frame["type"]).To(Equal("MODIFIED"))
	metadata := frameObjectMeta(frame)
	Expect(metadata["deletionTimestamp"]).NotTo(BeNil())
	Expect(metadata["finalizers"]).To(ContainElement(k8sfacade.FinalizerAdapters))

	// Finalization (force-delete) → DELETED.
	Expect(svc.ForceDelete(ctx, clusterKind, cluster.ID, "test")).To(BeNil())
	frame = ws.nextOfType("DELETED", watchFrameTimeout)
	Expect(frameObjectMeta(frame)["uid"]).To(Equal(cluster.ID))
}

func TestK8sFacadeWatch_LabelSelectorTransitions(t *testing.T) {
	RegisterTestingT(t)
	svc, h := setupResourceTest(t)
	ctx := context.Background()

	ws, code, _ := openWatch(t, h, "/apis/"+k8sfacade.GroupVersion+"/channels",
		map[string]string{"resourceVersion": "0", "labelSelector": "watched=yes"})
	Expect(code).To(Equal(http.StatusOK))
	defer ws.close()

	// Non-matching create is invisible.
	otherName := fmt.Sprintf("nomatch-%s", uuid.NewString()[:8])
	other, svcErr := svc.Create(ctx, "Channel", newChannelResource(otherName), nil)
	Expect(svcErr).To(BeNil())

	// Matching create arrives.
	name := fmt.Sprintf("match-%s", uuid.NewString()[:8])
	matching := newChannelResource(name)
	matching.Labels = []api.ResourceLabel{{Key: "watched", Value: "yes"}}
	created, svcErr := svc.Create(ctx, "Channel", matching, nil)
	Expect(svcErr).To(BeNil())

	frame := ws.next(watchFrameTimeout)
	Expect(frame["type"]).To(Equal("ADDED"))
	Expect(frameObjectMeta(frame)["uid"]).To(Equal(created.ID),
		"non-matching channel %s must not produce frames", other.ID)

	// Label removed → the object leaves the selector → DELETED to this watch.
	_, svcErr = svc.Patch(ctx, "Channel", created.ID, &api.ResourcePatch{
		Labels: map[string]string{"watched": "no"},
	})
	Expect(svcErr).To(BeNil())
	frame = ws.next(watchFrameTimeout)
	Expect(frame["type"]).To(Equal("DELETED"))
	Expect(frameObjectMeta(frame)["uid"]).To(Equal(created.ID))
}

func TestK8sFacadeWatch_Namespaced(t *testing.T) {
	RegisterTestingT(t)
	svc, h := setupResourceTest(t)
	ctx := context.Background()

	cluster, err := h.Factories.NewClusters(h.NewID())
	Expect(err).NotTo(HaveOccurred())
	otherCluster, err := h.Factories.NewClusters(h.NewID())
	Expect(err).NotTo(HaveOccurred())

	ws, code, _ := openWatch(t, h,
		"/apis/"+k8sfacade.GroupVersion+"/namespaces/"+cluster.ID+"/nodepools",
		map[string]string{"resourceVersion": "0"})
	Expect(code).To(Equal(http.StatusOK))
	defer ws.close()

	// NodePool in another namespace is invisible.
	ownerKind := clusterKind
	_, svcErr := svc.Create(ctx, "NodePool", &api.Resource{
		Kind: "NodePool", Name: "other-np", OwnerID: &otherCluster.ID, OwnerKind: &ownerKind,
		Spec:      []byte(`{"machine_type": "n1-standard-4", "replicas": 1}`),
		CreatedBy: "test@example.com", UpdatedBy: "test@example.com",
	}, nil)
	Expect(svcErr).To(BeNil())

	// NodePool in the watched namespace arrives.
	nodePool, svcErr := svc.Create(ctx, "NodePool", &api.Resource{
		Kind: "NodePool", Name: "watched-np", OwnerID: &cluster.ID, OwnerKind: &ownerKind,
		Spec:      []byte(`{"machine_type": "n1-standard-4", "replicas": 1}`),
		CreatedBy: "test@example.com", UpdatedBy: "test@example.com",
	}, nil)
	Expect(svcErr).To(BeNil())

	frame := ws.next(watchFrameTimeout)
	Expect(frame["type"]).To(Equal("ADDED"))
	metadata := frameObjectMeta(frame)
	Expect(metadata["uid"]).To(Equal(nodePool.ID))
	Expect(metadata["namespace"]).To(Equal(cluster.ID))
}

func TestK8sFacadeWatch_Bookmarks(t *testing.T) {
	RegisterTestingT(t)
	_, h := setupResourceTest(t)

	// The integration environment uses a 1m bookmark interval, so instead of
	// waiting for the ticker this only checks the parameter is accepted and
	// the stream stays healthy alongside timeoutSeconds handling.
	ws, code, _ := openWatch(t, h, "/apis/"+k8sfacade.GroupVersion+"/channels",
		map[string]string{"resourceVersion": "0", "allowWatchBookmarks": "true",
			"timeoutSeconds": "1"})
	Expect(code).To(Equal(http.StatusOK))
	defer ws.close()

	// timeoutSeconds=1 → clean EOF promptly.
	ws.eof(5 * time.Second)
}

func TestK8sFacadeWatch_410OnCompactedHistory(t *testing.T) {
	RegisterTestingT(t)
	svc, h := setupResourceTest(t)
	ctx := context.Background()

	// Create history, then compact everything below the current head.
	created, svcErr := svc.Create(ctx, "Channel",
		newChannelResource(fmt.Sprintf("gone-ch-%s", uuid.NewString()[:8])), nil)
	Expect(svcErr).To(BeNil())
	_, svcErr = svc.Patch(ctx, "Channel", created.ID, &api.ResourcePatch{
		Labels: map[string]string{"n": "1"},
	})
	Expect(svcErr).To(BeNil())

	eventDao := dao.NewResourceEventDao(h.DBFactory)
	head, err := eventDao.SafeHead(ctx)
	Expect(err).NotTo(HaveOccurred())
	_, err = eventDao.CompactBelow(ctx, head)
	Expect(err).NotTo(HaveOccurred())

	// Watching from a compacted RV must return 410 Gone with reason Expired.
	minRetained, err := eventDao.MinRetainedSeq(ctx)
	Expect(err).NotTo(HaveOccurred())
	Expect(minRetained).To(Equal(head))

	ws, code, body := openWatch(t, h, "/apis/"+k8sfacade.GroupVersion+"/channels",
		map[string]string{"resourceVersion": strconv.FormatInt(head-2, 10)})
	Expect(ws).To(BeNil())
	Expect(code).To(Equal(http.StatusGone))
	Expect(body["reason"]).To(Equal("Expired"))

	// Watching from the head itself is fine (no missing history).
	ws, code, _ = openWatch(t, h, "/apis/"+k8sfacade.GroupVersion+"/channels",
		map[string]string{"resourceVersion": strconv.FormatInt(head, 10), "timeoutSeconds": "1"})
	Expect(code).To(Equal(http.StatusOK))
	ws.eof(5 * time.Second)
	ws.close()
}
