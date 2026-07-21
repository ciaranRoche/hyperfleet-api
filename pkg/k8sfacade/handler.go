package k8sfacade

import (
	"errors"
	"net/http"
	"time"

	"github.com/gorilla/mux"
	"gorm.io/gorm"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"

	"github.com/openshift-hyperfleet/hyperfleet-api/pkg/api"
	"github.com/openshift-hyperfleet/hyperfleet-api/pkg/dao"
	"github.com/openshift-hyperfleet/hyperfleet-api/pkg/k8sfacade/cache"
	"github.com/openshift-hyperfleet/hyperfleet-api/pkg/registry"
)

// Handler serves the Kubernetes-compatible read facade.
type Handler struct {
	resourceDao      dao.ResourceDao
	eventDao         dao.ResourceEventDao
	feed             *cache.Feed
	bookmarkInterval time.Duration
}

// NewHandler builds the facade handler. feed may be nil (watch disabled,
// e.g. in narrow tests); list/get/discovery still work.
func NewHandler(
	resourceDao dao.ResourceDao,
	eventDao dao.ResourceEventDao,
	feed *cache.Feed,
	bookmarkInterval time.Duration,
) *Handler {
	if bookmarkInterval <= 0 {
		bookmarkInterval = time.Minute
	}
	return &Handler{
		resourceDao:      resourceDao,
		eventDao:         eventDao,
		feed:             feed,
		bookmarkInterval: bookmarkInterval,
	}
}

// RegisterRoutes mounts the facade on the root router (deliberately outside
// the /api/hyperfleet/v1 middleware chain: no request-timeout transaction
// middleware, no gzip, no OpenAPI schema validation). JWT auth still applies
// because it wraps the whole server handler.
func (h *Handler) RegisterRoutes(root *mux.Router) {
	root.HandleFunc("/api", h.handleLegacyAPIVersions).Methods(http.MethodGet)
	root.HandleFunc("/apis", h.handleAPIGroupList).Methods(http.MethodGet)
	root.HandleFunc("/apis/"+Group, h.handleAPIGroup).Methods(http.MethodGet)
	root.HandleFunc("/apis/"+GroupVersion, h.handleAPIResourceList).Methods(http.MethodGet)
	root.HandleFunc("/version", h.handleVersion).Methods(http.MethodGet)

	base := "/apis/" + GroupVersion
	for _, d := range registry.All() {
		descriptor := d
		if IsNamespaced(descriptor) {
			// Cluster-scope path lists across all namespaces; the namespaced
			// path scopes to one parent.
			root.HandleFunc(base+"/"+descriptor.Plural,
				func(w http.ResponseWriter, r *http.Request) { h.handleList(w, r, descriptor) },
			).Methods(http.MethodGet)
			root.HandleFunc(base+"/namespaces/{namespace}/"+descriptor.Plural,
				func(w http.ResponseWriter, r *http.Request) { h.handleList(w, r, descriptor) },
			).Methods(http.MethodGet)
			root.HandleFunc(base+"/namespaces/{namespace}/"+descriptor.Plural+"/{name}",
				func(w http.ResponseWriter, r *http.Request) { h.handleGet(w, r, descriptor) },
			).Methods(http.MethodGet)
		} else {
			root.HandleFunc(base+"/"+descriptor.Plural,
				func(w http.ResponseWriter, r *http.Request) { h.handleList(w, r, descriptor) },
			).Methods(http.MethodGet)
			root.HandleFunc(base+"/"+descriptor.Plural+"/{name}",
				func(w http.ResponseWriter, r *http.Request) { h.handleGet(w, r, descriptor) },
			).Methods(http.MethodGet)
		}
	}
}

// requestSelectors parses labelSelector/fieldSelector query params.
func requestSelectors(r *http.Request) (labels.Selector, fields.Selector, error) {
	labelSelector := labels.Everything()
	if raw := r.URL.Query().Get("labelSelector"); raw != "" {
		parsed, err := labels.Parse(raw)
		if err != nil {
			return nil, nil, err
		}
		labelSelector = parsed
	}
	fieldSelector := fields.Everything()
	if raw := r.URL.Query().Get("fieldSelector"); raw != "" {
		parsed, err := fields.ParseSelector(raw)
		if err != nil {
			return nil, nil, err
		}
		fieldSelector = parsed
	}
	return labelSelector, fieldSelector, nil
}

func matchesSelectors(r *api.Resource, labelSelector labels.Selector, fieldSelector fields.Selector) bool {
	labelSet := make(labels.Set, len(r.Labels))
	for _, l := range r.Labels {
		labelSet[l.Key] = l.Value
	}
	if !labelSelector.Matches(labelSet) {
		return false
	}
	fieldSet := fields.Set{"metadata.name": r.Name}
	if r.OwnerID != nil {
		fieldSet["metadata.namespace"] = *r.OwnerID
	}
	return fieldSelector.Matches(fieldSet)
}

func (h *Handler) handleList(w http.ResponseWriter, r *http.Request, d registry.EntityDescriptor) {
	ctx := r.Context()

	if r.URL.Query().Get("watch") == "true" || r.URL.Query().Get("watch") == "1" {
		h.handleWatch(w, r, d)
		return
	}

	labelSelector, fieldSelector, err := requestSelectors(r)
	if err != nil {
		writeBadRequest(ctx, w, err.Error())
		return
	}

	namespace := mux.Vars(r)["namespace"]

	// The list resourceVersion must be a safe watermark captured BEFORE the
	// row read: every mutation invisible to the read then has an event with
	// seq > listRv, so a watch started from listRv cannot miss it (it may
	// redeliver changes the list already contained, which is harmless).
	listRv, err := h.eventDao.SafeHead(ctx)
	if err != nil {
		writeInternalError(ctx, w, "failed to read resource version: "+err.Error())
		return
	}

	var resources api.ResourceList
	if namespace != "" {
		resources, err = h.resourceDao.FindByKindAndOwner(ctx, d.Kind, namespace)
	} else {
		resources, err = h.resourceDao.FindByKind(ctx, d.Kind)
	}
	if err != nil {
		writeInternalError(ctx, w, "failed to list "+d.Kind+": "+err.Error())
		return
	}

	items := make([]map[string]interface{}, 0, len(resources))
	for _, resource := range dedupeByScopeName(resources) {
		if !matchesSelectors(resource, labelSelector, fieldSelector) {
			continue
		}
		items = append(items, ToK8sObject(d, resource))
	}

	writeJSON(ctx, w, http.StatusOK, ToK8sList(d, items, listRv))
}

func (h *Handler) handleGet(w http.ResponseWriter, r *http.Request, d registry.EntityDescriptor) {
	ctx := r.Context()
	vars := mux.Vars(r)
	name := vars["name"]
	namespace := vars["namespace"]

	var resource *api.Resource
	var err error
	if IsNamespaced(d) {
		resource, err = h.resourceDao.GetByOwnerAndName(ctx, d.Kind, namespace, name)
	} else {
		resource, err = h.resourceDao.GetByName(ctx, d.Kind, name)
	}
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			writeNotFound(ctx, w, d.Kind, name)
			return
		}
		writeInternalError(ctx, w, "failed to get "+d.Kind+": "+err.Error())
		return
	}

	writeJSON(ctx, w, http.StatusOK, ToK8sObject(d, resource))
}

func (h *Handler) handleWatch(w http.ResponseWriter, r *http.Request, d registry.EntityDescriptor) {
	h.handleWatchStream(w, r, d)
}
