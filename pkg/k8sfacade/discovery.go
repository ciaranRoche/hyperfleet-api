package k8sfacade

import (
	"net/http"
	"sort"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/version"

	"github.com/openshift-hyperfleet/hyperfleet-api/pkg/api"
	"github.com/openshift-hyperfleet/hyperfleet-api/pkg/registry"
)

var groupVersionForDiscovery = metav1.GroupVersionForDiscovery{
	GroupVersion: GroupVersion,
	Version:      Version,
}

func apiGroup() metav1.APIGroup {
	return metav1.APIGroup{
		TypeMeta:         metav1.TypeMeta{APIVersion: "v1", Kind: "APIGroup"},
		Name:             Group,
		Versions:         []metav1.GroupVersionForDiscovery{groupVersionForDiscovery},
		PreferredVersion: groupVersionForDiscovery,
	}
}

// handleAPIGroupList serves GET /apis.
func (h *Handler) handleAPIGroupList(w http.ResponseWriter, r *http.Request) {
	groupList := metav1.APIGroupList{
		TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "APIGroupList"},
		Groups:   []metav1.APIGroup{apiGroup()},
	}
	writeJSON(r.Context(), w, http.StatusOK, groupList)
}

// handleAPIGroup serves GET /apis/hyperfleet.openshift.io.
func (h *Handler) handleAPIGroup(w http.ResponseWriter, r *http.Request) {
	writeJSON(r.Context(), w, http.StatusOK, apiGroup())
}

// handleAPIResourceList serves GET /apis/hyperfleet.openshift.io/v1alpha1.
func (h *Handler) handleAPIResourceList(w http.ResponseWriter, r *http.Request) {
	descriptors := registry.All()
	sort.Slice(descriptors, func(i, j int) bool {
		return descriptors[i].Plural < descriptors[j].Plural
	})
	resources := make([]metav1.APIResource, 0, len(descriptors))
	for _, d := range descriptors {
		resources = append(resources, metav1.APIResource{
			Name:       d.Plural,
			Kind:       d.Kind,
			Namespaced: IsNamespaced(d),
			Verbs:      metav1.Verbs{"get", "list", "watch"},
		})
	}
	list := metav1.APIResourceList{
		TypeMeta:     metav1.TypeMeta{APIVersion: "v1", Kind: "APIResourceList"},
		GroupVersion: GroupVersion,
		APIResources: resources,
	}
	writeJSON(r.Context(), w, http.StatusOK, list)
}

// handleLegacyAPIVersions serves GET /api. The facade has no legacy core
// group; an empty list keeps discovery-walking clients happy.
func (h *Handler) handleLegacyAPIVersions(w http.ResponseWriter, r *http.Request) {
	writeJSON(r.Context(), w, http.StatusOK, metav1.APIVersions{
		TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "APIVersions"},
		Versions: []string{},
	})
}

// handleVersion serves GET /version with a static, kubectl-friendly payload.
func (h *Handler) handleVersion(w http.ResponseWriter, r *http.Request) {
	writeJSON(r.Context(), w, http.StatusOK, version.Info{
		Major:      "1",
		Minor:      "0",
		GitVersion: "v1.0.0+hyperfleet-" + api.Version,
	})
}
