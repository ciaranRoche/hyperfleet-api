package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"testing"

	"github.com/google/uuid"
	. "github.com/onsi/gomega"
	"gopkg.in/resty.v1"

	"github.com/openshift-hyperfleet/hyperfleet-api/pkg/api"
	"github.com/openshift-hyperfleet/hyperfleet-api/pkg/k8sfacade"
	"github.com/openshift-hyperfleet/hyperfleet-api/test"
)

// facadeGet performs an authenticated GET against a root-level facade path
// and unmarshals the JSON response.
func facadeGet(t *testing.T, h *test.Helper, path string, query map[string]string) (int, map[string]interface{}) {
	t.Helper()
	account := h.NewRandAccount()
	ctx := h.NewAuthenticatedContext(account)
	token := test.GetAccessTokenFromContext(ctx)

	req := resty.R().SetHeader("Authorization", fmt.Sprintf("Bearer %s", token))
	if query != nil {
		req = req.SetQueryParams(query)
	}
	resp, err := req.Get(h.RootURL(path))
	Expect(err).NotTo(HaveOccurred())

	var body map[string]interface{}
	if len(resp.Body()) > 0 {
		Expect(json.Unmarshal(resp.Body(), &body)).To(Succeed(),
			"non-JSON response from %s: %s", path, resp.String())
	}
	return resp.StatusCode(), body
}

func metadataOf(obj map[string]interface{}) map[string]interface{} {
	metadata, ok := obj["metadata"].(map[string]interface{})
	Expect(ok).To(BeTrue(), "object missing metadata: %v", obj)
	return metadata
}

func itemsOf(list map[string]interface{}) []map[string]interface{} {
	raw, ok := list["items"].([]interface{})
	Expect(ok).To(BeTrue(), "list missing items: %v", list)
	items := make([]map[string]interface{}, 0, len(raw))
	for _, item := range raw {
		m, ok := item.(map[string]interface{})
		Expect(ok).To(BeTrue())
		items = append(items, m)
	}
	return items
}

func TestK8sFacade_RequiresAuth(t *testing.T) {
	RegisterTestingT(t)
	h, _ := test.RegisterIntegration(t)

	resp, err := resty.R().Get(h.RootURL("/apis"))
	Expect(err).NotTo(HaveOccurred())
	Expect(resp.StatusCode()).To(Equal(http.StatusUnauthorized),
		"facade must sit behind JWT auth")
}

func TestK8sFacade_Discovery(t *testing.T) {
	RegisterTestingT(t)
	h, _ := test.RegisterIntegration(t)

	// /apis — APIGroupList containing our group.
	code, groupList := facadeGet(t, h, "/apis", nil)
	Expect(code).To(Equal(http.StatusOK))
	Expect(groupList["kind"]).To(Equal("APIGroupList"))
	groups, _ := json.Marshal(groupList["groups"])
	Expect(string(groups)).To(ContainSubstring(k8sfacade.Group))

	// /apis/<group> — APIGroup with preferred version.
	code, group := facadeGet(t, h, "/apis/"+k8sfacade.Group, nil)
	Expect(code).To(Equal(http.StatusOK))
	Expect(group["kind"]).To(Equal("APIGroup"))
	Expect(group["preferredVersion"].(map[string]interface{})["groupVersion"]).
		To(Equal(k8sfacade.GroupVersion))

	// /apis/<group>/<version> — APIResourceList covering all registry kinds
	// with correct scoping.
	code, resourceList := facadeGet(t, h, "/apis/"+k8sfacade.GroupVersion, nil)
	Expect(code).To(Equal(http.StatusOK))
	Expect(resourceList["kind"]).To(Equal("APIResourceList"))
	Expect(resourceList["groupVersion"]).To(Equal(k8sfacade.GroupVersion))

	byName := map[string]map[string]interface{}{}
	for _, raw := range resourceList["resources"].([]interface{}) {
		r := raw.(map[string]interface{})
		byName[r["name"].(string)] = r
	}
	Expect(byName).To(HaveKey("clusters"))
	Expect(byName).To(HaveKey("nodepools"))
	Expect(byName["clusters"]["namespaced"]).To(BeFalse(), "Cluster is top-level → cluster-scoped")
	Expect(byName["nodepools"]["namespaced"]).To(BeTrue(), "NodePool has a parent → namespaced")
	Expect(byName["clusters"]["verbs"]).To(ContainElements("get", "list", "watch"))

	// /version — static version info.
	code, versionInfo := facadeGet(t, h, "/version", nil)
	Expect(code).To(Equal(http.StatusOK))
	Expect(versionInfo["gitVersion"]).NotTo(BeEmpty())
}

func TestK8sFacade_ListAndGetClusterScoped(t *testing.T) {
	RegisterTestingT(t)
	svc, h := setupResourceTest(t)
	ctx := context.Background()

	name := fmt.Sprintf("k8s-ch-%s", uuid.NewString()[:8])
	channel := newChannelResource(name)
	created, svcErr := svc.Create(ctx, "Channel", channel, nil)
	Expect(svcErr).To(BeNil())
	_, svcErr = svc.Patch(ctx, "Channel", created.ID, &api.ResourcePatch{
		Labels: map[string]string{"env": "test", "team": "hyperfleet"},
	})
	Expect(svcErr).To(BeNil())

	// GET by K8s name.
	code, obj := facadeGet(t, h, "/apis/"+k8sfacade.GroupVersion+"/channels/"+name, nil)
	Expect(code).To(Equal(http.StatusOK))
	Expect(obj["apiVersion"]).To(Equal(k8sfacade.GroupVersion))
	Expect(obj["kind"]).To(Equal("Channel"))

	metadata := metadataOf(obj)
	Expect(metadata["name"]).To(Equal(name))
	Expect(metadata["uid"]).To(Equal(created.ID))
	Expect(metadata["generation"]).To(BeNumerically("==", 2))
	Expect(metadata).NotTo(HaveKey("namespace"))
	Expect(metadata["labels"]).To(HaveKeyWithValue("env", "test"))
	annotations := metadata["annotations"].(map[string]interface{})
	Expect(annotations[k8sfacade.AnnotationID]).To(Equal(created.ID))

	// resourceVersion equals the resource's rv (latest event seq).
	rv := resourceRv(ctx, h, created.ID)
	Expect(metadata["resourceVersion"]).To(Equal(strconv.FormatInt(rv, 10)))

	// spec passthrough.
	Expect(obj["spec"]).To(HaveKeyWithValue("enabled_regex", ".*"))

	// 404 with metav1.Status shape.
	code, status := facadeGet(t, h, "/apis/"+k8sfacade.GroupVersion+"/channels/does-not-exist", nil)
	Expect(code).To(Equal(http.StatusNotFound))
	Expect(status["kind"]).To(Equal("Status"))
	Expect(status["reason"]).To(Equal("NotFound"))

	// LIST contains the object and carries a list resourceVersion >= object rv.
	code, list := facadeGet(t, h, "/apis/"+k8sfacade.GroupVersion+"/channels", nil)
	Expect(code).To(Equal(http.StatusOK))
	Expect(list["kind"]).To(Equal("ChannelList"))
	listRv, err := strconv.ParseInt(metadataOf(list)["resourceVersion"].(string), 10, 64)
	Expect(err).NotTo(HaveOccurred())
	Expect(listRv).To(BeNumerically(">=", rv))

	var found bool
	for _, item := range itemsOf(list) {
		if metadataOf(item)["name"] == name {
			found = true
		}
	}
	Expect(found).To(BeTrue(), "created channel must appear in list")

	// Label selector filtering (equality and set-based).
	code, filtered := facadeGet(t, h, "/apis/"+k8sfacade.GroupVersion+"/channels",
		map[string]string{"labelSelector": "env=test,team in (hyperfleet)"})
	Expect(code).To(Equal(http.StatusOK))
	Expect(itemsOf(filtered)).NotTo(BeEmpty())
	code, none := facadeGet(t, h, "/apis/"+k8sfacade.GroupVersion+"/channels",
		map[string]string{"labelSelector": "env=nomatch"})
	Expect(code).To(Equal(http.StatusOK))
	names := []string{}
	for _, item := range itemsOf(none) {
		names = append(names, metadataOf(item)["name"].(string))
	}
	Expect(names).NotTo(ContainElement(name))

	// fieldSelector metadata.name.
	code, byField := facadeGet(t, h, "/apis/"+k8sfacade.GroupVersion+"/channels",
		map[string]string{"fieldSelector": "metadata.name=" + name})
	Expect(code).To(Equal(http.StatusOK))
	Expect(itemsOf(byField)).To(HaveLen(1))
}

func TestK8sFacade_NamespacedNodePools(t *testing.T) {
	RegisterTestingT(t)
	svc, h := setupResourceTest(t)
	ctx := context.Background()

	cluster, err := h.Factories.NewClusters(h.NewID())
	Expect(err).NotTo(HaveOccurred())
	ownerKind := clusterKind
	nodePool, svcErr := svc.Create(ctx, "NodePool", &api.Resource{
		Kind:      "NodePool",
		Name:      fmt.Sprintf("k8s-np-%s", uuid.NewString()[:8]),
		OwnerID:   &cluster.ID,
		OwnerKind: &ownerKind,
		Spec:      []byte(`{"machine_type": "n1-standard-4", "replicas": 3}`),
		CreatedBy: "test@example.com",
		UpdatedBy: "test@example.com",
	}, nil)
	Expect(svcErr).To(BeNil())

	// Namespaced GET under the parent cluster's ID.
	path := "/apis/" + k8sfacade.GroupVersion + "/namespaces/" + cluster.ID + "/nodepools/" + nodePool.Name
	code, obj := facadeGet(t, h, path, nil)
	Expect(code).To(Equal(http.StatusOK), "namespaced get failed: %v", obj)

	metadata := metadataOf(obj)
	Expect(metadata["namespace"]).To(Equal(cluster.ID))
	Expect(metadata["name"]).To(Equal(nodePool.Name))
	ownerRefs := metadata["ownerReferences"].([]interface{})
	Expect(ownerRefs).To(HaveLen(1))
	Expect(ownerRefs[0].(map[string]interface{})["kind"]).To(Equal(clusterKind))
	Expect(ownerRefs[0].(map[string]interface{})["uid"]).To(Equal(cluster.ID))

	// Namespaced LIST scopes to the parent.
	code, list := facadeGet(t, h,
		"/apis/"+k8sfacade.GroupVersion+"/namespaces/"+cluster.ID+"/nodepools", nil)
	Expect(code).To(Equal(http.StatusOK))
	Expect(itemsOf(list)).To(HaveLen(1))

	// All-namespaces LIST includes it too.
	code, allList := facadeGet(t, h, "/apis/"+k8sfacade.GroupVersion+"/nodepools", nil)
	Expect(code).To(Equal(http.StatusOK))
	var seen bool
	for _, item := range itemsOf(allList) {
		if metadataOf(item)["uid"] == nodePool.ID {
			seen = true
		}
	}
	Expect(seen).To(BeTrue())

	// Different namespace → empty list, and GET → 404.
	code, otherList := facadeGet(t, h,
		"/apis/"+k8sfacade.GroupVersion+"/namespaces/other-cluster/nodepools", nil)
	Expect(code).To(Equal(http.StatusOK))
	Expect(itemsOf(otherList)).To(BeEmpty())
	code, _ = facadeGet(t, h,
		"/apis/"+k8sfacade.GroupVersion+"/namespaces/other-cluster/nodepools/"+nodePool.Name, nil)
	Expect(code).To(Equal(http.StatusNotFound))
}

func TestK8sFacade_SoftDeletedCarriesDeletionTimestamp(t *testing.T) {
	RegisterTestingT(t)
	svc, h := setupResourceTest(t)
	ctx := context.Background()

	cluster, err := h.Factories.NewClusters(h.NewID())
	Expect(err).NotTo(HaveOccurred())
	_, svcErr := svc.Delete(ctx, clusterKind, cluster.ID)
	Expect(svcErr).To(BeNil())

	code, obj := facadeGet(t, h,
		"/apis/"+k8sfacade.GroupVersion+"/clusters/"+cluster.Name, nil)
	Expect(code).To(Equal(http.StatusOK), "soft-deleted object must stay visible")

	metadata := metadataOf(obj)
	Expect(metadata["deletionTimestamp"]).NotTo(BeNil())
	Expect(metadata["finalizers"]).To(ContainElement(k8sfacade.FinalizerAdapters))
}
