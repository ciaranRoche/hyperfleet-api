package integration

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	. "github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"

	"github.com/openshift-hyperfleet/hyperfleet-api/pkg/api"
	"github.com/openshift-hyperfleet/hyperfleet-api/pkg/k8sfacade"
	"github.com/openshift-hyperfleet/hyperfleet-api/test"
)

var channelGVR = schema.GroupVersionResource{
	Group:    k8sfacade.Group,
	Version:  k8sfacade.Version,
	Resource: "channels",
}

const clusterKind = "Cluster"

var nodePoolGVR = schema.GroupVersionResource{
	Group:    k8sfacade.Group,
	Version:  k8sfacade.Version,
	Resource: "nodepools",
}

// informerEvents records handler firings from a shared informer.
type informerEvents struct {
	added   map[string]int
	updated map[string]int
	deleted map[string]int
	mu      sync.Mutex
}

func newInformerEvents() *informerEvents {
	return &informerEvents{
		added:   map[string]int{},
		updated: map[string]int{},
		deleted: map[string]int{},
	}
}

func (e *informerEvents) handler() cache.ResourceEventHandler {
	name := func(obj interface{}) string {
		if tombstone, ok := obj.(cache.DeletedFinalStateUnknown); ok {
			obj = tombstone.Obj
		}
		u, ok := obj.(*unstructured.Unstructured)
		if !ok {
			return ""
		}
		return u.GetName()
	}
	return cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			e.mu.Lock()
			defer e.mu.Unlock()
			e.added[name(obj)]++
		},
		UpdateFunc: func(_, obj interface{}) {
			e.mu.Lock()
			defer e.mu.Unlock()
			e.updated[name(obj)]++
		},
		DeleteFunc: func(obj interface{}) {
			e.mu.Lock()
			defer e.mu.Unlock()
			e.deleted[name(obj)]++
		},
	}
}

func (e *informerEvents) count(kind string, name string) int {
	e.mu.Lock()
	defer e.mu.Unlock()
	switch kind {
	case "added":
		return e.added[name]
	case "updated":
		return e.updated[name]
	default:
		return e.deleted[name]
	}
}

// newDynamicClient builds a client-go dynamic client pointed at the facade.
func newDynamicClient(t *testing.T, h *test.Helper) dynamic.Interface {
	t.Helper()
	account := h.NewRandAccount()
	authCtx := h.NewAuthenticatedContext(account)
	token := test.GetAccessTokenFromContext(authCtx)

	client, err := dynamic.NewForConfig(&rest.Config{
		Host:        h.RootURL(""),
		BearerToken: token,
	})
	Expect(err).NotTo(HaveOccurred())
	return client
}

// TestK8sInformer_EndToEnd runs an unmodified client-go dynamic shared
// informer against the facade: initial LIST+WATCH sync, then add, update,
// and delete handler firings driven through the native HyperFleet API.
func TestK8sInformer_EndToEnd(t *testing.T) {
	RegisterTestingT(t)
	svc, h := setupResourceTest(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Pre-existing object must arrive via the initial LIST.
	preName := fmt.Sprintf("pre-ch-%s", uuid.NewString()[:8])
	_, svcErr := svc.Create(ctx, "Channel", newChannelResource(preName), nil)
	Expect(svcErr).To(BeNil())

	client := newDynamicClient(t, h)
	factory := dynamicinformer.NewFilteredDynamicSharedInformerFactory(
		client, 0, metav1.NamespaceAll, nil)
	informer := factory.ForResource(channelGVR)
	events := newInformerEvents()
	_, err := informer.Informer().AddEventHandler(events.handler())
	Expect(err).NotTo(HaveOccurred())

	factory.Start(ctx.Done())
	synced := cache.WaitForCacheSync(ctx.Done(), informer.Informer().HasSynced)
	Expect(synced).To(BeTrue(), "informer cache must sync against the facade")

	// LIST populated the store with the pre-existing channel.
	Eventually(func() int { return events.count("added", preName) },
		10*time.Second, 100*time.Millisecond).Should(BeNumerically(">=", 1))
	obj, err := informer.Lister().Get(preName)
	Expect(err).NotTo(HaveOccurred())
	Expect(obj.(*unstructured.Unstructured).GetUID()).NotTo(BeEmpty())

	// Live ADDED through the watch.
	liveName := fmt.Sprintf("live-ch-%s", uuid.NewString()[:8])
	created, svcErr := svc.Create(ctx, "Channel", newChannelResource(liveName), nil)
	Expect(svcErr).To(BeNil())
	Eventually(func() int { return events.count("added", liveName) },
		10*time.Second, 100*time.Millisecond).Should(BeNumerically(">=", 1))

	// Live MODIFIED.
	_, svcErr = svc.Patch(ctx, "Channel", created.ID, &api.ResourcePatch{
		Labels: map[string]string{"seen": "yes"},
	})
	Expect(svcErr).To(BeNil())
	Eventually(func() int { return events.count("updated", liveName) },
		10*time.Second, 100*time.Millisecond).Should(BeNumerically(">=", 1))

	// Informer store reflects the label after the update.
	Eventually(func() string {
		stored, getErr := informer.Lister().Get(liveName)
		if getErr != nil {
			return ""
		}
		return stored.(*unstructured.Unstructured).GetLabels()["seen"]
	}, 10*time.Second, 100*time.Millisecond).Should(Equal("yes"))

	// Live DELETED (Channel hard-deletes).
	_, svcErr = svc.Delete(ctx, "Channel", created.ID)
	Expect(svcErr).To(BeNil())
	Eventually(func() int { return events.count("deleted", liveName) },
		10*time.Second, 100*time.Millisecond).Should(BeNumerically(">=", 1))
	Eventually(func() bool {
		_, getErr := informer.Lister().Get(liveName)
		return getErr != nil
	}, 10*time.Second, 100*time.Millisecond).Should(BeTrue(), "deleted object must leave the store")
}

// TestK8sInformer_NamespacedNodePools verifies a namespace-scoped informer
// (parent cluster ID as namespace) sees only its namespace.
func TestK8sInformer_NamespacedNodePools(t *testing.T) {
	RegisterTestingT(t)
	svc, h := setupResourceTest(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cluster, err := h.Factories.NewClusters(h.NewID())
	Expect(err).NotTo(HaveOccurred())
	otherCluster, err := h.Factories.NewClusters(h.NewID())
	Expect(err).NotTo(HaveOccurred())

	client := newDynamicClient(t, h)
	factory := dynamicinformer.NewFilteredDynamicSharedInformerFactory(
		client, 0, cluster.ID, nil)
	informer := factory.ForResource(nodePoolGVR)
	events := newInformerEvents()
	_, err = informer.Informer().AddEventHandler(events.handler())
	Expect(err).NotTo(HaveOccurred())

	factory.Start(ctx.Done())
	Expect(cache.WaitForCacheSync(ctx.Done(), informer.Informer().HasSynced)).To(BeTrue())

	ownerKind := clusterKind
	inNs, svcErr := svc.Create(ctx, "NodePool", &api.Resource{
		Kind: "NodePool", Name: "informer-np", OwnerID: &cluster.ID, OwnerKind: &ownerKind,
		Spec:      []byte(`{"machine_type": "n1-standard-4", "replicas": 1}`),
		CreatedBy: "test@example.com", UpdatedBy: "test@example.com",
	}, nil)
	Expect(svcErr).To(BeNil())
	_, svcErr = svc.Create(ctx, "NodePool", &api.Resource{
		Kind: "NodePool", Name: "other-informer-np", OwnerID: &otherCluster.ID, OwnerKind: &ownerKind,
		Spec:      []byte(`{"machine_type": "n1-standard-4", "replicas": 1}`),
		CreatedBy: "test@example.com", UpdatedBy: "test@example.com",
	}, nil)
	Expect(svcErr).To(BeNil())

	Eventually(func() int { return events.count("added", "informer-np") },
		10*time.Second, 100*time.Millisecond).Should(BeNumerically(">=", 1))
	Expect(events.count("added", "other-informer-np")).To(BeZero(),
		"informer scoped to %s must not see other namespaces", cluster.ID)

	stored, err := informer.Lister().ByNamespace(cluster.ID).Get("informer-np")
	Expect(err).NotTo(HaveOccurred())
	Expect(stored.(*unstructured.Unstructured).GetNamespace()).To(Equal(cluster.ID))
	Expect(string(stored.(*unstructured.Unstructured).GetUID())).To(Equal(inNs.ID))
}
