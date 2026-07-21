// facade-watch-controller is a minimal Kubernetes-style controller used to
// manually verify the HyperFleet API's Kubernetes-compatible facade. It runs
// unmodified client-go shared informers (LIST + WATCH) against the facade
// and logs every add/update/delete it observes, including the aggregated
// Reconciled condition.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
)

const (
	group   = "hyperfleet.openshift.io"
	version = "v1alpha1"
)

// watchedResources are the facade resources this controller informs on.
var watchedResources = []string{"clusters", "nodepools", "channels"}

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	apiURL := os.Getenv("HYPERFLEET_API_URL")
	if apiURL == "" {
		apiURL = "http://hyperfleet-api:8000"
	}

	cfg := &rest.Config{
		Host:        apiURL,
		BearerToken: os.Getenv("HYPERFLEET_API_TOKEN"),
	}
	client, err := dynamic.NewForConfig(cfg)
	if err != nil {
		logger.Error("failed to build dynamic client", "error", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Resync 0: pure watch-driven, no periodic relist — the point of the test.
	factory := dynamicinformer.NewFilteredDynamicSharedInformerFactory(
		client, 0, metav1.NamespaceAll, nil)

	syncFuncs := make([]cache.InformerSynced, 0, len(watchedResources))
	for _, resource := range watchedResources {
		gvr := schema.GroupVersionResource{Group: group, Version: version, Resource: resource}
		informer := factory.ForResource(gvr)
		if _, err := informer.Informer().AddEventHandler(eventLogger(logger, resource)); err != nil {
			logger.Error("failed to add event handler", "resource", resource, "error", err)
			os.Exit(1)
		}
		syncFuncs = append(syncFuncs, informer.Informer().HasSynced)
	}

	logger.Info("starting informers", "api", apiURL, "resources", fmt.Sprint(watchedResources))
	factory.Start(ctx.Done())

	syncCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	if !cache.WaitForCacheSync(syncCtx.Done(), syncFuncs...) {
		logger.Error("informer caches failed to sync")
		os.Exit(1)
	}
	logger.Info("all informer caches synced — watching for events")

	<-ctx.Done()
	logger.Info("shutting down")
}

// eventLogger logs informer events the way a reconcile loop would receive them.
func eventLogger(logger *slog.Logger, resource string) cache.ResourceEventHandler {
	log := func(event string, obj interface{}) {
		u, ok := obj.(*unstructured.Unstructured)
		if !ok {
			if tombstone, isTombstone := obj.(cache.DeletedFinalStateUnknown); isTombstone {
				u, ok = tombstone.Obj.(*unstructured.Unstructured)
				if !ok {
					logger.Warn("tombstone without object", "resource", resource, "key", tombstone.Key)
					return
				}
			} else {
				return
			}
		}
		attrs := []any{
			"resource", resource,
			"name", u.GetName(),
			"uid", string(u.GetUID()),
			"rv", u.GetResourceVersion(),
			"generation", u.GetGeneration(),
		}
		if ns := u.GetNamespace(); ns != "" {
			attrs = append(attrs, "namespace", ns)
		}
		if labels := u.GetLabels(); len(labels) > 0 {
			attrs = append(attrs, "labels", fmt.Sprint(labels))
		}
		if u.GetDeletionTimestamp() != nil {
			attrs = append(attrs, "deletionTimestamp", u.GetDeletionTimestamp().Format(time.RFC3339))
		}
		if reconciled := conditionStatus(u, "Reconciled"); reconciled != "" {
			attrs = append(attrs, "reconciled", reconciled)
		}
		logger.Info(event, attrs...)
	}
	return cache.ResourceEventHandlerFuncs{
		AddFunc:    func(obj interface{}) { log("ADD", obj) },
		UpdateFunc: func(_, obj interface{}) { log("UPDATE", obj) },
		DeleteFunc: func(obj interface{}) { log("DELETE", obj) },
	}
}

// conditionStatus extracts status.conditions[type==condType].status.
func conditionStatus(u *unstructured.Unstructured, condType string) string {
	conditions, found, err := unstructured.NestedSlice(u.Object, "status", "conditions")
	if !found || err != nil {
		return ""
	}
	for _, raw := range conditions {
		condition, ok := raw.(map[string]interface{})
		if !ok {
			continue
		}
		if condition["type"] == condType {
			status, _ := condition["status"].(string)
			return status
		}
	}
	return ""
}
