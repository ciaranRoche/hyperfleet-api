package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"testing"

	"github.com/google/uuid"
	. "github.com/onsi/gomega"
	"gopkg.in/resty.v1"

	"github.com/openshift-hyperfleet/hyperfleet-api/pkg/api"
	"github.com/openshift-hyperfleet/hyperfleet-api/pkg/api/openapi"
	"github.com/openshift-hyperfleet/hyperfleet-api/pkg/dao"
	"github.com/openshift-hyperfleet/hyperfleet-api/test"
)

// eventsForResource returns all outbox events for one resource in seq order.
func eventsForResource(ctx context.Context, h *test.Helper, resourceID string) []api.ResourceEvent {
	var events []api.ResourceEvent
	err := h.DBFactory.New(ctx).
		Where("resource_id = ?", resourceID).
		Order("seq asc").Find(&events).Error
	Expect(err).NotTo(HaveOccurred())
	return events
}

func resourceRv(ctx context.Context, h *test.Helper, resourceID string) int64 {
	var rv int64
	err := h.DBFactory.New(ctx).Raw(
		"SELECT rv FROM resources WHERE id = ?", resourceID).Scan(&rv).Error
	Expect(err).NotTo(HaveOccurred())
	return rv
}

// TestResourceEvents_HardDeleteLifecycle covers the event stream of a kind
// without required adapters (Channel): ADDED on create, MODIFIED on patch,
// DELETED tombstone on hard delete.
func TestResourceEvents_HardDeleteLifecycle(t *testing.T) {
	RegisterTestingT(t)
	svc, h := setupResourceTest(t)
	ctx := context.Background()

	channel := newChannelResource(fmt.Sprintf("ev-ch-%s", uuid.NewString()[:8]))
	created, svcErr := svc.Create(ctx, "Channel", channel, nil)
	Expect(svcErr).To(BeNil())

	events := eventsForResource(ctx, h, created.ID)
	Expect(events).To(HaveLen(1))
	Expect(events[0].EventType).To(Equal(api.ResourceEventAdded))
	Expect(events[0].Kind).To(Equal("Channel"))
	Expect(created.Rv).To(Equal(events[0].Seq), "service should report the event seq as Rv")
	Expect(resourceRv(ctx, h, created.ID)).To(Equal(events[0].Seq), "resources.rv should be denormalized")

	snapshot, err := events[0].UnmarshalSnapshot()
	Expect(err).NotTo(HaveOccurred())
	Expect(snapshot.ID).To(Equal(created.ID))
	Expect(snapshot.Name).To(Equal(created.Name))
	Expect(snapshot.Generation).To(Equal(int32(1)))
	Expect(snapshot.DeletedTime).To(BeNil())

	// Patch labels — one MODIFIED event, rv advances.
	patched, svcErr := svc.Patch(ctx, "Channel", created.ID, &api.ResourcePatch{
		Labels: map[string]string{"tier": "stable"},
	})
	Expect(svcErr).To(BeNil())

	events = eventsForResource(ctx, h, created.ID)
	Expect(events).To(HaveLen(2))
	Expect(events[1].EventType).To(Equal(api.ResourceEventModified))
	Expect(events[1].Seq).To(BeNumerically(">", events[0].Seq))
	Expect(patched.Rv).To(Equal(events[1].Seq))
	Expect(resourceRv(ctx, h, created.ID)).To(Equal(events[1].Seq))

	snapshot, err = events[1].UnmarshalSnapshot()
	Expect(err).NotTo(HaveOccurred())
	Expect(labelsToMap(snapshot.Labels)).To(HaveKeyWithValue("tier", "stable"))
	Expect(snapshot.Generation).To(Equal(int32(2)))

	// No-op patch — no event.
	_, svcErr = svc.Patch(ctx, "Channel", created.ID, &api.ResourcePatch{})
	Expect(svcErr).To(BeNil())
	Expect(eventsForResource(ctx, h, created.ID)).To(HaveLen(2))

	// Hard delete (Channel has no required adapters) — DELETED tombstone
	// survives although the resource row is gone.
	_, svcErr = svc.Delete(ctx, "Channel", created.ID)
	Expect(svcErr).To(BeNil())
	Expect(checkResourceCount(ctx, h, []string{created.ID}, 0)).To(Succeed())

	events = eventsForResource(ctx, h, created.ID)
	Expect(events).To(HaveLen(3))
	Expect(events[2].EventType).To(Equal(api.ResourceEventDeleted))
	snapshot, err = events[2].UnmarshalSnapshot()
	Expect(err).NotTo(HaveOccurred())
	Expect(snapshot.ID).To(Equal(created.ID))
}

// TestResourceEvents_SoftDeleteLifecycle covers a kind with required adapters
// (Cluster): soft delete emits MODIFIED with deleted_time set, force-delete
// emits the final DELETED tombstone.
func TestResourceEvents_SoftDeleteLifecycle(t *testing.T) {
	RegisterTestingT(t)
	svc, h := setupResourceTest(t)
	ctx := context.Background()

	cluster, err := h.Factories.NewClusters(h.NewID())
	Expect(err).NotTo(HaveOccurred())

	events := eventsForResource(ctx, h, cluster.ID)
	Expect(events).To(HaveLen(1))
	Expect(events[0].EventType).To(Equal(api.ResourceEventAdded))

	// Soft delete: resource row remains, MODIFIED event carries deleted_time.
	_, svcErr := svc.Delete(ctx, "Cluster", cluster.ID)
	Expect(svcErr).To(BeNil())
	Expect(checkResourceCount(ctx, h, []string{cluster.ID}, 1)).To(Succeed())

	events = eventsForResource(ctx, h, cluster.ID)
	Expect(events).To(HaveLen(2))
	Expect(events[1].EventType).To(Equal(api.ResourceEventModified))
	snapshot, err := events[1].UnmarshalSnapshot()
	Expect(err).NotTo(HaveOccurred())
	Expect(snapshot.DeletedTime).NotTo(BeNil())
	Expect(resourceRv(ctx, h, cluster.ID)).To(Equal(events[1].Seq))

	// Force-delete finalizes: DELETED tombstone, row gone.
	svcErr = svc.ForceDelete(ctx, "Cluster", cluster.ID, "test cleanup")
	Expect(svcErr).To(BeNil())
	Expect(checkResourceCount(ctx, h, []string{cluster.ID}, 0)).To(Succeed())

	events = eventsForResource(ctx, h, cluster.ID)
	Expect(events).To(HaveLen(3))
	Expect(events[2].EventType).To(Equal(api.ResourceEventDeleted))
}

// TestResourceEvents_ConditionOnlyChange proves that adapter status reports
// which change aggregated conditions emit MODIFIED and advance rv WITHOUT
// bumping the resource generation — the gap that made updated_time/generation
// unusable as a change cursor.
func TestResourceEvents_ConditionOnlyChange(t *testing.T) {
	RegisterTestingT(t)
	_, h := setupResourceTest(t)
	ctx := context.Background()

	cluster, err := h.Factories.NewClusters(h.NewID())
	Expect(err).NotTo(HaveOccurred())
	preEvents := eventsForResource(ctx, h, cluster.ID)
	Expect(preEvents).To(HaveLen(1))

	account := h.NewRandAccount()
	authCtx := h.NewAuthenticatedContext(account)
	token := test.GetAccessTokenFromContext(authCtx)

	statusReq := newAdapterStatusRequest(
		"validation", cluster.Generation,
		[]openapi.ConditionRequest{
			{Type: api.AdapterConditionTypeAvailable, Status: openapi.AdapterConditionStatusTrue},
			{Type: api.AdapterConditionTypeApplied, Status: openapi.AdapterConditionStatusTrue},
			{Type: api.AdapterConditionTypeHealth, Status: openapi.AdapterConditionStatusTrue},
		},
		nil,
	)
	body, err := json.Marshal(statusReq)
	Expect(err).NotTo(HaveOccurred())

	putResp, err := resty.R().
		SetHeader("Content-Type", "application/json").
		SetHeader("Authorization", fmt.Sprintf("Bearer %s", token)).
		SetBody(body).
		Put(h.RestURL("/clusters/" + cluster.ID + "/statuses"))
	Expect(err).NotTo(HaveOccurred())
	Expect(putResp.StatusCode()).To(BeNumerically("<", 300),
		"adapter status PUT failed: %s", putResp.String())

	events := eventsForResource(ctx, h, cluster.ID)
	Expect(events).To(HaveLen(2), "condition change must emit an event")
	Expect(events[1].EventType).To(Equal(api.ResourceEventModified))

	snapshot, err := events[1].UnmarshalSnapshot()
	Expect(err).NotTo(HaveOccurred())
	Expect(snapshot.Generation).To(Equal(cluster.Generation),
		"condition-only change must not bump generation")
	Expect(snapshot.Conditions).NotTo(BeEmpty())
	Expect(resourceRv(ctx, h, cluster.ID)).To(Equal(events[1].Seq),
		"rv must advance on condition-only change")
}

// TestResourceEvents_ConcurrentOrdering hammers the API with concurrent
// mutations (through the transaction middleware) while a consumer polls the
// outbox. The consumed stream must be append-only: no event may ever appear
// below a previously observed head — the property watch caches rely on.
func TestResourceEvents_ConcurrentOrdering(t *testing.T) {
	RegisterTestingT(t)
	h, client := test.RegisterIntegration(t)
	ctx := context.Background()

	account := h.NewRandAccount()
	authCtx := h.NewAuthenticatedContext(account)

	eventDao := dao.NewResourceEventDao(h.DBFactory)
	startHead, err := eventDao.SafeHead(ctx)
	Expect(err).NotTo(HaveOccurred())

	const writers = 8
	const patchesPerWriter = 5

	clusters := make([]*api.Resource, writers)
	for i := range clusters {
		c, ferr := h.Factories.NewClusters(h.NewID())
		Expect(ferr).NotTo(HaveOccurred())
		clusters[i] = c
	}

	stopPolling := make(chan struct{})
	pollDone := make(chan error, 1)

	// Consumer: repeatedly read the full window and assert the previous read
	// is a strict prefix of the current one (append-only, ascending). One
	// final read runs after stopPolling closes so post-writer state is checked.
	go func() {
		var prev []int64
		for {
			events, listErr := eventDao.ListSince(ctx, startHead, 10000)
			if listErr != nil {
				pollDone <- listErr
				return
			}
			seqs := make([]int64, 0, len(events))
			for _, e := range events {
				seqs = append(seqs, e.Seq)
			}
			for i := 1; i < len(seqs); i++ {
				if seqs[i] <= seqs[i-1] {
					pollDone <- fmt.Errorf("events not strictly ascending: %v", seqs)
					return
				}
			}
			if len(seqs) < len(prev) {
				pollDone <- fmt.Errorf("event window shrank: had %d, now %d", len(prev), len(seqs))
				return
			}
			for i := range prev {
				if seqs[i] != prev[i] {
					pollDone <- fmt.Errorf(
						"append-only violation at index %d: event %d appeared below observed head %d",
						i, seqs[i], prev[len(prev)-1])
					return
				}
			}
			prev = seqs
			select {
			case <-stopPolling:
				pollDone <- nil
				return
			default:
			}
		}
	}()

	// Writers: concurrent PATCH requests through the real HTTP path
	// (transaction middleware + advisory-lock outbox insert).
	var writersWg sync.WaitGroup
	patchErrs := make(chan error, writers*patchesPerWriter)
	for w := 0; w < writers; w++ {
		writersWg.Add(1)
		go func(c *api.Resource, w int) {
			defer writersWg.Done()
			for p := 0; p < patchesPerWriter; p++ {
				spec := openapi.ClusterSpec{"writer": w, "iteration": p}
				resp, perr := client.PatchClusterByIdWithResponse(
					authCtx, c.ID,
					openapi.PatchClusterByIdJSONRequestBody{Spec: &spec},
					test.WithAuthToken(authCtx),
				)
				if perr != nil {
					patchErrs <- perr
					return
				}
				if resp.StatusCode() != http.StatusOK {
					patchErrs <- fmt.Errorf("patch status %d: %s", resp.StatusCode(), string(resp.Body))
					return
				}
			}
		}(clusters[w], w)
	}

	writersWg.Wait()
	close(stopPolling)
	Expect(<-pollDone).To(Succeed())

	close(patchErrs)
	for perr := range patchErrs {
		Expect(perr).NotTo(HaveOccurred())
	}

	// Every patch produced exactly one event.
	finalEvents, err := eventDao.ListSince(ctx, startHead, 10000)
	Expect(err).NotTo(HaveOccurred())
	modified := 0
	for _, e := range finalEvents {
		if e.EventType == api.ResourceEventModified {
			modified++
		}
	}
	Expect(modified).To(Equal(writers*patchesPerWriter),
		"expected one MODIFIED event per successful patch")
}
