// Package cache implements the in-process event feed backing the facade's
// watch endpoints. One Feed runs per API replica: it tails the shared
// resource_events outbox in seq order and fans events out to subscribed
// watches. Polling provides correctness; PostgreSQL LISTEN/NOTIFY only
// shortens the wait.
package cache

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/openshift-hyperfleet/hyperfleet-api/pkg/api"
	"github.com/openshift-hyperfleet/hyperfleet-api/pkg/dao"
	"github.com/openshift-hyperfleet/hyperfleet-api/pkg/db"
	"github.com/openshift-hyperfleet/hyperfleet-api/pkg/logger"
)

// notifyChannel is the pg_notify channel fired by the resource_events
// insert trigger (see migration 202607201200).
const notifyChannel = "resource_events"

// listBatchSize bounds one poll iteration; a full batch triggers an
// immediate follow-up poll.
const listBatchSize = 500

// subscriberBuffer is each watch subscription's channel capacity. A
// subscriber that falls this far behind is closed (the client re-watches,
// standard Kubernetes behavior).
const subscriberBuffer = 1024

// Event is one outbox event decoded for watch delivery.
type Event struct {
	Resource *api.Resource
	Kind     string
	Type     string
	Seq      int64
}

// Config tunes a Feed; see config.K8sFacadeConfig.
type Config struct {
	PollInterval time.Duration
	Retention    time.Duration
	MinKeep      int
}

// Feed tails resource_events and broadcasts to subscribers.
type Feed struct {
	eventDao       dao.ResourceEventDao
	sessionFactory db.SessionFactory
	subs           map[int64]*subscriber
	wake           chan struct{}
	cfg            Config
	head           atomic.Int64
	nextID         int64
	mu             sync.Mutex
	started        atomic.Bool
}

type subscriber struct {
	ch   chan Event
	kind string
	dead bool
}

func NewFeed(eventDao dao.ResourceEventDao, sessionFactory db.SessionFactory, cfg Config) *Feed {
	return &Feed{
		eventDao:       eventDao,
		sessionFactory: sessionFactory,
		cfg:            cfg,
		subs:           make(map[int64]*subscriber),
		wake:           make(chan struct{}, 1),
	}
}

// Start bootstraps the feed watermark and launches the tail loop, the
// NOTIFY hint listener, and the compaction janitor. Non-blocking.
func (f *Feed) Start(ctx context.Context) error {
	head, err := f.eventDao.SafeHead(ctx)
	if err != nil {
		return err
	}
	f.head.Store(head)
	f.started.Store(true)

	go f.run(ctx)
	go f.listenForHints(ctx)
	go f.janitor(ctx)

	logger.With(ctx, "head", head).Info("k8sfacade: watch feed started")
	return nil
}

// Ready reports whether the feed has bootstrapped.
func (f *Feed) Ready() bool {
	return f.started.Load()
}

// Head returns the highest seq the feed has fully processed. It is a safe
// list watermark: every event at or below it has been delivered to
// subscribers registered at the time.
func (f *Feed) Head() int64 {
	return f.head.Load()
}

// MinRetainedSeq exposes the compaction floor for 410 Gone decisions.
func (f *Feed) MinRetainedSeq(ctx context.Context) (int64, error) {
	return f.eventDao.MinRetainedSeq(ctx)
}

// ListSince replays committed events with seq > afterSeq (bounded by limit)
// straight from the outbox table. Used by watch handlers for replay before
// going live; the retention janitor bounds how far back this reaches.
func (f *Feed) ListSince(ctx context.Context, afterSeq int64, limit int) ([]Event, error) {
	rows, err := f.eventDao.ListSince(ctx, afterSeq, limit)
	if err != nil {
		return nil, err
	}
	events := make([]Event, 0, len(rows))
	for i := range rows {
		event, decodeErr := decode(&rows[i])
		if decodeErr != nil {
			logger.WithError(ctx, decodeErr).Error("k8sfacade: skipping undecodable event")
			continue
		}
		events = append(events, event)
	}
	return events, nil
}

// Subscribe registers a watch for one kind. Events with seq > the current
// head at subscription time are delivered in order. The channel is closed
// when the subscriber is too slow or the feed stops.
func (f *Feed) Subscribe(kind string) (int64, <-chan Event) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.nextID++
	id := f.nextID
	sub := &subscriber{kind: kind, ch: make(chan Event, subscriberBuffer)}
	f.subs[id] = sub
	return id, sub.ch
}

func (f *Feed) Unsubscribe(id int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if sub, ok := f.subs[id]; ok {
		delete(f.subs, id)
		if !sub.dead {
			close(sub.ch)
		}
	}
}

func (f *Feed) run(ctx context.Context) {
	ticker := time.NewTicker(f.cfg.PollInterval)
	defer ticker.Stop()
	for {
		fullBatch := f.poll(ctx)
		if fullBatch {
			// More events are waiting — keep draining without sleeping.
			continue
		}
		select {
		case <-ctx.Done():
			f.closeAll()
			return
		case <-f.wake:
		case <-ticker.C:
		}
	}
}

// poll consumes one batch and returns true when the batch was full.
func (f *Feed) poll(ctx context.Context) bool {
	rows, err := f.eventDao.ListSince(ctx, f.head.Load(), listBatchSize)
	if err != nil {
		logger.WithError(ctx, err).Error("k8sfacade: event poll failed")
		return false
	}
	for i := range rows {
		event, decodeErr := decode(&rows[i])
		if decodeErr != nil {
			logger.WithError(ctx, decodeErr).Error("k8sfacade: skipping undecodable event")
			f.head.Store(rows[i].Seq)
			continue
		}
		f.deliver(event)
		f.head.Store(event.Seq)
	}
	return len(rows) == listBatchSize
}

func (f *Feed) deliver(event Event) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for id, sub := range f.subs {
		if sub.kind != event.Kind {
			continue
		}
		select {
		case sub.ch <- event:
		default:
			// Slow consumer: close it, the client re-establishes the watch.
			sub.dead = true
			close(sub.ch)
			delete(f.subs, id)
		}
	}
}

func (f *Feed) closeAll() {
	f.mu.Lock()
	defer f.mu.Unlock()
	for id, sub := range f.subs {
		if !sub.dead {
			close(sub.ch)
		}
		delete(f.subs, id)
	}
}

// listenForHints wires the dormant LISTEN/NOTIFY plumbing as a latency
// optimization. NewListener blocks forever, so it runs in its own goroutine;
// delivery is best-effort by design.
func (f *Feed) listenForHints(ctx context.Context) {
	defer func() {
		if r := recover(); r != nil {
			logger.With(ctx, "panic", r).
				Error("k8sfacade: NOTIFY listener unavailable, relying on polling")
		}
	}()
	f.sessionFactory.NewListener(ctx, notifyChannel, func(string) {
		select {
		case f.wake <- struct{}{}:
		default:
		}
	})
}

// janitor compacts old events. pg_try_advisory_lock ensures only one
// replica compacts at a time; MinKeep events are always retained so recent
// watch replays and 410 decisions stay cheap.
func (f *Feed) janitor(ctx context.Context) {
	interval := f.cfg.Retention / 4
	if interval < time.Minute {
		interval = time.Minute
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			f.compact(ctx)
		}
	}
}

// janitorLockKey serializes compaction across replicas. Distinct from the
// outbox insert lock key.
const janitorLockKey int64 = 202607201201

func (f *Feed) compact(ctx context.Context) {
	session := f.sessionFactory.New(ctx)
	var locked bool
	if err := session.Raw("SELECT pg_try_advisory_lock(?)", janitorLockKey).Scan(&locked).Error; err != nil || !locked {
		return
	}
	defer func() {
		_ = session.Exec("SELECT pg_advisory_unlock(?)", janitorLockKey).Error
	}()

	floor := f.head.Load() - int64(f.cfg.MinKeep)
	if floor <= 0 {
		return
	}
	// Only drop events old enough that no live watch should need them.
	var cutoffSeq int64
	err := session.Raw(
		"SELECT COALESCE(MAX(seq), 0) FROM resource_events WHERE created_at < ? AND seq < ?",
		time.Now().Add(-f.cfg.Retention), floor,
	).Scan(&cutoffSeq).Error
	if err != nil || cutoffSeq == 0 {
		return
	}
	removed, err := f.eventDao.CompactBelow(ctx, cutoffSeq+1)
	if err != nil {
		logger.WithError(ctx, err).Error("k8sfacade: event compaction failed")
		return
	}
	if removed > 0 {
		logger.With(ctx, "removed", removed, "below_seq", cutoffSeq+1).
			Info("k8sfacade: compacted resource events")
	}
}

func decode(row *api.ResourceEvent) (Event, error) {
	resource, err := row.UnmarshalSnapshot()
	if err != nil {
		return Event{}, err
	}
	return Event{
		Seq:      row.Seq,
		Kind:     row.Kind,
		Type:     row.EventType,
		Resource: resource,
	}, nil
}
