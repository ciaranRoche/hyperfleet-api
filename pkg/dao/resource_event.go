package dao

import (
	"context"

	"gorm.io/gorm"

	"github.com/openshift-hyperfleet/hyperfleet-api/pkg/api"
	"github.com/openshift-hyperfleet/hyperfleet-api/pkg/db"
)

// resourceEventsLockKey is the pg_advisory_xact_lock key serializing outbox
// inserts. Holding the lock from seq allocation until commit guarantees that
// events become visible in seq order — a consumer that has seen seq N can
// never later find an unseen event with seq < N.
const resourceEventsLockKey int64 = 202607201200

type ResourceEventDao interface {
	// Create appends an outbox event and, for non-DELETED events, denormalizes
	// the new seq into resources.rv. Returns the assigned seq.
	Create(ctx context.Context, event *api.ResourceEvent) (int64, error)
	// ListSince returns up to limit events with seq > afterSeq, ascending.
	ListSince(ctx context.Context, afterSeq int64, limit int) ([]api.ResourceEvent, error)
	// SafeHead returns the highest committed seq, guaranteed to be a safe
	// watermark: no uncommitted transaction holds a lower seq.
	SafeHead(ctx context.Context) (int64, error)
	// MinRetainedSeq returns the lowest seq still present, or 0 when empty.
	// Requested resource versions below this floor must be answered 410 Gone.
	MinRetainedSeq(ctx context.Context) (int64, error)
	// CompactBelow deletes events with seq < seq, returning the rows removed.
	CompactBelow(ctx context.Context, seq int64) (int64, error)
}

var _ ResourceEventDao = &sqlResourceEventDao{}

type sqlResourceEventDao struct {
	sessionFactory db.SessionFactory
}

func NewResourceEventDao(sessionFactory db.SessionFactory) ResourceEventDao {
	return &sqlResourceEventDao{sessionFactory: sessionFactory}
}

func (d *sqlResourceEventDao) Create(ctx context.Context, event *api.ResourceEvent) (int64, error) {
	g2 := d.sessionFactory.New(ctx)
	// Single statement so the advisory lock is taken before seq allocation on
	// the same connection. The lock is held until the surrounding request
	// transaction commits, making seq order match commit-visibility order.
	var seq int64
	if err := g2.Raw(`INSERT INTO resource_events (resource_id, kind, event_type, object)
		SELECT ?, ?, ?, ?::jsonb FROM (SELECT pg_advisory_xact_lock(?)) AS lock
		RETURNING seq`,
		event.ResourceID, event.Kind, event.EventType, string(event.Object),
		resourceEventsLockKey,
	).Scan(&seq).Error; err != nil {
		db.MarkForRollback(ctx, err)
		return 0, err
	}
	event.Seq = seq

	if event.EventType != api.ResourceEventDeleted {
		if err := g2.Exec(
			"UPDATE resources SET rv = ? WHERE id = ?", seq, event.ResourceID,
		).Error; err != nil {
			db.MarkForRollback(ctx, err)
			return 0, err
		}
	}
	return seq, nil
}

func (d *sqlResourceEventDao) ListSince(
	ctx context.Context, afterSeq int64, limit int,
) ([]api.ResourceEvent, error) {
	g2 := d.sessionFactory.New(ctx)
	var events []api.ResourceEvent
	if err := g2.Where("seq > ?", afterSeq).
		Order("seq asc").Limit(limit).Find(&events).Error; err != nil {
		return nil, err
	}
	return events, nil
}

func (d *sqlResourceEventDao) SafeHead(ctx context.Context) (int64, error) {
	g2 := d.sessionFactory.New(ctx)
	var head int64
	// Acquiring the advisory lock proves every allocated seq has committed
	// (writers hold it until commit), so MAX(seq) is a safe watermark. Run in
	// a transaction so the lock spans the read; nested calls use savepoints.
	err := g2.Transaction(func(tx *gorm.DB) error {
		if err := tx.Exec("SELECT pg_advisory_xact_lock(?)", resourceEventsLockKey).Error; err != nil {
			return err
		}
		return tx.Raw("SELECT COALESCE(MAX(seq), 0) FROM resource_events").Scan(&head).Error
	})
	if err != nil {
		return 0, err
	}
	return head, nil
}

func (d *sqlResourceEventDao) MinRetainedSeq(ctx context.Context) (int64, error) {
	g2 := d.sessionFactory.New(ctx)
	var minSeq int64
	if err := g2.Raw("SELECT COALESCE(MIN(seq), 0) FROM resource_events").Scan(&minSeq).Error; err != nil {
		return 0, err
	}
	return minSeq, nil
}

func (d *sqlResourceEventDao) CompactBelow(ctx context.Context, seq int64) (int64, error) {
	g2 := d.sessionFactory.New(ctx)
	result := g2.Exec("DELETE FROM resource_events WHERE seq < ?", seq)
	if result.Error != nil {
		db.MarkForRollback(ctx, result.Error)
		return 0, result.Error
	}
	return result.RowsAffected, nil
}
