package migrations

import (
	"github.com/go-gormigrate/gormigrate/v2"
	"gorm.io/gorm"
)

// addResourceEvents creates the resource_events outbox that assigns every
// resource mutation a globally-ordered sequence number (the resource version
// used by the Kubernetes-compatible watch facade). Events carry a full JSONB
// snapshot so DELETED rows act as tombstones after the resource row is gone.
func addResourceEvents() *gormigrate.Migration {
	return &gormigrate.Migration{
		ID: "202607201200",
		Migrate: func(tx *gorm.DB) error {
			// resource_id has no FK on purpose: tombstone events must
			// outlive the resources row they describe.
			if err := tx.Exec(`CREATE TABLE IF NOT EXISTS resource_events (
				seq          BIGSERIAL PRIMARY KEY,
				resource_id  VARCHAR(255) NOT NULL,
				kind         VARCHAR(100) NOT NULL,
				event_type   VARCHAR(10)  NOT NULL,
				object       JSONB        NOT NULL,
				created_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW()
			);`).Error; err != nil {
				return err
			}

			if err := tx.Exec(
				"CREATE INDEX IF NOT EXISTS idx_resource_events_kind_seq " +
					"ON resource_events (kind, seq);",
			).Error; err != nil {
				return err
			}

			// Wake-up hint for in-process watch caches. Consumers must not
			// rely on NOTIFY delivery for correctness — they poll by seq.
			if err := tx.Exec(`CREATE OR REPLACE FUNCTION notify_resource_event() RETURNS trigger AS $$
			BEGIN
				PERFORM pg_notify('resource_events', NEW.seq::text);
				RETURN NEW;
			END $$ LANGUAGE plpgsql;`).Error; err != nil {
				return err
			}
			if err := tx.Exec(`DROP TRIGGER IF EXISTS resource_events_notify ON resource_events;`).Error; err != nil {
				return err
			}
			if err := tx.Exec(`CREATE TRIGGER resource_events_notify
				AFTER INSERT ON resource_events
				FOR EACH ROW EXECUTE FUNCTION notify_resource_event();`).Error; err != nil {
				return err
			}

			// Denormalized latest-event seq per resource.
			if err := tx.Exec(
				"ALTER TABLE resources ADD COLUMN IF NOT EXISTS rv BIGINT NOT NULL DEFAULT 0;",
			).Error; err != nil {
				return err
			}
			if err := tx.Exec(
				"CREATE INDEX IF NOT EXISTS idx_resources_kind_rv ON resources (kind, rv);",
			).Error; err != nil {
				return err
			}

			// Backfill: one ADDED event per pre-existing resource, in creation
			// order. The jsonb_build_object key set mirrors resourceSnapshot in
			// pkg/api/resource_event.go — keep the two in sync.
			if err := tx.Exec(`INSERT INTO resource_events (resource_id, kind, event_type, object)
			SELECT r.id, r.kind, 'ADDED', jsonb_build_object(
				'id', r.id,
				'kind', r.kind,
				'name', r.name,
				'href', r.href,
				'owner_id', r.owner_id,
				'owner_kind', r.owner_kind,
				'owner_href', r.owner_href,
				'created_by', r.created_by,
				'updated_by', r.updated_by,
				'deleted_by', r.deleted_by,
				'created_time', r.created_time,
				'updated_time', r.updated_time,
				'deleted_time', r.deleted_time,
				'generation', r.generation,
				'spec', r.spec,
				'labels', COALESCE((
					SELECT jsonb_agg(jsonb_build_object('key', l.key, 'value', l.value))
					FROM resource_labels l WHERE l.resource_id = r.id), '[]'::jsonb),
				'conditions', COALESCE((
					SELECT jsonb_agg(jsonb_build_object(
						'type', c.type,
						'status', c.status,
						'reason', c.reason,
						'message', c.message,
						'observed_generation', c.observed_generation,
						'created_time', c.created_time,
						'last_updated_time', c.last_updated_time,
						'last_transition_time', c.last_transition_time))
					FROM resource_conditions c WHERE c.resource_id = r.id), '[]'::jsonb),
				'references', COALESCE((
					SELECT jsonb_agg(jsonb_build_object(
						'source_id', rr.source_id,
						'ref_type', rr.ref_type,
						'target_id', rr.target_id,
						'target_kind', rr.target_kind))
					FROM resource_references rr WHERE rr.source_id = r.id), '[]'::jsonb)
			)
			FROM resources r
			ORDER BY r.created_time, r.id;`).Error; err != nil {
				return err
			}

			return tx.Exec(`UPDATE resources SET rv = e.seq
				FROM resource_events e
				WHERE e.resource_id = resources.id;`).Error
		},
	}
}
