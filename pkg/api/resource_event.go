package api

import (
	"encoding/json"
	"fmt"
	"time"

	"gorm.io/datatypes"
)

// Resource event types, matching Kubernetes watch event semantics.
const (
	ResourceEventAdded    = "ADDED"
	ResourceEventModified = "MODIFIED"
	ResourceEventDeleted  = "DELETED"
)

// ResourceEvent is one row in the resource_events outbox. Every resource
// mutation appends exactly one event per affected resource, in the same
// transaction as the mutation. The BIGSERIAL Seq is the global monotonic
// resource version: consumers replay events in Seq order to reconstruct
// resource state, and DELETED events act as tombstones that outlive the
// resource row (hence no foreign key on ResourceID).
type ResourceEvent struct {
	CreatedAt  time.Time      `json:"created_at"`
	ResourceID string         `json:"resource_id" gorm:"size:255;not null"`
	Kind       string         `json:"kind" gorm:"size:100;not null"`
	EventType  string         `json:"event_type" gorm:"size:10;not null"`
	Object     datatypes.JSON `json:"object" gorm:"type:jsonb;not null"`
	Seq        int64          `json:"seq" gorm:"primaryKey;autoIncrement"`
}

func (ResourceEvent) TableName() string {
	return "resource_events"
}

// resourceSnapshot is the wire shape of ResourceEvent.Object: the full native
// state of a resource including its associations. The migration backfill
// (202607201200_add_resource_events.go) builds the same shape in SQL — keep
// the two key sets in sync.
type resourceSnapshot struct {
	CreatedTime time.Time           `json:"created_time"`
	UpdatedTime time.Time           `json:"updated_time"`
	DeletedTime *time.Time          `json:"deleted_time,omitempty"`
	DeletedBy   *string             `json:"deleted_by,omitempty"`
	OwnerID     *string             `json:"owner_id,omitempty"`
	OwnerKind   *string             `json:"owner_kind,omitempty"`
	OwnerHref   *string             `json:"owner_href,omitempty"`
	CreatedBy   string              `json:"created_by"`
	UpdatedBy   string              `json:"updated_by"`
	Href        string              `json:"href,omitempty"`
	Name        string              `json:"name"`
	Kind        string              `json:"kind"`
	ID          string              `json:"id"`
	Spec        datatypes.JSON      `json:"spec"`
	Labels      []ResourceLabel     `json:"labels"`
	Conditions  []ResourceCondition `json:"conditions"`
	References  []ResourceReference `json:"references"`
	Generation  int32               `json:"generation"`
}

// NewResourceEvent builds an outbox event carrying a full snapshot of the
// resource's current in-memory state (which must include any association
// changes made during the mutation).
func NewResourceEvent(eventType string, r *Resource) (*ResourceEvent, error) {
	snapshot := resourceSnapshot{
		ID:          r.ID,
		Kind:        r.Kind,
		Name:        r.Name,
		Href:        r.Href,
		OwnerID:     r.OwnerID,
		OwnerKind:   r.OwnerKind,
		OwnerHref:   r.OwnerHref,
		CreatedBy:   r.CreatedBy,
		UpdatedBy:   r.UpdatedBy,
		DeletedBy:   r.DeletedBy,
		CreatedTime: r.CreatedTime,
		UpdatedTime: r.UpdatedTime,
		DeletedTime: r.DeletedTime,
		Generation:  r.Generation,
		Spec:        r.Spec,
		Labels:      r.Labels,
		Conditions:  r.Conditions,
		References:  r.References,
	}
	if snapshot.Labels == nil {
		snapshot.Labels = []ResourceLabel{}
	}
	if snapshot.Conditions == nil {
		snapshot.Conditions = []ResourceCondition{}
	}
	if snapshot.References == nil {
		snapshot.References = []ResourceReference{}
	}
	object, err := json.Marshal(snapshot)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal resource snapshot: %w", err)
	}
	return &ResourceEvent{
		ResourceID: r.ID,
		Kind:       r.Kind,
		EventType:  eventType,
		Object:     object,
	}, nil
}

// UnmarshalSnapshot decodes the event's object payload back into a Resource
// with its associations populated.
func (e *ResourceEvent) UnmarshalSnapshot() (*Resource, error) {
	var s resourceSnapshot
	if err := json.Unmarshal(e.Object, &s); err != nil {
		return nil, fmt.Errorf("failed to unmarshal resource snapshot for event %d: %w", e.Seq, err)
	}
	return &Resource{
		OwnerID:     s.OwnerID,
		OwnerHref:   s.OwnerHref,
		OwnerKind:   s.OwnerKind,
		DeletedBy:   s.DeletedBy,
		DeletedTime: s.DeletedTime,
		Meta: Meta{
			ID:          s.ID,
			CreatedTime: s.CreatedTime,
			UpdatedTime: s.UpdatedTime,
		},
		Kind:       s.Kind,
		Name:       s.Name,
		Href:       s.Href,
		CreatedBy:  s.CreatedBy,
		UpdatedBy:  s.UpdatedBy,
		Labels:     s.Labels,
		Spec:       s.Spec,
		Conditions: s.Conditions,
		References: s.References,
		Generation: s.Generation,
		Rv:         e.Seq,
	}, nil
}
