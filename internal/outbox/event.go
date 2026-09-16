package outbox

import (
	"time"

	"github.com/google/uuid"
)

type OutboxEvent struct {
	ID          string
	AggregateID string
	EventType   string
	Payload     []byte
	CreatedAt   time.Time
	PublishedAt *time.Time
}

func NewOutboxEvent(
	aggregateId string,
	eventType string,
	payload []byte,
) OutboxEvent {
	return OutboxEvent{
		ID:          uuid.NewString(),
		AggregateID: aggregateId,
		EventType:   eventType,
		Payload:     payload,
		CreatedAt:   time.Now().UTC(),
		PublishedAt: nil,
	}
}
