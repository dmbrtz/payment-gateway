package outbox

import (
	"context"
	"fmt"
	"payment-gateway/internal/publisher"
)

type OutboxProcessor struct {
	outboxRepository OutboxRepository
	eventPublisher   publisher.EventPublisher
}

func NewOutboxProcessor(
	outboxRepository OutboxRepository,
	eventPublisher publisher.EventPublisher,
) *OutboxProcessor {
	return &OutboxProcessor{
		outboxRepository: outboxRepository,
		eventPublisher:   eventPublisher,
	}
}

func (p *OutboxProcessor) Process(ctx context.Context, limit int) error {
	events, err := p.outboxRepository.GetPending(
		ctx,
		limit,
	)
	if err != nil {
		return fmt.Errorf("outbox repository get pending error: %w", err)
	}

	for _, event := range events {
		err = p.eventPublisher.PublishRawOutboxEvent(
			ctx,
			event.AggregateID,
			event.Payload,
		)
		if err != nil {
			return fmt.Errorf("event publisher publish raw outbox event error: %w", err)
		}

		err = p.outboxRepository.MarkPublished(
			ctx,
			event.ID,
		)
		if err != nil {
			return fmt.Errorf("outbox repository mark published error: %w", err)
		}
	}
	return nil
}
