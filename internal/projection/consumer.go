package projection

import (
	"context"
	"encoding/json"
	"fmt"
	"payment-gateway/internal/events"

	"github.com/segmentio/kafka-go"
)

type Consumer struct {
	consumer *kafka.Reader
	handler  *Handler
}

func NewConsumer(consumer *kafka.Reader, handler *Handler) *Consumer {
	return &Consumer{
		consumer: consumer,
		handler:  handler,
	}
}

type Envelope struct {
	Type events.EventType `json:"type"`
}

func (c *Consumer) Run(
	ctx context.Context,
) error {
	var envelope Envelope
	for {
		message, err := c.consumer.FetchMessage(ctx)
		if err != nil {
			return fmt.Errorf("fetch kafka consumer message error %w", err)
		}
		if err := json.Unmarshal(message.Value, &envelope); err != nil {
			return fmt.Errorf("unmarshal kafka consumer message error %w", err)
		}
		switch envelope.Type {
		case events.EventTypePaymentCreated:
			var event events.PaymentCreatedEvent
			if err := json.Unmarshal(message.Value, &event); err != nil {
				return fmt.Errorf("unmarshal kafka consumer PaymentCreatedEvent message error %w", err)
			}
			err := c.handler.HandlePaymentCreated(ctx, event)
			if err != nil {
				return fmt.Errorf("handle kafka consumer PaymentCreatedEvent error %w", err)
			}
		case events.EventTypePaymentStatusChanged:
			var event events.PaymentStatusChangedEvent
			if err := json.Unmarshal(message.Value, &event); err != nil {
				return fmt.Errorf("unmarshal kafka consumer PaymentStatusChangedEvent message error %w", err)
			}
			err := c.handler.HandlePaymentStatusChanged(ctx, event)
			if err != nil {
				return fmt.Errorf("handle kafka consumer PaymentStatusChangedEvent error %w", err)
			}
		default:
			return fmt.Errorf("unknown event type: %s", envelope.Type)
		}
		err = c.consumer.CommitMessages(ctx, message)

		if err != nil {
			return fmt.Errorf("commit consumer messages error %w", err)
		}
	}
}
