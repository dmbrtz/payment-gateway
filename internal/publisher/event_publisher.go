package publisher

import (
	"context"
	"encoding/json"
	"fmt"
	"payment-gateway/internal/events"

	"github.com/segmentio/kafka-go"
)

type KafkaEventPublisher struct {
	writer *kafka.Writer
}

func NewKafkaEventPublisher(
	broker string,
	topic string,
) *KafkaEventPublisher {
	return &KafkaEventPublisher{
		writer: &kafka.Writer{
			Addr:         kafka.TCP(broker),
			Topic:        topic,
			Balancer:     &kafka.Hash{},
			RequiredAcks: kafka.RequireAll,
			Async:        false,
		},
	}
}

func (p *KafkaEventPublisher) PublishPaymentCreated(
	ctx context.Context,
	event events.PaymentCreatedEvent,
) error {
	value, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal payment created event: %w", err)
	}

	err = p.writer.WriteMessages(ctx,
		kafka.Message{
			Key:   []byte(event.PaymentID),
			Value: value,
		})
	if err != nil {
		return fmt.Errorf("publish payment created event: %w", err)
	}
	return nil
}

func (p *KafkaEventPublisher) PublishPaymentStatusChanged(
	ctx context.Context,
	event events.PaymentStatusChangedEvent,
) error {
	value, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal payment status changed event: %w", err)
	}

	err = p.writer.WriteMessages(ctx,
		kafka.Message{
			Key:   []byte(event.PaymentID),
			Value: value,
		})
	if err != nil {
		return fmt.Errorf("publish payment status changed event: %w", err)
	}
	return nil
}

func (p *KafkaEventPublisher) Close() error {
	return p.writer.Close()
}
