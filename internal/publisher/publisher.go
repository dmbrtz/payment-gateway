package publisher

import (
	"context"
	"encoding/json"
	"fmt"
	"payment-gateway/internal/commands"
	"payment-gateway/internal/events"

	"github.com/segmentio/kafka-go"
)

type CommandPublisher interface {
	PublishCreatePayment(
		ctx context.Context,
		cmd commands.CreatePaymentCommand,
	) error

	PublishProviderCallback(
		ctx context.Context,
		cmd commands.ProviderCallbackCommand,
	) error
}

type KafkaPublisher struct {
	writer *kafka.Writer
}

func NewKafkaPublisher(broker string, topic string) *KafkaPublisher {
	return &KafkaPublisher{
		writer: &kafka.Writer{
			Addr:         kafka.TCP(broker),
			Topic:        topic,
			Balancer:     &kafka.Hash{},
			RequiredAcks: kafka.RequireAll,
			Async:        false,
		},
	}
}

func (p *KafkaPublisher) PublishCreatePayment(
	ctx context.Context,
	cmd commands.CreatePaymentCommand,
) error {
	value, err := json.Marshal(cmd)
	if err != nil {
		return fmt.Errorf("marshal create payment command: %w", err)
	}

	err = p.writer.WriteMessages(
		ctx, kafka.Message{
			Key:   []byte(cmd.PaymentID),
			Value: value,
		},
	)
	if err != nil {
		return fmt.Errorf("publish create payment command: %w", err)
	}

	fmt.Printf("published command to Kafka: command_id=%s payment_id=%s\n",
		cmd.CommandID,
		cmd.PaymentID,
	)

	return nil
}

func (p *KafkaPublisher) PublishProviderCallback(
	ctx context.Context,
	cmd commands.ProviderCallbackCommand,
) error {
	value, err := json.Marshal(cmd)
	if err != nil {
		return fmt.Errorf("marshal provider callback command: %w", err)
	}

	err = p.writer.WriteMessages(
		ctx,
		kafka.Message{
			Key:   []byte(cmd.PaymentID),
			Value: value,
		})

	if err != nil {
		return fmt.Errorf("publish provider callback command: %w", err)
	}

	return nil
}

func (p *KafkaPublisher) Close() error {
	return p.writer.Close()
}

type EventPublisher interface {
	PublishPaymentCreated(
		ctx context.Context,
		event events.PaymentCreatedEvent,
	) error
	PublishPaymentStatusChanged(
		ctx context.Context,
		event events.PaymentStatusChangedEvent,
	) error
}
