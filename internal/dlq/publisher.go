package dlq

import (
	"context"
	"encoding/json"

	"github.com/segmentio/kafka-go"
)

type Publisher interface {
	PublishDlqMessage(ctx context.Context, message Message) error
}

type DlqKafkaPublisher struct {
	writer *kafka.Writer
}

func NewDlqKafkaPublisher(broker string, topic string) *DlqKafkaPublisher {
	return &DlqKafkaPublisher{
		writer: &kafka.Writer{
			Addr:         kafka.TCP(broker),
			Topic:        topic,
			RequiredAcks: kafka.RequireAll,
			Async:        false,
		},
	}
}

func (p *DlqKafkaPublisher) PublishDlqMessage(ctx context.Context,
	message Message,
) error {
	value, err := json.Marshal(message)

	if err != nil {
		return err
	}

	err = p.writer.WriteMessages(ctx, kafka.Message{
		Key:   message.Key,
		Value: value,
	})

	if err != nil {
		return err
	}

	return nil

}

func (p *DlqKafkaPublisher) Close() error {
	return p.writer.Close()
}
