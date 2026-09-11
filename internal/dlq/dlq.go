package dlq

import (
	"context"
	"log/slog"
	"time"

	"github.com/segmentio/kafka-go"
)

type Message struct {
	OriginalTopic string    `json:"original_topic"`
	Partition     int       `json:"partition"`
	Offset        int64     `json:"offset"`
	Key           []byte    `json:"key"`
	Payload       []byte    `json:"payload"`
	Reason        string    `json:"reason"`
	FailedAt      time.Time `json:"failed_at"`
}

func SendToDlqAndCommit(ctx context.Context, reader *kafka.Reader, publisher *DlqKafkaPublisher, message kafka.Message, reason string) error {
	failedMessage := Message{
		OriginalTopic: message.Topic,
		Partition:     message.Partition,
		Offset:        message.Offset,
		Key:           message.Key,
		Payload:       message.Value,
		Reason:        reason,
		FailedAt:      time.Now().UTC(),
	}

	publishErr := publisher.PublishDlqMessage(
		ctx,
		failedMessage,
	)

	if publishErr != nil {
		slog.Error("failed to publish Dlq message", "error", publishErr)
		return publishErr
	}

	commitErr := reader.CommitMessages(ctx, message)

	if commitErr != nil {
		slog.Error("failed to commit original kafka message", "error", commitErr)
		return commitErr
	}

	return nil
}
