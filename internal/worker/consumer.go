package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"payment-gateway/internal/commands"
	"payment-gateway/internal/dlq"
	"payment-gateway/internal/metrics"
	"payment-gateway/internal/payment"
	"time"

	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel/trace"
)

type Consumer struct {
	reader       *kafka.Reader
	dlqPublisher *dlq.DlqKafkaPublisher
	worker       *Worker
	tracer       trace.Tracer
}

func NewConsumer(
	reader *kafka.Reader,
	dlqPublisher *dlq.DlqKafkaPublisher,
	worker *Worker,
	tracer trace.Tracer,
) *Consumer {
	return &Consumer{
		reader:       reader,
		dlqPublisher: dlqPublisher,
		worker:       worker,
		tracer:       tracer,
	}
}

func (c *Consumer) Run(ctx context.Context) error {
	for {
		message, err := c.reader.FetchMessage(ctx)
		if err != nil {
			slog.Error("failed to fetch kafka message", "error", err)
			continue
		}

		func() {
			spanCtx, span := c.tracer.Start(ctx, "process-payment")
			defer span.End()

			start := time.Now()

			var envelope commands.Envelope

			if unmarshalErr := json.Unmarshal(message.Value, &envelope); unmarshalErr != nil {
				err := dlq.SendToDlqAndCommit(
					spanCtx,
					c.reader,
					c.dlqPublisher,
					message,
					fmt.Sprintf("failed to unmarshal command envelope: %v",
						unmarshalErr,
					),
				)

				if err != nil {
					slog.Error("failed to SendToDlqAndCommit",
						"error", err,
					)
				}
				return
			}

			var commandID, paymentID string

			switch envelope.Type {
			case commands.CommandTypeCreatePayment:
				var cmd commands.CreatePaymentCommand

				if err := json.Unmarshal(message.Value, &cmd); err != nil {
					slog.Error("failed to unmarshal create payment command", "error", err)
					dlqErr := dlq.SendToDlqAndCommit(
						spanCtx,
						c.reader,
						c.dlqPublisher,
						message,
						fmt.Sprintf("failed to unmarshal create payment command: %v", err),
					)
					if dlqErr != nil {
						slog.Error("failed to SendToDlqAndCommit", dlqErr)
						return
					}
					return
				}

				commandID = cmd.CommandID
				paymentID = cmd.PaymentID

				err = retry(func() error {
					return c.worker.HandleCreatePayment(spanCtx, cmd)
				}, 3, 5*time.Second, func(err error) bool {
					if errors.Is(err, ErrIdempotencyKeyConflict) || errors.Is(err, payment.ErrInvalidAmount) || errors.Is(err, payment.ErrInvalidCurrency) || errors.Is(err, payment.ErrInvalidPaymentID) || errors.Is(err, payment.ErrInvalidProvider) || errors.Is(err, payment.ErrInvalidClientID) || errors.Is(err, payment.ErrInvalidIdempotencyKey) || errors.Is(err, payment.ErrInvalidStatusTransition) {
						return false
					}
					return true
				},
				)

				if errors.Is(err, ErrIdempotencyKeyConflict) {
					slog.Warn("Idempotency key conflict",
						"commandID", cmd.CommandID,
						"paymentID", cmd.PaymentID,
						"idempotencyKey", cmd.IdempotencyKey)
				} else if err != nil {
					metrics.PaymentsFailed.Inc()

					slog.Error("failed to handle create payment command",
						"commandID", cmd.CommandID,
						"paymentID", cmd.PaymentID,
						"error", err,
					)
					dlqErr := dlq.SendToDlqAndCommit(
						spanCtx,
						c.reader,
						c.dlqPublisher,
						message,
						fmt.Sprintf("create payment processing failed: %v", err),
					)
					if dlqErr != nil {
						slog.Error("failed to SendToDlqAndCommit",
							"error", dlqErr,
						)
					}
					return
				} else {
					metrics.PaymentsCreated.Inc()
				}

			case commands.CommandTypeProviderCallback:
				var cmd commands.ProviderCallbackCommand

				if err := json.Unmarshal(message.Value, &cmd); err != nil {
					slog.Error("failed to unmarshal provider callback command", "error", err)
					dlqErr := dlq.SendToDlqAndCommit(
						spanCtx,
						c.reader,
						c.dlqPublisher,
						message,
						fmt.Sprintf("failed to unmarshal provider callback command: %v", err),
					)

					if dlqErr != nil {
						slog.Error("failed to SendToDlqAndCommit", dlqErr)
					}
					return
				}

				commandID = cmd.CommandID
				paymentID = cmd.PaymentID

				err := retry(func() error {
					return c.worker.HandleProviderCallback(spanCtx, cmd)
				},
					3, 5*time.Second, func(err error) bool {
						if errors.Is(err, ErrInvalidProviderCallbackStatus) {
							return false
						}
						return true
					},
				)

				if err != nil {
					metrics.PaymentsFailed.Inc()
					slog.Error("failed to handle provider callback command")
					dlqErr := dlq.SendToDlqAndCommit(
						spanCtx,
						c.reader,
						c.dlqPublisher,
						message,
						fmt.Sprintf("provider callback processing failed: %v", err),
					)

					if dlqErr != nil {
						slog.Error("failed to SendToDlqAndCommit",
							"error", dlqErr,
						)
						return
					}
				}

			default:
				err := dlq.SendToDlqAndCommit(
					spanCtx,
					c.reader,
					c.dlqPublisher,
					message,
					fmt.Sprintf("unknown command type: %v", envelope.Type),
				)
				if err != nil {
					slog.Error("failed to SendToDlqAndCommit",
						"error", err,
					)
				}
				return

			}

			metrics.PaymentProcessingDuration.Observe(time.Since(start).Seconds())

			commCtx, commSpan := c.tracer.Start(spanCtx, "kafka-commit")

			err = c.reader.CommitMessages(commCtx, message)

			commSpan.End()

			if err != nil {
				slog.Error("commit messages error command_id=", commandID,
					"error", err,
				)
				return
			}

			slog.Info("committed command",
				"command_id", commandID,
				"payment_id", paymentID,
			)
		}()
	}
}
