package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"payment-gateway/internal/commands"
	"payment-gateway/internal/config"
	"payment-gateway/internal/database"
	"payment-gateway/internal/dlq"
	"payment-gateway/internal/metrics"
	"payment-gateway/internal/payment"
	"payment-gateway/internal/provider"
	"payment-gateway/internal/publisher"
	"payment-gateway/internal/repository"
	"payment-gateway/internal/tracing"
	"payment-gateway/internal/worker"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
)

func main() {
	ctx := context.Background()

	cfg, err := config.Load()

	tp, err := tracing.NewTracerProvider()
	if err != nil {
		slog.Error("failed to initilize tracer provider", "error", err)
		os.Exit(1)
	}

	defer func() {
		if err := tp.Shutdown(ctx); err != nil {
			slog.Error("failed to shutdown tracer provider", "error", err)
		}
	}()

	tracer := otel.Tracer("payment-worker")

	slog.Info("loaded kafka config",
		"broker", fmt.Sprintf("%q", cfg.KafkaBroker),
		"topic", fmt.Sprintf("%q", cfg.KafkaTopic),
		"group_id", fmt.Sprintf("%q", cfg.KafkaGroupID),
	)

	if err != nil {
		slog.Error("Error loading config", "error", err)
		os.Exit(1)
	}

	databasePool, err := database.NewPostgresPool(
		ctx,
		cfg.DatabaseURL,
	)

	if err != nil {
		slog.Error("failed to connect to database", "error", err)
		os.Exit(1)
	}
	defer databasePool.Close()

	paymentRepository :=
		repository.NewPostgresPaymentRepository(databasePool)

	eventPublisher := publisher.NewKafkaEventPublisher(
		cfg.KafkaBroker,
		cfg.KafkaEventsTopic,
	)

	defer func() {
		if err := eventPublisher.Close(); err != nil {
			slog.Error("failed to close event publisher:",
				"error", err)
		}
	}()

	dlqPublisher := dlq.NewDlqKafkaPublisher(
		cfg.KafkaBroker,
		cfg.KafkaDlqTopic,
	)

	defer func() {
		if err := dlqPublisher.Close(); err != nil {
			slog.Error("failed to close dlq publisher:",
				"error", err,
			)
		}
	}()

	paymentProvider := provider.NewMockProvider("success")

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{cfg.KafkaBroker},
		GroupID: cfg.KafkaGroupID,
		Topic:   cfg.KafkaTopic,
	})

	defer func() {
		if err := reader.Close(); err != nil {
			slog.Error("close Kafka reader", "error", err)
		}
	}()

	slog.Info("Payment Worker started")

	http.Handle("/metrics", promhttp.Handler())

	go func() {
		slog.Info("started worker metrics server",
			"port", 9091)

		if err := http.ListenAndServe(":9091", nil); err != nil {
			slog.Error("failed to start metrics server", "error", err)
			os.Exit(1)
		}
	}()

	paymentWorker := worker.NewWorker(
		paymentRepository,
		eventPublisher,
		tracer,
		paymentProvider,
	)

	for {
		message, err := reader.FetchMessage(ctx)
		if err != nil {
			slog.Error("failed to fetch kafka message", "error", err)
			continue
		}

		func() {
			spanCtx, span := tracer.Start(ctx, "process-payment")
			defer span.End()

			start := time.Now()

			var envelope commands.Envelope

			if unmarshalErr := json.Unmarshal(message.Value, &envelope); unmarshalErr != nil {
				err := dlq.SendToDlqAndCommit(
					spanCtx,
					reader,
					dlqPublisher,
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
						reader,
						dlqPublisher,
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
					return paymentWorker.HandleCreatePayment(spanCtx, cmd)
				}, 3, 5*time.Second, func(err error) bool {
					if errors.Is(err, worker.ErrIdempotencyKeyConflict) || errors.Is(err, payment.ErrInvalidAmount) || errors.Is(err, payment.ErrInvalidCurrency) || errors.Is(err, payment.ErrInvalidPaymentID) || errors.Is(err, payment.ErrInvalidProvider) || errors.Is(err, payment.ErrInvalidClientID) || errors.Is(err, payment.ErrInvalidIdempotencyKey) || errors.Is(err, payment.ErrInvalidStatusTransition) {
						return false
					}
					return true
				},
				)

				if errors.Is(err, worker.ErrIdempotencyKeyConflict) {
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
						reader,
						dlqPublisher,
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
						reader,
						dlqPublisher,
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
					return paymentWorker.HandleProviderCallback(spanCtx, cmd)
				},
					3, 5*time.Second, func(err error) bool {
						if errors.Is(err, worker.ErrInvalidProviderCallbackStatus) {
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
						reader,
						dlqPublisher,
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
					reader,
					dlqPublisher,
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

			commCtx, commSpan := tracer.Start(spanCtx, "kafka-commit")

			err = reader.CommitMessages(commCtx, message)

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

func retry(operation func() error, attempts int, delay time.Duration, isRetryable func(error) bool) error {
	var err error
	for i := 0; i < attempts; i++ {
		err = operation()
		if err != nil {
			if i < attempts-1 {
				if isRetryable(err) {
					time.Sleep(delay)
					continue
				} else {
					return err
				}
			}
		} else {
			return nil
		}
	}
	return err
}
