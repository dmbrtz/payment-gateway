package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"payment-gateway/internal/commands"
	"payment-gateway/internal/config"
	"payment-gateway/internal/database"
	"payment-gateway/internal/metrics"
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

	paymentProvider := provider.NewMockProvider("success")

	defer func() {
		if err := eventPublisher.Close(); err != nil {
			slog.Error("failed to close event publisher:",
				"error", err)
		}
	}()

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

			if err := json.Unmarshal(message.Value, &envelope); err != nil {
				slog.Error("failed to unmarshal command envelope", "error", err)
				return
			}

			var commandID, paymentID string

			switch envelope.Type {
			case commands.CommandTypeCreatePayment:
				var cmd commands.CreatePaymentCommand

				if err := json.Unmarshal(message.Value, &cmd); err != nil {
					slog.Error("failed to unmarshal create payment command", "error", err)
					return
				}

				commandID = cmd.CommandID
				paymentID = cmd.PaymentID

				if err := paymentWorker.HandleCreatePayment(spanCtx, cmd); err != nil {
					metrics.PaymentsFailed.Inc()
					slog.Error("failed to handle create payment command",
						"command_id", cmd.CommandID,
						"payment_id", cmd.PaymentID,
						"error", err,
					)
					return
				}

				metrics.PaymentsCreated.Inc()

			case commands.CommandTypeProviderCallback:
				var cmd commands.ProviderCallbackCommand

				if err := json.Unmarshal(message.Value, &cmd); err != nil {
					slog.Error("failed to unmarshal provider callback command", "error", err)
					return
				}

				commandID = cmd.CommandID
				paymentID = cmd.PaymentID

				if err := paymentWorker.HandleProviderCallback(spanCtx, cmd); err != nil {
					metrics.PaymentsFailed.Inc()

					slog.Error("failed to handle provider callback command",
						"command_id", cmd.CommandID,
						"payment_id", cmd.PaymentID,
						"status", cmd.Status,
						"error", err,
					)
					return
				}

			default:
				slog.Error("unknown command type", "type", envelope.Type)
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
