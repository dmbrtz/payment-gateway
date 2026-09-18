package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"payment-gateway/internal/config"
	"payment-gateway/internal/database"
	"payment-gateway/internal/dlq"
	"payment-gateway/internal/outbox"
	"payment-gateway/internal/provider"
	"payment-gateway/internal/repository"
	"payment-gateway/internal/tracing"
	"payment-gateway/internal/worker"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
)

func main() {
	ctx := context.Background()

	cfg, err := config.Load()

	if err != nil {
		slog.Error("failed to load config", "error", err)
		os.Exit(1)
	}

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

	outboxRepository := outbox.NewPostgresOutboxRepository(databasePool)

	transactionManager := database.NewPostgresTransactionManager(databasePool)

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
		tracer,
		paymentProvider,
		outboxRepository,
		transactionManager,
	)

	consumer := worker.NewConsumer(
		reader,
		dlqPublisher,
		paymentWorker,
		tracer,
	)

	if err := consumer.Run(ctx); err != nil {
		slog.Error("consumer error", "error", err)
	}
}
