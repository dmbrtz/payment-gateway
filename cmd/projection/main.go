package main

// DATABASE_URL=postgres://payment:payment@localhost:5432/payment_gateway?sslmode=disable;HTTP_ADDR=:8080;KAFKA_BROKER=127.0.0.1:9092;KAFKA_GROUP_ID=payment-worker;KAFKA_TOPIC=payment.commands;KAFKA_PROJECTION_GROUP_ID=payment-projection

import (
	"context"
	"log/slog"
	"os"
	"payment-gateway/internal/config"
	"payment-gateway/internal/database"
	"payment-gateway/internal/projection"

	"github.com/segmentio/kafka-go"
)

func main() {
	ctx := context.Background()
	cfg, err := config.Load()
	if err != nil {
		slog.Error("failed to load config", "err", err)
		os.Exit(1)
	}
	pool, err := database.NewPostgresPool(
		ctx,
		cfg.DatabaseURL,
	)
	if err != nil {
		slog.Error("failed to connect to postgres", "err", err)
		os.Exit(1)
	}
	defer pool.Close()

	repository := projection.NewPostgresRepository(pool)
	handler := projection.NewHandler(repository)
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{cfg.KafkaBroker},
		GroupID: cfg.KafkaProjectionGroupID,
		Topic:   cfg.KafkaEventsTopic,
	})
	defer func() {
		err := reader.Close()
		if err != nil {
			slog.Error("failed to close reader", "err", err)
		}
	}()
	consumer := projection.NewConsumer(reader, handler)
	err = consumer.Run(ctx)
	if err != nil {
		slog.Error("failed to run consumer", "err", err)
		os.Exit(1)
	}
}
