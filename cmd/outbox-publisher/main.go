package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"payment-gateway/internal/config"
	"payment-gateway/internal/database"
	"payment-gateway/internal/outbox"
	"payment-gateway/internal/publisher"
	"syscall"
	"time"
)

func main() {
	cfg, err := config.Load()

	if err != nil {
		slog.Error("Error loading config", "error", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)

	defer stop()

	ticker := time.NewTicker(5 * time.Second)

	defer ticker.Stop()

	pool, err := database.NewPostgresPool(
		ctx,
		cfg.DatabaseURL,
	)

	if err != nil {
		slog.Error("Error creating postgres pool", "error", err)
		os.Exit(1)
	}

	defer pool.Close()

	eventPublisher := publisher.NewKafkaEventPublisher(
		cfg.KafkaBroker,
		cfg.KafkaEventsTopic,
	)

	defer eventPublisher.Close()

	outboxRepository := outbox.NewPostgresOutboxRepository(pool)

	outboxProcessor := outbox.NewOutboxProcessor(
		outboxRepository,
		eventPublisher,
	)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			err = outboxProcessor.Process(
				ctx,
				100,
			)
			if err != nil {
				slog.Error("Error processing outbox", "error", err)
			}
		}
	}

}
