package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"payment-gateway/internal/api"
	"payment-gateway/internal/config"
	"payment-gateway/internal/database"
	"payment-gateway/internal/publisher"
	"payment-gateway/internal/repository"
	"payment-gateway/internal/tracing"

	"go.opentelemetry.io/otel"
)

func main() {
	cfg, err := config.Load()

	if err != nil {
		slog.Error("failed to load config file", "error", err)
		os.Exit(1)
	}

	ctx := context.Background()

	pool, err := database.NewPostgresPool(ctx, cfg.DatabaseURL)

	if err != nil {
		slog.Error("failed to connect to database", "error", err)
		os.Exit(1)
	}

	defer pool.Close()

	viewRepo := repository.NewPostgresPaymentViewRepository(pool)

	if err != nil {
		slog.Error("Error loading config", "error", err)
		os.Exit(1)
	}

	commandPublisher := publisher.NewKafkaCommandPublisher(
		cfg.KafkaBroker,
		cfg.KafkaTopic,
	)
	defer commandPublisher.Close()

	tp, err := tracing.NewTracerProvider()
	if err != nil {
		slog.Error("Error creating tracer provider", "error", err)
		os.Exit(1)
	}

	defer func() {
		if err := tp.Shutdown(context.Background()); err != nil {
			slog.Error("Error shutting down tracer provider", "error", err)
		}
	}()

	tracer := otel.Tracer("payment-api")

	handler := api.NewHandler(commandPublisher, tracer, viewRepo)

	http.HandleFunc(
		"/payments",
		handler.CreatePaymentHandler,
	)

	http.HandleFunc("/payments/", handler.GetPaymentHandler)

	slog.Info("API server started", "address", cfg.HTTPAddr)
	err = http.ListenAndServe(cfg.HTTPAddr, nil)
	if err != nil {
		fmt.Println("server error", err)
	}
}
