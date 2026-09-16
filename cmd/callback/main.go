package main

import (
	"log/slog"
	"net/http"
	"os"
	"payment-gateway/internal/callback"
	"payment-gateway/internal/config"
	"payment-gateway/internal/publisher"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		slog.Error("failed to load config", "err", err)
		os.Exit(1)
	}

	commandPublisher := publisher.NewKafkaCommandPublisher(
		cfg.KafkaBroker,
		cfg.KafkaTopic,
	)
	defer commandPublisher.Close()

	handler := callback.NewHandler(commandPublisher)

	http.HandleFunc(
		"/callbacks/provider",
		handler.HandlePaymentCallback,
	)

	slog.Info(
		"callback server started",
		"address", cfg.HTTPAddr,
	)

	if err := http.ListenAndServe(cfg.HTTPAddr, nil); err != nil {
		slog.Error("callback server error", "err", err)
		os.Exit(1)
	}

}
