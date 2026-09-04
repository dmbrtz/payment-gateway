package main

import (
	"context"
	"database/sql"
	"log/slog"
	"os"
	"payment-gateway/internal/config"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
)

func main() {
	cfg, err := config.Load()

	if err != nil {
		slog.Error("failed to load config", "error", err)
		os.Exit(1)
	}

	db, err := sql.Open("pgx", cfg.DatabaseURL)
	if err != nil {
		slog.Error("failed to connect to database", "error", err)
		os.Exit(1)
	}
	defer db.Close()

	if err = db.PingContext(context.Background()); err != nil {
		slog.Error("failed to ping database", "error", err)
		os.Exit(1)
	}

	if err = goose.SetDialect("postgres"); err != nil {
		slog.Error("failed to set dialect", "error", err)
		os.Exit(1)
	}

	if err = goose.Up(db, "migrations"); err != nil {
		slog.Error("failed to up migrations", "error", err)
		os.Exit(1)
	}

	slog.Info("DB migrations completed")
}
