package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"durableflow/internal/config"
	"durableflow/internal/db"
	"durableflow/internal/httpapi"
	"durableflow/internal/orchestrator"
	"durableflow/internal/outbox"
	"durableflow/internal/queue"
	"durableflow/internal/telemetry"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	cfg, err := config.LoadAPIConfig()
	if err != nil {
		logger.Error("invalid api configuration", "error", err)
		os.Exit(1)
	}

	shutdownTelemetry, err := telemetry.Setup(ctx, cfg.ServiceName, cfg.OTLPTraceEndpoint, logger)
	if err != nil {
		logger.Error("failed to initialize telemetry", "error", err)
		os.Exit(1)
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = shutdownTelemetry(shutdownCtx)
	}()

	pool, err := db.OpenWithRetry(ctx, cfg.DatabaseURL, cfg.StartupTimeout)
	if err != nil {
		logger.Error("failed to connect to postgres", "error", err)
		os.Exit(1)
	}
	defer pool.Close()

	if err := db.ApplyMigrations(ctx, pool, "migrations"); err != nil {
		logger.Error("failed to apply migrations", "error", err)
		os.Exit(1)
	}

	store := db.NewStore(pool)
	streams := queue.NewRedisStreams(cfg.RedisAddr, cfg.RedisStream, cfg.RedisGroup, logger)
	defer streams.Close()

	if err := streams.WaitForReady(ctx, cfg.StartupTimeout); err != nil {
		logger.Error("failed to connect to redis", "error", err)
		os.Exit(1)
	}

	service := orchestrator.NewService(store, logger)
	router := httpapi.NewRouter(logger, service, func(checkCtx context.Context) error {
		if err := store.Ping(checkCtx); err != nil {
			return err
		}
		return streams.Ping(checkCtx)
	})

	outboxPublisher := outbox.NewPublisher(store, streams, cfg.OutboxPollInterval, logger)
	go func() {
		if err := outboxPublisher.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			logger.Error("outbox publisher exited", "error", err)
		}
	}()

	server := &http.Server{
		Addr:    cfg.HTTPAddr,
		Handler: telemetry.Middleware(cfg.ServiceName, router),
	}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()

	logger.Info("api listening", "addr", cfg.HTTPAddr)
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		logger.Error("api server exited", "error", err)
		os.Exit(1)
	}
}
