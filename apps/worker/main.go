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
	"durableflow/internal/handlers"
	"durableflow/internal/httpapi"
	"durableflow/internal/orchestrator"
	"durableflow/internal/queue"
	"durableflow/internal/telemetry"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	cfg, err := config.LoadWorkerConfig()
	if err != nil {
		logger.Error("invalid worker configuration", "error", err)
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

	registry := handlers.NewRegistry(
		handlers.NewSampleEchoHandler(logger, store),
		handlers.NewNotificationSendHandler(logger, store),
	)

	worker := orchestrator.NewWorker(store, registry, logger)

	mux := http.NewServeMux()
	httpapi.RegisterHealthRoutes(mux, "worker", func(checkCtx context.Context) error {
		if err := store.Ping(checkCtx); err != nil {
			return err
		}
		return streams.Ping(checkCtx)
	})

	server := &http.Server{
		Addr:    cfg.HTTPAddr,
		Handler: telemetry.Middleware(cfg.ServiceName, mux),
	}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()

	go func() {
		logger.Info("worker consuming stream", "stream", cfg.RedisStream, "group", cfg.RedisGroup)
		if err := streams.Consume(ctx, queue.ConsumeOptions{
			Consumer:       cfg.RedisConsumer,
			ReclaimMinIdle: cfg.RedisReclaimMinIdle,
			ReclaimCount:   cfg.RedisReclaimCount,
		}, worker.HandleDispatchedTask); err != nil && !errors.Is(err, context.Canceled) {
			logger.Error("stream consumer exited", "error", err)
			stop()
		}
	}()

	logger.Info("worker health server listening", "addr", cfg.HTTPAddr)
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		logger.Error("worker http server exited", "error", err)
		os.Exit(1)
	}
}
