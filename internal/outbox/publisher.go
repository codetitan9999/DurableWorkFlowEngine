package outbox

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"time"

	"durableflow/internal/db"
	"durableflow/internal/domain"
	"durableflow/internal/queue"
	"durableflow/internal/telemetry"
)

type Publisher struct {
	store        *db.Store
	streams      *queue.RedisStreams
	pollInterval time.Duration
	logger       *slog.Logger
}

func NewPublisher(store *db.Store, streams *queue.RedisStreams, pollInterval time.Duration, logger *slog.Logger) *Publisher {
	if pollInterval <= 0 {
		pollInterval = 2 * time.Second
	}

	return &Publisher{
		store:        store,
		streams:      streams,
		pollInterval: pollInterval,
		logger:       logger,
	}
}

func (p *Publisher) Run(ctx context.Context) error {
	ticker := time.NewTicker(p.pollInterval)
	defer ticker.Stop()

	for {
		if err := p.publishOnce(ctx); err != nil && !errors.Is(err, context.Canceled) {
			p.logger.Error("outbox publish batch failed", "error", err)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (p *Publisher) publishOnce(ctx context.Context) error {
	retryCount, err := p.store.EnqueueDueTaskRetries(ctx, 20)
	if err != nil {
		return err
	}
	if retryCount > 0 {
		telemetry.RetriesEnqueued.WithLabelValues("api").Add(float64(retryCount))
		p.logger.InfoContext(ctx, "enqueued due task retries", "count", retryCount)
	}

	events, err := p.store.ListPendingOutbox(ctx, 20)
	if err != nil {
		return err
	}

	for _, event := range events {
		var payload domain.DispatchTaskPayload
		if err := json.Unmarshal(event.PayloadJSON, &payload); err != nil {
			p.logger.Error("invalid outbox payload", "event_id", event.ID, "error", err)
			_ = p.store.RecordOutboxFailure(ctx, event.ID, err)
			continue
		}

		if err := p.streams.DispatchTask(ctx, queue.TaskMessage{
			TaskID:      payload.TaskID,
			ExecutionID: payload.ExecutionID,
			HandlerKey:  payload.HandlerKey,
		}); err != nil {
			telemetry.DispatchEvents.WithLabelValues("api", "failed").Inc()
			_ = p.store.RecordOutboxFailure(ctx, event.ID, err)
			continue
		}

		if err := p.store.MarkOutboxDispatched(ctx, event.ID); err != nil {
			telemetry.DispatchEvents.WithLabelValues("api", "persist_error").Inc()
			return err
		}

		telemetry.DispatchEvents.WithLabelValues("api", "published").Inc()
		p.logger.InfoContext(ctx, "outbox event dispatched", "event_id", event.ID, "task_id", payload.TaskID)
	}

	return nil
}
