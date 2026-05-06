package orchestrator

import (
	"context"
	"fmt"
	"log/slog"

	"durableflow/internal/db"
	"durableflow/internal/handlers"
	"durableflow/internal/queue"
	"durableflow/internal/telemetry"
)

type Worker struct {
	store    *db.Store
	registry *handlers.Registry
	logger   *slog.Logger
}

func NewWorker(store *db.Store, registry *handlers.Registry, logger *slog.Logger) *Worker {
	return &Worker{
		store:    store,
		registry: registry,
		logger:   logger,
	}
}

func (w *Worker) HandleDispatchedTask(ctx context.Context, message queue.TaskMessage) error {
	task, attempt, alreadyCompleted, err := w.store.StartTaskAttempt(ctx, message.TaskID)
	if err != nil {
		telemetry.TasksProcessed.WithLabelValues("worker", message.HandlerKey, "db_error").Inc()
		return err
	}

	if alreadyCompleted {
		telemetry.TasksProcessed.WithLabelValues("worker", message.HandlerKey, "duplicate").Inc()
		w.logger.InfoContext(ctx, "skipping duplicate task message", "task_id", message.TaskID)
		return nil
	}

	handler, ok := w.registry.Get(task.HandlerKey)
	if !ok {
		err := fmt.Errorf("no handler registered for %q", task.HandlerKey)
		_ = w.store.FailTaskAttempt(ctx, task.ID, attempt.ID, err.Error())
		telemetry.TasksProcessed.WithLabelValues("worker", message.HandlerKey, "missing_handler").Inc()
		return err
	}

	output, err := handler.Handle(ctx, task)
	if err != nil {
		_ = w.store.FailTaskAttempt(ctx, task.ID, attempt.ID, err.Error())
		telemetry.TasksProcessed.WithLabelValues("worker", message.HandlerKey, "failed").Inc()
		return err
	}

	if err := w.store.CompleteTaskAttempt(ctx, task.ID, attempt.ID, output); err != nil {
		telemetry.TasksProcessed.WithLabelValues("worker", message.HandlerKey, "persist_error").Inc()
		return err
	}

	telemetry.TasksProcessed.WithLabelValues("worker", message.HandlerKey, "succeeded").Inc()
	w.logger.InfoContext(ctx, "task completed", "task_id", task.ID, "attempt_id", attempt.ID)
	return nil
}

