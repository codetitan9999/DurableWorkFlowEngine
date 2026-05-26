package orchestrator

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"durableflow/internal/db"
	"durableflow/internal/domain"
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

func (w *Worker) GetTaskSpecByTaskID(ctx context.Context, task_id string) (domain.WorkflowTaskSpec, error) {
	task, err := w.store.GetTaskInstance(ctx, task_id)
	if err != nil {
		return domain.WorkflowTaskSpec{}, fmt.Errorf("error fetching task from store: %w", err)
	}

	execution, err := w.store.GetWorkflowExecution(ctx, task.WorkflowExecutionID)
	if err != nil {
		return domain.WorkflowTaskSpec{}, fmt.Errorf("error fetching execution from store: %w", err)
	}

	workflowDef, err := w.store.GetWorkflowDefinition(ctx, execution.WorkflowDefinitionID)
	if err != nil {
		return domain.WorkflowTaskSpec{}, fmt.Errorf("error fetching workflow definition from store: %w", err)
	}

	workflowSpec, err := ParseAndValidateWorkflowDefinition(workflowDef.DefinitionJSON)
	if err != nil {
		return domain.WorkflowTaskSpec{}, fmt.Errorf("error parsing workflow definition JSON: %w", err)
	}

	taskSpec, err := FindTaskSpecByName(workflowSpec, task.TaskName)
	if err != nil {
		return domain.WorkflowTaskSpec{}, fmt.Errorf("error finding task spec by name: %w", err)
	}

	return taskSpec, nil
}
func (w *Worker) HandleDispatchedTask(ctx context.Context, message queue.TaskMessage) error {
	taskSpec, err := w.GetTaskSpecByTaskID(ctx, message.TaskID)
	if err != nil {
		telemetry.TasksProcessed.WithLabelValues("worker", message.HandlerKey, "task_spec_error").Inc()
		return fmt.Errorf("error getting task spec for task ID %s: %w", message.TaskID, err)
	}
	if taskSpec.MaxAttempts == 0 {
		taskSpec.MaxAttempts = 1 // Default to 1 attempt if not specified
	}

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
		if persistErr := w.store.FailTaskAttempt(ctx, task.ID, attempt.ID, err.Error()); persistErr != nil {
			telemetry.TasksProcessed.WithLabelValues("worker", message.HandlerKey, "persist_error").Inc()
			return fmt.Errorf("persist missing-handler failure: %w", persistErr)
		}
		telemetry.TasksProcessed.WithLabelValues("worker", message.HandlerKey, "missing_handler").Inc()
		return err
	}

	output, err := handler.Handle(ctx, task)
	if err != nil {
		if taskSpec.MaxAttempts > attempt.AttemptNumber {
			nextRunAt := time.Now().UTC().Add(time.Duration(taskSpec.BackoffSeconds) * time.Second)
			if persistErr := w.store.ScheduleTaskRetry(ctx, task.ID, attempt.ID, err.Error(), nextRunAt); persistErr != nil {
				telemetry.TasksProcessed.WithLabelValues("worker", message.HandlerKey, "persist_error").Inc()
				return fmt.Errorf("schedule task retry: %w", persistErr)
			}
			telemetry.TasksProcessed.WithLabelValues("worker", message.HandlerKey, "scheduled_retry").Inc()
			w.logger.InfoContext(ctx, "task attempt failed, scheduled for retry", "task_id", task.ID, "attempt_id", attempt.ID, "next_run_at", nextRunAt, "error", err.Error())
			return nil
		}

		if persistErr := w.store.FailTaskAttempt(ctx, task.ID, attempt.ID, err.Error()); persistErr != nil {
			telemetry.TasksProcessed.WithLabelValues("worker", message.HandlerKey, "persist_error").Inc()
			return fmt.Errorf("persist terminal task failure: %w", persistErr)
		}
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
