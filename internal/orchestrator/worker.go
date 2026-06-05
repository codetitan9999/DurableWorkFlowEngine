package orchestrator

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"durableflow/internal/domain"
	"durableflow/internal/handlers"
	"durableflow/internal/queue"
	"durableflow/internal/telemetry"
)

type workerStore interface {
	GetTaskInstance(ctx context.Context, id string) (domain.TaskInstance, error)
	GetWorkflowExecution(ctx context.Context, id string) (domain.WorkflowExecution, error)
	GetWorkflowDefinition(ctx context.Context, id string) (domain.WorkflowDefinition, error)
	StartTaskAttempt(ctx context.Context, taskID string) (domain.TaskInstance, domain.TaskAttempt, bool, error)
	ScheduleTaskRetry(ctx context.Context, taskID, attemptID, errorText string, nextRunAt time.Time) error
	FailTaskAttempt(ctx context.Context, taskID, attemptID, errorText string) error
	CompleteTaskAttempt(ctx context.Context, taskID, attemptID string, output json.RawMessage) error
	CompleteTaskAttemptAndEnqueueNextTask(ctx context.Context, taskID, attemptID, nextTaskName, nextHandlerKey string, output json.RawMessage) error
}

type Worker struct {
	store    workerStore
	registry *handlers.Registry
	logger   *slog.Logger
}

func NewWorker(store workerStore, registry *handlers.Registry, logger *slog.Logger) *Worker {
	return &Worker{
		store:    store,
		registry: registry,
		logger:   logger,
	}
}

func (w *Worker) GetWorkflowSpecAndTaskSpecByTaskID(ctx context.Context, taskID string) (domain.WorkflowDefinitionSpec, domain.WorkflowTaskSpec, error) {
	task, err := w.store.GetTaskInstance(ctx, taskID)
	if err != nil {
		return domain.WorkflowDefinitionSpec{}, domain.WorkflowTaskSpec{}, fmt.Errorf("error fetching task from store: %w", err)
	}

	execution, err := w.store.GetWorkflowExecution(ctx, task.WorkflowExecutionID)
	if err != nil {
		return domain.WorkflowDefinitionSpec{}, domain.WorkflowTaskSpec{}, fmt.Errorf("error fetching execution from store: %w", err)
	}

	workflowDef, err := w.store.GetWorkflowDefinition(ctx, execution.WorkflowDefinitionID)
	if err != nil {
		return domain.WorkflowDefinitionSpec{}, domain.WorkflowTaskSpec{}, fmt.Errorf("error fetching workflow definition from store: %w", err)
	}

	workflowSpec, err := ParseAndValidateWorkflowDefinition(workflowDef.DefinitionJSON)
	if err != nil {
		return domain.WorkflowDefinitionSpec{}, domain.WorkflowTaskSpec{}, fmt.Errorf("error parsing workflow definition JSON: %w", err)
	}

	taskSpec, err := FindTaskSpecByName(workflowSpec, task.TaskName)
	if err != nil {
		return domain.WorkflowDefinitionSpec{}, domain.WorkflowTaskSpec{}, fmt.Errorf("error finding task spec by name: %w", err)
	}

	return workflowSpec, taskSpec, nil
}

func (w *Worker) HandleDispatchedTask(ctx context.Context, message queue.TaskMessage) error {
	started := time.Now()
	outcome := "unknown"
	defer func() {
		telemetry.TaskProcessingDuration.WithLabelValues("worker", message.HandlerKey, outcome).Observe(time.Since(started).Seconds())
	}()

	workflowSpec, taskSpec, err := w.GetWorkflowSpecAndTaskSpecByTaskID(ctx, message.TaskID)
	if err != nil {
		outcome = "task_spec_error"
		telemetry.TasksProcessed.WithLabelValues("worker", message.HandlerKey, "task_spec_error").Inc()
		return fmt.Errorf("error getting task spec for task ID %s: %w", message.TaskID, err)
	}
	if taskSpec.MaxAttempts == 0 {
		taskSpec.MaxAttempts = 1 // Default to 1 attempt if not specified
	}

	task, attempt, alreadyCompleted, err := w.store.StartTaskAttempt(ctx, message.TaskID)
	if err != nil {
		outcome = "db_error"
		telemetry.TasksProcessed.WithLabelValues("worker", message.HandlerKey, "db_error").Inc()
		return err
	}

	if alreadyCompleted {
		outcome = "duplicate"
		telemetry.TasksProcessed.WithLabelValues("worker", message.HandlerKey, "duplicate").Inc()
		w.logger.InfoContext(ctx, "skipping duplicate task message", "task_id", message.TaskID)
		return nil
	}

	handler, ok := w.registry.Get(task.HandlerKey)
	if !ok {
		err := fmt.Errorf("no handler registered for %q", task.HandlerKey)
		if persistErr := w.store.FailTaskAttempt(ctx, task.ID, attempt.ID, err.Error()); persistErr != nil {
			outcome = "persist_error"
			telemetry.TasksProcessed.WithLabelValues("worker", message.HandlerKey, "persist_error").Inc()
			return fmt.Errorf("persist missing-handler failure: %w", persistErr)
		}
		outcome = "missing_handler"
		telemetry.TasksProcessed.WithLabelValues("worker", message.HandlerKey, "missing_handler").Inc()
		telemetry.DeadLetteredTasks.WithLabelValues("worker", message.HandlerKey, "missing_handler").Inc()
		w.logger.WarnContext(ctx, "task dead-lettered because no handler is registered", "task_id", task.ID, "attempt_id", attempt.ID, "handler_key", task.HandlerKey)
		return nil
	}

	output, err := handler.Handle(ctx, task)
	if err != nil {
		if taskSpec.MaxAttempts > attempt.AttemptNumber {
			nextRunAt := time.Now().UTC().Add(time.Duration(taskSpec.BackoffSeconds) * time.Second)
			if persistErr := w.store.ScheduleTaskRetry(ctx, task.ID, attempt.ID, err.Error(), nextRunAt); persistErr != nil {
				outcome = "persist_error"
				telemetry.TasksProcessed.WithLabelValues("worker", message.HandlerKey, "persist_error").Inc()
				return fmt.Errorf("schedule task retry: %w", persistErr)
			}
			outcome = "scheduled_retry"
			telemetry.TasksProcessed.WithLabelValues("worker", message.HandlerKey, "scheduled_retry").Inc()
			telemetry.RetriesScheduled.WithLabelValues("worker", message.HandlerKey).Inc()
			w.logger.InfoContext(ctx, "task attempt failed, scheduled for retry", "task_id", task.ID, "attempt_id", attempt.ID, "next_run_at", nextRunAt, "error", err.Error())
			return nil
		}

		if persistErr := w.store.FailTaskAttempt(ctx, task.ID, attempt.ID, err.Error()); persistErr != nil {
			outcome = "persist_error"
			telemetry.TasksProcessed.WithLabelValues("worker", message.HandlerKey, "persist_error").Inc()
			return fmt.Errorf("persist terminal task failure: %w", persistErr)
		}
		outcome = "failed"
		telemetry.TasksProcessed.WithLabelValues("worker", message.HandlerKey, "failed").Inc()
		telemetry.DeadLetteredTasks.WithLabelValues("worker", message.HandlerKey, "exhausted_attempts").Inc()
		w.logger.WarnContext(ctx, "task dead-lettered after exhausting attempts", "task_id", task.ID, "attempt_id", attempt.ID, "error", err.Error())
		return nil
	}

	if nextTaskSpec, hasNextTask, err := FindNextTaskSpec(workflowSpec, task.TaskName); err != nil {
		outcome = "task_spec_error"
		telemetry.TasksProcessed.WithLabelValues("worker", message.HandlerKey, "task_spec_error").Inc()
		return fmt.Errorf("error resolving next task for %s: %w", task.TaskName, err)
	} else if hasNextTask {
		if err := w.store.CompleteTaskAttemptAndEnqueueNextTask(ctx, task.ID, attempt.ID, nextTaskSpec.Name, nextTaskSpec.HandlerKey, output); err != nil {
			outcome = "persist_error"
			telemetry.TasksProcessed.WithLabelValues("worker", message.HandlerKey, "persist_error").Inc()
			return fmt.Errorf("error completing task attempt and enqueuing next task: %w", err)
		}
		outcome = "advanced"
		telemetry.TasksProcessed.WithLabelValues("worker", message.HandlerKey, "advanced").Inc()
		w.logger.InfoContext(ctx, "task completed and next task enqueued", "task_id", task.ID, "attempt_id", attempt.ID, "next_task", nextTaskSpec.Name)
		return nil
	}

	if err := w.store.CompleteTaskAttempt(ctx, task.ID, attempt.ID, output); err != nil {
		outcome = "persist_error"
		telemetry.TasksProcessed.WithLabelValues("worker", message.HandlerKey, "persist_error").Inc()
		return err
	}

	outcome = "succeeded"
	telemetry.TasksProcessed.WithLabelValues("worker", message.HandlerKey, "succeeded").Inc()
	w.logger.InfoContext(ctx, "task completed", "task_id", task.ID, "attempt_id", attempt.ID)
	return nil
}
