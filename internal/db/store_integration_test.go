package db_test

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
	"time"

	"durableflow/internal/db"
	"durableflow/internal/domain"
	"durableflow/internal/testutil"
)

func TestCreateExecutionAndTaskRollbackOnNextTaskConflict(t *testing.T) {
	store, pool := testutil.OpenIntegrationStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	definition := mustCreateWorkflowDefinition(t, ctx, store, fmt.Sprintf("rollback-%d", time.Now().UnixNano()))
	start := mustCreateExecution(t, ctx, store, definition.ID)

	task, attempt, alreadyCompleted, err := store.StartTaskAttempt(ctx, start.Task.ID)
	if err != nil {
		t.Fatalf("start task attempt: %v", err)
	}
	if alreadyCompleted {
		t.Fatal("expected task to be runnable")
	}

	if _, err := pool.Exec(ctx, `
		INSERT INTO task_instances (
			workflow_execution_id,
			task_name,
			handler_key,
			status,
			input_json,
			idempotency_key,
			created_at,
			updated_at
		)
		VALUES ($1, $2, $3, $4, $5, $6, NOW(), NOW())
	`,
		start.Execution.ID,
		"send-confirmation",
		"notifications.send",
		domain.TaskStatusPending,
		json.RawMessage(`{"seed":true}`),
		fmt.Sprintf("%s:%s", start.Execution.ID, "send-confirmation"),
	); err != nil {
		t.Fatalf("seed conflicting next task: %v", err)
	}

	err = store.CompleteTaskAttemptAndEnqueueNextTask(
		ctx,
		task.ID,
		attempt.ID,
		"send-confirmation",
		"notifications.send",
		json.RawMessage(`{"approved":true}`),
	)
	if err == nil {
		t.Fatal("expected next-task insert conflict to fail")
	}
	if !db.IsUniqueViolation(err) {
		t.Fatalf("expected unique violation, got %v", err)
	}

	snapshot, err := store.GetExecutionSnapshot(ctx, start.Execution.ID)
	if err != nil {
		t.Fatalf("get execution snapshot: %v", err)
	}
	if len(snapshot.Tasks) != 1 {
		t.Fatalf("expected only the original task after rollback, got %d", len(snapshot.Tasks))
	}
	if snapshot.Tasks[0].Task.Status != domain.TaskStatusRunning {
		t.Fatalf("expected original task to remain running after rollback, got %q", snapshot.Tasks[0].Task.Status)
	}
	if len(snapshot.Tasks[0].Attempts) != 1 {
		t.Fatalf("expected one attempt after rollback, got %d", len(snapshot.Tasks[0].Attempts))
	}
	if snapshot.Tasks[0].Attempts[0].Status != domain.TaskAttemptStatusRunning {
		t.Fatalf("expected attempt to remain running after rollback, got %q", snapshot.Tasks[0].Attempts[0].Status)
	}
}

func TestRetrySchedulingMaterializesOutboxEvent(t *testing.T) {
	store, _ := testutil.OpenIntegrationStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	definition := mustCreateWorkflowDefinition(t, ctx, store, fmt.Sprintf("retry-%d", time.Now().UnixNano()))
	start := mustCreateExecution(t, ctx, store, definition.ID)
	clearPendingOutboxForTask(t, ctx, store, start.Task.ID)

	task, attempt, alreadyCompleted, err := store.StartTaskAttempt(ctx, start.Task.ID)
	if err != nil {
		t.Fatalf("start task attempt: %v", err)
	}
	if alreadyCompleted {
		t.Fatal("expected task to be runnable")
	}

	nextRunAt := time.Now().UTC().Add(-1 * time.Second)
	if err := store.ScheduleTaskRetry(ctx, task.ID, attempt.ID, "temporary failure", nextRunAt); err != nil {
		t.Fatalf("schedule task retry: %v", err)
	}

	reloadedTask, err := store.GetTaskInstance(ctx, task.ID)
	if err != nil {
		t.Fatalf("reload task: %v", err)
	}
	if reloadedTask.Status != domain.TaskStatusPending {
		t.Fatalf("expected task to return to pending, got %q", reloadedTask.Status)
	}
	if reloadedTask.NextRunAt == nil {
		t.Fatal("expected next_run_at to be persisted")
	}

	enqueued, err := store.EnqueueDueTaskRetries(ctx, 20)
	if err != nil {
		t.Fatalf("enqueue due retries: %v", err)
	}
	if enqueued != 1 {
		t.Fatalf("expected one due retry to be enqueued, got %d", enqueued)
	}

	reloadedTask, err = store.GetTaskInstance(ctx, task.ID)
	if err != nil {
		t.Fatalf("reload task after enqueue: %v", err)
	}
	if reloadedTask.NextRunAt != nil {
		t.Fatal("expected next_run_at to be cleared after enqueue")
	}

	pending, err := store.ListPendingOutbox(ctx, 20)
	if err != nil {
		t.Fatalf("list pending outbox: %v", err)
	}
	matching := countPendingOutboxForTask(t, pending, task.ID)
	if matching != 1 {
		t.Fatalf("expected one retry dispatch event for task %s, got %d", task.ID, matching)
	}
}

func TestDeadLetterReplayResetsTaskAndExecution(t *testing.T) {
	store, _ := testutil.OpenIntegrationStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	definition := mustCreateWorkflowDefinition(t, ctx, store, fmt.Sprintf("replay-%d", time.Now().UnixNano()))
	start := mustCreateExecution(t, ctx, store, definition.ID)
	clearPendingOutboxForTask(t, ctx, store, start.Task.ID)

	task, attempt, alreadyCompleted, err := store.StartTaskAttempt(ctx, start.Task.ID)
	if err != nil {
		t.Fatalf("start task attempt: %v", err)
	}
	if alreadyCompleted {
		t.Fatal("expected task to be runnable")
	}

	if err := store.FailTaskAttempt(ctx, task.ID, attempt.ID, "missing handler"); err != nil {
		t.Fatalf("fail task attempt: %v", err)
	}

	deadTask, err := store.GetTaskInstance(ctx, task.ID)
	if err != nil {
		t.Fatalf("reload dead-lettered task: %v", err)
	}
	if deadTask.Status != domain.TaskStatusDeadLettered {
		t.Fatalf("expected dead-lettered task, got %q", deadTask.Status)
	}

	replayedTask, err := store.ReplayDeadLetteredTask(ctx, task.ID)
	if err != nil {
		t.Fatalf("replay dead-lettered task: %v", err)
	}
	if replayedTask.Status != domain.TaskStatusPending {
		t.Fatalf("expected replayed task to return to pending, got %q", replayedTask.Status)
	}

	snapshot, err := store.GetExecutionSnapshot(ctx, start.Execution.ID)
	if err != nil {
		t.Fatalf("reload snapshot after replay: %v", err)
	}
	if snapshot.Execution.Status != domain.ExecutionStatusRunning {
		t.Fatalf("expected execution to return to running, got %q", snapshot.Execution.Status)
	}

	if _, err := store.ReplayDeadLetteredTask(ctx, task.ID); err == nil {
		t.Fatal("expected replay of non-dead-lettered task to fail")
	} else if !db.IsTaskNotReplayable(err) {
		t.Fatalf("expected task-not-replayable error, got %v", err)
	}
}

func TestIdempotencyConflictAndCachedResponse(t *testing.T) {
	store, _ := testutil.OpenIntegrationStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	definition := mustCreateWorkflowDefinition(t, ctx, store, fmt.Sprintf("idem-%d", time.Now().UnixNano()))
	first := mustCreateExecution(t, ctx, store, definition.ID)
	second := mustCreateExecution(t, ctx, store, definition.ID)

	if _, replayed, err := store.BeginIdempotentTask(ctx, "sample.echo", "shared-key", first.Task.ID); err != nil {
		t.Fatalf("begin first idempotent task: %v", err)
	} else if replayed {
		t.Fatal("did not expect first reservation to replay cached output")
	}

	if _, _, err := store.BeginIdempotentTask(ctx, "sample.echo", "shared-key", second.Task.ID); err == nil {
		t.Fatal("expected conflicting reservation to fail")
	} else if !db.IsIdempotencyKeyInProgress(err) {
		t.Fatalf("expected idempotency in-progress error, got %v", err)
	}

	response := json.RawMessage(`{"ok":true}`)
	if err := store.CompleteIdempotentTask(ctx, "sample.echo", "shared-key", first.Task.ID, response); err != nil {
		t.Fatalf("complete idempotent task: %v", err)
	}

	cached, replayed, err := store.BeginIdempotentTask(ctx, "sample.echo", "shared-key", second.Task.ID)
	if err != nil {
		t.Fatalf("begin second idempotent task after completion: %v", err)
	}
	if !replayed {
		t.Fatal("expected second caller to reuse cached response")
	}
	if !jsonEqual(cached, response) {
		t.Fatalf("expected cached response %s, got %s", response, cached)
	}
}

func mustCreateWorkflowDefinition(t *testing.T, ctx context.Context, store *db.Store, name string) domain.WorkflowDefinition {
	t.Helper()

	item, err := store.CreateWorkflowDefinition(ctx, name, "integration test workflow", json.RawMessage(`{
		"entry_task": "validate-order",
		"tasks": [
			{
				"name": "validate-order",
				"handler_key": "sample.echo",
				"max_attempts": 3,
				"backoff_seconds": 1,
				"next_task": "send-confirmation"
			},
			{
				"name": "send-confirmation",
				"handler_key": "notifications.send"
			}
		]
	}`))
	if err != nil {
		t.Fatalf("create workflow definition: %v", err)
	}
	return item
}

func mustCreateExecution(t *testing.T, ctx context.Context, store *db.Store, workflowDefinitionID string) domain.ExecutionStartResult {
	t.Helper()

	result, err := store.CreateExecutionAndTask(ctx, workflowDefinitionID, json.RawMessage(`{"customer_id":"demo-customer-123"}`), "validate-order", "sample.echo")
	if err != nil {
		t.Fatalf("create execution: %v", err)
	}
	return result
}

func clearPendingOutboxForTask(t *testing.T, ctx context.Context, store *db.Store, taskID string) {
	t.Helper()

	pending, err := store.ListPendingOutbox(ctx, 100)
	if err != nil {
		t.Fatalf("list pending outbox: %v", err)
	}
	for _, event := range pending {
		var payload domain.DispatchTaskPayload
		if err := json.Unmarshal(event.PayloadJSON, &payload); err != nil {
			continue
		}
		if payload.TaskID != taskID {
			continue
		}
		if err := store.MarkOutboxDispatched(ctx, event.ID); err != nil {
			t.Fatalf("mark outbox dispatched: %v", err)
		}
	}
}

func countPendingOutboxForTask(t *testing.T, pending []domain.OutboxEvent, taskID string) int {
	t.Helper()

	count := 0
	for _, event := range pending {
		var payload domain.DispatchTaskPayload
		if err := json.Unmarshal(event.PayloadJSON, &payload); err != nil {
			continue
		}
		if payload.TaskID == taskID {
			count++
		}
	}
	return count
}

func jsonEqual(left, right json.RawMessage) bool {
	var leftValue any
	var rightValue any
	if err := json.Unmarshal(left, &leftValue); err != nil {
		return false
	}
	if err := json.Unmarshal(right, &rightValue); err != nil {
		return false
	}
	return reflect.DeepEqual(leftValue, rightValue)
}
