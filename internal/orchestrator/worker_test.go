package orchestrator

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"durableflow/internal/domain"
	"durableflow/internal/handlers"
	"durableflow/internal/queue"
)

type stubWorkerStore struct {
	task                domain.TaskInstance
	execution           domain.WorkflowExecution
	workflowDefinition  domain.WorkflowDefinition
	startTask           domain.TaskInstance
	startAttempt        domain.TaskAttempt
	alreadyCompleted    bool
	startTaskAttemptErr error
	scheduleRetryErr    error
	failTaskErr         error
	completeTaskErr     error
	advanceTaskErr      error
	scheduledRetryCall  *scheduledRetryCall
	failedTaskCall      *failedTaskCall
	completeTaskCall    *completeTaskCall
	advanceTaskCall     *advanceTaskCall
}

type scheduledRetryCall struct {
	taskID    string
	attemptID string
	errorText string
	nextRunAt time.Time
}

type failedTaskCall struct {
	taskID    string
	attemptID string
	errorText string
}

type completeTaskCall struct {
	taskID    string
	attemptID string
	output    json.RawMessage
}

type advanceTaskCall struct {
	taskID         string
	attemptID      string
	nextTaskName   string
	nextHandlerKey string
	output         json.RawMessage
}

func (s *stubWorkerStore) GetTaskInstance(context.Context, string) (domain.TaskInstance, error) {
	return s.task, nil
}

func (s *stubWorkerStore) GetWorkflowExecution(context.Context, string) (domain.WorkflowExecution, error) {
	return s.execution, nil
}

func (s *stubWorkerStore) GetWorkflowDefinition(context.Context, string) (domain.WorkflowDefinition, error) {
	return s.workflowDefinition, nil
}

func (s *stubWorkerStore) StartTaskAttempt(context.Context, string) (domain.TaskInstance, domain.TaskAttempt, bool, error) {
	return s.startTask, s.startAttempt, s.alreadyCompleted, s.startTaskAttemptErr
}

func (s *stubWorkerStore) ScheduleTaskRetry(_ context.Context, taskID, attemptID, errorText string, nextRunAt time.Time) error {
	s.scheduledRetryCall = &scheduledRetryCall{
		taskID:    taskID,
		attemptID: attemptID,
		errorText: errorText,
		nextRunAt: nextRunAt,
	}
	return s.scheduleRetryErr
}

func (s *stubWorkerStore) FailTaskAttempt(_ context.Context, taskID, attemptID, errorText string) error {
	s.failedTaskCall = &failedTaskCall{
		taskID:    taskID,
		attemptID: attemptID,
		errorText: errorText,
	}
	return s.failTaskErr
}

func (s *stubWorkerStore) CompleteTaskAttempt(_ context.Context, taskID, attemptID string, output json.RawMessage) error {
	s.completeTaskCall = &completeTaskCall{
		taskID:    taskID,
		attemptID: attemptID,
		output:    output,
	}
	return s.completeTaskErr
}

func (s *stubWorkerStore) CompleteTaskAttemptAndEnqueueNextTask(_ context.Context, taskID, attemptID, nextTaskName, nextHandlerKey string, output json.RawMessage) error {
	s.advanceTaskCall = &advanceTaskCall{
		taskID:         taskID,
		attemptID:      attemptID,
		nextTaskName:   nextTaskName,
		nextHandlerKey: nextHandlerKey,
		output:         output,
	}
	return s.advanceTaskErr
}

type stubHandler struct {
	key    string
	output json.RawMessage
	err    error
}

func (h stubHandler) Key() string {
	return h.key
}

func (h stubHandler) Handle(context.Context, domain.TaskInstance) (json.RawMessage, error) {
	return h.output, h.err
}

func newTestWorker(t *testing.T, store workerStore, handler handlers.Handler) *Worker {
	t.Helper()

	registry := handlers.NewRegistry(handler)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	return NewWorker(store, registry, logger)
}

func TestHandleDispatchedTaskSchedulesRetryWhenAttemptsRemain(t *testing.T) {
	store := &stubWorkerStore{
		task: domain.TaskInstance{
			ID:                  "task-1",
			WorkflowExecutionID: "exec-1",
			TaskName:            "validate-order",
		},
		execution: domain.WorkflowExecution{
			ID:                   "exec-1",
			WorkflowDefinitionID: "wf-1",
		},
		workflowDefinition: domain.WorkflowDefinition{
			ID: "wf-1",
			DefinitionJSON: []byte(`{
				"entry_task": "validate-order",
				"tasks": [
					{
						"name": "validate-order",
						"handler_key": "sample.echo",
						"max_attempts": 3,
						"backoff_seconds": 15
					}
				]
			}`),
		},
		startTask: domain.TaskInstance{
			ID:         "task-1",
			TaskName:   "validate-order",
			HandlerKey: "sample.echo",
		},
		startAttempt: domain.TaskAttempt{
			ID:            "attempt-1",
			AttemptNumber: 1,
		},
	}

	worker := newTestWorker(t, store, stubHandler{
		key: "sample.echo",
		err: errors.New("temporary downstream failure"),
	})

	err := worker.HandleDispatchedTask(context.Background(), queue.TaskMessage{
		TaskID:     "task-1",
		HandlerKey: "sample.echo",
	})
	if err != nil {
		t.Fatalf("expected retryable failure to be persisted without returning error, got %v", err)
	}

	if store.scheduledRetryCall == nil {
		t.Fatal("expected retry scheduling call")
	}
	if store.scheduledRetryCall.taskID != "task-1" {
		t.Fatalf("expected taskID task-1, got %q", store.scheduledRetryCall.taskID)
	}
	if store.failedTaskCall != nil {
		t.Fatal("did not expect terminal failure path for retryable error")
	}
	if store.completeTaskCall != nil || store.advanceTaskCall != nil {
		t.Fatal("did not expect success completion paths during retry scheduling")
	}
}

func TestHandleDispatchedTaskAdvancesWorkflowWhenNextTaskExists(t *testing.T) {
	store := &stubWorkerStore{
		task: domain.TaskInstance{
			ID:                  "task-1",
			WorkflowExecutionID: "exec-1",
			TaskName:            "validate-order",
		},
		execution: domain.WorkflowExecution{
			ID:                   "exec-1",
			WorkflowDefinitionID: "wf-1",
		},
		workflowDefinition: domain.WorkflowDefinition{
			ID: "wf-1",
			DefinitionJSON: []byte(`{
				"entry_task": "validate-order",
				"tasks": [
					{
						"name": "validate-order",
						"handler_key": "sample.echo",
						"next_task": "send-confirmation"
					},
					{
						"name": "send-confirmation",
						"handler_key": "notifications.send"
					}
				]
			}`),
		},
		startTask: domain.TaskInstance{
			ID:         "task-1",
			TaskName:   "validate-order",
			HandlerKey: "sample.echo",
		},
		startAttempt: domain.TaskAttempt{
			ID:            "attempt-1",
			AttemptNumber: 1,
		},
	}

	output := json.RawMessage(`{"approved":true}`)
	worker := newTestWorker(t, store, stubHandler{
		key:    "sample.echo",
		output: output,
	})

	err := worker.HandleDispatchedTask(context.Background(), queue.TaskMessage{
		TaskID:     "task-1",
		HandlerKey: "sample.echo",
	})
	if err != nil {
		t.Fatalf("expected successful advancement, got %v", err)
	}

	if store.advanceTaskCall == nil {
		t.Fatal("expected next task enqueue path")
	}
	if store.advanceTaskCall.nextTaskName != "send-confirmation" {
		t.Fatalf("expected next task send-confirmation, got %q", store.advanceTaskCall.nextTaskName)
	}
	if store.advanceTaskCall.nextHandlerKey != "notifications.send" {
		t.Fatalf("expected next handler notifications.send, got %q", store.advanceTaskCall.nextHandlerKey)
	}
	if string(store.advanceTaskCall.output) != string(output) {
		t.Fatalf("expected output %s, got %s", output, store.advanceTaskCall.output)
	}
	if store.completeTaskCall != nil {
		t.Fatal("did not expect terminal completion path when next task exists")
	}
}

func TestHandleDispatchedTaskCompletesExecutionWhenTaskIsTerminal(t *testing.T) {
	store := &stubWorkerStore{
		task: domain.TaskInstance{
			ID:                  "task-2",
			WorkflowExecutionID: "exec-1",
			TaskName:            "send-confirmation",
		},
		execution: domain.WorkflowExecution{
			ID:                   "exec-1",
			WorkflowDefinitionID: "wf-1",
		},
		workflowDefinition: domain.WorkflowDefinition{
			ID: "wf-1",
			DefinitionJSON: []byte(`{
				"entry_task": "send-confirmation",
				"tasks": [
					{
						"name": "send-confirmation",
						"handler_key": "notifications.send"
					}
				]
			}`),
		},
		startTask: domain.TaskInstance{
			ID:         "task-2",
			TaskName:   "send-confirmation",
			HandlerKey: "notifications.send",
		},
		startAttempt: domain.TaskAttempt{
			ID:            "attempt-2",
			AttemptNumber: 1,
		},
	}

	output := json.RawMessage(`{"sent":true}`)
	worker := newTestWorker(t, store, stubHandler{
		key:    "notifications.send",
		output: output,
	})

	err := worker.HandleDispatchedTask(context.Background(), queue.TaskMessage{
		TaskID:     "task-2",
		HandlerKey: "notifications.send",
	})
	if err != nil {
		t.Fatalf("expected terminal task success, got %v", err)
	}

	if store.completeTaskCall == nil {
		t.Fatal("expected terminal completion path")
	}
	if store.completeTaskCall.taskID != "task-2" {
		t.Fatalf("expected taskID task-2, got %q", store.completeTaskCall.taskID)
	}
	if string(store.completeTaskCall.output) != string(output) {
		t.Fatalf("expected output %s, got %s", output, store.completeTaskCall.output)
	}
	if store.advanceTaskCall != nil {
		t.Fatal("did not expect next-task advancement for terminal task")
	}
}

func TestHandleDispatchedTaskDeadLettersWhenAttemptsAreExhausted(t *testing.T) {
	store := &stubWorkerStore{
		task: domain.TaskInstance{
			ID:                  "task-3",
			WorkflowExecutionID: "exec-2",
			TaskName:            "charge-card",
		},
		execution: domain.WorkflowExecution{
			ID:                   "exec-2",
			WorkflowDefinitionID: "wf-2",
		},
		workflowDefinition: domain.WorkflowDefinition{
			ID: "wf-2",
			DefinitionJSON: []byte(`{
				"entry_task": "charge-card",
				"tasks": [
					{
						"name": "charge-card",
						"handler_key": "payments.charge",
						"max_attempts": 2,
						"backoff_seconds": 5
					}
				]
			}`),
		},
		startTask: domain.TaskInstance{
			ID:         "task-3",
			TaskName:   "charge-card",
			HandlerKey: "payments.charge",
		},
		startAttempt: domain.TaskAttempt{
			ID:            "attempt-2",
			AttemptNumber: 2,
		},
	}

	worker := newTestWorker(t, store, stubHandler{
		key: "payments.charge",
		err: errors.New("card processor rejected request"),
	})

	err := worker.HandleDispatchedTask(context.Background(), queue.TaskMessage{
		TaskID:     "task-3",
		HandlerKey: "payments.charge",
	})
	if err == nil {
		t.Fatal("expected terminal failure to return the handler error")
	}

	if store.failedTaskCall == nil {
		t.Fatal("expected dead-letter failure path")
	}
	if store.failedTaskCall.taskID != "task-3" {
		t.Fatalf("expected taskID task-3, got %q", store.failedTaskCall.taskID)
	}
	if store.scheduledRetryCall != nil {
		t.Fatal("did not expect retry scheduling when attempts are exhausted")
	}
	if store.completeTaskCall != nil || store.advanceTaskCall != nil {
		t.Fatal("did not expect success completion paths during terminal failure")
	}
}
