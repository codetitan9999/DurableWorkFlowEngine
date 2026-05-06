package domain

import (
	"encoding/json"
	"time"
)

const (
	WorkflowDefinitionStatusActive = "active"

	ExecutionStatusRunning   = "running"
	ExecutionStatusSucceeded = "succeeded"
	ExecutionStatusFailed    = "failed"

	TaskStatusPending   = "pending"
	TaskStatusRunning   = "running"
	TaskStatusSucceeded = "succeeded"
	TaskStatusFailed    = "failed"

	TaskAttemptStatusRunning   = "running"
	TaskAttemptStatusSucceeded = "succeeded"
	TaskAttemptStatusFailed    = "failed"
)

type WorkflowDefinition struct {
	ID             string          `json:"id"`
	Name           string          `json:"name"`
	Description    string          `json:"description"`
	Version        int             `json:"version"`
	Status         string          `json:"status"`
	DefinitionJSON json.RawMessage `json:"definition"`
	CreatedAt      time.Time       `json:"created_at"`
	UpdatedAt      time.Time       `json:"updated_at"`
}

type WorkflowExecution struct {
	ID                   string          `json:"id"`
	WorkflowDefinitionID string          `json:"workflow_definition_id"`
	Status               string          `json:"status"`
	InputJSON            json.RawMessage `json:"input"`
	OutputJSON           json.RawMessage `json:"output,omitempty"`
	ErrorText            string          `json:"error,omitempty"`
	CreatedAt            time.Time       `json:"created_at"`
	UpdatedAt            time.Time       `json:"updated_at"`
	StartedAt            *time.Time      `json:"started_at,omitempty"`
	CompletedAt          *time.Time      `json:"completed_at,omitempty"`
}

type TaskInstance struct {
	ID                  string          `json:"id"`
	WorkflowExecutionID string          `json:"workflow_execution_id"`
	TaskName            string          `json:"task_name"`
	HandlerKey          string          `json:"handler_key"`
	Status              string          `json:"status"`
	InputJSON           json.RawMessage `json:"input"`
	OutputJSON          json.RawMessage `json:"output,omitempty"`
	NextRunAt           *time.Time      `json:"next_run_at,omitempty"`
	LastErrorText       string          `json:"last_error,omitempty"`
	AttemptsTotal       int             `json:"attempts_total"`
	IdempotencyKey      string          `json:"idempotency_key"`
	DispatchedAt        *time.Time      `json:"dispatched_at,omitempty"`
	CompletedAt         *time.Time      `json:"completed_at,omitempty"`
	CreatedAt           time.Time       `json:"created_at"`
	UpdatedAt           time.Time       `json:"updated_at"`
}

type TaskAttempt struct {
	ID            string          `json:"id"`
	TaskInstanceID string         `json:"task_instance_id"`
	AttemptNumber int             `json:"attempt_number"`
	Status        string          `json:"status"`
	StartedAt     time.Time       `json:"started_at"`
	FinishedAt    *time.Time      `json:"finished_at,omitempty"`
	ErrorText     string          `json:"error,omitempty"`
	OutputJSON    json.RawMessage `json:"output,omitempty"`
	CreatedAt     time.Time       `json:"created_at"`
}

type OutboxEvent struct {
	ID            string          `json:"id"`
	AggregateType string          `json:"aggregate_type"`
	AggregateID   string          `json:"aggregate_id"`
	EventType     string          `json:"event_type"`
	PayloadJSON   json.RawMessage `json:"payload"`
	AvailableAt   time.Time       `json:"available_at"`
	DispatchedAt  *time.Time      `json:"dispatched_at,omitempty"`
	AttemptCount  int             `json:"attempt_count"`
	LastErrorText string          `json:"last_error,omitempty"`
	CreatedAt     time.Time       `json:"created_at"`
}

type DispatchTaskPayload struct {
	TaskID      string `json:"task_id"`
	ExecutionID string `json:"execution_id"`
	HandlerKey  string `json:"handler_key"`
}

type ExecutionStartResult struct {
	Execution WorkflowExecution `json:"execution"`
	Task      TaskInstance      `json:"task"`
}

type ExecutionSnapshot struct {
	Execution WorkflowExecution `json:"execution"`
	Tasks     []TaskInstance    `json:"tasks"`
}

