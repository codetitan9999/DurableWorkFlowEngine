package orchestrator

import (
	"testing"

	"durableflow/internal/domain"
)

func TestParseAndValidateWorkflowDefinitionAcceptsLinearNextTask(t *testing.T) {
	raw := []byte(`{
		"entry_task": "validate-order",
		"tasks": [
			{
				"name": "validate-order",
				"handler_key": "sample.echo",
				"next_task": "send-confirmation"
			},
			{
				"name": "send-confirmation",
				"handler_key": "sample.echo"
			}
		]
	}`)

	spec, err := ParseAndValidateWorkflowDefinition(raw)
	if err != nil {
		t.Fatalf("expected workflow definition to validate, got error: %v", err)
	}

	if spec.EntryTask != "validate-order" {
		t.Fatalf("expected entry task validate-order, got %q", spec.EntryTask)
	}
}

func TestParseAndValidateWorkflowDefinitionRejectsInvalidNextTask(t *testing.T) {
	raw := []byte(`{
		"entry_task": "validate-order",
		"tasks": [
			{
				"name": "validate-order",
				"handler_key": "sample.echo",
				"next_task": "missing-task"
			}
		]
	}`)

	if _, err := ParseAndValidateWorkflowDefinition(raw); err == nil {
		t.Fatal("expected invalid next_task to fail validation")
	}
}

func TestParseAndValidateWorkflowDefinitionRejectsNegativeRetrySettings(t *testing.T) {
	raw := []byte(`{
		"entry_task": "validate-order",
		"tasks": [
			{
				"name": "validate-order",
				"handler_key": "sample.echo",
				"max_attempts": -1,
				"backoff_seconds": -5
			}
		]
	}`)

	if _, err := ParseAndValidateWorkflowDefinition(raw); err == nil {
		t.Fatal("expected negative retry settings to fail validation")
	}
}

func TestParseAndValidateWorkflowDefinitionRejectsSelfReferencingNextTask(t *testing.T) {
	raw := []byte(`{
		"entry_task": "validate-order",
		"tasks": [
			{
				"name": "validate-order",
				"handler_key": "sample.echo",
				"next_task": "validate-order"
			}
		]
	}`)

	if _, err := ParseAndValidateWorkflowDefinition(raw); err == nil {
		t.Fatal("expected self-referencing next_task to fail validation")
	}
}

func TestParseAndValidateWorkflowDefinitionAcceptsRetrySettings(t *testing.T) {
	raw := []byte(`{
		"entry_task": "validate-order",
		"tasks": [
			{
				"name": "validate-order",
				"handler_key": "sample.echo",
				"max_attempts": 3,
				"backoff_seconds": 15
			}
		]
	}`)

	spec, err := ParseAndValidateWorkflowDefinition(raw)
	if err != nil {
		t.Fatalf("expected retry settings to validate, got error: %v", err)
	}

	if spec.Tasks[0].MaxAttempts != 3 {
		t.Fatalf("expected max_attempts 3, got %d", spec.Tasks[0].MaxAttempts)
	}
	if spec.Tasks[0].BackoffSeconds != 15 {
		t.Fatalf("expected backoff_seconds 15, got %d", spec.Tasks[0].BackoffSeconds)
	}
}

func TestFindNextTaskSpecReturnsNextTaskWhenPresent(t *testing.T) {
	workflowSpec := domain.WorkflowDefinitionSpec{
		EntryTask: "validate-order",
		Tasks: []domain.WorkflowTaskSpec{
			{
				Name:       "validate-order",
				HandlerKey: "sample.echo",
				NextTask:   "send-confirmation",
			},
			{
				Name:       "send-confirmation",
				HandlerKey: "sample.echo",
			},
		},
	}

	nextTask, hasNextTask, err := FindNextTaskSpec(workflowSpec, "validate-order")
	if err != nil {
		t.Fatalf("expected next task lookup to succeed, got error: %v", err)
	}
	if !hasNextTask {
		t.Fatal("expected next task to be present")
	}
	if nextTask.Name != "send-confirmation" {
		t.Fatalf("expected next task send-confirmation, got %q", nextTask.Name)
	}
}

func TestFindNextTaskSpecReturnsFalseWhenTaskEndsWorkflow(t *testing.T) {
	workflowSpec := domain.WorkflowDefinitionSpec{
		EntryTask: "validate-order",
		Tasks: []domain.WorkflowTaskSpec{
			{
				Name:       "validate-order",
				HandlerKey: "sample.echo",
			},
		},
	}

	_, hasNextTask, err := FindNextTaskSpec(workflowSpec, "validate-order")
	if err != nil {
		t.Fatalf("expected terminal task lookup to succeed, got error: %v", err)
	}
	if hasNextTask {
		t.Fatal("expected no next task for terminal task")
	}
}

func TestNormalizeDeadLetterTaskLimit(t *testing.T) {
	tests := []struct {
		name  string
		input int
		want  int
	}{
		{name: "defaults missing or invalid values", input: 0, want: defaultDeadLetterTaskLimit},
		{name: "defaults negative values", input: -1, want: defaultDeadLetterTaskLimit},
		{name: "keeps in-range values", input: 25, want: 25},
		{name: "caps overly large values", input: maxDeadLetterTaskLimit + 1, want: maxDeadLetterTaskLimit},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := normalizeDeadLetterTaskLimit(tt.input)
			if got != tt.want {
				t.Fatalf("expected %d, got %d", tt.want, got)
			}
		})
	}
}
