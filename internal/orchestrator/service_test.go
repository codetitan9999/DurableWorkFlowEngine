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
