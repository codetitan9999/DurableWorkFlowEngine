package orchestrator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"durableflow/internal/db"
	"durableflow/internal/domain"
)

type Service struct {
	store  *db.Store
	logger *slog.Logger
}

type CreateWorkflowDefinitionRequest struct {
	Name        string
	Description string
	Definition  json.RawMessage
}

type TriggerExecutionRequest struct {
	WorkflowDefinitionID string
	Input                json.RawMessage
}

func NewService(store *db.Store, logger *slog.Logger) *Service {
	return &Service{
		store:  store,
		logger: logger,
	}
}

func (s *Service) CreateWorkflowDefinition(ctx context.Context, req CreateWorkflowDefinitionRequest) (domain.WorkflowDefinition, error) {
	name := strings.TrimSpace(req.Name)
	if name == "" {
		return domain.WorkflowDefinition{}, errors.New("workflow name is required")
	}

	definition := req.Definition
	if len(definition) == 0 {
		definition = []byte(`{
  "entry_task": "sample-task",
  "tasks": [
    {
      "name": "sample-task",
      "handler_key": "sample.echo"
    }
  ]
	}`)
	}

	if _, err := ParseAndValidateWorkflowDefinition(definition); err != nil {
		return domain.WorkflowDefinition{}, err
	}

	s.logger.InfoContext(ctx, "creating workflow definition", "name", name)
	item, err := s.store.CreateWorkflowDefinition(ctx, name, req.Description, definition)
	if err != nil {
		if db.IsUniqueViolation(err) {
			return domain.WorkflowDefinition{}, errors.New("workflow name already exists")
		}
		return domain.WorkflowDefinition{}, fmt.Errorf("create workflow definition: %w", err)
	}

	return item, nil
}

func (s *Service) TriggerExecution(ctx context.Context, req TriggerExecutionRequest) (domain.ExecutionStartResult, error) {
	if strings.TrimSpace(req.WorkflowDefinitionID) == "" {
		return domain.ExecutionStartResult{}, errors.New("workflow_definition_id is required")
	}

	workflowDefinition, err := s.store.GetWorkflowDefinition(ctx, req.WorkflowDefinitionID)
	if err != nil {
		if db.IsNotFound(err) {
			return domain.ExecutionStartResult{}, fmt.Errorf("workflow definition not found: %w", err)
		}
		return domain.ExecutionStartResult{}, fmt.Errorf("get workflow definition: %w", err)
	}

	workflowSpec, err := ParseAndValidateWorkflowDefinition(workflowDefinition.DefinitionJSON)
	if err != nil {
		return domain.ExecutionStartResult{}, err
	}

	entryTask, err := FindEntryTask(workflowSpec)
	if err != nil {
		return domain.ExecutionStartResult{}, err
	}

	// TODO: This currently creates only the entry task. Later phases should expand the workflow graph from the definition.
	result, err := s.store.CreateExecutionAndTask(ctx, req.WorkflowDefinitionID, req.Input, entryTask.Name, entryTask.HandlerKey)
	if err != nil {
		return domain.ExecutionStartResult{}, fmt.Errorf("create execution: %w", err)
	}

	s.logger.InfoContext(ctx, "execution triggered", "execution_id", result.Execution.ID, "task_id", result.Task.ID)
	return result, nil
}

func (s *Service) GetExecutionSnapshot(ctx context.Context, executionID string) (domain.ExecutionSnapshot, error) {
	return s.store.GetExecutionSnapshot(ctx, executionID)
}

func ParseAndValidateWorkflowDefinition(raw json.RawMessage) (domain.WorkflowDefinitionSpec, error) {
	var workflowSpec domain.WorkflowDefinitionSpec
	if err := json.Unmarshal(raw, &workflowSpec); err != nil {
		return domain.WorkflowDefinitionSpec{}, fmt.Errorf("unmarshal workflow definition: %w", err)
	}
	if strings.TrimSpace(workflowSpec.EntryTask) == "" {
		return domain.WorkflowDefinitionSpec{}, errors.New("workflow definition must have an entry_task")
	}
	if len(workflowSpec.Tasks) == 0 {
		return domain.WorkflowDefinitionSpec{}, errors.New("workflow definition must have at least one task")
	}

	entryTaskFound := false
	for _, task := range workflowSpec.Tasks {
		if strings.TrimSpace(task.Name) == "" {
			return domain.WorkflowDefinitionSpec{}, errors.New("workflow definition tasks must have a name")
		}
		if strings.TrimSpace(task.HandlerKey) == "" {
			return domain.WorkflowDefinitionSpec{}, errors.New("workflow definition tasks must have a handler_key")
		}
		if strings.TrimSpace(task.Name) == strings.TrimSpace(workflowSpec.EntryTask) {
			entryTaskFound = true
		}
		if task.MaxAttempts < 0 {
			return domain.WorkflowDefinitionSpec{}, errors.New("workflow definition tasks must have max_attempts of at least 0")
		}
		if task.BackoffSeconds < 0 {
			return domain.WorkflowDefinitionSpec{}, errors.New("workflow definition tasks must have backoff_seconds of at least 0")
		}
		if strings.TrimSpace(task.NextTask) != "" && strings.TrimSpace(task.NextTask) == strings.TrimSpace(task.Name) {
			return domain.WorkflowDefinitionSpec{}, errors.New("workflow definition tasks cannot have next_task that points to itself")
		}
		if strings.TrimSpace(task.NextTask) != "" {
			if _, err := FindTaskSpecByName(workflowSpec, task.NextTask); err != nil {
				return domain.WorkflowDefinitionSpec{}, fmt.Errorf("workflow definition tasks have invalid next_task: %w", err)
			}
		}
	}
	if !entryTaskFound {
		return domain.WorkflowDefinitionSpec{}, errors.New("entry_task must be one of the tasks defined in the workflow definition")
	}

	return workflowSpec, nil
}

func FindEntryTask(workflowSpec domain.WorkflowDefinitionSpec) (domain.WorkflowTaskSpec, error) {
	entryTaskName := strings.TrimSpace(workflowSpec.EntryTask)
	for _, task := range workflowSpec.Tasks {
		if strings.TrimSpace(task.Name) == entryTaskName {
			return task, nil
		}
	}

	return domain.WorkflowTaskSpec{}, errors.New("entry_task must be one of the tasks defined in the workflow definition")
}

func FindTaskSpecByName(workflowSpec domain.WorkflowDefinitionSpec, taskName string) (domain.WorkflowTaskSpec, error) {
	for _, task := range workflowSpec.Tasks {
		if strings.TrimSpace(task.Name) == strings.TrimSpace(taskName) {
			return task, nil
		}
	}

	return domain.WorkflowTaskSpec{}, errors.New("task not found in workflow definition: " + taskName)
}
func FindNextTaskSpec(workflowSpec domain.WorkflowDefinitionSpec, currentTaskName string) (domain.WorkflowTaskSpec, bool, error) {
	taskSpec, err := FindTaskSpecByName(workflowSpec, currentTaskName)
	if err != nil {
		return domain.WorkflowTaskSpec{}, false, fmt.Errorf("failed to find current task: %w", err)
	}
	if strings.TrimSpace(taskSpec.NextTask) == "" {
		return domain.WorkflowTaskSpec{}, false, nil
	}
	nextTaskSpec, err := FindTaskSpecByName(workflowSpec, taskSpec.NextTask)
	if err != nil {
		return domain.WorkflowTaskSpec{}, false, fmt.Errorf("invalid next_task reference: %w", err)
	}
	return nextTaskSpec, true, nil
}
