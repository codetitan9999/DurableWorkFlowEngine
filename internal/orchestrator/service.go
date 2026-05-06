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

	s.logger.InfoContext(ctx, "creating workflow definition", "name", name)
	return s.store.CreateWorkflowDefinition(ctx, name, req.Description, definition)
}

func (s *Service) TriggerExecution(ctx context.Context, req TriggerExecutionRequest) (domain.ExecutionStartResult, error) {
	if strings.TrimSpace(req.WorkflowDefinitionID) == "" {
		return domain.ExecutionStartResult{}, errors.New("workflow_definition_id is required")
	}

	// TODO: This currently creates one sample task. Later phases should expand the workflow graph from the definition.
	result, err := s.store.CreateExecutionAndTask(ctx, req.WorkflowDefinitionID, req.Input)
	if err != nil {
		return domain.ExecutionStartResult{}, fmt.Errorf("create execution: %w", err)
	}

	s.logger.InfoContext(ctx, "execution triggered", "execution_id", result.Execution.ID, "task_id", result.Task.ID)
	return result, nil
}

func (s *Service) GetExecutionSnapshot(ctx context.Context, executionID string) (domain.ExecutionSnapshot, error) {
	return s.store.GetExecutionSnapshot(ctx, executionID)
}

