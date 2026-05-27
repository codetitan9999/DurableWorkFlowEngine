package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"time"

	"durableflow/internal/domain"
)

type idempotencyStore interface {
	BeginIdempotentTask(ctx context.Context, handlerKey, idempotencyKey, ownerTaskID string) (json.RawMessage, bool, error)
	CompleteIdempotentTask(ctx context.Context, handlerKey, idempotencyKey, ownerTaskID string, response json.RawMessage) error
	ReleaseIdempotentTask(ctx context.Context, handlerKey, idempotencyKey, ownerTaskID string) error
}

type SampleEchoHandler struct {
	logger           *slog.Logger
	idempotencyStore idempotencyStore
}

func NewSampleEchoHandler(logger *slog.Logger, idempotencyStore idempotencyStore) *SampleEchoHandler {
	return &SampleEchoHandler{
		logger:           logger,
		idempotencyStore: idempotencyStore,
	}
}

func (h *SampleEchoHandler) Key() string {
	return "sample.echo"
}

func (h *SampleEchoHandler) Handle(ctx context.Context, task domain.TaskInstance) (json.RawMessage, error) {
	h.logger.InfoContext(ctx, "processing sample task", "task_id", task.ID, "idempotency_key", task.IdempotencyKey)

	if h.idempotencyStore == nil {
		return nil, errors.New("sample handler idempotency store is required")
	}

	if existingResponse, replayed, err := h.idempotencyStore.BeginIdempotentTask(ctx, h.Key(), task.IdempotencyKey, task.ID); err != nil {
		return nil, err
	} else if replayed {
		h.logger.InfoContext(ctx, "returning cached idempotent response", "task_id", task.ID, "idempotency_key", task.IdempotencyKey)
		return existingResponse, nil
	}

	var input map[string]any
	if len(task.InputJSON) > 0 {
		if err := json.Unmarshal(task.InputJSON, &input); err != nil {
			_ = h.idempotencyStore.ReleaseIdempotentTask(ctx, h.Key(), task.IdempotencyKey, task.ID)
			return nil, err
		}
	}

	// TODO: Replace this with real business handlers once the execution model becomes definition-driven.
	response, err := json.Marshal(map[string]any{
		"handler":      h.Key(),
		"task_id":      task.ID,
		"processed_at": time.Now().UTC().Format(time.RFC3339),
		"echo":         input,
	})
	if err != nil {
		_ = h.idempotencyStore.ReleaseIdempotentTask(ctx, h.Key(), task.IdempotencyKey, task.ID)
		return nil, err
	}

	if err := h.idempotencyStore.CompleteIdempotentTask(ctx, h.Key(), task.IdempotencyKey, task.ID, response); err != nil {
		_ = h.idempotencyStore.ReleaseIdempotentTask(ctx, h.Key(), task.IdempotencyKey, task.ID)
		return nil, err
	}

	return response, nil
}
