package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"time"

	"durableflow/internal/domain"
)

type NotificationSendHandler struct {
	logger           *slog.Logger
	idempotencyStore idempotencyStore
}

func NewNotificationSendHandler(logger *slog.Logger, idempotencyStore idempotencyStore) *NotificationSendHandler {
	return &NotificationSendHandler{
		logger:           logger,
		idempotencyStore: idempotencyStore,
	}
}

func (h *NotificationSendHandler) Key() string {
	return "notifications.send"
}

func (h *NotificationSendHandler) Handle(ctx context.Context, task domain.TaskInstance) (json.RawMessage, error) {
	h.logger.InfoContext(ctx, "processing notification task", "task_id", task.ID, "idempotency_key", task.IdempotencyKey)

	if h.idempotencyStore == nil {
		return nil, errors.New("notification handler idempotency store is required")
	}

	if existingResponse, replayed, err := h.idempotencyStore.BeginIdempotentTask(ctx, h.Key(), task.IdempotencyKey, task.ID); err != nil {
		return nil, err
	} else if replayed {
		h.logger.InfoContext(ctx, "returning cached idempotent notification response", "task_id", task.ID, "idempotency_key", task.IdempotencyKey)
		return existingResponse, nil
	}

	var input map[string]any
	if len(task.InputJSON) > 0 {
		if err := json.Unmarshal(task.InputJSON, &input); err != nil {
			_ = h.idempotencyStore.ReleaseIdempotentTask(ctx, h.Key(), task.IdempotencyKey, task.ID)
			return nil, err
		}
	}

	response, err := json.Marshal(map[string]any{
		"handler":         h.Key(),
		"task_id":         task.ID,
		"notification_id": task.IdempotencyKey,
		"sent_at":         time.Now().UTC().Format(time.RFC3339),
		"payload":         input,
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
