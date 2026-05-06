package handlers

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"durableflow/internal/domain"
)

type SampleEchoHandler struct {
	logger *slog.Logger
}

func NewSampleEchoHandler(logger *slog.Logger) *SampleEchoHandler {
	return &SampleEchoHandler{logger: logger}
}

func (h *SampleEchoHandler) Key() string {
	return "sample.echo"
}

func (h *SampleEchoHandler) Handle(ctx context.Context, task domain.TaskInstance) (json.RawMessage, error) {
	h.logger.InfoContext(ctx, "processing sample task", "task_id", task.ID, "idempotency_key", task.IdempotencyKey)

	var input map[string]any
	if len(task.InputJSON) > 0 {
		if err := json.Unmarshal(task.InputJSON, &input); err != nil {
			return nil, err
		}
	}

	// TODO: Replace this with real business handlers once the execution model becomes definition-driven.
	return json.Marshal(map[string]any{
		"handler":      h.Key(),
		"task_id":      task.ID,
		"processed_at": time.Now().UTC().Format(time.RFC3339),
		"echo":         input,
	})
}

