package handlers

import (
	"context"
	"encoding/json"

	"durableflow/internal/domain"
)

type Handler interface {
	Key() string
	Handle(ctx context.Context, task domain.TaskInstance) (json.RawMessage, error)
}

type Registry struct {
	handlers map[string]Handler
}

func NewRegistry(items ...Handler) *Registry {
	handlersByKey := make(map[string]Handler, len(items))
	for _, item := range items {
		handlersByKey[item.Key()] = item
	}

	return &Registry{handlers: handlersByKey}
}

func (r *Registry) Get(key string) (Handler, bool) {
	handler, ok := r.handlers[key]
	return handler, ok
}

