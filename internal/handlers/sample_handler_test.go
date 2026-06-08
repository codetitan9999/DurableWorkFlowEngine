package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"testing"

	"durableflow/internal/domain"
)

type stubIdempotencyStore struct {
	beginResponse   json.RawMessage
	beginReplay     bool
	beginErr        error
	completeErr     error
	releaseErr      error
	beginCalls      int
	completeCalls   int
	releaseCalls    int
	completedOutput json.RawMessage
}

func (s *stubIdempotencyStore) BeginIdempotentTask(context.Context, string, string, string) (json.RawMessage, bool, error) {
	s.beginCalls++
	return s.beginResponse, s.beginReplay, s.beginErr
}

func (s *stubIdempotencyStore) CompleteIdempotentTask(context.Context, string, string, string, json.RawMessage) error {
	s.completeCalls++
	return s.completeErr
}

func (s *stubIdempotencyStore) ReleaseIdempotentTask(context.Context, string, string, string) error {
	s.releaseCalls++
	return s.releaseErr
}

func TestSampleEchoHandlerReturnsCachedResponseWhenIdempotencyRecordExists(t *testing.T) {
	store := &stubIdempotencyStore{
		beginResponse: json.RawMessage(`{"cached":true}`),
		beginReplay:   true,
	}

	handler := NewSampleEchoHandler(
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		store,
	)

	output, err := handler.Handle(context.Background(), domain.TaskInstance{
		ID:             "task-1",
		IdempotencyKey: "idem-1",
	})
	if err != nil {
		t.Fatalf("expected cached response, got error: %v", err)
	}

	if string(output) != `{"cached":true}` {
		t.Fatalf("expected cached response, got %s", output)
	}
	if store.completeCalls != 0 {
		t.Fatal("did not expect completion write when replaying cached response")
	}
}

func TestSampleEchoHandlerCompletesIdempotentExecutionOnFirstRun(t *testing.T) {
	store := &stubIdempotencyStore{}
	handler := NewSampleEchoHandler(
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		store,
	)

	output, err := handler.Handle(context.Background(), domain.TaskInstance{
		ID:             "task-1",
		IdempotencyKey: "idem-1",
		InputJSON:      json.RawMessage(`{"customer_id":"cust-1"}`),
	})
	if err != nil {
		t.Fatalf("expected successful first run, got error: %v", err)
	}

	if len(output) == 0 {
		t.Fatal("expected non-empty output")
	}
	if store.beginCalls != 1 {
		t.Fatalf("expected one begin call, got %d", store.beginCalls)
	}
	if store.completeCalls != 1 {
		t.Fatalf("expected one complete call, got %d", store.completeCalls)
	}
	if store.releaseCalls != 0 {
		t.Fatalf("did not expect release call on successful run, got %d", store.releaseCalls)
	}
}

func TestSampleEchoHandlerReleasesReservationWhenInputIsInvalid(t *testing.T) {
	store := &stubIdempotencyStore{}
	handler := NewSampleEchoHandler(
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		store,
	)

	_, err := handler.Handle(context.Background(), domain.TaskInstance{
		ID:             "task-1",
		IdempotencyKey: "idem-1",
		InputJSON:      json.RawMessage(`{"customer_id":`),
	})
	if err == nil {
		t.Fatal("expected invalid input to fail")
	}
	if store.releaseCalls != 1 {
		t.Fatalf("expected one release call, got %d", store.releaseCalls)
	}
}

func TestSampleEchoHandlerReturnsBeginError(t *testing.T) {
	store := &stubIdempotencyStore{
		beginErr: errors.New("reservation failed"),
	}
	handler := NewSampleEchoHandler(
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		store,
	)

	_, err := handler.Handle(context.Background(), domain.TaskInstance{
		ID:             "task-1",
		IdempotencyKey: "idem-1",
	})
	if err == nil {
		t.Fatal("expected begin error to be returned")
	}
	if store.completeCalls != 0 {
		t.Fatal("did not expect completion after begin failure")
	}
}

func TestSampleEchoHandlerReleasesReservationWhenCompleteFails(t *testing.T) {
	store := &stubIdempotencyStore{
		completeErr: errors.New("write failed"),
	}
	handler := NewSampleEchoHandler(
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		store,
	)

	_, err := handler.Handle(context.Background(), domain.TaskInstance{
		ID:             "task-1",
		IdempotencyKey: "idem-1",
		InputJSON:      json.RawMessage(`{"customer_id":"cust-1"}`),
	})
	if err == nil {
		t.Fatal("expected completion error to be returned")
	}
	if store.completeCalls != 1 {
		t.Fatalf("expected one completion call, got %d", store.completeCalls)
	}
	if store.releaseCalls != 1 {
		t.Fatalf("expected reservation release after completion error, got %d", store.releaseCalls)
	}
}
