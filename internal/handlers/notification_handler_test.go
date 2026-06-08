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

func TestNotificationSendHandlerReturnsCachedResponseWhenIdempotencyRecordExists(t *testing.T) {
	store := &stubIdempotencyStore{
		beginResponse: json.RawMessage(`{"notification_id":"idem-1","cached":true}`),
		beginReplay:   true,
	}

	handler := NewNotificationSendHandler(
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		store,
	)

	output, err := handler.Handle(context.Background(), domain.TaskInstance{
		ID:             "task-2",
		IdempotencyKey: "idem-1",
	})
	if err != nil {
		t.Fatalf("expected cached response, got error: %v", err)
	}

	if string(output) != `{"notification_id":"idem-1","cached":true}` {
		t.Fatalf("expected cached response, got %s", output)
	}
	if store.completeCalls != 0 {
		t.Fatal("did not expect completion write when replaying cached response")
	}
}

func TestNotificationSendHandlerCompletesIdempotentExecutionOnFirstRun(t *testing.T) {
	store := &stubIdempotencyStore{}
	handler := NewNotificationSendHandler(
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		store,
	)

	output, err := handler.Handle(context.Background(), domain.TaskInstance{
		ID:             "task-2",
		IdempotencyKey: "idem-1",
		InputJSON:      json.RawMessage(`{"customer_id":"cust-1","order_id":"order-1"}`),
	})
	if err != nil {
		t.Fatalf("expected successful notification send, got error: %v", err)
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

func TestNotificationSendHandlerReleasesReservationWhenInputIsInvalid(t *testing.T) {
	store := &stubIdempotencyStore{}
	handler := NewNotificationSendHandler(
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		store,
	)

	_, err := handler.Handle(context.Background(), domain.TaskInstance{
		ID:             "task-2",
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

func TestNotificationSendHandlerReleasesReservationWhenCompleteFails(t *testing.T) {
	store := &stubIdempotencyStore{
		completeErr: errors.New("write failed"),
	}
	handler := NewNotificationSendHandler(
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		store,
	)

	_, err := handler.Handle(context.Background(), domain.TaskInstance{
		ID:             "task-2",
		IdempotencyKey: "idem-1",
		InputJSON:      json.RawMessage(`{"customer_id":"cust-1","order_id":"order-1"}`),
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
