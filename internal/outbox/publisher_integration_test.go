package outbox

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"durableflow/internal/domain"
	"durableflow/internal/queue"
	"durableflow/internal/testutil"

	"github.com/redis/go-redis/v9"
)

func TestPublishOnceDispatchesPendingOutboxEvent(t *testing.T) {
	store, _ := testutil.OpenIntegrationStore(t)
	redisAddr := testutil.RequireIntegrationRedis(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	clearAllPendingOutbox(t, ctx, store)

	definition, err := store.CreateWorkflowDefinition(ctx, fmt.Sprintf("outbox-%d", time.Now().UnixNano()), "outbox integration test", json.RawMessage(`{
		"entry_task": "validate-order",
		"tasks": [
			{
				"name": "validate-order",
				"handler_key": "sample.echo"
			}
		]
	}`))
	if err != nil {
		t.Fatalf("create workflow definition: %v", err)
	}

	start, err := store.CreateExecutionAndTask(ctx, definition.ID, json.RawMessage(`{"customer_id":"demo"}`), "validate-order", "sample.echo")
	if err != nil {
		t.Fatalf("create execution: %v", err)
	}

	streamName := fmt.Sprintf("durableflow.test.outbox.%d", time.Now().UnixNano())
	groupName := fmt.Sprintf("durableflow-test-group-%d", time.Now().UnixNano())
	streams := queue.NewRedisStreams(redisAddr, streamName, groupName, testutil.DiscardLogger())
	t.Cleanup(func() { _ = streams.Close() })

	pub := NewPublisher(store, streams, 10*time.Millisecond, testutil.DiscardLogger())
	if err := pub.publishOnce(ctx); err != nil {
		t.Fatalf("publish once: %v", err)
	}

	pending, err := store.ListPendingOutbox(ctx, 20)
	if err != nil {
		t.Fatalf("list pending outbox: %v", err)
	}
	for _, event := range pending {
		var payload domain.DispatchTaskPayload
		if err := json.Unmarshal(event.PayloadJSON, &payload); err == nil && payload.TaskID == start.Task.ID {
			t.Fatalf("expected task %s outbox event to be dispatched", start.Task.ID)
		}
	}

	redisClient := redis.NewClient(&redis.Options{Addr: redisAddr})
	t.Cleanup(func() {
		_ = redisClient.Del(ctx, streamName).Err()
		_ = redisClient.Close()
	})

	messages, err := redisClient.XRange(ctx, streamName, "-", "+").Result()
	if err != nil {
		t.Fatalf("read dispatched stream messages: %v", err)
	}
	if len(messages) == 0 {
		t.Fatal("expected at least one stream message")
	}

	found := false
	for _, message := range messages {
		payloadText, ok := message.Values["payload"].(string)
		if !ok {
			continue
		}

		var task queue.TaskMessage
		if err := json.Unmarshal([]byte(payloadText), &task); err != nil {
			continue
		}
		if task.TaskID == start.Task.ID {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected to find dispatched task id %s in stream messages", start.Task.ID)
	}
}

func clearAllPendingOutbox(t *testing.T, ctx context.Context, store interface {
	ListPendingOutbox(context.Context, int) ([]domain.OutboxEvent, error)
	MarkOutboxDispatched(context.Context, string) error
}) {
	t.Helper()

	pending, err := store.ListPendingOutbox(ctx, 200)
	if err != nil {
		t.Fatalf("list pending outbox: %v", err)
	}
	for _, event := range pending {
		if err := store.MarkOutboxDispatched(ctx, event.ID); err != nil {
			t.Fatalf("mark outbox dispatched: %v", err)
		}
	}
}
