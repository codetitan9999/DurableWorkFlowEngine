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
	if len(messages) != 1 {
		t.Fatalf("expected one stream message, got %d", len(messages))
	}

	payloadText, ok := messages[0].Values["payload"].(string)
	if !ok {
		t.Fatalf("expected payload string, got %#v", messages[0].Values["payload"])
	}

	var task queue.TaskMessage
	err = json.Unmarshal([]byte(payloadText), &task)
	if err != nil {
		t.Fatalf("decode dispatched message: %v", err)
	}
	if task.TaskID != start.Task.ID {
		t.Fatalf("expected task id %s, got %s", start.Task.ID, task.TaskID)
	}
}
