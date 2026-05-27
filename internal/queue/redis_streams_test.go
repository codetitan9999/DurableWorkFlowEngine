package queue

import (
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestNormalizeConsumeOptions(t *testing.T) {
	opts := normalizeConsumeOptions(ConsumeOptions{})

	if opts.ReclaimMinIdle != 30*time.Second {
		t.Fatalf("expected default reclaim idle of 30s, got %s", opts.ReclaimMinIdle)
	}
	if opts.ReclaimCount != 10 {
		t.Fatalf("expected default reclaim count of 10, got %d", opts.ReclaimCount)
	}
	if opts.ReadCount != 1 {
		t.Fatalf("expected default read count of 1, got %d", opts.ReadCount)
	}
	if opts.Block != 5*time.Second {
		t.Fatalf("expected default read block of 5s, got %s", opts.Block)
	}
}

func TestDecodeTaskMessage(t *testing.T) {
	message := redis.XMessage{
		ID: "1-0",
		Values: map[string]any{
			"payload": `{"task_id":"task-1","execution_id":"exec-1","handler_key":"sample.echo"}`,
		},
	}

	task, err := decodeTaskMessage(message)
	if err != nil {
		t.Fatalf("expected message to decode, got error: %v", err)
	}

	if task.TaskID != "task-1" {
		t.Fatalf("expected task_id task-1, got %q", task.TaskID)
	}
	if task.ExecutionID != "exec-1" {
		t.Fatalf("expected execution_id exec-1, got %q", task.ExecutionID)
	}
	if task.HandlerKey != "sample.echo" {
		t.Fatalf("expected handler_key sample.echo, got %q", task.HandlerKey)
	}
}

func TestDecodeTaskMessageRejectsMissingPayload(t *testing.T) {
	_, err := decodeTaskMessage(redis.XMessage{ID: "1-0", Values: map[string]any{}})
	if err == nil {
		t.Fatal("expected missing payload to fail")
	}
}
