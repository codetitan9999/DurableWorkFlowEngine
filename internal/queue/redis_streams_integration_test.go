package queue

import (
	"context"
	"fmt"
	"testing"
	"time"

	"durableflow/internal/testutil"

	"github.com/redis/go-redis/v9"
)

func TestConsumeReclaimsAbandonedPendingMessage(t *testing.T) {
	redisAddr := testutil.RequireIntegrationRedis(t)
	logger := testutil.DiscardLogger()

	streamName := fmt.Sprintf("durableflow.test.reclaim.%d", time.Now().UnixNano())
	groupName := fmt.Sprintf("durableflow-test-group-%d", time.Now().UnixNano())

	streams := NewRedisStreams(redisAddr, streamName, groupName, logger)
	t.Cleanup(func() { _ = streams.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := streams.EnsureGroup(ctx); err != nil {
		t.Fatalf("ensure group: %v", err)
	}
	if err := streams.DispatchTask(ctx, TaskMessage{
		TaskID:      "task-1",
		ExecutionID: "exec-1",
		HandlerKey:  "sample.echo",
	}); err != nil {
		t.Fatalf("dispatch task: %v", err)
	}

	rawClient := redis.NewClient(&redis.Options{Addr: redisAddr})
	t.Cleanup(func() {
		_ = rawClient.Del(ctx, streamName).Err()
		_ = rawClient.Close()
	})

	initialRead, err := rawClient.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    groupName,
		Consumer: "worker-1",
		Streams:  []string{streamName, ">"},
		Count:    1,
		Block:    100 * time.Millisecond,
	}).Result()
	if err != nil {
		t.Fatalf("initial xreadgroup: %v", err)
	}
	if len(initialRead) != 1 || len(initialRead[0].Messages) != 1 {
		t.Fatalf("expected one claimed pending message, got %+v", initialRead)
	}

	time.Sleep(75 * time.Millisecond)

	handled := make(chan TaskMessage, 1)
	consumeCtx, consumeCancel := context.WithCancel(ctx)
	defer consumeCancel()

	go func() {
		_ = streams.Consume(consumeCtx, ConsumeOptions{
			Consumer:       "worker-2",
			ReclaimMinIdle: 25 * time.Millisecond,
			ReclaimCount:   10,
			ReadCount:      1,
			Block:          50 * time.Millisecond,
		}, func(_ context.Context, task TaskMessage) error {
			handled <- task
			return nil
		})
	}()

	select {
	case task := <-handled:
		if task.TaskID != "task-1" {
			t.Fatalf("expected reclaimed task-1, got %s", task.TaskID)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for reclaimed message")
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		pending, err := rawClient.XPending(ctx, streamName, groupName).Result()
		if err != nil {
			t.Fatalf("xpending: %v", err)
		}
		if pending.Count == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("expected reclaimed message to be acked, pending count is %d", pending.Count)
		}
		time.Sleep(25 * time.Millisecond)
	}

	consumeCancel()
}
