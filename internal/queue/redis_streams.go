package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"durableflow/internal/telemetry"

	"github.com/redis/go-redis/v9"
)

type TaskMessage struct {
	TaskID      string `json:"task_id"`
	ExecutionID string `json:"execution_id"`
	HandlerKey  string `json:"handler_key"`
}

type ConsumeOptions struct {
	Consumer       string
	ReclaimMinIdle time.Duration
	ReclaimCount   int64
	ReadCount      int64
	Block          time.Duration
}

type RedisStreams struct {
	client *redis.Client
	stream string
	group  string
	logger *slog.Logger
}

func NewRedisStreams(addr, stream, group string, logger *slog.Logger) *RedisStreams {
	return &RedisStreams{
		client: redis.NewClient(&redis.Options{Addr: addr}),
		stream: stream,
		group:  group,
		logger: logger,
	}
}

func (r *RedisStreams) Close() error {
	return r.client.Close()
}

func (r *RedisStreams) Ping(ctx context.Context) error {
	return r.client.Ping(ctx).Err()
}

func (r *RedisStreams) WaitForReady(ctx context.Context, timeout time.Duration) error {
	if timeout <= 0 {
		timeout = 30 * time.Second
	}

	deadline := time.Now().Add(timeout)
	var lastErr error

	for {
		if err := r.Ping(ctx); err == nil {
			return nil
		} else {
			lastErr = err
		}

		if time.Now().After(deadline) {
			return lastErr
		}

		select {
		case <-ctx.Done():
			return errors.Join(ctx.Err(), lastErr)
		case <-time.After(1 * time.Second):
		}
	}
}

func (r *RedisStreams) EnsureGroup(ctx context.Context) error {
	err := r.client.XGroupCreateMkStream(ctx, r.stream, r.group, "0").Err()
	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		return err
	}
	return nil
}

func (r *RedisStreams) DispatchTask(ctx context.Context, message TaskMessage) error {
	payload, err := json.Marshal(message)
	if err != nil {
		return err
	}

	return r.client.XAdd(ctx, &redis.XAddArgs{
		Stream: r.stream,
		Values: map[string]any{
			"payload": string(payload),
		},
	}).Err()
}

func (r *RedisStreams) Consume(ctx context.Context, opts ConsumeOptions, handle func(context.Context, TaskMessage) error) error {
	opts = normalizeConsumeOptions(opts)

	if err := r.EnsureGroup(ctx); err != nil {
		return err
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		reclaimed, err := r.claimPending(ctx, opts)
		if err != nil {
			return err
		}
		if len(reclaimed) > 0 {
			telemetry.ReclaimedMessages.WithLabelValues("worker", r.stream, r.group).Add(float64(len(reclaimed)))
			r.logger.Info("reclaimed pending stream messages", "stream", r.stream, "group", r.group, "consumer", opts.Consumer, "count", len(reclaimed))
			if err := r.processMessages(ctx, opts.Consumer, reclaimed, handle); err != nil {
				return err
			}
			continue
		}

		streams, err := r.client.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    r.group,
			Consumer: opts.Consumer,
			Streams:  []string{r.stream, ">"},
			Count:    opts.ReadCount,
			Block:    opts.Block,
		}).Result()
		if err != nil {
			if errors.Is(err, redis.Nil) {
				continue
			}
			return err
		}

		for _, stream := range streams {
			if err := r.processMessages(ctx, opts.Consumer, stream.Messages, handle); err != nil {
				return err
			}
		}
	}
}

func normalizeConsumeOptions(opts ConsumeOptions) ConsumeOptions {
	if opts.ReclaimMinIdle <= 0 {
		opts.ReclaimMinIdle = 30 * time.Second
	}
	if opts.ReclaimCount <= 0 {
		opts.ReclaimCount = 10
	}
	if opts.ReadCount <= 0 {
		opts.ReadCount = 1
	}
	if opts.Block <= 0 {
		opts.Block = 5 * time.Second
	}
	return opts
}

func (r *RedisStreams) claimPending(ctx context.Context, opts ConsumeOptions) ([]redis.XMessage, error) {
	messages, _, err := r.client.XAutoClaim(ctx, &redis.XAutoClaimArgs{
		Stream:   r.stream,
		Group:    r.group,
		Consumer: opts.Consumer,
		MinIdle:  opts.ReclaimMinIdle,
		Start:    "0-0",
		Count:    opts.ReclaimCount,
	}).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return nil, err
	}
	return messages, nil
}

func (r *RedisStreams) processMessages(ctx context.Context, consumer string, messages []redis.XMessage, handle func(context.Context, TaskMessage) error) error {
	for _, message := range messages {
		task, err := decodeTaskMessage(message)
		if err != nil {
			return err
		}

		if err := handle(ctx, task); err != nil {
			r.logger.Error("task processing failed", "task_id", task.TaskID, "stream_message_id", message.ID, "consumer", consumer, "error", err)
			continue
		}

		if err := r.client.XAck(ctx, r.stream, r.group, message.ID).Err(); err != nil {
			return err
		}
	}

	return nil
}

func decodeTaskMessage(message redis.XMessage) (TaskMessage, error) {
	payloadValue, ok := message.Values["payload"]
	if !ok {
		return TaskMessage{}, fmt.Errorf("stream message %s missing payload", message.ID)
	}

	payloadText, ok := payloadValue.(string)
	if !ok {
		payloadText = fmt.Sprint(payloadValue)
	}

	var task TaskMessage
	if err := json.Unmarshal([]byte(payloadText), &task); err != nil {
		return TaskMessage{}, fmt.Errorf("decode stream message %s: %w", message.ID, err)
	}

	return task, nil
}
