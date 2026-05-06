package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

type TaskMessage struct {
	TaskID      string `json:"task_id"`
	ExecutionID string `json:"execution_id"`
	HandlerKey  string `json:"handler_key"`
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

func (r *RedisStreams) Consume(ctx context.Context, consumer string, handle func(context.Context, TaskMessage) error) error {
	if err := r.EnsureGroup(ctx); err != nil {
		return err
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		streams, err := r.client.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    r.group,
			Consumer: consumer,
			Streams:  []string{r.stream, ">"},
			Count:    1,
			Block:    5 * time.Second,
		}).Result()
		if err != nil {
			if errors.Is(err, redis.Nil) {
				continue
			}
			return err
		}

		for _, stream := range streams {
			for _, message := range stream.Messages {
				payloadValue, ok := message.Values["payload"]
				if !ok {
					return fmt.Errorf("stream message %s missing payload", message.ID)
				}

				payloadText, ok := payloadValue.(string)
				if !ok {
					payloadText = fmt.Sprint(payloadValue)
				}

				var task TaskMessage
				if err := json.Unmarshal([]byte(payloadText), &task); err != nil {
					return fmt.Errorf("decode stream message %s: %w", message.ID, err)
				}

				if err := handle(ctx, task); err != nil {
					r.logger.Error("task processing failed", "task_id", task.TaskID, "stream_message_id", message.ID, "error", err)
					continue
				}

				if err := r.client.XAck(ctx, r.stream, r.group, message.ID).Err(); err != nil {
					return err
				}
			}
		}
	}
}
