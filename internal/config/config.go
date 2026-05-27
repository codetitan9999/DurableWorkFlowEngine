package config

import (
	"fmt"
	"os"
	"time"
)

type Config struct {
	ServiceName         string
	HTTPAddr            string
	DatabaseURL         string
	RedisAddr           string
	RedisStream         string
	RedisGroup          string
	RedisConsumer       string
	RedisReclaimMinIdle time.Duration
	RedisReclaimCount   int64
	StartupTimeout      time.Duration
	OutboxPollInterval  time.Duration
	OTLPTraceEndpoint   string
}

func LoadAPIConfig() Config {
	return Config{
		ServiceName:         envOrDefault("SERVICE_NAME", "durableflow-api"),
		HTTPAddr:            envOrDefault("API_HTTP_ADDR", ":8080"),
		DatabaseURL:         envOrDefault("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/durableflow?sslmode=disable"),
		RedisAddr:           envOrDefault("REDIS_ADDR", "localhost:6379"),
		RedisStream:         envOrDefault("REDIS_STREAM", "durableflow.tasks"),
		RedisGroup:          envOrDefault("REDIS_GROUP", "durableflow-workers"),
		RedisConsumer:       envOrDefault("REDIS_CONSUMER", "api"),
		RedisReclaimMinIdle: durationOrDefault("REDIS_RECLAIM_MIN_IDLE", 30*time.Second),
		RedisReclaimCount:   int64OrDefault("REDIS_RECLAIM_COUNT", 10),
		StartupTimeout:      durationOrDefault("STARTUP_TIMEOUT", 30*time.Second),
		OutboxPollInterval:  durationOrDefault("OUTBOX_POLL_INTERVAL", 2*time.Second),
		OTLPTraceEndpoint:   os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"),
	}
}

func LoadWorkerConfig() Config {
	return Config{
		ServiceName:         envOrDefault("SERVICE_NAME", "durableflow-worker"),
		HTTPAddr:            envOrDefault("WORKER_HTTP_ADDR", ":8081"),
		DatabaseURL:         envOrDefault("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/durableflow?sslmode=disable"),
		RedisAddr:           envOrDefault("REDIS_ADDR", "localhost:6379"),
		RedisStream:         envOrDefault("REDIS_STREAM", "durableflow.tasks"),
		RedisGroup:          envOrDefault("REDIS_GROUP", "durableflow-workers"),
		RedisConsumer:       envOrDefault("REDIS_CONSUMER", fmt.Sprintf("worker-%d", time.Now().Unix())),
		RedisReclaimMinIdle: durationOrDefault("REDIS_RECLAIM_MIN_IDLE", 30*time.Second),
		RedisReclaimCount:   int64OrDefault("REDIS_RECLAIM_COUNT", 10),
		StartupTimeout:      durationOrDefault("STARTUP_TIMEOUT", 30*time.Second),
		OutboxPollInterval:  durationOrDefault("OUTBOX_POLL_INTERVAL", 2*time.Second),
		OTLPTraceEndpoint:   os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"),
	}
}

func envOrDefault(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func durationOrDefault(key string, fallback time.Duration) time.Duration {
	raw := os.Getenv(key)
	if raw == "" {
		return fallback
	}

	value, err := time.ParseDuration(raw)
	if err != nil {
		return fallback
	}

	return value
}

func int64OrDefault(key string, fallback int64) int64 {
	raw := os.Getenv(key)
	if raw == "" {
		return fallback
	}

	var value int64
	_, err := fmt.Sscan(raw, &value)
	if err != nil || value <= 0 {
		return fallback
	}

	return value
}
