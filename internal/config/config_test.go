package config

import (
	"strings"
	"testing"
	"time"
)

func TestLoadAPIConfigDefaults(t *testing.T) {
	t.Setenv("SERVICE_NAME", "durableflow-api")
	t.Setenv("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/durableflow?sslmode=disable")
	t.Setenv("REDIS_ADDR", "localhost:6379")
	t.Setenv("REDIS_STREAM", "durableflow.tasks")
	t.Setenv("REDIS_GROUP", "durableflow-workers")

	cfg, err := LoadAPIConfig()
	if err != nil {
		t.Fatalf("expected defaults to load, got error: %v", err)
	}

	if cfg.HTTPAddr != ":8080" {
		t.Fatalf("expected default api addr :8080, got %q", cfg.HTTPAddr)
	}
	if cfg.RedisConsumer != "api" {
		t.Fatalf("expected default api consumer, got %q", cfg.RedisConsumer)
	}
	if cfg.OutboxPollInterval != 2*time.Second {
		t.Fatalf("expected default outbox interval 2s, got %s", cfg.OutboxPollInterval)
	}
}

func TestLoadAPIConfigRejectsInvalidDuration(t *testing.T) {
	t.Setenv("SERVICE_NAME", "durableflow-api")
	t.Setenv("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/durableflow?sslmode=disable")
	t.Setenv("REDIS_ADDR", "localhost:6379")
	t.Setenv("REDIS_STREAM", "durableflow.tasks")
	t.Setenv("REDIS_GROUP", "durableflow-workers")
	t.Setenv("OUTBOX_POLL_INTERVAL", "later")

	_, err := LoadAPIConfig()
	if err == nil {
		t.Fatal("expected invalid duration to fail")
	}
	if !strings.Contains(err.Error(), "OUTBOX_POLL_INTERVAL") {
		t.Fatalf("expected OUTBOX_POLL_INTERVAL error, got %v", err)
	}
}

func TestLoadWorkerConfigRejectsInvalidInteger(t *testing.T) {
	t.Setenv("SERVICE_NAME", "durableflow-worker")
	t.Setenv("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/durableflow?sslmode=disable")
	t.Setenv("REDIS_ADDR", "localhost:6379")
	t.Setenv("REDIS_STREAM", "durableflow.tasks")
	t.Setenv("REDIS_GROUP", "durableflow-workers")
	t.Setenv("REDIS_RECLAIM_COUNT", "zero")

	_, err := LoadWorkerConfig()
	if err == nil {
		t.Fatal("expected invalid reclaim count to fail")
	}
	if !strings.Contains(err.Error(), "REDIS_RECLAIM_COUNT") {
		t.Fatalf("expected REDIS_RECLAIM_COUNT error, got %v", err)
	}
}

func TestConfigValidateRejectsMissingFields(t *testing.T) {
	err := (Config{}).Validate()
	if err == nil {
		t.Fatal("expected missing required fields to fail validation")
	}

	for _, want := range []string{
		"SERVICE_NAME",
		"DATABASE_URL",
		"REDIS_ADDR",
		"REDIS_STREAM",
		"REDIS_GROUP",
		"REDIS_CONSUMER",
		"OUTBOX_POLL_INTERVAL",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("expected validation error to mention %s, got %v", want, err)
		}
	}
}
