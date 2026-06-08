package config

import (
	"errors"
	"fmt"
	"os"
	"strings"
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

func LoadAPIConfig() (Config, error) {
	cfg, err := loadConfig("api")
	if err != nil {
		return Config{}, err
	}
	if cfg.ServiceName == "" {
		cfg.ServiceName = "durableflow-api"
	}
	if cfg.HTTPAddr == "" {
		cfg.HTTPAddr = ":8080"
	}
	if cfg.RedisConsumer == "" {
		cfg.RedisConsumer = "api"
	}
	return cfg, cfg.Validate()
}

func LoadWorkerConfig() (Config, error) {
	cfg, err := loadConfig("worker")
	if err != nil {
		return Config{}, err
	}
	if cfg.ServiceName == "" {
		cfg.ServiceName = "durableflow-worker"
	}
	if cfg.HTTPAddr == "" {
		cfg.HTTPAddr = ":8081"
	}
	if cfg.RedisConsumer == "" {
		cfg.RedisConsumer = fmt.Sprintf("worker-%d", time.Now().Unix())
	}
	return cfg, cfg.Validate()
}

func loadConfig(kind string) (Config, error) {
	httpAddrKey := "API_HTTP_ADDR"
	if kind == "worker" {
		httpAddrKey = "WORKER_HTTP_ADDR"
	}

	cfg := Config{
		ServiceName:       envOrDefault("SERVICE_NAME", ""),
		HTTPAddr:          envOrDefault(httpAddrKey, ""),
		DatabaseURL:       envOrDefault("DATABASE_URL", ""),
		RedisAddr:         envOrDefault("REDIS_ADDR", ""),
		RedisStream:       envOrDefault("REDIS_STREAM", ""),
		RedisGroup:        envOrDefault("REDIS_GROUP", ""),
		RedisConsumer:     envOrDefault("REDIS_CONSUMER", ""),
		OTLPTraceEndpoint: os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"),
	}

	var err error
	if cfg.RedisReclaimMinIdle, err = durationOrDefault("REDIS_RECLAIM_MIN_IDLE", 30*time.Second); err != nil {
		return Config{}, err
	}
	if cfg.RedisReclaimCount, err = int64OrDefault("REDIS_RECLAIM_COUNT", 10); err != nil {
		return Config{}, err
	}
	if cfg.StartupTimeout, err = durationOrDefault("STARTUP_TIMEOUT", 30*time.Second); err != nil {
		return Config{}, err
	}
	if cfg.OutboxPollInterval, err = durationOrDefault("OUTBOX_POLL_INTERVAL", 2*time.Second); err != nil {
		return Config{}, err
	}

	return cfg, nil
}

func (c Config) Validate() error {
	var problems []string

	if strings.TrimSpace(c.ServiceName) == "" {
		problems = append(problems, "SERVICE_NAME must not be empty")
	}
	if strings.TrimSpace(c.HTTPAddr) == "" {
		problems = append(problems, "HTTP listen address must not be empty")
	}
	if strings.TrimSpace(c.DatabaseURL) == "" {
		problems = append(problems, "DATABASE_URL must not be empty")
	}
	if strings.TrimSpace(c.RedisAddr) == "" {
		problems = append(problems, "REDIS_ADDR must not be empty")
	}
	if strings.TrimSpace(c.RedisStream) == "" {
		problems = append(problems, "REDIS_STREAM must not be empty")
	}
	if strings.TrimSpace(c.RedisGroup) == "" {
		problems = append(problems, "REDIS_GROUP must not be empty")
	}
	if strings.TrimSpace(c.RedisConsumer) == "" {
		problems = append(problems, "REDIS_CONSUMER must not be empty")
	}
	if c.RedisReclaimMinIdle <= 0 {
		problems = append(problems, "REDIS_RECLAIM_MIN_IDLE must be greater than 0")
	}
	if c.RedisReclaimCount <= 0 {
		problems = append(problems, "REDIS_RECLAIM_COUNT must be greater than 0")
	}
	if c.StartupTimeout <= 0 {
		problems = append(problems, "STARTUP_TIMEOUT must be greater than 0")
	}
	if c.OutboxPollInterval <= 0 {
		problems = append(problems, "OUTBOX_POLL_INTERVAL must be greater than 0")
	}

	if len(problems) > 0 {
		return errors.New(strings.Join(problems, "; "))
	}
	return nil
}

func envOrDefault(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func durationOrDefault(key string, fallback time.Duration) (time.Duration, error) {
	raw := os.Getenv(key)
	if raw == "" {
		return fallback, nil
	}

	value, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("%s must be a valid duration: %w", key, err)
	}
	if value <= 0 {
		return 0, fmt.Errorf("%s must be greater than 0", key)
	}

	return value, nil
}

func int64OrDefault(key string, fallback int64) (int64, error) {
	raw := os.Getenv(key)
	if raw == "" {
		return fallback, nil
	}

	var value int64
	_, err := fmt.Sscan(raw, &value)
	if err != nil {
		return 0, fmt.Errorf("%s must be a valid integer: %w", key, err)
	}
	if value <= 0 {
		return 0, fmt.Errorf("%s must be greater than 0", key)
	}

	return value, nil
}
