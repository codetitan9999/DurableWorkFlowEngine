package testutil

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"durableflow/internal/db"

	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	migrationsOnce sync.Once
	migrationsErr  error
)

func RequireIntegrationDatabase(t testing.TB) string {
	t.Helper()

	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("integration test skipped: TEST_DATABASE_URL is not set")
	}
	return databaseURL
}

func RequireIntegrationRedis(t testing.TB) string {
	t.Helper()

	redisAddr := os.Getenv("TEST_REDIS_ADDR")
	if redisAddr == "" {
		t.Skip("integration test skipped: TEST_REDIS_ADDR is not set")
	}
	return redisAddr
}

func OpenIntegrationStore(t testing.TB) (*db.Store, *pgxpool.Pool) {
	t.Helper()

	databaseURL := RequireIntegrationDatabase(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)

	pool, err := db.Open(ctx, databaseURL)
	if err != nil {
		t.Fatalf("open integration database: %v", err)
	}
	t.Cleanup(pool.Close)

	migrationsOnce.Do(func() {
		migrationsErr = db.ApplyMigrations(ctx, pool, filepath.Join(repoRoot(), "migrations"))
	})
	if migrationsErr != nil {
		t.Fatalf("apply migrations: %v", migrationsErr)
	}

	return db.NewStore(pool), pool
}

func DiscardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func repoRoot() string {
	_, currentFile, _, ok := runtime.Caller(0)
	if !ok {
		panic("failed to resolve integration helper path")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(currentFile), "..", ".."))
}
