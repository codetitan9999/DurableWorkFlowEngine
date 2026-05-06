package db

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func Open(ctx context.Context, databaseURL string) (*pgxpool.Pool, error) {
	config, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, err
	}

	config.MaxConns = 8
	return pgxpool.NewWithConfig(ctx, config)
}

func OpenWithRetry(ctx context.Context, databaseURL string, timeout time.Duration) (*pgxpool.Pool, error) {
	if timeout <= 0 {
		timeout = 30 * time.Second
	}

	deadline := time.Now().Add(timeout)
	var lastErr error

	for {
		pool, err := Open(ctx, databaseURL)
		if err == nil {
			pingCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
			pingErr := pool.Ping(pingCtx)
			cancel()

			if pingErr == nil {
				return pool, nil
			}

			lastErr = pingErr
			pool.Close()
		} else {
			lastErr = err
		}

		if time.Now().After(deadline) {
			return nil, lastErr
		}

		select {
		case <-ctx.Done():
			return nil, errors.Join(ctx.Err(), lastErr)
		case <-time.After(1 * time.Second):
		}
	}
}
