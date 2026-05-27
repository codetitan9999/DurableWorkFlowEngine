package db

import (
	"context"
	"encoding/json"
	"errors"

	"durableflow/internal/domain"

	"github.com/jackc/pgx/v5"
)

var errIdempotencyKeyInProgress = errors.New("idempotency key is already in progress")

func (s *Store) BeginIdempotentTask(ctx context.Context, handlerKey, idempotencyKey, ownerTaskID string) (json.RawMessage, bool, error) {
	commandTag, err := s.pool.Exec(ctx, `
		INSERT INTO idempotency_records (
			handler_key,
			idempotency_key,
			owner_task_instance_id,
			status,
			created_at,
			updated_at
		)
		VALUES ($1, $2, $3, $4, NOW(), NOW())
		ON CONFLICT (handler_key, idempotency_key) DO NOTHING
	`, handlerKey, idempotencyKey, ownerTaskID, domain.IdempotencyStatusInProgress)
	if err != nil {
		return nil, false, err
	}

	if commandTag.RowsAffected() == 1 {
		return nil, false, nil
	}

	var status string
	var response []byte
	var currentOwnerTaskID *string
	if err := s.pool.QueryRow(ctx, `
		SELECT status, response_json, owner_task_instance_id::text
		FROM idempotency_records
		WHERE handler_key = $1 AND idempotency_key = $2
	`, handlerKey, idempotencyKey).Scan(&status, &response, &currentOwnerTaskID); err != nil {
		return nil, false, err
	}

	if status == domain.IdempotencyStatusCompleted {
		return response, true, nil
	}

	if currentOwnerTaskID != nil && *currentOwnerTaskID == ownerTaskID {
		return nil, false, nil
	}

	return nil, false, errIdempotencyKeyInProgress
}

func (s *Store) CompleteIdempotentTask(ctx context.Context, handlerKey, idempotencyKey, ownerTaskID string, response json.RawMessage) error {
	commandTag, err := s.pool.Exec(ctx, `
		UPDATE idempotency_records
		SET status = $3,
			response_json = $5,
			updated_at = NOW()
		WHERE handler_key = $1
			AND idempotency_key = $2
			AND owner_task_instance_id = $4::uuid
	`, handlerKey, idempotencyKey, domain.IdempotencyStatusCompleted, ownerTaskID, normalizeJSON(response))
	if err != nil {
		return err
	}
	if commandTag.RowsAffected() == 0 {
		return pgx.ErrNoRows
	}
	return nil
}

func (s *Store) ReleaseIdempotentTask(ctx context.Context, handlerKey, idempotencyKey, ownerTaskID string) error {
	_, err := s.pool.Exec(ctx, `
		DELETE FROM idempotency_records
		WHERE handler_key = $1
			AND idempotency_key = $2
			AND status = $3
			AND owner_task_instance_id = $4::uuid
	`, handlerKey, idempotencyKey, domain.IdempotencyStatusInProgress, ownerTaskID)
	return err
}

func IsIdempotencyKeyInProgress(err error) bool {
	return errors.Is(err, errIdempotencyKeyInProgress)
}
