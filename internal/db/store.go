package db

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"durableflow/internal/domain"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type rowScanner interface {
	Scan(dest ...any) error
}

type Store struct {
	pool *pgxpool.Pool
}

var errTaskNotReplayable = errors.New("task is not dead-lettered")

func NewStore(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

func makeTaskIdempotencyKey(executionID, taskName string) string {
	return fmt.Sprintf("%s:%s", executionID, taskName)
}

func (s *Store) Ping(ctx context.Context) error {
	return s.pool.Ping(ctx)
}

func (s *Store) CreateWorkflowDefinition(ctx context.Context, name, description string, definition json.RawMessage) (domain.WorkflowDefinition, error) {
	row := s.pool.QueryRow(ctx, `
		INSERT INTO workflow_definitions (name, description, definition_json, status)
		VALUES ($1, $2, $3, $4)
		RETURNING id, name, description, version, status, definition_json, created_at, updated_at
	`, name, description, normalizeJSON(definition), domain.WorkflowDefinitionStatusActive)

	return scanWorkflowDefinition(row)
}

func (s *Store) GetWorkflowDefinition(ctx context.Context, id string) (domain.WorkflowDefinition, error) {
	row := s.pool.QueryRow(ctx, `
		SELECT id, name, description, version, status, definition_json, created_at, updated_at
		FROM workflow_definitions
		WHERE id = $1
	`, id)

	return scanWorkflowDefinition(row)
}

func (s *Store) GetWorkflowExecution(ctx context.Context, id string) (domain.WorkflowExecution, error) {
	row := s.pool.QueryRow(ctx, `
		SELECT id, workflow_definition_id, status, input_json, output_json, error_text, created_at, updated_at, started_at, completed_at
		FROM workflow_executions
		WHERE id = $1
	`, id)

	return scanWorkflowExecution(row)
}

func (s *Store) GetTaskInstance(ctx context.Context, id string) (domain.TaskInstance, error) {
	row := s.pool.QueryRow(ctx, `
		SELECT id, workflow_execution_id, task_name, handler_key, status, input_json, output_json, next_run_at, last_error_text, attempts_total, idempotency_key, dispatched_at, completed_at, created_at, updated_at
		FROM task_instances
		WHERE id = $1
	`, id)

	return scanTaskInstance(row)
}

func (s *Store) CreateExecutionAndTask(ctx context.Context, workflowDefinitionID string, input json.RawMessage, taskName string, handlerKey string) (domain.ExecutionStartResult, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return domain.ExecutionStartResult{}, err
	}
	defer tx.Rollback(ctx)

	executionRow := tx.QueryRow(ctx, `
		INSERT INTO workflow_executions (
			workflow_definition_id,
			status,
			input_json,
			created_at,
			updated_at,
			started_at
		)
		VALUES ($1, $2, $3, NOW(), NOW(), NOW())
		RETURNING id, workflow_definition_id, status, input_json, output_json, error_text, created_at, updated_at, started_at, completed_at
	`, workflowDefinitionID, domain.ExecutionStatusRunning, normalizeJSON(input))

	execution, err := scanWorkflowExecution(executionRow)
	if err != nil {
		return domain.ExecutionStartResult{}, err
	}

	taskRow := tx.QueryRow(ctx, `
		INSERT INTO task_instances (
			workflow_execution_id,
			task_name,
			handler_key,
			status,
			input_json,
			idempotency_key,
			created_at,
			updated_at
		)
		VALUES ($1, $2, $3, $4, $5, $6, NOW(), NOW())
		RETURNING id, workflow_execution_id, task_name, handler_key, status, input_json, output_json, next_run_at, last_error_text, attempts_total, idempotency_key, dispatched_at, completed_at, created_at, updated_at
	`, execution.ID, taskName, handlerKey, domain.TaskStatusPending, normalizeJSON(input), makeTaskIdempotencyKey(execution.ID, taskName))

	task, err := scanTaskInstance(taskRow)
	if err != nil {
		return domain.ExecutionStartResult{}, err
	}

	payload, err := json.Marshal(domain.DispatchTaskPayload{
		TaskID:      task.ID,
		ExecutionID: execution.ID,
		HandlerKey:  task.HandlerKey,
	})
	if err != nil {
		return domain.ExecutionStartResult{}, err
	}

	if _, err := tx.Exec(ctx, `
		INSERT INTO outbox_events (
			aggregate_type,
			aggregate_id,
			event_type,
			payload_json,
			available_at
		)
		VALUES ($1, $2, $3, $4, NOW())
	`, "task_instance", task.ID, "task.dispatch", payload); err != nil {
		return domain.ExecutionStartResult{}, err
	}

	if err := tx.Commit(ctx); err != nil {
		return domain.ExecutionStartResult{}, err
	}

	return domain.ExecutionStartResult{
		Execution: execution,
		Task:      task,
	}, nil
}

func (s *Store) GetExecutionSnapshot(ctx context.Context, executionID string) (domain.ExecutionSnapshot, error) {
	executionRow := s.pool.QueryRow(ctx, `
		SELECT id, workflow_definition_id, status, input_json, output_json, error_text, created_at, updated_at, started_at, completed_at
		FROM workflow_executions
		WHERE id = $1
	`, executionID)

	execution, err := scanWorkflowExecution(executionRow)
	if err != nil {
		return domain.ExecutionSnapshot{}, err
	}

	rows, err := s.pool.Query(ctx, `
		SELECT id, workflow_execution_id, task_name, handler_key, status, input_json, output_json, next_run_at, last_error_text, attempts_total, idempotency_key, dispatched_at, completed_at, created_at, updated_at
		FROM task_instances
		WHERE workflow_execution_id = $1
		ORDER BY created_at
	`, executionID)
	if err != nil {
		return domain.ExecutionSnapshot{}, err
	}
	defer rows.Close()

	var tasks []domain.TaskInstance
	for rows.Next() {
		task, err := scanTaskInstance(rows)
		if err != nil {
			return domain.ExecutionSnapshot{}, err
		}
		tasks = append(tasks, task)
	}

	if err := rows.Err(); err != nil {
		return domain.ExecutionSnapshot{}, err
	}

	attemptRows, err := s.pool.Query(ctx, `
		SELECT a.id, a.task_instance_id, a.attempt_number, a.status, a.started_at, a.finished_at, a.error_text, a.output_json, a.created_at
		FROM task_attempts a
		JOIN task_instances t ON t.id = a.task_instance_id
		WHERE t.workflow_execution_id = $1
		ORDER BY a.task_instance_id, a.attempt_number
	`, executionID)
	if err != nil {
		return domain.ExecutionSnapshot{}, err
	}
	defer attemptRows.Close()

	attemptsByTask := make(map[string][]domain.TaskAttempt, len(tasks))
	for attemptRows.Next() {
		attempt, err := scanTaskAttempt(attemptRows)
		if err != nil {
			return domain.ExecutionSnapshot{}, err
		}
		attemptsByTask[attempt.TaskInstanceID] = append(attemptsByTask[attempt.TaskInstanceID], attempt)
	}

	if err := attemptRows.Err(); err != nil {
		return domain.ExecutionSnapshot{}, err
	}

	taskSnapshots := make([]domain.TaskSnapshot, 0, len(tasks))
	for _, task := range tasks {
		attempts := attemptsByTask[task.ID]
		if attempts == nil {
			attempts = []domain.TaskAttempt{}
		}

		taskSnapshots = append(taskSnapshots, domain.TaskSnapshot{
			Task:     task,
			Attempts: attempts,
		})
	}

	return domain.ExecutionSnapshot{
		Execution: execution,
		Tasks:     taskSnapshots,
	}, nil
}

func (s *Store) ListDeadLetteredTasks(ctx context.Context, limit int) ([]domain.TaskInstance, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, workflow_execution_id, task_name, handler_key, status, input_json, output_json, next_run_at, last_error_text, attempts_total, idempotency_key, dispatched_at, completed_at, created_at, updated_at
		FROM task_instances
		WHERE status = $1
		ORDER BY updated_at DESC
		LIMIT $2
	`, domain.TaskStatusDeadLettered, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tasks []domain.TaskInstance
	for rows.Next() {
		task, err := scanTaskInstance(rows)
		if err != nil {
			return nil, err
		}
		tasks = append(tasks, task)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return tasks, nil
}

func (s *Store) ReplayDeadLetteredTask(ctx context.Context, taskID string) (domain.TaskInstance, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return domain.TaskInstance{}, err
	}
	defer tx.Rollback(ctx)

	taskRow := tx.QueryRow(ctx, `
		SELECT id, workflow_execution_id, task_name, handler_key, status, input_json, output_json, next_run_at, last_error_text, attempts_total, idempotency_key, dispatched_at, completed_at, created_at, updated_at
		FROM task_instances
		WHERE id = $1
		FOR UPDATE
	`, taskID)

	task, err := scanTaskInstance(taskRow)
	if err != nil {
		return domain.TaskInstance{}, err
	}

	if task.Status != domain.TaskStatusDeadLettered {
		return domain.TaskInstance{}, errTaskNotReplayable
	}

	updatedTaskRow := tx.QueryRow(ctx, `
		UPDATE task_instances
		SET status = $2,
			output_json = NULL,
			next_run_at = NULL,
			last_error_text = NULL,
			completed_at = NULL,
			updated_at = NOW()
		WHERE id = $1
		RETURNING id, workflow_execution_id, task_name, handler_key, status, input_json, output_json, next_run_at, last_error_text, attempts_total, idempotency_key, dispatched_at, completed_at, created_at, updated_at
	`, task.ID, domain.TaskStatusPending)
	updatedTask, err := scanTaskInstance(updatedTaskRow)
	if err != nil {
		return domain.TaskInstance{}, err
	}

	if _, err := tx.Exec(ctx, `
		UPDATE workflow_executions
		SET status = $2,
			error_text = NULL,
			completed_at = NULL,
			updated_at = NOW()
		WHERE id = $1
	`, task.WorkflowExecutionID, domain.ExecutionStatusRunning); err != nil {
		return domain.TaskInstance{}, err
	}

	dispatchPayload, err := json.Marshal(domain.DispatchTaskPayload{
		TaskID:      updatedTask.ID,
		ExecutionID: updatedTask.WorkflowExecutionID,
		HandlerKey:  updatedTask.HandlerKey,
	})
	if err != nil {
		return domain.TaskInstance{}, err
	}

	if _, err := tx.Exec(ctx, `
		INSERT INTO outbox_events (
			aggregate_type,
			aggregate_id,
			event_type,
			payload_json,
			available_at
		)
		VALUES ($1, $2, $3, $4, NOW())
	`, "task_instance", updatedTask.ID, "task.dispatch", dispatchPayload); err != nil {
		return domain.TaskInstance{}, err
	}

	if err := tx.Commit(ctx); err != nil {
		return domain.TaskInstance{}, err
	}

	return updatedTask, nil
}

func (s *Store) StartTaskAttempt(ctx context.Context, taskID string) (domain.TaskInstance, domain.TaskAttempt, bool, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return domain.TaskInstance{}, domain.TaskAttempt{}, false, err
	}
	defer tx.Rollback(ctx)

	taskRow := tx.QueryRow(ctx, `
		SELECT id, workflow_execution_id, task_name, handler_key, status, input_json, output_json, next_run_at, last_error_text, attempts_total, idempotency_key, dispatched_at, completed_at, created_at, updated_at
		FROM task_instances
		WHERE id = $1
		FOR UPDATE
	`, taskID)

	task, err := scanTaskInstance(taskRow)
	if err != nil {
		return domain.TaskInstance{}, domain.TaskAttempt{}, false, err
	}

	if task.Status == domain.TaskStatusSucceeded || task.Status == domain.TaskStatusRunning {
		return task, domain.TaskAttempt{}, true, tx.Commit(ctx)
	}

	attemptNumber := task.AttemptsTotal + 1
	attemptRow := tx.QueryRow(ctx, `
		INSERT INTO task_attempts (
			task_instance_id,
			attempt_number,
			status,
			started_at,
			created_at
		)
		VALUES ($1, $2, $3, NOW(), NOW())
		RETURNING id, task_instance_id, attempt_number, status, started_at, finished_at, error_text, output_json, created_at
	`, task.ID, attemptNumber, domain.TaskAttemptStatusRunning)

	attempt, err := scanTaskAttempt(attemptRow)
	if err != nil {
		return domain.TaskInstance{}, domain.TaskAttempt{}, false, err
	}

	updatedTaskRow := tx.QueryRow(ctx, `
		UPDATE task_instances
		SET status = $2,
			attempts_total = $3,
			next_run_at = NULL,
			updated_at = NOW()
		WHERE id = $1
		RETURNING id, workflow_execution_id, task_name, handler_key, status, input_json, output_json, next_run_at, last_error_text, attempts_total, idempotency_key, dispatched_at, completed_at, created_at, updated_at
	`, task.ID, domain.TaskStatusRunning, attemptNumber)

	updatedTask, err := scanTaskInstance(updatedTaskRow)
	if err != nil {
		return domain.TaskInstance{}, domain.TaskAttempt{}, false, err
	}

	if err := tx.Commit(ctx); err != nil {
		return domain.TaskInstance{}, domain.TaskAttempt{}, false, err
	}

	return updatedTask, attempt, false, nil
}

func (s *Store) CompleteTaskAttempt(ctx context.Context, taskID, attemptID string, output json.RawMessage) error {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	var executionID string
	if err := tx.QueryRow(ctx, `SELECT workflow_execution_id FROM task_instances WHERE id = $1 FOR UPDATE`, taskID).Scan(&executionID); err != nil {
		return err
	}

	payload := normalizeJSON(output)
	if _, err := tx.Exec(ctx, `
		UPDATE task_attempts
		SET status = $2,
			finished_at = NOW(),
			output_json = $3
		WHERE id = $1
	`, attemptID, domain.TaskAttemptStatusSucceeded, payload); err != nil {
		return err
	}

	if _, err := tx.Exec(ctx, `
		UPDATE task_instances
		SET status = $2,
			output_json = $3,
			completed_at = NOW(),
			updated_at = NOW()
		WHERE id = $1
	`, taskID, domain.TaskStatusSucceeded, payload); err != nil {
		return err
	}

	if _, err := tx.Exec(ctx, `
		UPDATE workflow_executions
		SET status = $2,
			output_json = $3,
			completed_at = NOW(),
			updated_at = NOW()
		WHERE id = $1
	`, executionID, domain.ExecutionStatusSucceeded, payload); err != nil {
		return err
	}

	return tx.Commit(ctx)
}
func (s *Store) CompleteTaskAttemptAndEnqueueNextTask(
	ctx context.Context,
	taskID string,
	attemptID string,
	nextTaskName string,
	nextHandlerKey string,
	output json.RawMessage,
) error {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	var executionID string
	if err := tx.QueryRow(
		ctx,
		`SELECT workflow_execution_id FROM task_instances WHERE id = $1 FOR UPDATE`,
		taskID,
	).Scan(&executionID); err != nil {
		return err
	}

	payload := normalizeJSON(output)

	if _, err := tx.Exec(ctx, `
		UPDATE task_attempts
		SET status = $2,
			finished_at = NOW(),
			output_json = $3
		WHERE id = $1
	`, attemptID, domain.TaskAttemptStatusSucceeded, payload); err != nil {
		return err
	}

	if _, err := tx.Exec(ctx, `
		UPDATE task_instances
		SET status = $2,
			output_json = $3,
			completed_at = NOW(),
			updated_at = NOW()
		WHERE id = $1
	`, taskID, domain.TaskStatusSucceeded, payload); err != nil {
		return err
	}

	var nextTaskID string
	if err := tx.QueryRow(ctx, `
		INSERT INTO task_instances (
			workflow_execution_id,
			task_name,
			handler_key,
			status,
			input_json,
			idempotency_key,
			created_at,
			updated_at
		)
		VALUES ($1, $2, $3, $4, $5, $6, NOW(), NOW())
		RETURNING id
	`,
		executionID,
		nextTaskName,
		nextHandlerKey,
		domain.TaskStatusPending,
		payload,
		makeTaskIdempotencyKey(executionID, nextTaskName),
	).Scan(&nextTaskID); err != nil {
		return err
	}

	dispatchPayload, err := json.Marshal(domain.DispatchTaskPayload{
		TaskID:      nextTaskID,
		ExecutionID: executionID,
		HandlerKey:  nextHandlerKey,
	})
	if err != nil {
		return err
	}

	if _, err := tx.Exec(ctx, `
		INSERT INTO outbox_events (
			aggregate_type,
			aggregate_id,
			event_type,
			payload_json,
			available_at
		)
		VALUES ($1, $2, $3, $4, NOW())
	`, "task_instance", nextTaskID, "task.dispatch", dispatchPayload); err != nil {
		return err
	}

	return tx.Commit(ctx)
}
func (s *Store) EnqueueDueTaskRetries(ctx context.Context, limit int) (int, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)

	rows, err := tx.Query(ctx, `
		SELECT id, workflow_execution_id, handler_key
		FROM task_instances
		WHERE status = $1
			AND next_run_at IS NOT NULL
			AND next_run_at <= NOW()
		ORDER BY next_run_at
		LIMIT $2
		FOR UPDATE SKIP LOCKED
	`, domain.TaskStatusPending, limit)

	if err != nil {
		return 0, err
	}
	count := 0
	for rows.Next() {
		var taskID string
		var executionID string
		var handlerKey string
		if err := rows.Scan(&taskID, &executionID, &handlerKey); err != nil {
			rows.Close()
			return count, err
		}
		count++

		payload, err := json.Marshal(domain.DispatchTaskPayload{
			TaskID:      taskID,
			ExecutionID: executionID, // Not needed for dispatching the task, worker will look up execution ID by task ID
			HandlerKey:  handlerKey,  // Not needed for dispatching the task, worker will look up handler key by task ID
		})
		if err != nil {
			rows.Close()
			return count, err
		}

		if _, err := tx.Exec(ctx, `
			INSERT INTO outbox_events (
				aggregate_type,
				aggregate_id,
				event_type,
				payload_json,
				available_at
			)
			VALUES ($1, $2, $3, $4, NOW())
		`, "task_instance", taskID, "task.dispatch", payload); err != nil {
			rows.Close()
			return count, err
		}

		if _, err := tx.Exec(ctx, `
			UPDATE task_instances
			SET next_run_at = NULL,
				updated_at = NOW()
			WHERE id = $1
		`, taskID); err != nil {
			rows.Close()
			return count, err
		}
	}

	if err := rows.Err(); err != nil {
		rows.Close()
		return count, err
	}

	if err := tx.Commit(ctx); err != nil {
		return count, err
	}
	return count, nil
}
func (s *Store) ScheduleTaskRetry(ctx context.Context, taskID, attemptID, errorText string, nextRunAt time.Time) error {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx, `
		UPDATE task_attempts
		SET status = $2,
			finished_at = NOW(),
			error_text = $3
		WHERE id = $1
	`, attemptID, domain.TaskAttemptStatusFailed, truncate(errorText, 1000)); err != nil {
		return err
	}

	if _, err := tx.Exec(ctx, `
		UPDATE task_instances
		SET status = $2,
			last_error_text = $3,
			next_run_at = $4,
			updated_at = NOW()
		WHERE id = $1
	`, taskID, domain.TaskStatusPending, truncate(errorText, 1000), nextRunAt.UTC()); err != nil {
		return err
	}

	return tx.Commit(ctx)
}

func (s *Store) FailTaskAttempt(ctx context.Context, taskID, attemptID, errorText string) error {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	var executionID string
	if err := tx.QueryRow(ctx, `SELECT workflow_execution_id FROM task_instances WHERE id = $1 FOR UPDATE`, taskID).Scan(&executionID); err != nil {
		return err
	}

	if _, err := tx.Exec(ctx, `
		UPDATE task_attempts
		SET status = $2,
			finished_at = NOW(),
			error_text = $3
		WHERE id = $1
	`, attemptID, domain.TaskAttemptStatusFailed, truncate(errorText, 1000)); err != nil {
		return err
	}

	// TODO: When retries are added, this branch should schedule the next run instead of failing terminally.
	if _, err := tx.Exec(ctx, `
		UPDATE task_instances
		SET status = $2,
			last_error_text = $3,
			next_run_at = NULL,
			completed_at = NOW(),
			updated_at = NOW()
		WHERE id = $1
	`, taskID, domain.TaskStatusDeadLettered, truncate(errorText, 1000)); err != nil {
		return err
	}

	if _, err := tx.Exec(ctx, `
		UPDATE workflow_executions
		SET status = $2,
			error_text = $3,
			completed_at = NOW(),
			updated_at = NOW()
		WHERE id = $1
	`, executionID, domain.ExecutionStatusFailed, truncate(errorText, 1000)); err != nil {
		return err
	}

	return tx.Commit(ctx)
}

func (s *Store) ListPendingOutbox(ctx context.Context, limit int) ([]domain.OutboxEvent, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, aggregate_type, aggregate_id, event_type, payload_json, available_at, dispatched_at, attempt_count, last_error_text, created_at
		FROM outbox_events
		WHERE dispatched_at IS NULL
			AND available_at <= NOW()
		ORDER BY created_at
		LIMIT $1
	`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var events []domain.OutboxEvent
	for rows.Next() {
		event, err := scanOutboxEvent(rows)
		if err != nil {
			return nil, err
		}
		events = append(events, event)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return events, nil
}

func (s *Store) MarkOutboxDispatched(ctx context.Context, id string) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE outbox_events
		SET dispatched_at = NOW(),
			attempt_count = attempt_count + 1
		WHERE id = $1
	`, id)
	return err
}

func (s *Store) RecordOutboxFailure(ctx context.Context, id string, dispatchErr error) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE outbox_events
		SET attempt_count = attempt_count + 1,
			last_error_text = $2
		WHERE id = $1
	`, id, truncate(dispatchErr.Error(), 1000))
	return err
}

func normalizeJSON(raw json.RawMessage) []byte {
	if len(raw) == 0 {
		return []byte(`{}`)
	}
	return raw
}

func truncate(input string, limit int) string {
	if len(input) <= limit {
		return input
	}
	return input[:limit]
}

func stringValue(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

func scanWorkflowDefinition(row rowScanner) (domain.WorkflowDefinition, error) {
	var definition []byte
	item := domain.WorkflowDefinition{}
	err := row.Scan(
		&item.ID,
		&item.Name,
		&item.Description,
		&item.Version,
		&item.Status,
		&definition,
		&item.CreatedAt,
		&item.UpdatedAt,
	)
	item.DefinitionJSON = definition
	return item, err
}

func scanWorkflowExecution(row rowScanner) (domain.WorkflowExecution, error) {
	item := domain.WorkflowExecution{}
	var inputJSON []byte
	var outputJSON []byte
	var errorText *string
	err := row.Scan(
		&item.ID,
		&item.WorkflowDefinitionID,
		&item.Status,
		&inputJSON,
		&outputJSON,
		&errorText,
		&item.CreatedAt,
		&item.UpdatedAt,
		&item.StartedAt,
		&item.CompletedAt,
	)
	item.InputJSON = inputJSON
	item.OutputJSON = outputJSON
	item.ErrorText = stringValue(errorText)
	return item, err
}

func scanTaskInstance(row rowScanner) (domain.TaskInstance, error) {
	item := domain.TaskInstance{}
	var inputJSON []byte
	var outputJSON []byte
	var lastError *string
	err := row.Scan(
		&item.ID,
		&item.WorkflowExecutionID,
		&item.TaskName,
		&item.HandlerKey,
		&item.Status,
		&inputJSON,
		&outputJSON,
		&item.NextRunAt,
		&lastError,
		&item.AttemptsTotal,
		&item.IdempotencyKey,
		&item.DispatchedAt,
		&item.CompletedAt,
		&item.CreatedAt,
		&item.UpdatedAt,
	)
	item.InputJSON = inputJSON
	item.OutputJSON = outputJSON
	item.LastErrorText = stringValue(lastError)
	return item, err
}

func scanTaskAttempt(row rowScanner) (domain.TaskAttempt, error) {
	item := domain.TaskAttempt{}
	var outputJSON []byte
	var errorText *string
	err := row.Scan(
		&item.ID,
		&item.TaskInstanceID,
		&item.AttemptNumber,
		&item.Status,
		&item.StartedAt,
		&item.FinishedAt,
		&errorText,
		&outputJSON,
		&item.CreatedAt,
	)
	item.ErrorText = stringValue(errorText)
	item.OutputJSON = outputJSON
	return item, err
}

func scanOutboxEvent(row rowScanner) (domain.OutboxEvent, error) {
	item := domain.OutboxEvent{}
	var payloadJSON []byte
	var lastError *string
	err := row.Scan(
		&item.ID,
		&item.AggregateType,
		&item.AggregateID,
		&item.EventType,
		&payloadJSON,
		&item.AvailableAt,
		&item.DispatchedAt,
		&item.AttemptCount,
		&lastError,
		&item.CreatedAt,
	)
	item.PayloadJSON = payloadJSON
	item.LastErrorText = stringValue(lastError)
	return item, err
}

func IsNotFound(err error) bool {
	return errors.Is(err, pgx.ErrNoRows)
}

func IsUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}

func IsTaskNotReplayable(err error) bool {
	return errors.Is(err, errTaskNotReplayable)
}
