package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"strings"

	"durableflow/internal/db"
	"durableflow/internal/orchestrator"
	"durableflow/internal/telemetry"
)

type Router struct {
	logger  *slog.Logger
	service *orchestrator.Service
}

type createWorkflowRequest struct {
	Name        string          `json:"name"`
	Description string          `json:"description"`
	Definition  json.RawMessage `json:"definition"`
}

type triggerExecutionRequest struct {
	WorkflowDefinitionID string          `json:"workflow_definition_id"`
	Input                json.RawMessage `json:"input"`
}

func NewRouter(logger *slog.Logger, service *orchestrator.Service, healthFn func(context.Context) error) http.Handler {
	router := &Router{
		logger:  logger,
		service: service,
	}

	mux := http.NewServeMux()
	RegisterHealthRoutes(mux, "api", healthFn)
	mux.HandleFunc("/api/workflows", router.handleWorkflows)
	mux.HandleFunc("/api/executions", router.handleExecutions)
	mux.HandleFunc("/api/executions/", router.handleExecutionSnapshot)
	mux.HandleFunc("/api/tasks/", router.handleTaskActions)
	mux.HandleFunc("/api/dead-letter-tasks", router.handleDeadLetteredTasks)
	mux.HandleFunc("/api/dead-lettered-tasks", router.handleDeadLetteredTasks)

	return cors(mux)
}

func (rt *Router) handleWorkflows(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req createWorkflowRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid JSON body"})
		return
	}

	result, err := rt.service.CreateWorkflowDefinition(r.Context(), orchestrator.CreateWorkflowDefinitionRequest{
		Name:        req.Name,
		Description: req.Description,
		Definition:  req.Definition,
	})
	if err != nil {
		rt.logger.Error("create workflow failed", "error", err)
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}

	telemetry.WorkflowDefinitionsCreated.WithLabelValues("api").Inc()
	writeJSON(w, http.StatusCreated, result)
}

func (rt *Router) handleExecutions(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req triggerExecutionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid JSON body"})
		return
	}

	result, err := rt.service.TriggerExecution(r.Context(), orchestrator.TriggerExecutionRequest{
		WorkflowDefinitionID: req.WorkflowDefinitionID,
		Input:                req.Input,
	})
	if err != nil {
		if db.IsNotFound(err) {
			rt.logger.Error("trigger execution failed", "error", err)
			writeJSON(w, http.StatusNotFound, map[string]any{"error": "workflow definition not found"})
			return
		}

		rt.logger.Error("trigger execution failed", "error", err)
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}

	telemetry.WorkflowExecutionsCreated.WithLabelValues("api").Inc()
	writeJSON(w, http.StatusAccepted, result)
}

func (rt *Router) handleExecutionSnapshot(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	executionID := strings.TrimPrefix(r.URL.Path, "/api/executions/")
	if executionID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "execution id is required"})
		return
	}

	snapshot, err := rt.service.GetExecutionSnapshot(r.Context(), executionID)
	if err != nil {
		if db.IsNotFound(err) {
			writeJSON(w, http.StatusNotFound, map[string]any{"error": "execution not found"})
			return
		}

		rt.logger.Error("get execution snapshot failed", "error", err)
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": "failed to load execution"})
		return
	}

	writeJSON(w, http.StatusOK, snapshot)
}

func (rt *Router) handleDeadLetteredTasks(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	limit, err := parsePositiveIntQuery(r, "limit")
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}

	tasks, err := rt.service.GetDeadLetteredTasks(r.Context(), limit)
	if err != nil {
		rt.logger.Error("get dead lettered tasks failed", "error", err)
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": "failed to load dead lettered tasks"})
		return
	}

	writeJSON(w, http.StatusOK, tasks)
}

func (rt *Router) handleTaskActions(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	taskID, action, ok := parseTaskActionPath(r.URL.Path)
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]any{"error": "task action not found"})
		return
	}

	switch action {
	case "replay":
		task, err := rt.service.ReplayDeadLetteredTask(r.Context(), taskID)
		if err != nil {
			switch {
			case db.IsNotFound(err):
				writeJSON(w, http.StatusNotFound, map[string]any{"error": "task not found"})
			case db.IsTaskNotReplayable(err):
				writeJSON(w, http.StatusBadRequest, map[string]any{"error": "only dead-lettered tasks can be replayed"})
			default:
				rt.logger.Error("replay dead lettered task failed", "task_id", taskID, "error", err)
				writeJSON(w, http.StatusInternalServerError, map[string]any{"error": "failed to replay task"})
			}
			return
		}

		writeJSON(w, http.StatusAccepted, task)
	default:
		writeJSON(w, http.StatusNotFound, map[string]any{"error": "task action not found"})
	}
}

func parsePositiveIntQuery(r *http.Request, key string) (int, error) {
	raw := strings.TrimSpace(r.URL.Query().Get(key))
	if raw == "" {
		return 0, nil
	}

	value, err := strconv.Atoi(raw)
	if err != nil {
		return 0, errors.New(key + " must be a valid integer")
	}
	if value <= 0 {
		return 0, errors.New(key + " must be greater than 0")
	}

	return value, nil
}

func parseTaskActionPath(path string) (taskID string, action string, ok bool) {
	trimmed := strings.TrimPrefix(path, "/api/tasks/")
	parts := strings.Split(strings.Trim(trimmed, "/"), "/")
	if len(parts) != 2 || strings.TrimSpace(parts[0]) == "" || strings.TrimSpace(parts[1]) == "" {
		return "", "", false
	}

	return parts[0], parts[1], true
}

func cors(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		w.Header().Set("Access-Control-Allow-Methods", "GET,POST,OPTIONS")

		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}

		next.ServeHTTP(w, r)
	})
}
