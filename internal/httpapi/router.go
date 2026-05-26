package httpapi

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
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
