package httpapi

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func RegisterHealthRoutes(mux *http.ServeMux, service string, healthFn func(context.Context) error) {
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		checkCtx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()

		if err := healthFn(checkCtx); err != nil {
			writeJSON(w, http.StatusServiceUnavailable, map[string]any{
				"service": service,
				"status":  "unhealthy",
				"error":   err.Error(),
			})
			return
		}

		writeJSON(w, http.StatusOK, map[string]any{
			"service": service,
			"status":  "ok",
		})
	})
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

