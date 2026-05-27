package httpapi

import (
	"net/http/httptest"
	"testing"
)

func TestParsePositiveIntQuery(t *testing.T) {
	tests := []struct {
		name    string
		rawURL  string
		want    int
		wantErr bool
	}{
		{name: "missing value uses default path", rawURL: "/api/dead-letter-tasks", want: 0},
		{name: "valid value", rawURL: "/api/dead-letter-tasks?limit=25", want: 25},
		{name: "non-numeric value", rawURL: "/api/dead-letter-tasks?limit=abc", wantErr: true},
		{name: "non-positive value", rawURL: "/api/dead-letter-tasks?limit=0", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest("GET", tt.rawURL, nil)
			got, err := parsePositiveIntQuery(req, "limit")
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("expected no error, got %v", err)
			}
			if got != tt.want {
				t.Fatalf("expected %d, got %d", tt.want, got)
			}
		})
	}
}

func TestParseTaskActionPath(t *testing.T) {
	tests := []struct {
		name   string
		path   string
		taskID string
		action string
		ok     bool
	}{
		{
			name:   "valid replay path",
			path:   "/api/tasks/task-123/replay",
			taskID: "task-123",
			action: "replay",
			ok:     true,
		},
		{
			name: "missing action",
			path: "/api/tasks/task-123",
			ok:   false,
		},
		{
			name: "missing task id",
			path: "/api/tasks//replay",
			ok:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			taskID, action, ok := parseTaskActionPath(tt.path)
			if ok != tt.ok {
				t.Fatalf("expected ok=%t, got %t", tt.ok, ok)
			}
			if taskID != tt.taskID {
				t.Fatalf("expected taskID %q, got %q", tt.taskID, taskID)
			}
			if action != tt.action {
				t.Fatalf("expected action %q, got %q", tt.action, action)
			}
		})
	}
}
