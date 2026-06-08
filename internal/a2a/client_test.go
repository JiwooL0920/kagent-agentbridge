package a2a

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
)

func TestSendTask_Success(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			t.Errorf("expected POST, got %s", r.Method)
		}
		if r.URL.Path != "/agents/test-agent/" {
			t.Errorf("expected /agents/test-agent/, got %s", r.URL.Path)
		}
		if r.Header.Get("Content-Type") != "application/json" {
			t.Errorf("expected Content-Type: application/json")
		}
		if r.Header.Get("Authorization") != "Bearer test-token" {
			t.Errorf("expected Authorization: Bearer test-token")
		}

		body, _ := io.ReadAll(r.Body)
		var req TaskSendRequest
		if err := json.Unmarshal(body, &req); err != nil {
			t.Errorf("failed to unmarshal request: %v", err)
		}

		if req.JSONRPC != "2.0" {
			t.Errorf("expected jsonrpc 2.0, got %s", req.JSONRPC)
		}
		if req.Method != "message/send" {
			t.Errorf("expected method message/send, got %s", req.Method)
		}
		if req.ID != "req-123" {
			t.Errorf("expected id req-123, got %s", req.ID)
		}
		if req.Params.Message.MessageID != "req-123" {
			t.Errorf("expected messageId req-123, got %s", req.Params.Message.MessageID)
		}
		if req.Params.Message.Role != "user" {
			t.Errorf("expected role user, got %s", req.Params.Message.Role)
		}
		if len(req.Params.Message.Parts) != 1 || req.Params.Message.Parts[0].Text != "test task" {
			t.Errorf("expected message parts with 'test task'")
		}

		resp := TaskSendResponse{
			JSONRPC: "2.0",
			ID:      "req-123",
			Result: &TaskSendResult{
				TaskID:    "task-456",
				ContextID: "ctx-789",
				State:     "pending",
			},
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	client := NewClient(server.URL, "test-token", logger)

	taskID, contextID, state, err := client.SendTask(context.Background(), "test-agent", "req-123", "test task")
	if err != nil {
		t.Fatalf("SendTask failed: %v", err)
	}

	if taskID != "task-456" {
		t.Errorf("expected taskID task-456, got %s", taskID)
	}
	if contextID != "ctx-789" {
		t.Errorf("expected contextID ctx-789, got %s", contextID)
	}
	if state != "pending" {
		t.Errorf("expected state pending, got %s", state)
	}
}

func TestSendTask_NonOKStatus(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("server error"))
	}))
	defer server.Close()

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	client := NewClient(server.URL, "test-token", logger)

	_, _, _, err := client.SendTask(context.Background(), "test-agent", "req-123", "test task")
	if err == nil {
		t.Fatal("expected error for non-200 status, got nil")
	}
	if err.Error() == "" {
		t.Error("expected non-empty error message")
	}
}

func TestSendTask_A2AError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resp := TaskSendResponse{
			JSONRPC: "2.0",
			ID:      "req-123",
			Error: &TaskSendErrorDetail{
				Code:    -32600,
				Message: "Invalid Request",
			},
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	client := NewClient(server.URL, "test-token", logger)

	_, _, _, err := client.SendTask(context.Background(), "test-agent", "req-123", "test task")
	if err == nil {
		t.Fatal("expected error for A2A error response, got nil")
	}
	if err.Error() == "" {
		t.Error("expected non-empty error message")
	}
}

func TestSendTask_NoResult(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resp := TaskSendResponse{
			JSONRPC: "2.0",
			ID:      "req-123",
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	client := NewClient(server.URL, "test-token", logger)

	_, _, _, err := client.SendTask(context.Background(), "test-agent", "req-123", "test task")
	if err == nil {
		t.Fatal("expected error for no result, got nil")
	}
}
