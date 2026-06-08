package alertmanagerhook

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/jiwoolee/kagent-agentbridge/internal/httpapi"
)

type fakeSender struct {
	mu    sync.Mutex
	calls []sendCall
}

type sendCall struct {
	agent     string
	requestID string
	text      string
}

func (f *fakeSender) SendTask(_ context.Context, agent, requestID, text string) (string, string, string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, sendCall{agent: agent, requestID: requestID, text: text})
	return "id", "ctx", "queued", nil
}

func (f *fakeSender) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

func TestHandler_ValidPayloadAcceptedAndSent(t *testing.T) {
	sender := &fakeSender{}
	h := NewHandler(sender, Options{
		TargetAgent:       "triage-agent",
		AllowedSeverities: []string{"critical"},
		IncludeResolved:   false,
		Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
	})

	body := `{"status":"firing","alerts":[{"status":"firing","labels":{"alertname":"HighCPU","severity":"critical","namespace":"default","cluster":"dev"},"annotations":{"summary":"High CPU usage","description":"cpu over threshold"},"startsAt":"2026-06-08T00:00:00Z","endsAt":"0001-01-01T00:00:00Z"}],"groupKey":"abc"}`
	req := httptest.NewRequest(http.MethodPost, "/webhook/alertmanager", bytes.NewBufferString(body))
	rec := httptest.NewRecorder()

	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected status 202, got %d", rec.Code)
	}

	deadline := time.Now().Add(200 * time.Millisecond)
	for time.Now().Before(deadline) {
		if sender.callCount() == 1 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}

	t.Fatalf("expected one SendTask call, got %d", sender.callCount())
}

func TestHandler_SeverityFilteredNoSend(t *testing.T) {
	sender := &fakeSender{}
	h := NewHandler(sender, Options{
		TargetAgent:       "triage-agent",
		AllowedSeverities: []string{"critical"},
		IncludeResolved:   false,
		Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
	})

	body := `{"status":"firing","alerts":[{"status":"firing","labels":{"alertname":"HighCPU","severity":"warning","namespace":"default","cluster":"dev"},"annotations":{"summary":"warn","description":"warn"},"startsAt":"2026-06-08T00:00:00Z","endsAt":"0001-01-01T00:00:00Z"}],"groupKey":"abc"}`
	req := httptest.NewRequest(http.MethodPost, "/webhook/alertmanager", bytes.NewBufferString(body))
	rec := httptest.NewRecorder()

	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected status 202, got %d", rec.Code)
	}

	time.Sleep(30 * time.Millisecond)
	if sender.callCount() != 0 {
		t.Fatalf("expected zero SendTask calls, got %d", sender.callCount())
	}
}

func TestHandler_InvalidJSONReturnsBadRequest(t *testing.T) {
	sender := &fakeSender{}
	h := NewHandler(sender, Options{
		TargetAgent:       "triage-agent",
		AllowedSeverities: []string{"critical"},
		IncludeResolved:   false,
		Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
	})

	req := httptest.NewRequest(http.MethodPost, "/webhook/alertmanager", bytes.NewBufferString("{"))
	rec := httptest.NewRecorder()

	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected status 400, got %d", rec.Code)
	}
}

func TestHealthzEndpointReturnsOK(t *testing.T) {
	mux := httpapi.NewMux(nil)
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()

	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}
}
