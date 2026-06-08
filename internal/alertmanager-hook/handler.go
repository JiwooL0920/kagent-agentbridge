package alertmanagerhook

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/jiwoolee/kagent-agentbridge/internal/httpjson"
	"github.com/jiwoolee/kagent-agentbridge/internal/redis"
)

type Handler struct {
	sender            redis.TaskSender
	targetAgent       string
	allowedSeverities []string
	includeResolved   bool
	logger            *slog.Logger
}

func NewHandler(sender redis.TaskSender, opts Options) *Handler {
	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}

	return &Handler{
		sender:            sender,
		targetAgent:       opts.TargetAgent,
		allowedSeverities: opts.AllowedSeverities,
		includeResolved:   opts.IncludeResolved,
		logger:            logger,
	}
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/webhook/alertmanager" {
		http.NotFound(w, r)
		return
	}

	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var payload WebhookPayload
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		httpjson.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON payload"})
		return
	}

	for i, alert := range payload.Alerts {
		if !h.shouldForward(alert) {
			continue
		}

		requestID := fmt.Sprintf("amhook-%d-%d", time.Now().UnixNano(), i)
		text := FormatAlertMessage(alert)

		go func(reqID, message string) {
			_, _, _, err := h.sender.SendTask(context.Background(), h.targetAgent, reqID, message)
			if err != nil {
				h.logger.Error("failed to dispatch alert", "request_id", reqID, "error", err)
			}
		}(requestID, text)
	}

	httpjson.WriteJSON(w, http.StatusAccepted, map[string]string{"status": "accepted"})
}

func (h *Handler) shouldForward(alert Alert) bool {
	if !h.includeResolved && strings.EqualFold(alert.Status, "resolved") {
		return false
	}
	return SeverityFilter(alert, h.allowedSeverities)
}
