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
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
)

var tracer = otel.Tracer("github.com/jiwoolee/kagent-agentbridge/alertmanager-hook")

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
	ctx, span := tracer.Start(r.Context(), "alertmanager.webhook.received")
	defer span.End()

	if r.URL.Path != "/webhook/alertmanager" {
		span.SetAttributes(attribute.String("http.route", r.URL.Path))
		span.SetStatus(codes.Error, "not found")
		http.NotFound(w, r)
		return
	}

	if r.Method != http.MethodPost {
		span.SetStatus(codes.Error, "method not allowed")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var payload WebhookPayload
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "invalid JSON payload")
		httpjson.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON payload"})
		return
	}

	span.SetAttributes(attribute.Int("alert.count", len(payload.Alerts)))

	for i, alert := range payload.Alerts {
		if !h.shouldForward(alert) {
			continue
		}

		requestID := fmt.Sprintf("amhook-%d-%d", time.Now().UnixNano(), i)
		text := FormatAlertMessage(alert)

		go func(parent context.Context, reqID, message string) {
			_, _, _, err := h.sender.SendTask(parent, h.targetAgent, reqID, message)
			if err != nil {
				h.logger.Error("failed to dispatch alert", "request_id", reqID, "error", err)
			}
		}(ctx, requestID, text)
	}

	span.SetStatus(codes.Ok, "accepted")
	httpjson.WriteJSON(w, http.StatusAccepted, map[string]string{"status": "accepted"})
}

func (h *Handler) shouldForward(alert Alert) bool {
	if !h.includeResolved && strings.EqualFold(alert.Status, "resolved") {
		return false
	}
	return SeverityFilter(alert, h.allowedSeverities)
}
