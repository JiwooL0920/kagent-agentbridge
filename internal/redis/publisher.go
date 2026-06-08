package redis

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
)

type TaskSender interface {
	SendTask(ctx context.Context, agent, requestID, text string) (string, string, string, error)
}

type StreamPublisher struct {
	conn   *Conn
	stream string
	logger *slog.Logger
}

func NewStreamPublisher(conn *Conn, stream string, logger *slog.Logger) *StreamPublisher {
	return &StreamPublisher{
		conn:   conn,
		stream: stream,
		logger: logger,
	}
}

func (p *StreamPublisher) SendTask(ctx context.Context, agent, requestID, text string) (string, string, string, error) {
	artifactKey := fmt.Sprintf("incident:artifact:%s:alert", requestID)
	if err := p.conn.Set(artifactKey, text, 86400); err != nil {
		return "", "", "", fmt.Errorf("store artifact: %w", err)
	}

	fields := map[string]string{
		"workflow_id":  requestID,
		"agent_target": agent,
		"artifact_key": artifactKey,
		"step":         "investigation",
		"status":       "pending",
		"timestamp":    time.Now().UTC().Format(time.RFC3339),
	}

	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	if traceparent := carrier.Get("traceparent"); traceparent != "" {
		fields["traceparent"] = traceparent
	}
	if tracestate := carrier.Get("tracestate"); tracestate != "" {
		fields["tracestate"] = tracestate
	}

	id, err := p.conn.XAdd(p.stream, 1000, fields)
	if err != nil {
		return "", "", "", fmt.Errorf("publish to stream: %w", err)
	}

	p.logger.Info("task published to stream",
		"stream", p.stream,
		"agent", agent,
		"request_id", requestID,
		"stream_id", id)

	return id, "", "published", nil
}
