package redis

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

var publisherTracer = otel.Tracer("redis-publisher")

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
	ctx, span := publisherTracer.Start(ctx, "redis.stream.publish",
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			attribute.String("messaging.system", "redis"),
			attribute.String("messaging.destination.name", p.stream),
			attribute.String("messaging.operation", "publish"),
			attribute.String("workflow.id", requestID),
			attribute.String("agent.target", agent),
			attribute.String("alert.text_preview", truncate(text, 200)),
		),
	)
	defer span.End()

	artifactKey := fmt.Sprintf("incident:artifact:%s:alert", requestID)
	if err := p.conn.Set(artifactKey, text, 86400); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "artifact store failed")
		return "", "", "", fmt.Errorf("store artifact: %w", err)
	}
	span.SetAttributes(attribute.String("artifact.key", artifactKey))
	span.SetAttributes(attribute.Int("artifact.size_bytes", len(text)))

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
		span.RecordError(err)
		span.SetStatus(codes.Error, "XADD failed")
		return "", "", "", fmt.Errorf("publish to stream: %w", err)
	}

	span.SetAttributes(attribute.String("messaging.message.id", id))
	span.SetStatus(codes.Ok, "published")

	p.logger.Info("task published to stream",
		"stream", p.stream,
		"agent", agent,
		"request_id", requestID,
		"stream_id", id)

	return id, "", "published", nil
}

func truncate(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "..."
}
