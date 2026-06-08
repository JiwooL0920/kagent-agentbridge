package redis

import (
	"context"
	"fmt"
	"log/slog"
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
	artifactKey := fmt.Sprintf("artifact:%s", requestID)
	if err := p.conn.Set(artifactKey, text, 86400); err != nil {
		return "", "", "", fmt.Errorf("store artifact: %w", err)
	}

	fields := map[string]string{
		"agent":      agent,
		"request_id": requestID,
		"text":       text,
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
