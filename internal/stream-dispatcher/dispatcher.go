package streamdispatcher

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jiwoolee/kagent-agentbridge/internal/a2a"
	"github.com/jiwoolee/kagent-agentbridge/internal/redis"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

const dlqMaxLen = 1000

var tracer = otel.Tracer("github.com/jiwoolee/kagent-agentbridge/stream-dispatcher")

type Dispatcher struct {
	cfg      Config
	a2a      *a2a.Client
	conn     *redis.Conn
	router   *Router
	logger   *slog.Logger
	inFlight atomic.Int64
}

func New(cfg Config) (*Dispatcher, error) {
	conn, err := redis.DialSentinel(context.Background(), redis.SentinelConfig{
		SentinelAddrs: []string{fmt.Sprintf("%s:%d", cfg.RedisSentinelHost, cfg.RedisSentinelPort)},
		MasterName:    cfg.RedisSentinelMasterName,
		DB:            cfg.RedisDB,
		DialTimeout:   5 * time.Second,
	})
	if err != nil {
		return nil, fmt.Errorf("connect redis sentinel: %w", err)
	}

	logger := slog.Default().With("component", "stream-dispatcher")
	d := &Dispatcher{
		cfg:    cfg,
		conn:   conn,
		a2a:    a2a.NewClient(cfg.KagentA2AURL, cfg.InternalA2ABearerToken, logger),
		router: NewRouter(conn, cfg.RoutingKey),
		logger: logger,
	}

	return d, nil
}

func (d *Dispatcher) Close() error {
	if d.conn != nil {
		return d.conn.Close()
	}
	return nil
}

func (d *Dispatcher) Run(ctx context.Context) error {
	if err := d.ensureConsumerGroup(); err != nil {
		return err
	}

	if err := d.processPending(ctx); err != nil {
		return err
	}

	return d.consumeLoop(ctx)
}

func (d *Dispatcher) ensureConsumerGroup() error {
	err := d.conn.XGroupCreateMkStream(d.cfg.StreamName, d.cfg.ConsumerGroup, "$")
	if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
		return fmt.Errorf("ensure consumer group: %w", err)
	}
	return nil
}

func (d *Dispatcher) processPending(ctx context.Context) error {
	start := "0-0"
	for {
		if ctx.Err() != nil {
			return nil
		}

		messages, next, err := d.conn.XAutoClaim(
			d.cfg.StreamName,
			d.cfg.ConsumerGroup,
			d.cfg.ConsumerName,
			0,
			start,
			100,
		)
		if err != nil {
			return fmt.Errorf("xautoclaim pending messages: %w", err)
		}

		if len(messages) == 0 {
			if next == start || next == "0-0" {
				return nil
			}
			start = next
			continue
		}

		d.dispatchBatch(ctx, messages)
		start = next
	}
}

func (d *Dispatcher) consumeLoop(ctx context.Context) error {
	for {
		if ctx.Err() != nil {
			return nil
		}

		count := max(1, d.cfg.Workers-int(d.inFlight.Load()))

		messages, err := d.conn.XReadGroup(
			d.cfg.ConsumerGroup,
			d.cfg.ConsumerName,
			d.cfg.StreamName,
			">",
			count,
			d.cfg.BlockMS,
		)
		if err != nil {
			d.logger.Error("xreadgroup failed", "error", err)
			time.Sleep(500 * time.Millisecond)
			continue
		}

		if len(messages) == 0 {
			continue
		}

		d.dispatchBatch(ctx, messages)
	}
}

func (d *Dispatcher) dispatchBatch(ctx context.Context, messages []redis.StreamMessage) {
	sem := make(chan struct{}, d.cfg.Workers)
	var wg sync.WaitGroup

	for _, msg := range messages {
		if ctx.Err() != nil {
			return
		}

		sem <- struct{}{}
		wg.Add(1)
		d.inFlight.Add(1)

		go func(m redis.StreamMessage) {
			defer func() {
				d.inFlight.Add(-1)
				<-sem
				wg.Done()
			}()

			d.processMessage(ctx, m)
		}(msg)
	}

	wg.Wait()
}

func (d *Dispatcher) processMessage(ctx context.Context, msg redis.StreamMessage) {
	carrier := propagation.MapCarrier(msg.Fields)
	ctx = otel.GetTextMapPropagator().Extract(ctx, carrier)
	ctx, span := tracer.Start(ctx, "redis.incident-events.process", trace.WithSpanKind(trace.SpanKindConsumer))
	defer span.End()
	span.SetAttributes(
		attribute.String("messaging.system", "redis"),
		attribute.String("messaging.operation", "process"),
		attribute.String("messaging.message.id", msg.ID),
		attribute.String("workflow.id", msg.Fields["workflow_id"]),
		attribute.String("workflow.step", msg.Fields["step"]),
		attribute.String("workflow.status", msg.Fields["status"]),
		attribute.String("workflow.timestamp", msg.Fields["timestamp"]),
	)

	agent := msg.Fields["agent_target"]
	if agent == "" {
		step := msg.Fields["step"]
		if step == "" {
			span.SetStatus(codes.Error, "missing step and agent_target")
			d.moveToDLQ(msg, "missing step and agent_target")
			return
		}

		resolved, err := d.router.Resolve(ctx, step)
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			d.moveToDLQ(msg, err.Error())
			return
		}
		agent = resolved
	}
	span.SetAttributes(attribute.String("agent.target", agent))

	artifactKey := msg.Fields["artifact_key"]
	if artifactKey == "" {
		span.SetStatus(codes.Error, "missing artifact_key")
		d.moveToDLQ(msg, "missing artifact_key")
		return
	}

	artifact, err := d.conn.Get(artifactKey)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "artifact lookup failed")
		d.moveToDLQ(msg, fmt.Sprintf("get artifact %q: %v", artifactKey, err))
		return
	}
	if artifact == "" {
		span.SetStatus(codes.Error, "artifact not found")
		d.moveToDLQ(msg, fmt.Sprintf("artifact not found: %s", artifactKey))
		return
	}

	workflowID := msg.Fields["workflow_id"]
	step := msg.Fields["step"]
	if workflowID == "" || step == "" {
		span.SetStatus(codes.Error, "missing workflow_id or step")
		d.moveToDLQ(msg, "missing workflow_id or step")
		return
	}

	span.SetAttributes(
		attribute.String("artifact.key", artifactKey),
		attribute.Int("artifact.size_bytes", len(artifact)),
		attribute.String("artifact.preview", truncateStr(artifact, 300)),
	)

	requestID := workflowID + "-" + step
	ctx, a2aSpan := tracer.Start(ctx, "a2a.dispatch",
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			attribute.String("a2a.agent", agent),
			attribute.String("a2a.request_id", requestID),
			attribute.String("a2a.method", "message/send"),
		),
	)
	taskID, contextID, state, err := d.a2a.SendTask(ctx, agent, requestID, artifact)
	if err != nil {
		a2aSpan.RecordError(err)
		a2aSpan.SetStatus(codes.Error, "send task failed")
		a2aSpan.End()
		span.RecordError(err)
		span.SetStatus(codes.Error, "send task failed")
		d.moveToDLQ(msg, fmt.Sprintf("send task to agent %q: %v", agent, err))
		return
	}
	a2aSpan.SetAttributes(
		attribute.String("a2a.task_id", taskID),
		attribute.String("a2a.context_id", contextID),
		attribute.String("a2a.state", state),
	)
	a2aSpan.SetStatus(codes.Ok, "dispatched")
	a2aSpan.End()

	if err := d.conn.XAck(d.cfg.StreamName, d.cfg.ConsumerGroup, msg.ID); err != nil {
		span.RecordError(err)
		d.logger.Error("xack failed", "messageID", msg.ID, "error", err)
	}

	span.SetStatus(codes.Ok, "processed")
}

func (d *Dispatcher) moveToDLQ(msg redis.StreamMessage, reason string) {
	fields := make(map[string]string, len(msg.Fields)+3)
	maps.Copy(fields, msg.Fields)

	fields["error"] = reason
	fields["original_id"] = msg.ID
	fields["failed_at"] = strconv.FormatInt(time.Now().Unix(), 10)

	if _, err := d.conn.XAdd(d.cfg.DLQStream, dlqMaxLen, fields); err != nil {
		d.logger.Error("failed to write dlq", "messageID", msg.ID, "error", err)
		return
	}

	if err := d.conn.XAck(d.cfg.StreamName, d.cfg.ConsumerGroup, msg.ID); err != nil {
		d.logger.Error("failed to ack original after dlq", "messageID", msg.ID, "error", err)
	}

	d.logger.Warn("message moved to dlq", "messageID", msg.ID, "reason", reason)
}


func truncateStr(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "..."
}
