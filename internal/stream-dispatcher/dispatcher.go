package streamdispatcher

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jiwoolee/kagent-agentbridge/internal/a2a"
	"github.com/jiwoolee/kagent-agentbridge/internal/redis"
)

const dlqMaxLen = 1000

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
	agent := msg.Fields["agent_target"]
	if agent == "" {
		step := msg.Fields["step"]
		if step == "" {
			d.moveToDLQ(msg, "missing step and agent_target")
			return
		}

		resolved, err := d.router.Resolve(ctx, step)
		if err != nil {
			d.moveToDLQ(msg, err.Error())
			return
		}
		agent = resolved
	}

	artifactKey := msg.Fields["artifact_key"]
	if artifactKey == "" {
		d.moveToDLQ(msg, "missing artifact_key")
		return
	}

	artifact, err := d.conn.Get(artifactKey)
	if err != nil {
		d.moveToDLQ(msg, fmt.Sprintf("get artifact %q: %v", artifactKey, err))
		return
	}
	if artifact == "" {
		d.moveToDLQ(msg, fmt.Sprintf("artifact not found: %s", artifactKey))
		return
	}

	workflowID := msg.Fields["workflow_id"]
	step := msg.Fields["step"]
	if workflowID == "" || step == "" {
		d.moveToDLQ(msg, "missing workflow_id or step")
		return
	}

	requestID := workflowID + "-" + step
	if _, _, _, err := d.a2a.SendTask(ctx, agent, requestID, artifact); err != nil {
		d.moveToDLQ(msg, fmt.Sprintf("send task to agent %q: %v", agent, err))
		return
	}

	if err := d.conn.XAck(d.cfg.StreamName, d.cfg.ConsumerGroup, msg.ID); err != nil {
		d.logger.Error("xack failed", "messageID", msg.ID, "error", err)
	}
}

func (d *Dispatcher) moveToDLQ(msg redis.StreamMessage, reason string) {
	fields := make(map[string]string, len(msg.Fields)+3)
	for k, v := range msg.Fields {
		fields[k] = v
	}

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
