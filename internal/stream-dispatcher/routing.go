package streamdispatcher

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/jiwoolee/kagent-agentbridge/internal/redis"
)

type Router struct {
	conn        *redis.Conn
	routingKey  string
	mu          sync.RWMutex
	cache       map[string]string
	lastRefresh time.Time
	ttl         time.Duration
}

func NewRouter(conn *redis.Conn, routingKey string) *Router {
	return &Router{
		conn:       conn,
		routingKey: routingKey,
		cache:      make(map[string]string),
		ttl:        60 * time.Second,
	}
}

func (r *Router) Resolve(ctx context.Context, step string) (string, error) {
	_ = ctx
	now := time.Now()

	r.mu.RLock()
	if now.Sub(r.lastRefresh) < r.ttl {
		agent := r.cache[step]
		r.mu.RUnlock()
		if agent == "" {
			return "", fmt.Errorf("no route found for step %q", step)
		}
		return agent, nil
	}
	r.mu.RUnlock()

	r.mu.Lock()
	defer r.mu.Unlock()

	if time.Since(r.lastRefresh) < r.ttl {
		agent := r.cache[step]
		if agent == "" {
			return "", fmt.Errorf("no route found for step %q", step)
		}
		return agent, nil
	}

	data, err := r.conn.HGetAll(r.routingKey)
	if err != nil {
		return "", fmt.Errorf("refresh routing table: %w", err)
	}

	r.cache = data
	r.lastRefresh = time.Now()

	agent := r.cache[step]
	if agent == "" {
		return "", fmt.Errorf("no route found for step %q", step)
	}

	return agent, nil
}
