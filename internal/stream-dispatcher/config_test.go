package streamdispatcher

import "testing"

func TestLoadConfigFromEnv_DefaultsAndRequired(t *testing.T) {
	t.Setenv("REDIS_SENTINEL_HOST", "redis-sentinel")
	t.Setenv("KAGENT_A2A_URL", "http://kagent.local")

	cfg, err := LoadConfigFromEnv()
	if err != nil {
		t.Fatalf("LoadConfigFromEnv returned error: %v", err)
	}

	if cfg.RedisSentinelPort != 26379 {
		t.Fatalf("expected default REDIS_SENTINEL_PORT=26379, got %d", cfg.RedisSentinelPort)
	}
	if cfg.RedisSentinelMasterName != "mymaster" {
		t.Fatalf("expected default REDIS_SENTINEL_MASTER_NAME=mymaster, got %s", cfg.RedisSentinelMasterName)
	}
	if cfg.RedisDB != 4 {
		t.Fatalf("expected default REDIS_DB=4, got %d", cfg.RedisDB)
	}
	if cfg.StreamName != "incident:events" {
		t.Fatalf("expected default STREAM_NAME=incident:events, got %s", cfg.StreamName)
	}
	if cfg.DLQStream != "incident:events:dlq" {
		t.Fatalf("expected default DLQ_STREAM=incident:events:dlq, got %s", cfg.DLQStream)
	}
	if cfg.ConsumerGroup != "dispatch-group" {
		t.Fatalf("expected default CONSUMER_GROUP=dispatch-group, got %s", cfg.ConsumerGroup)
	}
	if cfg.RoutingKey != "incident:routing" {
		t.Fatalf("expected default ROUTING_KEY=incident:routing, got %s", cfg.RoutingKey)
	}
	if cfg.Workers != 10 {
		t.Fatalf("expected default DISPATCHER_WORKERS=10, got %d", cfg.Workers)
	}
	if cfg.BlockMS != 5000 {
		t.Fatalf("expected default BLOCK_MS=5000, got %d", cfg.BlockMS)
	}
	if cfg.MinIdleMS != 90000 {
		t.Fatalf("expected default MIN_IDLE_MS=90000, got %d", cfg.MinIdleMS)
	}
}

func TestLoadConfigFromEnv_RequiresMandatoryValues(t *testing.T) {
	t.Setenv("REDIS_SENTINEL_HOST", "")
	t.Setenv("KAGENT_A2A_URL", "")

	_, err := LoadConfigFromEnv()
	if err == nil {
		t.Fatal("expected error when required env vars are missing")
	}
}
