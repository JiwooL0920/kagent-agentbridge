package streamdispatcher

import (
	"fmt"
	"os"
	"strconv"
)

type Config struct {
	RedisSentinelHost       string
	RedisSentinelPort       int
	RedisSentinelMasterName string
	RedisDB                 int
	StreamName              string
	DLQStream               string
	ConsumerGroup           string
	ConsumerName            string
	RoutingKey              string
	Workers                 int
	BlockMS                 int64
	MinIdleMS               int64
	KagentA2AURL            string
	InternalA2ABearerToken  string
}

func LoadConfigFromEnv() (Config, error) {
	hostname, err := os.Hostname()
	if err != nil || hostname == "" {
		hostname = "stream-dispatcher"
	}

	cfg := Config{
		RedisSentinelHost:       os.Getenv("REDIS_SENTINEL_HOST"),
		RedisSentinelPort:       getEnvInt("REDIS_SENTINEL_PORT", 26379),
		RedisSentinelMasterName: getEnvString("REDIS_SENTINEL_MASTER_NAME", "mymaster"),
		RedisDB:                 getEnvInt("REDIS_DB", 4),
		StreamName:              getEnvString("STREAM_NAME", "incident:events"),
		DLQStream:               getEnvString("DLQ_STREAM", "incident:events:dlq"),
		ConsumerGroup:           getEnvString("CONSUMER_GROUP", "dispatch-group"),
		ConsumerName:            getEnvString("CONSUMER_NAME", hostname),
		RoutingKey:              getEnvString("ROUTING_KEY", "incident:routing"),
		Workers:                 getEnvInt("DISPATCHER_WORKERS", 10),
		BlockMS:                 getEnvInt64("BLOCK_MS", 5000),
		MinIdleMS:               getEnvInt64("MIN_IDLE_MS", 90000),
		KagentA2AURL:            os.Getenv("KAGENT_A2A_URL"),
		InternalA2ABearerToken:  os.Getenv("INTERNAL_A2A_BEARER_TOKEN"),
	}

	if cfg.RedisSentinelHost == "" {
		return Config{}, fmt.Errorf("REDIS_SENTINEL_HOST is required")
	}
	if cfg.KagentA2AURL == "" {
		return Config{}, fmt.Errorf("KAGENT_A2A_URL is required")
	}
	if cfg.Workers < 1 {
		cfg.Workers = 1
	}

	return cfg, nil
}

func getEnvString(key, fallback string) string {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	return v
}

func getEnvInt(key string, fallback int) int {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return fallback
	}
	return n
}

func getEnvInt64(key string, fallback int64) int64 {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return fallback
	}
	return n
}
