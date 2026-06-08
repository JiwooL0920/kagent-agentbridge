package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

type Config struct {
	HTTPAddr string
	Redis    RedisConfig
	A2A      A2AConfig
	Hook     AlertmanagerHookConfig
}

type RedisConfig struct {
	SentinelHost       string
	SentinelPort       int
	SentinelMasterName string
	DB                 int
	StreamName         string
}

type A2AConfig struct {
	URL         string
	BearerToken string
}

type AlertmanagerHookConfig struct {
	TargetAgent       string
	AllowedSeverities []string
	IncludeResolved   bool
}

func Load() (Config, error) {
	cfg := Config{
		HTTPAddr: getEnvString("HTTP_ADDR", ":8080"),
		Redis: RedisConfig{
			SentinelHost:       os.Getenv("REDIS_SENTINEL_HOST"),
			SentinelPort:       getEnvInt("REDIS_SENTINEL_PORT", 26379),
			SentinelMasterName: getEnvString("REDIS_SENTINEL_MASTER_NAME", "mymaster"),
			DB:                 getEnvInt("REDIS_DB", 4),
			StreamName:         getEnvString("REDIS_STREAM_NAME", "incident:events"),
		},
		A2A: A2AConfig{
			URL:         os.Getenv("KAGENT_A2A_URL"),
			BearerToken: os.Getenv("INTERNAL_A2A_BEARER_TOKEN"),
		},
		Hook: AlertmanagerHookConfig{
			TargetAgent:       getEnvString("ALERTMANAGER_TARGET_AGENT", "triage-agent"),
			AllowedSeverities: getEnvCSV("ALERTMANAGER_ALLOWED_SEVERITIES", []string{"critical"}),
			IncludeResolved:   getEnvBool("ALERTMANAGER_INCLUDE_RESOLVED", false),
		},
	}

	if cfg.Redis.SentinelHost == "" && cfg.A2A.URL == "" {
		return Config{}, fmt.Errorf("KAGENT_A2A_URL is required when REDIS_SENTINEL_HOST is not set")
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

func getEnvBool(key string, fallback bool) bool {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return fallback
	}
	return b
}

func getEnvCSV(key string, fallback []string) []string {
	v := os.Getenv(key)
	if strings.TrimSpace(v) == "" {
		return fallback
	}

	parts := strings.Split(v, ",")
	result := make([]string, 0, len(parts))
	for _, p := range parts {
		trimmed := strings.TrimSpace(p)
		if trimmed != "" {
			result = append(result, trimmed)
		}
	}
	if len(result) == 0 {
		return fallback
	}
	return result
}
