package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"

	alertmanagerhook "github.com/jiwoolee/kagent-agentbridge/internal/alertmanager-hook"
	"github.com/jiwoolee/kagent-agentbridge/internal/a2a"
	"github.com/jiwoolee/kagent-agentbridge/internal/config"
	"github.com/jiwoolee/kagent-agentbridge/internal/httpapi"
	"github.com/jiwoolee/kagent-agentbridge/internal/redis"
	"github.com/jiwoolee/kagent-agentbridge/internal/server"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{}))

	cfg, err := config.Load()
	if err != nil {
		logger.Error("failed to load config", "error", err)
		os.Exit(1)
	}

	sender, cleanup, err := buildSender(ctx, cfg, logger)
	if err != nil {
		logger.Error("failed to initialize sender", "error", err)
		os.Exit(1)
	}
	defer cleanup()

	hookHandler := alertmanagerhook.NewHandler(sender, alertmanagerhook.Options{
		TargetAgent:       cfg.Hook.TargetAgent,
		AllowedSeverities: cfg.Hook.AllowedSeverities,
		IncludeResolved:   cfg.Hook.IncludeResolved,
		Logger:            logger,
	})

	routes := httpapi.NewMux(hookHandler)
	httpServer := server.New(cfg.HTTPAddr, routes)

	logger.Info("starting alertmanager-hook", "addr", cfg.HTTPAddr)
	if err := httpServer.ListenAndServe(ctx); err != nil {
		logger.Error("server stopped with error", "error", err)
		os.Exit(1)
	}

	logger.Info("shutting down alertmanager-hook")
}

func buildSender(ctx context.Context, cfg config.Config, logger *slog.Logger) (redis.TaskSender, func(), error) {
	if cfg.Redis.SentinelHost != "" {
		conn, err := redis.DialSentinel(ctx, redis.SentinelConfig{
			SentinelAddrs: []string{fmt.Sprintf("%s:%d", cfg.Redis.SentinelHost, cfg.Redis.SentinelPort)},
			MasterName:    cfg.Redis.SentinelMasterName,
			DB:            cfg.Redis.DB,
		})
		if err != nil {
			return nil, nil, err
		}

		publisher := redis.NewStreamPublisher(conn, cfg.Redis.StreamName, logger)
		return publisher, func() { _ = conn.Close() }, nil
	}

	client := a2a.NewClient(cfg.A2A.URL, cfg.A2A.BearerToken, logger)
	return client, func() {}, nil
}
