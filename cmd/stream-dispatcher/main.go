package main

import (
	"context"
	"errors"
	"log"
	"os"
	"os/signal"
	"syscall"

	streamdispatcher "github.com/jiwoolee/kagent-agentbridge/internal/stream-dispatcher"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	log.Println("starting stream-dispatcher...")
	cfg, err := streamdispatcher.LoadConfigFromEnv()
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	dispatcher, err := streamdispatcher.New(cfg)
	if err != nil {
		log.Fatalf("create dispatcher: %v", err)
	}
	defer func() {
		if err := dispatcher.Close(); err != nil {
			log.Printf("close dispatcher: %v", err)
		}
	}()

	go func() {
		if err := dispatcher.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("dispatcher exited with error: %v", err)
			cancel()
		}
	}()

	<-ctx.Done()
	log.Println("shutting down stream-dispatcher")
}
