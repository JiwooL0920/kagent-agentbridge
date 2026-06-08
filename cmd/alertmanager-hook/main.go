package main

import (
	"context"
	"log"
	"os"
	"os/signal"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	log.Println("starting alertmanager-hook...")

	<-ctx.Done()
	log.Println("shutting down alertmanager-hook")
	os.Exit(0)
}
