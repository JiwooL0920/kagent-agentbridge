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

	log.Println("starting stream-dispatcher...")

	<-ctx.Done()
	log.Println("shutting down stream-dispatcher")
	os.Exit(0)
}
