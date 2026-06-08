package server

import (
	"context"
	"errors"
	"net/http"
	"time"
)

type Server struct {
	addr   string
	server *http.Server
}

func New(addr string, handler http.Handler) *Server {
	return &Server{
		addr: addr,
		server: &http.Server{
			Addr:         addr,
			Handler:      handler,
			ReadTimeout:  15 * time.Second,
			WriteTimeout: 15 * time.Second,
			IdleTimeout:  60 * time.Second,
		},
	}
}

func (s *Server) ListenAndServe(ctx context.Context) error {
	errChan := make(chan error, 1)
	
	go func() {
		if err := s.server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errChan <- err
		}
	}()
	
	select {
	case err := <-errChan:
		return err
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		return s.server.Shutdown(shutdownCtx)
	}
}
