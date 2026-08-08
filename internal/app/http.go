package app

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"time"
)

func startHTTPServer(addr string, metrics *metrics, ready *atomic.Bool) (*http.Server, <-chan error, error) {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, nil, fmt.Errorf("listen for operational http: %w", err)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "ok\n") })
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		if !ready.Load() {
			http.Error(w, "not ready", http.StatusServiceUnavailable)
			return
		}
		_, _ = io.WriteString(w, "ok\n")
	})
	mux.Handle("/metrics", metrics.handler())
	server := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	errors := make(chan error, 1)
	go func() {
		if err := server.Serve(listener); err != nil && err != http.ErrServerClosed {
			errors <- err
		}
	}()
	return server, errors, nil
}

func operationalHTTPWorker(server *http.Server, errors <-chan error) func(context.Context) error {
	return func(ctx context.Context) error {
		select {
		case err := <-errors:
			return fmt.Errorf("operational http server: %w", err)
		case <-ctx.Done():
			stopHTTPServer(server)
			return nil
		}
	}
}

func stopHTTPServer(server *http.Server) {
	if server != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(ctx)
	}
}

func CheckHealth(addr string) error {
	if addr == "" {
		addr = "127.0.0.1:9090"
	}
	if strings.HasPrefix(addr, ":") {
		addr = "127.0.0.1" + addr
	}
	client := http.Client{Timeout: 3 * time.Second}
	response, err := client.Get("http://" + addr + "/healthz")
	if err != nil {
		return fmt.Errorf("request health: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("health status %s", response.Status)
	}
	return nil
}
