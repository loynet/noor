package app

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	gateway "github.com/loynet/ptchan-gateway/clients/go"

	"noor/internal/notify"
	"noor/internal/storage"
	"noor/internal/telegram"
	"noor/internal/watch/streams"
	"noor/internal/watch/threads"
)

func Run(ctx context.Context, cfg Config) error {
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	metrics := newMetrics()
	var ready atomic.Bool
	server, httpErrors, err := startHTTPServer(cfg.HTTPAddr, metrics, &ready)
	if err != nil {
		return err
	}
	defer stopHTTPServer(server)
	db, err := storage.Open(cfg.SQLitePath)
	if err != nil {
		return err
	}
	defer db.Close()
	queue, err := notify.OpenStore(db)
	if err != nil {
		return err
	}
	notifier := notify.Notifier{Store: queue, Sender: telegram.New(cfg.TelegramToken), Metrics: metrics}
	var workers []func(context.Context) error
	workers = append(workers, operationalHTTPWorker(server, httpErrors))
	workers = append(workers, notifier.Run)
	var threadsWatcher *threads.Watcher
	var streamsWatcher *streams.Watcher
	if cfg.Streams.Enabled {
		streamsWatcher = &streams.Watcher{Channels: cfg.Streams.Channels, Target: cfg.NotificationChatID, DB: db, Metrics: metrics}
		if err := streamsWatcher.Initialize(ctx); err != nil {
			return err
		}
		workers = append(workers, func(ctx context.Context) error { return streamsWatcher.Run(ctx, cfg.Streams.Interval) })
	}
	if cfg.Threads.Enabled {
		threadsWatcher = &threads.Watcher{Config: cfg.Threads.Config, DB: db, Metrics: metrics}
		if err := threadsWatcher.Initialize(ctx); err != nil {
			return err
		}
		listener, err := net.Listen("tcp", cfg.WebhookAddr)
		if err != nil {
			return fmt.Errorf("listen for gateway events: %w", err)
		}
		workers = append(workers, gatewayServer(listener, cfg.Threads.Secret, threadsWatcher, metrics))
	}
	workers = append(workers, cleanupWorker(queue, threadsWatcher, streamsWatcher, cfg.Retention))
	ready.Store(true)
	var group sync.WaitGroup
	workerErrors := make(chan error, len(workers))
	for _, worker := range workers {
		group.Add(1)
		go func(worker func(context.Context) error) { defer group.Done(); workerErrors <- worker(runCtx) }(worker)
	}
	select {
	case <-ctx.Done():
		cancel()
		group.Wait()
		return nil
	case err := <-workerErrors:
		cancel()
		group.Wait()
		if err == nil {
			return fmt.Errorf("noor worker stopped")
		}
		return err
	}
}

func cleanupWorker(queue *notify.Store, threadsWatcher *threads.Watcher, streamsWatcher *streams.Watcher, retention time.Duration) func(context.Context) error {
	return func(ctx context.Context) error {
		ticker := time.NewTicker(time.Hour)
		defer ticker.Stop()
		for {
			cutoff := time.Now().UTC().Add(-retention)
			if _, err := queue.PruneCompletedBefore(ctx, cutoff); err != nil {
				return err
			}
			if threadsWatcher != nil {
				if _, err := threadsWatcher.PruneBefore(ctx, cutoff); err != nil {
					return err
				}
			}
			if streamsWatcher != nil {
				if _, err := streamsWatcher.PruneInactiveBefore(ctx, cutoff); err != nil {
					return err
				}
			}
			select {
			case <-ctx.Done():
				return nil
			case <-ticker.C:
			}
		}
	}
}

func gatewayServer(listener net.Listener, secret string, watcher *threads.Watcher, metrics *metrics) func(context.Context) error {
	return func(ctx context.Context) error {
		server := &http.Server{Handler: gatewayHandler(secret, watcher, metrics), ReadHeaderTimeout: 5 * time.Second}
		stopped := make(chan error, 1)
		go func() {
			if err := server.Serve(listener); err != nil && err != http.ErrServerClosed {
				stopped <- err
			}
		}()
		select {
		case <-ctx.Done():
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			return server.Shutdown(shutdownCtx)
		case err := <-stopped:
			return err
		}
	}
}

func gatewayHandler(secret string, watcher *threads.Watcher, metrics *metrics) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/internal/ptchan/events", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			metrics.webhook("method_not_allowed")
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, gateway.DefaultWebhookMaxBodyBytes))
		if err != nil {
			var maxBytesErr *http.MaxBytesError
			if errors.As(err, &maxBytesErr) {
				metrics.webhook("payload_too_large")
				http.Error(w, "payload too large", http.StatusRequestEntityTooLarge)
				return
			}
			metrics.webhook("bad_request")
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		event, err := gateway.VerifyWebhookBody(secret, r.Header.Get("x-ptchan-event-id"), r.Header.Get("x-ptchan-timestamp"), r.Header.Get("x-ptchan-signature"), body)
		if err != nil {
			if errors.Is(err, gateway.ErrWebhookAuthentication) {
				metrics.webhook("unauthorized")
				http.Error(w, "unauthorized", http.StatusUnauthorized)
			} else {
				metrics.webhook("invalid_event")
				http.Error(w, "bad request", http.StatusBadRequest)
			}
			return
		}
		if err := watcher.Consume(r.Context(), *event); err != nil {
			metrics.webhook("consumer_error")
			metrics.thread(event.Post.Board, metricThreadKind(event.Kind), "error")
			http.Error(w, "gateway unavailable", http.StatusBadGateway)
			return
		}
		metrics.webhook("success")
		metrics.thread(event.Post.Board, metricThreadKind(event.Kind), "processed")
		w.WriteHeader(http.StatusNoContent)
	})
	return mux
}

func metricThreadKind(kind gateway.EventKind) string {
	switch kind {
	case gateway.ThreadCreated, gateway.PostCreated:
		return string(kind)
	default:
		return "unknown"
	}
}
