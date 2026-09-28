package streams

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"time"
	"unicode"

	"noor/internal/notify"
	"noor/internal/storage"
)

const viewerBaseURL = "https://miau.gg/"
const maxStatusBytes = 4096
const statusURL = "https://ptch.net/stream.json"

type Watcher struct {
	Target  int64
	DB      *storage.DB
	Client  *http.Client
	Now     func() time.Time
	Logger  *slog.Logger
	Metrics interface {
		ObserveStreamProbe(string)
		ObserveNotificationEnqueue(string, string)
	}
}

func (w *Watcher) Initialize(ctx context.Context) error {
	_, err := w.DB.ExecContext(ctx, `
CREATE TABLE IF NOT EXISTS streams (
  id INTEGER PRIMARY KEY CHECK (id = 1),
  live INTEGER NOT NULL,
  name TEXT NOT NULL,
  epoch INTEGER NOT NULL
);`)
	if err != nil {
		return fmt.Errorf("create streams schema: %w", err)
	}
	return nil
}

func (w *Watcher) Run(ctx context.Context, interval time.Duration) error {
	w.pollAndLog(ctx)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			w.pollAndLog(ctx)
		}
	}
}

func (w *Watcher) pollAndLog(ctx context.Context) {
	if err := w.Poll(ctx); err != nil && ctx.Err() == nil {
		if w.Metrics != nil {
			w.Metrics.ObserveStreamProbe("error")
		}
		w.logger().Warn("streams watcher poll failed", "error", err)
	} else if w.Metrics != nil {
		w.Metrics.ObserveStreamProbe("success")
	}
}

func (w *Watcher) logger() *slog.Logger {
	if w.Logger != nil {
		return w.Logger
	}
	return slog.Default()
}

func (w *Watcher) Poll(ctx context.Context) error {
	status, err := w.fetchStatus(ctx)
	if err != nil {
		return err
	}
	return w.record(ctx, status)
}

type streamStatus struct {
	Live *bool  `json:"live"`
	Name string `json:"name"`
}

func (w *Watcher) fetchStatus(ctx context.Context) (streamStatus, error) {
	client := w.Client
	if client == nil {
		client = &http.Client{Timeout: 20 * time.Second}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, statusURL, nil)
	if err != nil {
		return streamStatus{}, fmt.Errorf("create stream status request: %w", err)
	}
	response, err := client.Do(req)
	if err != nil {
		return streamStatus{}, fmt.Errorf("request stream status: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return streamStatus{}, fmt.Errorf("stream status returned HTTP %d", response.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, maxStatusBytes+1))
	if err != nil {
		return streamStatus{}, fmt.Errorf("read stream status: %w", err)
	}
	if len(body) > maxStatusBytes {
		return streamStatus{}, fmt.Errorf("stream status exceeds %d bytes", maxStatusBytes)
	}
	var status streamStatus
	if err := json.Unmarshal(body, &status); err != nil {
		return streamStatus{}, fmt.Errorf("decode stream status: %w", err)
	}
	if status.Live == nil {
		return streamStatus{}, fmt.Errorf("stream status missing live field")
	}
	if *status.Live && !validName(status.Name) {
		return streamStatus{}, fmt.Errorf("stream status has invalid live name")
	}
	return status, nil
}

func validName(name string) bool {
	if len(name) == 0 || len(name) > 128 || name == "." || name == ".." {
		return false
	}
	for _, r := range name {
		if unicode.IsControl(r) || unicode.IsSpace(r) || r == '/' || r == '\\' {
			return false
		}
	}
	return true
}

func (w *Watcher) record(ctx context.Context, status streamStatus) error {
	live := *status.Live
	tx, err := w.DB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin stream update: %w", err)
	}
	defer tx.Rollback()
	var active, epoch int
	var name string
	err = tx.QueryRowContext(ctx, `SELECT live, name, epoch FROM streams WHERE id = 1`).Scan(&active, &name, &epoch)
	if err != nil && err != sql.ErrNoRows {
		return fmt.Errorf("load stream: %w", err)
	}
	firstObservation := err == sql.ErrNoRows
	wasLive := active != 0
	now := w.now()
	announce := !firstObservation && live && (!wasLive || name != status.Name)
	if announce {
		epoch++
	}
	if live {
		name = status.Name
	} else {
		name = ""
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO streams (id, live, name, epoch) VALUES (1, ?, ?, ?)
ON CONFLICT(id) DO UPDATE SET live = excluded.live, name = excluded.name, epoch = excluded.epoch`, storage.Bool(live), name, epoch); err != nil {
		return fmt.Errorf("store stream: %w", err)
	}
	if announce {
		if _, err := notify.Enqueue(ctx, tx, fmt.Sprintf("stream:%d", epoch), w.Target, "🔴 Stream live\n"+viewerBaseURL+url.PathEscape(status.Name), now); err != nil {
			if w.Metrics != nil {
				w.Metrics.ObserveNotificationEnqueue("streams", "error")
			}
			return fmt.Errorf("queue stream notification: %w", err)
		}
		if w.Metrics != nil {
			w.Metrics.ObserveNotificationEnqueue("streams", "queued")
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit stream update: %w", err)
	}
	return nil
}

func (w *Watcher) now() time.Time {
	if w.Now != nil {
		return w.Now().UTC()
	}
	return time.Now().UTC()
}
