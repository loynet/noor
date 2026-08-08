package streams

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"noor/internal/notify"
	"noor/internal/storage"
)

const missedProbesBeforeOffline = 2

type Channel struct {
	Key      string
	ProbeURL string
	PageURL  string
}

type Watcher struct {
	Channels []Channel
	Target   int64
	DB       *storage.DB
	Client   *http.Client
	Now      func() time.Time
	Logger   *slog.Logger
	Metrics  interface {
		ObserveStreamProbe(string)
		ObserveNotificationEnqueue(string, string)
	}
}

func (w *Watcher) Initialize(ctx context.Context) error {
	_, err := w.DB.ExecContext(ctx, `
CREATE TABLE IF NOT EXISTS streams (
  stream_key TEXT PRIMARY KEY,
  active INTEGER NOT NULL,
  misses INTEGER NOT NULL,
  epoch INTEGER NOT NULL,
  inactive_since TEXT
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
	var errs []error
	for _, channel := range w.Channels {
		live, err := w.isLive(ctx, channel)
		if err != nil {
			errs = append(errs, fmt.Errorf("probe %s: %w", channel.Key, err))
			continue
		}
		if err := w.record(ctx, channel, live); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// PruneInactiveBefore removes streams that have remained offline long enough.
// Live streams retain their row so a restart cannot re-announce them.
func (w *Watcher) PruneInactiveBefore(ctx context.Context, cutoff time.Time) (int64, error) {
	result, err := w.DB.ExecContext(ctx, `DELETE FROM streams WHERE active = 0 AND inactive_since < ?`, storage.Time(cutoff))
	if err != nil {
		return 0, fmt.Errorf("clean up inactive streams: %w", err)
	}
	count, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("count cleaned inactive streams: %w", err)
	}
	return count, nil
}

func (w *Watcher) isLive(ctx context.Context, channel Channel) (bool, error) {
	client := w.Client
	if client == nil {
		client = &http.Client{Timeout: 20 * time.Second}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, channel.ProbeURL, nil)
	if err != nil {
		return false, fmt.Errorf("create request: %w", err)
	}
	response, err := client.Do(req)
	if err != nil {
		return false, fmt.Errorf("send request: %w", err)
	}
	defer response.Body.Close()
	switch response.StatusCode {
	case http.StatusOK:
		return true, nil
	case http.StatusNotFound:
		return false, nil
	default:
		return false, fmt.Errorf("unexpected status: %s", response.Status)
	}
}

func (w *Watcher) record(ctx context.Context, channel Channel, live bool) error {
	tx, err := w.DB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin stream update: %w", err)
	}
	defer tx.Rollback()
	var active, misses, epoch int
	var inactiveSince sql.NullString
	err = tx.QueryRowContext(ctx, `SELECT active, misses, epoch, inactive_since FROM streams WHERE stream_key = ?`, channel.Key).Scan(&active, &misses, &epoch, &inactiveSince)
	if err != nil && err != sql.ErrNoRows {
		return fmt.Errorf("load stream %s: %w", channel.Key, err)
	}
	firstObservation := err == sql.ErrNoRows
	wasLive := active != 0
	now := w.now()
	if firstObservation {
		if live {
			active = 1
		} else {
			inactiveSince = sql.NullString{String: storage.Time(now), Valid: true}
		}
	} else if live {
		active, misses = 1, 0
		inactiveSince = sql.NullString{}
		if !wasLive {
			epoch++
		}
	} else if wasLive {
		misses++
		if misses >= missedProbesBeforeOffline {
			active, misses = 0, 0
			inactiveSince = sql.NullString{String: storage.Time(now), Valid: true}
		}
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO streams (stream_key, active, misses, epoch, inactive_since) VALUES (?, ?, ?, ?, ?)
ON CONFLICT(stream_key) DO UPDATE SET active = excluded.active, misses = excluded.misses, epoch = excluded.epoch, inactive_since = excluded.inactive_since`, channel.Key, active, misses, epoch, inactiveSince); err != nil {
		return fmt.Errorf("store stream %s: %w", channel.Key, err)
	}
	if live && !wasLive && !firstObservation {
		if _, err := notify.Enqueue(ctx, tx, fmt.Sprintf("stream:%s:%d", channel.Key, epoch), w.Target, "🔴 Stream live\n"+channel.PageURL, now); err != nil {
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
