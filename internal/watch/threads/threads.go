package threads

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	gateway "github.com/loynet/ptchan-gateway/clients/go"

	"noor/internal/notify"
	"noor/internal/storage"
)

type Config struct {
	Target          int64
	BaseURL         string
	MinReplyPosts   int
	KeywordDenylist []string
	MaxThreadAge    time.Duration
	EventRetention  time.Duration
}

type Watcher struct {
	Config  Config
	DB      *storage.DB
	Now     func() time.Time
	Logger  *slog.Logger
	Metrics interface{ ObserveNotificationEnqueue(string, string) }
}

func (w *Watcher) Initialize(ctx context.Context) error {
	_, err := w.DB.ExecContext(ctx, `
CREATE TABLE IF NOT EXISTS thread_events (event_id TEXT PRIMARY KEY, processed_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS watched_threads (
  thread_key TEXT PRIMARY KEY,
  board TEXT NOT NULL,
  thread_id INTEGER NOT NULL,
  created_at TEXT NOT NULL,
  last_seen_at TEXT NOT NULL,
  replies INTEGER NOT NULL,
  ignored INTEGER NOT NULL,
  has_op INTEGER NOT NULL,
  notified INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS thread_watcher_state (name TEXT PRIMARY KEY, value TEXT NOT NULL);
`)
	if err != nil {
		return fmt.Errorf("create threads schema: %w", err)
	}
	var value string
	err = w.DB.QueryRowContext(ctx, `SELECT value FROM thread_watcher_state WHERE name = 'bootstrap_at'`).Scan(&value)
	if err == sql.ErrNoRows {
		_, err = w.DB.ExecContext(ctx, `INSERT INTO thread_watcher_state (name, value) VALUES ('bootstrap_at', ?)`, storage.Time(w.now()))
	}
	if err != nil {
		return fmt.Errorf("set threads bootstrap watermark: %w", err)
	}
	return nil
}

func (w *Watcher) Consume(ctx context.Context, event gateway.WebhookEvent) error {
	if event.Kind != gateway.ThreadCreated && event.Kind != gateway.PostCreated {
		return nil
	}
	if w.Config.EventRetention > 0 && event.ObservedAt.Before(w.now().Add(-w.Config.EventRetention)) {
		return nil
	}
	tx, err := w.DB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin thread event: %w", err)
	}
	defer tx.Rollback()
	var found int
	err = tx.QueryRowContext(ctx, `SELECT 1 FROM thread_events WHERE event_id = ?`, event.EventID).Scan(&found)
	if err == nil {
		return tx.Commit()
	}
	if err != sql.ErrNoRows {
		return fmt.Errorf("check thread event receipt: %w", err)
	}

	key := event.Post.Board + ":" + strconv.FormatInt(event.Post.ThreadID, 10)
	var createdRaw string
	var replies, ignored, hasOP, notified int
	err = tx.QueryRowContext(ctx, `SELECT created_at, replies, ignored, has_op, notified FROM watched_threads WHERE thread_key = ?`, key).Scan(&createdRaw, &replies, &ignored, &hasOP, &notified)
	if err != nil && err != sql.ErrNoRows {
		return fmt.Errorf("load watched thread: %w", err)
	}
	createdAt := event.Post.Date
	if createdAt.IsZero() {
		createdAt = w.now()
	}
	if err == nil {
		createdAt, err = storage.ParseTime(createdRaw)
		if err != nil {
			return fmt.Errorf("parse thread creation time: %w", err)
		}
	}
	if event.Kind == "thread.created" {
		ignored = storage.Bool(!w.allows(event.Post, createdAt))
		hasOP = 1
	} else {
		replies++
	}
	now := w.now()
	bootstrapAt, err := bootstrap(ctx, tx)
	if err != nil {
		return err
	}
	if hasOP != 0 && notified == 0 && ignored == 0 && replies >= w.Config.MinReplyPosts && !event.ObservedAt.Before(bootstrapAt) {
		text := fmt.Sprintf("/%s/ #%d\n%d replies\n%s/%s/thread/%d.html", event.Post.Board, event.Post.ThreadID, replies, strings.TrimRight(w.Config.BaseURL, "/"), event.Post.Board, event.Post.ThreadID)
		queued, err := notify.Enqueue(ctx, tx, "thread:"+key, w.Config.Target, text, now)
		if err != nil {
			if w.Metrics != nil {
				w.Metrics.ObserveNotificationEnqueue("threads", "error")
			}
			return err
		}
		if queued {
			if w.Metrics != nil {
				w.Metrics.ObserveNotificationEnqueue("threads", "queued")
			}
			w.logger().Info("thread notification queued", "board", event.Post.Board, "observed_replies", replies)
			notified = 1
		}
	}
	_, err = tx.ExecContext(ctx, `
INSERT INTO watched_threads (thread_key, board, thread_id, created_at, last_seen_at, replies, ignored, has_op, notified)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(thread_key) DO UPDATE SET last_seen_at = excluded.last_seen_at, replies = excluded.replies, ignored = excluded.ignored, has_op = excluded.has_op, notified = excluded.notified`,
		key, event.Post.Board, event.Post.ThreadID, storage.Time(createdAt), storage.Time(now), replies, ignored, hasOP, notified)
	if err != nil {
		return fmt.Errorf("store watched thread: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO thread_events (event_id, processed_at) VALUES (?, ?)`, event.EventID, storage.Time(now)); err != nil {
		return fmt.Errorf("record thread event: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit thread event: %w", err)
	}
	return nil
}

func (w *Watcher) logger() *slog.Logger {
	if w.Logger != nil {
		return w.Logger
	}
	return slog.Default()
}

func (w *Watcher) PruneBefore(ctx context.Context, cutoff time.Time) (int64, error) {
	tx, err := w.DB.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("begin threads cleanup: %w", err)
	}
	defer tx.Rollback()
	var total int64
	for _, query := range []string{
		`DELETE FROM thread_events WHERE processed_at < ?`,
		`DELETE FROM watched_threads WHERE last_seen_at < ?`,
	} {
		result, err := tx.ExecContext(ctx, query, storage.Time(cutoff))
		if err != nil {
			return 0, fmt.Errorf("clean up threads state: %w", err)
		}
		count, err := result.RowsAffected()
		if err != nil {
			return 0, fmt.Errorf("count cleaned threads state: %w", err)
		}
		total += count
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit threads cleanup: %w", err)
	}
	return total, nil
}

func bootstrap(ctx context.Context, tx *sql.Tx) (time.Time, error) {
	var raw string
	if err := tx.QueryRowContext(ctx, `SELECT value FROM thread_watcher_state WHERE name = 'bootstrap_at'`).Scan(&raw); err != nil {
		return time.Time{}, fmt.Errorf("load threads bootstrap watermark: %w", err)
	}
	bootstrapAt, err := storage.ParseTime(raw)
	if err != nil {
		return time.Time{}, fmt.Errorf("parse threads bootstrap watermark: %w", err)
	}
	return bootstrapAt, nil
}

func (w *Watcher) allows(post gateway.Post, createdAt time.Time) bool {
	text := strings.ToLower(post.Subject + "\n" + post.Message)
	for _, keyword := range w.Config.KeywordDenylist {
		keyword = strings.ToLower(strings.TrimSpace(keyword))
		if keyword != "" && strings.Contains(text, keyword) {
			return false
		}
	}
	return w.Config.MaxThreadAge == 0 || w.now().Sub(createdAt) <= w.Config.MaxThreadAge
}

func (w *Watcher) now() time.Time {
	if w.Now != nil {
		return w.Now().UTC()
	}
	return time.Now().UTC()
}
