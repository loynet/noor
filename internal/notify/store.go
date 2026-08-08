package notify

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"noor/internal/storage"
)

type Store struct{ db *storage.DB }

type Notification struct {
	ID       int64
	Target   int64
	Text     string
	Attempts int
}

func OpenStore(db *storage.DB) (*Store, error) {
	store := &Store{db: db}
	_, err := db.ExecContext(context.Background(), `
CREATE TABLE IF NOT EXISTS notifications (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  dedupe_key TEXT NOT NULL UNIQUE,
  target INTEGER NOT NULL,
  text TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'pending',
  attempts INTEGER NOT NULL DEFAULT 0,
  next_attempt_at TEXT NOT NULL,
  created_at TEXT NOT NULL,
  delivered_at TEXT,
  failed_at TEXT
);
CREATE INDEX IF NOT EXISTS notifications_pending ON notifications(status, next_attempt_at, id);
`)
	if err != nil {
		return nil, fmt.Errorf("create notification schema: %w", err)
	}
	return store, nil
}

func (s *Store) Enqueue(ctx context.Context, dedupeKey string, target int64, text string, now time.Time) (bool, error) {
	return Enqueue(ctx, s.db, dedupeKey, target, text, now)
}

type execer interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}

func Enqueue(ctx context.Context, db execer, dedupeKey string, target int64, text string, now time.Time) (bool, error) {
	result, err := db.ExecContext(ctx, `
INSERT OR IGNORE INTO notifications (dedupe_key, target, text, next_attempt_at, created_at)
VALUES (?, ?, ?, ?, ?);`, dedupeKey, target, text, storage.Time(now), storage.Time(now))
	if err != nil {
		return false, fmt.Errorf("enqueue notification: %w", err)
	}
	count, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("count queued notification: %w", err)
	}
	return count == 1, nil
}

func (s *Store) Due(ctx context.Context, now time.Time, limit int) ([]Notification, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT id, target, text, attempts FROM notifications
WHERE status = 'pending' AND next_attempt_at <= ? ORDER BY id LIMIT ?;`, storage.Time(now), limit)
	if err != nil {
		return nil, fmt.Errorf("find due notifications: %w", err)
	}
	defer rows.Close()
	var notifications []Notification
	for rows.Next() {
		var notification Notification
		if err := rows.Scan(&notification.ID, &notification.Target, &notification.Text, &notification.Attempts); err != nil {
			return nil, fmt.Errorf("scan notification: %w", err)
		}
		notifications = append(notifications, notification)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate notifications: %w", err)
	}
	return notifications, nil
}

func (s *Store) Delivered(ctx context.Context, id int64, now time.Time) error {
	_, err := s.db.ExecContext(ctx, `UPDATE notifications SET status = 'delivered', delivered_at = ? WHERE id = ?`, storage.Time(now), id)
	if err != nil {
		return fmt.Errorf("mark notification delivered: %w", err)
	}
	return nil
}

func (s *Store) Retry(ctx context.Context, id int64, attempts int, next time.Time) error {
	_, err := s.db.ExecContext(ctx, `UPDATE notifications SET attempts = ?, next_attempt_at = ? WHERE id = ?`, attempts, storage.Time(next), id)
	if err != nil {
		return fmt.Errorf("reschedule notification: %w", err)
	}
	return nil
}

func (s *Store) Failed(ctx context.Context, id int64, now time.Time) error {
	_, err := s.db.ExecContext(ctx, `UPDATE notifications SET status = 'failed', failed_at = ? WHERE id = ?`, storage.Time(now), id)
	if err != nil {
		return fmt.Errorf("mark notification failed: %w", err)
	}
	return nil
}

func (s *Store) PruneCompletedBefore(ctx context.Context, cutoff time.Time) (int64, error) {
	result, err := s.db.ExecContext(ctx, `DELETE FROM notifications WHERE (status = 'delivered' AND delivered_at < ?) OR (status = 'failed' AND failed_at < ?)`, storage.Time(cutoff), storage.Time(cutoff))
	if err != nil {
		return 0, fmt.Errorf("prune delivered notifications: %w", err)
	}
	count, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("count pruned notifications: %w", err)
	}
	return count, nil
}

func (s *Store) PendingStats(ctx context.Context, now time.Time) (int, time.Duration, error) {
	var count int
	var oldest sql.NullString
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*), MIN(created_at) FROM notifications WHERE status = 'pending'`).Scan(&count, &oldest); err != nil {
		return 0, 0, fmt.Errorf("load pending notification stats: %w", err)
	}
	if !oldest.Valid {
		return count, 0, nil
	}
	created, err := storage.ParseTime(oldest.String)
	if err != nil {
		return 0, 0, fmt.Errorf("parse oldest pending notification: %w", err)
	}
	age := now.Sub(created)
	if age < 0 {
		age = 0
	}
	return count, age, nil
}
