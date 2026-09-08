package threads

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	gateway "github.com/loynet/ptchan-gateway/clients/go"
	"noor/internal/localization"
	"noor/internal/notify"
	"noor/internal/storage"
)

const (
	snapshotRetention   = 30 * 24 * time.Hour
	minimumSnapshots    = 10
	topHighlightPercent = 10
)

type Config struct {
	Target          int64
	BaseURL         string
	MinReplyPosts   int
	KeywordDenylist []string
	MaxThreadAge    time.Duration
	EventRetention  time.Duration
	Locale          string
}
type Watcher struct {
	Config  Config
	DB      *storage.DB
	Now     func() time.Time
	Logger  *slog.Logger
	Metrics interface{ ObserveNotificationEnqueue(string, string) }
}
type state struct {
	board                                                                    string
	threadID                                                                 int64
	createdAt, halfAt, thresholdAt                                           time.Time
	replies, replyCharacters, replyAttachments, capcodeReplies, martaReplies int64
	ignored, hasOP, completed                                                int
}

func (w *Watcher) Initialize(ctx context.Context) error {
	if w.Config.MinReplyPosts < 1 {
		return fmt.Errorf("threads min_reply_posts must be at least 1")
	}
	_, err := w.DB.ExecContext(ctx, `
CREATE TABLE IF NOT EXISTS thread_events (event_id TEXT PRIMARY KEY, processed_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS watched_threads (
 thread_key TEXT PRIMARY KEY, board TEXT NOT NULL, thread_id INTEGER NOT NULL, created_at TEXT NOT NULL, last_seen_at TEXT NOT NULL, replies INTEGER NOT NULL, ignored INTEGER NOT NULL, has_op INTEGER NOT NULL,
 half_at TEXT, threshold_at TEXT, reply_characters INTEGER NOT NULL DEFAULT 0, reply_attachments INTEGER NOT NULL DEFAULT 0, capcode_replies INTEGER NOT NULL DEFAULT 0, marta_replies INTEGER NOT NULL DEFAULT 0, completed INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS thread_snapshots (
 thread_key TEXT NOT NULL, threshold INTEGER NOT NULL, board TEXT NOT NULL, elapsed_nanoseconds INTEGER NOT NULL, acceleration_ppm INTEGER NOT NULL, reply_characters INTEGER NOT NULL, reply_attachments INTEGER NOT NULL, capcode_replies INTEGER NOT NULL, marta_replies INTEGER NOT NULL, completed_at TEXT NOT NULL,
 PRIMARY KEY (thread_key, threshold)
);
CREATE INDEX IF NOT EXISTS thread_snapshots_recent ON thread_snapshots(threshold, completed_at);
CREATE TABLE IF NOT EXISTS thread_watcher_state (name TEXT PRIMARY KEY, value TEXT NOT NULL);`)
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
	var receipt int
	if err := tx.QueryRowContext(ctx, `SELECT 1 FROM thread_events WHERE event_id = ?`, event.EventID).Scan(&receipt); err == nil {
		return tx.Commit()
	} else if err != sql.ErrNoRows {
		return fmt.Errorf("check thread event receipt: %w", err)
	}
	key := event.Post.Board + ":" + strconv.FormatInt(event.Post.ThreadID, 10)
	s := state{board: event.Post.Board, threadID: event.Post.ThreadID, createdAt: event.Post.Date}
	err = loadState(ctx, tx, key, &s)
	if err != nil && err != sql.ErrNoRows {
		return fmt.Errorf("load watched thread: %w", err)
	}
	if s.createdAt.IsZero() {
		s.createdAt = w.now()
	}
	now := w.now()
	if s.completed != 0 {
		return w.recordAndCommit(ctx, tx, event.EventID, now)
	}
	if event.Kind == gateway.ThreadCreated {
		s.createdAt = event.Post.Date
		if s.createdAt.IsZero() {
			s.createdAt = now
		}
		s.ignored = storage.Bool(!w.allows(event.Post, s.createdAt))
		s.hasOP = 1
	} else {
		s.replies++
		s.replyCharacters += int64(utf8.RuneCountInString(event.Post.Message))
		s.replyAttachments += event.Post.AttachmentCount
		if event.Post.Capcode != "" {
			s.capcodeReplies++
		}
		if event.Post.Origin != nil && event.Post.Origin.Kind == gateway.IntegrationOrigin && event.Post.Origin.Name == "marta" {
			s.martaReplies++
		}
		if s.halfAt.IsZero() && s.replies >= int64((w.Config.MinReplyPosts+1)/2) {
			s.halfAt = eventTime(event.Post.Date, now)
		}
		if s.thresholdAt.IsZero() && s.replies >= int64(w.Config.MinReplyPosts) {
			s.thresholdAt = eventTime(event.Post.Date, now)
		}
	}
	bootstrapAt, err := bootstrap(ctx, tx)
	if err != nil {
		return err
	}
	complete := s.hasOP != 0 && !s.thresholdAt.IsZero()
	if complete {
		s.completed = 1
		var highlight localization.Highlight
		if err := insertSnapshot(ctx, tx, key, w.Config.MinReplyPosts, s, now); err != nil {
			return err
		}
		highlight, err = rankHighlight(ctx, tx, key, w.Config.MinReplyPosts, s, now)
		if err != nil {
			return err
		}
		if s.ignored == 0 && !event.ObservedAt.Before(bootstrapAt) {
			text := localization.Thread(w.Config.Locale, localization.ThreadNotification{Board: s.board, ThreadID: s.threadID, Threshold: w.Config.MinReplyPosts, Elapsed: s.thresholdAt.Sub(s.createdAt), URL: threadURL(w.Config.BaseURL, s.board, s.threadID), Highlight: highlight})
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
				w.logger().Info("thread notification queued", "board", s.board)
			}
		}
	}
	if err := storeState(ctx, tx, key, s, now); err != nil {
		return err
	}
	return w.recordAndCommit(ctx, tx, event.EventID, now)
}

func loadState(ctx context.Context, tx *sql.Tx, key string, s *state) error {
	var created string
	var half, threshold sql.NullString
	err := tx.QueryRowContext(ctx, `SELECT board,thread_id,created_at,replies,ignored,has_op,half_at,threshold_at,reply_characters,reply_attachments,capcode_replies,marta_replies,completed FROM watched_threads WHERE thread_key=?`, key).Scan(&s.board, &s.threadID, &created, &s.replies, &s.ignored, &s.hasOP, &half, &threshold, &s.replyCharacters, &s.replyAttachments, &s.capcodeReplies, &s.martaReplies, &s.completed)
	if err != nil {
		return err
	}
	var parseErr error
	s.createdAt, parseErr = storage.ParseTime(created)
	if parseErr != nil {
		return parseErr
	}
	if half.Valid {
		s.halfAt, parseErr = storage.ParseTime(half.String)
		if parseErr != nil {
			return parseErr
		}
	}
	if threshold.Valid {
		s.thresholdAt, parseErr = storage.ParseTime(threshold.String)
		if parseErr != nil {
			return parseErr
		}
	}
	return nil
}
func storeState(ctx context.Context, tx *sql.Tx, key string, s state, now time.Time) error {
	_, err := tx.ExecContext(ctx, `INSERT INTO watched_threads (thread_key,board,thread_id,created_at,last_seen_at,replies,ignored,has_op,half_at,threshold_at,reply_characters,reply_attachments,capcode_replies,marta_replies,completed) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(thread_key) DO UPDATE SET last_seen_at=excluded.last_seen_at,replies=excluded.replies,ignored=excluded.ignored,has_op=excluded.has_op,half_at=excluded.half_at,threshold_at=excluded.threshold_at,reply_characters=excluded.reply_characters,reply_attachments=excluded.reply_attachments,capcode_replies=excluded.capcode_replies,marta_replies=excluded.marta_replies,completed=excluded.completed`, key, s.board, s.threadID, storage.Time(s.createdAt), storage.Time(now), s.replies, s.ignored, s.hasOP, nullableTime(s.halfAt), nullableTime(s.thresholdAt), s.replyCharacters, s.replyAttachments, s.capcodeReplies, s.martaReplies, s.completed)
	if err != nil {
		return fmt.Errorf("store watched thread: %w", err)
	}
	return nil
}
func nullableTime(t time.Time) any {
	if t.IsZero() {
		return nil
	}
	return storage.Time(t)
}
func eventTime(at, fallback time.Time) time.Time {
	if at.IsZero() {
		return fallback
	}
	return at
}
func (w *Watcher) recordAndCommit(ctx context.Context, tx *sql.Tx, id string, now time.Time) error {
	if _, err := tx.ExecContext(ctx, `INSERT INTO thread_events (event_id,processed_at) VALUES (?,?)`, id, storage.Time(now)); err != nil {
		return fmt.Errorf("record thread event: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit thread event: %w", err)
	}
	return nil
}

func insertSnapshot(ctx context.Context, tx *sql.Tx, key string, threshold int, s state, now time.Time) error {
	acceleration := acceleration(s, threshold)
	_, err := tx.ExecContext(ctx, `INSERT INTO thread_snapshots (thread_key,threshold,board,elapsed_nanoseconds,acceleration_ppm,reply_characters,reply_attachments,capcode_replies,marta_replies,completed_at) VALUES (?,?,?,?,?,?,?,?,?,?)`, key, threshold, s.board, s.thresholdAt.Sub(s.createdAt).Nanoseconds(), acceleration, s.replyCharacters, s.replyAttachments, s.capcodeReplies, s.martaReplies, storage.Time(now))
	if err != nil {
		return fmt.Errorf("store thread snapshot: %w", err)
	}
	return nil
}
func acceleration(s state, threshold int) int64 {
	if threshold < 2 {
		return -1
	}
	first, second := s.halfAt.Sub(s.createdAt), s.thresholdAt.Sub(s.halfAt)
	if first <= 0 || second < 0 {
		return -1
	}
	return int64(second * 1000000 / first)
}
func rankHighlight(ctx context.Context, tx *sql.Tx, key string, threshold int, s state, now time.Time) (localization.Highlight, error) {
	type candidate struct {
		kind   localization.HighlightKind
		metric string
		value  int64
		lower  bool
		count  int
	}
	accel := acceleration(s, threshold)
	if threshold < 2 || s.thresholdAt.Sub(s.halfAt) > s.halfAt.Sub(s.createdAt)/2 {
		accel = -1
	}
	items := []candidate{{localization.SpeedHighlight, "elapsed_nanoseconds", s.thresholdAt.Sub(s.createdAt).Nanoseconds(), true, 0}, {localization.AccelerationHighlight, "acceleration_ppm", accel, true, 0}, {localization.CapcodeHighlight, "capcode_replies", s.capcodeReplies, false, int(s.capcodeReplies)}, {localization.MartaHighlight, "marta_replies", s.martaReplies, false, int(s.martaReplies)}, {localization.LongFormHighlight, "reply_characters", s.replyCharacters, false, 0}, {localization.MediaHighlight, "reply_attachments", s.replyAttachments, false, 0}}
	for _, item := range items {
		if item.value < 0 || ((item.kind == localization.CapcodeHighlight || item.kind == localization.MartaHighlight) && item.value == 0) {
			continue
		}
		percent, ok, err := rank(ctx, tx, key, threshold, item.metric, item.value, item.lower, now)
		if err != nil {
			return localization.Highlight{}, err
		}
		if ok && percent <= topHighlightPercent {
			return localization.Highlight{Kind: item.kind, Percent: percent, Count: item.count}, nil
		}
	}
	return localization.Highlight{}, nil
}
func rank(ctx context.Context, tx *sql.Tx, key string, threshold int, metric string, value int64, lower bool, now time.Time) (int, bool, error) {
	op := ">="
	if lower {
		op = "<="
	}
	where := `threshold=? AND completed_at>=? AND thread_key!=?`
	if metric == "acceleration_ppm" {
		where += ` AND acceleration_ppm >= 0`
	}
	q := `SELECT COUNT(*), COUNT(CASE WHEN ` + metric + ` ` + op + ` ? THEN 1 END) FROM thread_snapshots WHERE ` + where
	var total, better int
	if err := tx.QueryRowContext(ctx, q, value, threshold, storage.Time(now.Add(-snapshotRetention)), key).Scan(&total, &better); err != nil {
		return 0, false, err
	}
	if total < minimumSnapshots {
		return 0, false, nil
	}
	return (100*(better+1) + total) / (total + 1), true, nil
}

func threadURL(base, board string, id int64) string {
	return fmt.Sprintf("%s/%s/thread/%d.html", strings.TrimRight(base, "/"), board, id)
}
func bootstrap(ctx context.Context, tx *sql.Tx) (time.Time, error) {
	var raw string
	if err := tx.QueryRowContext(ctx, `SELECT value FROM thread_watcher_state WHERE name='bootstrap_at'`).Scan(&raw); err != nil {
		return time.Time{}, fmt.Errorf("load threads bootstrap watermark: %w", err)
	}
	return storage.ParseTime(raw)
}
func (w *Watcher) logger() *slog.Logger {
	if w.Logger != nil {
		return w.Logger
	}
	return slog.Default()
}
func (w *Watcher) allows(post gateway.Post, created time.Time) bool {
	text := strings.ToLower(post.Subject + "\n" + post.Message)
	for _, keyword := range w.Config.KeywordDenylist {
		keyword = strings.ToLower(strings.TrimSpace(keyword))
		if keyword != "" && strings.Contains(text, keyword) {
			return false
		}
	}
	return w.Config.MaxThreadAge == 0 || w.now().Sub(created) <= w.Config.MaxThreadAge
}
func (w *Watcher) now() time.Time {
	if w.Now != nil {
		return w.Now().UTC()
	}
	return time.Now().UTC()
}
func (w *Watcher) PruneBefore(ctx context.Context, cutoff time.Time) (int64, error) {
	tx, err := w.DB.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("begin threads cleanup: %w", err)
	}
	defer tx.Rollback()
	var total int64
	for _, item := range []struct {
		q  string
		at time.Time
	}{{`DELETE FROM thread_events WHERE processed_at < ?`, cutoff}, {`DELETE FROM watched_threads WHERE last_seen_at < ?`, cutoff}, {`DELETE FROM thread_snapshots WHERE completed_at < ?`, w.now().Add(-snapshotRetention)}} {
		r, e := tx.ExecContext(ctx, item.q, storage.Time(item.at))
		if e != nil {
			return 0, fmt.Errorf("clean up threads state: %w", e)
		}
		n, e := r.RowsAffected()
		if e != nil {
			return 0, e
		}
		total += n
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return total, nil
}
