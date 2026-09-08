package threads

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	gateway "github.com/loynet/ptchan-gateway/clients/go"
	"noor/internal/localization"
	"noor/internal/notify"
	"noor/internal/storage"
)

func testWatcher(t *testing.T, cfg Config) (*Watcher, *storage.DB, *notify.Store, time.Time) {
	t.Helper()
	db, err := storage.Open(filepath.Join(t.TempDir(), "noor.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	q, err := notify.OpenStore(db)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	cfg.Target = 1
	cfg.BaseURL = "https://ptchan.org"
	cfg.EventRetention = time.Hour
	w := &Watcher{Config: cfg, DB: db, Now: func() time.Time { return now }}
	if err := w.Initialize(context.Background()); err != nil {
		t.Fatal(err)
	}
	return w, db, q, now
}
func event(id string, kind gateway.EventKind, now time.Time, post gateway.Post) gateway.WebhookEvent {
	return gateway.WebhookEvent{EventID: id, Kind: kind, Source: "ptchan", ObservedAt: now, Post: post}
}
func post(id int64, date time.Time) gateway.Post {
	return gateway.Post{Board: "test", ThreadID: 1, PostID: id, Date: date}
}

func TestFreezesSnapshotAtThreshold(t *testing.T) {
	w, db, q, now := testWatcher(t, Config{MinReplyPosts: 2})
	op := post(1, now.Add(-20*time.Minute))
	if err := w.Consume(context.Background(), event("op", gateway.ThreadCreated, now, op)); err != nil {
		t.Fatal(err)
	}
	first := post(2, now.Add(-10*time.Minute))
	first.Message = "hello"
	first.AttachmentCount = 1
	second := post(3, now)
	second.Message = "world"
	second.Capcode = "mod"
	third := post(4, now.Add(time.Minute))
	third.Message = "should not count"
	third.AttachmentCount = 9
	for _, e := range []gateway.WebhookEvent{event("a", gateway.PostCreated, now, first), event("b", gateway.PostCreated, now, second), event("c", gateway.PostCreated, now, third)} {
		if err := w.Consume(context.Background(), e); err != nil {
			t.Fatal(err)
		}
	}
	var chars, attachments, capcodes int
	if err := db.QueryRowContext(context.Background(), `SELECT reply_characters,reply_attachments,capcode_replies FROM thread_snapshots`).Scan(&chars, &attachments, &capcodes); err != nil {
		t.Fatal(err)
	}
	if chars != 10 || attachments != 1 || capcodes != 1 {
		t.Fatalf("snapshot = %d %d %d", chars, attachments, capcodes)
	}
	due, err := q.Due(context.Background(), now, 10)
	if err != nil || len(due) != 1 {
		t.Fatalf("notifications %#v %v", due, err)
	}
}

func TestInitializeRejectsZeroThreshold(t *testing.T) {
	db, err := storage.Open(filepath.Join(t.TempDir(), "noor.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	w := Watcher{Config: Config{MinReplyPosts: 0}, DB: db}
	if err := w.Initialize(context.Background()); err == nil {
		t.Fatal("Initialize accepted a zero reply threshold")
	}
}

func TestFilteredThreadSnapshotsWithoutNotification(t *testing.T) {
	w, db, q, now := testWatcher(t, Config{MinReplyPosts: 1, KeywordDenylist: []string{"skip"}})
	op := post(1, now.Add(-time.Minute))
	op.Subject = "skip"
	for _, e := range []gateway.WebhookEvent{event("op", gateway.ThreadCreated, now, op), event("r", gateway.PostCreated, now, post(2, now))} {
		if err := w.Consume(context.Background(), e); err != nil {
			t.Fatal(err)
		}
	}
	var n int
	db.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM thread_snapshots`).Scan(&n)
	if n != 1 {
		t.Fatal(n)
	}
	due, _ := q.Due(context.Background(), now, 10)
	if len(due) != 0 {
		t.Fatal(len(due))
	}
}
func TestHighlightPriorityAndRanking(t *testing.T) {
	w, db, q, now := testWatcher(t, Config{MinReplyPosts: 2})
	for i := 0; i < 25; i++ {
		_, err := db.ExecContext(context.Background(), `INSERT INTO thread_snapshots (thread_key,threshold,board,elapsed_nanoseconds,acceleration_ppm,reply_characters,reply_attachments,capcode_replies,marta_replies,completed_at) VALUES (?,?,?,?,?,?,?,?,?,?)`, fmt.Sprintf("old:%d", i), 2, "test", (2 * time.Hour).Nanoseconds(), 1000000, 1, 0, 0, 0, storage.Time(now.Add(-time.Hour)))
		if err != nil {
			t.Fatal(err)
		}
	}
	op := post(1, now.Add(-30*time.Minute))
	r1 := post(2, now.Add(-20*time.Minute))
	r1.Message = "long"
	r2 := post(3, now)
	r2.Message = "long"
	for _, e := range []gateway.WebhookEvent{event("op", gateway.ThreadCreated, now, op), event("r1", gateway.PostCreated, now, r1), event("r2", gateway.PostCreated, now, r2)} {
		if err := w.Consume(context.Background(), e); err != nil {
			t.Fatal(err)
		}
	}
	due, _ := q.Due(context.Background(), now, 10)
	if len(due) != 1 || !strings.Contains(due[0].Text, "Blink and you missed it") {
		t.Fatalf("%#v", due)
	}
}

func TestGeneralHighlightsRequireTopTenPercent(t *testing.T) {
	_, db, _, now := testWatcher(t, Config{MinReplyPosts: 1})
	for i := 0; i < minimumSnapshots; i++ {
		elapsed := 2 * time.Hour
		if i == 0 {
			elapsed = time.Hour
		}
		_, err := db.ExecContext(context.Background(), `INSERT INTO thread_snapshots (thread_key,threshold,board,elapsed_nanoseconds,acceleration_ppm,reply_characters,reply_attachments,capcode_replies,marta_replies,completed_at) VALUES (?,?,?,?,?,?,?,?,?,?)`, fmt.Sprintf("old:%d", i), 1, "test", elapsed.Nanoseconds(), -1, 0, 0, 0, 0, storage.Time(now.Add(-time.Hour)))
		if err != nil {
			t.Fatal(err)
		}
	}
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	s := state{createdAt: now.Add(-90 * time.Minute), thresholdAt: now}
	h, err := rankHighlight(context.Background(), tx, "current", 1, s, now)
	if err != nil {
		t.Fatal(err)
	}
	if h.Kind != "" {
		t.Fatalf("top-19%% highlight = %#v", h)
	}

	s.createdAt = now.Add(-30 * time.Minute)
	h, err = rankHighlight(context.Background(), tx, "current", 1, s, now)
	if err != nil {
		t.Fatal(err)
	}
	if h.Kind != localization.SpeedHighlight || h.Percent != topHighlightPercent {
		t.Fatalf("top-10%% highlight = %#v", h)
	}
}

func TestMartaAndCapcodeAreCounted(t *testing.T) {
	w, db, _, now := testWatcher(t, Config{MinReplyPosts: 2})
	if err := w.Consume(context.Background(), event("op", gateway.ThreadCreated, now, post(1, now.Add(-time.Minute)))); err != nil {
		t.Fatal(err)
	}
	a := post(2, now.Add(-30*time.Second))
	a.Capcode = "x"
	a.Origin = &gateway.PostOrigin{Kind: gateway.IntegrationOrigin, Name: "marta"}
	b := post(3, now)
	b.Origin = &gateway.PostOrigin{Kind: gateway.IntegrationOrigin, Name: "not-marta"}
	for _, e := range []gateway.WebhookEvent{event("a", gateway.PostCreated, now, a), event("b", gateway.PostCreated, now, b)} {
		if err := w.Consume(context.Background(), e); err != nil {
			t.Fatal(err)
		}
	}
	var capcodes, marta int
	db.QueryRowContext(context.Background(), `SELECT capcode_replies,marta_replies FROM thread_snapshots`).Scan(&capcodes, &marta)
	if capcodes != 1 || marta != 1 {
		t.Fatalf("%d %d", capcodes, marta)
	}
}
func TestPruneSnapshotsAfterThirtyDays(t *testing.T) {
	w, db, _, now := testWatcher(t, Config{MinReplyPosts: 1})
	_, err := db.ExecContext(context.Background(), `INSERT INTO thread_snapshots (thread_key,threshold,board,elapsed_nanoseconds,acceleration_ppm,reply_characters,reply_attachments,capcode_replies,marta_replies,completed_at) VALUES ('old',1,'test',1,-1,0,0,0,0,?)`, storage.Time(now.Add(-31*24*time.Hour)))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.PruneBefore(context.Background(), now); err != nil {
		t.Fatal(err)
	}
	var n int
	db.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM thread_snapshots`).Scan(&n)
	if n != 0 {
		t.Fatal(n)
	}
}

func TestAccelerationGateAndCapcodeTie(t *testing.T) {
	_, db, _, now := testWatcher(t, Config{MinReplyPosts: 10})
	for i := 0; i < 25; i++ {
		_, err := db.ExecContext(context.Background(), `INSERT INTO thread_snapshots (thread_key,threshold,board,elapsed_nanoseconds,acceleration_ppm,reply_characters,reply_attachments,capcode_replies,marta_replies,completed_at) VALUES (?,?,?,?,?,?,?,?,?,?)`, fmt.Sprintf("old:%d", i), 10, "test", (10 * time.Minute).Nanoseconds(), 1000000, 100, 10, 1, 0, storage.Time(now.Add(-time.Hour)))
		if err != nil {
			t.Fatal(err)
		}
	}
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	s := state{createdAt: now.Add(-30 * time.Minute), halfAt: now.Add(-5 * time.Minute), thresholdAt: now, capcodeReplies: 1}
	h, err := rankHighlight(context.Background(), tx, "current", 10, s, now)
	if err != nil {
		t.Fatal(err)
	}
	if h.Kind != localization.AccelerationHighlight {
		t.Fatalf("highlight = %#v", h)
	}
	s.halfAt = now.Add(-20 * time.Minute)
	s.thresholdAt = now.Add(-10 * time.Minute) // The second half is not twice as fast.
	h, err = rankHighlight(context.Background(), tx, "current", 10, s, now)
	if err != nil {
		t.Fatal(err)
	}
	if h.Kind != "" {
		t.Fatalf("highlight with acceleration gate = %#v", h)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
}
