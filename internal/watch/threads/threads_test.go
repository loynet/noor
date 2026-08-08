package threads

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	gateway "github.com/loynet/ptchan-gateway/clients/go"
	"noor/internal/notify"
	"noor/internal/storage"
)

func TestWatcherWaitsForOPAndDeduplicates(t *testing.T) {
	db, err := storage.Open(filepath.Join(t.TempDir(), "noor.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	queue, err := notify.OpenStore(db)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	watcher := &Watcher{Config: Config{Target: 1, BaseURL: "https://ptchan.org", MinReplyPosts: 2, EventRetention: time.Hour}, DB: db, Now: func() time.Time { return now }}
	if err := watcher.Initialize(context.Background()); err != nil {
		t.Fatal(err)
	}
	post := func(id int64) gateway.WebhookEvent {
		return gateway.WebhookEvent{EventID: fmt.Sprintf("post-%d", id), Kind: gateway.PostCreated, Source: "ptchan", ObservedAt: now, Post: gateway.Post{Board: "test", ThreadID: 1, PostID: id, Date: now}}
	}
	if err := watcher.Consume(context.Background(), post(2)); err != nil {
		t.Fatal(err)
	}
	if err := watcher.Consume(context.Background(), post(3)); err != nil {
		t.Fatal(err)
	}
	if due, err := queue.Due(context.Background(), now, 10); err != nil || len(due) != 0 {
		t.Fatalf("notifications before OP = %#v, %v", due, err)
	}
	op := gateway.WebhookEvent{EventID: "op", Kind: gateway.ThreadCreated, Source: "ptchan", ObservedAt: now, Post: gateway.Post{Board: "test", ThreadID: 1, PostID: 1, Date: now}}
	if err := watcher.Consume(context.Background(), op); err != nil {
		t.Fatal(err)
	}
	if err := watcher.Consume(context.Background(), op); err != nil {
		t.Fatal(err)
	}
	due, err := queue.Due(context.Background(), now, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(due) != 1 {
		t.Fatalf("notifications = %d, want 1", len(due))
	}
}
