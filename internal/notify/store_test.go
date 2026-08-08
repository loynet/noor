package notify

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"noor/internal/storage"
)

func TestEnqueueDeduplicatesAndSchedules(t *testing.T) {
	db, err := storage.Open(filepath.Join(t.TempDir(), "noor.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	store, err := OpenStore(db)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	queued, err := store.Enqueue(context.Background(), "thread:b:1", 1, "hello", now)
	if err != nil || !queued {
		t.Fatalf("first enqueue = (%v, %v), want (true, nil)", queued, err)
	}
	queued, err = store.Enqueue(context.Background(), "thread:b:1", 1, "hello", now)
	if err != nil || queued {
		t.Fatalf("second enqueue = (%v, %v), want (false, nil)", queued, err)
	}
	due, err := store.Due(context.Background(), now, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(due) != 1 || due[0].Text != "hello" {
		t.Fatalf("due = %#v", due)
	}
}
