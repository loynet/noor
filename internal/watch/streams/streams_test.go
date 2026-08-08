package streams

import (
	"context"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"noor/internal/notify"
	"noor/internal/storage"
)

func TestWatcherQueuesOneNotificationPerLivePeriod(t *testing.T) {
	live := false
	db, err := storage.Open(filepath.Join(t.TempDir(), "noor.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	store, err := notify.OpenStore(db)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	client := &http.Client{Transport: roundTripper(func(*http.Request) (*http.Response, error) {
		status := http.StatusNotFound
		if live {
			status = http.StatusOK
		}
		return &http.Response{StatusCode: status, Body: http.NoBody}, nil
	})}
	watcher := &Watcher{Channels: []Channel{{Key: "main", ProbeURL: "https://probe.test/main", PageURL: "https://example.test/main"}}, Target: 1, DB: db, Client: client, Now: func() time.Time { return now }}
	if err := watcher.Initialize(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := watcher.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	live = true
	if err := watcher.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := watcher.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	due, err := store.Due(context.Background(), now, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(due) != 1 {
		t.Fatalf("notifications = %d, want 1", len(due))
	}
}

func TestWatcherDoesNotAnnounceInitiallyLiveStream(t *testing.T) {
	live := true
	db, err := storage.Open(filepath.Join(t.TempDir(), "noor.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	store, err := notify.OpenStore(db)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	client := &http.Client{Transport: roundTripper(func(*http.Request) (*http.Response, error) {
		status := http.StatusNotFound
		if live {
			status = http.StatusOK
		}
		return &http.Response{StatusCode: status, Body: http.NoBody}, nil
	})}
	watcher := &Watcher{Channels: []Channel{{Key: "main", ProbeURL: "https://probe.test/main", PageURL: "https://example.test/main"}}, Target: 1, DB: db, Client: client, Now: func() time.Time { return now }}
	if err := watcher.Initialize(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := watcher.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	due, err := store.Due(context.Background(), now, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(due) != 0 {
		t.Fatalf("initially live notifications = %d, want 0", len(due))
	}
	live = false
	if err := watcher.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := watcher.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	live = true
	if err := watcher.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	due, err = store.Due(context.Background(), now, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(due) != 1 {
		t.Fatalf("notifications after new live period = %d, want 1", len(due))
	}
}

type roundTripper func(*http.Request) (*http.Response, error)

func (f roundTripper) RoundTrip(request *http.Request) (*http.Response, error) { return f(request) }
