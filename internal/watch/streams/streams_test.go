package streams

import (
	"context"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"noor/internal/notify"
	"noor/internal/storage"
)

func TestWatcherQueuesOnLiveTransitionsAndNameChanges(t *testing.T) {
	response := `{"live":false}`
	statusCode := http.StatusOK
	watcher, store, now := testWatcher(t, &response, &statusCode)
	ctx := context.Background()
	if err := watcher.Poll(ctx); err != nil {
		t.Fatal(err)
	}
	response = `{"live":true,"name":"l29utp0"}`
	if err := watcher.Poll(ctx); err != nil {
		t.Fatal(err)
	}
	watcher = &Watcher{Target: watcher.Target, DB: watcher.DB, Client: watcher.Client, Now: watcher.Now}
	if err := watcher.Initialize(ctx); err != nil {
		t.Fatal(err)
	}
	if err := watcher.Poll(ctx); err != nil {
		t.Fatal(err)
	}
	due, err := store.Due(ctx, now, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(due) != 1 || due[0].Text != "🔴 Stream live\nhttps://miau.gg/l29utp0" {
		t.Fatalf("first live period: %+v", due)
	}

	response = `{"live":true,"name":"second"}`
	if err := watcher.Poll(ctx); err != nil {
		t.Fatal(err)
	}
	due, err = store.Due(ctx, now, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(due) != 2 || due[1].Text != "🔴 Stream live\nhttps://miau.gg/second" {
		t.Fatalf("name change: %+v", due)
	}

	response = `{"live":false}`
	if err := watcher.Poll(ctx); err != nil {
		t.Fatal(err)
	}
	watcher = &Watcher{Target: watcher.Target, DB: watcher.DB, Client: watcher.Client, Now: watcher.Now}
	if err := watcher.Initialize(ctx); err != nil {
		t.Fatal(err)
	}
	response = `{"live":true,"name":"second"}`
	if err := watcher.Poll(ctx); err != nil {
		t.Fatal(err)
	}
	due, err = store.Due(ctx, now, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(due) != 3 || due[2].Text != "🔴 Stream live\nhttps://miau.gg/second" {
		t.Fatalf("new live period: %+v", due)
	}
}

func TestWatcherDoesNotAnnounceInitiallyLiveStream(t *testing.T) {
	response := `{"live":true,"name":"initial"}`
	statusCode := http.StatusOK
	watcher, store, now := testWatcher(t, &response, &statusCode)
	ctx := context.Background()
	if err := watcher.Poll(ctx); err != nil {
		t.Fatal(err)
	}
	due, err := store.Due(ctx, now, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(due) != 0 {
		t.Fatalf("initially live notifications = %d, want 0", len(due))
	}
	response = `{"live":false}`
	if err := watcher.Poll(ctx); err != nil {
		t.Fatal(err)
	}
	response = `{"live":true,"name":"next"}`
	if err := watcher.Poll(ctx); err != nil {
		t.Fatal(err)
	}
	due, err = store.Due(ctx, now, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(due) != 1 {
		t.Fatalf("notifications after new live period = %d, want 1", len(due))
	}
}

func TestInvalidStatusDoesNotMarkStreamOffline(t *testing.T) {
	response := `{"live":true,"name":"initial"}`
	statusCode := http.StatusOK
	watcher, store, now := testWatcher(t, &response, &statusCode)
	ctx := context.Background()
	if err := watcher.Poll(ctx); err != nil {
		t.Fatal(err)
	}
	for _, bad := range []struct {
		body string
		code int
	}{
		{`{"live":true}`, http.StatusOK},
		{`{"name":"missing-live"}`, http.StatusOK},
		{`{"live":true,"name":"bad\nname"}`, http.StatusOK},
		{`not json`, http.StatusOK},
		{`{"live":false}`, http.StatusBadGateway},
		{strings.Repeat("x", maxStatusBytes+1), http.StatusOK},
	} {
		response, statusCode = bad.body, bad.code
		if err := watcher.Poll(ctx); err == nil {
			t.Fatalf("expected error for HTTP %d body %q", bad.code, bad.body[:min(len(bad.body), 40)])
		}
	}
	response, statusCode = `{"live":true,"name":"initial"}`, http.StatusOK
	if err := watcher.Poll(ctx); err != nil {
		t.Fatal(err)
	}
	due, err := store.Due(ctx, now, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(due) != 0 {
		t.Fatalf("notifications after invalid statuses = %d, want 0", len(due))
	}
}

func testWatcher(t *testing.T, response *string, statusCode *int) (*Watcher, *notify.Store, time.Time) {
	t.Helper()
	db, err := storage.Open(filepath.Join(t.TempDir(), "noor.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	store, err := notify.OpenStore(db)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	client := &http.Client{Transport: roundTripper(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: *statusCode, Body: io.NopCloser(strings.NewReader(*response))}, nil
	})}
	watcher := &Watcher{Target: 1, DB: db, Client: client, Now: func() time.Time { return now }}
	if err := watcher.Initialize(context.Background()); err != nil {
		t.Fatal(err)
	}
	return watcher, store, now
}

type roundTripper func(*http.Request) (*http.Response, error)

func (f roundTripper) RoundTrip(request *http.Request) (*http.Response, error) { return f(request) }
