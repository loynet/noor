package notify

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"noor/internal/storage"
)

func TestDeliverStopsRetryingKnownPermanentError(t *testing.T) {
	db, err := storage.Open(filepath.Join(t.TempDir(), "noor.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	store, err := OpenStore(db)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 8, 6, 12, 0, 0, 0, time.UTC)
	if _, err := store.Enqueue(context.Background(), "test", 1, "hello", now); err != nil {
		t.Fatal(err)
	}
	notifier := Notifier{Store: store, Sender: permanentSender{}, Now: func() time.Time { return now }}
	if err := notifier.Deliver(context.Background()); err != nil {
		t.Fatal(err)
	}
	due, err := store.Due(context.Background(), now.Add(time.Hour), 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(due) != 0 {
		t.Fatalf("due notifications = %d, want 0", len(due))
	}
}

func TestDeliverRetriesUnknownOutcomeThenDelivers(t *testing.T) {
	db, err := storage.Open(filepath.Join(t.TempDir(), "noor.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	store, err := OpenStore(db)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 8, 6, 12, 0, 0, 0, time.UTC)
	if _, err := store.Enqueue(context.Background(), "test", 1, "hello", now); err != nil {
		t.Fatal(err)
	}
	sender := &sequenceSender{results: []error{errors.New("request timed out"), nil}}
	notifier := Notifier{Store: store, Sender: sender, Now: func() time.Time { return now }}
	if err := notifier.Deliver(context.Background()); err != nil {
		t.Fatal(err)
	}
	due, err := store.Due(context.Background(), now, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(due) != 0 {
		t.Fatalf("due notifications after unknown outcome = %d, want 0", len(due))
	}
	now = now.Add(time.Second)
	due, err = store.Due(context.Background(), now, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(due) != 1 || due[0].Attempts != 1 {
		t.Fatalf("rescheduled notification = %#v, want one notification with one attempt", due)
	}
	if err := notifier.Deliver(context.Background()); err != nil {
		t.Fatal(err)
	}
	due, err = store.Due(context.Background(), now.Add(time.Hour), 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(due) != 0 || sender.calls != 2 {
		t.Fatalf("due notifications = %d, sender calls = %d; want 0 and 2", len(due), sender.calls)
	}
}

type permanentSender struct{}

func (permanentSender) Send(context.Context, int64, string) error { return permanentError{} }

type permanentError struct{}

func (permanentError) Error() string   { return errors.New("rejected").Error() }
func (permanentError) Retryable() bool { return false }

type sequenceSender struct {
	results []error
	calls   int
}

func (s *sequenceSender) Send(context.Context, int64, string) error {
	result := s.results[s.calls]
	s.calls++
	return result
}
