package notify

import (
	"context"
	"errors"
	"fmt"
	"time"
)

type Sender interface {
	Send(context.Context, int64, string) error
}
type Metrics interface {
	ObserveNotificationDelivery(string)
	SetPendingNotifications(int, time.Duration)
}

type Notifier struct {
	Store   *Store
	Sender  Sender
	Now     func() time.Time
	Metrics Metrics
}

func (n Notifier) Run(ctx context.Context) error {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		if err := n.Deliver(ctx); err != nil && ctx.Err() == nil {
			return err
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

func (n Notifier) Deliver(ctx context.Context) error {
	now := n.now()
	items, err := n.Store.Due(ctx, now, 10)
	if err != nil {
		return err
	}
	for _, item := range items {
		deliveryErr := n.Sender.Send(ctx, item.Target, item.Text)
		if deliveryErr == nil {
			if n.Metrics != nil {
				n.Metrics.ObserveNotificationDelivery("success")
			}
			if err := n.Store.Delivered(ctx, item.ID, n.now()); err != nil {
				return err
			}
			continue
		}
		var classified interface{ Retryable() bool }
		if errors.As(deliveryErr, &classified) && !classified.Retryable() {
			if n.Metrics != nil {
				n.Metrics.ObserveNotificationDelivery("permanent_error")
			}
			if err := n.Store.Failed(ctx, item.ID, n.now()); err != nil {
				return err
			}
			continue
		}
		attempts := item.Attempts + 1
		if n.Metrics != nil {
			n.Metrics.ObserveNotificationDelivery("error")
		}
		delay := backoff(attempts)
		var retryable interface{ RetryDelay() time.Duration }
		if errors.As(deliveryErr, &retryable) && retryable.RetryDelay() > delay {
			delay = retryable.RetryDelay()
		}
		if err := n.Store.Retry(ctx, item.ID, attempts, n.now().Add(delay)); err != nil {
			return fmt.Errorf("reschedule failed notification: %w", err)
		}
	}
	count, age, err := n.Store.PendingStats(ctx, n.now())
	if err != nil {
		return err
	}
	if n.Metrics != nil {
		n.Metrics.SetPendingNotifications(count, age)
	}
	return nil
}

func (n Notifier) now() time.Time {
	if n.Now != nil {
		return n.Now()
	}
	return time.Now().UTC()
}

func backoff(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	if attempt > 7 {
		attempt = 7
	}
	return time.Second * time.Duration(1<<(attempt-1))
}
