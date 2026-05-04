package retry

import (
	"context"
	"time"
)

// DefaultDelays defines the wait durations between successive retries.
var DefaultDelays = []time.Duration{2 * time.Second, 3 * time.Second, 5 * time.Second}

// Do calls fn immediately, then retries up to len(delays) times with increasing waits.
// Stops early on success or context cancellation.
// Pass explicit delays to override DefaultDelays (useful in tests).
func Do(ctx context.Context, fn func() error, delays ...time.Duration) error {
	if len(delays) == 0 {
		delays = DefaultDelays
	}

	err := fn()
	for _, delay := range delays {
		if err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
		err = fn()
	}
	return err
}
