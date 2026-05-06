package retry

import (
	"context"
	"time"
)

// DefaultDelays are used when no retry delays are provided.
var DefaultDelays = []time.Duration{2 * time.Second, 3 * time.Second, 5 * time.Second}

// Do runs fn once, then retries after each delay until success or context cancellation.
// Passing delays overrides DefaultDelays.
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
