package retry_test

import (
	"context"
	"errors"
	"testing"

	"github.com/DanMke/event-processing-platform/internal/retry"
)

func TestDo_SuccessOnFirstAttempt(t *testing.T) {
	calls := 0
	err := retry.Do(context.Background(), func() error {
		calls++
		return nil
	}, 0, 0, 0)

	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if calls != 1 {
		t.Errorf("expected 1 call, got %d", calls)
	}
}

func TestDo_SuccessOnRetry(t *testing.T) {
	calls := 0
	err := retry.Do(context.Background(), func() error {
		calls++
		if calls < 3 {
			return errors.New("transient")
		}
		return nil
	}, 0, 0, 0)

	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if calls != 3 {
		t.Errorf("expected 3 calls, got %d", calls)
	}
}

func TestDo_ExhaustsAllRetries(t *testing.T) {
	calls := 0
	err := retry.Do(context.Background(), func() error {
		calls++
		return errors.New("always fails")
	}, 0, 0, 0)

	if err == nil {
		t.Error("expected error after exhausting retries, got nil")
	}
	// Initial call plus three retries.
	if calls != 4 {
		t.Errorf("expected 4 calls, got %d", calls)
	}
}

func TestDo_ContextCancelledStopsRetry(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	calls := 0
	err := retry.Do(ctx, func() error {
		calls++
		return errors.New("fails")
	}, 0, 0, 0)

	if err == nil {
		t.Error("expected error after context cancellation, got nil")
	}
	if calls < 1 {
		t.Errorf("expected at least 1 call, got %d", calls)
	}
}
