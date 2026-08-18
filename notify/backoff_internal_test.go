package notify

import (
	"context"
	"testing"
	"time"
)

// TestBackoffGrowsAndIsJittered pins both halves of "retry with jitter":
// growth without jitter synchronises a fleet onto the same retry instants,
// and jitter without growth is a random hammer. The draw is over [d/2, d),
// not the whole interval; see backoff.
func TestBackoffGrowsAndIsJittered(t *testing.T) {
	const base = 100 * time.Millisecond

	for attempt := 1; attempt <= 4; attempt++ {
		nominal := base << (attempt - 1)
		lo, hi := nominal/2, nominal
		distinct := make(map[time.Duration]bool)
		for range 200 {
			d := backoff(attempt, base)
			if d < lo || d >= hi {
				t.Fatalf("backoff(%d, %s) = %s, want [%s, %s)", attempt, base, d, lo, hi)
			}
			distinct[d] = true
		}
		if len(distinct) < 2 {
			t.Errorf("backoff(%d, %s) returned one value in 200 draws (%v): there is no jitter", attempt, base, distinct)
		}
	}
}

// TestBackoffIsCapped: a wait longer than the cap could only ever be
// interrupted, never served.
func TestBackoffIsCapped(t *testing.T) {
	for _, attempt := range []int{8, 20, 64} {
		if d := backoff(attempt, time.Second); d > maxBackoff {
			t.Errorf("backoff(%d, 1s) = %s, want at most %s", attempt, d, maxBackoff)
		}
	}
}

// TestWaitGivesUpWhenTheNotifierIsShuttingDown: a retry wait has to be
// interruptible or Flush's grace is a suggestion.
func TestWaitGivesUpWhenTheNotifierIsShuttingDown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	start := time.Now()
	if wait(ctx, time.Minute) {
		t.Error("wait reported that a full minute elapsed on a cancelled context")
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("wait took %s to notice a cancelled context", elapsed)
	}
}

// TestRetryableSeparatesUnavailableFromUnwilling: the distinction the whole
// retry policy rests on.
func TestRetryableSeparatesUnavailableFromUnwilling(t *testing.T) {
	for _, status := range []int{0, 429, 500, 502, 503, 504} {
		if !retryable(status) {
			t.Errorf("status %d should be retryable", status)
		}
	}
	for _, status := range []int{200, 201, 204, 301, 400, 401, 403, 404, 410, 422} {
		if retryable(status) {
			t.Errorf("status %d should not be retryable", status)
		}
	}
}
