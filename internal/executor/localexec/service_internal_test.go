//go:build unix

package localexec

import (
	"context"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

// A Stop that lands after the group was swept but before done closes (the
// output still draining) sends nothing: the process was reaped, so its id
// may already belong to someone else. It returns once done closes. The
// window is set up directly: swept, done still open.
func TestStopAfterTheSweepSignalsNothing(t *testing.T) {
	var sent atomic.Int32
	orig := killGroup
	killGroup = func(int, syscall.Signal) error {
		sent.Add(1)
		return nil
	}
	t.Cleanup(func() { killGroup = orig })

	l := &localService{pgid: 4242, done: make(chan struct{}), swept: true}
	stopped := make(chan error, 1)
	go func() { stopped <- l.Stop(context.Background(), time.Minute) }()

	select {
	case err := <-stopped:
		t.Fatalf("Stop returned (%v) before done closed", err)
	case <-time.After(100 * time.Millisecond):
	}
	close(l.done)
	select {
	case err := <-stopped:
		if err != nil {
			t.Fatalf("Stop: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Stop did not return once done closed")
	}
	if n := sent.Load(); n != 0 {
		t.Errorf("Stop sent %d signal(s) to a swept group, want none", n)
	}
}
