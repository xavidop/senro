package main

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/xavidop/senro/internal/attachsrv"
)

// These tests pin two deadlocks, both reproduced under ordinary load.
//
// First: waitForRegistrationOrExit carried a fixed deadline, after which
// runPipeline fell back to an unconditional `<-waitDone` with no further
// attempt to attach. For a pipeline built with WaitForClient: true, the
// only thing that could satisfy WaitForClient was exactly the connection
// this package had just given up making. Fixed by removing the deadline:
// attachsrv.Register happens before WaitForClient's block, so a slow
// registration means a slow process, not one that will never register.
//
// Second, found immediately after: an interim waitForRegistrationOrExit
// returned only (entry, found), assuming runPipeline could tell "exited"
// from "cancelled" with a non-blocking peek at waitDone. The `case
// <-waitDone:` arm had already consumed the single buffered value, so the
// peek found nothing, killed an already-dead process, and read waitDone a
// second time, which nothing would ever send to. Hence the three-value
// return; TestWaitForRegistrationOrExitDoesNotDoubleDrainWaitDone pins
// it.

func TestWaitForRegistrationOrExitFindsALateRegistration(t *testing.T) {
	isolateRegistry(t)
	pid := liveSubprocess(t)

	// Nothing registered when this call starts: a process slow to reach
	// attach.Listen, registering well past what the old hard-coded
	// deadline allowed.
	registered := make(chan struct{})
	go func() {
		time.Sleep(650 * time.Millisecond)
		registerEntry(t, attachsrv.Entry{
			PID: pid, Socket: filepath.Join(t.TempDir(), "s.sock"), RunID: "late-registration",
		})
		close(registered)
	}()

	waitDone := make(chan error) // the process is alive (liveSubprocess) and never exits on its own here
	entry, found, exited := waitForRegistrationOrExit(context.Background(), pid, waitDone)
	if !found {
		t.Fatal("found = false, want true — a registration that arrives late must still be picked up, not missed by a deadline that already gave up")
	}
	if exited {
		t.Error("exited = true alongside found = true — waitDone must be untouched here")
	}
	if entry.RunID != "late-registration" {
		t.Errorf("RunID = %q, want %q", entry.RunID, "late-registration")
	}
	<-registered
}

func TestWaitForRegistrationOrExitReturnsOnProcessExitWithoutEverRegistering(t *testing.T) {
	isolateRegistry(t)
	waitDone := make(chan error, 1)
	waitDone <- nil // the process already exited; nothing was ever registered for it

	_, found, exited := waitForRegistrationOrExit(context.Background(), 999999, waitDone)
	if found {
		t.Fatal("found = true, want false — nothing was ever registered for this pid")
	}
	if !exited {
		t.Fatal("exited = false, want true — the process exit is what ended this call")
	}
}

// TestWaitForRegistrationOrExitDoesNotDoubleDrainWaitDone pins the second
// hang: exited=true must mean waitDone is already fully consumed.
// Reverting to a two-value return reads as a harmless simplification and
// would pass every other test here; only a test that reads waitDone again,
// as the real bug did, catches it.
func TestWaitForRegistrationOrExitDoesNotDoubleDrainWaitDone(t *testing.T) {
	isolateRegistry(t)
	waitDone := make(chan error, 1)
	waitDone <- nil

	_, found, exited := waitForRegistrationOrExit(context.Background(), 999999, waitDone)
	if found || !exited {
		t.Fatalf("found=%v exited=%v, want found=false exited=true", found, exited)
	}

	// What runPipeline's !found branch does when exited is true: nothing.
	// Reading waitDone again would hang forever, so assert it directly
	// with a bounded wait rather than trust the contract.
	select {
	case <-waitDone:
		t.Fatal("a second read of waitDone succeeded — it should have been fully drained already")
	case <-time.After(100 * time.Millisecond):
		// Correct: nothing more was ever going to arrive.
	}
}

// TestWaitForRegistrationOrExitRespectsContextCancellation keeps Ctrl-C
// effective while waiting on a registration that may never come. Without
// it, removing the old deadline would trade one hang for another.
func TestWaitForRegistrationOrExitRespectsContextCancellation(t *testing.T) {
	isolateRegistry(t)
	ctx, cancel := context.WithCancel(context.Background())
	waitDone := make(chan error) // never fires

	type result struct{ found, exited bool }
	done := make(chan result, 1)
	go func() {
		_, found, exited := waitForRegistrationOrExit(ctx, 999999, waitDone)
		done <- result{found, exited}
	}()

	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case r := <-done:
		if r.found {
			t.Error("found = true after ctx cancellation, want false")
		}
		if r.exited {
			t.Error("exited = true after ctx cancellation (process never exited), want false — waitDone must still be untouched so the caller can kill-then-reap")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("waitForRegistrationOrExit did not return after ctx was cancelled")
	}
}
