package containerexec

import (
	"context"
	"io"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/xavidop/senro/internal/cas"
	"github.com/xavidop/senro/internal/dockerd/dockertest"
	senroexec "github.com/xavidop/senro/internal/executor"
	"github.com/xavidop/senro/internal/plan"
	"github.com/xavidop/senro/internal/workspace"
)

// slowWriter is a step's log writer that takes its time, so the daemon's log
// stream is still being drained when Run's grace period expires. late counts
// the bytes written after the test declared Run returned, which is exactly
// the thing that must never happen.
type slowWriter struct {
	delay time.Duration

	mu       sync.Mutex
	returned bool
	total    int
	late     int
}

func (w *slowWriter) Write(p []byte) (int, error) {
	time.Sleep(w.delay)
	w.mu.Lock()
	defer w.mu.Unlock()
	w.total += len(p)
	if w.returned {
		w.late += len(p)
	}
	return len(p), nil
}

func (w *slowWriter) markReturned() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.returned = true
}

func (w *slowWriter) counts() (total, late int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.total, w.late
}

// The container half of localexec's waitDelay contract: when Run returns,
// nothing this executor started may still be writing to the caller's
// writers. runAttempt flushes and closes them on that understanding, so the
// drain must be STOPPED, not merely stopped being waited for; a bare timeout
// on a channel reproduces the deadline and not the teardown.
//
// The setup forces the expiry branch: far more output than the writer can
// swallow inside a deliberately shortened grace period.
func TestRunDoesNotLeaveTheLogStreamWritingAfterItReturns(t *testing.T) {
	c := dockertest.Require(t)
	dockertest.Pull(t, c)

	prev := logDrainGrace
	logDrainGrace = 150 * time.Millisecond
	t.Cleanup(func() { logDrainGrace = prev })

	store, err := cas.Open(filepath.Join(t.TempDir(), "cas"))
	if err != nil {
		t.Fatal(err)
	}
	ex, err := New(plan.ExecutorSpec{Kind: plan.ExecutorContainer, Image: dockertest.Image},
		workspace.NewSnapshotter(store), WithClient(c), WithRunID("log-drain"))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	sb, err := ex.Sandbox(ctx, senroexec.SandboxSpec{StepID: "chatty", Attempt: 1})
	if err != nil {
		t.Fatalf("Sandbox: %v", err)
	}
	defer func() { _ = sb.Close(context.Background(), false) }()

	// Two megabytes arrives as roughly sixty-four 32KiB demux chunks, and at
	// 25ms a chunk that is about 1.6s of writing: an order of magnitude more
	// than the grace period above, so Run is guaranteed to give up waiting
	// while the drain is still in flight.
	out := &slowWriter{delay: 25 * time.Millisecond}
	code, err := sb.Run(ctx, senroexec.Cmd{
		Args: []string{"sh", "-c", "yes senro-log-drain-probe | head -c 2000000"},
	}, out, io.Discard)
	out.markReturned()
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if code != 0 {
		t.Fatalf("exit %d, want 0", code)
	}

	atReturn, _ := out.counts()
	if atReturn >= 2000000 {
		t.Fatalf("the whole log drained inside the grace period (%d bytes); this test proves "+
			"nothing about what happens when it does not", atReturn)
	}

	// Long enough that an abandoned drain would certainly have written more.
	time.Sleep(2 * time.Second)
	total, late := out.counts()
	if late != 0 {
		t.Errorf("%d byte(s) were written to the step's log writer after Run returned "+
			"(%d at return, %d in total); the log stream was abandoned rather than stopped, so the "+
			"engine flushes and closes a writer something else still holds", late, atReturn, total)
	}
}
