package render_test

import (
	"bytes"
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/internal/eventlog"
	"github.com/xavidop/senro/internal/render"
	"github.com/xavidop/senro/internal/source"
)

// Every fixture in plain_test.go is a finished run, so Plain's
// live-subscription branch never executes there. This test drives Plain
// from a FileSource opened with follow=true over a ledger the test keeps
// appending to, the same composition `senro attach --follow` uses. It also
// drives run.finished through the live path, checks Plain's RETURNED status
// (the value --follow exits on), and asserts the snapshot's last line
// appears exactly once: a regression from st.Seq+1 to st.Seq would
// redeliver it as a live duplicate.
func TestPlainPrintsAStepThatFinishesAfterItStarted(t *testing.T) {
	dir := t.TempDir()
	l, err := eventlog.Open(dir)
	if err != nil {
		t.Fatalf("eventlog.Open: %v", err)
	}
	t.Cleanup(func() { _ = l.Close() })

	mustAppend := func(e api.Event) {
		t.Helper()
		if _, err := l.Append(e); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}

	// Everything up to step.started is on disk before Plain looks, so it
	// arrives through the snapshot, not the branch under test.
	mustAppend(api.Event{Type: api.RunStarted, Run: "run1", Payload: mustBody(api.RunStartedBody{
		Pipeline:      "ci",
		EngineVersion: "test",
		PlanDigest:    "digest1",
		StartedAt:     time.Now().UTC(),
	})})
	mustAppend(api.Event{Type: api.StepCreated, Run: "run1", Step: "build", Payload: mustBody(api.StepCreatedBody{
		Kind: "exec",
	})})
	mustAppend(api.Event{Type: api.StepStarted, Run: "run1", Step: "build", Attempt: 1})

	fs, err := source.OpenFile(dir, true)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	defer func() { _ = fs.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var buf syncBuffer
	var status api.RunStatus
	var plainErr error
	done := make(chan struct{})
	go func() {
		defer close(done)
		status, plainErr = render.Plain(ctx, fs, &buf)
	}()

	// The snapshot print proves State() has returned, so step.finished,
	// appended after this, can only arrive through Subscribe's live path.
	waitForSubstring(t, &buf, "build started\n", 2*time.Second)

	mustAppend(api.Event{Type: api.StepFinished, Run: "run1", Step: "build", Attempt: 1, Payload: mustBody(api.StepFinishedBody{
		State: api.StateSucceeded,
	})})
	waitForSubstring(t, &buf, "build succeeded\n", 2*time.Second)

	// FileSource's follow loop does not stop on run.finished, so Plain will
	// not return on its own; what can be checked while it still runs is
	// that run.finished's line reaches the live output and the fold behind
	// Plain's return value reflects it.
	mustAppend(api.Event{Type: api.RunFinished, Run: "run1", Payload: mustBody(api.RunFinishedBody{
		Status: api.RunSucceeded,
	})})
	waitForSubstring(t, &buf, "run succeeded\n", 2*time.Second)

	cancel()
	<-done // Plain must actually return once its Source stops delivering.

	if plainErr != nil {
		t.Fatalf("Plain: %v", plainErr)
	}
	if status != api.RunSucceeded {
		t.Errorf("Plain's returned status = %q, want %q — this is the value senro attach --follow exits on", status, api.RunSucceeded)
	}

	// The snapshot printed "build started" once; subscribing from st.Seq
	// instead of st.Seq+1 would print it a second time.
	if n := strings.Count(buf.String(), "build started\n"); n != 1 {
		t.Errorf(`"build started" line appeared %d times, want exactly 1 — the live subscription must not redeliver what the snapshot already printed:\n%s`, n, buf.String())
	}
}

// The live half of TestPlainPrintsStepOutputFromTheSnapshot: output that
// appears while the renderer is watching must reach the log as it arrives.
// Two steps run at once on purpose, which is why every line must name its
// step.
func TestPlainStreamsStepOutputAsItArrives(t *testing.T) {
	dir := t.TempDir()
	fx := newLoggedRun(t, dir)
	t.Cleanup(fx.close)

	// On disk before Plain looks, so delivered through the snapshot; every
	// log line below arrives through the live subscription instead.
	fx.append(api.Event{Type: api.RunStarted, Run: "run1", Payload: mustBody(api.RunStartedBody{
		Pipeline: "ci", EngineVersion: "test", PlanDigest: "digest1", StartedAt: time.Now().UTC(),
	})})
	fx.append(api.Event{Type: api.StepCreated, Run: "run1", Step: "lint", Payload: mustBody(api.StepCreatedBody{Kind: "exec"})})
	fx.append(api.Event{Type: api.StepCreated, Run: "run1", Step: "test", Payload: mustBody(api.StepCreatedBody{Kind: "exec"})})

	fs, err := source.OpenFile(dir, true)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	defer func() { _ = fs.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var buf syncBuffer
	var status api.RunStatus
	var plainErr error
	done := make(chan struct{})
	go func() {
		defer close(done)
		status, plainErr = render.Plain(ctx, fs, &buf)
	}()

	fx.append(api.Event{Type: api.StepStarted, Run: "run1", Step: "lint", Attempt: 1})
	fx.append(api.Event{Type: api.StepStarted, Run: "run1", Step: "test", Attempt: 1})
	waitForSubstring(t, &buf, "test started\n", 5*time.Second)

	fx.log("lint", 1, api.StreamStdout, "scanning 42 files\n")
	waitForSubstring(t, &buf, "lint stdout | scanning 42 files\n", 5*time.Second)

	// Interleaved deliberately, as two concurrent steps produce it.
	fx.log("test", 1, api.StreamStdout, "ok  	pkg/one\n")
	fx.log("lint", 1, api.StreamStderr, "style: line too long\n")
	fx.log("lint", 1, api.StreamStdout, "42 files, 1 problem\n")
	waitForSubstring(t, &buf, "lint stdout | 42 files, 1 problem\n", 5*time.Second)

	fx.append(api.Event{Type: api.StepFinished, Run: "run1", Step: "lint", Attempt: 1, Payload: mustBody(api.StepFinishedBody{
		State: api.StateFailed, ExitCode: 1, Error: "exit status 1",
	})})
	fx.append(api.Event{Type: api.StepFinished, Run: "run1", Step: "test", Attempt: 1, Payload: mustBody(api.StepFinishedBody{
		State: api.StateSucceeded,
	})})
	fx.append(api.Event{Type: api.RunFinished, Run: "run1", Payload: mustBody(api.RunFinishedBody{Status: api.RunFailed})})
	waitForSubstring(t, &buf, "run failed\n", 5*time.Second)

	cancel()
	<-done
	if plainErr != nil {
		t.Fatalf("Plain: %v", plainErr)
	}
	if status != api.RunFailed {
		t.Errorf("status = %q, want %q", status, api.RunFailed)
	}

	out := buf.String()
	for _, want := range []string{
		"lint stdout | scanning 42 files\n",
		"lint stderr | style: line too long\n",
		"lint stdout | 42 files, 1 problem\n",
		"test stdout | ok  	pkg/one\n",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("output missing %q:\n%s", want, out)
		}
	}
	// One step's output must never be attributed to the other, whatever
	// order the two interleaved in.
	if strings.Contains(out, "test stdout | scanning 42 files") || strings.Contains(out, "lint stdout | ok  	pkg/one") {
		t.Errorf("a line was attributed to the wrong step:\n%s", out)
	}
	if a, b := strings.Index(out, "scanning 42 files"), strings.Index(out, "42 files, 1 problem"); a > b {
		t.Errorf("one step's output must stay in the order it was produced:\n%s", out)
	}
	// A batching renderer must not reorder the verdict ahead of the
	// evidence.
	if o, s := strings.Index(out, "42 files, 1 problem"), strings.Index(out, "lint failed"); o > s {
		t.Errorf("a step's output must print before its terminal line:\n%s", out)
	}
	// Output must not be duplicated by the snapshot-then-subscribe seam.
	if n := strings.Count(out, "lint stdout | scanning 42 files\n"); n != 1 {
		t.Errorf("output line printed %d times, want exactly 1:\n%s", n, out)
	}
}

// --- helpers ---

// syncBuffer is a bytes.Buffer safe for one writer (Plain's goroutine) and
// one concurrent reader (the test, polling).
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// waitForSubstring polls b for want, failing the test if it does not appear
// within timeout.
func waitForSubstring(t *testing.T, b *syncBuffer, want string, timeout time.Duration) {
	t.Helper()
	deadline := time.After(timeout)
	for {
		if strings.Contains(b.String(), want) {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for %q in output; got:\n%s", want, b.String())
		case <-time.After(5 * time.Millisecond):
		}
	}
}

// TestPlainPrintsABreakpointThatIsHitLive is the live half of
// TestPlainReportsAStepHeldAtABreakpointFromASnapshot: a pause happening
// NOW in a watched log must not read as a hang. Driven like
// TestPlainPrintsAStepThatFinishesAfterItStarted, so breakpoint.hit can
// only arrive through Subscribe's live delivery.
func TestPlainPrintsABreakpointThatIsHitLive(t *testing.T) {
	dir := t.TempDir()
	l, err := eventlog.Open(dir)
	if err != nil {
		t.Fatalf("eventlog.Open: %v", err)
	}
	t.Cleanup(func() { _ = l.Close() })

	mustAppend := func(e api.Event) {
		t.Helper()
		if _, err := l.Append(e); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	mustAppend(api.Event{Type: api.RunStarted, Run: "run1", Payload: mustBody(api.RunStartedBody{
		Pipeline: "ci", EngineVersion: "test", StartedAt: time.Now().UTC(),
	})})
	mustAppend(api.Event{Type: api.StepCreated, Run: "run1", Step: "setup", Payload: mustBody(api.StepCreatedBody{Kind: "exec"})})
	mustAppend(api.Event{Type: api.StepStarted, Run: "run1", Step: "setup", Attempt: 1})

	fs, err := source.OpenFile(dir, true)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	defer func() { _ = fs.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var buf syncBuffer
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = render.Plain(ctx, fs, &buf)
	}()
	waitForSubstring(t, &buf, "setup started\n", 2*time.Second)

	mustAppend(api.Event{Type: api.StepFinished, Run: "run1", Step: "setup", Attempt: 1, Payload: mustBody(api.StepFinishedBody{
		State: api.StateSucceeded,
	})})
	mustAppend(api.Event{Type: api.BreakpointHit, Run: "run1", Step: "deploy", Payload: mustBody(api.BreakpointHitBody{
		ClientID: "operator",
	})})
	waitForSubstring(t, &buf, "deploy paused at breakpoint\n", 2*time.Second)

	// Released: the printed line must be the step's real outcome, never a
	// pause it has already come out of.
	mustAppend(api.Event{Type: api.StepStarted, Run: "run1", Step: "deploy", Attempt: 1})
	waitForSubstring(t, &buf, "deploy started\n", 2*time.Second)

	cancel()
	<-done
}
