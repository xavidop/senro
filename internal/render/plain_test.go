package render_test

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/internal/eventlog"
	"github.com/xavidop/senro/internal/render"
	"github.com/xavidop/senro/internal/source"
)

// The plain renderer is a Source client, never an engine code path: a
// second path would let a TTY run and a CI log drift in what they report.
func TestPlainRendersFromAFileSource(t *testing.T) {
	dir := writeRun(t, twoStepRun())
	fs, _ := source.OpenFile(dir, false)
	defer func() { _ = fs.Close() }()

	var buf bytes.Buffer
	status, err := render.Plain(context.Background(), fs, &buf)
	if err != nil {
		t.Fatalf("Plain: %v", err)
	}
	if status != api.RunSucceeded {
		t.Errorf("status = %s, want succeeded", status)
	}

	out := buf.String()
	for _, want := range []string{"setup", "build"} {
		if !strings.Contains(out, want) {
			t.Errorf("output missing %q:\n%s", want, out)
		}
	}
	// A full line, not a bare "succeeded" substring: "setup succeeded"
	// would satisfy a loose check with printRun disabled entirely.
	if !strings.Contains(out, "run succeeded\n") {
		t.Errorf("output missing the run's final summary line %q:\n%s", "run succeeded", out)
	}
}

func TestPlainReportsAFailedRunAndNamesTheCause(t *testing.T) {
	dir := writeRun(t, failedRun()) // "deploy" exits 9
	fs, _ := source.OpenFile(dir, false)
	defer func() { _ = fs.Close() }()

	var buf bytes.Buffer
	status, _ := render.Plain(context.Background(), fs, &buf)
	if status != api.RunFailed {
		t.Errorf("status = %s, want failed", status)
	}
	out := buf.String()
	// The full line: a bare "deploy" check passes whether or not the
	// failure reason (s.Error) is ever printed, and the cause is the point.
	if !strings.Contains(out, "deploy failed: exit status 9\n") {
		t.Errorf("a failed run must name the step and the cause:\n%s", out)
	}
}

// A recovered run is not a clean run, and the renderer is where an operator
// finds that out.
func TestPlainDistinguishesRecovery(t *testing.T) {
	dir := writeRun(t, recoveredRun())
	fs, _ := source.OpenFile(dir, false)
	defer func() { _ = fs.Close() }()

	var buf bytes.Buffer
	status, _ := render.Plain(context.Background(), fs, &buf)
	if status != api.RunSucceededWithRecovery {
		t.Errorf("status = %s, want succeeded_with_recovery", status)
	}
	// The full line, so a formatting regression cannot hide behind an
	// unrelated match.
	if !strings.Contains(buf.String(), "flaky recovered\n") {
		t.Errorf("a recovered run must say so:\n%s", buf.String())
	}
}

// A CI log with no step output tells its reader nothing about why a
// failure failed. This is the snapshot half: every byte was on disk before
// the renderer looked, which is what an offline attach and a late State()
// both read; a renderer handling only live markers would drop it all.
func TestPlainPrintsStepOutputFromTheSnapshot(t *testing.T) {
	dir := t.TempDir()
	fx := newLoggedRun(t, dir)
	fx.append(api.Event{Type: api.RunStarted, Run: "run1", Payload: mustBody(api.RunStartedBody{
		Pipeline: "ci", EngineVersion: "test", PlanDigest: "digest1", StartedAt: time.Now().UTC(),
	})})
	fx.append(api.Event{Type: api.StepCreated, Run: "run1", Step: "unit", Payload: mustBody(api.StepCreatedBody{Kind: "exec"})})
	fx.append(api.Event{Type: api.StepStarted, Run: "run1", Step: "unit", Attempt: 1})
	fx.log("unit", 1, api.StreamStdout, "ok  	pkg/one	0.01s\n")
	fx.log("unit", 1, api.StreamStderr, "warning: deprecated flag\n")
	fx.log("unit", 1, api.StreamStdout, "FAIL	pkg/two	0.02s\n")
	fx.append(api.Event{Type: api.StepFinished, Run: "run1", Step: "unit", Attempt: 1, Payload: mustBody(api.StepFinishedBody{
		State: api.StateFailed, ExitCode: 1, Error: "exit status 1",
	})})
	fx.append(api.Event{Type: api.RunFinished, Run: "run1", Payload: mustBody(api.RunFinishedBody{Status: api.RunFailed})})
	fx.close()

	f, err := source.OpenFile(dir, false)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	defer func() { _ = f.Close() }()

	var buf bytes.Buffer
	if _, err := render.Plain(context.Background(), f, &buf); err != nil {
		t.Fatalf("Plain: %v", err)
	}
	out := buf.String()

	// Every line is attributed to its step and stream: unattributed
	// interleaved output invites reading one step's error as another's.
	for _, want := range []string{
		"unit stdout | ok  	pkg/one	0.01s\n",
		"unit stdout | FAIL	pkg/two	0.02s\n",
		"unit stderr | warning: deprecated flag\n",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("output missing %q:\n%s", want, out)
		}
	}
	// stdout and stderr are separate files; collapsing them throws away a
	// distinction the event stream already pays to carry.
	if strings.Contains(out, "unit stdout | warning: deprecated flag") {
		t.Errorf("a stderr line was attributed to stdout:\n%s", out)
	}
	// Ordering within one step's stream is the one ordering guarantee a
	// reader can rely on (across steps, interleaving is the truth).
	if a, b := strings.Index(out, "pkg/one"), strings.Index(out, "pkg/two"); a > b {
		t.Errorf("a step's own output must stay in order:\n%s", out)
	}
	// The output arrives before the verdict: the evidence belongs above.
	if o, s := strings.Index(out, "pkg/two"), strings.Index(out, "unit failed"); o > s {
		t.Errorf("a step's output must print before its terminal line:\n%s", out)
	}
}

// A retry writes a different file (logs/<step>/<attempt>/<stream>). One
// cursor per step rather than per attempt would treat the second attempt's
// output as already printed: exactly the output an operator most wants.
func TestPlainPrintsEveryAttemptsOutput(t *testing.T) {
	dir := t.TempDir()
	fx := newLoggedRun(t, dir)
	fx.append(api.Event{Type: api.RunStarted, Run: "run1", Payload: mustBody(api.RunStartedBody{
		Pipeline: "ci", EngineVersion: "test", PlanDigest: "digest1", StartedAt: time.Now().UTC(),
	})})
	fx.append(api.Event{Type: api.StepCreated, Run: "run1", Step: "flaky", Payload: mustBody(api.StepCreatedBody{Kind: "exec"})})
	fx.append(api.Event{Type: api.StepStarted, Run: "run1", Step: "flaky", Attempt: 1})
	fx.log("flaky", 1, api.StreamStderr, "connection reset by peer\n")
	fx.append(api.Event{Type: api.StepRetried, Run: "run1", Step: "flaky", Attempt: 2, Payload: mustBody(api.StepRetriedBody{
		Attempt: 2, Reason: "exit status 1", Predicate: "OnInfra",
	})})
	fx.append(api.Event{Type: api.StepStarted, Run: "run1", Step: "flaky", Attempt: 2})
	fx.log("flaky", 2, api.StreamStdout, "uploaded 3 artifacts\n")
	fx.append(api.Event{Type: api.StepFinished, Run: "run1", Step: "flaky", Attempt: 2, Payload: mustBody(api.StepFinishedBody{
		State: api.StateRecovered,
	})})
	fx.append(api.Event{Type: api.RunFinished, Run: "run1", Payload: mustBody(api.RunFinishedBody{
		Status: api.RunSucceededWithRecovery,
	})})
	fx.close()

	f, err := source.OpenFile(dir, false)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	defer func() { _ = f.Close() }()

	var buf bytes.Buffer
	if _, err := render.Plain(context.Background(), f, &buf); err != nil {
		t.Fatalf("Plain: %v", err)
	}
	if out := buf.String(); !strings.Contains(out, "flaky stdout | uploaded 3 artifacts\n") {
		t.Errorf("the retried attempt's own output is missing:\n%s", out)
	}
}

// A command killed halfway through a line still wrote those bytes, often
// the most interesting in the log; waiting for a newline that never comes
// would drop them.
func TestPlainPrintsAnUnterminatedFinalLine(t *testing.T) {
	dir := t.TempDir()
	fx := newLoggedRun(t, dir)
	fx.append(api.Event{Type: api.RunStarted, Run: "run1", Payload: mustBody(api.RunStartedBody{
		Pipeline: "ci", EngineVersion: "test", PlanDigest: "digest1", StartedAt: time.Now().UTC(),
	})})
	fx.append(api.Event{Type: api.StepCreated, Run: "run1", Step: "probe", Payload: mustBody(api.StepCreatedBody{Kind: "exec"})})
	fx.append(api.Event{Type: api.StepStarted, Run: "run1", Step: "probe", Attempt: 1})
	fx.log("probe", 1, api.StreamStdout, "waiting for healthcheck")
	fx.append(api.Event{Type: api.StepFinished, Run: "run1", Step: "probe", Attempt: 1, Payload: mustBody(api.StepFinishedBody{
		State: api.StateFailed, ExitCode: 137, Error: "signal: killed",
	})})
	fx.append(api.Event{Type: api.RunFinished, Run: "run1", Payload: mustBody(api.RunFinishedBody{Status: api.RunFailed})})
	fx.close()

	f, err := source.OpenFile(dir, false)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	defer func() { _ = f.Close() }()

	var buf bytes.Buffer
	if _, err := render.Plain(context.Background(), f, &buf); err != nil {
		t.Fatalf("Plain: %v", err)
	}
	if out := buf.String(); !strings.Contains(out, "probe stdout | waiting for healthcheck\n") {
		t.Errorf("a line the step never terminated must still be printed:\n%s", out)
	}
}

func TestPlainWritesNoAnsiEscapes(t *testing.T) {
	// This renderer exists for non-TTY output. An escape sequence in a CI log
	// is the most common way this feature ships broken.
	dir := writeRun(t, twoStepRun())
	fs, _ := source.OpenFile(dir, false)
	defer func() { _ = fs.Close() }()

	var buf bytes.Buffer
	_, _ = render.Plain(context.Background(), fs, &buf)
	if bytes.ContainsRune(buf.Bytes(), 0x1b) {
		t.Errorf("plain output contains an ANSI escape:\n%q", buf.String())
	}
}

// --- helpers ---
//
// Re-implemented rather than imported from internal/source's tests: those
// are unexported test-only helpers, and this package needs fixtures the
// source package's own tests never need.

// twoStepRun returns a finished, successful two-step run: setup then build.
// Deliberately unstamped (no Seq, no TS): writeRun sends it through
// eventlog.Ledger, the only thing in the real system allowed to assign
// those.
func twoStepRun() []api.Event {
	return []api.Event{
		{Type: api.RunStarted, Run: "run1", Payload: mustBody(api.RunStartedBody{
			Pipeline:      "ci",
			EngineVersion: "test",
			PlanDigest:    "digest1",
			StartedAt:     time.Now().UTC(),
		})},
		{Type: api.StepCreated, Run: "run1", Step: "setup", Payload: mustBody(api.StepCreatedBody{
			Kind: "exec",
		})},
		{Type: api.StepCreated, Run: "run1", Step: "build", Payload: mustBody(api.StepCreatedBody{
			Kind: "exec", Needs: []string{"setup"},
		})},
		{Type: api.StepStarted, Run: "run1", Step: "setup", Attempt: 1},
		{Type: api.StepFinished, Run: "run1", Step: "setup", Attempt: 1, Payload: mustBody(api.StepFinishedBody{
			State: api.StateSucceeded,
		})},
		{Type: api.StepStarted, Run: "run1", Step: "build", Attempt: 1},
		{Type: api.StepFinished, Run: "run1", Step: "build", Attempt: 1, Payload: mustBody(api.StepFinishedBody{
			State: api.StateSucceeded,
		})},
		{Type: api.RunFinished, Run: "run1", Payload: mustBody(api.RunFinishedBody{
			Status: api.RunSucceeded,
		})},
	}
}

// failedRun returns a one-step run whose only step, deploy, exits 9.
func failedRun() []api.Event {
	return []api.Event{
		{Type: api.RunStarted, Run: "run1", Payload: mustBody(api.RunStartedBody{
			Pipeline:      "ci",
			EngineVersion: "test",
			PlanDigest:    "digest1",
			StartedAt:     time.Now().UTC(),
		})},
		{Type: api.StepCreated, Run: "run1", Step: "deploy", Payload: mustBody(api.StepCreatedBody{
			Kind: "exec",
		})},
		{Type: api.StepStarted, Run: "run1", Step: "deploy", Attempt: 1},
		{Type: api.StepFinished, Run: "run1", Step: "deploy", Attempt: 1, Payload: mustBody(api.StepFinishedBody{
			State: api.StateFailed, ExitCode: 9, Error: "exit status 9",
		})},
		{Type: api.RunFinished, Run: "run1", Payload: mustBody(api.RunFinishedBody{
			Status: api.RunFailed,
		})},
	}
}

// recoveredRun returns a one-step run whose only step, flaky, fails once
// and succeeds on retry. StateRecovered is deliberately not StateSucceeded:
// a run full of recovered steps must stay distinguishable from a clean one.
func recoveredRun() []api.Event {
	return []api.Event{
		{Type: api.RunStarted, Run: "run1", Payload: mustBody(api.RunStartedBody{
			Pipeline:      "ci",
			EngineVersion: "test",
			PlanDigest:    "digest1",
			StartedAt:     time.Now().UTC(),
		})},
		{Type: api.StepCreated, Run: "run1", Step: "flaky", Payload: mustBody(api.StepCreatedBody{
			Kind: "exec",
		})},
		{Type: api.StepStarted, Run: "run1", Step: "flaky", Attempt: 1},
		{Type: api.StepRetried, Run: "run1", Step: "flaky", Attempt: 2, Payload: mustBody(api.StepRetriedBody{
			Attempt: 2, Reason: "exit status 1", Predicate: "OnInfra",
		})},
		{Type: api.StepStarted, Run: "run1", Step: "flaky", Attempt: 2},
		{Type: api.StepFinished, Run: "run1", Step: "flaky", Attempt: 2, Payload: mustBody(api.StepFinishedBody{
			State: api.StateRecovered,
		})},
		{Type: api.RunFinished, Run: "run1", Payload: mustBody(api.RunFinishedBody{
			Status: api.RunSucceededWithRecovery,
		})},
	}
}

// loggedRun builds a run directory the way the engine builds one: events
// through eventlog.Ledger, step output through eventlog.LogSet. The
// marker's byte range is read off the writer, as the engine reads it:
// hand-maintained offsets could describe bytes the file does not contain,
// making a renderer bug and a fixture bug look identical.
type loggedRun struct {
	t      *testing.T
	ledger *eventlog.Ledger
	logs   *eventlog.LogSet
}

func newLoggedRun(t *testing.T, dir string) *loggedRun {
	t.Helper()
	l, err := eventlog.Open(dir)
	if err != nil {
		t.Fatalf("eventlog.Open: %v", err)
	}
	return &loggedRun{t: t, ledger: l, logs: eventlog.NewLogSet(dir)}
}

func (r *loggedRun) append(e api.Event) {
	r.t.Helper()
	if _, err := r.ledger.Append(e); err != nil {
		r.t.Fatalf("Append: %v", err)
	}
}

// log writes data to one step attempt's stream, then appends the marker: the
// engine's own order, which makes the marker a promise the file can already
// keep.
func (r *loggedRun) log(step string, attempt int, stream, data string) {
	r.t.Helper()
	w, err := r.logs.Writer(step, attempt, stream)
	if err != nil {
		r.t.Fatalf("LogSet.Writer: %v", err)
	}
	off := w.Offset()
	n, err := w.Write([]byte(data))
	if err != nil {
		r.t.Fatalf("LogWriter.Write: %v", err)
	}
	r.append(api.Event{Type: api.StepLogAppended, Run: "run1", Step: step, Attempt: attempt,
		Payload: mustBody(api.StepLogAppendedBody{
			Stream: stream, Offset: off, Len: int64(n),
			Lines: strings.Count(data[:n], "\n"),
		})})
}

func (r *loggedRun) close() {
	r.t.Helper()
	if err := r.logs.Close(); err != nil {
		r.t.Fatalf("LogSet.Close: %v", err)
	}
	if err := r.ledger.Close(); err != nil {
		r.t.Fatalf("Ledger.Close: %v", err)
	}
}

// writeRun creates a temp dir and writes events through eventlog.Ledger, so
// Seq and V are stamped exactly as a real run stamps them.
func writeRun(t *testing.T, events []api.Event) string {
	t.Helper()
	dir := t.TempDir()

	l, err := eventlog.Open(dir)
	if err != nil {
		t.Fatalf("eventlog.Open: %v", err)
	}
	for _, e := range events {
		if _, err := l.Append(e); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	if err := l.Close(); err != nil {
		t.Fatalf("Close ledger: %v", err)
	}
	return dir
}

func mustBody(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}

// TestPlainReportsAStepHeldAtABreakpointFromASnapshot: a held step has no
// State or Error, so without its own printStep branch it prints nothing and
// the pause is invisible.
func TestPlainReportsAStepHeldAtABreakpointFromASnapshot(t *testing.T) {
	dir := writeRun(t, []api.Event{
		{Type: api.RunStarted, Run: "run1", Payload: mustBody(api.RunStartedBody{
			Pipeline: "ci", EngineVersion: "test", StartedAt: time.Now().UTC(),
		})},
		{Type: api.StepCreated, Run: "run1", Step: "deploy", Payload: mustBody(api.StepCreatedBody{Kind: "exec"})},
		{Type: api.BreakpointHit, Run: "run1", Step: "deploy", Payload: mustBody(api.BreakpointHitBody{
			ClientID: "operator",
		})},
	})
	fs, _ := source.OpenFile(dir, false)
	defer func() { _ = fs.Close() }()

	var buf bytes.Buffer
	if _, err := render.Plain(context.Background(), fs, &buf); err != nil {
		t.Fatalf("Plain: %v", err)
	}
	if out := buf.String(); !strings.Contains(out, "deploy paused at breakpoint\n") {
		t.Errorf("output never says the run stopped at a breakpoint, so a reader cannot tell it from a hang:\n%q", out)
	}
}
