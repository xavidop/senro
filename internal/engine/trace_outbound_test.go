package engine_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/xavidop/senro"
	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/artifact"
	"github.com/xavidop/senro/exec"
	"github.com/xavidop/senro/internal/engine"
	"github.com/xavidop/senro/internal/eventlog"
	"github.com/xavidop/senro/internal/executor/localexec"
	"github.com/xavidop/senro/internal/sink"
	"github.com/xavidop/senro/retry"
)

// echoTraceparent is a step command that reports the trace context its own
// process was launched with. It is the only way to observe what this feature
// actually does: the environment is not in the event stream (deliberately, it
// is where secret PATHS live), so the step itself has to say.
func echoTraceparent() senro.Action {
	return exec.Command("sh", "-c", `printf '%s' "$TRACEPARENT"`)
}

// runOutbound runs p through the real local executor and returns the run
// directory alongside the ledger, since the evidence for this feature is in
// the step's own log rather than in an event.
func runOutbound(t *testing.T, p *senro.Plan, opts engine.Options) (string, []api.Event) {
	t.Helper()
	dir := t.TempDir()
	opts.Dir = dir
	opts.Executor = localexec.New(dir, nil)
	opts.Sink = sink.Nop()
	if opts.MaxParallel == 0 {
		opts.MaxParallel = 4
	}
	if opts.RunID == "" {
		opts.RunID = "01OUTBOUND"
	}
	if _, err := engine.Run(t.Context(), p, opts); err != nil {
		t.Fatalf("Run: %v", err)
	}
	return dir, readLedger(t, dir)
}

// stdoutOf reads what one attempt of one step wrote.
func stdoutOf(t *testing.T, dir, step string, attempt int) string {
	t.Helper()
	b, err := os.ReadFile(eventlog.NewLogSet(dir).Path(step, attempt, api.StreamStdout))
	if err != nil {
		t.Fatalf("reading %s attempt %d stdout: %v", step, attempt, err)
	}
	return strings.TrimSpace(string(b))
}

// TestAStepsCommandIsLaunchedInsideItsOwnSpan is the end-to-end claim for the
// local executor: a traced tool running inside a step becomes a child of that
// step's attempt rather than the root of a fresh, unconnected trace.
//
// The step prints the traceparent its process was actually launched with, and
// the assertion is against the ledger: same trace as the run, parent span
// equal to this step's OWN step.started span.
func TestAStepsCommandIsLaunchedInsideItsOwnSpan(t *testing.T) {
	pipe := senro.New("outbound")
	l := pipe.Workflow("main")
	l.Step("build", echoTraceparent())

	dir, events := runOutbound(t, build(t, pipe), engine.Options{TraceParent: inboundHeader})
	starts := stepStarts(t, events)

	got := stdoutOf(t, dir, "build", 1)
	p, ok := api.ParseTraceParent(got)
	if !ok {
		t.Fatalf("the step was launched with TRACEPARENT=%q, which no conformant tool will accept", got)
	}
	if p.TraceID != inboundTraceID {
		t.Errorf("the step's own trace ID is %q, want the run's %q", p.TraceID, inboundTraceID)
	}
	if want := starts["build"][1].SpanID; p.SpanID != want {
		t.Errorf("the step's command reports parent span %q, want this attempt's own span %q",
			p.SpanID, want)
	}
	if p.SpanID == runStarted(t, events).SpanID {
		t.Error("the step's command was handed the RUN's span: every tool in every step would " +
			"report the same parent and the trace would be flat")
	}
}

// TestEachAttemptRunsInsideItsOwnSpan carries the per-attempt rule out to the
// work itself. A retried step is two attempts with two outcomes, and the tool
// that ran in the second must not report itself as part of the first.
func TestEachAttemptRunsInsideItsOwnSpan(t *testing.T) {
	pipe := senro.New("outbound")
	l := pipe.Workflow("main")
	l.Step("flaky", exec.Command("sh", "-c",
		`printf '%s' "$TRACEPARENT"; if [ -f ../marker ]; then exit 0; else touch ../marker; exit 1; fi`)).
		RetryPolicy(retry.Policy{MaxAttempts: 2, On: retry.OnExitCode(1)})

	dir, events := runOutbound(t, build(t, pipe), engine.Options{MaxParallel: 1})
	starts := stepStarts(t, events)

	first, ok := api.ParseTraceParent(stdoutOf(t, dir, "flaky", 1))
	if !ok {
		t.Fatal("attempt 1 was launched with no usable traceparent")
	}
	second, ok := api.ParseTraceParent(stdoutOf(t, dir, "flaky", 2))
	if !ok {
		t.Fatal("attempt 2 was launched with no usable traceparent")
	}

	if first.SpanID == second.SpanID {
		t.Errorf("both attempts ran under span %q: a retry is a second span, not a reused one",
			first.SpanID)
	}
	if want := starts["flaky"][1].SpanID; first.SpanID != want {
		t.Errorf("attempt 1 ran under %q, want its own span %q", first.SpanID, want)
	}
	if want := starts["flaky"][2].SpanID; second.SpanID != want {
		t.Errorf("attempt 2 ran under %q, want its own span %q", second.SpanID, want)
	}
	if first.TraceID != second.TraceID {
		t.Errorf("the two attempts are in different traces, %q and %q", first.TraceID, second.TraceID)
	}
}

// TestAHandlersCommandIsLaunchedInsideTheHandlersSpan keeps the handler path
// level with the step path. A cleanup handler that calls a traced tool is
// exactly the work somebody is looking for when they ask why the next run
// cannot take the lock, and a handler whose tool started a fresh trace would
// be absent from the run's.
func TestAHandlersCommandIsLaunchedInsideTheHandlersSpan(t *testing.T) {
	pipe := senro.New("outbound")
	l := pipe.Workflow("main")
	l.Step("boom", exec.Command("sh", "-c", "exit 3")).
		ContinueOnError().
		OnFailure(senro.Handler("notify", echoTraceparent()))

	dir, events := runOutbound(t, build(t, pipe), engine.Options{TraceParent: inboundHeader})

	const logStep = "boom/on_failure/notify"
	var handlerSpan string
	for _, e := range events {
		if e.Type == api.HandlerStarted && e.Step == logStep {
			var b api.HandlerBody
			if err := e.Decode(&b); err != nil {
				t.Fatalf("decode handler.started: %v", err)
			}
			handlerSpan = b.SpanID
		}
	}
	if handlerSpan == "" {
		t.Fatal("no handler.started span to compare against")
	}

	p, ok := api.ParseTraceParent(stdoutOf(t, dir, logStep, 1))
	if !ok {
		t.Fatal("the handler was launched with no usable traceparent")
	}
	if p.TraceID != inboundTraceID {
		t.Errorf("the handler's trace ID is %q, want the run's %q", p.TraceID, inboundTraceID)
	}
	if p.SpanID != handlerSpan {
		t.Errorf("the handler's command reports parent span %q, want the handler's own %q",
			p.SpanID, handlerSpan)
	}
}

// TestAStepThatDeclaresATraceparentIsLaunchedWithItsOwn is the author's
// override, proved where it matters: in the environment the process actually
// received, not merely in the slice the engine assembled.
func TestAStepThatDeclaresATraceparentIsLaunchedWithItsOwn(t *testing.T) {
	const declared = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"

	pipe := senro.New("outbound")
	l := pipe.Workflow("main")
	l.Step("build", echoTraceparent()).Env("TRACEPARENT", declared)

	dir, _ := runOutbound(t, build(t, pipe), engine.Options{TraceParent: inboundHeader})

	if got := stdoutOf(t, dir, "build", 1); got != declared {
		t.Errorf("the step ran with TRACEPARENT=%q, want the one it declared, %q", got, declared)
	}
}

// TestARunThatStartedItsOwnTraceStillPropagatesIt is the standalone case: a
// run with nothing above it is still a trace, and the tools inside its steps
// still belong to it. Nothing about propagation depends on there having been
// an inbound traceparent.
func TestARunThatStartedItsOwnTraceStillPropagatesIt(t *testing.T) {
	pipe := senro.New("outbound")
	l := pipe.Workflow("main")
	l.Step("build", echoTraceparent())

	dir, events := runOutbound(t, build(t, pipe), engine.Options{})

	p, ok := api.ParseTraceParent(stdoutOf(t, dir, "build", 1))
	if !ok {
		t.Fatal("a run that started its own trace propagated nothing usable to its step")
	}
	if p.TraceID != events[0].TraceID {
		t.Errorf("the step's trace ID is %q, want the run's own %q", p.TraceID, events[0].TraceID)
	}
	if want := stepStarts(t, events)["build"][1].SpanID; p.SpanID != want {
		t.Errorf("the step ran under span %q, want %q", p.SpanID, want)
	}
}

// TestTwoRunsInDifferentTracesShareACacheKey is the property this feature
// rests on: every command is launched with a TRACEPARENT carrying that
// attempt's span, and such a variable in a cache key means nothing ever
// hits again, silently. It does not enter the key: EnvComponent digests
// only CacheEnv-declared names, built from the node's DECLARED environment
// through EffectiveEnv rather than the launched one. Asserted end to end
// rather than on either mechanism: two runs of the same pure step in two
// traces must produce the SAME digest, and the second must be served.
func TestTwoRunsInDifferentTracesShareACacheKey(t *testing.T) {
	const otherHeader = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"

	cacheDir := t.TempDir()
	work := t.TempDir()
	if err := os.WriteFile(filepath.Join(work, "in.txt"), []byte("input\n"), 0o600); err != nil {
		t.Fatalf("fixture: %v", err)
	}
	// This step mounts no workspace, so its declared Inputs resolve against
	// the coordinator's own working directory; the same Chdir every other
	// pure-step test in this package does, for the same reason.
	t.Chdir(work)

	pipeline := func() *senro.Pipeline {
		pipe := senro.New("cached")
		pipe.Workflow("main").
			Step("pure", echoTraceparent()).
			WorkDir(work).Pure().Inputs(artifact.File("in.txt"))
		return pipe
	}

	firstDir := t.TempDir()
	if err := senro.Run(t.Context(), pipeline(), senro.WithDir(firstDir),
		senro.WithCacheDir(cacheDir), senro.WithTraceContext(inboundHeader, "")); err != nil {
		t.Fatalf("first Run: %v", err)
	}
	secondDir := t.TempDir()
	if err := senro.Run(t.Context(), pipeline(), senro.WithDir(secondDir),
		senro.WithCacheDir(cacheDir), senro.WithTraceContext(otherHeader, "")); err != nil {
		t.Fatalf("second Run: %v", err)
	}

	first, second := readLedger(t, firstDir), readLedger(t, secondDir)
	if first[0].TraceID == second[0].TraceID {
		t.Fatal("both runs are in the same trace, so this test proves nothing")
	}

	var missBody api.CacheMissBody
	if err := findEvent(t, first, api.CacheMiss).Decode(&missBody); err != nil {
		t.Fatalf("decode cache.miss: %v", err)
	}
	if !hasEvent(second, api.CacheHit) {
		t.Fatal("the second run MISSED: a per-attempt traceparent has reached the cache key, " +
			"and no pure step will ever hit again")
	}
	var hitBody api.CacheHitBody
	if err := findEvent(t, second, api.CacheHit).Decode(&hitBody); err != nil {
		t.Fatalf("decode cache.hit: %v", err)
	}
	if hitBody.Key != missBody.Key {
		t.Errorf("the two runs computed different keys, %q and %q", missBody.Key, hitBody.Key)
	}
}

// TestTheTracestateTravelsWithTheTraceparent keeps the pair together. Vendor
// routing data that reaches senro and stops there is routing data the next
// hop never sees, which is the whole reason the header exists.
func TestTheTracestateTravelsWithTheTraceparent(t *testing.T) {
	pipe := senro.New("outbound")
	l := pipe.Workflow("main")
	l.Step("build", exec.Command("sh", "-c", `printf '%s' "$TRACESTATE"`))

	dir, _ := runOutbound(t, build(t, pipe), engine.Options{
		TraceParent: inboundHeader, TraceState: "congo=t61rcWkgMzE",
	})

	if got := stdoutOf(t, dir, "build", 1); got != "congo=t61rcWkgMzE" {
		t.Errorf("the step ran with TRACESTATE=%q, want the run's inbound value", got)
	}
}
