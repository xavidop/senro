package engine_test

import (
	"testing"

	"github.com/xavidop/senro"
	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/exec"
	"github.com/xavidop/senro/internal/engine"
	"github.com/xavidop/senro/internal/executor/localexec"
	"github.com/xavidop/senro/internal/sink"
	"github.com/xavidop/senro/retry"
)

// The W3C specification's own example values, used here as an inbound
// context so a reader can check them against the document rather than
// against another line of this file.
const (
	inboundTraceID = "4bf92f3577b34da6a3ce929d0e0e4736"
	inboundSpanID  = "00f067aa0ba902b7"
	inboundHeader  = "00-" + inboundTraceID + "-" + inboundSpanID + "-01"
)

// runTraced builds and runs p, returning the run's whole ledger.
func runTraced(t *testing.T, p *senro.Plan, opts engine.Options) []api.Event {
	t.Helper()
	dir := t.TempDir()
	opts.Dir = dir
	opts.Executor = localexec.New(dir, nil)
	opts.Sink = sink.Nop()
	if opts.MaxParallel == 0 {
		opts.MaxParallel = 4
	}
	if opts.RunID == "" {
		opts.RunID = "01TEST"
	}
	if _, err := engine.Run(t.Context(), p, opts); err != nil {
		t.Fatalf("Run: %v", err)
	}
	return readLedger(t, dir)
}

func build(t *testing.T, pipe *senro.Pipeline) *senro.Plan {
	t.Helper()
	p, err := pipe.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	return p
}

// runStarted decodes the run.started body, failing the test if there is not
// exactly one.
func runStarted(t *testing.T, events []api.Event) api.RunStartedBody {
	t.Helper()
	var body api.RunStartedBody
	found := 0
	for _, e := range events {
		if e.Type == api.RunStarted {
			found++
			if err := e.Decode(&body); err != nil {
				t.Fatalf("decode run.started: %v", err)
			}
		}
	}
	if found != 1 {
		t.Fatalf("found %d run.started events, want exactly 1", found)
	}
	return body
}

// stepStarts decodes every step.started body, keyed by step ID and attempt.
func stepStarts(t *testing.T, events []api.Event) map[string]map[int]api.StepStartedBody {
	t.Helper()
	out := map[string]map[int]api.StepStartedBody{}
	for _, e := range events {
		if e.Type != api.StepStarted {
			continue
		}
		var b api.StepStartedBody
		if err := e.Decode(&b); err != nil {
			t.Fatalf("decode step.started for %s: %v", e.Step, err)
		}
		if out[e.Step] == nil {
			out[e.Step] = map[int]api.StepStartedBody{}
		}
		out[e.Step][e.Attempt] = b
	}
	return out
}

func stepFinishes(t *testing.T, events []api.Event) map[string]api.StepFinishedBody {
	t.Helper()
	out := map[string]api.StepFinishedBody{}
	for _, e := range events {
		if e.Type != api.StepFinished {
			continue
		}
		var b api.StepFinishedBody
		if err := e.Decode(&b); err != nil {
			t.Fatalf("decode step.finished for %s: %v", e.Step, err)
		}
		out[e.Step] = b
	}
	return out
}

// TestEveryEventInARunCarriesTheSameTraceID is the property the whole
// feature rests on. A trace ID that varies event to event does not make a
// trace with a few odd entries, it makes as many single-event traces as the
// run has events, every one of them useless.
//
// It also asserts the ID is a VALID one on every event, not merely equal to
// itself, so a run that consistently emitted the all-zero reserved value
// would still fail here.
func TestEveryEventInARunCarriesTheSameTraceID(t *testing.T) {
	pipe := senro.New("traced")
	l := pipe.Workflow("main")
	l.Step("setup", exec.Command("echo", "setup"))
	l.Step("build", exec.Command("echo", "build")).Needs("setup")

	events := runTraced(t, build(t, pipe), engine.Options{})
	if len(events) == 0 {
		t.Fatal("no events")
	}

	first := events[0].TraceID
	if !api.ValidTraceID(first) {
		t.Fatalf("first event's trace_id = %q, which is not a valid W3C trace ID", first)
	}
	for _, e := range events {
		if e.TraceID != first {
			t.Errorf("seq %d (%s) trace_id = %q, want %q: one run is one trace",
				e.Seq, e.Type, e.TraceID, first)
		}
	}
}

// TestTwoRunsAreTwoTraces is the other half of the identity rule. Sharing an
// ID across runs would merge every build the machine ever did into one
// unreadable trace.
func TestTwoRunsAreTwoTraces(t *testing.T) {
	pipe := senro.New("traced")
	pipe.Workflow("main").Step("one", exec.Command("true"))
	p := build(t, pipe)

	a := runTraced(t, p, engine.Options{})
	b := runTraced(t, p, engine.Options{})
	if a[0].TraceID == b[0].TraceID {
		t.Errorf("two runs share trace_id %q", a[0].TraceID)
	}
}

// TestARunWithNoInboundTraceStartsItsOwn covers the standalone case: senro
// invoked by a person at a terminal, with nothing upstream to belong to.
// The run is then the root, and saying so means having no parent rather
// than having an invented one.
func TestARunWithNoInboundTraceStartsItsOwn(t *testing.T) {
	pipe := senro.New("traced")
	pipe.Workflow("main").Step("one", exec.Command("true"))

	events := runTraced(t, build(t, pipe), engine.Options{})
	body := runStarted(t, events)

	if !api.ValidSpanID(body.SpanID) {
		t.Errorf("run.started span_id = %q, which is not a valid span ID", body.SpanID)
	}
	if body.ParentSpanID != "" {
		t.Errorf("run.started parent_span_id = %q, want empty: nothing started this trace but the run", body.ParentSpanID)
	}
	if body.TraceState != "" {
		t.Errorf("run.started tracestate = %q, want empty", body.TraceState)
	}
}

// TestAnInboundTraceparentBecomesTheRunsParent is the highest-value
// behaviour here. A trace that starts at senro tells an operator only what
// they already knew; one that joins the push to the pipeline to the deploy
// is the reason to have any of this.
func TestAnInboundTraceparentBecomesTheRunsParent(t *testing.T) {
	pipe := senro.New("traced")
	pipe.Workflow("main").Step("one", exec.Command("true"))

	events := runTraced(t, build(t, pipe), engine.Options{
		TraceParent: inboundHeader,
		TraceState:  "vendorname=opaqueValue,other=1",
	})
	body := runStarted(t, events)

	if events[0].TraceID != inboundTraceID {
		t.Errorf("trace_id = %q, want the inbound trace %q: the run must JOIN it, not start a new one",
			events[0].TraceID, inboundTraceID)
	}
	if body.ParentSpanID != inboundSpanID {
		t.Errorf("run.started parent_span_id = %q, want the inbound span %q", body.ParentSpanID, inboundSpanID)
	}
	if body.SpanID == inboundSpanID {
		t.Error("the run reused the inbound span ID as its own: a child span needs its own ID")
	}
	if !api.ValidSpanID(body.SpanID) {
		t.Errorf("run.started span_id = %q, which is not a valid span ID", body.SpanID)
	}
	if body.TraceFlags != "01" {
		t.Errorf("run.started trace_flags = %q, want %q: an upstream sampling decision must survive", body.TraceFlags, "01")
	}
	if body.TraceState != "vendorname=opaqueValue,other=1" {
		t.Errorf("run.started tracestate = %q, want the inbound value verbatim", body.TraceState)
	}

	for _, e := range events {
		if e.TraceID != inboundTraceID {
			t.Fatalf("seq %d (%s) left the inbound trace: trace_id = %q", e.Seq, e.Type, e.TraceID)
		}
	}
}

// TestAMalformedInboundTraceparentStartsAFreshTrace is the refusal the
// specification requires. Propagating a value senro could not fully parse
// would produce a link to a trace that does not exist, which is worse than
// no link: it is indistinguishable, in the dashboard that receives it, from
// a real trace whose other half was lost.
func TestAMalformedInboundTraceparentStartsAFreshTrace(t *testing.T) {
	for _, tc := range []struct {
		name   string
		header string
	}{
		{"truncated trace ID", "00-4bf92f3577b34da6a3ce929d0e0e473-00f067aa0ba902b7-01"},
		{"all-zero trace ID", "00-00000000000000000000000000000000-00f067aa0ba902b7-01"},
		{"all-zero span ID", "00-" + inboundTraceID + "-0000000000000000-01"},
		{"uppercase", "00-4BF92F3577B34DA6A3CE929D0E0E4736-00f067aa0ba902b7-01"},
		{"forbidden version ff", "ff-" + inboundTraceID + "-" + inboundSpanID + "-01"},
		{"missing fields", "00-" + inboundTraceID},
		{"not a traceparent", "please trace my build"},
		{"empty", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pipe := senro.New("traced")
			pipe.Workflow("main").Step("one", exec.Command("true"))

			events := runTraced(t, build(t, pipe), engine.Options{TraceParent: tc.header})
			body := runStarted(t, events)

			if !api.ValidTraceID(events[0].TraceID) {
				t.Fatalf("trace_id = %q, want a freshly generated valid one", events[0].TraceID)
			}
			if events[0].TraceID == inboundTraceID {
				t.Error("senro adopted the trace ID out of a traceparent it should have refused")
			}
			if body.ParentSpanID != "" {
				t.Errorf("run.started parent_span_id = %q, want empty: a refused traceparent leaves no parent behind",
					body.ParentSpanID)
			}
		})
	}
}

// TestAMalformedTracestateIsNotAReasonToDropTheTrace keeps the two halves
// independent. tracestate is opaque vendor data senro never interprets, so
// there is nothing for it to be malformed AGAINST here, and dropping a
// perfectly good traceparent because the state beside it looked odd would
// lose the parentage that is the whole point.
func TestAMalformedTracestateIsNotAReasonToDropTheTrace(t *testing.T) {
	pipe := senro.New("traced")
	pipe.Workflow("main").Step("one", exec.Command("true"))

	events := runTraced(t, build(t, pipe), engine.Options{
		TraceParent: inboundHeader,
		TraceState:  "!!! not really a tracestate !!!",
	})
	if events[0].TraceID != inboundTraceID {
		t.Errorf("trace_id = %q, want the inbound trace %q", events[0].TraceID, inboundTraceID)
	}
}

// TestSpanParentageMirrorsTheGraphNotTheClock is the shape test. The
// pipeline is a diamond:
//
//	   fetch
//	   /   \
//	lint   test
//	   \   /
//	  package
//
// lint and test have no edge between them and must not acquire one: with
// MaxParallel 1 they run one after the other on the wall clock, and a model
// built from that order would report a pipeline with no parallelism at all.
// Both must be children of fetch. package waits on both: a span has exactly
// one parent, so it takes the first need in plan order and records the rest
// as links, which is what OpenTelemetry links are FOR.
func TestSpanParentageMirrorsTheGraphNotTheClock(t *testing.T) {
	pipe := senro.New("traced")
	l := pipe.Workflow("main")
	l.Step("fetch", exec.Command("true"))
	l.Step("lint", exec.Command("true")).Needs("fetch")
	l.Step("test", exec.Command("true")).Needs("fetch")
	l.Step("package", exec.Command("true")).Needs("lint", "test")

	events := runTraced(t, build(t, pipe), engine.Options{MaxParallel: 1})
	run := runStarted(t, events)
	starts := stepStarts(t, events)

	span := func(step string) api.StepStartedBody {
		t.Helper()
		b, ok := starts[step][1]
		if !ok {
			t.Fatalf("no step.started for %s attempt 1", step)
		}
		if !api.ValidSpanID(b.SpanID) {
			t.Fatalf("%s span_id = %q, which is not a valid span ID", step, b.SpanID)
		}
		return b
	}

	fetch, lint, test, pkg := span("fetch"), span("lint"), span("test"), span("package")

	if fetch.ParentSpanID != run.SpanID {
		t.Errorf("fetch parent = %q, want the run span %q: a step with no needs hangs off the run",
			fetch.ParentSpanID, run.SpanID)
	}
	if lint.ParentSpanID != fetch.SpanID {
		t.Errorf("lint parent = %q, want fetch %q", lint.ParentSpanID, fetch.SpanID)
	}
	if test.ParentSpanID != fetch.SpanID {
		t.Errorf("test parent = %q, want fetch %q: lint ran first on the clock but lint is not test's parent",
			test.ParentSpanID, fetch.SpanID)
	}
	if pkg.ParentSpanID != lint.SpanID {
		t.Errorf("package parent = %q, want lint %q (the first need in plan order)", pkg.ParentSpanID, lint.SpanID)
	}
	if len(pkg.LinkedSpanIDs) != 1 || pkg.LinkedSpanIDs[0] != test.SpanID {
		t.Errorf("package linked_span_ids = %v, want exactly [%q]: the needs a span cannot make its parent become links",
			pkg.LinkedSpanIDs, test.SpanID)
	}
	if len(fetch.LinkedSpanIDs) != 0 {
		t.Errorf("fetch linked_span_ids = %v, want none", fetch.LinkedSpanIDs)
	}
	if len(lint.LinkedSpanIDs) != 0 {
		t.Errorf("lint linked_span_ids = %v, want none", lint.LinkedSpanIDs)
	}
}

// TestEverySpanIDInARunIsDistinct is the other identifier rule. Two spans
// with one ID are one span to every consumer, and the span that gets
// silently discarded is not the one you would choose.
func TestEverySpanIDInARunIsDistinct(t *testing.T) {
	pipe := senro.New("traced")
	l := pipe.Workflow("main")
	l.Step("a", exec.Command("true"))
	l.Step("b", exec.Command("true")).Needs("a")
	l.Step("c", exec.Command("true")).Needs("a")

	events := runTraced(t, build(t, pipe), engine.Options{})

	seen := map[string]string{}
	claim := func(owner, id string) {
		t.Helper()
		if id == "" {
			t.Errorf("%s has no span ID", owner)
			return
		}
		if prev, dup := seen[id]; dup {
			t.Errorf("%s reused span ID %q, already held by %s", owner, id, prev)
			return
		}
		seen[id] = owner
	}

	claim("the run", runStarted(t, events).SpanID)
	for step, attempts := range stepStarts(t, events) {
		for attempt, b := range attempts {
			claim(step, b.SpanID)
			_ = attempt
		}
	}
	if len(seen) != 4 {
		t.Errorf("collected %d distinct span IDs, want 4 (one run, three steps)", len(seen))
	}
}

// TestARetriedStepGetsASecondSpan is the correctness note about retries. An
// attempt that failed and an attempt that succeeded are two pieces of work
// with two durations and two outcomes; folding them into one span reports a
// step that took the sum of both and succeeded, which is a description of
// something that did not happen.
//
// Both attempts keep the same parent, because parentage comes from the graph
// and the graph did not change when the step was retried.
func TestARetriedStepGetsASecondSpan(t *testing.T) {
	pipe := senro.New("traced")
	l := pipe.Workflow("main")
	l.Step("setup", exec.Command("true"))
	l.Step("flaky", exec.Command("sh", "-c",
		`if [ -f ../marker ]; then exit 0; else touch ../marker; exit 1; fi`)).
		Needs("setup").
		RetryPolicy(retry.Policy{MaxAttempts: 2, On: retry.OnExitCode(1)})

	events := runTraced(t, build(t, pipe), engine.Options{MaxParallel: 1})
	starts := stepStarts(t, events)

	setup, ok := starts["setup"][1]
	if !ok {
		t.Fatal("no step.started for setup")
	}
	one, ok := starts["flaky"][1]
	if !ok {
		t.Fatal("no step.started for flaky attempt 1")
	}
	two, ok := starts["flaky"][2]
	if !ok {
		t.Fatal("no step.started for flaky attempt 2: a retry must announce itself as a new attempt")
	}

	if one.SpanID == two.SpanID {
		t.Errorf("both attempts at flaky carry span ID %q: a retry is a second span, not a reused one", one.SpanID)
	}
	if !api.ValidSpanID(one.SpanID) || !api.ValidSpanID(two.SpanID) {
		t.Errorf("attempt spans %q and %q are not both valid span IDs", one.SpanID, two.SpanID)
	}
	if one.ParentSpanID != setup.SpanID || two.ParentSpanID != setup.SpanID {
		t.Errorf("attempt parents %q and %q, want both %q: retrying a step does not move it in the graph",
			one.ParentSpanID, two.ParentSpanID, setup.SpanID)
	}
}

// TestAStepThatNeverStartedStillGetsASpan covers the events an exporter
// would otherwise have no way to close. A step skipped by a false condition
// emits step.finished and no step.started at all, so the finish event is the
// only place its span can be named, and without one the step is simply
// absent from the trace: an operator looking for why nothing deployed finds
// nothing rather than a skip.
//
// A step restored from cache has the same shape and the same fix.
func TestAStepThatNeverStartedStillGetsASpan(t *testing.T) {
	pipe := senro.New("traced")
	l := pipe.Workflow("main")
	l.Step("build", exec.Command("true"))
	l.Step("deploy", exec.Command("true")).Needs("build").When(senro.Branch("release"))

	events := runTraced(t, build(t, pipe), engine.Options{Params: senro.Params{"branch": "main"}})
	starts := stepStarts(t, events)
	finishes := stepFinishes(t, events)

	if _, started := starts["deploy"]; started {
		t.Fatal("deploy started after all, so this test no longer covers what it was written for")
	}
	deploy, ok := finishes["deploy"]
	if !ok {
		t.Fatal("no step.finished for deploy")
	}
	if deploy.State != api.StateSkippedCondition {
		t.Fatalf("deploy state = %s, want skipped_condition", deploy.State)
	}
	if !api.ValidSpanID(deploy.SpanID) {
		t.Errorf("skipped deploy step.finished span_id = %q, which is not a valid span ID", deploy.SpanID)
	}
	build := starts["build"][1]
	if deploy.ParentSpanID != build.SpanID {
		t.Errorf("skipped deploy parent = %q, want build %q: a step that never ran still sits where the graph put it",
			deploy.ParentSpanID, build.SpanID)
	}
	if deploy.SpanID == build.SpanID {
		t.Error("the skipped step borrowed its parent's span ID")
	}
}

// TestStepFinishedNamesTheSpanStepStartedOpened keeps the ordinary path
// self-describing without making it redundant: the finish event repeats the
// span ID so a reader never has to correlate on (step, attempt) to know
// which span ended, and omits the parent because the start event already
// said where the span hangs.
func TestStepFinishedNamesTheSpanStepStartedOpened(t *testing.T) {
	pipe := senro.New("traced")
	l := pipe.Workflow("main")
	l.Step("a", exec.Command("true"))
	l.Step("b", exec.Command("true")).Needs("a")

	events := runTraced(t, build(t, pipe), engine.Options{})
	starts := stepStarts(t, events)
	finishes := stepFinishes(t, events)

	for _, step := range []string{"a", "b"} {
		started, finished := starts[step][1], finishes[step]
		if finished.SpanID != started.SpanID {
			t.Errorf("%s: step.finished span_id = %q, step.started span_id = %q: they are one span",
				step, finished.SpanID, started.SpanID)
		}
		if finished.ParentSpanID != "" {
			t.Errorf("%s: step.finished repeats parent_span_id %q, which step.started already carried",
				step, finished.ParentSpanID)
		}
	}
}

// TestACleanupHandlerGetsItsOwnSpan covers the work easiest to leave out of
// a trace and worst to be missing from one. A handler is not a step (no
// attempts, no retry loop, no log markers), so anything modelling a run by
// walking steps skips it, yet a cleanup that ran thirty seconds and failed
// is exactly the span somebody wants when the next run cannot take the
// lock. Its parent is the attempt whose failure triggered it, not the run:
// hanging it off the run loses the only fact that explains why it ran.
func TestACleanupHandlerGetsItsOwnSpan(t *testing.T) {
	pipe := senro.New("traced")
	l := pipe.Workflow("main")
	l.Step("deploy", exec.Command("sh", "-c", "exit 9")).
		OnFailure(senro.Handler("collect", exec.Command("echo", "evidence")))

	events := runTraced(t, build(t, pipe), engine.Options{MaxParallel: 1})

	deploy := stepStarts(t, events)["deploy"][1]
	if !api.ValidSpanID(deploy.SpanID) {
		t.Fatalf("deploy span_id = %q", deploy.SpanID)
	}

	var started, ended api.HandlerBody
	var startedSeen, endedSeen bool
	for _, e := range events {
		switch e.Type {
		case api.HandlerStarted:
			if err := e.Decode(&started); err != nil {
				t.Fatalf("decode handler.started: %v", err)
			}
			startedSeen = true
		case api.HandlerSucceeded, api.HandlerFailed:
			if err := e.Decode(&ended); err != nil {
				t.Fatalf("decode handler completion: %v", err)
			}
			endedSeen = true
		}
	}
	if !startedSeen || !endedSeen {
		t.Fatalf("handler events missing: started=%v ended=%v", startedSeen, endedSeen)
	}

	if !api.ValidSpanID(started.SpanID) {
		t.Errorf("handler.started span_id = %q, which is not a valid span ID: a handler that ran is not a handler that is invisible",
			started.SpanID)
	}
	if ended.SpanID != started.SpanID {
		t.Errorf("handler completion span_id = %q, started span_id = %q: one handler run is one span",
			ended.SpanID, started.SpanID)
	}
	if started.SpanID == deploy.SpanID {
		t.Error("the handler borrowed its parent step's span rather than opening its own")
	}
	if started.ParentSpanID != deploy.SpanID {
		t.Errorf("handler parent_span_id = %q, want the failed deploy attempt %q",
			started.ParentSpanID, deploy.SpanID)
	}
}

// TestRunFinishedNamesTheRunSpan lets an exporter close the root span from
// the last event alone, which matters to a client that joined the stream
// late and never saw run.started.
func TestRunFinishedNamesTheRunSpan(t *testing.T) {
	pipe := senro.New("traced")
	pipe.Workflow("main").Step("one", exec.Command("true"))

	events := runTraced(t, build(t, pipe), engine.Options{})
	run := runStarted(t, events)

	last := events[len(events)-1]
	if last.Type != api.RunFinished {
		t.Fatalf("last event is %s, want run.finished", last.Type)
	}
	var body api.RunFinishedBody
	if err := last.Decode(&body); err != nil {
		t.Fatalf("decode run.finished: %v", err)
	}
	if body.SpanID != run.SpanID {
		t.Errorf("run.finished span_id = %q, want the run span %q", body.SpanID, run.SpanID)
	}
}
