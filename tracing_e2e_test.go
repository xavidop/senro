package senro_test

import (
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/xavidop/senro"
	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/exec"
	"github.com/xavidop/senro/executor/container"
	"github.com/xavidop/senro/internal/dockerd/dockertest"
	"github.com/xavidop/senro/internal/eventlog"
)

// The W3C specification's own example values.
const (
	upstreamTraceID = "4bf92f3577b34da6a3ce929d0e0e4736"
	upstreamSpanID  = "00f067aa0ba902b7"
	upstreamHeader  = "00-" + upstreamTraceID + "-" + upstreamSpanID + "-01"
)

// collector gathers a run's events through the public Sink interface, which
// is the same seam an exporter uses. Locked, because Emit is called on the
// engine's goroutine and read on the test's.
type collector struct {
	mu     sync.Mutex
	events []api.Event
}

func (c *collector) Emit(e api.Event) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.events = append(c.events, e)
}

func (c *collector) all() []api.Event {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]api.Event(nil), c.events...)
}

// runCollected runs a one-step pipeline and returns everything a sink saw.
func runCollected(t *testing.T, opts ...senro.Option) []api.Event {
	t.Helper()
	pipe := senro.New("traced")
	pipe.Workflow("main").Step("one", exec.Command("true"))

	c := &collector{}
	opts = append(opts, senro.WithSink(c), senro.WithDir(t.TempDir()))
	if err := senro.Run(t.Context(), pipe, opts...); err != nil {
		t.Fatalf("Run: %v", err)
	}
	events := c.all()
	if len(events) == 0 {
		t.Fatal("the sink saw no events")
	}
	return events
}

func runStartedBody(t *testing.T, events []api.Event) api.RunStartedBody {
	t.Helper()
	for _, e := range events {
		if e.Type == api.RunStarted {
			var b api.RunStartedBody
			if err := e.Decode(&b); err != nil {
				t.Fatalf("decode run.started: %v", err)
			}
			return b
		}
	}
	t.Fatal("no run.started event")
	return api.RunStartedBody{}
}

// TestARunJoinsTheTraceInItsEnvironment: a run is almost always a child of a
// CI job or webhook delivery, and every one of those exports TRACEPARENT. No
// option is passed here; picking the variable up without being asked is the
// point.
func TestARunJoinsTheTraceInItsEnvironment(t *testing.T) {
	t.Setenv("TRACEPARENT", upstreamHeader)
	t.Setenv("TRACESTATE", "congo=t61rcWkgMzE")

	events := runCollected(t)
	body := runStartedBody(t, events)

	if events[0].TraceID != upstreamTraceID {
		t.Errorf("trace_id = %q, want the environment's trace %q", events[0].TraceID, upstreamTraceID)
	}
	if body.ParentSpanID != upstreamSpanID {
		t.Errorf("parent_span_id = %q, want the environment's span %q", body.ParentSpanID, upstreamSpanID)
	}
	if body.TraceState != "congo=t61rcWkgMzE" {
		t.Errorf("tracestate = %q, want the environment's value", body.TraceState)
	}
}

// TestALowercaseTraceparentInTheEnvironmentIsAlsoRead covers the spelling
// half the world uses. The variable is conventionally TRACEPARENT, but the
// header it comes from is lowercase and plenty of tooling exports it that
// way; refusing one spelling silently loses the trace, and there is nothing
// in the run's output to suggest why.
func TestALowercaseTraceparentInTheEnvironmentIsAlsoRead(t *testing.T) {
	t.Setenv("traceparent", upstreamHeader)

	events := runCollected(t)
	if events[0].TraceID != upstreamTraceID {
		t.Errorf("trace_id = %q, want %q", events[0].TraceID, upstreamTraceID)
	}
}

// TestWithTraceContextOverridesTheEnvironment is what an embedder needs. A
// program that already holds a span in a context.Context, from its own
// OpenTelemetry setup, has better information than whatever the process was
// launched with, and a library that preferred the environment would be
// unusable from inside a traced server.
func TestWithTraceContextOverridesTheEnvironment(t *testing.T) {
	const explicitTrace = "0af7651916cd43dd8448eb211c80319c"
	const explicitSpan = "b7ad6b7169203331"
	t.Setenv("TRACEPARENT", upstreamHeader)
	t.Setenv("TRACESTATE", "fromenv=1")

	events := runCollected(t, senro.WithTraceContext(
		"00-"+explicitTrace+"-"+explicitSpan+"-01", "fromcaller=1"))
	body := runStartedBody(t, events)

	if events[0].TraceID != explicitTrace {
		t.Errorf("trace_id = %q, want the caller's trace %q", events[0].TraceID, explicitTrace)
	}
	if body.ParentSpanID != explicitSpan {
		t.Errorf("parent_span_id = %q, want the caller's span %q", body.ParentSpanID, explicitSpan)
	}
	if body.TraceState != "fromcaller=1" {
		t.Errorf("tracestate = %q, want the caller's value", body.TraceState)
	}
}

// TestAMalformedTraceparentInTheEnvironmentIsIgnored: the run must start its
// own trace. Propagating a value senro could not parse produces a link to a
// trace that does not exist, and refusing to run would let an unrelated
// observability variable break somebody's build.
func TestAMalformedTraceparentInTheEnvironmentIsIgnored(t *testing.T) {
	for _, tc := range []struct {
		name   string
		header string
	}{
		{"an unexpanded variable", "$TRACEPARENT"},
		{"truncated", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa"},
		{"all-zero trace ID", "00-00000000000000000000000000000000-00f067aa0ba902b7-01"},
		{"uppercase", "00-4BF92F3577B34DA6A3CE929D0E0E4736-00f067aa0ba902b7-01"},
		{"forbidden version", "ff-" + upstreamTraceID + "-" + upstreamSpanID + "-01"},
		{"prose", "trace this please"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("TRACEPARENT", tc.header)

			events := runCollected(t)
			body := runStartedBody(t, events)

			if !api.ValidTraceID(events[0].TraceID) {
				t.Fatalf("trace_id = %q, want a fresh valid one", events[0].TraceID)
			}
			if events[0].TraceID == upstreamTraceID {
				t.Error("senro adopted a trace ID out of a traceparent it should have refused")
			}
			if body.ParentSpanID != "" {
				t.Errorf("parent_span_id = %q, want empty", body.ParentSpanID)
			}
		})
	}
}

// TestAMalformedTraceparentDoesNotDragTheTracestateInWithIt keeps the
// refusal complete. A tracestate belongs to the trace its traceparent named,
// so carrying it into a trace senro started instead would attach one
// vendor's routing data to a trace that vendor has never heard of.
func TestAMalformedTraceparentDoesNotDragTheTracestateInWithIt(t *testing.T) {
	t.Setenv("TRACEPARENT", "nonsense")
	t.Setenv("TRACESTATE", "congo=t61rcWkgMzE")

	body := runStartedBody(t, runCollected(t))
	if body.TraceState != "" {
		t.Errorf("tracestate = %q, want empty: it belongs to the trace that was refused", body.TraceState)
	}
}

// TestARunWithNothingInTheEnvironmentStartsItsOwnTrace is the standalone
// case, and it must stay silent rather than warning: senro on a laptop is
// not misconfigured for having no CI system above it.
func TestARunWithNothingInTheEnvironmentStartsItsOwnTrace(t *testing.T) {
	t.Setenv("TRACEPARENT", "")
	t.Setenv("TRACESTATE", "")
	t.Setenv("traceparent", "")

	events := runCollected(t)
	body := runStartedBody(t, events)

	if !api.ValidTraceID(events[0].TraceID) {
		t.Errorf("trace_id = %q, want a valid generated one", events[0].TraceID)
	}
	if !api.ValidSpanID(body.SpanID) {
		t.Errorf("span_id = %q, want a valid generated one", body.SpanID)
	}
	if body.ParentSpanID != "" {
		t.Errorf("parent_span_id = %q, want empty", body.ParentSpanID)
	}
}

// tracedStepRun runs a one-step pipeline whose step reports the trace context
// its own process was launched with, and returns that report alongside the
// step's span from the ledger. on is the executor to run it on, so the two
// executors this feature is proved end to end on share one body.
//
// The step's own stdout is the only place the answer can come from. The
// environment is deliberately absent from the event stream (it is where secret
// PATHS live), so nothing but the step itself can say what it received.
func tracedStepRun(t *testing.T, opts ...senro.WorkflowOption) (reported, span, traceID string) {
	t.Helper()

	pipe := senro.New("traced")
	w := pipe.Workflow("main", opts...)
	w.Step("tool", exec.Command("sh", "-c", `printf '%s' "$TRACEPARENT"`))

	dir := t.TempDir()
	c := &collector{}
	if err := senro.Run(t.Context(), pipe, senro.WithSink(c), senro.WithDir(dir),
		senro.WithTraceContext(upstreamHeader, "")); err != nil {
		t.Fatalf("Run: %v", err)
	}

	for _, e := range c.all() {
		traceID = e.TraceID
		if e.Type == api.StepStarted && e.Step == "tool" {
			var b api.StepStartedBody
			if err := e.Decode(&b); err != nil {
				t.Fatalf("decode step.started: %v", err)
			}
			span = b.SpanID
		}
	}
	if span == "" {
		t.Fatal("no step.started span to compare against")
	}

	out, err := os.ReadFile(eventlog.NewLogSet(dir).Path("tool", 1, api.StreamStdout))
	if err != nil {
		t.Fatalf("reading the step's stdout: %v", err)
	}
	return strings.TrimSpace(string(out)), span, traceID
}

// assertToolRanInsideTheStep is the whole outbound claim: the tool the step
// ran was launched inside the run's trace, as a child of that step's own
// attempt.
func assertToolRanInsideTheStep(t *testing.T, reported, span, traceID string) {
	t.Helper()

	p, ok := api.ParseTraceParent(reported)
	if !ok {
		t.Fatalf("the step's tool was launched with TRACEPARENT=%q, which no conformant "+
			"consumer will accept, so it starts a trace of its own", reported)
	}
	if p.TraceID != traceID {
		t.Errorf("the tool is in trace %q, the run is in %q: the pipeline is traced and the "+
			"work it runs is not", p.TraceID, traceID)
	}
	if p.TraceID != upstreamTraceID {
		t.Errorf("the tool is in trace %q, want the inbound trace %q the run joined",
			p.TraceID, upstreamTraceID)
	}
	if p.SpanID != span {
		t.Errorf("the tool's parent span is %q, want the step's own attempt span %q", p.SpanID, span)
	}
}

// TestAToolInsideAStepBecomesAChildOfThatStep is the end-to-end proof on the
// local executor, through senro.Run rather than through anything internal.
func TestAToolInsideAStepBecomesAChildOfThatStep(t *testing.T) {
	reported, span, traceID := tracedStepRun(t)
	assertToolRanInsideTheStep(t, reported, span, traceID)
}

// TestAToolInsideAContainerisedStepBecomesAChildOfThatStep is the same proof
// on the container executor, where the environment is built a completely
// different way (the daemon lays the declared env over the image's ENV). One
// assertion body, two substrates.
func TestAToolInsideAContainerisedStepBecomesAChildOfThatStep(t *testing.T) {
	c := dockertest.Require(t)
	dockertest.Pull(t, c)

	reported, span, traceID := tracedStepRun(t, senro.On(container.Image(dockertest.Image)))
	assertToolRanInsideTheStep(t, reported, span, traceID)
}

// TestAnExporterCanBeAnOrdinarySink is the claim the whole design rests on:
// that senro needs no OpenTelemetry dependency because an exporter is a Sink
// somebody writes in their own program.
//
// It builds spans out of nothing but the public event stream, exactly as
// examples/otelexport does, and asserts the result is a connected tree
// rooted at the inbound span. If this test needs anything senro does not
// publish, the design has a hole in it.
func TestAnExporterCanBeAnOrdinarySink(t *testing.T) {
	pipe := senro.New("traced")
	l := pipe.Workflow("main")
	l.Step("fetch", exec.Command("true"))
	l.Step("lint", exec.Command("true")).Needs("fetch")
	l.Step("test", exec.Command("true")).Needs("fetch")

	type span struct{ id, parent, name string }
	var mu sync.Mutex
	spans := map[string]span{}
	var traceID string

	sink := senro.SinkFunc(func(e api.Event) {
		mu.Lock()
		defer mu.Unlock()
		traceID = e.TraceID
		switch e.Type {
		case api.RunStarted:
			var b api.RunStartedBody
			_ = e.Decode(&b)
			spans[b.SpanID] = span{b.SpanID, b.ParentSpanID, "run"}
		case api.StepStarted:
			var b api.StepStartedBody
			_ = e.Decode(&b)
			spans[b.SpanID] = span{b.SpanID, b.ParentSpanID, e.Step}
		}
	})

	if err := senro.Run(t.Context(), pipe, senro.WithSink(sink),
		senro.WithDir(t.TempDir()), senro.WithTraceContext(upstreamHeader, "")); err != nil {
		t.Fatalf("Run: %v", err)
	}

	if traceID != upstreamTraceID {
		t.Fatalf("trace_id = %q, want %q", traceID, upstreamTraceID)
	}
	if len(spans) != 4 {
		t.Fatalf("built %d spans, want 4 (one run, three steps)", len(spans))
	}

	// Every span must reach the inbound parent by following parents. A cycle
	// or a dangling parent means the stream did not carry enough to build a
	// tree, which is the finding this test exists to produce.
	for id, s := range spans {
		seen := map[string]bool{}
		for cur := s; ; {
			if seen[cur.id] {
				t.Fatalf("span %s (%s) is in a parent cycle", id, s.name)
			}
			seen[cur.id] = true
			next, ok := spans[cur.parent]
			if !ok {
				if cur.parent != upstreamSpanID {
					t.Errorf("span %s (%s) walks up to %q, which is neither a span in this run nor the inbound parent %q",
						id, s.name, cur.parent, upstreamSpanID)
				}
				break
			}
			cur = next
		}
	}
}
