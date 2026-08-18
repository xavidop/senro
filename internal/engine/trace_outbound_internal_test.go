package engine

import (
	"strings"
	"testing"

	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/internal/plan"
)

// The W3C specification's own example values, so a reader can check them
// against the document rather than against another line of this file.
const (
	outTraceID = "4bf92f3577b34da6a3ce929d0e0e4736"
	outSpanID  = "00f067aa0ba902b7"
	outHeader  = "00-" + outTraceID + "-" + outSpanID + "-01"
)

// tableFor is a span table for a one-node plan, joined to the inbound
// context given.
func tableFor(traceparent, tracestate string) *spanTable {
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{{ID: "build", Kind: "exec", Cmd: []string{"true"}}}}
	return newSpanTable(p, traceparent, tracestate)
}

// envValue reads NAME's value out of an environment slice, reporting how many
// entries declared that name: a duplicate is the failure this returns a count
// for, since which of two a process sees is unspecified.
func envValue(env []string, name string) (string, int) {
	value, found := "", 0
	for _, kv := range env {
		n, v, ok := strings.Cut(kv, "=")
		if ok && n == name {
			value, found = v, found+1
		}
	}
	return value, found
}

// TestTheOutboundTraceparentCarriesTheAttemptsOwnSpan is the whole feature in
// one assertion. A tool that reads TRACEPARENT out of its environment must
// become a child of the ATTEMPT that ran it: the run's own span would make
// every tool in every step report the same parent and flatten the trace into
// a list.
func TestTheOutboundTraceparentCarriesTheAttemptsOwnSpan(t *testing.T) {
	tab := tableFor(outHeader, "congo=t61rcWkgMzE")
	span, _, _ := tab.begin("build")

	env := tab.outboundEnv(nil, span)

	got, n := envValue(env, "TRACEPARENT")
	if n != 1 {
		t.Fatalf("found %d TRACEPARENT entries in %v, want exactly 1", n, env)
	}
	p, ok := api.ParseTraceParent(got)
	if !ok {
		t.Fatalf("TRACEPARENT=%q is not a value a conformant consumer will accept", got)
	}
	if p.TraceID != outTraceID {
		t.Errorf("outbound trace ID = %q, want the run's %q", p.TraceID, outTraceID)
	}
	if p.SpanID != span {
		t.Errorf("outbound span = %q, want this attempt's own span %q", p.SpanID, span)
	}
	if p.SpanID == tab.runSpan {
		t.Error("outbound span is the RUN's span: every tool in every step would report the same parent")
	}
	if !p.Sampled() {
		t.Error("the inbound sampled flag was dropped on the way out")
	}
}

// TestEveryAttemptExportsItsOwnTraceparent keeps a retried step honest. Each
// attempt has its own span (three attempts are three durations and three
// outcomes), so each attempt's command must carry its own.
func TestEveryAttemptExportsItsOwnTraceparent(t *testing.T) {
	tab := tableFor(outHeader, "")

	first, _, _ := tab.begin("build")
	firstEnv := tab.outboundEnv(nil, first)
	second, _, _ := tab.begin("build")
	secondEnv := tab.outboundEnv(nil, second)

	a, _ := envValue(firstEnv, "TRACEPARENT")
	b, _ := envValue(secondEnv, "TRACEPARENT")
	if a == b {
		t.Fatalf("both attempts exported %q: the second attempt is a span of its own", a)
	}
	pa, _ := api.ParseTraceParent(a)
	pb, _ := api.ParseTraceParent(b)
	if pa.TraceID != pb.TraceID {
		t.Errorf("attempts are in different traces: %q and %q", pa.TraceID, pb.TraceID)
	}
	if pb.SpanID != second {
		t.Errorf("second attempt exported span %q, want its own %q", pb.SpanID, second)
	}
}

// TestAStepThatDeclaresItsOwnTraceContextKeepsIt is the rule that outranks
// everything else here: an author who wrote a traceparent meant it, and
// overwriting it reroutes that step's work into a trace they never named.
// Both spellings, because senro reads TRACEPARENT before traceparent:
// exporting the uppercase name beside an author's lowercase one would
// override it by another route.
func TestAStepThatDeclaresItsOwnTraceContextKeepsIt(t *testing.T) {
	for _, name := range []string{"TRACEPARENT", "traceparent"} {
		t.Run(name, func(t *testing.T) {
			const declared = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
			tab := tableFor(outHeader, "congo=t61rcWkgMzE")
			span, _, _ := tab.begin("build")

			env := tab.outboundEnv([]string{name + "=" + declared}, span)

			got, n := envValue(env, name)
			if n != 1 || got != declared {
				t.Errorf("%s = %q (%d entries), want the declared %q exactly once", name, got, n, declared)
			}
			if _, n := envValue(env, "TRACEPARENT"); name == "traceparent" && n != 0 {
				t.Error("senro exported TRACEPARENT beside the author's traceparent, which overrides it")
			}
			if _, n := envValue(env, "TRACESTATE"); n != 0 {
				t.Error("senro's tracestate was exported beside a traceparent it does not belong to")
			}
		})
	}
}

// TestADeclaredTracestateIsNotReplaced covers the other half of the pair. A
// step that names its own vendor state keeps it, and still gets senro's
// traceparent: the state is the author's, the span is this attempt's.
func TestADeclaredTracestateIsNotReplaced(t *testing.T) {
	tab := tableFor(outHeader, "congo=t61rcWkgMzE")
	span, _, _ := tab.begin("build")

	env := tab.outboundEnv([]string{"TRACESTATE=mine=1"}, span)

	if got, n := envValue(env, "TRACESTATE"); n != 1 || got != "mine=1" {
		t.Errorf("TRACESTATE = %q (%d entries), want the declared value exactly once", got, n)
	}
	if _, n := envValue(env, "TRACEPARENT"); n != 1 {
		t.Errorf("found %d TRACEPARENT entries, want the attempt's own", n)
	}
}

// TestNoTracestateIsExportedWhenTheRunHasNone keeps senro from inventing
// vendor routing data. An empty TRACESTATE is not the same as no TRACESTATE:
// it is a value a downstream propagator will carry and try to parse.
func TestNoTracestateIsExportedWhenTheRunHasNone(t *testing.T) {
	tab := tableFor(outHeader, "")
	span, _, _ := tab.begin("build")

	env := tab.outboundEnv(nil, span)

	if _, n := envValue(env, "TRACESTATE"); n != 0 {
		t.Errorf("found %d TRACESTATE entries, want none: this run has no vendor state to carry", n)
	}
}

// TestTheOutboundEnvNeverWritesIntoTheCallersSlice guards a trap the secret
// delivery path already documents: the step's declared environment is the
// PLAN's own slice, and appending to it in place would leave the next attempt,
// and every other reader of that node, holding this attempt's traceparent.
func TestTheOutboundEnvNeverWritesIntoTheCallersSlice(t *testing.T) {
	tab := tableFor(outHeader, "congo=t61rcWkgMzE")
	span, _, _ := tab.begin("build")

	declared := make([]string, 1, 8)
	declared[0] = "CGO_ENABLED=0"

	_ = tab.outboundEnv(declared, span)

	if len(declared) != 1 {
		t.Fatalf("the caller's slice grew to %d entries", len(declared))
	}
	spare := declared[:cap(declared)]
	for i, kv := range spare[1:] {
		if kv != "" {
			t.Errorf("the caller's spare capacity was written at %d: %q", i+1, kv)
		}
	}
}

// TestANilSpanTableExportsNothing matches what every other method on this
// type does with a nil receiver, and matters for the same reason: an internal
// test that assembles a runCore by hand must not have to build a trace it
// does not exercise.
func TestANilSpanTableExportsNothing(t *testing.T) {
	var tab *spanTable
	env := tab.outboundEnv([]string{"A=1"}, "00f067aa0ba902b7")
	if len(env) != 1 || env[0] != "A=1" {
		t.Errorf("outboundEnv = %v, want the declared environment untouched", env)
	}
}

// TestAnUnusableSpanExportsNothing is api.TraceParent.String's empty-string
// contract carried through to the environment. A header of all-zero
// identifiers is syntactically perfect and means INVALID to every consumer,
// which is strictly worse than saying nothing at all.
func TestAnUnusableSpanExportsNothing(t *testing.T) {
	tab := tableFor(outHeader, "")
	for _, span := range []string{"", "0000000000000000", "not-hex"} {
		env := tab.outboundEnv(nil, span)
		if len(env) != 0 {
			t.Errorf("span %q exported %v, want nothing", span, env)
		}
	}
}
