package engine

import (
	"strings"
	"sync"

	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/internal/plan"
)

// The names a step's own command reads its inbound trace context from, in
// both spellings. senro EXPORTS the uppercase pair (what OpenTelemetry SDKs
// and CI systems use) and RECOGNISES the lowercase pair as something a
// pipeline author may have declared for themselves (see
// senro.envTraceContext).
const (
	traceParentEnv      = "TRACEPARENT"
	traceStateEnv       = "TRACESTATE"
	traceParentEnvLower = "traceparent"
	traceStateEnvLower  = "tracestate"
)

// spanTable is a run's trace context and the span every step is currently
// on. The rule for choosing a span's parent is the dependency graph, NOT
// wall-clock order, which is the single decision this type is really for:
// see parentLocked.
//
// The trace-scoped fields are written once, before the run's first event,
// so they need no lock; only the per-step span map is contended.
//
// Every method tolerates a nil receiver and returns no identifiers at all.
// Run always builds one, so nil only reaches internal tests that assemble a
// runCore by hand. No identifiers rather than partial ones is the
// deliberate part: an event with no span_id is skipped, while one with a
// span_id and an empty parent claims a place in a trace and names nothing
// to hang it from, a lie a consumer cannot detect.
type spanTable struct {
	// traceID is stable for the whole run, which is what makes the run one
	// trace. It is either an inbound trace joined or a fresh one started, and
	// nothing after construction can change it.
	traceID string
	// runSpan is the run's own span: the root, and the parent of every step
	// the graph gives no other.
	runSpan string
	// parentSpan is the span the run is a child of, empty when this run
	// started the trace.
	parentSpan string
	// flags and state are the rest of the inbound W3C context, carried
	// through untouched. senro never acts on either.
	flags byte
	state string

	// needs is every step's declared dependencies, in plan order, read-only
	// after construction. Copied out of the plan so parentage does not
	// depend on holding a *plan.Plan at every emit site.
	needs map[string][]string

	mu sync.Mutex
	// open maps a step to the span ID of its LATEST attempt. Latest, not
	// first: a dependent that starts after its need was retried belongs under
	// the attempt that actually produced what it consumed.
	open map[string]string
}

// newSpanTable resolves the run's trace context and indexes the plan's
// edges. traceparent and tracestate arrive raw and unvalidated; a
// traceparent that does not parse is IGNORED and the run starts a fresh
// trace, as the specification requires: salvaging half a malformed value
// would link to a trace that does not exist. tracestate is never parsed and
// is only carried when a valid traceparent came with it: vendor state
// belonging to a trace this run is not in is somebody else's routing data.
func newSpanTable(p *plan.Plan, traceparent, tracestate string) *spanTable {
	t := &spanTable{
		runSpan: api.NewSpanID(),
		needs:   make(map[string][]string, len(p.Nodes)),
		open:    make(map[string]string, len(p.Nodes)),
	}
	for i := range p.Nodes {
		if n := &p.Nodes[i]; len(n.Needs) > 0 {
			t.needs[n.ID] = n.Needs
		}
	}

	if in, ok := api.ParseTraceParent(traceparent); ok {
		t.traceID, t.parentSpan, t.flags, t.state = in.TraceID, in.SpanID, in.Flags, tracestate
		return t
	}
	t.traceID = api.NewTraceID()
	return t
}

// begin mints a span for a fresh attempt at stepID, records it as the
// step's current span, and returns it with the parentage the graph gives
// it. Once per attempt, and per attempt is the point: three attempts are
// three durations and three outcomes, so a retried step is three spans.
func (t *spanTable) begin(stepID string) (span, parent string, links []string) {
	if t == nil {
		return "", "", nil
	}
	span = api.NewSpanID()

	t.mu.Lock()
	defer t.mu.Unlock()
	parent, links = t.parentLocked(stepID)
	t.open[stepID] = span
	return span, parent, links
}

// finishSpan is what a step.finished emit site asks for: the span to close,
// and the parentage to state alongside it.
//
// parent and links come back EMPTY when the step already had a span open
// (the ordinary path): step.started said where the span hangs, and a second
// copy of that fact could disagree. They are filled in when this call had
// to MINT the span because the step never started one, which is not rare: a
// cached, condition-skipped, or upstream-skipped step emits step.finished
// with no step.started, and this event is the only place its parentage can
// ever be stated.
//
// Deciding and recording under ONE lock acquisition, rather than
// look-then-act: only one goroutine reaches here per step (oc.settle's
// claim), but that fact lives in a different file, and a split here would
// silently turn a future second caller into two spans for one step. The
// span ID is minted before the lock and discarded unused on the ordinary
// path, keeping crypto/rand out of the critical section.
func (t *spanTable) finishSpan(stepID string) (span, parent string, links []string) {
	if t == nil {
		return "", "", nil
	}
	fresh := api.NewSpanID()

	t.mu.Lock()
	defer t.mu.Unlock()
	if open := t.open[stepID]; open != "" {
		return open, "", nil
	}
	parent, links = t.parentLocked(stepID)
	t.open[stepID] = fresh
	return fresh, parent, links
}

// handlerSpan mints a span for one handler run, parented on the attempt
// whose outcome triggered it.
//
// Not registered in t.open: that map answers "what is this STEP's current
// span", and no step may declare a handler as a need. One call per handler
// RUN, not per handler event: handler.started and its completion event are
// the two ends of ONE span, and minting per event would produce
// zero-duration spans describing a handler that never ran. The parent falls
// back to the run span if the triggering step somehow has none, since an
// empty parent is a reference to nothing.
func (t *spanTable) handlerSpan(parentStep string) (span, parent string) {
	if t == nil {
		return "", ""
	}
	span = api.NewSpanID()

	t.mu.Lock()
	defer t.mu.Unlock()
	if parent = t.open[parentStep]; parent == "" {
		parent = t.runSpan
	}
	return span, parent
}

// parentLocked chooses a step's parent span and links from the dependency
// graph. The caller holds t.mu.
//
// The rule: no needs means the run's own span; otherwise the first need IN
// PLAN ORDER is the parent and the rest are links (a span has exactly one
// parent, and OpenTelemetry links exist for causality that is not
// containment). Plan order rather than completion order, so two runs of one
// pipeline produce identically shaped traces. A need with no span yet is
// skipped: it should not happen, but the alternative is an empty parent, a
// reference to nothing.
//
// The rule deliberately does NOT use when anything ran: two steps serial
// only because MaxParallel was 1 are siblings, and wall-clock nesting would
// report a pipeline with no parallelism at all.
func (t *spanTable) parentLocked(stepID string) (parent string, links []string) {
	var have []string
	for _, need := range t.needs[stepID] {
		if s := t.open[need]; s != "" {
			have = append(have, s)
		}
	}
	if len(have) == 0 {
		return t.runSpan, nil
	}
	return have[0], have[1:]
}

// outboundEnv is env plus the W3C trace context that span belongs to: the
// only place senro propagates a trace OUTWARDS, so a traced tool inside a
// step becomes a child of that step instead of the root of an unconnected
// trace. span is always the attempt's or handler run's, never the run's:
// the run's span would flatten the trace into a list.
//
// TRACEPARENT is exported only when api.TraceParent.String accepts the
// context: it renders an invalid one as "" rather than an all-zero header,
// which the specification reserves to mean "discard this". TRACESTATE only
// when a valid inbound traceparent brought one (see newSpanTable): senro
// never invents vendor routing data, and an empty TRACESTATE is not the
// same as none.
//
// A declared name always wins: an author who wrote a traceparent meant it,
// so it is never replaced or shadowed by the other spelling, and a declared
// traceparent also suppresses senro's TRACESTATE (vendor state belongs to
// the trace its traceparent named). A declared TRACESTATE alone does not
// suppress the traceparent: it says which vendor state the steps carry, not
// which trace the work belongs to.
//
// The result is a fresh slice whenever anything is added: env is frequently
// plan.Node.Env itself, and appending in place would leave this attempt's
// traceparent visible to the next attempt and to anything digesting the
// plan.
func (t *spanTable) outboundEnv(env []string, span string) []string {
	if t == nil {
		return env
	}
	header := api.TraceParent{TraceID: t.traceID, SpanID: span, Flags: t.flags}.String()
	if header == "" {
		return env
	}
	if declaresEnv(env, traceParentEnv) || declaresEnv(env, traceParentEnvLower) {
		return env
	}

	out := make([]string, len(env), len(env)+2)
	copy(out, env)
	out = append(out, traceParentEnv+"="+header)
	if t.state != "" && !declaresEnv(env, traceStateEnv) && !declaresEnv(env, traceStateEnvLower) {
		out = append(out, traceStateEnv+"="+t.state)
	}
	return out
}

// declaresEnv reports whether env sets name. The comparison is on the name
// alone, so "TRACEPARENT=" counts: a step that exports an empty value has
// still spoken about that variable, and a downstream reader takes an empty
// string as "no inbound trace" rather than as "ask senro instead".
func declaresEnv(env []string, name string) bool {
	for _, kv := range env {
		if n, _, ok := strings.Cut(kv, "="); ok && n == name {
			return true
		}
	}
	return false
}
