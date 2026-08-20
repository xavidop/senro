package api

import "time"

// RunStartedBody is the payload of a run.started event.
//
// The four trace fields are the run's whole place in a distributed trace,
// stated once, here, because every one of them is fixed for the run's
// lifetime. The trace ID itself is NOT among them: it is on the envelope
// (Event.TraceID), on every event, because that repetition is exactly what
// makes a run one trace rather than one traced event followed by a great
// many untraced ones.
type RunStartedBody struct {
	Pipeline      string    `json:"pipeline"`
	EngineVersion string    `json:"engine_version"`
	PlanDigest    string    `json:"plan_digest"`
	CWD           string    `json:"cwd,omitempty"`
	StartedAt     time.Time `json:"started_at"`

	// SpanID is the run's own span: the root of every span this run produces,
	// and the parent of any step whose place in the graph gives it no other.
	// Sixteen lowercase hex characters; see ValidSpanID.
	SpanID string `json:"span_id,omitempty"`

	// ParentSpanID is the span this run is a child of, taken from an inbound
	// traceparent (the TRACEPARENT environment variable, or
	// senro.WithTraceContext).
	//
	// Empty means the run started the trace: nothing upstream offered one, or
	// what it offered was malformed and was ignored. The two are deliberately
	// indistinguishable here; either way this run has no parent.
	ParentSpanID string `json:"parent_span_id,omitempty"`

	// TraceFlags is the W3C trace-flags byte as two lowercase hex characters,
	// carried through from an inbound traceparent and "00" when there was
	// none. See TraceFlagSampled and ParseTraceFlags.
	//
	// A byte rather than a boolean, so a flag defined after this build
	// shipped survives senro rather than being rounded off to the one bit
	// this build knows.
	TraceFlags string `json:"trace_flags,omitempty"`

	// TraceState is the W3C tracestate value, verbatim, and empty when there
	// was none. senro never parses, validates or acts on it: it is opaque
	// vendor routing data whose only job is to reach downstream unchanged.
	// Independent of the traceparent: a tracestate senro cannot make sense
	// of is never a reason to drop a good parentage.
	TraceState string `json:"tracestate,omitempty"`
}

// RunFinishedBody is the payload of a run.finished event. The Steps histogram
// lets a client report the outcome without holding every step's state.
type RunFinishedBody struct {
	Status   RunStatus     `json:"status"`
	Steps    map[State]int `json:"steps,omitempty"`
	Duration time.Duration `json:"duration_ns"`

	// CleanupAbandoned reports that the run stopped waiting for cleanup that
	// was still running, and closed its ledger with it unfinished. A handler
	// the engine gave up on leaves a handler.started with no handler.failed,
	// indistinguishable from one that succeeded quietly, so the run has to
	// say so: a lock may still be held, and the next run needs to know.
	CleanupAbandoned bool `json:"cleanup_abandoned,omitempty"`

	// SpanID repeats the run's own span, the one RunStartedBody.SpanID
	// opened, so this event alone is enough to close it: a client that
	// joined mid-run or overran its ring never saw run.started, and the
	// run's last event must be understandable on its own.
	SpanID string `json:"span_id,omitempty"`
}

// PlanResolvedBody is the payload of a plan.resolved event. It ties a run to
// its timetable so a FileSource can find the plan without a second read.
type PlanResolvedBody struct {
	Digest string `json:"digest"`
	Nodes  int    `json:"nodes"`
}

// PlanExpandedBody is the payload of a plan.expanded event.
//
// Children are recorded in full so a re-run reconstitutes exactly the same set
// without re-running discovery. Order is sorted by the engine; an expander
// returning a nondeterministic order is a bug.
type PlanExpandedBody struct {
	Parent   string   `json:"parent"`
	Children []string `json:"children"`
	// Count is the expander's own tally, recorded for provenance. Renderers
	// derive totals from len(Children), not from this field.
	Count   int `json:"count"`
	Skipped int `json:"skipped"`
}

// PlanGeneratedBody is the payload of a plan.generated event: one generator's
// fragment, spliced into the graph that was already running.
//
// Children are recorded in full for the reason PlanExpandedBody records them
// in full: a reader reconstitutes the set without re-deriving it, and here
// there is no plan.json to fall back on, because a generated node cannot be
// in the file written before the run.
//
// Digest names the fragment's bytes in the CAS. It is what makes a
// nondeterministic generator reproducible: a re-run replays the recorded
// fragment rather than calling the generator again (design §2.8.1).
type PlanGeneratedBody struct {
	Generator string   `json:"generator"`
	Children  []string `json:"children"`
	// Nodes and Edges are the generator's own tally, recorded for provenance.
	// Renderers derive totals from len(Children), not from these.
	Nodes  int    `json:"nodes"`
	Edges  int    `json:"edges"`
	Digest string `json:"digest"`
}

// PlanExpansionSkippedBody is the payload of a plan.expansion_skipped event,
// emitted when an expansion produced no children at all.
type PlanExpansionSkippedBody struct {
	Parent string `json:"parent"`
	Reason string `json:"reason"`
}
