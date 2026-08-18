package api

import "time"

// Log stream names.
const (
	StreamStdout = "stdout"
	StreamStderr = "stderr"
)

// StepCreatedBody is the payload of a step.created event, emitted once per
// node when the plan is resolved or when an expansion adds children.
type StepCreatedBody struct {
	Kind  string   `json:"kind"` // "exec" or "func"
	Group string   `json:"group,omitempty"`
	Needs []string `json:"needs,omitempty"`
}

// StepStartedBody is the payload of a step.started event.
type StepStartedBody struct {
	Cmd []string `json:"cmd,omitempty"`
	// WorkDir's wire name is "workdir", deliberately one token rather than the
	// module's usual snake_case for multi-word names. It matches Dockerfile
	// WORKDIR and the OCI image config's WorkingDir convention, which is what a
	// reader of this event is already holding in their head. Do not "correct"
	// it to "work_dir" later: this is published API and a rename is breaking.
	WorkDir       string `json:"workdir,omitempty"`
	ExecutorClass string `json:"executor_class,omitempty"`
	Platform      string `json:"platform,omitempty"`
	// Func names the registered function a func step invoked; empty for an
	// exec step, whose Cmd says what ran. Without it a func step's start is
	// an empty command with no other clue to what ran.
	Func string `json:"func,omitempty"`

	// SpanID is this ATTEMPT's span, not the step's. A step retried three
	// times produces three step.started events carrying three different span
	// IDs, because three attempts are three pieces of work with three
	// durations and three outcomes, and one span claiming to be all of them
	// would describe something that did not happen.
	SpanID string `json:"span_id,omitempty"`

	// ParentSpanID is the span this attempt hangs off, taken from the
	// DEPENDENCY GRAPH: the span of the first of the step's Needs in plan
	// order, or the run's own span when the step has none. Two steps run
	// back to back only because of a concurrency limit are siblings;
	// deriving parentage from wall-clock order would report a pipeline with
	// no parallelism, exactly what an operator opened the trace to find.
	ParentSpanID string `json:"parent_span_id,omitempty"`

	// LinkedSpanIDs are the step's remaining needs, in plan order, present
	// only where there is more than one. A span has exactly one parent, so
	// the needs that could not become the parent are recorded as
	// OpenTelemetry links: causal but not containment. Without them a fan-in
	// step claims to have waited on one thing when it waited on five.
	LinkedSpanIDs []string `json:"linked_span_ids,omitempty"`
}

// StepFinishedBody is the payload of a step.finished event.
//
// ExitCode is the workload's verdict; Error is set only for infrastructure
// failure. They are separate because retry predicates key off the difference.
type StepFinishedBody struct {
	State State `json:"state"`
	// ExitCode is omitted when zero. Absent means either "ran and exited 0" or
	// "no exit code applies"; State disambiguates: a succeeded or failed step
	// ran, a skipped or cached one did not.
	ExitCode int           `json:"exit_code,omitempty"`
	Duration time.Duration `json:"duration_ns"`
	Error    string        `json:"error,omitempty"`
	// Cached records provenance: this result was restored from cache rather
	// than produced by running the workload. It is NOT what makes a step
	// count as cached in RunState.Group (that is State == StateCached): an
	// engine restoring from cache should emit both together, since Cached
	// alone leaves the step out of GroupCounts.Cached.
	Cached bool `json:"cached,omitempty"`
	// Reason explains a terminal state that is not a failure and so has no
	// Error to carry: today, only a step skipped because a When condition
	// was false ("condition branch:main is false"). It names the condition
	// the pipeline declared, never a value it was compared against.
	// Additive and omitempty.
	Reason string `json:"reason,omitempty"`

	// SpanID is the span this attempt's step.started opened, repeated so a
	// reader never has to correlate on (step, attempt) to know which span
	// just ended.
	SpanID string `json:"span_id,omitempty"`

	// ParentSpanID and LinkedSpanIDs appear here EXACTLY when this event
	// opened the span itself: when the step never started. A cached,
	// condition-skipped or upstream-skipped step emits no step.started, and
	// such steps are a large fraction of a healthy pipeline's nodes; without
	// a span named here they would be absent from the trace entirely.
	//
	// Absent on the ordinary path on purpose: step.started already said
	// where the span hangs, and two copies of one fact can disagree.
	// StartedAt for a span opened here is this event's own timestamp less
	// its Duration.
	ParentSpanID  string   `json:"parent_span_id,omitempty"`
	LinkedSpanIDs []string `json:"linked_span_ids,omitempty"`
}

// StepRetriedBody is the payload of a step.retried event.
//
// Predicate records which retry rule fired, so a run full of infrastructure
// retries stays distinguishable from one full of flaky tests.
//
// Attempt is the attempt about to START, which makes this event the END of
// the one before it: a failed attempt that is going to be retried emits no
// step.finished at all, because the step has not finished. A consumer
// building spans therefore closes the step's currently open span here, and
// opens the next one at the step.started that follows. No span ID is carried
// for the same reason none is needed: a step has at most one attempt in
// flight, so "the open span for this step" is never ambiguous.
type StepRetriedBody struct {
	Attempt   int    `json:"attempt"`
	Reason    string `json:"reason"`
	Predicate string `json:"predicate"`
	BackoffMS int64  `json:"backoff_ms"`
}

// StepLogAppendedBody is the payload of a step.log.appended event.
//
// It is a marker, not content: a byte range into the step's log file. Clients
// fetch the bytes on demand. In a 300-node fan-out a client needs the log body
// of exactly one step, so keeping content off the lifecycle channel is what
// makes that channel affordable to deliver losslessly.
type StepLogAppendedBody struct {
	Stream string `json:"stream"`
	Offset int64  `json:"offset"`
	Len    int64  `json:"len"`
	Lines  int    `json:"lines"`
}
