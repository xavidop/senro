package api

import "time"

// RunState is the fold of a run's event stream: everything a client needs to
// render, derived from events alone.
//
// The same value backs the live attach server, the TUI, offline replay from
// events.jsonl, and the WASM browser UI. One implementation, or the state
// machines drift and the web UI reports a pass while the TUI reports a fail.
type RunState struct {
	Seq uint64 `json:"seq"`
	// ProtoMajor and ProtoMinor identify the protocol version (see Version,
	// VersionMinor) of the engine SERVING this state, set at hub
	// construction, not folded from any event: a client's first contact is
	// GET /api/state, which can happen before a single event exists, and a
	// value only available after folding would leave that window at 0/0,
	// indistinguishable from "no engine at all" to CheckVersion.
	//
	// Zero on a RunState folded from disk (source.FileSource) or a bare
	// NewRunState(): there is no live engine to report a version for.
	ProtoMajor int                        `json:"proto_major,omitempty"`
	ProtoMinor int                        `json:"proto_minor,omitempty"`
	Run        RunInfo                    `json:"run"`
	Steps      map[string]*StepState      `json:"steps"`
	Expansions map[string]*ExpansionState `json:"expansions"`
	// Handlers is every OnFailure and Always handler the stream announced,
	// keyed by the composite handler ID its events carry in the envelope's
	// step field ("deploy/on_failure/collect").
	//
	// Deliberately NOT entries in Steps: a handler is not a plan node, and
	// putting it there would make every renderer's step count wrong. Each
	// handler is also listed on its parent's StepState.Handlers, in the
	// order they ran.
	Handlers map[string]*HandlerState `json:"handlers,omitempty"`
	// Order records step IDs in creation order, so renderers have a stable
	// layout that does not depend on map iteration.
	Order []string `json:"order"`
}

// NewRunState returns an empty state ready for Apply.
func NewRunState() *RunState {
	return &RunState{
		Steps:      make(map[string]*StepState),
		Expansions: make(map[string]*ExpansionState),
		Handlers:   make(map[string]*HandlerState),
	}
}

// RunInfo is run-level state.
type RunInfo struct {
	ID            string `json:"id"`
	Pipeline      string `json:"pipeline,omitempty"`
	EngineVersion string `json:"engine_version,omitempty"`
	PlanDigest    string `json:"plan_digest,omitempty"`
	// Status is the engine's own verdict, taken verbatim from run.finished.
	// It is authoritative: where it disagrees with RollUp over the folded step
	// states, prefer this. RollUp is for summarising a partial or in-flight
	// stream, not for second-guessing a finished run.
	Status RunStatus `json:"status,omitempty"`
	// omitzero, not omitempty: encoding/json ignores omitempty on struct
	// types, so a zero time would ship as "0001-01-01T00:00:00Z" and a client
	// would render an unfinished run as finished in the year 1.
	Started  time.Time `json:"started,omitzero"`
	Finished time.Time `json:"finished,omitzero"`
	Done     bool      `json:"done"`
	// Paused is a run a client has stopped with OpRunPause: the scheduler
	// dispatches nothing new until OpRunResume (or a cancel). Folded true by
	// the control.applied event recording an accepted run.pause, false by
	// run.resume, and false again by run.finished.
	//
	// The run-level companion to StepState.Paused, for the same reason:
	// without it a run stopped on purpose and a run that has hung are the
	// same picture.
	//
	// Deliberately NOT folded from a new event type: a pause takes effect
	// run-wide the instant it is accepted, so control.applied is already the
	// event that says the run stopped.
	//
	// omitempty, so a snapshot of a run nobody paused is byte-for-byte what
	// it was before this field existed.
	Paused bool `json:"paused,omitempty"`
	// CleanupAbandoned mirrors RunFinishedBody's field of the same name: the
	// run closed its ledger with cleanup still running. Folded here so a
	// renderer can show "a lock may still be held" without decoding events.
	CleanupAbandoned bool `json:"cleanup_abandoned,omitempty"`
}

// StepState is one step's state. State is empty until the step reaches a
// terminal state; a started-but-unfinished step has a non-zero Started and an
// empty State.
type StepState struct {
	ID      string `json:"id"`
	Kind    string `json:"kind,omitempty"`
	Group   string `json:"group,omitempty"`
	State   State  `json:"state,omitempty"`
	Attempt int    `json:"attempt,omitempty"`
	// omitzero, not omitempty: encoding/json ignores omitempty on struct
	// types, so a zero time would ship as "0001-01-01T00:00:00Z" and a client
	// would render a running step as finished in the year 1.
	Started  time.Time `json:"started,omitzero"`
	Finished time.Time `json:"finished,omitzero"`
	ExitCode int       `json:"exit_code,omitempty"`
	Cached   bool      `json:"cached,omitempty"`
	Error    string    `json:"error,omitempty"`
	Needs    []string  `json:"needs,omitempty"`
	// Paused is a step the scheduler is holding at a breakpoint: folded true
	// by breakpoint.hit, false again the moment the step moves on or the
	// breakpoint is cleared.
	//
	// Without it a held step is indistinguishable from one still waiting on
	// its dependencies: no Started, no State, no Error. A run stopped on
	// purpose and a run that has hung must not look identical.
	//
	// omitempty, so a snapshot of a run with no breakpoint is byte-for-byte
	// what it was before this field existed.
	Paused bool `json:"paused,omitempty"`
	// LogBytes tracks total bytes appended per stream, so a client knows how
	// much scrollback exists without opening the file.
	LogBytes map[string]int64 `json:"log_bytes,omitempty"`
	// Handlers lists this step's handler IDs in the order they started, as
	// keys into RunState.Handlers, so a renderer can show a step's cleanup
	// story without re-scanning the raw stream.
	Handlers []string `json:"handlers,omitempty"`
}

// Running reports whether the step has started but not reached a terminal state.
func (s *StepState) Running() bool {
	return !s.Started.IsZero() && !s.State.Terminal()
}

// HandlerState is one OnFailure or Always handler's state.
//
// State is empty until the handler reaches a terminal state, and a handler
// that stays that way after the run is done is one the engine never saw
// finish: cleanup abandoned when the grace ran out. That is a different fact
// from a handler that succeeded quietly, and RunInfo.CleanupAbandoned is the
// run-level companion to it.
type HandlerState struct {
	// ID is the composite handler ID ("<parent>/<kind>/<handler>"), which is
	// also the key its log files are stored under.
	ID string `json:"id"`
	// Parent is the step whose settling triggered this handler.
	Parent string `json:"parent"`
	// Kind is "on_failure" or "always". It is the only record of which list a
	// handler came from; the events are otherwise identical in shape.
	Kind  string `json:"kind,omitempty"`
	State State  `json:"state,omitempty"`
	// omitzero, not omitempty: encoding/json ignores omitempty on struct
	// types, so a zero time would ship as "0001-01-01T00:00:00Z".
	Started  time.Time `json:"started,omitzero"`
	Finished time.Time `json:"finished,omitzero"`
	Error    string    `json:"error,omitempty"`
}

// Running reports whether the handler started and never reached a terminal
// state.
func (h *HandlerState) Running() bool {
	return !h.Started.IsZero() && !h.State.Terminal()
}

// ExpansionState records a resolved fan-out.
type ExpansionState struct {
	Parent   string   `json:"parent"`
	Children []string `json:"children"`
	Count    int      `json:"count"`
	Skipped  int      `json:"skipped"`
}

// GroupCounts is the collapsed summary a renderer shows for an expansion:
// "37 units · 2 failed · 31 cached · 4 running".
//
// The fields are NOT mutually exclusive and do not sum to Total. Done counts
// every child in a terminal state, so it is a superset of both Failed and
// Cached; Running and Done are disjoint by construction, since a running step
// is by definition not terminal. Children counted only in Total are those
// created but not yet dispatched. Render each field on its own terms rather
// than deriving one from the others.
type GroupCounts struct {
	Total   int `json:"total"`
	Running int `json:"running"`
	Failed  int `json:"failed"`
	// Cached counts children whose State == StateCached. It deliberately does
	// not consult StepState.Cached: an engine wanting a step in this tally
	// must emit State: StateCached.
	Cached int `json:"cached"`
	Done   int `json:"done"`
}

// Group summarises an expansion's children. Aggregation lives in the fold so
// every client reports identical counts.
func (s *RunState) Group(parent string) GroupCounts {
	exp, ok := s.Expansions[parent]
	if !ok {
		return GroupCounts{}
	}
	var c GroupCounts
	c.Total = len(exp.Children)
	for _, id := range exp.Children {
		st, ok := s.Steps[id]
		if !ok {
			continue
		}
		switch {
		case st.State.Failed():
			c.Failed++
		case st.State == StateCached:
			c.Cached++
		case st.Running():
			c.Running++
		}
		if st.State.Terminal() {
			c.Done++
		}
	}
	return c
}
