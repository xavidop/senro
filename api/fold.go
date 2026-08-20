package api

import (
	"fmt"
	"time"
)

// Apply folds one event into the state.
//
// Two rules govern this function and neither is negotiable:
//
//  1. Unknown event types are ignored, not rejected. A newer engine emits
//     types this build has never seen, and erroring on them would make every
//     schema addition a breaking change.
//  2. Unknown payload fields are ignored, which encoding/json does for free.
//
// An out-of-order sequence number IS an error: it means the caller lost
// ordering, and silently folding it would produce a state that never existed.
func (s *RunState) Apply(e Event) error {
	// A RunState rehydrated from JSON can arrive with nil maps; the attach path
	// is snapshot-then-delta, so Apply must tolerate one it did not construct.
	if s.Steps == nil {
		s.Steps = make(map[string]*StepState)
	}
	if s.Expansions == nil {
		s.Expansions = make(map[string]*ExpansionState)
	}
	if s.Handlers == nil {
		s.Handlers = make(map[string]*HandlerState)
	}
	// No special case for e.Seq == 0: a real engine never emits it (the
	// ledger increments before assigning, so the first event is Seq 1), and
	// exempting 0 would let a malformed or foreign event bypass the ordering
	// check. Equality (e.Seq == s.Seq) is deliberately allowed: a client
	// resuming one seq too early replays the last event it already folded,
	// and that replay must be idempotent, not an error. See
	// TestApplyLogBytesIdempotentOnReplay.
	if e.Seq < s.Seq {
		return fmt.Errorf("api: out-of-order event: seq %d after %d", e.Seq, s.Seq)
	}
	if e.Seq > s.Seq {
		s.Seq = e.Seq
	}

	// run is on every envelope. A client subscribing mid-stream never sees
	// run.started, so seeding it here is what makes "fold survives a
	// truncated stream" true. Assigns a string, materialises no step state,
	// so it is harmless for any event type.
	if s.Run.ID == "" && e.Run != "" {
		s.Run.ID = e.Run
	}

	switch e.Type {
	case RunStarted:
		var b RunStartedBody
		if err := e.Decode(&b); err != nil {
			return err
		}
		s.Run.ID = e.Run
		s.Run.Pipeline = b.Pipeline
		s.Run.EngineVersion = b.EngineVersion
		s.Run.PlanDigest = b.PlanDigest
		s.Run.Started = b.StartedAt

	case RunFinished:
		var b RunFinishedBody
		if err := e.Decode(&b); err != nil {
			return err
		}
		s.Run.Status = b.Status
		s.Run.Finished = e.TS
		s.Run.Done = true
		s.Run.CleanupAbandoned = b.CleanupAbandoned
		// A paused run can still be cancelled, and run.finished is the only
		// event that follows; leaving the flag set would render a finished
		// run as resumable.
		s.Run.Paused = false

	case PlanResolved:
		var b PlanResolvedBody
		if err := e.Decode(&b); err != nil {
			return err
		}
		s.Run.PlanDigest = b.Digest

	case PlanExpanded:
		var b PlanExpandedBody
		if err := e.Decode(&b); err != nil {
			return err
		}
		s.Expansions[b.Parent] = &ExpansionState{
			Parent:   b.Parent,
			Children: b.Children,
			Count:    b.Count,
			Skipped:  b.Skipped,
		}
		// Materialise children so a renderer can show the group immediately,
		// before any per-child step.created arrives.
		for _, id := range b.Children {
			st := s.step(id)
			st.Group = b.Parent
		}

	case PlanGenerated:
		var b PlanGeneratedBody
		if err := e.Decode(&b); err != nil {
			return err
		}
		s.Expansions[b.Generator] = &ExpansionState{
			Parent:   b.Generator,
			Children: b.Children,
			Count:    b.Nodes,
		}
		// Materialised for the reason an expansion's children are: a renderer
		// can show the generated nodes the moment they exist, before any
		// per-child step.created arrives.
		for _, id := range b.Children {
			st := s.step(id)
			st.Group = b.Generator
		}

	case StepCreated:
		var b StepCreatedBody
		if err := e.Decode(&b); err != nil {
			return err
		}
		st := s.step(e.Step)
		st.Kind = b.Kind
		st.Needs = b.Needs
		if b.Group != "" {
			st.Group = b.Group
		} else if e.Group != "" {
			st.Group = e.Group
		}

	case StepStarted:
		st := s.step(e.Step)
		st.Started = e.TS
		// A new attempt starts clean. Clearing State and Finished but leaving
		// ExitCode and Error behind renders a running step as "exit 137".
		st.Finished = time.Time{}
		st.State = ""
		st.ExitCode = 0
		st.Error = ""
		st.Paused = false
		if e.Attempt > st.Attempt {
			st.Attempt = e.Attempt
		}

	case StepFinished:
		var b StepFinishedBody
		if err := e.Decode(&b); err != nil {
			return err
		}
		st := s.step(e.Step)
		st.State = b.State
		st.ExitCode = b.ExitCode
		st.Error = b.Error
		st.Finished = e.TS
		st.Paused = false
		if b.Cached {
			st.Cached = true
		}

	case StepRetried:
		var b StepRetriedBody
		if err := e.Decode(&b); err != nil {
			return err
		}
		st := s.step(e.Step)
		st.Attempt = b.Attempt
		// A new attempt clears the previous terminal state: the step is
		// pending again, and rendering it as failed would be wrong.
		st.State = ""
		st.Started = time.Time{}
		st.Finished = time.Time{}
		st.ExitCode = 0
		st.Error = ""
		st.Paused = false
		// Each attempt gets its own log file starting at byte 0 (see
		// eventlog.LogSet.Path). LogBytes accumulates via max() (see
		// StepLogAppended below), so leaving the old attempt's high-water
		// mark would hide the new attempt's smaller offsets entirely.
		st.LogBytes = nil

	case StepLogAppended:
		var b StepLogAppendedBody
		if err := e.Decode(&b); err != nil {
			return err
		}
		st := s.step(e.Step)
		if st.LogBytes == nil {
			st.LogBytes = make(map[string]int64)
		}
		// Derive the high-water mark from the marker's own offset rather than
		// accumulating, so replaying an event (a client resuming one seq too
		// early) cannot inflate the count.
		st.LogBytes[b.Stream] = max(st.LogBytes[b.Stream], b.Offset+b.Len)

	case CacheHit:
		s.step(e.Step).Cached = true

	case BreakpointHit:
		// The payload names who armed it; the fold does not need that and does
		// not decode it. The one underivable bit: this step is held, and the
		// run is stopped rather than stuck. Cleared by StepStarted,
		// StepFinished, StepRetried and ControlApplied below.
		s.step(e.Step).Paused = true

	case ControlApplied:
		var b ControlAppliedBody
		if err := e.Decode(&b); err != nil {
			return err
		}
		// Clearing a breakpoint is the only signal that a held step is no
		// longer held: its next event (step.started) may be arbitrarily far
		// away, or never arrive if the run is cancelled first.
		//
		// Non-creating on purpose, unlike the step-scoped cases above: this
		// envelope is run-scoped, the id comes from a payload field, and
		// materialising a step from one would let a hand-edited or foreign
		// ledger conjure steps that were never in any plan.
		switch b.Op {
		case OpBreakpointClear:
			if st, ok := s.Steps[b.Args["step"]]; ok {
				st.Paused = false
			}
		case OpRunPause:
			// Unlike a breakpoint, no separate event announces the scheduler
			// acted: a pause takes effect the instant it is accepted, so this
			// event IS the stop. See RunInfo.Paused.
			s.Run.Paused = true
		case OpRunResume:
			s.Run.Paused = false
		}

	case HandlerStarted, HandlerSucceeded, HandlerFailed:
		var b HandlerBody
		if err := e.Decode(&b); err != nil {
			return err
		}
		h := s.handler(e.Step, b.Parent)
		if b.Kind != "" {
			h.Kind = b.Kind
		}
		switch e.Type {
		case HandlerStarted:
			h.Started = e.TS
		case HandlerSucceeded:
			h.State = StateSucceeded
			h.Finished = e.TS
		case HandlerFailed:
			// Same distinction a step gets: a panic is not a failure the
			// handler's author anticipated, and a reader deciding whether to
			// trust the cleanup needs to tell them apart.
			h.State = StateFailed
			if b.Panicked {
				h.State = StatePanicked
			}
			h.Finished = e.TS
			h.Error = b.Error
		}
	}

	// Attempt is a routing field on every step-scoped envelope, so a client
	// that joined mid-stream can recover it without decoding payloads. Seeded
	// after the switch via a non-creating lookup on purpose: a handled branch
	// has already created the step, while an unhandled or unrecognised type
	// must not materialise one.
	if e.Step != "" && e.Attempt > 0 {
		if st, ok := s.Steps[e.Step]; ok && e.Attempt > st.Attempt {
			st.Attempt = e.Attempt
		}
	}

	// Every other known type, and every unknown type, advances Seq and is
	// otherwise ignored. This is deliberate.
	return nil
}

// step returns the step's state, creating it if the stream never announced
// it: a truncated or mid-stream log must still fold cleanly.
//
// An empty id returns a fresh, unstored StepState rather than one keyed by
// "" in Steps/Order: a known type with an empty Step field (unreachable from
// senro's engine, reachable from a foreign events.jsonl) would otherwise
// materialise a phantom "" entry. The caller still mutates a real *StepState
// either way; the mutation just lands on a value nothing else reads.
func (s *RunState) step(id string) *StepState {
	if id == "" {
		return &StepState{}
	}
	if st, ok := s.Steps[id]; ok {
		return st
	}
	st := &StepState{ID: id}
	s.Steps[id] = st
	s.Order = append(s.Order, id)
	return st
}

// handler returns the handler's state, creating it (and linking it to its
// parent step) the first time the stream mentions it. The parent is
// materialised through s.step so a mid-stream join stays coherent. The
// handler itself never enters Steps or Order: it is not a plan node, and
// counting it as one would make every step count wrong.
//
// An empty id returns a fresh, unstored HandlerState, for the same reason as
// step's empty-id case.
func (s *RunState) handler(id, parent string) *HandlerState {
	if id == "" {
		return &HandlerState{Parent: parent}
	}
	if h, ok := s.Handlers[id]; ok {
		return h
	}
	h := &HandlerState{ID: id, Parent: parent}
	s.Handlers[id] = h
	if parent != "" {
		st := s.step(parent)
		st.Handlers = append(st.Handlers, id)
	}
	return h
}
