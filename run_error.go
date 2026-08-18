package senro

import (
	"fmt"
	"strings"
	"sync"

	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/internal/sink"
)

// maxRunErrorSteps bounds how many step ids RunError.Error names before
// "and N more": every step already has its own step.finished in
// events.jsonl, and Error's job is to make the first look worth taking.
const maxRunErrorSteps = 3

// RunErrorStep is one step behind a RunError's Status. See RunError.Steps
// for which State qualifies a step for this list.
type RunErrorStep struct {
	// ID is the step's id, exactly as declared with (*WorkflowBuilder).Step.
	ID string
	// State is the step's own terminal state, see api.State.
	State api.State
	// ExitCode is the step's process exit code. Shown by String only when
	// State is StateFailed and the code is nonzero: a step that never ran a
	// process reports exit 0, and "(exit 0)" next to "failed" would read as
	// a passing process on a failing run.
	ExitCode int
}

// String renders one step the way RunError.Error embeds it:
// `"id" state[ (exit N)]`.
func (s RunErrorStep) String() string {
	label := fmt.Sprintf("%q %s", s.ID, stepStateVerb(s.State))
	if s.State == api.StateFailed && s.ExitCode != 0 {
		label += fmt.Sprintf(" (exit %d)", s.ExitCode)
	}
	return label
}

// stepStateVerb is the human phrase for one step's terminal State, as it
// reads inside RunError's own message: `step "test" <verb>`.
func stepStateVerb(s api.State) string {
	switch s {
	case api.StateFailed:
		return "failed"
	case api.StateTimedOut:
		return "timed out"
	case api.StatePanicked:
		return "panicked"
	case api.StateCancelled:
		return "cancelled"
	case api.StateSkippedUpstreamFailed:
		return "skipped (upstream failed)"
	default:
		// Defensive only: runErrorStepFilter never selects a State outside
		// the five above. A future State still renders as something legible.
		return string(s)
	}
}

// runErrorStepFilter reports which step State qualifies a step for
// RunError.Steps, given the run's rolled-up Status. Mirrors api.RollUp's
// precedence: RunCancelled names only StateCancelled steps, RunPartial only
// StateSkippedUpstreamFailed (RollUp only returns partial when nothing
// actually failed).
//
// nil means "no State ever qualifies": the fallback for a success Status
// (which never becomes a RunError) or a future one. Total rather than
// panicking.
func runErrorStepFilter(status api.RunStatus) func(api.State) bool {
	switch status {
	case api.RunFailed:
		return func(s api.State) bool { return s.Failed() }
	case api.RunCancelled:
		return func(s api.State) bool { return s == api.StateCancelled }
	case api.RunPartial:
		return func(s api.State) bool { return s == api.StateSkippedUpstreamFailed }
	default:
		return nil
	}
}

// newRunError builds a RunError from a run's Status, the directory Run wrote
// to, and the *api.RunState folded from the events the engine emitted (see
// foldingSink); it never reads events.jsonl back off disk.
//
// It walks st.Order (creation order, the plan's node order) rather than
// ranging over st.Steps, so which steps land in Steps and which fold into
// StepsOmitted is deterministic rather than map-ordered.
func newRunError(status api.RunStatus, dir string, st *api.RunState) *RunError {
	e := &RunError{Status: status, Dir: dir}
	qualifies := runErrorStepFilter(status)
	if qualifies == nil || st == nil {
		return e
	}
	for _, id := range st.Order {
		step := st.Steps[id]
		if step == nil || !qualifies(step.State) {
			continue
		}
		if len(e.Steps) < maxRunErrorSteps {
			e.Steps = append(e.Steps, RunErrorStep{ID: step.ID, State: step.State, ExitCode: step.ExitCode})
			continue
		}
		e.StepsOmitted++
	}
	return e
}

// stepsClause renders RunError.Steps and StepsOmitted as Error's middle
// clause: `step "test" failed (exit 1)`, or `steps "a" ..., "b" ..., and N
// more`. Error skips the clause entirely when e.Steps is empty.
func (e *RunError) stepsClause() string {
	noun := "step"
	if len(e.Steps)+e.StepsOmitted > 1 {
		noun = "steps"
	}
	names := make([]string, len(e.Steps))
	for i, s := range e.Steps {
		names[i] = s.String()
	}
	clause := noun + " " + strings.Join(names, ", ")
	if e.StepsOmitted > 0 {
		clause += fmt.Sprintf(", and %d more", e.StepsOmitted)
	}
	return clause
}

// foldingSink tees every event to the caller's own sink while folding it
// into a private *api.RunState via api.RunState.Apply, the identical fold
// every other reader of the stream uses. Run reads that fold when the engine
// returns to build a RunError (see newRunError), instead of re-opening
// events.jsonl.
//
// Emit runs synchronously and starts no goroutine of its own (sink.Multi
// would, breaking TestRunWithNoOptionsStartsNoGoroutines). Reading state
// back out without further locking is safe only once engine.Run returns:
// runCore seals the stream before emitting run.finished, so no further Emit
// can race that read.
type foldingSink struct {
	next  sink.Sink
	mu    sync.Mutex
	state *api.RunState
}

func newFoldingSink(next sink.Sink) *foldingSink {
	return &foldingSink{next: next, state: api.NewRunState()}
}

func (f *foldingSink) Emit(e api.Event) {
	f.next.Emit(e)
	f.mu.Lock()
	// Apply's one error case is a regressing Seq, which a well-behaved
	// engine never produces. Ignored because Emit's contract is "never
	// fails"; the worst case is an incomplete RunError.Steps, not a wrong
	// one, and e still reaches f.next either way.
	_ = f.state.Apply(e)
	f.mu.Unlock()
}

func (f *foldingSink) Control() <-chan sink.ControlRequest { return f.next.Control() }

// Shells passes the engine's interactive-session channel straight through,
// making this wrapper a sink.ShellHost exactly when what it wraps is one.
// This is the ONE wrapper standing between every observer and the engine, so
// an optional capability it forgets to forward works in every unit test and
// silently does nothing in a real run; see
// TestAShellOnALocalStepReadsWhatTheStepWouldHaveSeen, which caught exactly
// that.
func (f *foldingSink) Shells() <-chan sink.ShellRequest {
	if h, ok := f.next.(sink.ShellHost); ok {
		return h.Shells()
	}
	return nil
}

// SetAppender passes the engine's ledger appender straight through: the fold
// has no events of its own to record, but the sinks behind it might (see
// senro.Reporter), and forgetting to forward here would silently break
// WithSink's reporting path in a real run.
func (f *foldingSink) SetAppender(a sink.Appender) {
	if r, ok := f.next.(sink.Reporter); ok {
		r.SetAppender(a)
	}
}
