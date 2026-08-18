package api

// State is a step's terminal state. Downstream behaviour (the UI, exit codes,
// notifications, the analyzer) depends on this being specific rather than a
// boolean.
type State string

const (
	StateSucceeded State = "succeeded"
	StateCached    State = "cached"
	StateFailed    State = "failed"
	StateTimedOut  State = "timed_out"
	StateCancelled State = "cancelled"

	StateSkippedUpstreamFailed State = "skipped_upstream_failed"

	// StateSkippedManual is a step an operator deliberately took out of the
	// run with the step.skip control operation (OpStepSkip).
	//
	// A step that needs a manually skipped one inherits this same state,
	// transitively, rather than StateSkippedUpstreamFailed: nothing failed,
	// so nothing is to blame. ContinueOnError does not rescue such a
	// dependent: it promises survival of a FAILURE, not a run against output
	// that was never produced.
	//
	// RollUp treats this as clean, not RunPartial: a run whose operator
	// skipped a step and whose every other step passed reports succeeded.
	StateSkippedManual State = "skipped_manual"

	StateSkippedCondition State = "skipped_condition"

	// StateRecovered is a step that failed at least once and passed on retry.
	// Distinct from StateSucceeded on purpose: a run full of recovered steps is
	// a run with flaky infrastructure, and collapsing the two hides that.
	StateRecovered State = "recovered"

	StatePanicked State = "panicked"
)

var terminalStates = map[State]bool{
	StateSucceeded: true, StateCached: true, StateFailed: true,
	StateTimedOut: true, StateCancelled: true,
	StateSkippedUpstreamFailed: true, StateSkippedManual: true,
	StateSkippedCondition: true, StateRecovered: true, StatePanicked: true,
}

// Terminal reports whether s is one of the defined terminal states.
func (s State) Terminal() bool { return terminalStates[s] }

// Failed reports whether the step ended in a way that indicts the workload or
// its environment. Cancellation is deliberately not a failure (the operator
// asked for it) and skips are not failures either.
func (s State) Failed() bool {
	switch s {
	case StateFailed, StateTimedOut, StatePanicked:
		return true
	}
	return false
}

// RunStatus is a run's rolled-up outcome.
type RunStatus string

const (
	RunSucceeded             RunStatus = "succeeded"
	RunSucceededWithRecovery RunStatus = "succeeded_with_recovery"
	RunPartial               RunStatus = "partial"
	RunFailed                RunStatus = "failed"
	RunCancelled             RunStatus = "cancelled"
)

// RollUp reduces step states to a run status.
//
// Precedence, strongest first: cancelled, failed, partial, recovered, clean.
// Cancellation outranks failure because a step that failed while the run was
// being torn down says nothing useful about the workload.
func RollUp(states []State) RunStatus {
	var cancelled, failed, skippedUpstream, recovered bool
	for _, s := range states {
		switch {
		case s == StateCancelled:
			cancelled = true
		case s.Failed():
			failed = true
		case s == StateSkippedUpstreamFailed:
			skippedUpstream = true
		case s == StateRecovered:
			recovered = true
		}
	}
	switch {
	case cancelled:
		return RunCancelled
	case failed:
		return RunFailed
	case skippedUpstream:
		return RunPartial
	case recovered:
		return RunSucceededWithRecovery
	default:
		return RunSucceeded
	}
}
