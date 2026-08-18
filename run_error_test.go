package senro

// White-box unit tests of RunError's construction (newRunError,
// RunErrorStep.String, stepStateVerb): package senro, a deliberate exception
// to run_test.go's "public API only" rule, because two cases below have no
// path through a live engine.Run at all. RunPartial in particular: the
// scheduler never settles a node skipped_upstream_failed without its
// upstream also failing or being cancelled in the same run, and api.RollUp
// ranks both above partial, so no live run ever rolls up to RunPartial;
// these tests hand-construct the fold instead. See run_test.go for the
// black-box counterparts for every case that IS reachable through senro.Run.

import (
	"testing"

	"github.com/xavidop/senro/api"
)

// stepState is a small constructor to keep each test's fixture readable:
// the id, the terminal State that matters for RunError, and the exit code.
func stepState(id string, state api.State, exitCode int) *api.StepState {
	return &api.StepState{ID: id, State: state, ExitCode: exitCode}
}

// runState builds an *api.RunState the way api.RunState.Apply would have
// left it, without going through Apply: these tests care about newRunError's
// own logic, not the fold that would normally feed it.
func runState(steps ...*api.StepState) *api.RunState {
	st := api.NewRunState()
	for _, s := range steps {
		st.Steps[s.ID] = s
		st.Order = append(st.Order, s.ID)
	}
	return st
}

// TestNewRunErrorFailedNamesTheFailedStep is the primary case: one failed
// step, its exit code, and the run directory's events.jsonl.
func TestNewRunErrorFailedNamesTheFailedStep(t *testing.T) {
	st := runState(
		stepState("test", api.StateFailed, 1),
		stepState("build", api.StateSkippedUpstreamFailed, 0),
	)
	e := newRunError(api.RunFailed, "runs/xyz", st)

	if e.Status != api.RunFailed {
		t.Errorf("Status = %q, want %q", e.Status, api.RunFailed)
	}
	if e.Dir != "runs/xyz" {
		t.Errorf("Dir = %q, want %q", e.Dir, "runs/xyz")
	}
	want := []RunErrorStep{{ID: "test", State: api.StateFailed, ExitCode: 1}}
	if len(e.Steps) != len(want) || e.Steps[0] != want[0] {
		t.Fatalf("Steps = %+v, want %+v (build is skipped_upstream_failed, not itself failed)", e.Steps, want)
	}
	if e.StepsOmitted != 0 {
		t.Errorf("StepsOmitted = %d, want 0", e.StepsOmitted)
	}

	wantMsg := `senro: run failed: step "test" failed (exit 1); see runs/xyz/events.jsonl`
	if got := e.Error(); got != wantMsg {
		t.Errorf("Error() = %q, want %q", got, wantMsg)
	}
}

// TestNewRunErrorFailedDegradesGracefullyPastTheCap proves the "name the
// first few and say how many more" requirement precisely: exactly which
// steps are shown, in creation order, and an exact omitted count, not
// just "the string contains more somewhere".
func TestNewRunErrorFailedDegradesGracefullyPastTheCap(t *testing.T) {
	st := runState(
		stepState("s0", api.StateFailed, 1),
		stepState("s1", api.StateFailed, 1),
		stepState("s2", api.StateFailed, 1),
		stepState("s3", api.StateFailed, 1),
		stepState("s4", api.StateFailed, 1),
	)
	e := newRunError(api.RunFailed, "runs/xyz", st)

	if len(e.Steps) != 3 {
		t.Fatalf("len(Steps) = %d, want 3", len(e.Steps))
	}
	for i, id := range []string{"s0", "s1", "s2"} {
		if e.Steps[i].ID != id {
			t.Errorf("Steps[%d].ID = %q, want %q (creation order)", i, e.Steps[i].ID, id)
		}
	}
	if e.StepsOmitted != 2 {
		t.Fatalf("StepsOmitted = %d, want 2", e.StepsOmitted)
	}

	wantMsg := `senro: run failed: steps "s0" failed (exit 1), "s1" failed (exit 1), "s2" failed (exit 1), and 2 more; see runs/xyz/events.jsonl`
	if got := e.Error(); got != wantMsg {
		t.Errorf("Error() = %q, want %q", got, wantMsg)
	}
}

// TestNewRunErrorCancelledIgnoresStepsThatFailedOnTheirOwn: a step that
// failed before the cancel reached it is a real fact, but a RunCancelled
// message must not call it a "failed step". Cancellation outranks failure in
// api.RollUp, and newRunError must honour the same precedence when picking
// which steps to name.
func TestNewRunErrorCancelledIgnoresStepsThatFailedOnTheirOwn(t *testing.T) {
	st := runState(
		stepState("flaky", api.StateFailed, 1),
		stepState("deploy", api.StateCancelled, 0),
		stepState("cleanup", api.StateSucceeded, 0),
	)
	e := newRunError(api.RunCancelled, "runs/xyz", st)

	want := []RunErrorStep{{ID: "deploy", State: api.StateCancelled}}
	if len(e.Steps) != len(want) || e.Steps[0] != want[0] {
		t.Fatalf("Steps = %+v, want %+v", e.Steps, want)
	}

	wantMsg := `senro: run cancelled: step "deploy" cancelled; see runs/xyz/events.jsonl`
	if got := e.Error(); got != wantMsg {
		t.Errorf("Error() = %q, want %q", got, wantMsg)
	}
}

// TestNewRunErrorPartialNamesTheSkippedStepNotAnyFailure is "a run that
// failed with no step-level failure at all": see this file's own package
// doc for why RunPartial can only be reached this way, by construction,
// rather than through a live Run.
func TestNewRunErrorPartialNamesTheSkippedStepNotAnyFailure(t *testing.T) {
	st := runState(
		stepState("infra", api.StateSucceeded, 0),
		stepState("deploy", api.StateSkippedUpstreamFailed, 0),
	)
	e := newRunError(api.RunPartial, "runs/xyz", st)

	want := []RunErrorStep{{ID: "deploy", State: api.StateSkippedUpstreamFailed}}
	if len(e.Steps) != len(want) || e.Steps[0] != want[0] {
		t.Fatalf("Steps = %+v, want %+v", e.Steps, want)
	}

	wantMsg := `senro: run partial: step "deploy" skipped (upstream failed); see runs/xyz/events.jsonl`
	if got := e.Error(); got != wantMsg {
		t.Errorf("Error() = %q, want %q", got, wantMsg)
	}
}

// TestNewRunErrorFallsBackWhenStatusHasNoMatchingStep: api.RollUp never
// returns RunPartial without a StateSkippedUpstreamFailed step behind it,
// but a fold that disagrees must still produce a sane one-line message
// rather than an empty clause or a panic.
func TestNewRunErrorFallsBackWhenStatusHasNoMatchingStep(t *testing.T) {
	st := runState(stepState("infra", api.StateSucceeded, 0))
	e := newRunError(api.RunPartial, "runs/xyz", st)

	if len(e.Steps) != 0 {
		t.Errorf("Steps = %+v, want none", e.Steps)
	}
	if e.StepsOmitted != 0 {
		t.Errorf("StepsOmitted = %d, want 0", e.StepsOmitted)
	}

	wantMsg := `senro: run partial; see runs/xyz/events.jsonl`
	if got := e.Error(); got != wantMsg {
		t.Errorf("Error() = %q, want %q", got, wantMsg)
	}
}

// TestNewRunErrorUnknownStatusNamesNoSteps guards forward compatibility: a
// RunStatus this build has never seen must not crash newRunError, and must
// not guess at which steps are to blame.
func TestNewRunErrorUnknownStatusNamesNoSteps(t *testing.T) {
	st := runState(stepState("test", api.StateFailed, 1))
	e := newRunError(api.RunStatus("something_future"), "runs/xyz", st)

	if len(e.Steps) != 0 {
		t.Errorf("Steps = %+v, want none for a status newRunError has no filter for", e.Steps)
	}

	wantMsg := `senro: run something_future; see runs/xyz/events.jsonl`
	if got := e.Error(); got != wantMsg {
		t.Errorf("Error() = %q, want %q", got, wantMsg)
	}
}

// TestRunErrorStepStringPerState pins the exact phrase for every terminal
// State newRunError can select, including the exit-code gate: shown only for
// StateFailed with a nonzero code, since "(exit 0)" next to "failed" would
// read backwards.
func TestRunErrorStepStringPerState(t *testing.T) {
	cases := []struct {
		step RunErrorStep
		want string
	}{
		{RunErrorStep{ID: "test", State: api.StateFailed, ExitCode: 1}, `"test" failed (exit 1)`},
		{RunErrorStep{ID: "test", State: api.StateFailed, ExitCode: 0}, `"test" failed`},
		{RunErrorStep{ID: "test", State: api.StateTimedOut, ExitCode: -1}, `"test" timed out`},
		{RunErrorStep{ID: "test", State: api.StatePanicked}, `"test" panicked`},
		{RunErrorStep{ID: "deploy", State: api.StateCancelled}, `"deploy" cancelled`},
		{RunErrorStep{ID: "build", State: api.StateSkippedUpstreamFailed}, `"build" skipped (upstream failed)`},
	}
	for _, tc := range cases {
		if got := tc.step.String(); got != tc.want {
			t.Errorf("%+v.String() = %q, want %q", tc.step, got, tc.want)
		}
	}
}

// TestRunErrorWithoutDirOmitsSeeClause covers a RunError a caller builds by
// hand (Run itself always sets Dir, but the type is exported and its zero
// value must still render sensibly).
func TestRunErrorWithoutDirOmitsSeeClause(t *testing.T) {
	e := &RunError{Status: api.RunFailed}
	want := "senro: run failed"
	if got := e.Error(); got != want {
		t.Errorf("Error() = %q, want %q", got, want)
	}
}
