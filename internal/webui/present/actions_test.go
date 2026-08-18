package present_test

import (
	"slices"
	"testing"

	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/internal/webui/present"
)

// ops is the op names of a set of actions, for comparing against what a
// case expects without asserting on labels and tones that are allowed to be
// reworded.
func ops(as []present.Action) []string {
	out := make([]string, 0, len(as))
	for _, a := range as {
		out = append(out, a.Op)
	}
	return out
}

// A finished run offers nothing. This is the case that matters most: a
// finished run is what an operator most often has open, and every op the
// engine serves would be refused on one.
func TestAFinishedRunOffersNoControls(t *testing.T) {
	st := fold(t,
		api.Event{Type: api.RunStarted, Payload: payload(t, `{"pipeline":"p"}`)},
		api.Event{Type: api.StepCreated, Step: "build", Payload: payload(t, `{"kind":"shell"}`)},
		api.Event{Type: api.StepFinished, Step: "build", Payload: payload(t, `{"state":"failed"}`)},
		api.Event{Type: api.RunFinished, Payload: payload(t, `{"status":"failed"}`)},
	)
	if got := present.RunActions(st); len(got) != 0 {
		t.Errorf("RunActions on a finished run = %v, want none", ops(got))
	}
	// Including for a step that would otherwise be retryable: the step is
	// finished and failed, which on a LIVE run offers Retry.
	if got := present.StepActions(st, "build"); len(got) != 0 {
		t.Errorf("StepActions on a finished run = %v, want none", ops(got))
	}
}

// The pause button follows the fold, not this page's own memory of what it
// last sent: a pause applied from the TUI or the CLI has to turn this into
// a Resume without anybody telling the browser separately.
func TestAPausedRunOffersResumeRatherThanPause(t *testing.T) {
	live := fold(t,
		api.Event{Type: api.RunStarted, Payload: payload(t, `{"pipeline":"p"}`)},
	)
	if got, want := ops(present.RunActions(live)), []string{api.OpRunPause, api.OpRunCancel}; !slices.Equal(got, want) {
		t.Errorf("RunActions on a live run = %v, want %v", got, want)
	}

	paused := fold(t,
		api.Event{Type: api.RunStarted, Payload: payload(t, `{"pipeline":"p"}`)},
		api.Event{Type: api.ControlApplied, Payload: payload(t, `{"op":"run.pause","client_id":"c"}`)},
	)
	if got, want := ops(present.RunActions(paused)), []string{api.OpRunResume, api.OpRunCancel}; !slices.Equal(got, want) {
		t.Errorf("RunActions on a paused run = %v, want %v", got, want)
	}

	resumed := fold(t,
		api.Event{Type: api.RunStarted, Payload: payload(t, `{"pipeline":"p"}`)},
		api.Event{Type: api.ControlApplied, Payload: payload(t, `{"op":"run.pause","client_id":"c"}`)},
		api.Event{Type: api.ControlApplied, Payload: payload(t, `{"op":"run.resume","client_id":"c"}`)},
	)
	if got, want := ops(present.RunActions(resumed)), []string{api.OpRunPause, api.OpRunCancel}; !slices.Equal(got, want) {
		t.Errorf("RunActions after a resume = %v, want %v", got, want)
	}
}

// A step held at a breakpoint gets the release and nothing else. It has not
// run, so there is nothing to retry.
func TestAHeldStepOffersOnlyTheRelease(t *testing.T) {
	st := fold(t,
		api.Event{Type: api.RunStarted, Payload: payload(t, `{"pipeline":"p"}`)},
		api.Event{Type: api.StepCreated, Step: "deploy", Payload: payload(t, `{"kind":"shell"}`)},
		api.Event{Type: api.BreakpointHit, Step: "deploy"},
	)
	if got, want := ops(present.StepActions(st, "deploy")), []string{api.OpBreakpointClear}; !slices.Equal(got, want) {
		t.Errorf("StepActions on a held step = %v, want %v", got, want)
	}
}

// A running step offers nothing, because every op that could apply to it is
// refused while an attempt is in flight, and senro has no "stop this one
// step": cancel is run-wide.
func TestARunningStepOffersNothing(t *testing.T) {
	st := fold(t,
		api.Event{Type: api.RunStarted, Payload: payload(t, `{"pipeline":"p"}`)},
		api.Event{Type: api.StepCreated, Step: "build", Payload: payload(t, `{"kind":"shell"}`)},
		api.Event{Type: api.StepStarted, Step: "build", Attempt: 1, TS: now},
	)
	if got := present.StepActions(st, "build"); len(got) != 0 {
		t.Errorf("StepActions on a running step = %v, want none", ops(got))
	}
}

// A finished step is the one an operator wants to act on, and rerun_from is
// offered only here: the engine refuses it outright for a step that has not
// run at all.
func TestAFinishedStepOffersRetryAndRerun(t *testing.T) {
	for _, state := range []string{"failed", "succeeded", "timed_out", "panicked"} {
		t.Run(state, func(t *testing.T) {
			st := fold(t,
				api.Event{Type: api.RunStarted, Payload: payload(t, `{"pipeline":"p"}`)},
				api.Event{Type: api.StepCreated, Step: "build", Payload: payload(t, `{"kind":"shell"}`)},
				api.Event{Type: api.StepStarted, Step: "build", Attempt: 1, TS: now},
				api.Event{Type: api.StepFinished, Step: "build", Payload: payload(t, `{"state":"`+state+`"}`)},
			)
			want := []string{api.OpStepRetry, api.OpRunRerunFrom}
			if got := ops(present.StepActions(st, "build")); !slices.Equal(got, want) {
				t.Errorf("StepActions on a %s step = %v, want %v", state, got, want)
			}
		})
	}
}

// A step that has not started yet is the only one where skipping and arming
// a breakpoint are meaningful.
func TestAPendingStepOffersSkipAndBreakpoint(t *testing.T) {
	st := fold(t,
		api.Event{Type: api.RunStarted, Payload: payload(t, `{"pipeline":"p"}`)},
		api.Event{Type: api.StepCreated, Step: "deploy", Payload: payload(t, `{"kind":"shell","needs":["build"]}`)},
	)
	want := []string{api.OpBreakpointSet, api.OpStepSkip}
	if got := ops(present.StepActions(st, "deploy")); !slices.Equal(got, want) {
		t.Errorf("StepActions on a pending step = %v, want %v", got, want)
	}
}

// A retried step is pending again, and the fold clears its terminal state,
// so the offer follows it back rather than staying on Retry.
func TestARetriedStepIsOfferedAsPendingAgain(t *testing.T) {
	st := fold(t,
		api.Event{Type: api.RunStarted, Payload: payload(t, `{"pipeline":"p"}`)},
		api.Event{Type: api.StepCreated, Step: "build", Payload: payload(t, `{"kind":"shell"}`)},
		api.Event{Type: api.StepStarted, Step: "build", Attempt: 1, TS: now},
		api.Event{Type: api.StepFinished, Step: "build", Payload: payload(t, `{"state":"failed"}`)},
		api.Event{Type: api.StepRetried, Step: "build", Payload: payload(t, `{"attempt":2}`)},
	)
	want := []string{api.OpBreakpointSet, api.OpStepSkip}
	if got := ops(present.StepActions(st, "build")); !slices.Equal(got, want) {
		t.Errorf("StepActions after a retry = %v, want %v", got, want)
	}
}

// Nothing panics, and nothing is offered, before there is a run or for a
// step the fold has never heard of. Every other function in this package is
// held to the same rule.
func TestNoActionsBeforeThereIsAnythingToActOn(t *testing.T) {
	if got := present.RunActions(nil); got != nil {
		t.Errorf("RunActions(nil) = %v, want nil", ops(got))
	}
	if got := present.StepActions(nil, "x"); got != nil {
		t.Errorf("StepActions(nil) = %v, want nil", ops(got))
	}
	empty := api.NewRunState()
	if got := present.RunActions(empty); len(got) != 0 {
		t.Errorf("RunActions on an empty state = %v, want none", ops(got))
	}
	st := fold(t, api.Event{Type: api.RunStarted, Payload: payload(t, `{"pipeline":"p"}`)})
	if got := present.StepActions(st, "never-heard-of-it"); len(got) != 0 {
		t.Errorf("StepActions for an unknown step = %v, want none", ops(got))
	}
	if got := present.StepActions(st, ""); len(got) != 0 {
		t.Errorf("StepActions for no selection = %v, want none", ops(got))
	}
}

// Every op this package offers is one api actually declares. A typo in an
// op string would otherwise reach the engine as an unknown operation and be
// refused, with the button looking correct the whole time.
func TestEveryOfferedOpIsRealAndForwardable(t *testing.T) {
	declared := api.DeclaredOps()

	// Every state a step can be offered actions in, so this covers the
	// whole table rather than whichever branch happened to be taken.
	states := []*api.RunState{
		fold(t,
			api.Event{Type: api.RunStarted, Payload: payload(t, `{"pipeline":"p"}`)},
			api.Event{Type: api.StepCreated, Step: "s", Payload: payload(t, `{"kind":"shell"}`)}),
		fold(t,
			api.Event{Type: api.RunStarted, Payload: payload(t, `{"pipeline":"p"}`)},
			api.Event{Type: api.StepCreated, Step: "s", Payload: payload(t, `{"kind":"shell"}`)},
			api.Event{Type: api.BreakpointHit, Step: "s"}),
		fold(t,
			api.Event{Type: api.RunStarted, Payload: payload(t, `{"pipeline":"p"}`)},
			api.Event{Type: api.StepCreated, Step: "s", Payload: payload(t, `{"kind":"shell"}`)},
			api.Event{Type: api.StepStarted, Step: "s", Attempt: 1, TS: now},
			api.Event{Type: api.StepFinished, Step: "s", Payload: payload(t, `{"state":"failed"}`)}),
		fold(t,
			api.Event{Type: api.RunStarted, Payload: payload(t, `{"pipeline":"p"}`)},
			api.Event{Type: api.ControlApplied, Payload: payload(t, `{"op":"run.pause","client_id":"c"}`)}),
	}

	seen := map[string]bool{}
	for _, st := range states {
		for _, a := range append(present.RunActions(st), present.StepActions(st, "s")...) {
			seen[a.Op] = true
			if !slices.Contains(declared, a.Op) {
				t.Errorf("action %q offers op %q, which api does not declare", a.Label, a.Op)
			}
			if a.Op == api.OpRunCancel || a.Op == api.OpRunPause || a.Op == api.OpRunResume {
				if a.Step != "" {
					t.Errorf("run-scoped op %q carries step %q", a.Op, a.Step)
				}
			} else if a.Step == "" {
				t.Errorf("step-scoped op %q carries no step", a.Op)
			}
			if a.Label == "" {
				t.Errorf("op %q has no label", a.Op)
			}
		}
	}
	if len(seen) == 0 {
		t.Fatal("no actions were offered in any state: this test is asserting nothing")
	}
}
