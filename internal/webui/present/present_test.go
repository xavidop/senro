package present_test

import (
	"strings"
	"testing"
	"time"

	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/internal/webui/present"
)

var now = time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)

// fold builds a state the only legitimate way: by folding events. Building
// a RunState by hand would let these tests assert against a shape the fold
// never produces.
func fold(t *testing.T, events ...api.Event) *api.RunState {
	t.Helper()
	st := api.NewRunState()
	for i, e := range events {
		e.V = api.Version
		e.Seq = uint64(i + 1)
		if e.Run == "" {
			e.Run = "r1"
		}
		if err := st.Apply(e); err != nil {
			t.Fatalf("Apply(%d): %v", i, err)
		}
	}
	return st
}

func payload(t *testing.T, s string) []byte {
	t.Helper()
	return []byte(s)
}

// A step held at a breakpoint must not read as a step that has not started.
// Nothing else in StepState distinguishes them: no state, no start, no
// error. A run stopped on purpose and a run that has hung look identical
// without this.
func TestAPausedStepIsNotPending(t *testing.T) {
	st := fold(t,
		api.Event{Type: api.StepCreated, Step: "deploy", Payload: payload(t, `{"kind":"shell"}`)},
		api.Event{Type: api.BreakpointHit, Step: "deploy"},
	)
	got := present.StepBadge(st.Steps["deploy"])
	if got.Label != "paused" {
		t.Fatalf("badge = %+v, want the paused label", got)
	}
	if got.Tone == present.ToneNeutral {
		t.Error("a held run is drawn in the same tone as one that has not started yet")
	}
}

// A retried step is pending again, not failed. The fold clears the previous
// attempt's state; a renderer that kept showing the failure would report a
// run as broken while it is busy recovering.
func TestARetriedStepIsNotStillFailed(t *testing.T) {
	ts := now
	st := fold(t,
		api.Event{Type: api.StepStarted, Step: "build", TS: ts},
		api.Event{Type: api.StepFinished, Step: "build", TS: ts.Add(time.Second),
			Payload: payload(t, `{"state":"failed","exit_code":2}`)},
		api.Event{Type: api.StepRetried, Step: "build", Payload: payload(t, `{"attempt":2}`)},
	)
	if got := present.StepBadge(st.Steps["build"]); got.Label != "pending" {
		t.Fatalf("badge = %+v, want pending after a retry", got)
	}
}

// Recovered is a success with a story, and the site's palette rule puts it
// on the same side as succeeded. Drawing it as a failure would make every
// run with flaky infrastructure look broken.
func TestRecoveredIsToldApartFromFailedAndFromPlainSuccess(t *testing.T) {
	if got := present.StateTone(api.StateRecovered); got != present.ToneOK {
		t.Errorf("StateTone(recovered) = %q, want ok", got)
	}
	if got := present.StateTone(api.StateFailed); got != present.ToneFailed {
		t.Errorf("StateTone(failed) = %q, want failed", got)
	}
	if got := present.StateTone(api.StateTimedOut); got != present.ToneFailed {
		t.Errorf("StateTone(timed_out) = %q, want failed: a timeout indicts the workload", got)
	}
	if got := present.StateTone(api.StateCancelled); got != present.ToneNeutral {
		t.Errorf("StateTone(cancelled) = %q, want neutral: the operator asked for it", got)
	}
	for _, s := range []api.State{api.StateSkippedManual, api.StateSkippedCondition, api.StateCached} {
		if got := present.StateTone(s); got != present.ToneNeutral {
			t.Errorf("StateTone(%s) = %q, want neutral", s, got)
		}
	}
}

// A state from a newer engine that this build has never heard of must draw
// as something rather than crash or be reported as a failure, matching the
// fold's own stance on unknown event types.
func TestAnUnknownStateDrawsNeutral(t *testing.T) {
	if got := present.StateTone(api.State("quantum_superposed")); got != present.ToneNeutral {
		t.Errorf("StateTone(unknown) = %q, want neutral", got)
	}
}

// The engine's own verdict is authoritative. A run that finished failed
// must not be redrawn as succeeded because the steps this client happened
// to see all passed.
func TestTheRunsOwnVerdictWins(t *testing.T) {
	st := fold(t,
		api.Event{Type: api.StepStarted, Step: "build"},
		api.Event{Type: api.StepFinished, Step: "build", Payload: payload(t, `{"state":"succeeded"}`)},
		api.Event{Type: api.RunFinished, Payload: payload(t, `{"status":"failed"}`)},
	)
	got := present.RunBadge(st)
	if got.Label != string(api.RunFailed) {
		t.Fatalf("run badge = %+v, want the engine's own failed verdict", got)
	}
	if got.Tone != present.ToneFailed {
		t.Errorf("tone = %q, want failed", got.Tone)
	}
}

// Steps are listed in the fold's own creation order, so the browser and the
// terminal lay the same run out the same way.
func TestRowsFollowTheFoldsOrderNotAnAlphabeticalOne(t *testing.T) {
	st := fold(t,
		api.Event{Type: api.StepCreated, Step: "zebra", Payload: payload(t, `{"kind":"shell"}`)},
		api.Event{Type: api.StepCreated, Step: "alpha", Payload: payload(t, `{"kind":"shell"}`)},
		api.Event{Type: api.StepCreated, Step: "middle", Payload: payload(t, `{"kind":"shell"}`)},
	)
	rows := present.Rows(st, now)
	want := []string{"zebra", "alpha", "middle"}
	if len(rows) != len(want) {
		t.Fatalf("got %d rows, want %d", len(rows), len(want))
	}
	for i, w := range want {
		if rows[i].ID != w {
			t.Errorf("row %d = %q, want %q: the list was reordered", i, rows[i].ID, w)
		}
	}
}

// A handler is not a step. The fold keeps handlers out of Steps precisely so
// no renderer counts them as plan nodes; this asserts the list and the
// counts follow that.
func TestHandlersAreNotStepsInTheListOrTheCounts(t *testing.T) {
	st := fold(t,
		api.Event{Type: api.StepCreated, Step: "deploy", Payload: payload(t, `{"kind":"shell"}`)},
		api.Event{Type: api.StepFinished, Step: "deploy", Payload: payload(t, `{"state":"failed"}`)},
		api.Event{Type: api.HandlerStarted, Step: "deploy/on_failure/collect",
			Payload: payload(t, `{"parent":"deploy","kind":"on_failure"}`)},
		api.Event{Type: api.HandlerSucceeded, Step: "deploy/on_failure/collect",
			Payload: payload(t, `{"parent":"deploy","kind":"on_failure"}`)},
	)
	rows := present.Rows(st, now)
	if len(rows) != 1 || rows[0].ID != "deploy" {
		t.Fatalf("rows = %+v, want just the step", rows)
	}
	for _, c := range present.Counts(st) {
		if c.Label == "steps" && c.N != 1 {
			t.Errorf("step count = %d, want 1: a handler was counted as a step", c.N)
		}
	}
	// It does belong in the step's own detail, which is where a reader
	// looking at a failure finds out whether the evidence was collected.
	var seen bool
	for _, f := range present.Detail(st, "deploy", now) {
		if strings.Contains(f.Value, "deploy/on_failure/collect") {
			seen = true
		}
	}
	if !seen {
		t.Error("the step's detail does not mention its handler")
	}
}

// Cleanup that never finished is a run-level fact with consequences outside
// the run: a lock may still be held. It belongs where somebody sees it.
func TestAbandonedCleanupIsSaidOutLoud(t *testing.T) {
	st := fold(t,
		api.Event{Type: api.RunStarted, Payload: payload(t, `{"pipeline":"demo"}`)},
		api.Event{Type: api.RunFinished, Payload: payload(t, `{"status":"succeeded","cleanup_abandoned":true}`)},
	)
	if got := present.Subtitle(st); !strings.Contains(got, "cleanup abandoned") {
		t.Fatalf("subtitle = %q, want it to report the abandoned cleanup", got)
	}
}

// A handler that started and never reported finishing is not the same as
// one that succeeded quietly, and the detail must not collapse them.
func TestAnUnfinishedHandlerIsDistinguishedFromASuccessfulOne(t *testing.T) {
	st := fold(t,
		api.Event{Type: api.StepFinished, Step: "deploy", Payload: payload(t, `{"state":"failed"}`)},
		api.Event{Type: api.HandlerStarted, Step: "deploy/always/unlock",
			Payload: payload(t, `{"parent":"deploy","kind":"always"}`)},
	)
	var found string
	for _, f := range present.Detail(st, "deploy", now) {
		if strings.Contains(f.Value, "deploy/always/unlock") {
			found = f.Value
		}
	}
	if found == "" {
		t.Fatal("the handler is missing from the detail")
	}
	if strings.Contains(found, "succeeded") {
		t.Fatalf("handler rendered as %q: a handler that never finished must not read as one that did", found)
	}
}

// A step that has not started has no duration. Rendering "0s" makes a plan
// full of pending work look like a plan full of instantaneous work.
func TestAStepThatNeverStartedHasNoDuration(t *testing.T) {
	if got := present.Elapsed(time.Time{}, time.Time{}, now); got != "" {
		t.Fatalf("Elapsed = %q, want empty", got)
	}
}

// A running step's duration is measured to the caller's clock, so it keeps
// ticking, which is what makes a live view live.
func TestARunningStepsDurationGrowsWithTheClock(t *testing.T) {
	started := now.Add(-90 * time.Second)
	first := present.Elapsed(started, time.Time{}, now)
	later := present.Elapsed(started, time.Time{}, now.Add(30*time.Second))
	if first == later {
		t.Fatalf("Elapsed was %q at both clocks: a running step is frozen", first)
	}
	if !strings.Contains(first, "m") {
		t.Errorf("Elapsed = %q, want minutes for a 90 second step", first)
	}
}

func TestDurationScalesWithMagnitude(t *testing.T) {
	for _, tc := range []struct {
		d    time.Duration
		want string
	}{
		{250 * time.Millisecond, "250ms"},
		{2500 * time.Millisecond, "2.5s"},
		{95 * time.Second, "1m35s"},
		{3*time.Hour + 4*time.Minute, "3h04m"},
	} {
		if got := present.Elapsed(now, now.Add(tc.d), now); got != tc.want {
			t.Errorf("Elapsed(%v) = %q, want %q", tc.d, got, tc.want)
		}
	}
}

// Clock skew between an engine and a browser can produce a finish before a
// start. Rendering a negative duration would be a claim about causality.
func TestANegativeDurationIsNotRendered(t *testing.T) {
	if got := present.Elapsed(now, now.Add(-time.Minute), now); got != "" {
		t.Fatalf("Elapsed = %q, want empty for a finish before its start", got)
	}
}

// Exit code zero is a real value, and showing it on every successful step is
// noise; showing it on a RUNNING step would be wrong, since the fold clears
// it when a step starts.
func TestExitCodeIsShownOnlyWhenItSaysSomething(t *testing.T) {
	ok := fold(t,
		api.Event{Type: api.StepStarted, Step: "build"},
		api.Event{Type: api.StepFinished, Step: "build", Payload: payload(t, `{"state":"succeeded","exit_code":0}`)},
	)
	for _, f := range present.Detail(ok, "build", now) {
		if f.Label == "exit" {
			t.Error("a successful step reports an exit code")
		}
	}
	bad := fold(t,
		api.Event{Type: api.StepStarted, Step: "build"},
		api.Event{Type: api.StepFinished, Step: "build", Payload: payload(t, `{"state":"failed","exit_code":137}`)},
	)
	var seen bool
	for _, f := range present.Detail(bad, "build", now) {
		if f.Label == "exit" && f.Value == "137" {
			seen = true
		}
	}
	if !seen {
		t.Error("a failed step does not report its exit code")
	}
}

// An expansion is summarised through the fold's own Group aggregation, so
// this page and the terminal report identical counts.
func TestAnExpansionIsSummarisedThroughTheFoldsOwnAggregation(t *testing.T) {
	st := fold(t,
		api.Event{Type: api.StepCreated, Step: "test", Payload: payload(t, `{"kind":"shell"}`)},
		api.Event{Type: api.PlanExpanded, Payload: payload(t,
			`{"parent":"test","children":["test[a]","test[b]"],"count":2}`)},
		api.Event{Type: api.StepFinished, Step: "test[a]", Payload: payload(t, `{"state":"succeeded"}`)},
		api.Event{Type: api.StepFinished, Step: "test[b]", Payload: payload(t, `{"state":"failed"}`)},
	)
	var summary string
	for _, f := range present.Detail(st, "test", now) {
		if f.Label == "expanded" {
			summary = f.Value
		}
	}
	if summary == "" {
		t.Fatal("no expansion summary")
	}
	if !strings.Contains(summary, "2 children") || !strings.Contains(summary, "1 failed") {
		t.Fatalf("summary = %q, want the fold's own counts", summary)
	}
	// The children are rows, indented, because they are steps.
	rows := present.Rows(st, now)
	var children int
	for _, r := range rows {
		if r.Child {
			children++
		}
	}
	if children != 2 {
		t.Errorf("%d rows marked as expansion children, want 2", children)
	}
}

// Log stream names come out of a map, and a pane that reshuffled itself
// between frames on map iteration order would be unreadable.
func TestLogStreamsAreListedInAStableOrder(t *testing.T) {
	st := fold(t,
		api.Event{Type: api.StepLogAppended, Step: "build",
			Payload: payload(t, `{"stream":"stdout","offset":0,"len":10}`)},
		api.Event{Type: api.StepLogAppended, Step: "build",
			Payload: payload(t, `{"stream":"stderr","offset":0,"len":5}`)},
	)
	var first []string
	for i := 0; i < 20; i++ {
		var got []string
		for _, f := range present.Detail(st, "build", now) {
			if f.Label == "stdout" || f.Label == "stderr" {
				got = append(got, f.Label)
			}
		}
		if first == nil {
			first = got
			continue
		}
		if strings.Join(got, ",") != strings.Join(first, ",") {
			t.Fatalf("stream order changed between renders: %v then %v", first, got)
		}
	}
	if len(first) != 2 || first[0] != "stderr" {
		t.Fatalf("streams = %v, want a stable sorted order", first)
	}
}

// A nil state is what the page holds before its first snapshot arrives, and
// every function here has to survive it rather than taking the tab down.
func TestNothingPanicsBeforeTheFirstSnapshot(t *testing.T) {
	if got := present.RunBadge(nil); got.Label == "" {
		t.Error("RunBadge(nil) has no label")
	}
	if got := present.Rows(nil, now); got != nil {
		t.Errorf("Rows(nil) = %v, want nil", got)
	}
	if got := present.Counts(nil); got != nil {
		t.Errorf("Counts(nil) = %v, want nil", got)
	}
	if got := present.Detail(nil, "x", now); got != nil {
		t.Errorf("Detail(nil) = %v, want nil", got)
	}
	if got := present.Subtitle(nil); got == "" {
		t.Error("Subtitle(nil) is empty")
	}
	if got := present.StepBadge(nil); got.Label == "" {
		t.Error("StepBadge(nil) has no label")
	}
}
