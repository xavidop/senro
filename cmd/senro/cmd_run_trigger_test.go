package main

import (
	"strings"
	"testing"
	"time"
)

// --- parseRunArgs: --trigger-event is forwarded to the pipeline, never
// acted on here. ---

func TestParseRunArgsForwardsTriggerEventToThePipeline(t *testing.T) {
	pkg, _, pipelineArgs, err := parseRunArgs([]string{"./ci", "--trigger-event", "ev.json"})
	if err != nil {
		t.Fatalf("parseRunArgs: %v", err)
	}
	if pkg != "./ci" {
		t.Errorf("pkg = %q", pkg)
	}
	want := []string{"--trigger-event=ev.json"}
	if len(pipelineArgs) != 1 || pipelineArgs[0] != want[0] {
		t.Fatalf("pipelineArgs = %v, want %v", pipelineArgs, want)
	}
}

func TestParseRunArgsAcceptsTriggerEventWithAnEquals(t *testing.T) {
	_, _, pipelineArgs, err := parseRunArgs([]string{"--trigger-event=ev.json", "./ci"})
	if err != nil {
		t.Fatalf("parseRunArgs: %v", err)
	}
	if len(pipelineArgs) != 1 || pipelineArgs[0] != "--trigger-event=ev.json" {
		t.Fatalf("pipelineArgs = %v", pipelineArgs)
	}
}

// TestParseRunArgsPassesTheTriggerEventPathThroughVerbatim: the pipeline
// runs in this process's working directory, so a relative path still
// resolves and rewriting one would only break what the operator typed.
func TestParseRunArgsPassesTheTriggerEventPathThroughVerbatim(t *testing.T) {
	for _, p := range []string{"-", "./a/b.json", "/abs/ev.json", "../up.json"} {
		_, _, pipelineArgs, err := parseRunArgs([]string{"./ci", "--trigger-event", p})
		if err != nil {
			t.Fatalf("parseRunArgs(%q): %v", p, err)
		}
		if got := pipelineArgs[0]; got != "--trigger-event="+p {
			t.Errorf("forwarded %q, want --trigger-event=%s", got, p)
		}
	}
}

// TestParseRunArgsKeepsTriggerEventAfterExplicitPipelineArgs, so a pipeline
// reading positional arguments still sees them in the order it was given
// them.
func TestParseRunArgsKeepsTriggerEventAfterExplicitPipelineArgs(t *testing.T) {
	_, _, pipelineArgs, err := parseRunArgs([]string{
		"./ci", "--trigger-event", "ev.json", "--", "foo", "bar",
	})
	if err != nil {
		t.Fatalf("parseRunArgs: %v", err)
	}
	want := []string{"foo", "bar", "--trigger-event=ev.json"}
	if len(pipelineArgs) != len(want) {
		t.Fatalf("pipelineArgs = %v, want %v", pipelineArgs, want)
	}
	for i := range want {
		if pipelineArgs[i] != want[i] {
			t.Fatalf("pipelineArgs = %v, want %v", pipelineArgs, want)
		}
	}
}

// TestParseRunArgsWithoutTriggerEventForwardsNothing: every existing
// invocation is untouched.
func TestParseRunArgsWithoutTriggerEventForwardsNothing(t *testing.T) {
	_, _, pipelineArgs, err := parseRunArgs([]string{"./ci", "--ui=plain"})
	if err != nil {
		t.Fatalf("parseRunArgs: %v", err)
	}
	if len(pipelineArgs) != 0 {
		t.Fatalf("pipelineArgs = %v, want none", pipelineArgs)
	}
}

// TestParseRunArgsTriggerEventNeedsAValue: an empty one is a typo in a
// dispatcher's template, and leaving the flag off is how you ask for no
// event.
func TestParseRunArgsTriggerEventNeedsAValue(t *testing.T) {
	for _, args := range [][]string{
		{"./ci", "--trigger-event"},
		{"./ci", "--trigger-event="},
	} {
		if _, _, _, err := parseRunArgs(args); err == nil {
			t.Errorf("parseRunArgs(%v) was accepted", args)
		}
	}
}

// TestParseRunArgsDoesNotClaimTriggerEventAfterADoubleDash. Everything after
// "--" belongs to the pipeline verbatim, including a flag that happens to
// share this one's name.
func TestParseRunArgsDoesNotClaimTriggerEventAfterADoubleDash(t *testing.T) {
	_, _, pipelineArgs, err := parseRunArgs([]string{"./ci", "--", "--trigger-event", "theirs.json"})
	if err != nil {
		t.Fatalf("parseRunArgs: %v", err)
	}
	want := []string{"--trigger-event", "theirs.json"}
	if len(pipelineArgs) != len(want) || pipelineArgs[0] != want[0] || pipelineArgs[1] != want[1] {
		t.Fatalf("pipelineArgs = %v, want %v untouched", pipelineArgs, want)
	}
}

// --- the exit-code contract ---

// TestExitNoTriggerMatchIs78 pins the constant that used to be a comment
// saying no code path may produce it. It is EX_CONFIG, and it is now a
// public part of the CLI's contract.
func TestExitNoTriggerMatchIs78(t *testing.T) {
	if exitNoTriggerMatch != 78 {
		t.Errorf("exitNoTriggerMatch = %d, want 78", exitNoTriggerMatch)
	}
	for _, other := range []int{exitSuccess, exitRunFailed, exitUsage, exitCancelled} {
		if other == exitNoTriggerMatch {
			t.Errorf("%d collides with the no-trigger-match code", other)
		}
	}
}

// TestReportNoTriggerMatchExplainsItselfAndChangesNothing: the exit code is
// correct without the message, which adds only the difference between
// "nothing to do" and "this crashed", identical at a terminal.
func TestReportNoTriggerMatchExplainsItselfAndChangesNothing(t *testing.T) {
	var stderr strings.Builder
	if got := reportNoTriggerMatch(exitNoTriggerMatch, &stderr); got != exitNoTriggerMatch {
		t.Errorf("code = %d, want %d unchanged", got, exitNoTriggerMatch)
	}
	if !strings.Contains(stderr.String(), "no trigger matched") {
		t.Errorf("stderr = %q, want it to say what happened", stderr.String())
	}

	for _, code := range []int{exitSuccess, exitRunFailed, exitUsage, exitCancelled, 42} {
		var quiet strings.Builder
		if got := reportNoTriggerMatch(code, &quiet); got != code {
			t.Errorf("code %d became %d", code, got)
		}
		if quiet.String() != "" {
			t.Errorf("code %d printed %q", code, quiet.String())
		}
	}
}

// --- end to end, through a real pipeline binary ---

// TestCmdRunForwardsTheTriggerEventAndPropagates78 is the feature seen from
// a script: the pipeline's own decision comes back as the exit code, and
// the CLI takes none of its own.
func TestCmdRunForwardsTheTriggerEventAndPropagates78(t *testing.T) {
	isolateRegistry(t)
	fastRegistrationPoll(t, 10*time.Millisecond)

	var stdout, stderr strings.Builder
	code := cmdRun([]string{
		"./testdata/fixtures/trigger",
		"--trigger-event", "./testdata/events/push-topic.json",
	}, &stdout, &stderr, false)

	if code != exitNoTriggerMatch {
		t.Fatalf("exit code = %d, want %d; stdout=%s stderr=%s",
			code, exitNoTriggerMatch, stdout.String(), stderr.String())
	}
	if !strings.Contains(stderr.String(), "no trigger matched") {
		t.Errorf("stderr = %q, want it to explain the 78", stderr.String())
	}
	if strings.Contains(stdout.String(), "the fixture ran") {
		t.Errorf("the pipeline ran anyway: %q", stdout.String())
	}
}

// TestCmdRunForwardsTheTriggerEventAndRunsOnAMatch is the other half. Same
// binary, same flag, different event, and now the pipeline runs and exits 0.
func TestCmdRunForwardsTheTriggerEventAndRunsOnAMatch(t *testing.T) {
	isolateRegistry(t)
	fastRegistrationPoll(t, 10*time.Millisecond)

	var stdout, stderr strings.Builder
	code := cmdRun([]string{
		"./testdata/fixtures/trigger",
		"--trigger-event", "./testdata/events/push-main.json",
	}, &stdout, &stderr, false)

	if code != exitSuccess {
		t.Fatalf("exit code = %d, want %d; stdout=%s stderr=%s",
			code, exitSuccess, stdout.String(), stderr.String())
	}
	if !strings.Contains(stdout.String(), "the fixture ran") {
		t.Errorf("stdout = %q, want the pipeline's own output", stdout.String())
	}
	if strings.Contains(stderr.String(), "no trigger matched") {
		t.Errorf("stderr claimed a no-match for a run that happened: %q", stderr.String())
	}
}

// TestCmdRunWithoutTheFlagRunsThePipelineUngated: the fixture declares a
// trigger, but with no event there is nothing to gate on, so it runs, which
// is why forgetting the flag over-runs visibly.
func TestCmdRunWithoutTheFlagRunsThePipelineUngated(t *testing.T) {
	isolateRegistry(t)
	fastRegistrationPoll(t, 10*time.Millisecond)

	var stdout, stderr strings.Builder
	code := cmdRun([]string{"./testdata/fixtures/trigger"}, &stdout, &stderr, false)
	if code != exitSuccess {
		t.Fatalf("exit code = %d, want %d; stderr=%s", code, exitSuccess, stderr.String())
	}
	if !strings.Contains(stdout.String(), "the fixture ran") {
		t.Errorf("stdout = %q, want the pipeline to have run", stdout.String())
	}
}
