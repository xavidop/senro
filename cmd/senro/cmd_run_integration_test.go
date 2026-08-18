package main

import (
	"strings"
	"testing"
	"time"
)

// These tests build and exec real fixture binaries under
// testdata/fixtures/; each fixture's package doc says what it proves.
// success/ and failure/ are written entirely against senro's public API,
// as an out-of-module pipeline author would.

// fastRegistrationPoll swaps the discovery-poll interval for one test.
// There is no timeout left to shorten (see waitForRegistrationOrExit): a
// fixture that never registers is picked up as soon as it exits, via
// waitDone, whatever the poll interval.
func fastRegistrationPoll(t *testing.T, interval time.Duration) {
	t.Helper()
	prev := registrationPollInterval
	registrationPollInterval = interval
	t.Cleanup(func() { registrationPollInterval = prev })
}

func TestCmdRunSuccessFixtureExitsZero(t *testing.T) {
	isolateRegistry(t)
	fastRegistrationPoll(t, 10*time.Millisecond)

	var stdout, stderr strings.Builder
	code := cmdRun([]string{"./testdata/fixtures/success", "--ui=none"}, &stdout, &stderr, false)
	if code != exitSuccess {
		t.Fatalf("exit code = %d, want %d; stderr=%s", code, exitSuccess, stderr.String())
	}
}

// TestCmdRunFailureFixtureExitsOneEvenThoughProcessExitsZero is the
// load-bearing case for "exit code is the run's exit code": the fixture's
// own process always returns 0, so a senro run that merely passed the
// child's code through would report success for a failed run.
func TestCmdRunFailureFixtureExitsOneEvenThoughProcessExitsZero(t *testing.T) {
	isolateRegistry(t)
	fastRegistrationPoll(t, 10*time.Millisecond)

	var stdout, stderr strings.Builder
	code := cmdRun([]string{"./testdata/fixtures/failure", "--ui=plain"}, &stdout, &stderr, false)
	if code != exitRunFailed {
		t.Fatalf("exit code = %d, want %d (the RUN's status, not the process's own exit 0); stdout=%s stderr=%s",
			code, exitRunFailed, stdout.String(), stderr.String())
	}
	if !strings.Contains(stdout.String(), "failed") {
		t.Errorf("plain output = %q, want it to report the failure", stdout.String())
	}
}

// TestCmdRunNoAttachFixtureRelaysOutputAndPropagatesExitCode is the
// fallback path: a target that never calls attach.Listen still runs, its
// stdout is relayed (proving args after "--" were forwarded), and its exit
// code passes through, there being no run status to prefer.
func TestCmdRunNoAttachFixtureRelaysOutputAndPropagatesExitCode(t *testing.T) {
	isolateRegistry(t)
	fastRegistrationPoll(t, 10*time.Millisecond)

	var stdout, stderr strings.Builder
	code := cmdRun([]string{"./testdata/fixtures/noattach", "--", "foo", "bar"}, &stdout, &stderr, false)
	if code != 42 {
		t.Fatalf("exit code = %d, want 42 (the process's own exit code); stdout=%s stderr=%s",
			code, stdout.String(), stderr.String())
	}
	if !strings.Contains(stdout.String(), "foo|bar") {
		t.Errorf("stdout = %q, want it to contain the forwarded args \"foo|bar\"", stdout.String())
	}
}

// TestRunPassesThePackageToThePipelineForCrossCompilation pins the one wire
// name between these two processes: a func step on a remote host of another
// platform has to be cross-compiled from the package, and a Go program
// records nothing about where its own source is. This process knows and
// says so; senro.WithFuncBuild reads it. A rename on one side alone is the
// failure this pins.
func TestRunPassesThePackageToThePipelineForCrossCompilation(t *testing.T) {
	isolateRegistry(t)
	fastRegistrationPoll(t, 10*time.Millisecond)

	var stdout, stderr strings.Builder
	code := cmdRun([]string{"./testdata/fixtures/funcpkg"}, &stdout, &stderr, false)
	if code != 43 {
		t.Fatalf("exit code = %d, want 43; stdout=%s stderr=%s", code, stdout.String(), stderr.String())
	}
	want := funcPkgEnv + "=./testdata/fixtures/funcpkg"
	if !strings.Contains(stdout.String(), want) {
		t.Errorf("the pipeline saw %q, want it to contain %q", stdout.String(), want)
	}
}
