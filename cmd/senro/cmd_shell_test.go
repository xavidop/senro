package main

import (
	"bytes"
	"strings"
	"testing"
)

// TestShellRequiresAStep is the one flag with no sensible default: a session
// stands in ONE step's workspaces, so there is nothing to guess.
func TestShellRequiresAStep(t *testing.T) {
	var out, errb bytes.Buffer
	code := cmdShell(nil, &out, &errb, strings.NewReader(""))
	if code != exitUsage {
		t.Errorf("exit = %d, want %d", code, exitUsage)
	}
	if !strings.Contains(errb.String(), "--step is required") {
		t.Errorf("stderr = %q, want it to name the missing flag", errb.String())
	}
}

func TestShellRefusesPidAndRunTogether(t *testing.T) {
	var out, errb bytes.Buffer
	code := cmdShell([]string{"--pid", "1", "--run", "r", "--step", "build"}, &out, &errb, strings.NewReader(""))
	if code != exitUsage {
		t.Errorf("exit = %d, want %d", code, exitUsage)
	}
	if !strings.Contains(errb.String(), "mutually exclusive") {
		t.Errorf("stderr = %q, want the mutual exclusion named", errb.String())
	}
}

// TestShellOnAFinishedRunPointsAtWsPull: a session needs the running
// engine that owns the workspaces, so somebody who just wants to see what
// a failed step left must be told what to run instead.
func TestShellOnAFinishedRunPointsAtWsPull(t *testing.T) {
	// The registry the discovery below reads, isolated from the developer's
	// own live runs the same way discover_test.go isolates it.
	dir := t.TempDir()
	t.Setenv("HOME", dir)
	t.Setenv("XDG_RUNTIME_DIR", dir)
	var out, errb bytes.Buffer
	code := cmdShell([]string{"--run", "no-such-run", "--step", "build"}, &out, &errb, strings.NewReader(""))
	if code != exitUsage {
		t.Errorf("exit = %d, want %d", code, exitUsage)
	}
	if !strings.Contains(errb.String(), "ws pull") {
		t.Errorf("stderr = %q, want it to point at senro ws pull", errb.String())
	}
}

// TestShellUsageIsListed keeps the command discoverable: a key or a command
// that exists and is undocumented is one an operator finds by accident.
func TestShellUsageIsListed(t *testing.T) {
	if !strings.Contains(usage, "senro shell") {
		t.Error("the usage text does not mention senro shell")
	}
	for _, want := range []string{"--step", "workspaces", "No secrets", "ws pull"} {
		if !strings.Contains(usage, want) {
			t.Errorf("the usage text for senro shell does not mention %q", want)
		}
	}
}

// TestShellExitCodePassesTheCommandsOwnStatusThrough is what makes
// `senro shell --step build -- test -f out/binary` usable in a script: the
// command's answer is the process's answer.
func TestShellExitCodePassesTheCommandsOwnStatusThrough(t *testing.T) {
	for _, tc := range []struct{ in, want int }{
		{0, exitSuccess},
		{1, 1},
		{7, 7},
		{130, 130},
		// A command killed by a signal, which os/exec reports as -1: not
		// something a process can exit with, so it becomes a plain failure
		// rather than whatever the OS would make of a negative status.
		{-1, exitRunFailed},
	} {
		if got := shellExitCode(tc.in); got != tc.want {
			t.Errorf("shellExitCode(%d) = %d, want %d", tc.in, got, tc.want)
		}
	}
}

// TestShellBannerSetsTheRightExpectationForEachKind: the two kinds of
// session feel completely different. The pipe banner must say there is no
// prompt, or the session reads as a hung tool, and how to get one; the
// terminal banner must say how to get OUT, since in raw mode the
// operator's usual reflexes go to the far side.
func TestShellBannerSetsTheRightExpectationForEachKind(t *testing.T) {
	for _, want := range []string{"no prompt", "^D", "--tty"} {
		if !strings.Contains(shellBanner, want) {
			t.Errorf("the pipe banner does not mention %q: %q", want, shellBanner)
		}
	}
	for _, want := range []string{"terminal", "^D", "^C"} {
		if !strings.Contains(ttyBanner, want) {
			t.Errorf("the terminal banner does not mention %q: %q", want, ttyBanner)
		}
	}
}
