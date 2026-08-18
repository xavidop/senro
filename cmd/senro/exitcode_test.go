package main

import (
	"testing"

	"github.com/xavidop/senro/api"
)

// TestExitCodeContractValues pins the four public constants: 0 success, 1
// run failed, 2 usage, 130 cancelled. 78 (EX_CONFIG) is reserved for a
// trigger outcome and must never be one of these.
func TestExitCodeContractValues(t *testing.T) {
	cases := map[string]int{
		"exitSuccess":   0,
		"exitRunFailed": 1,
		"exitUsage":     2,
		"exitCancelled": 130,
	}
	got := map[string]int{
		"exitSuccess":   exitSuccess,
		"exitRunFailed": exitRunFailed,
		"exitUsage":     exitUsage,
		"exitCancelled": exitCancelled,
	}
	for name, want := range cases {
		if got[name] != want {
			t.Errorf("%s = %d, want %d", name, got[name], want)
		}
	}
}

func TestExitCodeForRunStatus(t *testing.T) {
	cases := []struct {
		status api.RunStatus
		want   int
	}{
		{api.RunSucceeded, exitSuccess},
		{api.RunSucceededWithRecovery, exitSuccess},
		{api.RunFailed, exitRunFailed},
		{api.RunPartial, exitRunFailed},
		{api.RunCancelled, exitCancelled},
		// Empty: the run never reached a terminal state while attached, a
		// deliberate early detach ('q'). Quitting the UI must not read to a
		// script as a failed deploy.
		{"", exitSuccess},
	}
	for _, c := range cases {
		if got := exitCodeForRunStatus(c.status); got != c.want {
			t.Errorf("exitCodeForRunStatus(%q) = %d, want %d", c.status, got, c.want)
		}
	}
}

// TestExitCodeNeverProduces78 is the negative half of the contract: no
// RunStatus, including ones this build has never seen, may produce the
// code reserved for a trigger outcome.
func TestExitCodeNeverProduces78(t *testing.T) {
	for _, status := range []api.RunStatus{
		api.RunSucceeded, api.RunSucceededWithRecovery, api.RunPartial,
		api.RunFailed, api.RunCancelled, "", "some-future-status",
	} {
		if got := exitCodeForRunStatus(status); got == 78 {
			t.Errorf("exitCodeForRunStatus(%q) = 78, which is reserved for trigger matching", status)
		}
	}
}

// TestExitCodeSignalInterruptedIsAlways130 pins the OS-signal path
// separately from RunStatus: Ctrl-C reports 130 whatever partial status had
// been folded, typically none, which is why the operator was watching.
func TestExitCodeSignalInterruptedIsAlways130(t *testing.T) {
	for _, status := range []api.RunStatus{"", api.RunSucceeded, api.RunFailed} {
		if got := exitCodeForInterrupted(status); got != exitCancelled {
			t.Errorf("exitCodeForInterrupted(%q) = %d, want %d", status, got, exitCancelled)
		}
	}
}
