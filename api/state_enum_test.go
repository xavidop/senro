package api_test

import (
	"testing"

	"github.com/xavidop/senro/api"
)

func TestStateFailed(t *testing.T) {
	cases := map[api.State]bool{
		api.StateSucceeded:             false,
		api.StateCached:                false,
		api.StateRecovered:             false,
		api.StateSkippedCondition:      false,
		api.StateSkippedManual:         false,
		api.StateSkippedUpstreamFailed: false,
		api.StateFailed:                true,
		api.StateTimedOut:              true,
		api.StatePanicked:              true,
		api.StateCancelled:             false,
	}
	for state, want := range cases {
		if got := state.Failed(); got != want {
			t.Errorf("%s.Failed() = %v, want %v", state, got, want)
		}
	}
}

// The whole point of the taxonomy: a run where every failure was recovered is
// NOT the same as a clean run. Most CI systems show both green, which is how
// flaky infrastructure stays invisible for months.
func TestRollUpDistinguishesRecovery(t *testing.T) {
	cases := []struct {
		name   string
		states []api.State
		want   api.RunStatus
	}{
		{
			name:   "all clean",
			states: []api.State{api.StateSucceeded, api.StateCached},
			want:   api.RunSucceeded,
		},
		{
			name:   "one recovered",
			states: []api.State{api.StateSucceeded, api.StateRecovered},
			want:   api.RunSucceededWithRecovery,
		},
		{
			name:   "one failed",
			states: []api.State{api.StateSucceeded, api.StateFailed},
			want:   api.RunFailed,
		},
		{
			name:   "failure outranks recovery",
			states: []api.State{api.StateRecovered, api.StateFailed},
			want:   api.RunFailed,
		},
		{
			name:   "cancelled outranks failure",
			states: []api.State{api.StateFailed, api.StateCancelled},
			want:   api.RunCancelled,
		},
		{
			name:   "upstream-skipped alone is partial",
			states: []api.State{api.StateSucceeded, api.StateSkippedUpstreamFailed},
			want:   api.RunPartial,
		},
		{
			name:   "empty run succeeds",
			states: nil,
			want:   api.RunSucceeded,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := api.RollUp(tc.states); got != tc.want {
				t.Errorf("RollUp(%v) = %s, want %s", tc.states, got, tc.want)
			}
		})
	}
}

func TestStateTerminal(t *testing.T) {
	// Every declared state must be registered as terminal. Listing them here
	// means adding an eleventh state without registering it fails this test
	// rather than silently returning false at runtime.
	all := []api.State{
		api.StateSucceeded, api.StateCached, api.StateFailed,
		api.StateTimedOut, api.StateCancelled,
		api.StateSkippedUpstreamFailed, api.StateSkippedManual,
		api.StateSkippedCondition, api.StateRecovered, api.StatePanicked,
	}
	if len(all) != 10 {
		t.Fatalf("expected 10 declared states, listed %d", len(all))
	}
	for _, s := range all {
		if !s.Terminal() {
			t.Errorf("%s.Terminal() = false, want true — missing from terminalStates?", s)
		}
	}
	if api.State("running").Terminal() {
		t.Error("an undeclared state must not report terminal")
	}
}
