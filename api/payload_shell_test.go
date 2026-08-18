package api_test

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/xavidop/senro/api"
)

// TestShellTypesAreDeclaredNotReserved is the counterpart to
// TestClientEventsAreReservedNotDeclared: shell.opened and shell.closed spent
// this project's whole life in reservedTypes, and the moment the engine
// really emits them they have to move, or the published list tells a client
// the two events it is about to receive are for a feature that does not
// exist.
func TestShellTypesAreDeclaredNotReserved(t *testing.T) {
	for _, ty := range []api.Type{api.ShellOpened, api.ShellClosed} {
		var found bool
		for _, declared := range api.DeclaredTypes() {
			if declared == ty {
				found = true
			}
		}
		if !found {
			t.Errorf("%s is emitted by this build and must be declared, not reserved", ty)
		}
		if !ty.Known() {
			t.Errorf("%s must stay known", ty)
		}
	}
}

// TestShellBodiesRoundTrip pins the wire field names. A client reading
// shell.closed to find out whether an operator is still standing in a failed
// step's workspace decodes these names and nothing else.
func TestShellBodiesRoundTrip(t *testing.T) {
	open := api.ShellOpenedBody{
		Session: "s1", ClientID: "c3",
		Cmd:        []string{"/bin/sh"},
		Workspaces: []string{"src", "out"},
	}
	b, err := json.Marshal(open)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	for _, want := range []string{`"session":"s1"`, `"client_id":"c3"`, `"cmd":["/bin/sh"]`, `"workspaces":["src","out"]`} {
		if !strings.Contains(string(b), want) {
			t.Errorf("shell.opened body %s is missing %s", b, want)
		}
	}
	var back api.ShellOpenedBody
	if err := json.Unmarshal(b, &back); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if back.Session != open.Session || back.ClientID != open.ClientID || len(back.Workspaces) != 2 {
		t.Errorf("round trip = %+v, want %+v", back, open)
	}

	closed := api.ShellClosedBody{Session: "s1", ClientID: "c3", ExitCode: 130, Duration: 90 * time.Second}
	cb, err := json.Marshal(closed)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	for _, want := range []string{`"session":"s1"`, `"exit_code":130`, `"duration_ns":90000000000`} {
		if !strings.Contains(string(cb), want) {
			t.Errorf("shell.closed body %s is missing %s", cb, want)
		}
	}
	// Error is omitted for a session that ended by its command exiting, so a
	// reader can use its presence alone to tell the two apart.
	if strings.Contains(string(cb), `"error"`) {
		t.Errorf("shell.closed body %s carries an empty error field", cb)
	}
}

// TestShellEventBodiesCarryNoSessionTraffic is a structural guard, not a
// behavioural one: neither body may grow a field that could hold what an
// operator typed or what the session printed. See ShellOpenedBody's own doc:
// the ledger records that a shell existed, never what happened inside it.
func TestShellEventBodiesCarryNoSessionTraffic(t *testing.T) {
	b, err := json.Marshal(api.ShellOpenedBody{})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	for _, forbidden := range []string{"stdin", "stdout", "stderr", "output", "input", "transcript"} {
		if strings.Contains(string(b), forbidden) {
			t.Errorf("shell.opened body carries a %q field: session traffic must never enter the ledger", forbidden)
		}
	}
	cb, err := json.Marshal(api.ShellClosedBody{})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	for _, forbidden := range []string{"stdin", "stdout", "stderr", "output", "input", "transcript"} {
		if strings.Contains(string(cb), forbidden) {
			t.Errorf("shell.closed body carries a %q field: session traffic must never enter the ledger", forbidden)
		}
	}
}
