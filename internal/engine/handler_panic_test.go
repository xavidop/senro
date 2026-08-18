package engine_test

import (
	"testing"

	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/internal/plan"
)

// A func STEP that panics settles as api.StatePanicked; a func HANDLER that
// panicked settled as plain failed, so the two paths disagreed about the
// same fact. Easy to miss because the evidence was already right: rc.invoke
// is shared, so the panic's stack reached the handler's log exactly as it
// reaches a step's, and only the classification differed.
func TestAPanickingHandlerIsRecordedAsPanickedNotMerelyFailed(t *testing.T) {
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{{
		ID: "deploy", Kind: "exec", Cmd: []string{"sh", "-c", "exit 9"},
		OnFailure: []plan.Node{{
			ID: "cleanup", Kind: "func", Func: &plan.FuncSpec{Name: "enginetest/panic"},
		}},
	}}}
	_, events := runToDirAndEvents(t, p)

	var found bool
	var body api.HandlerBody
	for _, e := range events {
		if e.Type == api.HandlerFailed {
			found = true
			if err := e.Decode(&body); err != nil {
				t.Fatalf("decode handler.failed: %v", err)
			}
		}
	}
	if !found {
		t.Fatal("no handler.failed event: a panicking handler must still be recorded as having failed")
	}
	if !body.Panicked {
		t.Errorf("handler.failed payload does not report the panic (error = %q); a client cannot tell it apart from a handler that returned an error, while the same panic in a step yields %q",
			body.Error, api.StatePanicked)
	}

	// And the fold, which is what every client actually reads, must turn that
	// into the same state a panicking step gets.
	var st api.RunState
	for _, e := range events {
		if err := st.Apply(e); err != nil {
			t.Fatalf("apply %s: %v", e.Type, err)
		}
	}
	var got api.State
	for _, h := range st.Handlers {
		got = h.State
	}
	if got != api.StatePanicked {
		t.Errorf("folded handler state = %q, want %q", got, api.StatePanicked)
	}
}
