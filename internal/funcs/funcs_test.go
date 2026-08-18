package funcs_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"

	"github.com/xavidop/senro/internal/funcs"
)

type fakeCtx struct{ context.Context }

func (fakeCtx) RunID() string                                { return "r" }
func (fakeCtx) StepID() string                               { return "s" }
func (fakeCtx) Attempt() int                                 { return 1 }
func (fakeCtx) Workspace(string) (funcs.WorkspacePath, bool) { return "", false }
func (fakeCtx) Secret(string) string                         { return "" }
func (fakeCtx) Stdout() io.Writer                            { return io.Discard }
func (fakeCtx) Stderr() io.Writer                            { return io.Discard }
func (fakeCtx) Logger() *slog.Logger                         { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func TestRegisterAndInvoke(t *testing.T) {
	// The registry is process-global state, and this repository's test gate
	// runs `go test -count=2`, which calls this function twice in the SAME
	// process: without a reset, the second call to Register below finds
	// "test/echo" already there from the first iteration and panics on a
	// duplicate registration that this test never intended to make. See
	// ResetForTest's own doc.
	t.Cleanup(funcs.ResetForTest)

	var got string
	funcs.Register("test/echo", func(_ funcs.Ctx, p json.RawMessage) error {
		got = string(p)
		return nil
	})
	if err := funcs.Invoke(fakeCtx{context.Background()}, "test/echo", json.RawMessage(`{"a":1}`)); err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	if got != `{"a":1}` {
		t.Errorf("params = %q", got)
	}
}

func TestInvokingAnUnregisteredNameNamesWhatIsRegistered(t *testing.T) {
	err := funcs.Invoke(fakeCtx{context.Background()}, "test/nope", nil)
	if err == nil {
		t.Fatal("Invoke accepted an unregistered name")
	}
	if !strings.Contains(err.Error(), "test/nope") {
		t.Errorf("the error does not name the function: %v", err)
	}
}

// TestAPanickingFunctionBecomesAnErrorRatherThanACrash is not a nicety. A
// local Func runs IN the coordinator's process, so an unrecovered panic takes
// the whole run down: the ledger is never sealed, run.finished is never
// emitted, every attached client sees a socket close, and the workspaces of
// every other in-flight step are left unsnapshotted. Recovering here turns
// that into one failed step.
func TestAPanickingFunctionBecomesAnErrorRatherThanACrash(t *testing.T) {
	t.Cleanup(funcs.ResetForTest) // see TestRegisterAndInvoke's own comment
	funcs.Register("test/panic", func(funcs.Ctx, json.RawMessage) error {
		panic("boom")
	})
	err := funcs.Invoke(fakeCtx{context.Background()}, "test/panic", nil)
	if err == nil {
		t.Fatal("a panicking function returned no error")
	}
	var pe *funcs.PanicError
	if !errors.As(err, &pe) {
		t.Fatalf("err = %T, want a *funcs.PanicError so the engine can report panicked", err)
	}
	if len(pe.Stack) == 0 {
		t.Error("the panic carries no stack, so nobody can find it")
	}
	if !strings.Contains(err.Error(), "boom") {
		t.Errorf("the error does not carry the panic value: %v", err)
	}
}

func TestRegisteringTheSameNameTwicePanics(t *testing.T) {
	t.Cleanup(funcs.ResetForTest) // see TestRegisterAndInvoke's own comment
	defer func() {
		if recover() == nil {
			t.Fatal("a duplicate registration was accepted")
		}
	}()
	funcs.Register("test/dup", func(funcs.Ctx, json.RawMessage) error { return nil })
	funcs.Register("test/dup", func(funcs.Ctx, json.RawMessage) error { return nil })
}

func TestWorkspacePathJoins(t *testing.T) {
	w := funcs.WorkspacePath("/runs/1/ws/build")
	if got := w.Path("out", "app"); got != "/runs/1/ws/build/out/app" {
		t.Errorf("Path = %q", got)
	}
	if got := w.Path(); got != "/runs/1/ws/build" {
		t.Errorf("Path() = %q", got)
	}
}
