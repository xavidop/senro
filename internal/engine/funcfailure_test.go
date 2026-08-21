package engine_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/xavidop/senro"
	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/internal/plan"
)

func init() {
	// Reports what ctx.Failure() said, in one line, so a test can assert on
	// the whole answer rather than one field at a time.
	senro.RegisterFunc("failuretest/report", func(ctx senro.Ctx, p struct{}) error {
		f, ok := ctx.Failure()
		if !ok {
			_, _ = fmt.Fprintln(ctx.Stdout(), "no-failure")
			return nil
		}
		_, _ = fmt.Fprintf(ctx.Stdout(), "step=%s state=%s exit=%d attempt=%d err=%q tail=%q\n",
			f.Step, f.State, f.ExitCode, f.Attempt, f.Error, strings.TrimSpace(f.LogTail))
		return nil
	})
}

// The whole point of ctx.Failure: a func handler is told what an Exec
// handler reads out of SENRO_FAILURE_*, and the two describe one attempt.
func TestAFuncHandlerReadsTheFailure(t *testing.T) {
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{{
		ID: "boom", Kind: "exec", Cmd: []string{"sh", "-c", "echo broke here >&2; exit 3"},
		OnFailure: []plan.Node{{
			ID: "notify", Kind: "func",
			Func: &plan.FuncSpec{Name: "failuretest/report"},
		}},
	}}}
	dir, events := runToDirAndEvents(t, p)
	if !hasEventFor(events, api.HandlerSucceeded, "boom/on_failure/notify") {
		t.Fatal("the func handler did not run")
	}

	got := readLog(t, dir, "boom/on_failure/notify", 1, api.StreamStdout)
	for _, want := range []string{
		`step=boom`,
		`state=` + string(api.StateFailed),
		`exit=3`,
		`attempt=1`,
	} {
		if !strings.Contains(got, want) {
			t.Errorf("ctx.Failure() reported %q, missing %q", got, want)
		}
	}
	// LogTail is the field an environment has no room for, and the reason
	// it is worth having: a func handler can classify without going and
	// opening the parent's log file.
	if !strings.Contains(got, "broke here") {
		t.Errorf("ctx.Failure().LogTail did not carry the failed attempt's output: %q", got)
	}
}

// A step is not cleaning up after anything, so ok must be false rather than
// a zero value that reads like a step called "" that exited 0. A function
// used both ways branches on exactly this.
func TestAFuncStepHasNoFailure(t *testing.T) {
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{{
		ID: "work", Kind: "func", Func: &plan.FuncSpec{Name: "failuretest/report"},
	}}}
	dir, events := runToDirAndEvents(t, p)
	if !hasEventFor(events, api.StepFinished, "work") {
		t.Fatal("the func step did not run")
	}
	if got := readLog(t, dir, "work", 1, api.StreamStdout); got != "no-failure\n" {
		t.Errorf("a func STEP reported a failure: %q", got)
	}
}

// The attempt a handler is told about is the one the step actually reached,
// so the handler can find that attempt's log. Retries make this the field
// most likely to be wrong.
func TestAFuncHandlerIsToldTheAttemptTheStepReached(t *testing.T) {
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{{
		ID: "boom", Kind: "exec", Cmd: []string{"sh", "-c", "exit 1"},
		Retry: &plan.RetrySpec{MaxAttempts: 3, Predicate: "exit_code:1"},
		OnFailure: []plan.Node{{
			ID: "notify", Kind: "func",
			Func: &plan.FuncSpec{Name: "failuretest/report"},
		}},
	}}}
	dir, _ := runToDirAndEvents(t, p)
	if got := readLog(t, dir, "boom/on_failure/notify", 1, api.StreamStdout); !strings.Contains(got, "attempt=3") {
		t.Errorf("the handler was told %q, want the third and final attempt", got)
	}
}

// An Always handler runs whatever the outcome, so its Failure describes a
// step that succeeded. State is what tells the two cases apart, which is
// why it is on the struct rather than implied by ok.
func TestAnAlwaysFuncHandlerIsToldASuccess(t *testing.T) {
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{{
		ID: "fine", Kind: "exec", Cmd: []string{"sh", "-c", "exit 0"},
		Always: []plan.Node{{
			ID: "cleanup", Kind: "func",
			Func: &plan.FuncSpec{Name: "failuretest/report"},
		}},
	}}}
	dir, events := runToDirAndEvents(t, p)
	if !hasEventFor(events, api.HandlerSucceeded, "fine/always/cleanup") {
		t.Fatal("the Always func handler did not run")
	}
	got := readLog(t, dir, "fine/always/cleanup", 1, api.StreamStdout)
	if !strings.Contains(got, "step=fine") {
		t.Errorf("an Always handler must still be told which step it belongs to: %q", got)
	}
	if strings.Contains(got, "no-failure") {
		t.Errorf("an Always handler must get a Failure describing the outcome, got %q", got)
	}
	if !strings.Contains(got, "state="+string(api.StateSucceeded)) {
		t.Errorf("an Always handler on a passing step must be told it succeeded: %q", got)
	}
}
