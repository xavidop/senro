package engine_test

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/xavidop/senro"
	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/exec"
	"github.com/xavidop/senro/internal/stepid"
	"github.com/xavidop/senro/retry"
)

func init() {
	senro.RegisterFunc("enginetest/handler-reads-workspace", func(ctx senro.Ctx, p struct {
		File string `json:"file"`
	}) error {
		ws, ok := ctx.Workspace("build")
		if !ok {
			return errors.New("the handler cannot see its parent's workspace")
		}
		b, err := os.ReadFile(ws.Path(p.File))
		if err != nil {
			return err
		}
		_, err = ctx.Stdout().Write(b)
		return err
	})
}

// handlerLog reads one handler's stdout out of a run directory, by the same
// composite log-step id the handler's own events carry.
func handlerLog(t *testing.T, runDir, parent, kind, handler string) string {
	t.Helper()
	path := filepath.Join(runDir, "logs",
		stepid.Encode(parent+"/"+kind+"/"+handler), "1", api.StreamStdout)
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading handler %q's stdout: %v", handler, err)
	}
	return string(b)
}

// TestAHandlerReadsTheFailedStepsWorkspace is the smallest pipeline showing
// workspace inheritance: a step writes a file into its mounted workspace and
// fails, and its OnFailure handler reads that file back. Without
// inheritance `cat build.log` prints nothing and the handler reports "no
// log" for a step that produced one.
//
// The assertion is on CONTENT, never a path: comparing sandbox directories
// would pass for localexec, whose Sandbox is a pure function of (StepID,
// Attempt), while proving nothing about an executor with a real sandbox
// lifecycle (see the repository root's container-executor twin).
func TestAHandlerReadsTheFailedStepsWorkspace(t *testing.T) {
	ws := senro.Workspace("build", senro.Scope(senro.ScopeRun))
	pipe := senro.New("ci")
	l := pipe.Workflow("main")
	l.Step("deploy", exec.Command("sh", "-c", "echo the-parents-evidence > build.log; exit 9")).
		WorkDir("/build").Mount(ws.At("/build", senro.RW)).
		OnFailure(senro.Handler("collect", exec.Command("sh", "-c",
			`if [ -f build.log ]; then cat build.log; else echo NO-LOG-FOUND; fi`)))
	p, err := pipe.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	_, _, runDir, _ := runWithStorage(t, p)

	got := strings.TrimSpace(handlerLog(t, runDir, "deploy", "on_failure", "collect"))
	if got != "the-parents-evidence" {
		t.Errorf("the handler read %q, want %q: a cleanup handler that cannot read the files "+
			"the failed step wrote can only report that something went wrong, which the "+
			"operator already knew", got, "the-parents-evidence")
	}
}

// TestAnAlwaysHandlerReadsTheStepsWorkspaceToo is the other handler list, on
// the settle-time path, for a step that SUCCEEDED. Always is where evidence
// collection and artifact upload actually live, and the two lists reach
// runHandlers through different call sites (runStep for OnFailure,
// runAlwaysAtSettle for Always), which is exactly the shape that has diverged
// in this package before.
func TestAnAlwaysHandlerReadsTheStepsWorkspaceToo(t *testing.T) {
	ws := senro.Workspace("build", senro.Scope(senro.ScopeRun))
	pipe := senro.New("ci")
	l := pipe.Workflow("main")
	l.Step("deploy", exec.Command("sh", "-c", "echo always-sees-this > build.log")).
		WorkDir("/build").Mount(ws.At("/build", senro.RW)).
		Always(senro.Handler("upload", exec.Command("sh", "-c",
			`if [ -f build.log ]; then cat build.log; else echo NO-LOG-FOUND; fi`)))
	p, err := pipe.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	_, _, runDir, _ := runWithStorage(t, p)

	got := strings.TrimSpace(handlerLog(t, runDir, "deploy", "always", "upload"))
	if got != "always-sees-this" {
		t.Errorf("the Always handler read %q, want %q", got, "always-sees-this")
	}
}

// TestAFuncHandlerReachesTheParentsWorkspace closes the fourth divergence at
// the runAttempt/execHandler seam. runAttempt hands rc.invoke the attempt's
// realized mounts, which is what funcCtx.Workspace answers from; execHandler
// handed it nil, so ctx.Workspace(name) reported (,"" false) for EVERY func
// handler, forever, whatever its parent mounted. That is not a missing
// feature a caller can route around: false is the value the interface reserves
// for "this step did not mount that workspace", so a func handler was told a
// lie it had no way to detect.
func TestAFuncHandlerReachesTheParentsWorkspace(t *testing.T) {
	ws := senro.Workspace("build", senro.Scope(senro.ScopeRun))
	pipe := senro.New("ci")
	l := pipe.Workflow("main")
	l.Step("deploy", exec.Command("sh", "-c", "echo func-handler-evidence > build.log; exit 9")).
		WorkDir("/build").Mount(ws.At("/build", senro.RW)).
		OnFailure(senro.Handler("collect", senro.Func("enginetest/handler-reads-workspace",
			map[string]string{"file": "build.log"})))
	p, err := pipe.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	_, _, runDir, _ := runWithStorage(t, p)

	got := strings.TrimSpace(handlerLog(t, runDir, "deploy", "on_failure", "collect"))
	if got != "func-handler-evidence" {
		t.Errorf("the func handler read %q, want %q", got, "func-handler-evidence")
	}
}

// TestAHandlerSeesTheAttemptThatActuallyFailed pins inheritance to the LAST
// attempt's workspace rather than the first's. A ScopeRun workspace is one
// directory for the whole run, so the third attempt's writes are what is
// there when the handler runs; this proves the handler is not being pointed at
// an earlier attempt's view of it.
func TestAHandlerSeesTheAttemptThatActuallyFailed(t *testing.T) {
	ws := senro.Workspace("build", senro.Scope(senro.ScopeRun))
	pipe := senro.New("ci")
	l := pipe.Workflow("main")
	l.Step("deploy", exec.Command("sh", "-c", `echo one-more-attempt >> build.log; exit 9`)).
		WorkDir("/build").Mount(ws.At("/build", senro.RW)).
		RetryPolicy(retry.Policy{
			MaxAttempts: 3,
			Backoff:     retry.Backoff{Base: time.Millisecond},
			On:          retry.OnExitCode(9),
		}).
		OnFailure(senro.Handler("collect", exec.Command("sh", "-c", "wc -l < build.log")))
	p, err := pipe.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	_, _, runDir, _ := runWithStorage(t, p)

	got := strings.TrimSpace(handlerLog(t, runDir, "deploy", "on_failure", "collect"))
	if got != "3" {
		t.Errorf("the handler counted %q lines in build.log, want 3: it is not looking at the "+
			"workspace as the step's FINAL attempt left it", got)
	}
}
