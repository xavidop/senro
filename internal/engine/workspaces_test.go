package engine_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/xavidop/senro"
	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/exec"
	"github.com/xavidop/senro/internal/engine"
	"github.com/xavidop/senro/internal/executor/localexec"
	"github.com/xavidop/senro/internal/sink"
	"github.com/xavidop/senro/internal/storage"
)

func runWithStorage(t *testing.T, p *senro.Plan) (api.RunStatus, []api.Event, string, *storage.Storage) {
	t.Helper()
	runDir := filepath.Join(t.TempDir(), "run")
	store, err := storage.Open(filepath.Join(t.TempDir(), "cache"))
	if err != nil {
		t.Fatalf("storage.Open: %v", err)
	}
	rec := sink.Recording()
	status, err := engine.Run(context.Background(), p, engine.Options{
		Dir:      runDir,
		Executor: localexec.New(runDir, store.Snapshotter),
		Sink:     rec,
		Storage:  store,
		RunID:    "r1",
	})
	if err != nil {
		t.Fatalf("engine.Run: %v", err)
	}
	return status, rec.Events(), runDir, store
}

// One workspace, two steps, and the second sees what the first wrote. This
// is what "shared across steps within a run" means.
func TestAScopeRunWorkspaceCarriesDataBetweenSteps(t *testing.T) {
	ws := senro.Workspace("build", senro.Scope(senro.ScopeRun))
	pipe := senro.New("ci")
	l := pipe.Workflow("main")
	l.Step("produce", exec.Command("sh", "-c", "echo artifact > out.txt")).
		WorkDir("/build").Mount(ws.At("/build", senro.RW))
	l.Step("consume", exec.Command("sh", "-c", "cat out.txt")).
		Needs("produce").WorkDir("/build").Mount(ws.At("/build", senro.RO))
	p, err := pipe.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	status, events, runDir, _ := runWithStorage(t, p)
	if status != api.RunSucceeded {
		t.Fatalf("status = %s, want succeeded", status)
	}
	b, err := os.ReadFile(filepath.Join(runDir, "logs", "consume", "1", "stdout"))
	if err != nil {
		t.Fatalf("read consume stdout: %v", err)
	}
	if strings.TrimSpace(string(b)) != "artifact" {
		t.Errorf("consume saw %q, want the file produce wrote", b)
	}
	if countType(events, api.WSSnapshot) == 0 {
		t.Error("no ws.snapshot was emitted")
	}
}

func TestWSSnapshotCarriesTheDigestTheIndexAndTheCounts(t *testing.T) {
	ws := senro.Workspace("build", senro.Scope(senro.ScopeRun))
	pipe := senro.New("ci")
	l := pipe.Workflow("main")
	l.Step("produce", exec.Command("sh", "-c", "echo hi > a.txt")).
		WorkDir("/build").Mount(ws.At("/build", senro.RW))
	p, err := pipe.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	_, events, _, _ := runWithStorage(t, p)
	var body api.WSSnapshotBody
	var found bool
	for _, e := range events {
		if e.Type == api.WSSnapshot {
			if err := e.Decode(&body); err != nil {
				t.Fatalf("decode ws.snapshot: %v", err)
			}
			found = true
		}
	}
	if !found {
		t.Fatal("no ws.snapshot event")
	}
	if body.Name != "build" || body.Digest == "" || body.Index == "" {
		t.Errorf("ws.snapshot = %+v, want a named workspace with both digests", body)
	}
	if body.Files != 1 || body.Bytes != 3 {
		t.Errorf("ws.snapshot = %+v, want one file of three bytes", body)
	}
}

// Failure is when the workspace matters most: a failed step's evidence must
// still be captured.
func TestAFailedStepStillSnapshotsItsWorkspace(t *testing.T) {
	ws := senro.Workspace("build", senro.Scope(senro.ScopeRun))
	pipe := senro.New("ci")
	l := pipe.Workflow("main")
	l.Step("boom", exec.Command("sh", "-c", "echo evidence > clue.txt; exit 7")).
		WorkDir("/build").Mount(ws.At("/build", senro.RW))
	p, err := pipe.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	status, events, _, _ := runWithStorage(t, p)
	if status != api.RunFailed {
		t.Fatalf("status = %s, want failed", status)
	}
	if countType(events, api.WSSnapshot) == 0 {
		t.Error("a failed step left no ws.snapshot, so the evidence it wrote is unaddressable")
	}
}

// A step that deletes the very directory it was mounted at: a snapshot
// failing mid-run, end to end rather than through a fake. The command
// itself succeeds (exit 0); only the capture that follows it fails, so this
// is the guard that stops a workload success from masking an unaddressable
// result. See the exit-versus-error distinction in snapshotMounts.
func TestAStepThatDeletesItsOwnWorkspaceFailsAtSnapshotTime(t *testing.T) {
	ws := senro.Workspace("vanish", senro.Scope(senro.ScopeRun))
	pipe := senro.New("ci")
	l := pipe.Workflow("main")
	l.Step("rmrf", exec.Command("sh", "-c", `rm -rf "$PWD"`)).
		WorkDir("/vanish").Mount(ws.At("/vanish", senro.RW))
	p, err := pipe.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	status, events, _, _ := runWithStorage(t, p)
	if status != api.RunFailed {
		t.Fatalf("status = %s, want failed: the workspace was gone by the time the post-step snapshot ran", status)
	}
	if countType(events, api.WSSnapshot) != 0 {
		t.Error("a workspace that no longer exists cannot have produced a ws.snapshot")
	}
	var msg string
	for _, e := range events {
		if e.Type == api.StepFinished && e.Step == "rmrf" {
			var b api.StepFinishedBody
			_ = e.Decode(&b)
			msg = b.Error
			if b.ExitCode != 0 {
				t.Errorf("ExitCode = %d, want 0: the command itself succeeded, only the capture after it failed", b.ExitCode)
			}
		}
	}
	if msg == "" {
		t.Error("step.finished carries no error explaining why the step failed")
	}
}

func TestNoSnapshotSuppressesTheSnapshot(t *testing.T) {
	ws := senro.Workspace("scratchpad", senro.Scope(senro.ScopeRun))
	pipe := senro.New("ci")
	l := pipe.Workflow("main")
	l.Step("noisy", exec.Command("sh", "-c", "echo junk > j.txt")).
		WorkDir("/w").Mount(ws.At("/w", senro.RW)).NoSnapshot()
	p, err := pipe.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	_, events, _, _ := runWithStorage(t, p)
	if n := countType(events, api.WSSnapshot); n != 0 {
		t.Errorf("NoSnapshot() still produced %d ws.snapshot events", n)
	}
}

// The class fix. Local cannot enforce read-only, so it detects the breach
// instead of pretending it cannot happen.
func TestAStepThatWritesThroughAReadOnlyMountFails(t *testing.T) {
	ws := senro.Workspace("src", senro.Scope(senro.ScopeRun))
	pipe := senro.New("ci")
	l := pipe.Workflow("main")
	l.Step("seed", exec.Command("sh", "-c", "echo original > f.txt")).
		WorkDir("/src").Mount(ws.At("/src", senro.RW))
	l.Step("sneaky", exec.Command("sh", "-c", "echo tampered > f.txt")).
		Needs("seed").WorkDir("/src").Mount(ws.At("/src", senro.RO))
	p, err := pipe.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	status, events, _, _ := runWithStorage(t, p)
	if status != api.RunFailed {
		t.Fatalf("status = %s, want failed: a step wrote through a read-only mount", status)
	}
	var msg string
	for _, e := range events {
		if e.Type == api.StepFinished && e.Step == "sneaky" {
			var b api.StepFinishedBody
			_ = e.Decode(&b)
			msg = b.Error
		}
	}
	if !strings.Contains(msg, "read-only") || !strings.Contains(msg, "src") {
		t.Errorf("the failure does not name the breach or the workspace: %q", msg)
	}
}

// The negative half of the read-only check: a step that only reads must not
// be failed by it, or every read-only mount in every pipeline breaks.
func TestAStepThatOnlyReadsThroughAReadOnlyMountSucceeds(t *testing.T) {
	ws := senro.Workspace("src", senro.Scope(senro.ScopeRun))
	pipe := senro.New("ci")
	l := pipe.Workflow("main")
	l.Step("seed", exec.Command("sh", "-c", "echo original > f.txt")).
		WorkDir("/src").Mount(ws.At("/src", senro.RW))
	l.Step("reader", exec.Command("cat", "f.txt")).
		Needs("seed").WorkDir("/src").Mount(ws.At("/src", senro.RO))
	p, err := pipe.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	if status, _, _, _ := runWithStorage(t, p); status != api.RunSucceeded {
		t.Errorf("status = %s, want succeeded", status)
	}
}

// The exit-versus-error distinction, from the read-only check's own angle:
// when a step's own command already fails, ITS exit code is the verdict
// that must reach step.finished. A read-only violation the same attempt
// also happens to trigger must not silently overwrite a real exit code with
// a different one (or none at all). Exit is the workload's verdict; the
// snapshot check exists to catch a workspace lie, not to relitigate an exit
// code the command itself already reported.
func TestAFailedCommandsExitCodeSurvivesACoincidentReadOnlyViolation(t *testing.T) {
	ws := senro.Workspace("src", senro.Scope(senro.ScopeRun))
	pipe := senro.New("ci")
	l := pipe.Workflow("main")
	l.Step("seed", exec.Command("sh", "-c", "echo original > f.txt")).
		WorkDir("/src").Mount(ws.At("/src", senro.RW))
	l.Step("sneaky", exec.Command("sh", "-c", "echo tampered > f.txt; exit 3")).
		Needs("seed").WorkDir("/src").Mount(ws.At("/src", senro.RO))
	p, err := pipe.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	status, events, _, _ := runWithStorage(t, p)
	if status != api.RunFailed {
		t.Fatalf("status = %s, want failed", status)
	}
	var body api.StepFinishedBody
	var found bool
	for _, e := range events {
		if e.Type == api.StepFinished && e.Step == "sneaky" {
			_ = e.Decode(&body)
			found = true
		}
	}
	if !found {
		t.Fatal("no step.finished for sneaky")
	}
	if body.ExitCode != 3 {
		t.Errorf("ExitCode = %d, want 3: the command's own exit is the workload's verdict and must survive "+
			"a coincident read-only violation", body.ExitCode)
	}
}

// The check that catches a caller who forgot to supply storage. A run that
// silently drops every workspace and every cache is indistinguishable from a
// working one until somebody notices the cache never hits.
func TestRunRefusesAPlanNeedingStorageWhenThereIsNone(t *testing.T) {
	ws := senro.Workspace("w", senro.Scope(senro.ScopeRun))
	pipe := senro.New("ci")
	l := pipe.Workflow("main")
	l.Step("s", exec.Command("true")).WorkDir("/w").Mount(ws.At("/w", senro.RW))
	p, err := pipe.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	runDir := filepath.Join(t.TempDir(), "run")
	_, err = engine.Run(context.Background(), p, engine.Options{
		Dir: runDir, Executor: localexec.New(runDir, nil), RunID: "r1",
	})
	if err == nil {
		t.Fatal("a plan declaring a workspace ran with no storage configured")
	}
	if !strings.Contains(err.Error(), "storage") {
		t.Errorf("the error does not name what is missing: %v", err)
	}
}

func TestAPlanNeedingNoStorageStillRunsWithNone(t *testing.T) {
	pipe := senro.New("ci")
	l := pipe.Workflow("main")
	l.Step("s", exec.Command("true"))
	p, err := pipe.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	runDir := filepath.Join(t.TempDir(), "run")
	status, err := engine.Run(context.Background(), p, engine.Options{
		Dir: runDir, Executor: localexec.New(runDir, nil), RunID: "r1",
	})
	if err != nil {
		t.Fatalf("engine.Run: %v", err)
	}
	if status != api.RunSucceeded {
		t.Errorf("status = %s, want succeeded", status)
	}
}

// countType is a helper this file adds for the whole package.
func countType(events []api.Event, ty api.Type) int {
	var n int
	for _, e := range events {
		if e.Type == ty {
			n++
		}
	}
	return n
}
