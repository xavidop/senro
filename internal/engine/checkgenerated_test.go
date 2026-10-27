package engine_test

import (
	"strings"
	"testing"

	"github.com/xavidop/senro"
	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/exec"
)

// checkGenerated runs a seed step that writes gen.txt into a run-scoped
// workspace, then a "codegen" step running cmd with CheckGenerated, and
// returns the run status, codegen's step.finished body and every event.
func checkGenerated(t *testing.T, cmd string) (api.RunStatus, api.StepFinishedBody, []api.Event) {
	t.Helper()
	ws := senro.Workspace("src", senro.Scope(senro.ScopeRun))
	pipe := senro.New("ci")
	l := pipe.Workflow("main")
	l.Step("seed", exec.Command("sh", "-c", "echo original > gen.txt")).
		WorkDir("/src").Mount(ws.At("/src", senro.RW))
	l.Step("codegen", exec.Command("sh", "-c", cmd)).
		Needs("seed").WorkDir("/src").Mount(ws.At("/src", senro.RW)).CheckGenerated()
	p, err := pipe.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	status, events, _, _ := runWithStorage(t, p)
	var body api.StepFinishedBody
	var found bool
	for _, e := range events {
		if e.Type == api.StepFinished && e.Step == "codegen" {
			if err := e.Decode(&body); err != nil {
				t.Fatalf("decode step.finished: %v", err)
			}
			found = true
		}
	}
	if !found {
		t.Fatal("no step.finished for codegen")
	}
	return status, body, events
}

func TestCheckGeneratedFailsAStepWhoseWorkspaceChanged(t *testing.T) {
	status, body, events := checkGenerated(t, "echo appended >> gen.txt")
	if status != api.RunFailed {
		t.Fatalf("status = %s, want failed: codegen changed a generated file", status)
	}
	if body.State != api.StateFailed {
		t.Errorf("codegen state = %s, want failed", body.State)
	}
	if body.ExitCode != 0 {
		t.Errorf("ExitCode = %d, want 0: the command succeeded, the check failed it", body.ExitCode)
	}
	want := "generated files are out of date in workspace src: 1 changed: gen.txt"
	if !strings.Contains(body.Error, want) {
		t.Errorf("Error = %q, want it to contain %q", body.Error, want)
	}
	// The drifted snapshot is kept, so `senro ws diff` can show it.
	var snapped bool
	for _, e := range events {
		if e.Type == api.WSSnapshot && e.Step == "codegen" && e.Attempt == 1 {
			snapped = true
		}
	}
	if !snapped {
		t.Error("no ws.snapshot for codegen's attempt: the drift must stay addressable")
	}
}

func TestCheckGeneratedPassesAStepThatRegeneratesIdenticalContent(t *testing.T) {
	// Rewriting the same bytes is what an up-to-date generator does.
	status, body, _ := checkGenerated(t, "echo original > gen.txt")
	if status != api.RunSucceeded {
		t.Fatalf("status = %s (error %q), want succeeded: nothing changed", status, body.Error)
	}
	if body.State != api.StateSucceeded {
		t.Errorf("codegen state = %s, want succeeded", body.State)
	}
}

func TestCheckGeneratedDoesNotRunWhenTheGeneratorItselfFails(t *testing.T) {
	status, body, _ := checkGenerated(t, "echo appended >> gen.txt; exit 3")
	if status != api.RunFailed {
		t.Fatalf("status = %s, want failed", status)
	}
	if body.State != api.StateFailed {
		t.Errorf("codegen state = %s, want failed", body.State)
	}
	// A plain non-zero exit is reported by its exit code alone, with an
	// empty Error: that is the generator's own failure, untouched.
	if body.ExitCode != 3 {
		t.Errorf("ExitCode = %d, want 3: the generator's own exit status", body.ExitCode)
	}
	if strings.Contains(body.Error, "out of date") {
		t.Errorf("Error = %q: the drift check ran on a failed attempt", body.Error)
	}
}

// A workspace no earlier step wrote starts from nothing, so every file the
// step creates is drift.
func TestCheckGeneratedComparesAgainstAnEmptyWorkspace(t *testing.T) {
	ws := senro.Workspace("out", senro.Scope(senro.ScopeRun))
	pipe := senro.New("ci")
	l := pipe.Workflow("main")
	l.Step("codegen", exec.Command("sh", "-c", "echo a > a.txt; echo b > b.txt")).
		WorkDir("/out").Mount(ws.At("/out", senro.RW)).CheckGenerated()
	p, err := pipe.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	status, events, _, _ := runWithStorage(t, p)
	if status != api.RunFailed {
		t.Fatalf("status = %s, want failed", status)
	}
	var body api.StepFinishedBody
	for _, e := range events {
		if e.Type == api.StepFinished && e.Step == "codegen" {
			_ = e.Decode(&body)
		}
	}
	want := "generated files are out of date in workspace out: 2 changed: a.txt, b.txt"
	if !strings.Contains(body.Error, want) {
		t.Errorf("Error = %q, want it to contain %q", body.Error, want)
	}
}

// Two read-write workspaces drifting in one attempt: one sentence each, in
// the order the step's mounts declare them, joined with "; ".
func TestCheckGeneratedReportsEveryDriftedWorkspace(t *testing.T) {
	apiWS := senro.Workspace("api", senro.Scope(senro.ScopeRun))
	docs := senro.Workspace("docs", senro.Scope(senro.ScopeRun))
	pipe := senro.New("ci")
	l := pipe.Workflow("main")
	// No WorkDir: the local executor runs in the sandbox root, where each
	// mount is reachable at its At path, leading slash stripped.
	l.Step("codegen", exec.Command("sh", "-c", "echo a > api/client.go; echo d > docs/ref.md")).
		Mount(apiWS.At("/api", senro.RW), docs.At("/docs", senro.RW)).CheckGenerated()
	p, err := pipe.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	status, events, _, _ := runWithStorage(t, p)
	if status != api.RunFailed {
		t.Fatalf("status = %s, want failed", status)
	}
	var body api.StepFinishedBody
	for _, e := range events {
		if e.Type == api.StepFinished && e.Step == "codegen" {
			_ = e.Decode(&body)
		}
	}
	want := "generated files are out of date in workspace api: 1 changed: client.go; " +
		"generated files are out of date in workspace docs: 1 changed: ref.md"
	if body.Error != want {
		t.Errorf("Error =\n %q\nwant\n %q", body.Error, want)
	}
}
