package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/xavidop/senro"
	"github.com/xavidop/senro/exec"
)

// seedRerunnableRun produces a run directory with a recorded plan.json and
// two steps, the second depending on the first, each touching a marker so a
// re-run can be told apart from no run at all.
func seedRerunnableRun(t *testing.T, root string) (runDir, first, second string) {
	t.Helper()
	first = filepath.Join(root, "first")
	second = filepath.Join(root, "second")
	runDir = filepath.Join(root, "run")

	pipe := senro.New("ci")
	l := pipe.Workflow("main")
	l.Step("a", exec.Command("touch", first))
	l.Step("b", exec.Command("touch", second)).Needs("a")

	if err := senro.Run(context.Background(), pipe, senro.WithDir(runDir)); err != nil {
		t.Fatalf("seeding a run: %v", err)
	}
	for _, p := range []string{first, second} {
		if err := os.Remove(p); err != nil {
			t.Fatalf("clearing %s: %v", p, err)
		}
	}
	return runDir, first, second
}

// A re-run executes the plan the run RECORDED, without the pipeline package
// and without a Go toolchain: plan.json is the record of what the steps were
// meant to run.
func TestRerunExecutesTheRecordedPlan(t *testing.T) {
	root := t.TempDir()
	runDir, first, second := seedRerunnableRun(t, root)

	var stdout, stderr bytes.Buffer
	code := cmdRerun([]string{"--run", runDir, "--dir", filepath.Join(root, "again")}, &stdout, &stderr)
	if code != exitSuccess {
		t.Fatalf("exit = %d, want %d; stderr=%s", code, exitSuccess, stderr.String())
	}
	for _, p := range []string{first, second} {
		if _, err := os.Stat(p); err != nil {
			t.Errorf("a recorded step did not re-run: %v", err)
		}
	}
}

// --step selects the step, what it needs, and everything below it. Its
// dependencies are in the set because a step cannot run without its inputs:
// leaving them out settles them as skipped, and a dependent of a skipped step
// is skipped too, so the one step asked for would be the one that did not run.
func TestRerunStepRunsThatStepWithItsNeedsAndDependents(t *testing.T) {
	root := t.TempDir()
	first := filepath.Join(root, "first")
	second := filepath.Join(root, "second")
	unrelated := filepath.Join(root, "unrelated")
	runDir := filepath.Join(root, "run")

	pipe := senro.New("ci")
	l := pipe.Workflow("main")
	l.Step("a", exec.Command("touch", first))
	l.Step("b", exec.Command("touch", second)).Needs("a")
	l.Step("c", exec.Command("touch", unrelated))

	if err := senro.Run(context.Background(), pipe, senro.WithDir(runDir)); err != nil {
		t.Fatalf("seeding a run: %v", err)
	}
	for _, p := range []string{first, second, unrelated} {
		if err := os.Remove(p); err != nil {
			t.Fatalf("clearing %s: %v", p, err)
		}
	}

	var stdout, stderr bytes.Buffer
	code := cmdRerun([]string{"--run", runDir, "--step", "b", "--dir", filepath.Join(root, "again")},
		&stdout, &stderr)
	if code != exitSuccess {
		t.Fatalf("exit = %d, want %d; stderr=%s", code, exitSuccess, stderr.String())
	}
	if _, err := os.Stat(second); err != nil {
		t.Errorf("the selected step did not run: %v", err)
	}
	if _, err := os.Stat(first); err != nil {
		t.Errorf("the step it needs did not run, so it could not have had its inputs: %v", err)
	}
	if _, err := os.Stat(unrelated); err == nil {
		t.Error("a branch unrelated to the selected step ran anyway")
	}
}

// A step the recorded plan never had is a usage error, named, rather than a
// run that quietly does nothing.
func TestRerunRejectsAStepTheRecordedPlanDoesNotHave(t *testing.T) {
	root := t.TempDir()
	runDir, _, _ := seedRerunnableRun(t, root)

	var stdout, stderr bytes.Buffer
	code := cmdRerun([]string{"--run", runDir, "--step", "nope"}, &stdout, &stderr)
	if code != exitUsage {
		t.Fatalf("exit = %d, want %d (usage)", code, exitUsage)
	}
	if !bytes.Contains(stderr.Bytes(), []byte("nope")) {
		t.Errorf("stderr %q must name the step it could not find", stderr.String())
	}
}
