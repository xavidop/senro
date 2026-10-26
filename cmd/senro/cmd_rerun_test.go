package main

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/xavidop/senro"
	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/exec"
	"github.com/xavidop/senro/testreport"
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

// seedTestsRun records a run whose step "s" declares a test selection of
// "--only {tests}" and whose command writes the argv it was given to a file,
// so a rerun's narrowed command can be read back.
func seedTestsRun(t *testing.T, root string) (runDir, out string) {
	t.Helper()
	out = filepath.Join(root, "out")
	runDir = filepath.Join(root, "run")
	pipe := senro.New("ci")
	l := pipe.Workflow("main")
	l.Step("s", exec.Command("sh", "-c", `echo "$@" > `+out, "sh")).
		Tests(testreport.Spec{Format: testreport.FormatTAP, Select: []string{"--only", testreport.Placeholder}})
	l.Step("plain", exec.Command("true"))
	l.Step("nosel", exec.Command("true")).Tests(testreport.Spec{Format: testreport.FormatTAP})
	if err := senro.Run(context.Background(), pipe, senro.WithDir(runDir)); err != nil {
		t.Fatalf("seeding a run: %v", err)
	}
	if err := os.Remove(out); err != nil {
		t.Fatal(err)
	}
	return runDir, out
}

func appendFailed(t *testing.T, runDir, name string, seq int) {
	t.Helper()
	appendTest(t, runDir, "s", name, api.TestFailed, seq)
}

// appendTest records one test.finished for step in the run's ledger, so a
// rerun's --test has a recorded test to resolve against.
func appendTest(t *testing.T, runDir, step, name string, outcome api.TestOutcome, seq int) {
	t.Helper()
	f, err := os.OpenFile(filepath.Join(runDir, "events.jsonl"), os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	line := fmt.Sprintf(`{"v":1,"seq":%d,"ts":"2026-08-07T10:00:02Z","type":"test.finished","run":"r","step":%q,"attempt":1,"payload":{"name":%q,"suite":"pkg","outcome":%q,"duration_ns":1}}`+"\n",
		seq, step, name, outcome)
	if _, err := f.WriteString(line); err != nil {
		t.Fatal(err)
	}
}

// seedGoTestRun is seedTestsRun for a step declaring testreport.GoTestJSON(),
// whose -run selection is where name shaping matters.
func seedGoTestRun(t *testing.T, root string) (runDir, out string) {
	t.Helper()
	out = filepath.Join(root, "out")
	runDir = filepath.Join(root, "run")
	pipe := senro.New("ci")
	pipe.Workflow("main").Step("g", exec.Command("sh", "-c", `echo "$@" > `+out, "sh")).
		Tests(testreport.GoTestJSON())
	if err := senro.Run(context.Background(), pipe, senro.WithDir(runDir)); err != nil {
		t.Fatalf("seeding a run: %v", err)
	}
	if err := os.Remove(out); err != nil {
		t.Fatal(err)
	}
	return runDir, out
}

// rerunArgv runs cmdRerun and returns the argv the rerun's step was given.
func rerunArgv(t *testing.T, out string, args ...string) string {
	t.Helper()
	var stdout, stderr bytes.Buffer
	if code := cmdRerun(args, &stdout, &stderr); code != exitSuccess {
		t.Fatalf("exit = %d; stderr=%s", code, stderr.String())
	}
	b, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	return strings.TrimSpace(string(b))
}

// A name the run never recorded would select nothing, and a test run that
// runs nothing passes: refused before any run starts, naming both.
func TestRerunTestRefusesANameTheRunDidNotRecord(t *testing.T) {
	root := t.TempDir()
	runDir, _ := seedTestsRun(t, root)
	appendTest(t, runDir, "s", "TestA", api.TestPassed, 900)
	again := filepath.Join(root, "again")
	var stdout, stderr bytes.Buffer
	code := cmdRerun([]string{"--run", runDir, "--step", "s", "--test", "TestNope", "--dir", again}, &stdout, &stderr)
	if code != exitUsage {
		t.Fatalf("exit = %d, want %d (usage); stderr = %q", code, exitUsage, stderr.String())
	}
	if !strings.Contains(stderr.String(), `step "s" recorded no test "TestNope"`) {
		t.Errorf("stderr %q must name the step and the test", stderr.String())
	}
	if _, err := os.Stat(again); err == nil {
		t.Error("a run directory was created for a refused selection")
	}
}

// suite.name is how every client prints a test, so a name copied from one
// resolves to the bare name the Select takes.
func TestRerunTestAcceptsSuiteDotName(t *testing.T) {
	root := t.TempDir()
	runDir, out := seedTestsRun(t, root)
	appendTest(t, runDir, "s", "TestA", api.TestPassed, 900)
	got := rerunArgv(t, out, "--run", runDir, "--step", "s", "--test", "pkg.TestA",
		"--dir", filepath.Join(root, "again"))
	if got != "--only TestA" {
		t.Errorf("the step ran with %q, want --only TestA", got)
	}
}

// A failed subtest on a go test step reruns its parent: -run splits on "/",
// so the subtest's own name inside the group would select nothing.
func TestRerunFailedSubtestRerunsItsParent(t *testing.T) {
	root := t.TempDir()
	runDir, out := seedGoTestRun(t, root)
	appendTest(t, runDir, "g", "TestP/sub (x)", api.TestFailed, 900)
	got := rerunArgv(t, out, "--run", runDir, "--step", "g", "--failed-tests",
		"--dir", filepath.Join(root, "again"))
	if got != "-run ^(TestP)$" {
		t.Errorf("the step ran with %q, want -run ^(TestP)$", got)
	}
}

// A regexp metacharacter in a go test name is quoted, so the name selects
// itself and not whatever the pattern it spells would match.
func TestRerunGoTestNameIsRegexpQuoted(t *testing.T) {
	root := t.TempDir()
	runDir, out := seedGoTestRun(t, root)
	appendTest(t, runDir, "g", "TestA+B", api.TestPassed, 900)
	got := rerunArgv(t, out, "--run", runDir, "--step", "g", "--test", "TestA+B",
		"--dir", filepath.Join(root, "again"))
	if got != `-run ^(TestA\+B)$` {
		t.Errorf("the step ran with %q, want -run ^(TestA\\+B)$", got)
	}
}

func TestRerunTestRequiresStep(t *testing.T) {
	runDir, _ := seedTestsRun(t, t.TempDir())
	var stdout, stderr bytes.Buffer
	code := cmdRerun([]string{"--run", runDir, "--test", "TestA"}, &stdout, &stderr)
	if code != exitUsage || !strings.Contains(stderr.String(), "--step") {
		t.Fatalf("exit = %d, stderr = %q", code, stderr.String())
	}
}

func TestRerunTestRefusesAStepThatCannotSelect(t *testing.T) {
	runDir, _ := seedTestsRun(t, t.TempDir())
	var stdout, stderr bytes.Buffer
	code := cmdRerun([]string{"--run", runDir, "--step", "plain", "--test", "TestA"}, &stdout, &stderr)
	if code != exitUsage || !strings.Contains(stderr.String(), `"plain"`) {
		t.Fatalf("exit = %d, stderr = %q", code, stderr.String())
	}
}

func TestRerunTestNarrowsTheCommand(t *testing.T) {
	root := t.TempDir()
	runDir, out := seedTestsRun(t, root)
	appendTest(t, runDir, "s", "TestA", api.TestPassed, 900)
	appendTest(t, runDir, "s", "TestB", api.TestPassed, 901)
	var stdout, stderr bytes.Buffer
	code := cmdRerun([]string{"--run", runDir, "--step", "s", "--test", "TestA", "--test", "TestB",
		"--dir", filepath.Join(root, "again")}, &stdout, &stderr)
	if code != exitSuccess {
		t.Fatalf("exit = %d; stderr=%s", code, stderr.String())
	}
	b, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), "--only TestA|TestB") {
		t.Errorf("the step ran with %q", b)
	}
}

func TestRerunFailedTestsReadsTheLedger(t *testing.T) {
	root := t.TempDir()
	runDir, out := seedTestsRun(t, root)
	appendFailed(t, runDir, "TestB", 900)
	appendFailed(t, runDir, "TestA", 901)
	var stdout, stderr bytes.Buffer
	code := cmdRerun([]string{"--run", runDir, "--step", "s", "--failed-tests",
		"--dir", filepath.Join(root, "again")}, &stdout, &stderr)
	if code != exitSuccess {
		t.Fatalf("exit = %d; stderr=%s", code, stderr.String())
	}
	b, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), "--only TestA|TestB") {
		t.Errorf("the step ran with %q", b)
	}
}

func TestRerunFailedTestsWithNoneFailedIsAUsageError(t *testing.T) {
	runDir, _ := seedTestsRun(t, t.TempDir())
	var stdout, stderr bytes.Buffer
	code := cmdRerun([]string{"--run", runDir, "--step", "s", "--failed-tests"}, &stdout, &stderr)
	if code != exitUsage || !strings.Contains(stderr.String(), `no test on step "s" failed`) {
		t.Fatalf("exit = %d, stderr = %q", code, stderr.String())
	}
}

// A step that declares Tests but no Select is refused before any run starts:
// the run directory --dir names must not exist afterwards.
func TestRerunTestRefusesAStepWithTestsButNoSelect(t *testing.T) {
	root := t.TempDir()
	runDir, _ := seedTestsRun(t, root)
	again := filepath.Join(root, "again")
	var stdout, stderr bytes.Buffer
	code := cmdRerun([]string{"--run", runDir, "--step", "nosel", "--test", "X", "--dir", again}, &stdout, &stderr)
	if code != exitUsage || !strings.Contains(stderr.String(), `"nosel"`) {
		t.Fatalf("exit = %d, stderr = %q", code, stderr.String())
	}
	if _, err := os.Stat(again); err == nil {
		t.Error("a run directory was created for a refused selection")
	}
}
