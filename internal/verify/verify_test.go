package verify_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/xavidop/senro"
	"github.com/xavidop/senro/artifact"
	"github.com/xavidop/senro/exec"
	"github.com/xavidop/senro/internal/cache"
	"github.com/xavidop/senro/internal/cas"
	"github.com/xavidop/senro/internal/engine"
	"github.com/xavidop/senro/internal/executor/localexec"
	"github.com/xavidop/senro/internal/plan"
	"github.com/xavidop/senro/internal/sink"
	"github.com/xavidop/senro/internal/storage"
	"github.com/xavidop/senro/internal/verify"
)

// fixture is one run against one cache root, plus everything a Check needs.
// A real engine run rather than a hand-assembled cache directory: a fixture
// that wrote the entry itself would test this package against its own
// assumptions.
type fixture struct {
	cacheRoot string
	runDir    string
	store     *storage.Storage
}

// runOnce runs p once and returns the fixture.
//
// maxParallel is explicit because siblings sharing a ScopeRun workspace are
// racy by design (see internal/engine's wsManager): a test that needs two
// steps unordered IN THE GRAPH does not need them executing simultaneously,
// and asking for that would make it flake on a loaded machine.
func runOnce(t *testing.T, p *senro.Plan, cacheRoot, runID string, maxParallel int) *fixture {
	t.Helper()
	store, err := storage.Open(cacheRoot)
	if err != nil {
		t.Fatalf("storage.Open: %v", err)
	}
	runDir := filepath.Join(t.TempDir(), "run")
	if _, err := engine.Run(context.Background(), p, engine.Options{
		Dir:         runDir,
		Executor:    localexec.New(runDir, store.Snapshotter),
		Sink:        sink.Recording(),
		Storage:     store,
		RunID:       runID,
		MaxParallel: maxParallel,
	}); err != nil {
		t.Fatalf("engine.Run: %v", err)
	}
	return &fixture{cacheRoot: cacheRoot, runDir: runDir, store: store}
}

// check runs a verification pass over f, with execution and classification on
// unless the caller says otherwise.
func (f *fixture) check(t *testing.T, mutate func(*verify.Options)) verify.Report {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(f.runDir, "plan.json"))
	if err != nil {
		t.Fatalf("read plan.json: %v", err)
	}
	p, err := plan.Unmarshal(b)
	if err != nil {
		t.Fatalf("plan.Unmarshal: %v", err)
	}
	recs, err := cache.ReadRecords(filepath.Join(f.runDir, "cache"))
	if err != nil {
		t.Fatalf("cache.ReadRecords: %v", err)
	}
	opts := verify.Options{
		Plan: p, Records: recs, Storage: f.store,
		WorkRoot: filepath.Join(t.TempDir(), "verify"),
		Execute:  true, Classify: true,
	}
	if mutate != nil {
		mutate(&opts)
	}
	rep, err := verify.Check(context.Background(), opts)
	if err != nil {
		t.Fatalf("verify.Check: %v", err)
	}
	return rep
}

// stepResult finds one step's result, failing the test rather than returning
// a zero value a later assertion would misread as a verdict.
func stepResult(t *testing.T, rep verify.Report, id string) verify.Step {
	t.Helper()
	for _, s := range rep.Steps {
		if s.Step == id {
			return s
		}
	}
	t.Fatalf("no result for step %q in %v", id, summarize(rep))
	return verify.Step{}
}

func summarize(rep verify.Report) string {
	var b strings.Builder
	for _, s := range rep.Steps {
		fmt.Fprintf(&b, "%s=%s(%s) ", s.Step, s.Verdict, s.Reason)
	}
	return b.String()
}

// pipelineReading builds a pipeline whose Pure() step reads a file OUTSIDE
// its workspace: an undeclared file inside the mount still moves the key,
// but an absolute path outside it is invisible to every key component.
func pipelineReading(t *testing.T, outside string) *senro.Plan {
	t.Helper()
	ws := senro.Workspace("src", senro.Scope(senro.ScopeRun))
	pipe := senro.New("ci")
	w := pipe.Workflow("main")
	w.Step("seed", exec.Command("sh", "-c", "printf 'seed\\n' > seed.txt")).
		WorkDir("/src").Mount(ws.At("/src", senro.RW))
	w.Step("build", exec.Command("sh", "-c", "cat "+outside+" > out.txt")).
		Needs("seed").WorkDir("/src").Mount(ws.At("/src", senro.RW)).
		Pure().Inputs(artifact.Glob("seed.txt")).Outputs(artifact.File("out.txt"))
	p, err := pipe.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	return p
}

// TestAStepThatDependsOnSomethingOutsideItsKeyIsCaught is the test this
// package exists for: a step declared Pure() that reads a file no key
// component covers, caught by name with both digests.
func TestAStepThatDependsOnSomethingOutsideItsKeyIsCaught(t *testing.T) {
	outsideDir := t.TempDir()
	outside := filepath.Join(outsideDir, "dependency.txt")
	if err := os.WriteFile(outside, []byte("version one\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	f := runOnce(t, pipelineReading(t, outside), filepath.Join(t.TempDir(), "cache"), "r1", 1)

	// The world moves under the entry, as a registry or a clock would.
	if err := os.WriteFile(outside, []byte("version two\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	rep := f.check(t, nil)
	s := stepResult(t, rep, "build")

	if s.Verdict != verify.Mismatch {
		t.Fatalf("a step that reads a file outside its key should be a mismatch, got %q (%s)",
			s.Verdict, s.Reason)
	}
	if rep.Findings() != 1 {
		t.Errorf("Findings() = %d, want 1", rep.Findings())
	}

	// What must be there: which output, what the cache holds, what a re-run
	// produced.
	var out *verify.Difference
	for i := range s.Differences {
		if s.Differences[i].Kind == "output" && s.Differences[i].Name == "out.txt" {
			out = &s.Differences[i]
		}
	}
	if out == nil {
		t.Fatalf("no difference named the declared output out.txt: %+v", s.Differences)
	}
	if out.Cached == out.Rerun || out.Cached == "" || out.Rerun == "" {
		t.Errorf("the output difference must carry both digests, got cached=%q rerun=%q",
			out.Cached, out.Rerun)
	}
	if out.RerunAgain != out.Rerun {
		t.Errorf("the second re-run agreed with the first, so its value must be reported as such: "+
			"rerun=%q again=%q", out.Rerun, out.RerunAgain)
	}
	// The step's own declaration is what a reader needs next to the finding.
	if len(s.Inputs) == 0 || len(s.Outputs) == 0 {
		t.Errorf("a finding must name the step's declared Inputs and Outputs, got %v / %v",
			s.Inputs, s.Outputs)
	}
	if s.Hermeticity != cache.HermeticityTrusted {
		t.Errorf("the entry's hermeticity should be reported as %q, got %q",
			cache.HermeticityTrusted, s.Hermeticity)
	}
}

// TestAStepThatProducesDifferentBytesEveryRunIsNotReportedAsImpure is the
// false-alarm half: an output embedding something that changes every
// execution disagrees with its entry too, and is not a mismatch. Still not
// a pass.
func TestAStepThatProducesDifferentBytesEveryRunIsNotReportedAsImpure(t *testing.T) {
	counter := filepath.Join(t.TempDir(), "counter")
	if err := os.WriteFile(counter, []byte("0\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	// A counter outside the workspace written into the declared output: the
	// stand-in for an archive embedding a build timestamp.
	cmd := fmt.Sprintf("n=$(cat %s); echo $((n+1)) > %s; echo $n > out.txt", counter, counter)

	ws := senro.Workspace("src", senro.Scope(senro.ScopeRun))
	pipe := senro.New("ci")
	w := pipe.Workflow("main")
	w.Step("seed", exec.Command("sh", "-c", "printf 'seed\\n' > seed.txt")).
		WorkDir("/src").Mount(ws.At("/src", senro.RW))
	w.Step("stamp", exec.Command("sh", "-c", cmd)).
		Needs("seed").WorkDir("/src").Mount(ws.At("/src", senro.RW)).
		Pure().Inputs(artifact.Glob("seed.txt")).Outputs(artifact.File("out.txt"))
	p, err := pipe.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	f := runOnce(t, p, filepath.Join(t.TempDir(), "cache"), "r1", 1)
	s := stepResult(t, f.check(t, nil), "stamp")

	if s.Verdict != verify.NonDeterministic {
		t.Fatalf("a step whose output differs from itself on two re-runs is not evidence of impurity; "+
			"want %q, got %q (%s) %+v", verify.NonDeterministic, s.Verdict, s.Reason, s.Differences)
	}
	if s.Verdict == verify.Verified {
		t.Error("a step nobody can reproduce must not read as verified")
	}
	// Both rounds have to be visible, or the verdict is taken on faith.
	for _, d := range s.Differences {
		if d.Kind == "output" && d.RerunAgain == d.Rerun {
			t.Errorf("the two re-runs are recorded as agreeing, which is not what happened: %+v", d)
		}
	}
}

// TestWithoutClassificationANonDeterministicStepIsAnUnclassifiedMismatch
// pins the honest degradation: with the second round declined, the command
// reports what it can see and does not invent the distinction.
func TestWithoutClassificationANonDeterministicStepIsAnUnclassifiedMismatch(t *testing.T) {
	counter := filepath.Join(t.TempDir(), "counter")
	if err := os.WriteFile(counter, []byte("0\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	cmd := fmt.Sprintf("n=$(cat %s); echo $((n+1)) > %s; echo $n > out.txt", counter, counter)
	ws := senro.Workspace("src", senro.Scope(senro.ScopeRun))
	pipe := senro.New("ci")
	w := pipe.Workflow("main")
	w.Step("seed", exec.Command("sh", "-c", "printf 'seed\\n' > seed.txt")).
		WorkDir("/src").Mount(ws.At("/src", senro.RW))
	w.Step("stamp", exec.Command("sh", "-c", cmd)).
		Needs("seed").WorkDir("/src").Mount(ws.At("/src", senro.RW)).
		Pure().Inputs(artifact.Glob("seed.txt")).Outputs(artifact.File("out.txt"))
	p, err := pipe.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	f := runOnce(t, p, filepath.Join(t.TempDir(), "cache"), "r1", 1)
	s := stepResult(t, f.check(t, func(o *verify.Options) { o.Classify = false }), "stamp")

	if s.Verdict != verify.Mismatch {
		t.Fatalf("want %q with classification off, got %q", verify.Mismatch, s.Verdict)
	}
	for _, d := range s.Differences {
		if d.RerunAgain != "" {
			t.Errorf("no second round was run, so nothing may be reported for one: %+v", d)
		}
	}
}

// TestAConcurrentSiblingSharingAWorkspaceIsNotAFinding: a ScopeRun
// workspace is one directory shared by every step that mounts it, so a
// step's snapshot contains an unordered sibling's writes while an isolated
// re-run has no siblings. Reporting that as a mismatch would flag an honest
// step in almost every pipeline with any parallelism.
func TestAConcurrentSiblingSharingAWorkspaceIsNotAFinding(t *testing.T) {
	ws := senro.Workspace("src", senro.Scope(senro.ScopeRun))
	pipe := senro.New("ci")
	w := pipe.Workflow("main")
	w.Step("seed", exec.Command("sh", "-c", "printf 'seed\\n' > seed.txt")).
		WorkDir("/src").Mount(ws.At("/src", senro.RW))
	// Two Pure() steps unordered in the graph: that freedom is the guard's
	// whole input. The run is still serialized (maxParallel 1) so the test
	// cannot fail on the unrelated shared-directory race.
	for _, id := range []string{"alpha", "beta"} {
		w.Step(id, exec.Command("sh", "-c", "wc -c < seed.txt > "+id+".txt")).
			Needs("seed").WorkDir("/src").Mount(ws.At("/src", senro.RW)).
			Pure().Inputs(artifact.Glob("seed.txt")).Outputs(artifact.File(id + ".txt"))
	}
	p, err := pipe.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	f := runOnce(t, p, filepath.Join(t.TempDir(), "cache"), "r1", 1)
	rep := f.check(t, nil)

	for _, id := range []string{"alpha", "beta"} {
		s := stepResult(t, rep, id)
		if s.Verdict != verify.Verified {
			t.Fatalf("%s: want %q, got %q with %+v", id, verify.Verified, s.Verdict, s.Differences)
		}
		// Declining to compare must be stated, never silent.
		if len(s.NotCompared) == 0 {
			t.Errorf("%s: the workspace comparison was dropped without saying so", id)
		}
		joined := strings.Join(s.NotCompared, " ")
		if !strings.Contains(joined, "src") || !strings.Contains(joined, "unordered") {
			t.Errorf("%s: the note should name the workspace and why: %q", id, joined)
		}
	}
	if rep.Findings() != 0 {
		t.Errorf("Findings() = %d, want 0: %s", rep.Findings(), summarize(rep))
	}
}

// TestAnOrderedSiblingStillHasItsWorkspaceCompared is the mutation check:
// dropping the workspace comparison whenever ANY sibling mounts it would
// throw away the only signal a Pure() step with no declared Outputs has.
func TestAnOrderedSiblingStillHasItsWorkspaceCompared(t *testing.T) {
	counter := filepath.Join(t.TempDir(), "counter")
	if err := os.WriteFile(counter, []byte("0\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	ws := senro.Workspace("src", senro.Scope(senro.ScopeRun))
	pipe := senro.New("ci")
	w := pipe.Workflow("main")
	w.Step("seed", exec.Command("sh", "-c", "printf 'seed\\n' > seed.txt")).
		WorkDir("/src").Mount(ws.At("/src", senro.RW))
	// Ordered after seed, same workspace, no declared Outputs: the workspace
	// digest is the only comparison. Its undeclared file changes every run.
	cmd := "n=$(cat " + counter + "); echo $((n+1)) > " + counter + "; echo $n > side.txt"
	w.Step("side", exec.Command("sh", "-c", cmd)).
		Needs("seed").WorkDir("/src").Mount(ws.At("/src", senro.RW)).
		Pure().Inputs(artifact.Glob("seed.txt"))
	p, err := pipe.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	f := runOnce(t, p, filepath.Join(t.TempDir(), "cache"), "r1", 1)
	s := stepResult(t, f.check(t, nil), "side")

	if s.Verdict == verify.Verified {
		t.Fatalf("the workspace comparison was dropped for a step that has no other signal: %+v", s)
	}
	if len(s.NotCompared) != 0 {
		t.Errorf("nothing should have been declined for an unambiguously ordered step: %v", s.NotCompared)
	}
	var sawWorkspace bool
	for _, d := range s.Differences {
		if d.Kind == "workspace" {
			sawWorkspace = true
		}
	}
	if !sawWorkspace {
		t.Errorf("the workspace difference is the whole finding here: %+v", s.Differences)
	}
}

// TestAGenuinelyPureStepVerifies is the clean case: a tool that flags
// everything is a tool nobody runs.
func TestAGenuinelyPureStepVerifies(t *testing.T) {
	ws := senro.Workspace("src", senro.Scope(senro.ScopeRun))
	pipe := senro.New("ci")
	w := pipe.Workflow("main")
	w.Step("seed", exec.Command("sh", "-c", "printf 'senro\\n' > greeting.txt")).
		WorkDir("/src").Mount(ws.At("/src", senro.RW))
	w.Step("measure", exec.Command("sh", "-c", "wc -c < greeting.txt > greeting.size")).
		Needs("seed").WorkDir("/src").Mount(ws.At("/src", senro.RW)).
		Pure().Inputs(artifact.Glob("greeting.txt")).Outputs(artifact.File("greeting.size"))
	p, err := pipe.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	f := runOnce(t, p, filepath.Join(t.TempDir(), "cache"), "r1", 1)
	rep := f.check(t, nil)
	s := stepResult(t, rep, "measure")

	if s.Verdict != verify.Verified {
		t.Fatalf("want %q, got %q (%s) %+v", verify.Verified, s.Verdict, s.Reason, s.Differences)
	}
	if len(s.Differences) != 0 {
		t.Errorf("a verified step has nothing to report: %+v", s.Differences)
	}
	if rep.Findings() != 0 {
		t.Errorf("Findings() = %d, want 0", rep.Findings())
	}
	if !rep.Executed || rep.Checked != 1 {
		t.Errorf("Executed=%v Checked=%d, want true/1", rep.Executed, rep.Checked)
	}
}

// TestNothingRunsWithoutExplicitConsent: the premise is that Pure() may be
// false, so the command cannot help itself to the claim's safety corollary
// either. Not executing is the default.
func TestNothingRunsWithoutExplicitConsent(t *testing.T) {
	sentinel := filepath.Join(t.TempDir(), "deployed")
	ws := senro.Workspace("src", senro.Scope(senro.ScopeRun))
	pipe := senro.New("ci")
	w := pipe.Workflow("main")
	w.Step("seed", exec.Command("sh", "-c", "printf 'seed\\n' > seed.txt")).
		WorkDir("/src").Mount(ws.At("/src", senro.RW))
	w.Step("deploy", exec.Command("sh", "-c", "echo shipped >> "+sentinel+"; echo ok > out.txt")).
		Needs("seed").WorkDir("/src").Mount(ws.At("/src", senro.RW)).
		Pure().Inputs(artifact.Glob("seed.txt")).Outputs(artifact.File("out.txt"))
	p, err := pipe.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	f := runOnce(t, p, filepath.Join(t.TempDir(), "cache"), "r1", 1)
	before, err := os.ReadFile(sentinel)
	if err != nil {
		t.Fatalf("the run itself should have written the sentinel: %v", err)
	}

	rep := f.check(t, func(o *verify.Options) { o.Execute = false })
	s := stepResult(t, rep, "deploy")

	if s.Verdict != verify.Planned {
		t.Fatalf("want %q without consent, got %q", verify.Planned, s.Verdict)
	}
	if rep.Executed {
		t.Error("Report.Executed must be false when nothing was executed")
	}
	after, err := os.ReadFile(sentinel)
	if err != nil {
		t.Fatal(err)
	}
	if string(after) != string(before) {
		t.Fatalf("the step was executed without consent: sentinel went from %q to %q", before, after)
	}
}

// TestCheckNeverWritesTheActionCache: saving over the entry being compared
// would make the second invocation pass silently, with the evidence gone.
// Byte-for-byte over the entries tree, on a pass that finds a mismatch.
func TestCheckNeverWritesTheActionCache(t *testing.T) {
	outside := filepath.Join(t.TempDir(), "dependency.txt")
	if err := os.WriteFile(outside, []byte("version one\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	f := runOnce(t, pipelineReading(t, outside), filepath.Join(t.TempDir(), "cache"), "r1", 1)
	if err := os.WriteFile(outside, []byte("version two\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	actionDir := filepath.Join(f.cacheRoot, "action")
	before := treeDigest(t, actionDir)

	rep := f.check(t, nil)
	if rep.Findings() != 1 {
		t.Fatalf("this test needs the mismatch case to actually happen: %s", summarize(rep))
	}

	if after := treeDigest(t, actionDir); after != before {
		t.Fatalf("the action cache changed under a verification pass: %s became %s", before, after)
	}

	// The entry still says what it said: a second pass finds the same
	// mismatch.
	second := f.check(t, nil)
	if s := stepResult(t, second, "build"); s.Verdict != verify.Mismatch {
		t.Fatalf("the second pass lost the finding: %q (%s)", s.Verdict, s.Reason)
	}
}

// TestAStepDeclaringSecretsIsSkippedRatherThanRunWithout: the values live
// in the struct the binary handed senro.WithSecrets, not in the run
// directory, so a re-run without them would be a different step.
func TestAStepDeclaringSecretsIsSkippedRatherThanRunWithout(t *testing.T) {
	n := &plan.Node{
		ID: "publish", Kind: "exec", Pure: true,
		Cmd:     []string{"sh", "-c", "true"},
		Secrets: []plan.SecretSpec{{Name: "Token"}},
	}
	if got := verifyRefusal(t, n); !strings.Contains(got, "secrets") {
		t.Fatalf("a step declaring secrets must be refused by name, got %q", got)
	}
}

// TestAStepOnANonLocalExecutorIsSkipped keeps the blast radius local:
// verifying a container or k8s step would pull an image or create a pod.
func TestAStepOnANonLocalExecutorIsSkipped(t *testing.T) {
	n := &plan.Node{
		ID: "build", Kind: "exec", Pure: true,
		Cmd:      []string{"sh", "-c", "true"},
		Executor: &plan.ExecutorSpec{Kind: plan.ExecutorContainer, Image: "node:22"},
	}
	if got := verifyRefusal(t, n); !strings.Contains(got, "container") {
		t.Fatalf("a container step must be refused by name, got %q", got)
	}
}

// verifyRefusal drives one node through a whole Check and returns the skip
// reason, exercising the refusal through the real entry point.
func verifyRefusal(t *testing.T, n *plan.Node) string {
	t.Helper()
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{*n}}
	store, err := storage.Open(filepath.Join(t.TempDir(), "cache"))
	if err != nil {
		return err.Error()
	}
	rep, err := verify.Check(context.Background(), verify.Options{
		Plan:    p,
		Records: []cache.Record{{Step: n.ID, Digest: cas.Digest("sha256:" + strings.Repeat("0", 64))}},
		Storage: store, WorkRoot: filepath.Join(t.TempDir(), "verify"),
		Execute: true,
	})
	if err != nil {
		return err.Error()
	}
	if len(rep.Steps) != 1 {
		return "no result"
	}
	if rep.Steps[0].Verdict != verify.Skipped {
		return string(rep.Steps[0].Verdict)
	}
	return rep.Steps[0].Reason
}

// TestLimitBoundsTheCheckAndSaysSo: a bounded sample must never be mistaken
// for the whole set, which is what Truncated is for.
func TestLimitBoundsTheCheckAndSaysSo(t *testing.T) {
	ws := senro.Workspace("src", senro.Scope(senro.ScopeRun))
	pipe := senro.New("ci")
	w := pipe.Workflow("main")
	w.Step("seed", exec.Command("sh", "-c", "printf 'seed\\n' > seed.txt")).
		WorkDir("/src").Mount(ws.At("/src", senro.RW))
	for _, id := range []string{"a", "b", "c"} {
		w.Step(id, exec.Command("sh", "-c", "cp seed.txt "+id+".txt")).
			Needs("seed").WorkDir("/src").Mount(ws.At("/src", senro.RW)).
			Pure().Inputs(artifact.Glob("seed.txt")).Outputs(artifact.File(id + ".txt"))
	}
	p, err := pipe.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	f := runOnce(t, p, filepath.Join(t.TempDir(), "cache"), "r1", 1)

	rep := f.check(t, func(o *verify.Options) { o.Limit = 2; o.Execute = false })
	if rep.Checked != 2 || rep.Truncated != 1 {
		t.Fatalf("Checked=%d Truncated=%d, want 2/1: %s", rep.Checked, rep.Truncated, summarize(rep))
	}
	// Plan order, not alphabetical.
	if rep.Steps[0].Step != "a" || rep.Steps[1].Step != "b" {
		t.Errorf("want the first two steps in plan order, got %s", summarize(rep))
	}
}

// TestOneStepCanBeNamed keeps the narrow invocation honest: naming a step
// checks that step and nothing else.
func TestOneStepCanBeNamed(t *testing.T) {
	ws := senro.Workspace("src", senro.Scope(senro.ScopeRun))
	pipe := senro.New("ci")
	w := pipe.Workflow("main")
	w.Step("seed", exec.Command("sh", "-c", "printf 'seed\\n' > seed.txt")).
		WorkDir("/src").Mount(ws.At("/src", senro.RW))
	for _, id := range []string{"a", "b"} {
		w.Step(id, exec.Command("sh", "-c", "cp seed.txt "+id+".txt")).
			Needs("seed").WorkDir("/src").Mount(ws.At("/src", senro.RW)).
			Pure().Inputs(artifact.Glob("seed.txt")).Outputs(artifact.File(id + ".txt"))
	}
	p, err := pipe.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	f := runOnce(t, p, filepath.Join(t.TempDir(), "cache"), "r1", 1)

	rep := f.check(t, func(o *verify.Options) { o.Steps = []string{"b"} })
	if rep.Checked != 1 || rep.Steps[0].Step != "b" {
		t.Fatalf("want only step b, got %s", summarize(rep))
	}
}

// TestAStepWithNoWorkspaceIsSkippedRatherThanRunAgainstTheCheckout: its
// Inputs resolve against the directory the pipeline ran in, which cannot be
// reconstituted from a content address; running it anyway would mean
// running against the caller's checkout.
func TestAStepWithNoWorkspaceIsSkippedRatherThanRunAgainstTheCheckout(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "note.txt"), []byte("hi\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	cwd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chdir(dir); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chdir(cwd) })

	pipe := senro.New("ci")
	w := pipe.Workflow("main")
	w.Step("look", exec.Command("sh", "-c", "true")).
		Pure().Inputs(artifact.Glob("note.txt"))
	p, err := pipe.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	f := runOnce(t, p, filepath.Join(t.TempDir(), "cache"), "r1", 1)

	s := stepResult(t, f.check(t, nil), "look")
	if s.Verdict != verify.Skipped {
		t.Fatalf("want %q, got %q", verify.Skipped, s.Verdict)
	}
	if !strings.Contains(s.Reason, "checkout") {
		t.Errorf("the refusal must say why, got %q", s.Reason)
	}
}

// treeDigest is a content address for a whole directory tree: every path
// and byte in stable order, so "did not change at all" is assertable.
func treeDigest(t *testing.T, root string) string {
	t.Helper()
	h := sha256.New()
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(root, p)
		if err != nil {
			return err
		}
		_, _ = fmt.Fprintf(h, "%s\n", filepath.ToSlash(rel))
		if d.IsDir() {
			return nil
		}
		f, err := os.Open(p)
		if err != nil {
			return err
		}
		defer func() { _ = f.Close() }()
		_, err = io.Copy(h, f)
		return err
	})
	if err != nil {
		t.Fatalf("walk %s: %v", root, err)
	}
	return hex.EncodeToString(h.Sum(nil))
}
