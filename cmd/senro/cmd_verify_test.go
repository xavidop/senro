package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/xavidop/senro"
	"github.com/xavidop/senro/artifact"
	"github.com/xavidop/senro/exec"
)

// seedVerifiableRun runs a pipeline with one honest Pure() step and one
// that reads a file outside its workspace, which no cache key component
// covers. It returns the run directory and that file's path, so a test can
// move the world under the entry.
func seedVerifiableRun(t *testing.T, cacheRoot string) (runDir, outside string) {
	t.Helper()
	outside = filepath.Join(t.TempDir(), "dependency.txt")
	if err := os.WriteFile(outside, []byte("version one\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	ws := senro.Workspace("src", senro.Scope(senro.ScopeRun))
	pipe := senro.New("ci")
	w := pipe.Workflow("main")
	w.Step("seed", exec.Command("sh", "-c", "printf 'seed\\n' > seed.txt")).
		WorkDir("/src").Mount(ws.At("/src", senro.RW))
	w.Step("honest", exec.Command("sh", "-c", "wc -c < seed.txt > honest.txt")).
		Needs("seed").WorkDir("/src").Mount(ws.At("/src", senro.RW)).
		Pure().Inputs(artifact.Glob("seed.txt")).Outputs(artifact.File("honest.txt"))
	// Needs("honest"), not Needs("seed"): two siblings writing one shared
	// ScopeRun workspace are racy by design, and this fixture is about
	// verify's verdicts. internal/verify covers the unordered case.
	w.Step("fetch", exec.Command("sh", "-c", "cat "+outside+" > fetched.txt")).
		Needs("honest").WorkDir("/src").Mount(ws.At("/src", senro.RW)).
		Pure().Inputs(artifact.Glob("seed.txt")).Outputs(artifact.File("fetched.txt"))

	runDir = filepath.Join(t.TempDir(), "run")
	if err := senro.Run(context.Background(), pipe,
		senro.WithDir(runDir), senro.WithRunID("verify-run"), senro.WithCacheDir(cacheRoot)); err != nil {
		t.Fatalf("seedVerifiableRun: Run: %v", err)
	}
	return runDir, outside
}

// TestVerifyRequiresACheckToBeNamed: a bare `senro verify` must not pick a
// default, because the day a second check exists the same command line would
// silently start meaning something else.
func TestVerifyRequiresACheckToBeNamed(t *testing.T) {
	var out, errOut bytes.Buffer
	if code := cmdVerify(nil, &out, &errOut); code != exitUsage {
		t.Fatalf("exit = %d, want %d", code, exitUsage)
	}
	if !strings.Contains(errOut.String(), "--recheck-pure") {
		t.Errorf("the refusal should name the check that exists:\n%s", errOut.String())
	}
}

func TestVerifyRejectsUnknownFlagsAndBadLimits(t *testing.T) {
	for _, args := range [][]string{
		{"--recheck-pure", "--nope"},
		{"--recheck-pure", "--limit", "-1"},
		{"--recheck-pure", "--limit", "lots"},
	} {
		var out, errOut bytes.Buffer
		if code := cmdVerify(args, &out, &errOut); code != exitUsage {
			t.Errorf("%v: exit = %d, want %d", args, code, exitUsage)
		}
	}
}

// TestVerifyPlansWithoutRunningAnything is the safety default at the CLI
// boundary: no --rerun, nothing executed, and the report says so rather than
// leaving a reader to infer it from an absence.
func TestVerifyPlansWithoutRunningAnything(t *testing.T) {
	cacheRoot := t.TempDir()
	runDir, outside := seedVerifiableRun(t, cacheRoot)
	if err := os.WriteFile(outside, []byte("version two\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	// If the impure step were re-run, this is what it would read; the digest
	// it produced during the run above is the one the entry holds, and a plan
	// that quietly executed would find a mismatch. Finding none is the proof.
	var out, errOut bytes.Buffer
	code := cmdVerify([]string{"--recheck-pure", "--run", runDir, "--cache-dir", cacheRoot},
		&out, &errOut)
	if code != exitSuccess {
		t.Fatalf("exit = %d, stderr: %s", code, errOut.String())
	}
	if !strings.Contains(out.String(), "PLANNED") {
		t.Errorf("every step should be planned, not checked:\n%s", out.String())
	}
	if !strings.Contains(out.String(), "--rerun") {
		t.Errorf("the report must say how to actually run it:\n%s", out.String())
	}
	if strings.Contains(out.String(), "MISMATCH") {
		t.Errorf("nothing was supposed to run, so nothing could have mismatched:\n%s", out.String())
	}
}

// TestVerifyCatchesAnImpureStepAndStillExitsZero pins both halves at once:
// the finding is reported, and the exit code is 0, because a finding is an
// answer, exactly as `senro ws diff` treats a difference.
func TestVerifyCatchesAnImpureStepAndStillExitsZero(t *testing.T) {
	cacheRoot := t.TempDir()
	runDir, outside := seedVerifiableRun(t, cacheRoot)
	if err := os.WriteFile(outside, []byte("version two\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	var out, errOut bytes.Buffer
	code := cmdVerify([]string{"--recheck-pure", "--run", runDir, "--cache-dir", cacheRoot, "--rerun"},
		&out, &errOut)
	if code != exitSuccess {
		t.Fatalf("exit = %d, want %d (a finding is not a failed run), stderr: %s",
			code, exitSuccess, errOut.String())
	}
	s := out.String()
	if !strings.Contains(s, "MISMATCH") || !strings.Contains(s, "fetch") {
		t.Fatalf("the impure step was not reported:\n%s", s)
	}
	if !strings.Contains(s, "VERIFIED") || !strings.Contains(s, "honest") {
		t.Errorf("the honest step should verify, or the tool flags everything:\n%s", s)
	}
	// Actionable, not a label: the output that differed, both digests, and
	// what the step says it depends on.
	for _, want := range []string{"fetched.txt", "declared inputs", "declared outputs"} {
		if !strings.Contains(s, want) {
			t.Errorf("the finding is missing %q:\n%s", want, s)
		}
	}
}

// TestVerifyFailOnMismatchIsTheOptInGate: the same pass, with the flag a CI
// job would use.
func TestVerifyFailOnMismatchIsTheOptInGate(t *testing.T) {
	cacheRoot := t.TempDir()
	runDir, outside := seedVerifiableRun(t, cacheRoot)
	if err := os.WriteFile(outside, []byte("version two\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	var out, errOut bytes.Buffer
	code := cmdVerify([]string{
		"--recheck-pure", "--run", runDir, "--cache-dir", cacheRoot, "--rerun", "--fail-on-mismatch",
	}, &out, &errOut)
	if code != exitRunFailed {
		t.Fatalf("exit = %d, want %d with --fail-on-mismatch, stdout:\n%s", code, exitRunFailed, out.String())
	}
}

// TestVerifyFailOnMismatchStaysZeroWhenEverythingVerifies is the mutation
// check for the test above: a --fail-on-mismatch that returned 1
// unconditionally would pass it and be useless.
func TestVerifyFailOnMismatchStaysZeroWhenEverythingVerifies(t *testing.T) {
	cacheRoot := t.TempDir()
	runDir, _ := seedVerifiableRun(t, cacheRoot)

	var out, errOut bytes.Buffer
	code := cmdVerify([]string{
		"--recheck-pure", "--run", runDir, "--cache-dir", cacheRoot, "--rerun", "--fail-on-mismatch",
		"--step", "honest",
	}, &out, &errOut)
	if code != exitSuccess {
		t.Fatalf("exit = %d, want %d when nothing mismatched, stdout:\n%s", code, exitSuccess, out.String())
	}
}

// TestVerifyJSONIsOneParsableDocument: a script reading stdout must never get
// a half document, and the field names are the wire contract.
func TestVerifyJSONIsOneParsableDocument(t *testing.T) {
	cacheRoot := t.TempDir()
	runDir, outside := seedVerifiableRun(t, cacheRoot)
	if err := os.WriteFile(outside, []byte("version two\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	var out, errOut bytes.Buffer
	if code := cmdVerify([]string{
		"--recheck-pure", "--run", runDir, "--cache-dir", cacheRoot, "--rerun", "--json",
	}, &out, &errOut); code != exitSuccess {
		t.Fatalf("exit = %d, stderr: %s", code, errOut.String())
	}

	var doc struct {
		Executed bool `json:"executed"`
		Checked  int  `json:"checked"`
		Steps    []struct {
			Step        string `json:"step"`
			Verdict     string `json:"verdict"`
			Hermeticity string `json:"hermeticity"`
			Differences []struct {
				Kind   string `json:"kind"`
				Name   string `json:"name"`
				Cached string `json:"cached"`
				Rerun  string `json:"rerun"`
			} `json:"differences"`
		} `json:"steps"`
	}
	if err := json.Unmarshal(out.Bytes(), &doc); err != nil {
		t.Fatalf("stdout is not one JSON document: %v\n%s", err, out.String())
	}
	if !doc.Executed || doc.Checked != 2 {
		t.Fatalf("executed=%v checked=%d, want true/2", doc.Executed, doc.Checked)
	}
	found := false
	for _, s := range doc.Steps {
		if s.Step != "fetch" {
			continue
		}
		found = true
		if s.Verdict != "mismatch" {
			t.Errorf("fetch verdict = %q, want mismatch", s.Verdict)
		}
		if s.Hermeticity != "trusted" {
			t.Errorf("hermeticity = %q, want trusted", s.Hermeticity)
		}
		if len(s.Differences) == 0 {
			t.Error("a mismatch with no differences is not actionable")
		}
	}
	if !found {
		t.Errorf("the impure step is missing from the document:\n%s", out.String())
	}
}

// TestVerifyNamingAnUnknownStepIsAUsageError: naming something that has no
// cache record must say so rather than silently report an empty pass, which
// would read exactly like "everything is fine".
func TestVerifyNamingAnUnknownStepIsAUsageError(t *testing.T) {
	cacheRoot := t.TempDir()
	runDir, _ := seedVerifiableRun(t, cacheRoot)

	var out, errOut bytes.Buffer
	code := cmdVerify([]string{
		"--recheck-pure", "--run", runDir, "--cache-dir", cacheRoot, "--step", "nosuchstep",
	}, &out, &errOut)
	if code != exitUsage {
		t.Fatalf("exit = %d, want %d", code, exitUsage)
	}
	if !strings.Contains(errOut.String(), "nosuchstep") {
		t.Errorf("the message should name the step:\n%s", errOut.String())
	}
}

// TestVerifyDispatchesFromMain proves the top-level switch actually wires
// "verify" through, by looking for cmdVerify's own distinctive refusal rather
// than main's generic "unknown command".
func TestVerifyDispatchesFromMain(t *testing.T) {
	var out, errOut bytes.Buffer
	if code := run([]string{"verify"}, &out, &errOut); code != exitUsage {
		t.Fatalf("exit = %d, want %d", code, exitUsage)
	}
	if !strings.Contains(errOut.String(), "--recheck-pure") {
		t.Errorf("verify was not dispatched to:\n%s", errOut.String())
	}
}
