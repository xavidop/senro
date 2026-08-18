package main

import (
	"bytes"
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/xavidop/senro"
	"github.com/xavidop/senro/artifact"
	"github.com/xavidop/senro/exec"
	"github.com/xavidop/senro/internal/scratch"
)

func TestParseSizeAcceptsSuffixes(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want int64
	}{
		{"1024", 1024},
		{"1K", 1024},
		{"2M", 2 * 1024 * 1024},
		{"3G", 3 * 1024 * 1024 * 1024},
		{"50g", 50 * 1024 * 1024 * 1024},
	} {
		got, err := parseSize(tc.in)
		if err != nil {
			t.Errorf("parseSize(%q): %v", tc.in, err)
			continue
		}
		if got != tc.want {
			t.Errorf("parseSize(%q) = %d, want %d", tc.in, got, tc.want)
		}
	}
	for _, bad := range []string{"", "-1", "1T B", "many", "1.5G"} {
		if _, err := parseSize(bad); err == nil {
			t.Errorf("parseSize(%q) returned no error", bad)
		}
	}
}

func TestCacheGCReportsWhatItFreed(t *testing.T) {
	root := t.TempDir()
	seedCacheRoot(t, root)

	var out, errOut bytes.Buffer
	code := cmdCache([]string{"gc", "--cache-dir", root, "--max-size", "1K"}, &out, &errOut)
	if code != exitSuccess {
		t.Fatalf("exit = %d, stderr: %s", code, errOut.String())
	}
	for _, want := range []string{"objects", "freed"} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("output is missing %q:\n%s", want, out.String())
		}
	}
}

func TestCacheGCDryRunSaysSoAndChangesNothing(t *testing.T) {
	root := t.TempDir()
	seedCacheRoot(t, root)
	before := countFiles(t, filepath.Join(root, "cas"))

	var out, errOut bytes.Buffer
	if code := cmdCache([]string{"gc", "--cache-dir", root, "--max-size", "1", "--dry-run"}, &out, &errOut); code != exitSuccess {
		t.Fatalf("exit = %d, stderr: %s", code, errOut.String())
	}
	if !strings.Contains(out.String(), "dry run") {
		t.Errorf("a dry run did not say so:\n%s", out.String())
	}
	if after := countFiles(t, filepath.Join(root, "cas")); after != before {
		t.Errorf("a dry run changed the store: %d files became %d", before, after)
	}
}

// The mutation check for the two tests above, each of which passes on its
// own against a --dry-run that is ignored, or against one that is always
// on. This proves the flag gates deletion in both directions on the same
// seeded root: with it nothing shrinks, without it something does.
func TestCacheGCWithoutDryRunActuallyDeletesSomething(t *testing.T) {
	root := t.TempDir()
	seedCacheRoot(t, root)
	before := countFiles(t, filepath.Join(root, "cas"))

	var out, errOut bytes.Buffer
	if code := cmdCache([]string{"gc", "--cache-dir", root, "--max-size", "1"}, &out, &errOut); code != exitSuccess {
		t.Fatalf("exit = %d, stderr: %s", code, errOut.String())
	}
	after := countFiles(t, filepath.Join(root, "cas"))
	if after >= before {
		t.Errorf("a real (non-dry-run) gc under a 1-byte budget left %d files, started with %d; want fewer", after, before)
	}
}

func TestCacheRejectsAnUnknownSubcommandWithAUsageCode(t *testing.T) {
	var out, errOut bytes.Buffer
	if code := cmdCache([]string{"vacuum"}, &out, &errOut); code != exitUsage {
		t.Errorf("exit = %d, want %d", code, exitUsage)
	}
	if !strings.Contains(errOut.String(), "vacuum") {
		t.Errorf("the error does not name the unknown subcommand: %s", errOut.String())
	}
}

func TestCacheGCOnAnUnopenableRootIsAUsageError(t *testing.T) {
	blocked := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(blocked, []byte("x"), 0o644); err != nil {
		t.Fatalf("seed: %v", err)
	}
	var out, errOut bytes.Buffer
	if code := cmdCache([]string{"gc", "--cache-dir", blocked}, &out, &errOut); code == exitSuccess {
		t.Error("gc over an unopenable cache root reported success")
	}
}

func TestCacheGCRefusesAnUnknownFlag(t *testing.T) {
	var out, errOut bytes.Buffer
	if code := cmdCache([]string{"gc", "--bogus"}, &out, &errOut); code != exitUsage {
		t.Errorf("exit = %d, want %d", code, exitUsage)
	}
	if !strings.Contains(errOut.String(), "bogus") {
		t.Errorf("the error does not name the unknown flag: %s", errOut.String())
	}
}

func TestCacheGCRefusesABadKeepFailedDuration(t *testing.T) {
	var out, errOut bytes.Buffer
	if code := cmdCache([]string{"gc", "--keep-failed", "not-a-duration"}, &out, &errOut); code != exitUsage {
		t.Errorf("exit = %d, want %d", code, exitUsage)
	}
}

// seedCacheRoot runs a small two-step pure pipeline through senro.Run with
// WithCacheDir(root), so the fixture is a cache root a real run produced
// rather than one hand-assembled to satisfy cmdCacheGC.
func seedCacheRoot(t *testing.T, root string) {
	t.Helper()
	ws := senro.Workspace("src", senro.Scope(senro.ScopeRun))
	pipe := senro.New("ci")
	l := pipe.Workflow("main")
	l.Step("seed", exec.Command("sh", "-c", "printf 'package main\\n' > main.go")).
		WorkDir("/src").Mount(ws.At("/src", senro.RW))
	l.Step("compile", exec.Command("sh", "-c", "echo compiled | tee out.txt")).
		Needs("seed").WorkDir("/src").Mount(ws.At("/src", senro.RW)).
		Pure().Inputs(artifact.Glob("**/*.go")).Outputs(artifact.File("out.txt"))
	runDir := filepath.Join(t.TempDir(), "run")
	if err := senro.Run(context.Background(), pipe,
		senro.WithDir(runDir), senro.WithRunID("seed-run"), senro.WithCacheDir(root)); err != nil {
		t.Fatalf("seedCacheRoot: Run: %v", err)
	}
}

// countFiles walks dir counting regular files, so a test can tell whether a
// sweep actually removed anything without depending on GCStats' own
// counters agreeing with itself.
func countFiles(t *testing.T, dir string) int {
	t.Helper()
	n := 0
	err := filepath.WalkDir(dir, func(_ string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() {
			n++
		}
		return nil
	})
	if err != nil {
		t.Fatalf("countFiles(%s): %v", dir, err)
	}
	return n
}

// TestCacheDispatchesToExplain checks for cmdCacheExplain's own message
// rather than cmdCache's generic "unknown subcommand": a cmdCache that
// dropped the "explain" case would still pass every test calling
// cmdCacheExplain directly, so the dispatch needs its own check.
func TestCacheDispatchesToExplain(t *testing.T) {
	var out, errOut bytes.Buffer
	code := cmdCache([]string{"explain"}, &out, &errOut)
	if code != exitUsage {
		t.Fatalf("exit = %d, want %d", code, exitUsage)
	}
	if strings.Contains(errOut.String(), `unknown subcommand "explain"`) {
		t.Errorf("stderr = %q, \"explain\" is still reported as an unknown subcommand", errOut.String())
	}
}

// A cache miss you can explain is the point of `cache explain`. seedRuns
// runs the same pure pipeline twice against one cache root, changing an
// input between them, so the second run's records describe a real miss.
func TestCacheExplainNamesTheComponentThatChanged(t *testing.T) {
	base := t.TempDir()
	t.Chdir(base)
	seedRuns(t, base)

	var out, errOut bytes.Buffer
	code := cmdCacheExplain([]string{"--run", "r2", "compile"}, &out, &errOut)
	if code != exitSuccess {
		t.Fatalf("exit = %d, stderr: %s", code, errOut.String())
	}
	got := out.String()
	if !strings.Contains(got, "MISS") || !strings.Contains(got, "compile") {
		t.Errorf("output does not report the miss:\n%s", got)
	}
	if !strings.Contains(got, "input_digests") && !strings.Contains(got, "workspace_digests") {
		t.Errorf("output does not name the component that changed:\n%s", got)
	}
	if !strings.Contains(got, "unchanged") {
		t.Errorf("output does not say what stayed the same, so a reader cannot tell one change from all of them:\n%s", got)
	}
}

func TestCacheExplainWithNoStepSummarisesEveryStep(t *testing.T) {
	base := t.TempDir()
	t.Chdir(base)
	seedRuns(t, base)

	var out, errOut bytes.Buffer
	if code := cmdCacheExplain([]string{"--run", "r2"}, &out, &errOut); code != exitSuccess {
		t.Fatalf("exit = %d, stderr: %s", code, errOut.String())
	}
	if !strings.Contains(out.String(), "compile") {
		t.Errorf("the summary omits a step with a record:\n%s", out.String())
	}
}

func TestCacheExplainForAStepWithNoRecordIsAUsageError(t *testing.T) {
	base := t.TempDir()
	t.Chdir(base)
	seedRuns(t, base)

	var out, errOut bytes.Buffer
	code := cmdCacheExplain([]string{"--run", "r2", "not-a-step"}, &out, &errOut)
	if code != exitUsage {
		t.Errorf("exit = %d, want %d", code, exitUsage)
	}
	if !strings.Contains(errOut.String(), "not-a-step") {
		t.Errorf("the error does not name the step: %s", errOut.String())
	}
}

func TestCacheExplainReportsScratchOutcomes(t *testing.T) {
	base := t.TempDir()
	t.Chdir(base)
	seedRunsWithScratch(t, base)

	var out, errOut bytes.Buffer
	if code := cmdCacheExplain([]string{"--run", "r2"}, &out, &errOut); code != exitSuccess {
		t.Fatalf("exit = %d, stderr: %s", code, errOut.String())
	}
	if !strings.Contains(out.String(), "scratch") || !strings.Contains(out.String(), "deps") {
		t.Errorf("the summary does not report the scratch cache, which is invisible everywhere else:\n%s", out.String())
	}
}

// A cache a remote step mounted and never handed back is saved as NOTHING,
// and this is the only place that decision is ever visible: a run reporting
// it as an ordinary cold miss would look exactly like a cache that simply
// had no entry yet.
func TestCacheExplainSaysAScratchCacheDidNotComeBack(t *testing.T) {
	base := t.TempDir()
	t.Chdir(base)
	seedRunsWithScratch(t, base)

	cacheDir := filepath.Join(base, "runs", "r2", "cache")
	if err := scratch.WriteRecords(cacheDir, []scratch.Record{
		{Name: "deps", Key: "deps-v1", Unread: true},
	}); err != nil {
		t.Fatalf("WriteRecords: %v", err)
	}

	var out, errOut bytes.Buffer
	if code := cmdCacheExplain([]string{"--run", "r2"}, &out, &errOut); code != exitSuccess {
		t.Fatalf("exit = %d, stderr: %s", code, errOut.String())
	}
	if !strings.Contains(out.String(), "never came back") {
		t.Errorf("the summary reports a cache that never came back as an ordinary miss:\n%s", out.String())
	}
}

// A real Pure() step that never reached cacheLookup because its dependency
// failed is a distinct negative case from a name that never existed: the
// hazard is a lookup that degrades gracefully for the unknown name but
// misreports one that legitimately exists with no record.
func TestCacheExplainForAStepThatNeverRanIsAUsageError(t *testing.T) {
	base := t.TempDir()
	t.Chdir(base)
	seedRunWithASkippedStep(t, base)

	var out, errOut bytes.Buffer
	code := cmdCacheExplain([]string{"--run", "r1", "b"}, &out, &errOut)
	if code != exitUsage {
		t.Errorf("exit = %d, want %d", code, exitUsage)
	}
	if !strings.Contains(errOut.String(), "b") {
		t.Errorf("the error does not name the step that never ran: %s", errOut.String())
	}
}

// A run that never touched the cache at all. Not a usage error (--run
// named a real run) and not a failure: the honest answer is "nothing
// happened here", which cmdCacheExplain must say rather than error.
func TestCacheExplainForARunWithNoCacheRecordSaysSo(t *testing.T) {
	base := t.TempDir()
	t.Chdir(base)
	seedRunWithoutWorkspaces(t, base)

	var out, errOut bytes.Buffer
	if code := cmdCacheExplain([]string{"--run", "r1"}, &out, &errOut); code != exitSuccess {
		t.Fatalf("exit = %d, stderr: %s", code, errOut.String())
	}
	if !strings.Contains(out.String(), "no cache activity") {
		t.Errorf("a run with no cache record produced no explanation:\n%s", out.String())
	}
}

// seedRuns runs a two-step pure pipeline twice, at runs/r1 and runs/r2
// against one shared cache root, with different source content, so r2's
// "compile" misses against r1's entry: a real miss a real run produced.
//
// SENRO_CACHE_DIR is set to the same directory passed to WithCacheDir,
// because cmdWS and cmdCacheExplain have no --cache-dir flag and resolve
// the storage root from the environment; without it, later commands could
// not find the run this seeded.
func seedRuns(t *testing.T, base string) {
	t.Helper()
	cacheDir := filepath.Join(base, "cache")
	t.Setenv("SENRO_CACHE_DIR", cacheDir)

	run := func(id, seedCmd string) {
		ws := senro.Workspace("src", senro.Scope(senro.ScopeRun))
		pipe := senro.New("ci")
		l := pipe.Workflow("main")
		l.Step("seed", exec.Command("sh", "-c", seedCmd)).
			WorkDir("/src").Mount(ws.At("/src", senro.RW))
		l.Step("compile", exec.Command("sh", "-c", "echo compiled | tee out.txt")).
			Needs("seed").WorkDir("/src").Mount(ws.At("/src", senro.RW)).
			Pure().Inputs(artifact.Glob("**/*.go")).Outputs(artifact.File("out.txt"))
		dir := filepath.Join(base, "runs", id)
		if err := senro.Run(context.Background(), pipe,
			senro.WithDir(dir), senro.WithRunID(id), senro.WithCacheDir(cacheDir)); err != nil {
			t.Fatalf("seedRuns: Run %s: %v", id, err)
		}
	}
	run("r1", "printf 'package main\\n' > main.go")
	run("r2", "printf 'package main // changed\\n' > main.go")
}

// seedRunsWithScratch is seedRuns' scratch-cache sibling: one step mounting
// a scratch cache with a fixed key, run twice, so the second run's record
// shows a real restore.
func seedRunsWithScratch(t *testing.T, base string) {
	t.Helper()
	cacheDir := filepath.Join(base, "cache")
	t.Setenv("SENRO_CACHE_DIR", cacheDir)

	run := func(id string) {
		c := senro.ScratchCache("deps", senro.Key("deps-v1"))
		pipe := senro.New("ci")
		l := pipe.Workflow("main")
		l.Step("install", exec.Command("sh", "-c", "true")).Mount(c.At("/deps"))
		dir := filepath.Join(base, "runs", id)
		if err := senro.Run(context.Background(), pipe,
			senro.WithDir(dir), senro.WithRunID(id), senro.WithCacheDir(cacheDir)); err != nil {
			t.Fatalf("seedRunsWithScratch: Run %s: %v", id, err)
		}
	}
	run("r1")
	run("r2")
}

// seedRunWithASkippedStep seeds runs/r1 where "b" is Pure() (so it would
// have a record if it ran) but Needs("a"), which always fails, so "b" is
// skipped and legitimately has no record, unlike a step not in the plan at
// all. senro.Run returns a *senro.RunError, since the run reached a real
// terminal status.
func seedRunWithASkippedStep(t *testing.T, base string) {
	t.Helper()
	cacheDir := filepath.Join(base, "cache")
	t.Setenv("SENRO_CACHE_DIR", cacheDir)

	pipe := senro.New("ci")
	l := pipe.Workflow("main")
	l.Step("a", exec.Command("sh", "-c", "exit 1"))
	l.Step("b", exec.Command("sh", "-c", "true")).Needs("a").Pure().Inputs(artifact.Glob("*.go"))
	dir := filepath.Join(base, "runs", "r1")
	err := senro.Run(context.Background(), pipe,
		senro.WithDir(dir), senro.WithRunID("r1"), senro.WithCacheDir(cacheDir))
	var rerr *senro.RunError
	if !errors.As(err, &rerr) {
		t.Fatalf("seedRunWithASkippedStep: Run: want a *senro.RunError (a failed run, not an engine error), got %v", err)
	}
}
