package main

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/xavidop/senro"
	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/artifact"
	"github.com/xavidop/senro/exec"
	"github.com/xavidop/senro/internal/cas"
	"github.com/xavidop/senro/internal/eventlog"
	"github.com/xavidop/senro/internal/storage"
)

func TestWSLsListsEveryWorkspaceWithItsDigestAndSize(t *testing.T) {
	base := t.TempDir()
	t.Chdir(base)
	seedRuns(t, base)

	var out, errOut bytes.Buffer
	if code := cmdWS([]string{"ls", "r2"}, &out, &errOut); code != exitSuccess {
		t.Fatalf("exit = %d, stderr: %s", code, errOut.String())
	}
	got := out.String()
	for _, want := range []string{"src", "sha256:", "files"} {
		if !strings.Contains(got, want) {
			t.Errorf("output is missing %q:\n%s", want, got)
		}
	}
}

// The index exists so a file list is readable without the body. This is what
// reads it; without this command the index would be stored by every snapshot
// and never opened by anything.
func TestWSLsWithAWorkspaceNameListsItsFilesFromTheIndex(t *testing.T) {
	base := t.TempDir()
	t.Chdir(base)
	seedRuns(t, base)

	var out, errOut bytes.Buffer
	if code := cmdWS([]string{"ls", "r2", "src"}, &out, &errOut); code != exitSuccess {
		t.Fatalf("exit = %d, stderr: %s", code, errOut.String())
	}
	if !strings.Contains(out.String(), "main.go") {
		t.Errorf("the file listing does not name a file the workspace holds:\n%s", out.String())
	}
}

// TestWSLsCacheDirFlagResolvesARunThatUsedACustomCacheDir: senro.WithCacheDir
// lets a library caller put a run's cache anywhere, and --cache-dir is the
// only way to point `ws ls <run> <name>`'s index lookup at it.
func TestWSLsCacheDirFlagResolvesARunThatUsedACustomCacheDir(t *testing.T) {
	base := t.TempDir()
	t.Chdir(base)
	// Pinned to a WRONG path rather than left to the ambient environment,
	// so the first cmdWS call below fails reliably rather than depending on
	// the test runner's own environment.
	t.Setenv("SENRO_CACHE_DIR", filepath.Join(base, "not-the-run-s-cache"))

	// Deliberately NOT $SENRO_CACHE_DIR and NOT the platform default: the
	// one thing resolveCacheDir("") can never find on its own.
	customCacheDir := filepath.Join(base, "somewhere-else", "cache")
	ws := senro.Workspace("src", senro.Scope(senro.ScopeRun))
	pipe := senro.New("ci")
	l := pipe.Workflow("main")
	l.Step("seed", exec.Command("sh", "-c", "printf 'package main\\n' > main.go")).
		WorkDir("/src").Mount(ws.At("/src", senro.RW))
	runDir := filepath.Join(base, "runs", "r1")
	if err := senro.Run(context.Background(), pipe,
		senro.WithDir(runDir), senro.WithRunID("r1"), senro.WithCacheDir(customCacheDir)); err != nil {
		t.Fatalf("Run: %v", err)
	}

	var out, errOut bytes.Buffer
	if code := cmdWS([]string{"ls", "r1", "src"}, &out, &errOut); code == exitSuccess {
		t.Fatalf("ws ls without --cache-dir unexpectedly resolved a run that used a custom cache dir")
	}

	out.Reset()
	errOut.Reset()
	if code := cmdWS([]string{"ls", "--cache-dir", customCacheDir, "r1", "src"}, &out, &errOut); code != exitSuccess {
		t.Fatalf("exit = %d, stderr: %s", code, errOut.String())
	}
	if !strings.Contains(out.String(), "main.go") {
		t.Errorf("the file listing does not name a file the workspace holds:\n%s", out.String())
	}
}

func TestWSLsForAnUnknownWorkspaceIsAUsageError(t *testing.T) {
	base := t.TempDir()
	t.Chdir(base)
	seedRuns(t, base)

	var out, errOut bytes.Buffer
	if code := cmdWS([]string{"ls", "r2", "nope"}, &out, &errOut); code != exitUsage {
		t.Errorf("exit = %d, want %d", code, exitUsage)
	}
	if !strings.Contains(errOut.String(), "nope") {
		t.Errorf("the error does not name the workspace: %s", errOut.String())
	}
}

func TestWSLsOnARunWithNoWorkspacesSaysSo(t *testing.T) {
	base := t.TempDir()
	t.Chdir(base)
	seedRunWithoutWorkspaces(t, base)

	var out, errOut bytes.Buffer
	if code := cmdWS([]string{"ls", "r1"}, &out, &errOut); code != exitSuccess {
		t.Fatalf("exit = %d, stderr: %s", code, errOut.String())
	}
	if !strings.Contains(out.String(), "no workspaces") {
		t.Errorf("a run with no workspaces produced no explanation:\n%s", out.String())
	}
}

func TestWSRejectsAnUnknownSubcommand(t *testing.T) {
	// A genuine typo. "pull" and "diff" are real subcommands now and must
	// never reach this branch; the tests further down exercise both.
	var out, errOut bytes.Buffer
	if code := cmdWS([]string{"lss", "r1", "src"}, &out, &errOut); code != exitUsage {
		t.Errorf("exit = %d, want %d", code, exitUsage)
	}
	if !strings.Contains(errOut.String(), "lss") {
		t.Errorf("the error does not name the unknown subcommand: %s", errOut.String())
	}
	if !strings.Contains(errOut.String(), "unknown subcommand") {
		t.Errorf("a typo should read as a typo: %s", errOut.String())
	}
}

// Bare `senro ws` names no subcommand at all. It is not a typo either, so it
// gets the usage text rather than `unknown subcommand "(none)"`.
func TestWSWithNoSubcommandPrintsUsage(t *testing.T) {
	var out, errOut bytes.Buffer
	if code := cmdWS(nil, &out, &errOut); code != exitUsage {
		t.Errorf("exit = %d, want %d", code, exitUsage)
	}
	for _, want := range []string{"senro ws ls", "senro ws pull", "senro ws diff"} {
		if !strings.Contains(errOut.String(), want) {
			t.Errorf("the usage text does not name %q:\n%s", want, errOut.String())
		}
	}
}

// A workspace with zero files (mounted, but never written to) is a valid
// answer, so `ws ls` must say so rather than print nothing, which would look
// identical to a bug that silently listed no entries.
func TestWSLsOnAWorkspaceWithZeroFilesSaysSo(t *testing.T) {
	base := t.TempDir()
	t.Chdir(base)
	seedRunWithEmptyWorkspace(t, base)

	var out, errOut bytes.Buffer
	if code := cmdWS([]string{"ls", "r1", "empty"}, &out, &errOut); code != exitSuccess {
		t.Fatalf("exit = %d, stderr: %s", code, errOut.String())
	}
	if !strings.Contains(out.String(), "no files") {
		t.Errorf("an empty workspace's file listing said nothing rather than explaining itself:\n%s", out.String())
	}
}

// gc's references() deliberately does not protect a snapshot's index
// object, since cache.Result stores only a body digest, so a sweep can take
// an index whose body survives. `ws ls` must turn that into an honest
// message: not exitRunFailed (the run may have succeeded) and not
// exitSuccess (the file list genuinely cannot be produced).
func TestWSLsForARunWhoseIndexWasCollectedGivesAnHonestMessage(t *testing.T) {
	base := t.TempDir()
	t.Chdir(base)
	seedRuns(t, base)

	// Confirm the baseline works before breaking it, so a later failure is
	// known to come from the deletion below and not from a seeding mistake.
	var out, errOut bytes.Buffer
	if code := cmdWS([]string{"ls", "r2", "src"}, &out, &errOut); code != exitSuccess {
		t.Fatalf("baseline ws ls before deleting the index: exit = %d, stderr: %s", code, errOut.String())
	}

	indexDigest := wsIndexDigest(t, filepath.Join(base, "runs", "r2"), "src")
	cacheRoot, err := storage.DefaultRoot()
	if err != nil {
		t.Fatalf("storage.DefaultRoot: %v", err)
	}
	store, err := storage.Open(cacheRoot)
	if err != nil {
		t.Fatalf("storage.Open: %v", err)
	}
	if err := store.CAS.Delete(indexDigest); err != nil {
		t.Fatalf("delete index object: %v", err)
	}
	_ = store.Close()

	out.Reset()
	errOut.Reset()
	code := cmdWS([]string{"ls", "r2", "src"}, &out, &errOut)
	if code == exitRunFailed {
		t.Errorf("a gc-swept index surfaced as an unexplained run failure: exit %d, stderr %s", code, errOut.String())
	}
	if code == exitSuccess {
		t.Errorf("ws ls reported success for a workspace whose index is gone: %s", out.String())
	}
	if !strings.Contains(errOut.String(), "src") {
		t.Errorf("the message does not name the workspace: %s", errOut.String())
	}
}

// A workspace whose only event is ws.restored (a cache hit, restored
// without re-snapshotting) must still appear in the listing, and asking for
// its file list must fail with an honest message rather than "no workspace
// %q", which would be actively wrong.
func TestWSLsStillReportsAWorkspaceRestoredFromACacheHit(t *testing.T) {
	base := t.TempDir()
	t.Chdir(base)
	seedRunsWithACacheHit(t, base)

	var out, errOut bytes.Buffer
	if code := cmdWS([]string{"ls", "r2"}, &out, &errOut); code != exitSuccess {
		t.Fatalf("exit = %d, stderr: %s", code, errOut.String())
	}
	if !strings.Contains(out.String(), "src") {
		t.Errorf("a workspace whose only event in this run was a cache-hit restore vanished from the listing:\n%s", out.String())
	}
	// Stronger than merely finding the name: "seed" snapshots "src" fresh
	// mid-run before "compile" restores it to a later state, so a
	// latestSnapshots ignoring ws.restored would still print an "src" line,
	// just a stale one. Wrong data presented as current is worse than
	// omission, and this is what catches it.
	if !strings.Contains(out.String(), "cached") {
		t.Errorf("the listing did not mark \"src\" as having come from a cache hit with no index, "+
			"so it may be silently showing a stale pre-hit snapshot instead:\n%s", out.String())
	}

	out.Reset()
	errOut.Reset()
	code := cmdWS([]string{"ls", "r2", "src"}, &out, &errOut)
	if code == exitRunFailed {
		t.Errorf("a documented limitation (no index for a workspace restored from a cache hit) surfaced as an unexplained run failure: exit %d, stderr %s", code, errOut.String())
	}
	if code != exitUsage {
		t.Errorf("exit = %d, want %d (an honest, documented limitation, not success and not a crash)", code, exitUsage)
	}
	msg := errOut.String()
	if !strings.Contains(msg, "src") {
		t.Errorf("the message does not name the workspace: %s", msg)
	}
	if !strings.Contains(msg, "cache") && !strings.Contains(msg, "index") {
		t.Errorf("the message does not explain that no index is available: %s", msg)
	}
}

// TestWSLsFlagsAWorkspaceOverTheSizeThreshold exercises formatWSLine
// directly with a fabricated size on each side of the threshold, since
// building an actual 2 GiB workspace is impractical.
func TestWSLsFlagsAWorkspaceOverTheSizeThreshold(t *testing.T) {
	digest := "sha256:" + strings.Repeat("a", 64)
	small := formatWSLine("src", api.WSSnapshotBody{Digest: digest, Index: digest, Files: 1, Bytes: 100}, "r1")
	if strings.Contains(small, "LARGE") {
		t.Errorf("a tiny workspace was flagged LARGE: %s", small)
	}
	big := formatWSLine("src", api.WSSnapshotBody{Digest: digest, Index: digest, Files: 1, Bytes: largeWorkspaceBytes + 1}, "r1")
	if !strings.Contains(big, "LARGE") {
		t.Errorf("a workspace just over the 2 GiB threshold was not flagged: %s", big)
	}
}

// seedRunWithoutWorkspaces seeds runs/r1 with a plan that declares no
// workspace and no scratch cache and no Pure() step, so both `ws ls` and
// `cache explain` see a run with legitimately nothing to report.
func seedRunWithoutWorkspaces(t *testing.T, base string) {
	t.Helper()
	cacheDir := filepath.Join(base, "cache")
	t.Setenv("SENRO_CACHE_DIR", cacheDir)

	pipe := senro.New("ci")
	l := pipe.Workflow("main")
	l.Step("noop", exec.Command("sh", "-c", "true"))
	dir := filepath.Join(base, "runs", "r1")
	if err := senro.Run(context.Background(), pipe,
		senro.WithDir(dir), senro.WithRunID("r1"), senro.WithCacheDir(cacheDir)); err != nil {
		t.Fatalf("seedRunWithoutWorkspaces: Run: %v", err)
	}
}

// seedRunWithEmptyWorkspace seeds runs/r1 with a step that mounts a
// workspace but writes nothing into it, so the workspace's own snapshot is
// real and genuinely empty rather than hand-constructed.
func seedRunWithEmptyWorkspace(t *testing.T, base string) {
	t.Helper()
	cacheDir := filepath.Join(base, "cache")
	t.Setenv("SENRO_CACHE_DIR", cacheDir)

	ws := senro.Workspace("empty", senro.Scope(senro.ScopeRun))
	pipe := senro.New("ci")
	l := pipe.Workflow("main")
	l.Step("noop", exec.Command("sh", "-c", "true")).
		WorkDir("/empty").Mount(ws.At("/empty", senro.RW))
	dir := filepath.Join(base, "runs", "r1")
	if err := senro.Run(context.Background(), pipe,
		senro.WithDir(dir), senro.WithRunID("r1"), senro.WithCacheDir(cacheDir)); err != nil {
		t.Fatalf("seedRunWithEmptyWorkspace: Run: %v", err)
	}
}

// seedRunsWithACacheHit runs seedRuns' pipeline twice: runs/r1 is a cold
// miss that saves the entry, runs/r2 an exact hit. On r2, "src" is
// snapshotted fresh by "seed" and then RESTORED by "compile", so r2's
// ledger ends with ws.restored and that state has no index digest anywhere,
// since cache.Result stores a body digest alone. The run-produced version
// of the gc-sweep gap above: no sweep is needed to reach it.
func seedRunsWithACacheHit(t *testing.T, base string) {
	t.Helper()
	cacheDir := filepath.Join(base, "cache")
	t.Setenv("SENRO_CACHE_DIR", cacheDir)

	build := func() *senro.Pipeline {
		ws := senro.Workspace("src", senro.Scope(senro.ScopeRun))
		pipe := senro.New("ci")
		l := pipe.Workflow("main")
		l.Step("seed", exec.Command("sh", "-c", "printf 'package main\\n' > main.go")).
			WorkDir("/src").Mount(ws.At("/src", senro.RW))
		l.Step("compile", exec.Command("sh", "-c", "echo compiled | tee out.txt")).
			Needs("seed").WorkDir("/src").Mount(ws.At("/src", senro.RW)).
			Pure().Inputs(artifact.Glob("**/*.go")).Outputs(artifact.File("out.txt"))
		return pipe
	}

	run := func(id string) {
		dir := filepath.Join(base, "runs", id)
		if err := senro.Run(context.Background(), build(),
			senro.WithDir(dir), senro.WithRunID(id), senro.WithCacheDir(cacheDir)); err != nil {
			t.Fatalf("seedRunsWithACacheHit: Run %s: %v", id, err)
		}
	}
	run("r1")
	run("r2")
}

// wsIndexDigest reads dir's own event log for the most recent ws.snapshot
// naming workspace name and returns its index digest, so a test can delete
// that exact object from the CAS to simulate what a gc sweep leaves behind.
func wsIndexDigest(t *testing.T, dir, name string) cas.Digest {
	t.Helper()
	events, err := eventlog.Read(filepath.Join(dir, "events.jsonl"))
	if err != nil && len(events) == 0 {
		t.Fatalf("wsIndexDigest: read %s: %v", dir, err)
	}
	var found cas.Digest
	for _, e := range events {
		if e.Type != api.WSSnapshot {
			continue
		}
		var b api.WSSnapshotBody
		if err := e.Decode(&b); err != nil {
			continue
		}
		if b.Name == name && b.Index != "" {
			found = cas.Digest(b.Index)
		}
	}
	if found == "" {
		t.Fatalf("wsIndexDigest: no ws.snapshot with an index for workspace %q in %s", name, dir)
	}
	return found
}

// ---------------------------------------------------------------------------
// senro ws pull
// ---------------------------------------------------------------------------

// The point of the command: the actual bytes a step left behind, on a
// filesystem path a person can open in an editor.
func TestWSPullWritesAWorkspacesFilesToDisk(t *testing.T) {
	base := t.TempDir()
	t.Chdir(base)
	seedRuns(t, base)

	dest := filepath.Join(base, "pulled")
	var out, errOut bytes.Buffer
	if code := cmdWS([]string{"pull", "r2", "src", dest}, &out, &errOut); code != exitSuccess {
		t.Fatalf("exit = %d, stderr: %s", code, errOut.String())
	}
	body, err := os.ReadFile(filepath.Join(dest, "main.go"))
	if err != nil {
		t.Fatalf("ws pull did not write the workspace's own content: %v", err)
	}
	if !strings.Contains(string(body), "package main") {
		t.Errorf("main.go holds %q, not what the run wrote into it", string(body))
	}
	got := out.String()
	if !strings.Contains(got, dest) {
		t.Errorf("the summary does not say where the files went:\n%s", got)
	}
	if !strings.Contains(got, "sha256:") {
		t.Errorf("the summary does not name the body digest it pulled:\n%s", got)
	}
	// Nobody should ever have to guess whether their 0600 file was mangled by
	// a bug: a snapshot never carried the bit in the first place, and this is
	// the one place a reader is looking when they wonder.
	if !strings.Contains(got, "0644") || !strings.Contains(got, "0755") {
		t.Errorf("the summary does not say which modes a snapshot actually carries:\n%s", got)
	}
}

// DEST is optional, and defaulting it to the workspace's own name is the
// difference between a command you can type from memory and one you look up.
func TestWSPullDefaultsTheDestinationToTheWorkspaceName(t *testing.T) {
	base := t.TempDir()
	t.Chdir(base)
	seedRuns(t, base)

	var out, errOut bytes.Buffer
	if code := cmdWS([]string{"pull", "r2", "src"}, &out, &errOut); code != exitSuccess {
		t.Fatalf("exit = %d, stderr: %s", code, errOut.String())
	}
	if _, err := os.Stat(filepath.Join(base, "src", "main.go")); err != nil {
		t.Errorf("ws pull with no DEST did not write ./src: %v", err)
	}
}

// A pull REPLACES the destination, because a merge would leave a tree that is
// not the snapshot. That makes an existing non-empty destination a thing to
// refuse rather than to quietly overwrite.
func TestWSPullRefusesANonEmptyDestinationUnlessForced(t *testing.T) {
	base := t.TempDir()
	t.Chdir(base)
	seedRuns(t, base)

	dest := filepath.Join(base, "pulled")
	if err := os.MkdirAll(dest, 0o755); err != nil {
		t.Fatalf("seed dest: %v", err)
	}
	precious := filepath.Join(dest, "precious.txt")
	if err := os.WriteFile(precious, []byte("do not lose me\n"), 0o644); err != nil {
		t.Fatalf("seed dest: %v", err)
	}

	var out, errOut bytes.Buffer
	if code := cmdWS([]string{"pull", "r2", "src", dest}, &out, &errOut); code != exitUsage {
		t.Fatalf("exit = %d, want %d", code, exitUsage)
	}
	if _, err := os.Stat(precious); err != nil {
		t.Errorf("a refused pull deleted the destination's contents anyway: %v", err)
	}
	if !strings.Contains(errOut.String(), "--force") {
		t.Errorf("the refusal does not say how to proceed deliberately:\n%s", errOut.String())
	}

	out.Reset()
	errOut.Reset()
	if code := cmdWS([]string{"pull", "--force", "r2", "src", dest}, &out, &errOut); code != exitSuccess {
		t.Fatalf("--force: exit = %d, stderr: %s", code, errOut.String())
	}
	if _, err := os.Stat(precious); err == nil {
		t.Error("--force left a file from before the pull behind, so the destination is not the snapshot")
	}
	if _, err := os.Stat(filepath.Join(dest, "main.go")); err != nil {
		t.Errorf("--force did not write the snapshot: %v", err)
	}
}

// The oldest extraction bug there is. A workspace tarball is content from
// another run and eventually another machine, so it is untrusted input by
// construction and the reader must refuse rather than trust it.
func TestWSPullRefusesATarEntryThatEscapesTheDestination(t *testing.T) {
	base := t.TempDir()
	t.Chdir(base)
	seedRunWithAHostileWorkspaceTarball(t, base)

	dest := filepath.Join(base, "pulled")
	var out, errOut bytes.Buffer
	code := cmdWS([]string{"pull", "evil", "src", dest}, &out, &errOut)
	if code == exitSuccess {
		t.Fatalf("ws pull unpacked an archive with a %q entry:\n%s", "..", out.String())
	}
	if !strings.Contains(errOut.String(), "escapes") {
		t.Errorf("the refusal does not say what was wrong with the archive:\n%s", errOut.String())
	}
	// The whole point: not one byte outside the destination the user named.
	if _, err := os.Stat(filepath.Join(base, "escaped.txt")); err == nil {
		t.Error("ws pull wrote a file OUTSIDE the destination it was given")
	}
	// And no half-extracted tree left lying around next to it either.
	entries, err := os.ReadDir(base)
	if err != nil {
		t.Fatalf("read %s: %v", base, err)
	}
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), ".senro-restore-") {
			t.Errorf("a refused pull left its staging directory %q behind", e.Name())
		}
	}
}

// A cache-restored workspace has no recorded index, and ws pull does not
// need one: ws.restored carries the body digest, which is all a pull ever
// needed. The file count therefore comes from the extraction, not the
// ledger, which records zero here.
func TestWSPullWorksForAWorkspaceRestoredFromACacheHit(t *testing.T) {
	base := t.TempDir()
	t.Chdir(base)
	seedRunsWithACacheHit(t, base)

	// Confirm the premise, so a pass below cannot come from having
	// accidentally seeded a run that does have an index.
	var check, checkErr bytes.Buffer
	if code := cmdWS([]string{"ls", "r2", "src"}, &check, &checkErr); code != exitUsage {
		t.Fatalf("premise: ws ls on the cache-hit workspace exited %d, want %d (no index)", code, exitUsage)
	}

	dest := filepath.Join(base, "pulled")
	var out, errOut bytes.Buffer
	if code := cmdWS([]string{"pull", "r2", "src", dest}, &out, &errOut); code != exitSuccess {
		t.Fatalf("exit = %d, stderr: %s", code, errOut.String())
	}
	if _, err := os.Stat(filepath.Join(dest, "out.txt")); err != nil {
		t.Errorf("ws pull did not restore the cached workspace's own content: %v", err)
	}
	if strings.Contains(out.String(), "0 entries") {
		t.Errorf("the summary reported an empty workspace for a workspace that is not empty, "+
			"so it is reading the ledger's unset counts instead of what it wrote:\n%s", out.String())
	}
}

func TestWSPullForAnUnknownWorkspaceIsAUsageError(t *testing.T) {
	base := t.TempDir()
	t.Chdir(base)
	seedRuns(t, base)

	var out, errOut bytes.Buffer
	if code := cmdWS([]string{"pull", "r2", "nope", filepath.Join(base, "d")}, &out, &errOut); code != exitUsage {
		t.Errorf("exit = %d, want %d", code, exitUsage)
	}
	if !strings.Contains(errOut.String(), "nope") {
		t.Errorf("the error does not name the workspace: %s", errOut.String())
	}
}

// RUN cannot be optional the way `ws ls`'s is: with NAME and DEST both
// optional too, `senro ws pull src out` would be unreadable. Refusing is
// better than guessing which of the two a bare pair meant.
func TestWSPullWithoutARunAndAWorkspaceIsAUsageError(t *testing.T) {
	base := t.TempDir()
	t.Chdir(base)
	seedRuns(t, base)

	var out, errOut bytes.Buffer
	if code := cmdWS([]string{"pull", "src"}, &out, &errOut); code != exitUsage {
		t.Errorf("exit = %d, want %d", code, exitUsage)
	}
	if !strings.Contains(errOut.String(), "senro ws pull: want RUN NAME") {
		t.Errorf("the error does not say what the missing argument is:\n%s", errOut.String())
	}
}

// ---------------------------------------------------------------------------
// senro ws diff
// ---------------------------------------------------------------------------

// The question the command exists to answer: what did this step actually do
// to the tree.
func TestWSDiffReportsWhatChangedBetweenTwoRuns(t *testing.T) {
	base := t.TempDir()
	t.Chdir(base)
	seedDivergingRuns(t, base)

	var out, errOut bytes.Buffer
	// Exit 0 even though there ARE differences: 1 means "the run failed" in
	// this CLI's exit-code contract, and diff(1)'s convention cannot be
	// borrowed here without breaking it.
	if code := cmdWS([]string{"diff", "r1", "r2", "src"}, &out, &errOut); code != exitSuccess {
		t.Fatalf("exit = %d, stderr: %s", code, errOut.String())
	}
	got := out.String()
	for _, want := range []string{"added.txt", "gone.txt", "main.go", "build.sh"} {
		if !strings.Contains(got, want) {
			t.Errorf("the diff does not mention %q:\n%s", want, got)
		}
	}
	if !strings.Contains(got, "+ added.txt") {
		t.Errorf("a file only the second run has is not marked as added:\n%s", got)
	}
	if !strings.Contains(got, "- gone.txt") {
		t.Errorf("a file only the first run has is not marked as removed:\n%s", got)
	}
	if !strings.Contains(got, "M main.go") {
		t.Errorf("a file whose content changed is not marked as modified:\n%s", got)
	}
	// chmod +x with byte-identical content: the change most easily missed by
	// eye, and the one an index makes trivially visible.
	if !strings.Contains(got, "P build.sh") {
		t.Errorf("a file whose mode alone changed is not marked as a mode change:\n%s", got)
	}
}

// This is the design claim the index exists to make good on, so it gets a
// test that fails loudly if a body is ever fetched: both bodies are deleted
// from the store and the diff must still be answerable.
func TestWSDiffNeedsNoBodyAtAll(t *testing.T) {
	base := t.TempDir()
	t.Chdir(base)
	seedDivergingRuns(t, base)

	cacheRoot, err := storage.DefaultRoot()
	if err != nil {
		t.Fatalf("storage.DefaultRoot: %v", err)
	}
	store, err := storage.Open(cacheRoot)
	if err != nil {
		t.Fatalf("storage.Open: %v", err)
	}
	for _, run := range []string{"r1", "r2"} {
		if err := store.CAS.Delete(wsBodyDigest(t, filepath.Join(base, "runs", run), "src")); err != nil {
			t.Fatalf("delete %s body: %v", run, err)
		}
	}
	_ = store.Close()

	var out, errOut bytes.Buffer
	if code := cmdWS([]string{"diff", "r1", "r2", "src"}, &out, &errOut); code != exitSuccess {
		t.Fatalf("ws diff needed a body it should never have opened: exit %d, stderr: %s", code, errOut.String())
	}
	if !strings.Contains(out.String(), "main.go") {
		t.Errorf("the diff produced no answer from the indexes alone:\n%s", out.String())
	}
}

func TestWSDiffOfARunAgainstItselfSaysIdentical(t *testing.T) {
	base := t.TempDir()
	t.Chdir(base)
	seedDivergingRuns(t, base)

	var out, errOut bytes.Buffer
	if code := cmdWS([]string{"diff", "r1", "r1", "src"}, &out, &errOut); code != exitSuccess {
		t.Fatalf("exit = %d, stderr: %s", code, errOut.String())
	}
	if !strings.Contains(out.String(), "identical") {
		t.Errorf("two identical snapshots produced no explanation, which reads the same as a broken diff:\n%s",
			out.String())
	}
}

// Output is piped as well as read. --json is the form a script consumes, and
// its field names are a contract, so the test asserts on the literal keys a
// consumer sees rather than on this package's own structs.
func TestWSDiffJSONIsMachineReadable(t *testing.T) {
	base := t.TempDir()
	t.Chdir(base)
	seedDivergingRuns(t, base)

	var out, errOut bytes.Buffer
	if code := cmdWS([]string{"diff", "--json", "r1", "r2", "src"}, &out, &errOut); code != exitSuccess {
		t.Fatalf("exit = %d, stderr: %s", code, errOut.String())
	}
	var doc struct {
		Workspaces []struct {
			Name      string `json:"name"`
			Identical bool   `json:"identical"`
			A         struct {
				Run    string `json:"run"`
				Digest string `json:"digest"`
			} `json:"a"`
			B struct {
				Digest string `json:"digest"`
			} `json:"b"`
			Changes []struct {
				Path   string `json:"path"`
				Status string `json:"status"`
				A      *struct {
					Mode   string `json:"mode"`
					Size   int64  `json:"size"`
					Digest string `json:"digest"`
				} `json:"a"`
				B *struct {
					Mode string `json:"mode"`
				} `json:"b"`
			} `json:"changes"`
			Summary struct {
				Added     int `json:"added"`
				Removed   int `json:"removed"`
				Modified  int `json:"modified"`
				Mode      int `json:"mode"`
				Kind      int `json:"kind"`
				Unchanged int `json:"unchanged"`
			} `json:"summary"`
		} `json:"workspaces"`
	}
	if err := json.Unmarshal(out.Bytes(), &doc); err != nil {
		t.Fatalf("--json did not emit one JSON document: %v\n%s", err, out.String())
	}
	if len(doc.Workspaces) != 1 {
		t.Fatalf("want one workspace, got %d:\n%s", len(doc.Workspaces), out.String())
	}
	w := doc.Workspaces[0]
	if w.Name != "src" {
		t.Errorf("name = %q, want %q", w.Name, "src")
	}
	if w.Identical {
		t.Error("identical = true for two snapshots that differ")
	}
	if w.A.Digest == "" || w.B.Digest == "" || w.A.Digest == w.B.Digest {
		t.Errorf("both sides' digests must be reported and must differ: %+v", w)
	}
	if w.A.Run == "" {
		t.Errorf("a side does not say which run it came from: %+v", w)
	}
	if w.Summary.Added != 1 || w.Summary.Removed != 1 || w.Summary.Modified != 1 || w.Summary.Mode != 1 {
		t.Errorf("summary = %+v, want one of each of added, removed, modified, mode", w.Summary)
	}
	byPath := map[string]string{}
	modes := map[string]string{}
	for _, c := range w.Changes {
		byPath[c.Path] = c.Status
		if c.B != nil {
			modes[c.Path] = c.B.Mode
		}
	}
	for path, want := range map[string]string{
		"added.txt": "added", "gone.txt": "removed", "main.go": "modified", "build.sh": "mode",
	} {
		if byPath[path] != want {
			t.Errorf("%s: status = %q, want %q", path, byPath[path], want)
		}
	}
	// Octal, because 420 is not a thing anyone recognises as 0644.
	if modes["build.sh"] != "0755" {
		t.Errorf("build.sh: mode = %q, want %q", modes["build.sh"], "0755")
	}
}

// With no NAME, every workspace both runs have. A workspace only one of them
// has is reported rather than silently dropped, and it does not make the
// whole command fail: the comparison the user asked for still happened.
func TestWSDiffWithoutANameCoversEveryWorkspaceBothRunsHave(t *testing.T) {
	base := t.TempDir()
	t.Chdir(base)
	seedRunsWithAWorkspaceOnlyOneRunHas(t, base)

	var out, errOut bytes.Buffer
	if code := cmdWS([]string{"diff", "r1", "r2"}, &out, &errOut); code != exitSuccess {
		t.Fatalf("exit = %d, stderr: %s", code, errOut.String())
	}
	got := out.String()
	if !strings.Contains(got, "main.go") {
		t.Errorf("the workspace both runs have was not diffed:\n%s", got)
	}
	if !strings.Contains(got, "extra") {
		t.Errorf("the workspace only one run has was silently dropped:\n%s", got)
	}
}

// Naming a workspace one run does not have is a different situation from
// diffing everything: the user asked a question that has no answer, and the
// useful reply names what each run does have.
func TestWSDiffForAWorkspaceMissingFromOneRunSaysWhichRunHasIt(t *testing.T) {
	base := t.TempDir()
	t.Chdir(base)
	seedRunsWithAWorkspaceOnlyOneRunHas(t, base)

	var out, errOut bytes.Buffer
	if code := cmdWS([]string{"diff", "r1", "r2", "extra"}, &out, &errOut); code != exitUsage {
		t.Fatalf("exit = %d, want %d", code, exitUsage)
	}
	msg := errOut.String()
	if !strings.Contains(msg, "extra") {
		t.Errorf("the error does not name the workspace: %s", msg)
	}
	if !strings.Contains(msg, "r1") || !strings.Contains(msg, "r2") {
		t.Errorf("the error does not say which run has it and which does not: %s", msg)
	}
}

// The cache-hit case, on diff's side. Unlike ws pull, diff genuinely cannot
// proceed: a body digest says two snapshots differ but not how, and the
// whole point of this command is not downloading them.
func TestWSDiffForAWorkspaceWithNoRecordedIndexIsAnHonestUsageError(t *testing.T) {
	base := t.TempDir()
	t.Chdir(base)
	seedRunsWithACacheHit(t, base)
	seedAThirdRunWithADifferentWorkspace(t, base)

	var out, errOut bytes.Buffer
	// r2's "src" came from a cache hit and has no index; r3's differs from
	// it, so the digests cannot settle the question and the index is
	// genuinely needed.
	code := cmdWS([]string{"diff", "r2", "r3", "src"}, &out, &errOut)
	if code == exitRunFailed {
		t.Errorf("a documented limitation surfaced as an unexplained run failure: exit %d, stderr %s", code, errOut.String())
	}
	if code != exitUsage {
		t.Errorf("exit = %d, want %d", code, exitUsage)
	}
	msg := errOut.String()
	if !strings.Contains(msg, "src") {
		t.Errorf("the message does not name the workspace: %s", msg)
	}
	if !strings.Contains(msg, "index") {
		t.Errorf("the message does not explain that no index is available: %s", msg)
	}
	// Actionable, not merely honest: ws pull works for exactly this case,
	// because the body digest is all it needs.
	if !strings.Contains(msg, "senro ws pull") {
		t.Errorf("the message does not point at the command that does work here: %s", msg)
	}
}

// Two snapshots with the same content address ARE the same tree, so a
// workspace restored to exactly the previous run's state is answerable with
// no index on either side. The "no index" refusal must be reserved for the
// case where the question really cannot be answered.
func TestWSDiffAnswersFromDigestsAloneEvenWhenOneSideHasNoIndex(t *testing.T) {
	base := t.TempDir()
	t.Chdir(base)
	seedRunsWithACacheHit(t, base)

	var out, errOut bytes.Buffer
	if code := cmdWS([]string{"diff", "r1", "r2", "src"}, &out, &errOut); code != exitSuccess {
		t.Fatalf("exit = %d, stderr: %s", code, errOut.String())
	}
	if !strings.Contains(out.String(), "identical") {
		t.Errorf("a cache hit that restored the previous run's exact tree was not reported as identical:\n%s",
			out.String())
	}
}

// seedAThirdRunWithADifferentWorkspace adds runs/r3 to
// seedRunsWithACacheHit's seed: the same pipeline with a different main.go,
// so "compile" misses and snapshots "src" at a digest that is not r2's.
// r2's own came from a cache hit and has no index, so diffing the two is
// the case where the digests disagree and no index can say how.
func seedAThirdRunWithADifferentWorkspace(t *testing.T, base string) {
	t.Helper()
	cacheDir := filepath.Join(base, "cache")

	ws := senro.Workspace("src", senro.Scope(senro.ScopeRun))
	pipe := senro.New("ci")
	l := pipe.Workflow("main")
	l.Step("seed", exec.Command("sh", "-c", "printf 'package main // r3\\n' > main.go")).
		WorkDir("/src").Mount(ws.At("/src", senro.RW))
	l.Step("compile", exec.Command("sh", "-c", "echo compiled | tee out.txt")).
		Needs("seed").WorkDir("/src").Mount(ws.At("/src", senro.RW)).
		Pure().Inputs(artifact.Glob("**/*.go")).Outputs(artifact.File("out.txt"))
	dir := filepath.Join(base, "runs", "r3")
	if err := senro.Run(context.Background(), pipe,
		senro.WithDir(dir), senro.WithRunID("r3"), senro.WithCacheDir(cacheDir)); err != nil {
		t.Fatalf("seedAThirdRunWithADifferentWorkspace: Run r3: %v", err)
	}
}

// seedDivergingRuns seeds runs/r1 and runs/r2 with one impure step, so both
// snapshot fresh rather than ending on an index-less ws.restored, writing
// workspaces that differ in all four ways a diff can report: added,
// removed, rewritten, and mode-only.
func seedDivergingRuns(t *testing.T, base string) {
	t.Helper()
	cacheDir := filepath.Join(base, "cache")
	t.Setenv("SENRO_CACHE_DIR", cacheDir)

	run := func(id, script string) {
		ws := senro.Workspace("src", senro.Scope(senro.ScopeRun))
		pipe := senro.New("ci")
		l := pipe.Workflow("main")
		l.Step("seed", exec.Command("sh", "-c", script)).
			WorkDir("/src").Mount(ws.At("/src", senro.RW))
		dir := filepath.Join(base, "runs", id)
		if err := senro.Run(context.Background(), pipe,
			senro.WithDir(dir), senro.WithRunID(id), senro.WithCacheDir(cacheDir)); err != nil {
			t.Fatalf("seedDivergingRuns: Run %s: %v", id, err)
		}
	}
	run("r1", "printf 'v1\\n' > main.go; printf 'x\\n' > gone.txt; printf '#!/bin/sh\\n' > build.sh; chmod 644 build.sh")
	run("r2", "printf 'v2\\n' > main.go; printf 'y\\n' > added.txt; printf '#!/bin/sh\\n' > build.sh; chmod 755 build.sh")
}

// seedRunsWithAWorkspaceOnlyOneRunHas is the shape a diff between two runs
// of DIFFERENT pipelines has: one workspace in common, one that exists on
// one side only.
func seedRunsWithAWorkspaceOnlyOneRunHas(t *testing.T, base string) {
	t.Helper()
	cacheDir := filepath.Join(base, "cache")
	t.Setenv("SENRO_CACHE_DIR", cacheDir)

	run := func(id string, withExtra bool) {
		src := senro.Workspace("src", senro.Scope(senro.ScopeRun))
		pipe := senro.New("ci")
		l := pipe.Workflow("main")
		l.Step("seed", exec.Command("sh", "-c", "printf '"+id+"\\n' > main.go")).
			WorkDir("/src").Mount(src.At("/src", senro.RW))
		if withExtra {
			extra := senro.Workspace("extra", senro.Scope(senro.ScopeRun))
			l.Step("aside", exec.Command("sh", "-c", "printf 'aside\\n' > note.txt")).
				WorkDir("/extra").Mount(extra.At("/extra", senro.RW))
		}
		dir := filepath.Join(base, "runs", id)
		if err := senro.Run(context.Background(), pipe,
			senro.WithDir(dir), senro.WithRunID(id), senro.WithCacheDir(cacheDir)); err != nil {
			t.Fatalf("seedRunsWithAWorkspaceOnlyOneRunHas: Run %s: %v", id, err)
		}
	}
	run("r1", false)
	run("r2", true)
}

// seedRunWithAHostileWorkspaceTarball writes runs/evil, whose ledger names
// a body no senro ever produced: a tar carrying "../escaped.txt".
// Hand-built, because WriteTar cannot emit one, which is exactly why the
// READER has to be what refuses it.
func seedRunWithAHostileWorkspaceTarball(t *testing.T, base string) {
	t.Helper()
	cacheDir := filepath.Join(base, "cache")
	t.Setenv("SENRO_CACHE_DIR", cacheDir)

	const payload = "pwned\n"
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	hdr := &tar.Header{
		Name: "../escaped.txt", Mode: 0o644, Size: int64(len(payload)),
		Typeflag: tar.TypeReg, Format: tar.FormatPAX,
	}
	if err := tw.WriteHeader(hdr); err != nil {
		t.Fatalf("write hostile header: %v", err)
	}
	if _, err := tw.Write([]byte(payload)); err != nil {
		t.Fatalf("write hostile body: %v", err)
	}
	if err := tw.Close(); err != nil {
		t.Fatalf("close hostile tar: %v", err)
	}

	store, err := storage.Open(cacheDir)
	if err != nil {
		t.Fatalf("storage.Open: %v", err)
	}
	digest, err := store.CAS.Put(context.Background(), bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatalf("store hostile tarball: %v", err)
	}
	_ = store.Close()

	dir := filepath.Join(base, "runs", "evil")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", dir, err)
	}
	writeEventLog(t, dir, api.Event{
		Type:    api.WSSnapshot,
		Payload: mustJSON(t, api.WSSnapshotBody{Name: "src", Digest: string(digest), Bytes: int64(len(payload)), Files: 1}),
	})
}

// writeEventLog writes a minimal, well-formed events.jsonl holding exactly
// the events given, so a test can present the ws commands with a ledger no
// real run would produce.
func writeEventLog(t *testing.T, dir string, events ...api.Event) {
	t.Helper()
	var buf bytes.Buffer
	for i, e := range events {
		e.V = api.Version
		e.Seq = uint64(i + 1)
		e.TS = time.Unix(0, 0).UTC()
		b, err := json.Marshal(e)
		if err != nil {
			t.Fatalf("marshal event %d: %v", i, err)
		}
		buf.Write(b)
		buf.WriteByte('\n')
	}
	if err := os.WriteFile(filepath.Join(dir, "events.jsonl"), buf.Bytes(), 0o644); err != nil {
		t.Fatalf("write events.jsonl: %v", err)
	}
}

func mustJSON(t *testing.T, v any) json.RawMessage {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	return b
}

// wsBodyDigest is wsIndexDigest's sibling: the workspace's BODY digest,
// so a test can delete the tarball and prove a command never opens it.
func wsBodyDigest(t *testing.T, dir, name string) cas.Digest {
	t.Helper()
	events, err := eventlog.Read(filepath.Join(dir, "events.jsonl"))
	if err != nil && len(events) == 0 {
		t.Fatalf("wsBodyDigest: read %s: %v", dir, err)
	}
	var found cas.Digest
	for _, e := range events {
		if e.Type != api.WSSnapshot {
			continue
		}
		var b api.WSSnapshotBody
		if err := e.Decode(&b); err != nil {
			continue
		}
		if b.Name == name && b.Digest != "" {
			found = cas.Digest(b.Digest)
		}
	}
	if found == "" {
		t.Fatalf("wsBodyDigest: no ws.snapshot for workspace %q in %s", name, dir)
	}
	return found
}

// Bodies with DIFFERENT digests whose indexes agree means something is off,
// and flattening that into "identical in both runs" would hide the one
// thing worth seeing. Unreachable through any run this build produces, so
// the formatter is exercised directly.
func TestWSDiffDoesNotCallTwoDifferentBodiesIdentical(t *testing.T) {
	var b bytes.Buffer
	formatDiffWorkspace(&b, wsDiffWorkspace{
		Name:      "src",
		A:         &wsDiffSide{Run: "r1", Dir: "runs/r1", Digest: "sha256:aaaa"},
		B:         &wsDiffSide{Run: "r2", Dir: "runs/r2", Digest: "sha256:bbbb"},
		Identical: true,
		Changes:   []wsDiffChange{},
		Summary:   &wsDiffSummary{Unchanged: 3},
	})
	got := b.String()
	if strings.Contains(got, "identical in both runs") {
		t.Errorf("two different body digests were reported as identical:\n%s", got)
	}
	if !strings.Contains(got, "sha256:aaaa") || !strings.Contains(got, "sha256:bbbb") {
		t.Errorf("the output does not show both digests, which is the whole anomaly:\n%s", got)
	}

	b.Reset()
	formatDiffWorkspace(&b, wsDiffWorkspace{
		Name:      "src",
		A:         &wsDiffSide{Run: "r1", Dir: "runs/r1", Digest: "sha256:aaaa"},
		B:         &wsDiffSide{Run: "r2", Dir: "runs/r2", Digest: "sha256:aaaa"},
		Identical: true,
		Changes:   []wsDiffChange{},
	})
	if !strings.Contains(b.String(), "identical in both runs") {
		t.Errorf("two snapshots at the same content address are the same tree:\n%s", b.String())
	}
}

// A forced ws.snapshot is what the ws.snapshot CONTROL operation emits: a
// mid-run capture an operator asked for, never part of what the run
// produced. It arrives on the same event type as a real one and would
// otherwise win by being later, so `ws ls` would report a digest the run
// itself never used. Skipping it is what keeps these three commands
// describing the run.
func TestWSLsIgnoresAForcedSnapshot(t *testing.T) {
	base := t.TempDir()
	t.Chdir(base)
	seedRuns(t, base)

	dir := filepath.Join(base, "runs", "r2")
	settled := wsBodyDigest(t, dir, "src")
	forced := "sha256:" + strings.Repeat("f", 64)

	events, err := eventlog.Read(filepath.Join(dir, "events.jsonl"))
	if err != nil && len(events) == 0 {
		t.Fatalf("read %s: %v", dir, err)
	}
	// Appended last, so only the "forced" flag can keep it from winning.
	events = append(events, api.Event{
		Type: api.WSSnapshot, Step: "compile",
		Payload: mustJSON(t, api.WSSnapshotBody{
			Name: "src", Digest: forced, Bytes: 1, Files: 1, Forced: true,
		}),
	})
	writeEventLog(t, dir, events...)

	var out, errOut bytes.Buffer
	if code := cmdWS([]string{"ls", "r2"}, &out, &errOut); code != exitSuccess {
		t.Fatalf("exit = %d, stderr: %s", code, errOut.String())
	}
	if strings.Contains(out.String(), forced) {
		t.Errorf("ws ls reported the forced capture's digest as the workspace's state:\n%s", out.String())
	}
	if !strings.Contains(out.String(), string(settled)) {
		t.Errorf("ws ls no longer reports the digest the run actually produced (%s):\n%s", settled, out.String())
	}
}
