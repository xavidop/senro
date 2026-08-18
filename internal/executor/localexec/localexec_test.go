package localexec_test

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/xavidop/senro/internal/cas"
	"github.com/xavidop/senro/internal/executor"
	"github.com/xavidop/senro/internal/executor/localexec"
	"github.com/xavidop/senro/internal/workspace"
)

// newLocal returns an Executor backed by a fresh, isolated CAS under
// t.TempDir(). It calls cas.Open directly rather than storage.Open, which is
// why this package needs no TestMain for
// TestEveryTestPackageThatCanReachTheDefaultCacheRootHasIsolation.
func newLocal(t *testing.T, root string) executor.Executor {
	t.Helper()
	store, err := cas.Open(filepath.Join(t.TempDir(), "cas"))
	if err != nil {
		t.Fatalf("cas.Open: %v", err)
	}
	return localexec.New(root, workspace.NewSnapshotter(store))
}

func newSandbox(t *testing.T) executor.Sandbox {
	t.Helper()
	ex := newLocal(t, t.TempDir())
	sb, err := ex.Sandbox(context.Background(), executor.SandboxSpec{StepID: "a", Attempt: 1})
	if err != nil {
		t.Fatalf("Sandbox: %v", err)
	}
	t.Cleanup(func() { _ = sb.Close(context.Background(), false) })
	return sb
}

func TestRunCapturesStdoutAndExitZero(t *testing.T) {
	sb := newSandbox(t)
	var out, errb bytes.Buffer

	exit, err := sb.Run(context.Background(),
		executor.Cmd{Args: []string{"echo", "hello"}}, &out, &errb)
	if err != nil {
		t.Fatalf("Run returned an infrastructure error for a working command: %v", err)
	}
	if exit != 0 {
		t.Errorf("exit = %d, want 0", exit)
	}
	if out.String() != "hello\n" {
		t.Errorf("stdout = %q, want %q", out.String(), "hello\n")
	}
}

// The distinction this whole interface exists for: a command that runs and
// fails is NOT an infrastructure error.
func TestNonZeroExitIsNotAnError(t *testing.T) {
	sb := newSandbox(t)
	var out, errb bytes.Buffer

	exit, err := sb.Run(context.Background(),
		executor.Cmd{Args: []string{"sh", "-c", "echo oops >&2; exit 3"}}, &out, &errb)
	if err != nil {
		t.Fatalf("a non-zero exit must not be an error, got %v", err)
	}
	if exit != 3 {
		t.Errorf("exit = %d, want 3", exit)
	}
	if errb.String() != "oops\n" {
		t.Errorf("stderr = %q", errb.String())
	}
}

// A command that cannot start is infrastructure failure, and must classify.
func TestMissingBinaryIsInfraFailure(t *testing.T) {
	sb := newSandbox(t)
	var out, errb bytes.Buffer

	_, err := sb.Run(context.Background(),
		executor.Cmd{Args: []string{"senro-no-such-binary-xyz"}}, &out, &errb)
	if err == nil {
		t.Fatal("a missing binary must be an error")
	}
	if !executor.IsInfra(err) {
		t.Errorf("a missing binary must classify as infra failure, got %v", err)
	}
}

func TestDeclaredPlatformIsThisHost(t *testing.T) {
	p, err := newLocal(t, t.TempDir()).DeclaredPlatform(context.Background())
	if err != nil {
		t.Fatalf("DeclaredPlatform: %v", err)
	}
	if p.OS != runtime.GOOS || p.Arch != runtime.GOARCH {
		t.Errorf("Platform = %s, want %s/%s", p, runtime.GOOS, runtime.GOARCH)
	}
}

func TestContextCancellationStopsTheCommand(t *testing.T) {
	sb := newSandbox(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	var out, errb bytes.Buffer
	_, err := sb.Run(ctx, executor.Cmd{Args: []string{"sleep", "30"}}, &out, &errb)
	if err == nil {
		t.Fatal("a cancelled context must stop the command")
	}
}

// Cancelling *after* the process has started, unlike the test above. Before
// Start, exec.Cmd returns ctx.Err() directly; mid-flight, the kill makes the
// process die by signal and cmd.Run reports an *exec.ExitError that would
// look like an ordinary non-zero exit without the ctx.Err() check. Both
// paths must converge on infra, and the command must actually be killed.
func TestContextCancellationDuringRunIsInfra(t *testing.T) {
	sb := newSandbox(t)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	go func() {
		time.Sleep(150 * time.Millisecond)
		cancel()
	}()

	var out, errb bytes.Buffer
	start := time.Now()
	_, err := sb.Run(ctx, executor.Cmd{Args: []string{"sleep", "30"}}, &out, &errb)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("cancelling a running command must stop it and return an error")
	}
	if !executor.IsInfra(err) {
		t.Errorf("a mid-run cancellation must classify as infra failure, got %v", err)
	}
	// The process must be killed promptly rather than left to run out the
	// full 30s sleep. A generous bound keeps this from being flaky under
	// load while still catching "cancellation was ignored".
	if elapsed > 10*time.Second {
		t.Errorf("Run took %s after cancellation; the child process was not killed promptly", elapsed)
	}
}

// This test and its sibling below prove cmd.WaitDelay actually bounds the
// wait: a step that backgrounds anything would otherwise keep cmd.Run
// blocked for as long as the orphan lived (see waitDelay). Both are
// t.Parallel so the two grace periods overlap rather than add up.
func TestBackgroundProcessDoesNotHoldTheRunOpen(t *testing.T) {
	t.Parallel()
	sb := newSandbox(t)

	var out, errb bytes.Buffer
	start := time.Now()
	// The step itself finishes immediately; the orphan it leaves behind holds
	// the write end of stdout for 30 seconds.
	_, err := sb.Run(context.Background(),
		executor.Cmd{Args: []string{"sh", "-c", "sleep 30 & echo spawned"}}, &out, &errb)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	// The bound is deliberately loose: it must sit well above the 5s grace
	// period (so a loaded machine does not fail it) and well below the
	// orphan's own 30s lifetime (so "we waited for the orphan" cannot pass).
	if elapsed > 15*time.Second {
		t.Errorf("Run took %s for a step that exited immediately — it waited on the "+
			"background process instead of bounding the wait", elapsed)
	}
	if got := out.String(); got != "spawned\n" {
		t.Errorf("stdout = %q, want %q — output written before the step exited must "+
			"still be captured", got, "spawned\n")
	}
}

// A truncated log tail is not a failed workload: a command that exited zero
// but left a pipe open makes Wait return exec.ErrWaitDelay, and classifying
// that as infra would turn every step that starts a service into a false
// red.
func TestStepLeavingABackgroundProcessStillSucceeds(t *testing.T) {
	t.Parallel()
	sb := newSandbox(t)

	var out, errb bytes.Buffer
	exit, err := sb.Run(context.Background(),
		executor.Cmd{Args: []string{"sh", "-c", "sleep 30 & echo up"}}, &out, &errb)

	if err != nil {
		t.Fatalf("Run returned an error for a step that exited 0: %v — "+
			"WaitDelay expiry must not be reported as infrastructure failure", err)
	}
	if executor.IsInfra(err) {
		t.Errorf("err = %v, want no infra classification", err)
	}
	if exit != 0 {
		t.Errorf("exit = %d, want 0 — the workload succeeded", exit)
	}
}

// The other half of the WaitDelay classification: a step that leaves an
// orphan AND genuinely fails must keep its exit code and stay a workload
// verdict rather than becoming either a success or an infra error.
func TestBackgroundProcessDoesNotMaskANonZeroExit(t *testing.T) {
	t.Parallel()
	sb := newSandbox(t)

	var out, errb bytes.Buffer
	exit, err := sb.Run(context.Background(),
		executor.Cmd{Args: []string{"sh", "-c", "sleep 30 & echo up; exit 7"}}, &out, &errb)

	if err != nil {
		t.Fatalf("a non-zero exit must not be an error, got %v", err)
	}
	if exit != 7 {
		t.Errorf("exit = %d, want 7", exit)
	}
}

func TestRelativeDirIsRelativeToTheSandbox(t *testing.T) {
	root := t.TempDir()
	ex := newLocal(t, root)
	sb, err := ex.Sandbox(context.Background(), executor.SandboxSpec{StepID: "a", Attempt: 1})
	if err != nil {
		t.Fatalf("Sandbox: %v", err)
	}
	defer func() { _ = sb.Close(context.Background(), false) }()

	var out, errb bytes.Buffer
	// Ask for a relative subdirectory and print where we actually landed.
	if _, err := sb.Run(context.Background(),
		executor.Cmd{Args: []string{"sh", "-c", "mkdir -p sub && cd sub && pwd"}}, &out, &errb); err != nil {
		t.Fatalf("Run: %v", err)
	}
	base := strings.TrimSpace(out.String())

	out.Reset()
	errb.Reset()
	exit, err := sb.Run(context.Background(),
		executor.Cmd{Args: []string{"pwd"}, Dir: "sub"}, &out, &errb)
	if err != nil {
		t.Fatalf("Run with relative Dir: %v", err)
	}
	if exit != 0 {
		t.Fatalf("exit = %d, stderr = %q", exit, errb.String())
	}
	got := strings.TrimSpace(out.String())
	if got != base {
		t.Errorf("relative Dir resolved to %q, want %q — a relative workdir must stay inside the sandbox", got, base)
	}
}

func TestEmptyEnvDoesNotInheritTheCoordinators(t *testing.T) {
	t.Setenv("SENRO_LEAK_PROBE", "leaked")

	sb := newSandbox(t)
	var out, errb bytes.Buffer
	if _, err := sb.Run(context.Background(),
		executor.Cmd{Args: []string{"sh", "-c", "echo [$SENRO_LEAK_PROBE]"}}, &out, &errb); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got := strings.TrimSpace(out.String()); got != "[]" {
		t.Errorf("child saw %q — a step with no declared Env must not inherit the coordinator's", got)
	}
}

// PATH is the one exception to "empty means empty": a search path
// identifies the host, so it lives in the executor rather than the plan,
// where it would make plan.Digest() vary with the operator's own $PATH.
//
// Asserts the value the child actually sees rather than whether some binary
// resolved: /bin/sh has a compiled-in fallback search path.
func TestStepWithNoEnvStillGetsTheHostPATH(t *testing.T) {
	want := os.Getenv("PATH")
	if want == "" {
		t.Skip("coordinator has no PATH; nothing to propagate")
	}

	sb := newSandbox(t)
	var out, errb bytes.Buffer
	if _, err := sb.Run(context.Background(),
		executor.Cmd{Args: []string{"sh", "-c", "echo $PATH"}}, &out, &errb); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got := strings.TrimSpace(out.String()); got != want {
		t.Errorf("child $PATH = %q, want the host's %q", got, want)
	}
}

// A PATH the plan declares is a deliberate part of the pipeline and wins
// outright. The executor must not append its own behind it.
func TestDeclaredPATHIsNotOverridden(t *testing.T) {
	sb := newSandbox(t)
	var out, errb bytes.Buffer
	if _, err := sb.Run(context.Background(),
		executor.Cmd{
			Args: []string{"/bin/sh", "-c", "echo $PATH"},
			Env:  []string{"PATH=/senro/declared/bin"},
		}, &out, &errb); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got := strings.TrimSpace(out.String()); got != "/senro/declared/bin" {
		t.Errorf("child $PATH = %q, want the declared %q", got, "/senro/declared/bin")
	}
}

// EffectiveEnv must report the SAME PATH addition Run gives the step: it
// literally calls envWithDefaultPATH rather than recomputing it, and this
// pins that wiring so a cache key cannot drift from what the step receives.
func TestEffectiveEnvAddsTheExecutorsDefaultPATH(t *testing.T) {
	ex := localexec.New(t.TempDir(), nil)
	got, err := ex.EffectiveEnv(context.Background(), []string{"FOO=1"})
	if err != nil {
		t.Fatalf("EffectiveEnv: %v", err)
	}
	var sawPath, sawFoo bool
	for _, kv := range got {
		if strings.HasPrefix(kv, "PATH=") {
			sawPath = true
		}
		if kv == "FOO=1" {
			sawFoo = true
		}
	}
	if !sawFoo {
		t.Errorf("EffectiveEnv dropped a declared variable: %v", got)
	}
	if !sawPath {
		t.Errorf("EffectiveEnv did not add PATH for a step that declared none: %v", got)
	}
}

// The negative half, mirroring TestDeclaredPATHIsNotOverridden: a step that
// declares its own PATH must keep it in EffectiveEnv's report too.
func TestEffectiveEnvDoesNotOverrideADeclaredPATH(t *testing.T) {
	ex := localexec.New(t.TempDir(), nil)
	got, err := ex.EffectiveEnv(context.Background(), []string{"PATH=/custom/bin"})
	if err != nil {
		t.Fatalf("EffectiveEnv: %v", err)
	}
	if len(got) != 1 || got[0] != "PATH=/custom/bin" {
		t.Errorf("EffectiveEnv = %v, want the declared PATH unchanged", got)
	}
}

func TestAMountAtTheWorkDirBecomesTheWorkingDirectory(t *testing.T) {
	root := t.TempDir()
	wsDir := filepath.Join(t.TempDir(), "ws-src")
	if err := os.MkdirAll(wsDir, 0o755); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if err := os.WriteFile(filepath.Join(wsDir, "marker.txt"), []byte("here\n"), 0o644); err != nil {
		t.Fatalf("seed: %v", err)
	}

	ex := newLocal(t, root)
	sb, err := ex.Sandbox(context.Background(), executor.SandboxSpec{
		StepID: "s", Attempt: 1, WorkDir: "/src",
		Mounts: []executor.Mount{{Name: "src", At: "/src", Path: wsDir}},
	})
	if err != nil {
		t.Fatalf("Sandbox: %v", err)
	}
	defer func() { _ = sb.Close(context.Background(), false) }()

	var out bytes.Buffer
	exit, err := sb.Run(context.Background(), executor.Cmd{Args: []string{"cat", "marker.txt"}}, &out, io.Discard)
	if err != nil || exit != 0 {
		t.Fatalf("Run = %d, %v", exit, err)
	}
	if out.String() != "here\n" {
		t.Errorf("the step did not run inside the mounted workspace: %q", out.String())
	}
}

func TestAMountElsewhereIsReachableFromTheSandbox(t *testing.T) {
	root := t.TempDir()
	wsDir := filepath.Join(t.TempDir(), "ws-cache")
	if err := os.MkdirAll(wsDir, 0o755); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if err := os.WriteFile(filepath.Join(wsDir, "hit.txt"), []byte("cached\n"), 0o644); err != nil {
		t.Fatalf("seed: %v", err)
	}

	ex := newLocal(t, root)
	sb, err := ex.Sandbox(context.Background(), executor.SandboxSpec{
		StepID: "s", Attempt: 1,
		Mounts: []executor.Mount{{Name: "cache", At: "/deps", Path: wsDir}},
	})
	if err != nil {
		t.Fatalf("Sandbox: %v", err)
	}
	defer func() { _ = sb.Close(context.Background(), false) }()

	var out bytes.Buffer
	exit, err := sb.Run(context.Background(), executor.Cmd{Args: []string{"cat", "deps/hit.txt"}}, &out, io.Discard)
	if err != nil || exit != 0 {
		t.Fatalf("Run = %d, %v", exit, err)
	}
	if out.String() != "cached\n" {
		t.Errorf("the mount was not reachable from the sandbox: %q", out.String())
	}
}

func TestAStepWritesThroughAMountIntoTheWorkspaceDirectory(t *testing.T) {
	root := t.TempDir()
	wsDir := filepath.Join(t.TempDir(), "ws-out")
	if err := os.MkdirAll(wsDir, 0o755); err != nil {
		t.Fatalf("seed: %v", err)
	}

	ex := newLocal(t, root)
	sb, err := ex.Sandbox(context.Background(), executor.SandboxSpec{
		StepID: "s", Attempt: 1,
		Mounts: []executor.Mount{{Name: "out", At: "/out", Path: wsDir}},
	})
	if err != nil {
		t.Fatalf("Sandbox: %v", err)
	}
	defer func() { _ = sb.Close(context.Background(), false) }()

	exit, err := sb.Run(context.Background(),
		executor.Cmd{Args: []string{"sh", "-c", "echo built > out/app"}}, io.Discard, io.Discard)
	if err != nil || exit != 0 {
		t.Fatalf("Run = %d, %v", exit, err)
	}
	b, err := os.ReadFile(filepath.Join(wsDir, "app"))
	if err != nil {
		t.Fatalf("the write did not land in the workspace directory: %v", err)
	}
	if string(b) != "built\n" {
		t.Errorf("workspace file = %q", b)
	}
}

func TestSnapshotReturnsARealDigestForAMountedWorkspace(t *testing.T) {
	root := t.TempDir()
	wsDir := filepath.Join(t.TempDir(), "ws")
	if err := os.MkdirAll(wsDir, 0o755); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if err := os.WriteFile(filepath.Join(wsDir, "a.txt"), []byte("a\n"), 0o644); err != nil {
		t.Fatalf("seed: %v", err)
	}

	ex := newLocal(t, root)
	sb, err := ex.Sandbox(context.Background(), executor.SandboxSpec{
		StepID: "s", Attempt: 1,
		Mounts: []executor.Mount{{Name: "ws", At: "/ws", Path: wsDir}},
	})
	if err != nil {
		t.Fatalf("Sandbox: %v", err)
	}
	defer func() { _ = sb.Close(context.Background(), false) }()

	snap, err := sb.Snapshot(context.Background(), "ws")
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if !cas.Digest(snap.Digest).Valid() {
		t.Errorf("Snapshot returned %q, which is not a digest", snap.Digest)
	}
	if !cas.Digest(snap.Index).Valid() {
		t.Errorf("Snapshot returned index %q, which is not a digest", snap.Index)
	}
	if snap.Files != 1 || snap.Bytes != 2 {
		t.Errorf("Snapshot = %+v, want one file of two bytes", snap)
	}
}

// The negative half. A snapshot of something the sandbox does not have must
// be an error, not an empty digest: an empty digest is a perfectly stable
// content address for "nothing" and would poison every key downstream.
func TestSnapshotOfAnUnmountedNameIsAnError(t *testing.T) {
	ex := newLocal(t, t.TempDir())
	sb, err := ex.Sandbox(context.Background(), executor.SandboxSpec{StepID: "s", Attempt: 1})
	if err != nil {
		t.Fatalf("Sandbox: %v", err)
	}
	defer func() { _ = sb.Close(context.Background(), false) }()

	snap, err := sb.Snapshot(context.Background(), "nope")
	if err == nil {
		t.Fatalf("Snapshot of an unmounted workspace returned %+v and no error", snap)
	}
	if snap.Digest != "" {
		t.Errorf("a failed Snapshot still returned a digest: %q", snap.Digest)
	}
	// Without the explicit lookup, the zero-value Mount's empty Path would
	// still fail downstream, but with a confusing "stat :" message instead
	// of naming the undeclared mount; pin the message too.
	if !strings.Contains(err.Error(), `no mount named "nope"`) {
		t.Errorf("error = %q, want it to name the missing mount", err.Error())
	}
}

func TestSnapshotHonoursAMountsExcludes(t *testing.T) {
	wsDir := filepath.Join(t.TempDir(), "ws")
	for _, p := range []string{"keep.go", "drop.tmp"} {
		if err := os.MkdirAll(wsDir, 0o755); err != nil {
			t.Fatalf("seed: %v", err)
		}
		if err := os.WriteFile(filepath.Join(wsDir, p), []byte("x\n"), 0o644); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}

	ex := newLocal(t, t.TempDir())
	sb, err := ex.Sandbox(context.Background(), executor.SandboxSpec{
		StepID: "s", Attempt: 1,
		Mounts: []executor.Mount{{Name: "ws", At: "/ws", Path: wsDir, Exclude: []string{"**/*.tmp"}}},
	})
	if err != nil {
		t.Fatalf("Sandbox: %v", err)
	}
	defer func() { _ = sb.Close(context.Background(), false) }()

	snap, err := sb.Snapshot(context.Background(), "ws")
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if snap.Files != 1 {
		t.Errorf("Snapshot included %d files, want 1: the exclude was ignored", snap.Files)
	}
}

// .git and node_modules are excluded from every workspace whether or not the
// pipeline says so: this exclusion is mandatory, not merely a default.
func TestSnapshotAlwaysExcludesTheDefaultDirectories(t *testing.T) {
	wsDir := filepath.Join(t.TempDir(), "ws")
	if err := os.MkdirAll(filepath.Join(wsDir, ".git"), 0o755); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if err := os.WriteFile(filepath.Join(wsDir, ".git", "HEAD"), []byte("ref\n"), 0o644); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if err := os.WriteFile(filepath.Join(wsDir, "a.go"), []byte("package a\n"), 0o644); err != nil {
		t.Fatalf("seed: %v", err)
	}

	ex := newLocal(t, t.TempDir())
	sb, err := ex.Sandbox(context.Background(), executor.SandboxSpec{
		StepID: "s", Attempt: 1,
		Mounts: []executor.Mount{{Name: "ws", At: "/ws", Path: wsDir}},
	})
	if err != nil {
		t.Fatalf("Sandbox: %v", err)
	}
	defer func() { _ = sb.Close(context.Background(), false) }()

	snap, err := sb.Snapshot(context.Background(), "ws")
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if snap.Files != 1 {
		t.Errorf("Snapshot included %d entries, want 1: .git reached the snapshot", snap.Files)
	}
}

// The mount end of senro.PreserveSymlinks: an opted-in mount keeps its own
// directories literally named "node_modules" (where a pnpm-shaped tree's
// symlink targets live), while .git stays excluded either way.
func TestSnapshotWithPreserveSymlinksKeepsNodeModulesButNotGit(t *testing.T) {
	wsDir := filepath.Join(t.TempDir(), "ws")
	if err := os.MkdirAll(filepath.Join(wsDir, ".git"), 0o755); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if err := os.WriteFile(filepath.Join(wsDir, ".git", "HEAD"), []byte("ref\n"), 0o644); err != nil {
		t.Fatalf("seed: %v", err)
	}
	nested := filepath.Join(wsDir, "pkg", "node_modules", "left-pad")
	if err := os.MkdirAll(nested, 0o755); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if err := os.WriteFile(filepath.Join(nested, "index.js"), []byte("module.exports = 1\n"), 0o644); err != nil {
		t.Fatalf("seed: %v", err)
	}

	ex := newLocal(t, t.TempDir())
	sb, err := ex.Sandbox(context.Background(), executor.SandboxSpec{
		StepID: "s", Attempt: 1,
		Mounts: []executor.Mount{{Name: "ws", At: "/ws", Path: wsDir, PreserveSymlinks: true}},
	})
	if err != nil {
		t.Fatalf("Sandbox: %v", err)
	}
	defer func() { _ = sb.Close(context.Background(), false) }()

	snap, err := sb.Snapshot(context.Background(), "ws")
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	// pkg, pkg/node_modules, pkg/node_modules/left-pad and its index.js:
	// four entries; .git contributes none.
	if snap.Files != 4 {
		t.Errorf("Snapshot included %d entries, want 4 (PreserveSymlinks widened node_modules but not .git)", snap.Files)
	}
}

// A mount whose path escapes the sandbox is a declaration senro built, not
// user input, but a symlink written outside the sandbox is a filesystem
// write nobody asked for, so it is refused rather than trusted.
func TestAMountPathThatEscapesTheSandboxIsRefused(t *testing.T) {
	ex := newLocal(t, t.TempDir())
	_, err := ex.Sandbox(context.Background(), executor.SandboxSpec{
		StepID: "s", Attempt: 1,
		Mounts: []executor.Mount{{Name: "bad", At: "../../escape", Path: t.TempDir()}},
	})
	if err == nil {
		t.Fatal("a mount at a path escaping the sandbox was accepted")
	}
}

// A missing mount source is infrastructure, not workload: letting Sandbox
// succeed would create a dangling symlink and the step would meet the
// missing workspace as an ordinary read error.
func TestAMountWhoseSourceDoesNotExistIsRefused(t *testing.T) {
	ex := newLocal(t, t.TempDir())
	_, err := ex.Sandbox(context.Background(), executor.SandboxSpec{
		StepID: "s", Attempt: 1,
		Mounts: []executor.Mount{{Name: "ghost", At: "/ghost", Path: filepath.Join(t.TempDir(), "does-not-exist")}},
	})
	if err == nil {
		t.Fatal("a mount whose source does not exist was accepted")
	}
	if !executor.IsInfra(err) {
		t.Errorf("a missing mount source must classify as infra failure, got %v", err)
	}
}

// plan.validateStorage refuses this shape, but a SandboxSpec can be built
// directly, so the executor must refuse two mounts at one path itself: the
// second os.Symlink finds the first mount's link already there.
func TestTwoMountsAtTheSamePathAreRefused(t *testing.T) {
	wsA := filepath.Join(t.TempDir(), "ws-a")
	wsB := filepath.Join(t.TempDir(), "ws-b")
	for _, d := range []string{wsA, wsB} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}

	ex := newLocal(t, t.TempDir())
	_, err := ex.Sandbox(context.Background(), executor.SandboxSpec{
		StepID: "s", Attempt: 1,
		Mounts: []executor.Mount{
			{Name: "a", At: "/dup", Path: wsA},
			{Name: "b", At: "/dup", Path: wsB},
		},
	})
	if err == nil {
		t.Fatal("two mounts at the same sandbox path were both realized")
	}
	if !executor.IsInfra(err) {
		t.Errorf("a symlink collision must classify as infra failure, got %v", err)
	}
}

// Snapshot of a workspace the step deleted must fail, not return a digest
// for a missing directory: workspace.Snapshotter.Snapshot refuses a missing
// root, and localexec must not swallow that.
func TestSnapshotOfAWorkspaceTheStepDeletedIsAnError(t *testing.T) {
	wsDir := filepath.Join(t.TempDir(), "ws")
	if err := os.MkdirAll(wsDir, 0o755); err != nil {
		t.Fatalf("seed: %v", err)
	}

	ex := newLocal(t, t.TempDir())
	sb, err := ex.Sandbox(context.Background(), executor.SandboxSpec{
		StepID: "s", Attempt: 1,
		Mounts: []executor.Mount{{Name: "ws", At: "/ws", Path: wsDir}},
	})
	if err != nil {
		t.Fatalf("Sandbox: %v", err)
	}
	defer func() { _ = sb.Close(context.Background(), false) }()

	if err := os.RemoveAll(wsDir); err != nil {
		t.Fatalf("simulate the step deleting its workspace: %v", err)
	}

	snap, err := sb.Snapshot(context.Background(), "ws")
	if err == nil {
		t.Fatalf("Snapshot of a deleted workspace returned %+v and no error", snap)
	}
}

// RO is carried through for executors that can bind-mount read-only, but
// the local executor shares the coordinator's filesystem through a plain
// symlink, and enforcing read-only would mutate the permissions of the
// coordinator-side directory itself while concurrent steps or the run's own
// snapshot may be reading it. Pins the deliberate behaviour so a future
// change is a decision, not an accident.
func TestAReadOnlyMountIsNotEnforcedByTheLocalExecutor(t *testing.T) {
	wsDir := filepath.Join(t.TempDir(), "ws")
	if err := os.MkdirAll(wsDir, 0o755); err != nil {
		t.Fatalf("seed: %v", err)
	}

	ex := newLocal(t, t.TempDir())
	sb, err := ex.Sandbox(context.Background(), executor.SandboxSpec{
		StepID: "s", Attempt: 1,
		Mounts: []executor.Mount{{Name: "ro", At: "/ro", Path: wsDir, RO: true}},
	})
	if err != nil {
		t.Fatalf("Sandbox: %v", err)
	}
	defer func() { _ = sb.Close(context.Background(), false) }()

	exit, err := sb.Run(context.Background(),
		executor.Cmd{Args: []string{"sh", "-c", "echo written > ro/file"}}, io.Discard, io.Discard)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if exit != 0 {
		t.Fatalf("exit = %d, want 0 — RO is not enforced at this layer, see the doc comment above", exit)
	}
	if _, err := os.Stat(filepath.Join(wsDir, "file")); err != nil {
		t.Errorf("write through an RO mount did not land, contradicting the doc comment above: %v", err)
	}
}

// Proves the guard in Run's directory resolution: the engine passes the
// same absolute path as both SandboxSpec.WorkDir and Cmd.Dir (see
// internal/engine/attempt.go), and without the guard Run would try to chdir
// into the literal string "/src".
func TestCmdDirEqualToARealizedWorkDirStaysInTheSandbox(t *testing.T) {
	wsDir := filepath.Join(t.TempDir(), "ws-src")
	if err := os.MkdirAll(wsDir, 0o755); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if err := os.WriteFile(filepath.Join(wsDir, "marker.txt"), []byte("here\n"), 0o644); err != nil {
		t.Fatalf("seed: %v", err)
	}

	ex := newLocal(t, t.TempDir())
	sb, err := ex.Sandbox(context.Background(), executor.SandboxSpec{
		StepID: "s", Attempt: 1, WorkDir: "/src",
		Mounts: []executor.Mount{{Name: "src", At: "/src", Path: wsDir}},
	})
	if err != nil {
		t.Fatalf("Sandbox: %v", err)
	}
	defer func() { _ = sb.Close(context.Background(), false) }()

	var out bytes.Buffer
	exit, err := sb.Run(context.Background(),
		executor.Cmd{Args: []string{"cat", "marker.txt"}, Dir: "/src"}, &out, io.Discard)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if exit != 0 {
		t.Fatalf("exit = %d, want 0 — Cmd.Dir equal to the realized WorkDir must not be treated as a host path", exit)
	}
	if out.String() != "here\n" {
		t.Errorf("the step did not run inside the mounted workspace: %q", out.String())
	}
}

// Proves the guard above is load-bearing rather than a no-op: with no
// matching mount, an absolute Cmd.Dir really is a host path and must still
// be honoured as one.
func TestAbsoluteCmdDirNotMatchingAMountIsAHostPath(t *testing.T) {
	real := t.TempDir()
	if err := os.WriteFile(filepath.Join(real, "here.txt"), []byte("real\n"), 0o644); err != nil {
		t.Fatalf("seed: %v", err)
	}

	sb := newSandbox(t)
	var out bytes.Buffer
	exit, err := sb.Run(context.Background(),
		executor.Cmd{Args: []string{"cat", "here.txt"}, Dir: real}, &out, io.Discard)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if exit != 0 {
		t.Fatalf("exit = %d, want 0", exit)
	}
	if out.String() != "real\n" {
		t.Errorf("an absolute Cmd.Dir with no matching mount must still be honoured as a host path: %q", out.String())
	}
}
