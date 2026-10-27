package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/xavidop/senro"
	"github.com/xavidop/senro/exec"
	"github.com/xavidop/senro/internal/attachsrv"
	"github.com/xavidop/senro/internal/plan"
	"github.com/xavidop/senro/internal/storage"
)

// seedOfflineShellRun runs a one-step pipeline whose step mounts a
// workspace at /src, works there, and leaves marker.txt behind, then
// returns the run directory and the cache root its snapshot went to. The
// attach registry is pointed at an empty directory, so nothing a developer
// has running can make the run look live.
func seedOfflineShellRun(t *testing.T) (runDir, cacheDir string) {
	t.Helper()
	base := t.TempDir()
	t.Chdir(base)
	t.Setenv("HOME", filepath.Join(base, "home"))
	t.Setenv("XDG_RUNTIME_DIR", filepath.Join(base, "registry"))
	cacheDir = filepath.Join(base, "cache")

	ws := senro.Workspace("src", senro.Scope(senro.ScopeRun))
	pipe := senro.New("ci")
	pipe.Workflow("main").
		Step("build", exec.Command("sh", "-c", "printf 'left behind\\n' > marker.txt")).
		WorkDir("/src").Mount(ws.At("/src", senro.RW))
	runDir = filepath.Join(base, "runs", "r1")
	if err := senro.Run(context.Background(), pipe,
		senro.WithDir(runDir), senro.WithRunID("r1"), senro.WithCacheDir(cacheDir)); err != nil {
		t.Fatalf("Run: %v", err)
	}
	return runDir, cacheDir
}

// offlineRoot is the directory the offline banner names.
func offlineRoot(t *testing.T, stderr string) string {
	t.Helper()
	const prefix = "senro shell: offline, workspaces restored to "
	for _, line := range strings.Split(stderr, "\n") {
		if rest, ok := strings.CutPrefix(line, prefix); ok {
			return rest
		}
	}
	t.Fatalf("stderr does not announce the restored directory:\n%s", stderr)
	return ""
}

func TestOfflineShellRestoresTheStepsSnapshotsAndRunsInTheWorkDir(t *testing.T) {
	runDir, cacheDir := seedOfflineShellRun(t)

	var out, errb bytes.Buffer
	code := cmdShell([]string{"--run", runDir, "--step", "build", "--offline", "--cache-dir", cacheDir,
		"--keep", "--cmd", "sh", "-c", "pwd; cat marker.txt"}, &out, &errb, strings.NewReader(""))
	if code != exitSuccess {
		t.Fatalf("exit = %d, stderr: %s", code, errb.String())
	}
	root := offlineRoot(t, errb.String())
	t.Cleanup(func() { _ = os.RemoveAll(root) })

	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("stdout = %q, want the working directory and the marker", out.String())
	}
	wantDir, err := filepath.EvalSymlinks(filepath.Join(root, "src"))
	if err != nil {
		t.Fatalf("the workspace was not restored at its mount path: %v", err)
	}
	gotDir, err := filepath.EvalSymlinks(lines[0])
	if err != nil {
		t.Fatalf("pwd printed %q: %v", lines[0], err)
	}
	if gotDir != wantDir {
		t.Errorf("the shell ran in %q, want the mount at the step's WorkDir, %q", gotDir, wantDir)
	}
	if lines[1] != "left behind" {
		t.Errorf("marker.txt = %q, want the content the step wrote", lines[1])
	}
}

// TestOfflineShellIsTheFallbackWhenTheRunIsNotLive: --offline is not needed
// to reach the offline form; a --run naming nothing live gets it anyway.
func TestOfflineShellIsTheFallbackWhenTheRunIsNotLive(t *testing.T) {
	runDir, cacheDir := seedOfflineShellRun(t)

	var out, errb bytes.Buffer
	code := cmdShell([]string{"--run", runDir, "--step", "build", "--cache-dir", cacheDir,
		"--cmd", "cat", "marker.txt"}, &out, &errb, strings.NewReader(""))
	if code != exitSuccess {
		t.Fatalf("exit = %d, stderr: %s", code, errb.String())
	}
	if strings.TrimSpace(out.String()) != "left behind" {
		t.Errorf("stdout = %q, want the marker's content", out.String())
	}
}

func TestOfflineShellRefusesAStepWithNoSnapshots(t *testing.T) {
	runDir, cacheDir := seedOfflineShellRun(t)
	// A step the plan knows but whose snapshots this run never recorded:
	// strip every event, keeping the plan.
	if err := os.WriteFile(filepath.Join(runDir, "events.jsonl"), nil, 0o644); err != nil {
		t.Fatal(err)
	}

	var out, errb bytes.Buffer
	code := cmdShell([]string{"--run", runDir, "--step", "build", "--offline", "--cache-dir", cacheDir,
		"--cmd", "true"}, &out, &errb, strings.NewReader(""))
	if code != exitUsage {
		t.Errorf("exit = %d, want %d", code, exitUsage)
	}
	want := `senro shell: step "build" recorded no workspace snapshots in run ` + runDir
	if !strings.Contains(errb.String(), want) {
		t.Errorf("stderr = %q, want %q", errb.String(), want)
	}
}

func TestOfflineShellRefusesAnUnknownStep(t *testing.T) {
	runDir, cacheDir := seedOfflineShellRun(t)

	var out, errb bytes.Buffer
	code := cmdShell([]string{"--run", runDir, "--step", "nope", "--offline", "--cache-dir", cacheDir,
		"--cmd", "true"}, &out, &errb, strings.NewReader(""))
	if code != exitUsage {
		t.Errorf("exit = %d, want %d", code, exitUsage)
	}
	if !strings.Contains(errb.String(), `"nope"`) {
		t.Errorf("stderr = %q, want it to name the step", errb.String())
	}
}

func TestOfflineShellRemovesItsDirectoryUnlessKept(t *testing.T) {
	runDir, cacheDir := seedOfflineShellRun(t)

	var out, errb bytes.Buffer
	code := cmdShell([]string{"--run", runDir, "--step", "build", "--offline", "--cache-dir", cacheDir,
		"--cmd", "true"}, &out, &errb, strings.NewReader(""))
	if code != exitSuccess {
		t.Fatalf("exit = %d, stderr: %s", code, errb.String())
	}
	root := offlineRoot(t, errb.String())
	if _, err := os.Stat(root); !os.IsNotExist(err) {
		t.Errorf("%s is still there after the shell exited without --keep (stat: %v)", root, err)
		_ = os.RemoveAll(root)
	}

	out.Reset()
	errb.Reset()
	code = cmdShell([]string{"--run", runDir, "--step", "build", "--offline", "--cache-dir", cacheDir,
		"--keep", "--cmd", "true"}, &out, &errb, strings.NewReader(""))
	if code != exitSuccess {
		t.Fatalf("exit = %d, stderr: %s", code, errb.String())
	}
	root = offlineRoot(t, errb.String())
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	if _, err := os.Stat(filepath.Join(root, "src", "marker.txt")); err != nil {
		t.Errorf("--keep did not keep the restored files: %v", err)
	}
	if !strings.Contains(errb.String(), "senro shell: kept "+root) {
		t.Errorf("stderr = %q, want it to say where the files were kept", errb.String())
	}
}

// TestOfflineShellExitCodeIsTheShells: the same contract the live form has.
func TestOfflineShellExitCodeIsTheShells(t *testing.T) {
	runDir, cacheDir := seedOfflineShellRun(t)

	var out, errb bytes.Buffer
	code := cmdShell([]string{"--run", runDir, "--step", "build", "--offline", "--cache-dir", cacheDir,
		"--cmd", "sh", "-c", "exit 7"}, &out, &errb, strings.NewReader(""))
	if code != 7 {
		t.Errorf("exit = %d, want the command's own 7; stderr: %s", code, errb.String())
	}
}

// rewritePlan applies edit to the recorded plan's build step and writes it
// back, for a case a real run cannot record.
func rewritePlan(t *testing.T, runDir string, edit func(n *plan.Node)) {
	t.Helper()
	p, err := readRunPlan(runDir)
	if err != nil {
		t.Fatal(err)
	}
	n, ok := p.Node("build")
	if !ok {
		t.Fatal("the seeded plan has no build step")
	}
	edit(n)
	b, err := p.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(runDir, "plan.json"), b, 0o644); err != nil {
		t.Fatal(err)
	}
}

// TestOfflineShellDropsLoaderAndStartupVariables: a recorded step
// environment is applied to the operator's own shell, so nothing in it may
// inject code into it or what it starts.
func TestOfflineShellDropsLoaderAndStartupVariables(t *testing.T) {
	runDir, cacheDir := seedOfflineShellRun(t)
	denied := []string{"LD_PRELOAD=/x.so", "LD_LIBRARY_PATH=/x", "DYLD_INSERT_LIBRARIES=/x.dylib",
		"BASH_ENV=/x.sh", "ENV=/x.sh", "ZDOTDIR=/x", "PROMPT_COMMAND=touch /x", "SHELLOPTS=xtrace",
		"BASHOPTS=extglob", "IFS=x", "PS4=$(touch /x)", "BASH_FUNC_ls%%=() { touch /x; }"}
	rewritePlan(t, runDir, func(n *plan.Node) {
		n.Env = append(append(n.Env, "ORDINARY=visible"), denied...)
	})

	env := offlineShellEnv(func() *plan.Node {
		p, err := readRunPlan(runDir)
		if err != nil {
			t.Fatal(err)
		}
		n, _ := p.Node("build")
		return n
	}(), "r1", "build")
	for _, kv := range denied {
		name, _, _ := strings.Cut(kv, "=")
		for _, got := range env {
			if strings.HasPrefix(got, name+"=") {
				t.Errorf("the offline shell's environment carries %s", got)
			}
		}
	}
	if !slices.Contains(env, "ORDINARY=visible") {
		t.Errorf("the offline shell lost an ordinary variable: %v", env)
	}

	// And through the command, so the filter is on the path cmdShell takes.
	var out, errb bytes.Buffer
	code := cmdShell([]string{"--run", runDir, "--step", "build", "--offline", "--cache-dir", cacheDir,
		"--cmd", "env"}, &out, &errb, strings.NewReader(""))
	if code != exitSuccess {
		t.Fatalf("exit = %d, stderr: %s", code, errb.String())
	}
	if strings.Contains(out.String(), "LD_PRELOAD") || strings.Contains(out.String(), "BASH_ENV") ||
		strings.Contains(out.String(), "BASH_FUNC_") {
		t.Errorf("env printed a denied variable:\n%s", out.String())
	}
}

// TestOfflineShellStartsInAWorkDirInsideAMount: a WorkDir below a mount's
// path starts the shell at that path under the restored mount.
func TestOfflineShellStartsInAWorkDirInsideAMount(t *testing.T) {
	base := t.TempDir()
	t.Chdir(base)
	t.Setenv("HOME", filepath.Join(base, "home"))
	t.Setenv("XDG_RUNTIME_DIR", filepath.Join(base, "registry"))
	cacheDir := filepath.Join(base, "cache")
	ws := senro.Workspace("src", senro.Scope(senro.ScopeRun))
	pipe := senro.New("ci")
	pipe.Workflow("main").
		Step("build", exec.Command("sh", "-c", "mkdir -p app && printf deep > app/f.txt")).
		WorkDir("/src").Mount(ws.At("/src", senro.RW))
	runDir := filepath.Join(base, "runs", "r1")
	if err := senro.Run(context.Background(), pipe,
		senro.WithDir(runDir), senro.WithRunID("r1"), senro.WithCacheDir(cacheDir)); err != nil {
		t.Fatalf("Run: %v", err)
	}
	rewritePlan(t, runDir, func(n *plan.Node) { n.WorkDir = "/src/app" })

	var out, errb bytes.Buffer
	code := cmdShell([]string{"--run", runDir, "--step", "build", "--offline", "--cache-dir", cacheDir,
		"--cmd", "cat", "f.txt"}, &out, &errb, strings.NewReader(""))
	if code != exitSuccess {
		t.Fatalf("exit = %d, stderr: %s", code, errb.String())
	}
	if out.String() != "deep" {
		t.Errorf("stdout = %q, want f.txt from root/src/app", out.String())
	}
}

// TestOfflineShellKeepsHostileMountPathsInsideRoot: an At that climbs, and
// an empty one, both restore inside the temporary root.
func TestOfflineShellKeepsHostileMountPathsInsideRoot(t *testing.T) {
	runDir, cacheDir := seedOfflineShellRun(t)
	store, err := storage.Open(cacheDir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	snaps, err := stepSnapshots(runDir, "build")
	if err != nil {
		t.Fatal(err)
	}
	for _, at := range []string{"../x", "../../../etc", ""} {
		rewritePlan(t, runDir, func(n *plan.Node) {
			n.Mounts[0].At = at
			n.WorkDir = at
		})
		p, err := readRunPlan(runDir)
		if err != nil {
			t.Fatal(err)
		}
		root, workDir, err := restoreStepWorkspaces(context.Background(), store, p, "r1", "build", snaps)
		if err != nil {
			t.Fatalf("At %q: %v", at, err)
		}
		inside := func(path string) bool {
			rel, err := filepath.Rel(root, path)
			return err == nil && rel != ".." && !strings.HasPrefix(rel, "../")
		}
		if !inside(workDir) {
			t.Errorf("At %q: workDir %q is outside root %q", at, workDir, root)
		}
		if _, err := os.Stat(filepath.Join(workDir, "marker.txt")); err != nil {
			t.Errorf("At %q: the workspace was not restored at the shell's directory: %v", at, err)
		}
		_ = os.RemoveAll(root)
	}
	// Belt: nothing landed beside the temporary root.
	if _, err := os.Stat(filepath.Join(os.TempDir(), "x", "marker.txt")); err == nil {
		t.Error("an At of ../x restored outside the temporary root")
	}
}

// TestOfflineShellStripsSecretEnv: the offline form uses the rule a live
// session does. The step's recorded plan is given a secret and variables
// naming it after the run, because a run cannot record that combination
// (plan.Validate refuses it), and the rule must not depend on that.
func TestOfflineShellStripsSecretEnv(t *testing.T) {
	runDir, cacheDir := seedOfflineShellRun(t)
	p, err := readRunPlan(runDir)
	if err != nil {
		t.Fatal(err)
	}
	n, ok := p.Node("build")
	if !ok {
		t.Fatal("the seeded plan has no build step")
	}
	n.Secrets = []plan.SecretSpec{{Name: "Token", Env: "API_TOKEN"}}
	n.Env = append(n.Env, "ORDINARY=visible", "API_TOKEN=/run/senro/secrets/token",
		"SENRO_SECRET_TOKEN=/run/senro/secrets/token", "SENRO_SECRET_LEFTOVER=/x")
	b, err := p.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(runDir, "plan.json"), b, 0o644); err != nil {
		t.Fatal(err)
	}

	var out, errb bytes.Buffer
	code := cmdShell([]string{"--run", runDir, "--step", "build", "--offline", "--cache-dir", cacheDir,
		"--cmd", "env"}, &out, &errb, strings.NewReader(""))
	if code != exitSuccess {
		t.Fatalf("exit = %d, stderr: %s", code, errb.String())
	}
	env := out.String()
	for _, gone := range []string{"API_TOKEN", "SENRO_SECRET_"} {
		if strings.Contains(env, gone) {
			t.Errorf("the offline shell's environment carries %q:\n%s", gone, env)
		}
	}
	for _, kept := range []string{"ORDINARY=visible", "SENRO_SHELL=1", "SENRO_SHELL_RUN=r1",
		"SENRO_SHELL_STEP=build", "PATH="} {
		if !strings.Contains(env, kept) {
			t.Errorf("the offline shell's environment lacks %q:\n%s", kept, env)
		}
	}
}

// TestOfflineShellNamesTheMountsItCouldNotRestore: a workspace the step
// mounts but never snapshotted is an empty directory in the shell, and the
// banner says which ones and why.
func TestOfflineShellNamesTheMountsItCouldNotRestore(t *testing.T) {
	runDir, cacheDir := seedOfflineShellRun(t)
	rewritePlan(t, runDir, func(n *plan.Node) {
		n.Mounts = append(n.Mounts,
			plan.MountSpec{Workspace: "deps", At: "/deps"},
			plan.MountSpec{Workspace: "tools", At: "/tools"})
	})

	var out, errb bytes.Buffer
	code := cmdShell([]string{"--run", runDir, "--step", "build", "--offline", "--cache-dir", cacheDir,
		"--cmd", "true"}, &out, &errb, strings.NewReader(""))
	if code != exitSuccess {
		t.Fatalf("exit = %d, stderr: %s", code, errb.String())
	}
	if want := "senro shell: not restored (no snapshot by this step): deps, tools\n"; !strings.Contains(errb.String(), want) {
		t.Errorf("stderr = %q, want %q", errb.String(), want)
	}

	// Nothing to name: no line.
	runDir, cacheDir = seedOfflineShellRun(t)
	errb.Reset()
	if code := cmdShell([]string{"--run", runDir, "--step", "build", "--offline", "--cache-dir", cacheDir,
		"--cmd", "true"}, &out, &errb, strings.NewReader("")); code != exitSuccess {
		t.Fatalf("exit = %d, stderr: %s", code, errb.String())
	}
	if strings.Contains(errb.String(), "not restored") {
		t.Errorf("stderr names unrestored mounts when every mount was restored: %q", errb.String())
	}
}

// TestOfflineShellCleansUpWhenTheShellCannotStart: a command that never
// starts still leaves no restored directory behind.
func TestOfflineShellCleansUpWhenTheShellCannotStart(t *testing.T) {
	runDir, cacheDir := seedOfflineShellRun(t)

	var out, errb bytes.Buffer
	code := cmdShell([]string{"--run", runDir, "--step", "build", "--offline", "--cache-dir", cacheDir,
		"--cmd", filepath.Join(t.TempDir(), "no-such-shell")}, &out, &errb, strings.NewReader(""))
	if code != exitRunFailed {
		t.Errorf("exit = %d, want %d; stderr: %s", code, exitRunFailed, errb.String())
	}
	root := offlineRoot(t, errb.String())
	if _, err := os.Stat(root); !os.IsNotExist(err) {
		t.Errorf("%s is still there after the shell failed to start (stat: %v)", root, err)
		_ = os.RemoveAll(root)
	}
}

// TestShellOnARegisteredRunItCannotReachDoesNotGoOffline: a run with a live
// registration is live, and a socket that refuses is an error to report,
// not a cue to restore an older recording and stand in that instead.
func TestShellOnARegisteredRunItCannotReachDoesNotGoOffline(t *testing.T) {
	runDir, cacheDir := seedOfflineShellRun(t)
	unregister, err := attachsrv.Register(attachsrv.Entry{
		PID: os.Getpid(), Socket: filepath.Join(t.TempDir(), "gone.sock"), RunID: runDir,
		Pipeline: "ci", StartedAt: time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	t.Cleanup(unregister)

	var out, errb bytes.Buffer
	code := cmdShell([]string{"--run", runDir, "--step", "build", "--cache-dir", cacheDir,
		"--cmd", "true"}, &out, &errb, strings.NewReader(""))
	if code != exitUsage {
		t.Errorf("exit = %d, want %d; stderr: %s", code, exitUsage, errb.String())
	}
	if strings.Contains(errb.String(), "offline") {
		t.Errorf("a refused live socket fell back to the offline form: %q", errb.String())
	}
	if !strings.Contains(errb.String(), "could not connect") {
		t.Errorf("stderr = %q, want it to say the registered run could not be reached", errb.String())
	}
}
