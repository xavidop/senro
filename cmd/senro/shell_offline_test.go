package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/xavidop/senro"
	"github.com/xavidop/senro/exec"
	"github.com/xavidop/senro/internal/plan"
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
