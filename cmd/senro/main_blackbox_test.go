package main

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/internal/attachsrv"
)

// buildSenroBinary builds the real senro binary once per test process:
// the one place this suite proves the exit-code contract through the
// actual os.Exit(run(...)) boundary rather than by calling run() as a Go
// function. Every other test here is faster and covers more cases, but
// none proves main() wires run()'s return into a process exit code.
func buildSenroBinary(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	binPath := filepath.Join(dir, "senro")
	cmd := exec.Command("go", "build", "-o", binPath, ".")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("go build senro: %v\n%s", err, out)
	}
	return binPath
}

func TestBlackboxExitCodeNoArgsIsTwo(t *testing.T) {
	bin := buildSenroBinary(t)
	cmd := exec.Command(bin)
	err := cmd.Run()
	code := exitCodeFromErr(t, err)
	if code != exitUsage {
		t.Fatalf("exit code = %d, want %d", code, exitUsage)
	}
}

func TestBlackboxExitCodeHelpIsZero(t *testing.T) {
	bin := buildSenroBinary(t)
	cmd := exec.Command(bin, "--help")
	err := cmd.Run()
	code := exitCodeFromErr(t, err)
	if code != exitSuccess {
		t.Fatalf("exit code = %d, want %d", code, exitSuccess)
	}
}

// TestBlackboxExitCodeSuccessfulAttachIsZero drives a real live run
// through a real socket, registry and subprocess boundary and checks $?:
// the chain the function-level tests cannot see past.
func TestBlackboxExitCodeSuccessfulAttachIsZero(t *testing.T) {
	bin := buildSenroBinary(t)
	regDir, socketDir := isolateRegistryEnv(t)

	hub := attachsrv.NewHub(64)
	srv, err := attachsrv.Listen(context.Background(), attachsrv.Options{
		Bind: filepath.Join(socketDir, "s.sock"), Dir: socketDir, Hub: hub,
	})
	if err != nil {
		t.Fatalf("attachsrv.Listen: %v", err)
	}
	defer func() { _ = srv.Close() }()

	unregister, err := attachsrv.Register(attachsrv.Entry{
		Socket: filepath.Join(socketDir, "s.sock"), RunID: "r1", Pipeline: "ci",
	})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	defer unregister()

	hub.Emit(api.Event{V: api.Version, Seq: 1, Type: api.RunStarted, Run: "r1"})
	hub.Emit(api.Event{V: api.Version, Seq: 2, Type: api.RunFinished, Run: "r1",
		Payload: mustJSONAttach(api.RunFinishedBody{Status: api.RunSucceeded})})

	cmd := exec.Command(bin, "attach", "--ui=none")
	cmd.Env = append(os.Environ(), "HOME="+regDir, "XDG_RUNTIME_DIR="+regDir)

	done := make(chan error, 1)
	go func() { done <- cmd.Run() }()

	waitForSubscriber(t, hub)
	_ = hub.Close()

	select {
	case err := <-done:
		code := exitCodeFromErr(t, err)
		if code != exitSuccess {
			t.Fatalf("exit code = %d, want %d", code, exitSuccess)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("senro attach (subprocess) did not exit")
	}
}

// TestBlackboxExitCodeSigintIsOneThirty sends a REAL SIGINT to a real
// `senro attach` watching a never-finishing run and checks $? is 130: the
// cancellation case at the OS signal level, not only in unit tests.
func TestBlackboxExitCodeSigintIsOneThirty(t *testing.T) {
	bin := buildSenroBinary(t)
	regDir, socketDir := isolateRegistryEnv(t)

	hub := attachsrv.NewHub(64)
	srv, err := attachsrv.Listen(context.Background(), attachsrv.Options{
		Bind: filepath.Join(socketDir, "s.sock"), Dir: socketDir, Hub: hub,
	})
	if err != nil {
		t.Fatalf("attachsrv.Listen: %v", err)
	}
	defer func() { _ = srv.Close() }()
	defer func() { _ = hub.Close() }()

	unregister, err := attachsrv.Register(attachsrv.Entry{
		Socket: filepath.Join(socketDir, "s.sock"), RunID: "r1", Pipeline: "ci",
	})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	defer unregister()

	// A run that never finishes: run.started only. The subprocess must
	// still be watching (not exited on its own) when SIGINT arrives.
	hub.Emit(api.Event{V: api.Version, Seq: 1, Type: api.RunStarted, Run: "r1"})

	cmd := exec.Command(bin, "attach", "--ui=none")
	cmd.Env = append(os.Environ(), "HOME="+regDir, "XDG_RUNTIME_DIR="+regDir)
	if err := cmd.Start(); err != nil {
		t.Fatalf("start senro attach: %v", err)
	}

	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()

	waitForSubscriber(t, hub)
	if err := cmd.Process.Signal(syscall.SIGINT); err != nil {
		t.Fatalf("signal: %v", err)
	}

	select {
	case err := <-done:
		code := exitCodeFromErr(t, err)
		if code != exitCancelled {
			t.Fatalf("exit code = %d, want %d", code, exitCancelled)
		}
	case <-time.After(10 * time.Second):
		_ = cmd.Process.Kill()
		t.Fatal("senro attach (subprocess) did not exit after SIGINT")
	}
}

// isolateRegistryEnv is isolateRegistry's directory resolution as plain
// values rather than t.Setenv, which affects only THIS process, so a
// black-box test can hand the same isolated HOME and XDG_RUNTIME_DIR to a
// real subprocess. Returns the registry root and a second short directory
// safe for a unix socket path.
func isolateRegistryEnv(t *testing.T) (regDir, socketDir string) {
	t.Helper()
	regDir, err := os.MkdirTemp("", "bb")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(regDir) })
	t.Setenv("HOME", regDir)
	t.Setenv("XDG_RUNTIME_DIR", regDir)
	return regDir, mustShortDir(t)
}

// exitCodeFromErr extracts a process's exit code from the error cmd.Run()
// (or cmd.Wait()) returned: nil means 0, *exec.ExitError carries the real
// code, anything else is a test infrastructure failure.
func exitCodeFromErr(t *testing.T, err error) int {
	t.Helper()
	if err == nil {
		return 0
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		return exitErr.ExitCode()
	}
	t.Fatalf("cmd.Run/Wait failed for a reason other than a non-zero exit: %v", err)
	return -1
}
