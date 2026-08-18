package main

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/internal/attachsrv"
	"github.com/xavidop/senro/internal/source"
)

// isolateRegistry mirrors attachsrv's and attach's own test helpers.
func isolateRegistry(t *testing.T) {
	t.Helper()
	dir, err := os.MkdirTemp("", "cs")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	t.Setenv("HOME", dir)
	t.Setenv("XDG_RUNTIME_DIR", dir)
}

func registerEntry(t *testing.T, e attachsrv.Entry) attachsrv.Entry {
	t.Helper()
	if e.PID == 0 {
		e.PID = os.Getpid()
	}
	unregister, err := attachsrv.Register(e)
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	t.Cleanup(unregister)
	return e
}

// deadPID returns a pid that does not name a running process: a trivial
// subprocess, waited for. A recently-exited pid is not immediately reused
// on darwin or linux, the assumption attachsrv's reaping already relies
// on.
func deadPID(t *testing.T) int {
	t.Helper()
	cmd := exec.Command("true")
	if err := cmd.Run(); err != nil {
		t.Fatalf("spawn throwaway process: %v", err)
	}
	return cmd.Process.Pid
}

// liveSubprocess starts a long-lived process and returns its pid, so a
// test can register an Entry that Discover's liveness check genuinely
// finds alive, with no test-only seam in production code.
func liveSubprocess(t *testing.T) int {
	t.Helper()
	cmd := exec.Command("sleep", "30")
	if err := cmd.Start(); err != nil {
		t.Fatalf("start subprocess: %v", err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	})
	return cmd.Process.Pid
}

// --- selectEntry: no runs, several runs, --pid resolution ---

func TestSelectEntryNoRunsFound(t *testing.T) {
	isolateRegistry(t)
	_, err := selectEntry(0)
	if err == nil {
		t.Fatal("selectEntry(0) err = nil, want a distinct error for no live runs")
	}
	if !strings.Contains(err.Error(), "no live senro runs") {
		t.Errorf("error %q does not clearly say no runs were found", err.Error())
	}
}

func TestSelectEntryOneRunNoPid(t *testing.T) {
	isolateRegistry(t)
	want := registerEntry(t, attachsrv.Entry{Socket: filepath.Join(t.TempDir(), "s.sock"), RunID: "r1", Pipeline: "ci"})

	got, err := selectEntry(0)
	if err != nil {
		t.Fatalf("selectEntry(0): %v", err)
	}
	if got.RunID != want.RunID {
		t.Errorf("selectEntry returned RunID %q, want %q", got.RunID, want.RunID)
	}
}

func TestSelectEntrySeveralRunsNoneSpecified(t *testing.T) {
	isolateRegistry(t)
	pid1 := liveSubprocess(t)
	pid2 := liveSubprocess(t)
	registerEntry(t, attachsrv.Entry{PID: pid1, Socket: filepath.Join(t.TempDir(), "a.sock"), RunID: "r1", Pipeline: "ci"})
	registerEntry(t, attachsrv.Entry{PID: pid2, Socket: filepath.Join(t.TempDir(), "b.sock"), RunID: "r2", Pipeline: "deploy"})

	_, err := selectEntry(0)
	if err == nil {
		t.Fatal("selectEntry(0) err = nil, want an ambiguous-selection error")
	}
	if !strings.Contains(err.Error(), "2 live runs") {
		t.Errorf("error %q does not name the count", err.Error())
	}
	for _, want := range []string{"ci", "deploy"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not list %q for disambiguation", err.Error(), want)
		}
	}
}

func TestSelectEntryByPidNeverRegistered(t *testing.T) {
	isolateRegistry(t)
	_, err := selectEntry(999999)
	if err == nil {
		t.Fatal("selectEntry(999999) err = nil, want an error")
	}
	if strings.Contains(err.Error(), "stale") {
		t.Errorf("a pid that was never registered must not be reported as a stale entry: %q", err.Error())
	}
}

// TestSelectEntryByPidStaleEntryIsDistinctFromNeverRegistered: a dead
// entry must read differently from "no such run at all", or an operator
// cannot tell a mistyped pid from a run that crashed without cleaning up.
func TestSelectEntryByPidStaleEntryIsDistinctFromNeverRegistered(t *testing.T) {
	isolateRegistry(t)
	pid := deadPID(t)
	registerEntry(t, attachsrv.Entry{PID: pid, Socket: filepath.Join(t.TempDir(), "s.sock"), RunID: "r1"})

	_, err := selectEntry(pid)
	if err == nil {
		t.Fatal("selectEntry(deadPID) err = nil, want a stale-entry error")
	}
	if !strings.Contains(err.Error(), "stale") && !strings.Contains(err.Error(), "no longer running") {
		t.Errorf("error %q does not explain the entry is stale (dead process)", err.Error())
	}

	_, err2 := selectEntry(999999)
	if err2 == nil {
		t.Fatal("selectEntry(999999) err = nil, want an error")
	}
	if err.Error() == err2.Error() {
		t.Errorf("stale-entry and never-registered produced the SAME message: %q", err.Error())
	}
}

// --- connectLive: a socket that exists on disk but refuses the connection ---

func TestConnectLiveSocketExistsButRefusesConnection(t *testing.T) {
	sockPath := filepath.Join(mustShortDir(t), "s.sock")
	// Bind and immediately close: the path exists on disk (as a leftover
	// socket file, exactly what a process that died mid-run leaves behind)
	// but nothing is listening on it any more.
	ln, err := net.Listen("unix", sockPath)
	if err != nil {
		t.Fatalf("bind throwaway socket: %v", err)
	}
	_ = ln.Close()

	_, err = connectLive(context.Background(), attachsrv.Entry{PID: os.Getpid(), Socket: sockPath, RunID: "r1"})
	if err == nil {
		t.Fatal("connectLive err = nil, want a refusal")
	}
	if !strings.Contains(err.Error(), "could not connect") {
		t.Errorf("error %q does not explain the connection was refused", err.Error())
	}
}

func TestConnectLiveSucceedsAndWrapsFallback(t *testing.T) {
	isolateRegistry(t)
	dir := mustShortDir(t)
	hub := attachsrv.NewHub(64)
	srv, err := attachsrv.Listen(context.Background(), attachsrv.Options{
		Bind: filepath.Join(dir, "s.sock"), Dir: dir, Hub: hub,
	})
	if err != nil {
		t.Fatalf("attachsrv.Listen: %v", err)
	}
	defer func() { _ = srv.Close() }()
	defer func() { _ = hub.Close() }()

	src, err := connectLive(context.Background(), attachsrv.Entry{
		PID: os.Getpid(), Socket: filepath.Join(dir, "s.sock"), RunID: "r1",
	})
	if err != nil {
		t.Fatalf("connectLive: %v", err)
	}
	defer func() { _ = src.Close() }()

	st, err := src.State(context.Background())
	if err != nil {
		t.Fatalf("State: %v", err)
	}
	if st.ProtoMajor != api.Version {
		t.Errorf("ProtoMajor = %d, want %d", st.ProtoMajor, api.Version)
	}
}

// TestConnectLiveWithoutRunIDStaysLiveOnly is the branch
// TestConnectLiveSucceedsAndWrapsFallback does not cover: with no RunID
// there is no fallback directory to construct (runDir("") is "runs/", the
// parent of every run and none of them), so connectLive must return the
// live source as-is rather than wrap it in a Fallback that could only fail
// confusingly.
func TestConnectLiveWithoutRunIDStaysLiveOnly(t *testing.T) {
	isolateRegistry(t)
	dir := mustShortDir(t)
	hub := attachsrv.NewHub(64)
	srv, err := attachsrv.Listen(context.Background(), attachsrv.Options{
		Bind: filepath.Join(dir, "s.sock"), Dir: dir, Hub: hub,
	})
	if err != nil {
		t.Fatalf("attachsrv.Listen: %v", err)
	}
	defer func() { _ = srv.Close() }()
	defer func() { _ = hub.Close() }()

	src, err := connectLive(context.Background(), attachsrv.Entry{
		PID: os.Getpid(), Socket: filepath.Join(dir, "s.sock"), RunID: "",
	})
	if err != nil {
		t.Fatalf("connectLive: %v", err)
	}
	defer func() { _ = src.Close() }()

	if _, ok := src.(*source.LiveSource); !ok {
		t.Fatalf("connectLive with no RunID returned %T, want a bare *source.LiveSource (no Fallback wrapping)", src)
	}
}

// --- negotiateVersion ---

func TestNegotiateVersionEqualIsSilent(t *testing.T) {
	var stderr strings.Builder
	src := &fakeVersionSource{major: api.Version, minor: api.VersionMinor}
	if err := negotiateVersion(context.Background(), src, &stderr); err != nil {
		t.Fatalf("negotiateVersion: %v", err)
	}
	if stderr.Len() != 0 {
		t.Errorf("stderr = %q, want empty", stderr.String())
	}
}

func TestNegotiateVersionMinorMismatchWarnsButProceeds(t *testing.T) {
	var stderr strings.Builder
	src := &fakeVersionSource{major: api.Version, minor: api.VersionMinor + 1}
	if err := negotiateVersion(context.Background(), src, &stderr); err != nil {
		t.Fatalf("negotiateVersion: %v (want nil — minor mismatch must not fail)", err)
	}
	if stderr.Len() == 0 {
		t.Error("stderr is empty, want a warning about the minor version mismatch")
	}
}

func TestNegotiateVersionMajorMismatchRefusesWithActionableMessage(t *testing.T) {
	var stderr strings.Builder
	src := &fakeVersionSource{major: api.Version + 1, minor: 0}
	err := negotiateVersion(context.Background(), src, &stderr)
	if err == nil {
		t.Fatal("negotiateVersion err = nil, want a refusal for a major version mismatch")
	}
	var mismatch *api.VersionMismatchError
	if !errors.As(err, &mismatch) {
		t.Fatalf("err = %v (%T), want *api.VersionMismatchError wrapped in it", err, err)
	}
	// Not just "a mismatch was detected": the fields must be attributed to
	// the right side. Swapping negotiateVersion's argument order would
	// still produce a *VersionMismatchError, since detection is symmetric,
	// and would still pass a test that only checked errors.As, while
	// telling an operator to upgrade the wrong side.
	if mismatch.ClientMajor != api.Version || mismatch.ClientMinor != api.VersionMinor {
		t.Errorf("mismatch.Client = %d.%d, want this build's own %d.%d",
			mismatch.ClientMajor, mismatch.ClientMinor, api.Version, api.VersionMinor)
	}
	if mismatch.ServerMajor != api.Version+1 || mismatch.ServerMinor != 0 {
		t.Errorf("mismatch.Server = %d.%d, want %d.0", mismatch.ServerMajor, mismatch.ServerMinor, api.Version+1)
	}
	// Not a JSON decode error, not a generic failure: the exact regression
	// version negotiation exists to prevent.
	if strings.Contains(err.Error(), "invalid character") || strings.Contains(err.Error(), "looking for beginning of value") {
		t.Fatalf("error %q looks like a JSON decode error, not a version refusal", err.Error())
	}
}

func TestNegotiateVersionSkippedWhenSourceReportsNoProtocolVersion(t *testing.T) {
	// ProtoMajor == 0: a FileSource / post-mortem view, or a hub predating
	// negotiation, nothing to check against.
	var stderr strings.Builder
	src := &fakeVersionSource{major: 0, minor: 0}
	if err := negotiateVersion(context.Background(), src, &stderr); err != nil {
		t.Fatalf("negotiateVersion: %v", err)
	}
	if stderr.Len() != 0 {
		t.Errorf("stderr = %q, want empty", stderr.String())
	}
}

// fakeVersionSource is a source.Source stub exposing only what
// negotiateVersion reads, so the four negotiation cases need no real
// socket.
type fakeVersionSource struct {
	major, minor int
}

func (f *fakeVersionSource) State(context.Context) (*api.RunState, error) {
	return &api.RunState{ProtoMajor: f.major, ProtoMinor: f.minor}, nil
}
func (f *fakeVersionSource) Subscribe(context.Context, uint64) (<-chan api.Event, error) {
	return nil, errors.New("not implemented")
}
func (f *fakeVersionSource) Logs(context.Context, string, int, string, int64) (io.ReadCloser, error) {
	return nil, errors.New("not implemented")
}
func (f *fakeVersionSource) Control(context.Context, api.Frame) (api.Frame, error) {
	return api.Frame{}, errors.New("not implemented")
}
func (f *fakeVersionSource) Close() error { return nil }

var _ source.Source = (*fakeVersionSource)(nil)

// mustShortDir returns a fresh, short-prefixed temp dir safe for a real
// unix socket path (see attachsrv's own shortSocketPath for why t.TempDir()
// alone is not safe here: it nests the test's own name into the path).
func mustShortDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "cl")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return dir
}
