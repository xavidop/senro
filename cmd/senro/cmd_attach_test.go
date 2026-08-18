package main

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/internal/attachsrv"
	"github.com/xavidop/senro/internal/eventlog"
)

// waitForSubscriber blocks until hub reports at least one lifecycle
// subscriber. A hub that has emitted run.finished but not been Closed
// keeps its stream open, so these tests must close it only AFTER cmdAttach
// has subscribed, or Subscribe hangs with nothing left to close it. The
// budget is generous because nothing here is a latency assertion: a freshly
// exec'd subprocess reaching Subscribe under parallel load needed more than
// two seconds, and the only cost of waiting is how long a genuine hang
// takes to report.
const subscriberBudget = 60 * time.Second

func waitForSubscriber(t *testing.T, hub *attachsrv.Hub) {
	t.Helper()
	deadline := time.Now().Add(subscriberBudget)
	for time.Now().Before(deadline) {
		if hub.SubscriberCount() > 0 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("no subscriber registered on the hub within %s: the attach subprocess never reached its Subscribe call", subscriberBudget)
}

// --- usage errors: exit 2, distinct messages ---

func TestCmdAttachFollowWithoutRunIsUsageError(t *testing.T) {
	isolateRegistry(t)
	var stdout, stderr strings.Builder
	code := cmdAttach([]string{"--follow"}, &stdout, &stderr, false)
	if code != exitUsage {
		t.Fatalf("exit code = %d, want %d (usage)", code, exitUsage)
	}
	if !strings.Contains(stderr.String(), "--follow") {
		t.Errorf("stderr = %q, want it to mention --follow", stderr.String())
	}
}

func TestCmdAttachPidAndRunTogetherIsUsageError(t *testing.T) {
	isolateRegistry(t)
	var stdout, stderr strings.Builder
	code := cmdAttach([]string{"--pid", "123", "--run", "01ABC"}, &stdout, &stderr, false)
	if code != exitUsage {
		t.Fatalf("exit code = %d, want %d (usage)", code, exitUsage)
	}
}

func TestCmdAttachBadUIValueIsUsageError(t *testing.T) {
	isolateRegistry(t)
	var stdout, stderr strings.Builder
	code := cmdAttach([]string{"--ui=curses"}, &stdout, &stderr, false)
	if code != exitUsage {
		t.Fatalf("exit code = %d, want %d (usage)", code, exitUsage)
	}
}

// TestCmdAttachTUIOnNonTTYIsUsageErrorNotDowngrade checks this through the
// actual command, not resolveUIMode alone: --ui=tui on a non-TTY must exit
// 2 and must not silently render plain output.
func TestCmdAttachTUIOnNonTTYIsUsageErrorNotDowngrade(t *testing.T) {
	isolateRegistry(t)
	var stdout, stderr strings.Builder
	code := cmdAttach([]string{"--ui=tui"}, &stdout, &stderr, false)
	if code != exitUsage {
		t.Fatalf("exit code = %d, want %d (usage)", code, exitUsage)
	}
	if stdout.Len() != 0 {
		t.Errorf("stdout = %q, want empty — must not silently render plain output", stdout.String())
	}
}

// --- no runs / discovery failure propagates as usage error with the message ---

func TestCmdAttachNoRunsFound(t *testing.T) {
	isolateRegistry(t)
	var stdout, stderr strings.Builder
	code := cmdAttach(nil, &stdout, &stderr, false)
	if code != exitUsage {
		t.Fatalf("exit code = %d, want %d (usage)", code, exitUsage)
	}
	if !strings.Contains(stderr.String(), "no live senro runs") {
		t.Errorf("stderr = %q, want the no-runs-found message", stderr.String())
	}
}

// --- a real live run, attached with --ui=none and --ui=plain ---

func TestCmdAttachLiveRunUINone(t *testing.T) {
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

	unregister, err := attachsrv.Register(attachsrv.Entry{
		Socket: filepath.Join(dir, "s.sock"), RunID: "r1", Pipeline: "ci",
	})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	defer unregister()

	hub.Emit(api.Event{V: api.Version, Seq: 1, Type: api.RunStarted, Run: "r1"})
	hub.Emit(api.Event{V: api.Version, Seq: 2, Type: api.RunFinished, Run: "r1",
		Payload: mustJSONAttach(api.RunFinishedBody{Status: api.RunSucceeded})})

	var stdout, stderr strings.Builder
	done := make(chan int, 1)
	go func() { done <- cmdAttach([]string{"--ui=none"}, &stdout, &stderr, false) }()

	// cmdAttach's Subscribe sees no natural end while the hub is open, so
	// it blocks exactly as a real client watching a just-finished run
	// would. Close it here to deliver that signal.
	waitForSubscriber(t, hub)
	_ = hub.Close()

	select {
	case code := <-done:
		if code != exitSuccess {
			t.Fatalf("exit code = %d, want %d (success); stderr=%s", code, exitSuccess, stderr.String())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("cmdAttach did not return")
	}
	if stdout.Len() != 0 {
		t.Errorf("--ui=none produced output: %q", stdout.String())
	}
}

func TestCmdAttachLiveRunUIPlainReportsFailedRunAsExit1(t *testing.T) {
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

	unregister, err := attachsrv.Register(attachsrv.Entry{
		Socket: filepath.Join(dir, "s.sock"), RunID: "r1", Pipeline: "ci",
	})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	defer unregister()

	hub.Emit(api.Event{V: api.Version, Seq: 1, Type: api.RunStarted, Run: "r1"})
	hub.Emit(api.Event{V: api.Version, Seq: 2, Type: api.RunFinished, Run: "r1",
		Payload: mustJSONAttach(api.RunFinishedBody{Status: api.RunFailed})})

	var stdout, stderr strings.Builder
	done := make(chan int, 1)
	go func() { done <- cmdAttach([]string{"--ui=plain"}, &stdout, &stderr, false) }()

	waitForSubscriber(t, hub)
	_ = hub.Close()

	select {
	case code := <-done:
		if code != exitRunFailed {
			t.Fatalf("exit code = %d, want %d (run failed); stdout=%s stderr=%s", code, exitRunFailed, stdout.String(), stderr.String())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("cmdAttach did not return")
	}
	if !strings.Contains(stdout.String(), "failed") {
		t.Errorf("plain output = %q, want it to mention the failure", stdout.String())
	}
}

// --- --run against a purely offline run directory ---

func TestCmdAttachRunFlagOfflinePostMortem(t *testing.T) {
	isolateRegistry(t)
	cwd, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd: %v", err)
	}
	work := t.TempDir()
	if err := os.Chdir(work); err != nil {
		t.Fatalf("Chdir: %v", err)
	}
	t.Cleanup(func() { _ = os.Chdir(cwd) })

	dir := filepath.Join(work, "runs", "01OFFLINE")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	writeOfflineRun(t, dir)

	var stdout, stderr strings.Builder
	code := cmdAttach([]string{"--run", "01OFFLINE", "--ui=plain"}, &stdout, &stderr, false)
	if code != exitSuccess {
		t.Fatalf("exit code = %d, want %d; stderr=%s", code, exitSuccess, stderr.String())
	}
	if !strings.Contains(stdout.String(), "run succeeded") {
		t.Errorf("plain output = %q, want the final run line", stdout.String())
	}
}

func TestCmdAttachRunFlagNoSuchRunIsDistinctMessage(t *testing.T) {
	isolateRegistry(t)
	cwd, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd: %v", err)
	}
	work := t.TempDir()
	if err := os.Chdir(work); err != nil {
		t.Fatalf("Chdir: %v", err)
	}
	t.Cleanup(func() { _ = os.Chdir(cwd) })

	var stdout, stderr strings.Builder
	code := cmdAttach([]string{"--run", "01NOPE"}, &stdout, &stderr, false)
	if code != exitUsage {
		t.Fatalf("exit code = %d, want %d", code, exitUsage)
	}
	if !strings.Contains(stderr.String(), "01NOPE") {
		t.Errorf("stderr = %q, want it to name the missing run", stderr.String())
	}
}

func writeOfflineRun(t *testing.T, dir string) {
	t.Helper()
	l, err := eventlog.Open(dir)
	if err != nil {
		t.Fatalf("eventlog.Open: %v", err)
	}
	events := []api.Event{
		{Type: api.RunStarted, Run: "01OFFLINE", Payload: mustJSONAttach(api.RunStartedBody{
			Pipeline: "ci", EngineVersion: "test",
		})},
		{Type: api.RunFinished, Run: "01OFFLINE", Payload: mustJSONAttach(api.RunFinishedBody{Status: api.RunSucceeded})},
	}
	for _, e := range events {
		if _, err := l.Append(e); err != nil {
			t.Fatalf("append: %v", err)
		}
	}
	if err := l.Close(); err != nil {
		t.Fatalf("close ledger: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "plan.json"), []byte(`{"version":1,"nodes":[]}`), 0o644); err != nil {
		t.Fatalf("write plan.json: %v", err)
	}
}

func mustJSONAttach(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}
