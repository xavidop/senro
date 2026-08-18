package main

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/internal/attachsrv"
	"github.com/xavidop/senro/internal/sink"
)

// TestRunPipelineSendsGracefulCancelBeforeWaitingForExit pins the ordering:
// bestEffortCancel(src) sequenced after <-waitDone could never reach a
// still-running engine, since waitDone only unblocks once the process has
// exited.
//
// Proving it needs no engine, only that run.cancel reaches SOMETHING while
// the pipeline is alive. The goroutine below stands in for
// internal/engine/control.go's consumer. The child ignores INT and TERM,
// the default disposition of a main() with no signal handling, and exits
// only on a sentinel the consumer writes on run.cancel; with the ordering
// reversed the sentinel is never written and this times out.
func TestRunPipelineSendsGracefulCancelBeforeWaitingForExit(t *testing.T) {
	isolateRegistry(t)
	dir := t.TempDir()

	// A short, fixed-prefix directory for the socket: t.TempDir() nests
	// this test's long name into the path, past darwin's ~104-byte unix
	// socket limit.
	sockDir, err := os.MkdirTemp("", "rc")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(sockDir) })

	hub := attachsrv.NewHub(64)
	srv, err := attachsrv.Listen(context.Background(), attachsrv.Options{
		Bind: filepath.Join(sockDir, "s.sock"),
		Dir:  dir,
		Hub:  hub,
	})
	if err != nil {
		t.Fatalf("attachsrv.Listen: %v", err)
	}
	defer func() { _ = srv.Close() }()

	sentinel := filepath.Join(dir, "cancelled")

	// Stands in for internal/engine/control.go's own consumer: the one
	// thing bestEffortCancel needs to reach for the fix to matter at all.
	stop := make(chan struct{})
	t.Cleanup(func() { close(stop) })
	go func() {
		for {
			select {
			case req := <-hub.Control():
				if req.Op == api.OpRunCancel {
					_ = os.WriteFile(sentinel, []byte("x"), 0o644)
				}
				req.Reply <- sink.ControlResponse{ID: req.ID, OK: true}
			case <-stop:
				return
			}
		}
	}()

	// A child that ignores INT/TERM and exits only once it sees the
	// sentinel file: standing in for "the engine actually acted on
	// run.cancel and shut the pipeline down".
	script := `trap '' INT TERM
while [ ! -f "$1" ]; do sleep 0.02; done
exit 7`
	cmd := exec.Command("sh", "-c", script, "sh", sentinel)
	if err := cmd.Start(); err != nil {
		t.Fatalf("start child: %v", err)
	}

	registerEntry(t, attachsrv.Entry{PID: cmd.Process.Pid, Socket: srv.Addr(), RunID: "r1"})

	ctx, cancel := context.WithCancel(context.Background())
	var interrupted atomic.Bool
	go func() {
		time.Sleep(300 * time.Millisecond)
		interrupted.Store(true)
		cancel()
	}()

	var stdout, stderr strings.Builder
	done := make(chan int, 1)
	go func() { done <- runPipeline(ctx, cmd, uiNone, &stdout, &stderr, &interrupted) }()

	select {
	case code := <-done:
		if want := exitCodeForInterrupted(""); code != want {
			t.Errorf("exit code = %d, want %d (interrupted)", code, want)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("runPipeline did not return within 10s of the interrupt — bestEffortCancel never " +
			"reached the live engine while the process was still alive")
	}

	if _, err := os.Stat(sentinel); err != nil {
		t.Errorf("sentinel file was never written — run.cancel never reached the control consumer "+
			"while the process was still alive: %v", err)
	}
}
