package source_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	tea "github.com/charmbracelet/bubbletea"

	"github.com/xavidop/senro"
	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/attach"
	"github.com/xavidop/senro/exec"
	"github.com/xavidop/senro/internal/eventlog"
	"github.com/xavidop/senro/internal/render"
	"github.com/xavidop/senro/internal/source"
	"github.com/xavidop/senro/internal/tui"
	"github.com/xavidop/senro/retry"
)

// The claim across this whole phase is that the offline debugger is the
// same client with a different Source. A table running IDENTICAL
// assertions against every implementation is what makes that testable: if
// FileSource and LiveSource ever disagree, this table fails on both.
//
// Control is deliberately excluded: File and Live are NOT interchangeable
// there by design; see TestControlSucceedsLiveAndIsRefusedOnDisk.
//
// Every factory is built from the SAME fixture (writeRun) so all three
// serve byte-identical content.
func conformanceSources(t *testing.T) map[string]func(t *testing.T) source.Source {
	return map[string]func(t *testing.T) source.Source{
		"FileSource": func(t *testing.T) source.Source {
			t.Helper()
			dir := writeRun(t, twoStepRun())
			fs, err := source.OpenFile(dir, false)
			if err != nil {
				t.Fatalf("OpenFile: %v", err)
			}
			t.Cleanup(func() { _ = fs.Close() })
			return fs
		},
		"LiveSource": func(t *testing.T) source.Source {
			t.Helper()
			dir := writeRun(t, twoStepRun())
			_, hub, sockPath := newLiveServer(t, dir, liveServerOpts{})
			emitRecordedEvents(t, hub, dir)
			live, err := source.Dial(context.Background(), sockPath)
			if err != nil {
				t.Fatalf("Dial: %v", err)
			}
			t.Cleanup(func() { _ = live.Close() })
			return live
		},
		"FallbackSource (fallen back)": func(t *testing.T) source.Source {
			t.Helper()
			dir := writeRun(t, twoStepRun())
			srv, hub, sockPath := newLiveServer(t, dir, liveServerOpts{})
			live, err := source.Dial(context.Background(), sockPath)
			if err != nil {
				t.Fatalf("Dial: %v", err)
			}
			// Simulate the engine and its socket going away: once fallen
			// back, a FallbackSource must be indistinguishable from a
			// FileSource for State/Subscribe/Logs.
			if err := srv.Close(); err != nil {
				t.Fatalf("srv.Close: %v", err)
			}
			_ = hub.Close()
			fb := source.Fallback(live, dir)
			t.Cleanup(func() { _ = fb.Close() })
			return fb
		},
	}
}

func TestConformanceStateFoldsARecordedRun(t *testing.T) {
	for name, factory := range conformanceSources(t) {
		t.Run(name, func(t *testing.T) {
			src := factory(t)

			st, err := src.State(context.Background())
			if err != nil {
				t.Fatalf("State: %v", err)
			}
			if !st.Run.Done || st.Run.Status != api.RunSucceeded {
				t.Errorf("run = %+v, want a finished succeeded run", st.Run)
			}
			if len(st.Steps) != 2 || len(st.Order) != 2 {
				t.Errorf("Steps=%d Order=%d, want 2 and 2", len(st.Steps), len(st.Order))
			}
		})
	}
}

// Subscribe(fromSeq) is inclusive and strictly increasing. Reads exactly N
// events bounded by a timeout rather than draining to close: a live run's
// channel may legitimately never close on its own, and the interface does
// not promise otherwise.
func TestConformanceSubscribeIsInclusiveOfFromSeq(t *testing.T) {
	for name, factory := range conformanceSources(t) {
		t.Run(name, func(t *testing.T) {
			src := factory(t)

			all := recvN(t, src, 0, 9, 3*time.Second)
			if len(all) != 9 {
				t.Fatalf("fixture: got %d events, want 9", len(all))
			}
			target := all[1].Seq

			got := recvN(t, src, target, len(all)-1, 3*time.Second)
			if len(got) == 0 || got[0].Seq != target {
				t.Fatalf("Subscribe(%d) first event = %v, want seq %d exactly", target, got, target)
			}
			last := uint64(0)
			for i, e := range got {
				if e.Seq <= last {
					t.Fatalf("event %d regressed: seq %d after %d", i, e.Seq, last)
				}
				last = e.Seq
			}
			if len(got) != len(all)-1 {
				t.Errorf("Subscribe(%d) yielded %d events, want %d", target, len(got), len(all)-1)
			}
		})
	}
}

func TestConformanceLogsServesAByteRange(t *testing.T) {
	for name, factory := range conformanceSources(t) {
		t.Run(name, func(t *testing.T) {
			src := factory(t)

			rc, err := src.Logs(context.Background(), "build", 1, api.StreamStdout, 0)
			if err != nil {
				t.Fatalf("Logs: %v", err)
			}
			b, err := io.ReadAll(rc)
			_ = rc.Close()
			if err != nil {
				t.Fatalf("read: %v", err)
			}
			if len(b) == 0 {
				t.Fatal("Logs returned nothing for a step that wrote output")
			}

			rc2, err := src.Logs(context.Background(), "build", 1, api.StreamStdout, 2)
			if err != nil {
				t.Fatalf("Logs(from=2): %v", err)
			}
			b2, err := io.ReadAll(rc2)
			_ = rc2.Close()
			if err != nil {
				t.Fatalf("read: %v", err)
			}
			if len(b2) != len(b)-2 {
				t.Errorf("from=2 returned %d bytes, want %d", len(b2), len(b)-2)
			}
			if string(b2) != string(b[2:]) {
				t.Errorf("from=2 body = %q, want %q", b2, b[2:])
			}
		})
	}
}

// The one place File and Live are DELIBERATELY not interchangeable:
// Control acts on a run, and only a live engine can carry an action out.
// Excluded from the table above, asserted here as an explicit divergence.
func TestControlSucceedsLiveAndIsRefusedOnDisk(t *testing.T) {
	t.Run("FileSource", func(t *testing.T) {
		dir := writeRun(t, twoStepRun())
		fs, err := source.OpenFile(dir, false)
		if err != nil {
			t.Fatalf("OpenFile: %v", err)
		}
		defer func() { _ = fs.Close() }()

		_, err = fs.Control(context.Background(), api.Frame{
			V: api.Version, Kind: api.KindReq, ID: "c1", Type: api.OpRunCancel,
		})
		if err == nil {
			t.Error("Control on a file source succeeded, want ErrReadOnly")
		}
	})

	t.Run("LiveSource", func(t *testing.T) {
		dir := writeRun(t, twoStepRun())
		_, hub, sockPath := newLiveServer(t, dir, liveServerOpts{})

		go func() {
			req := <-hub.Control()
			req.Reply <- controlResponseOK(req.ID)
		}()

		live, err := source.Dial(context.Background(), sockPath)
		if err != nil {
			t.Fatalf("Dial: %v", err)
		}
		defer func() { _ = live.Close() }()

		res, err := live.Control(context.Background(), api.Frame{
			V: api.Version, Kind: api.KindReq, ID: "c1", Type: api.OpStepRetry,
		})
		if err != nil {
			t.Fatalf("Control: %v", err)
		}
		if res.OK == nil || !*res.OK {
			t.Errorf("OK = %v, want true", res.OK)
		}
		if res.ID != "c1" {
			t.Errorf("ID = %q, want %q", res.ID, "c1")
		}
	})
}

// --- The table above only counts events and checks seq order; a stream
// with every other field mutated would still pass it. This folds each
// implementation's SUBSCRIBED stream (not /api/state, a different server
// path) against the ground truth read from the same run's events.jsonl,
// field by field.

// conformanceFixture returns a fresh temp dir holding a fully-written,
// finished two-step run, and the exact events.jsonl content backing it:
// ground truth every conformance factory must reproduce, not just in count
// and seq order.
func conformanceFixture(t *testing.T) (dir string, want []api.Event) {
	t.Helper()
	dir = writeRun(t, twoStepRun())
	want, err := eventlog.Read(filepath.Join(dir, "events.jsonl"))
	if err != nil {
		t.Fatalf("eventlog.Read: %v", err)
	}
	return dir, want
}

func TestConformanceSubscribeDeliversIdenticalEventContent(t *testing.T) {
	cases := map[string]func(t *testing.T) (source.Source, []api.Event){
		"FileSource": func(t *testing.T) (source.Source, []api.Event) {
			dir, want := conformanceFixture(t)
			fs, err := source.OpenFile(dir, false)
			if err != nil {
				t.Fatalf("OpenFile: %v", err)
			}
			t.Cleanup(func() { _ = fs.Close() })
			return fs, want
		},
		"LiveSource": func(t *testing.T) (source.Source, []api.Event) {
			dir, want := conformanceFixture(t)
			_, hub, sockPath := newLiveServer(t, dir, liveServerOpts{})
			emitRecordedEvents(t, hub, dir)
			live, err := source.Dial(context.Background(), sockPath)
			if err != nil {
				t.Fatalf("Dial: %v", err)
			}
			t.Cleanup(func() { _ = live.Close() })
			return live, want
		},
		"FallbackSource (fallen back)": func(t *testing.T) (source.Source, []api.Event) {
			dir, want := conformanceFixture(t)
			srv, hub, sockPath := newLiveServer(t, dir, liveServerOpts{})
			live, err := source.Dial(context.Background(), sockPath)
			if err != nil {
				t.Fatalf("Dial: %v", err)
			}
			if err := srv.Close(); err != nil {
				t.Fatalf("srv.Close: %v", err)
			}
			_ = hub.Close()
			fb := source.Fallback(live, dir)
			t.Cleanup(func() { _ = fb.Close() })
			return fb, want
		},
	}

	for name, factory := range cases {
		t.Run(name, func(t *testing.T) {
			src, want := factory(t)

			got := recvN(t, src, 0, len(want), 3*time.Second)
			if len(got) != len(want) {
				t.Fatalf("got %d events, want %d", len(got), len(want))
			}
			for i := range want {
				g, w := got[i], want[i]
				switch {
				case g.Seq != w.Seq:
					t.Errorf("event %d: Seq = %d, want %d", i, g.Seq, w.Seq)
				case g.Type != w.Type:
					t.Errorf("event %d: Type = %q, want %q", i, g.Type, w.Type)
				case g.Run != w.Run:
					t.Errorf("event %d: Run = %q, want %q", i, g.Run, w.Run)
				case g.Step != w.Step:
					t.Errorf("event %d: Step = %q, want %q", i, g.Step, w.Step)
				case g.Attempt != w.Attempt:
					t.Errorf("event %d: Attempt = %d, want %d", i, g.Attempt, w.Attempt)
				case !g.TS.Equal(w.TS):
					t.Errorf("event %d: TS = %v, want %v", i, g.TS, w.TS)
				case !bytes.Equal(g.Payload, w.Payload):
					t.Errorf("event %d: Payload = %s, want %s", i, g.Payload, w.Payload)
				}
			}
		})
	}
}

// --- A fallen-back source serving a finished run's disk copy with
// follow=true and no stop condition would leave its Subscribe channel
// never closing, hanging every `for e := range ch` client. The table above
// never exercises channel-close semantics, so this runs render.Plain
// itself against all three implementations over a finished run and
// requires each to return.
//
// Own factories rather than conformanceSources: this test needs "the run
// is over AND the engine has gone", so the Live case closes its hub, the
// same shutdown signal a real engine gives. (A still-open hub is a
// legitimate thing to Subscribe against, and render.Plain correctly does
// not return against one.)
func TestConformanceRenderPlainReturnsForAFinishedRun(t *testing.T) {
	cases := map[string]func(t *testing.T) source.Source{
		"FileSource": func(t *testing.T) source.Source {
			dir := writeRun(t, twoStepRun())
			fs, err := source.OpenFile(dir, false)
			if err != nil {
				t.Fatalf("OpenFile: %v", err)
			}
			t.Cleanup(func() { _ = fs.Close() })
			return fs
		},
		"LiveSource": func(t *testing.T) source.Source {
			dir := writeRun(t, twoStepRun())
			_, hub, sockPath := newLiveServer(t, dir, liveServerOpts{})
			emitRecordedEvents(t, hub, dir)
			live, err := source.Dial(context.Background(), sockPath)
			if err != nil {
				t.Fatalf("Dial: %v", err)
			}
			t.Cleanup(func() { _ = live.Close() })

			// Close the hub only after Subscribe has registered: closing
			// earlier hits the already-covered 503 path, not the mid-stream
			// clean close a real engine's shutdown produces. Polling
			// hub.SubscriberCount() avoids a fixed delay that would flake
			// on a loaded CI box.
			go func() {
				deadline := time.Now().Add(10 * time.Second)
				for hub.SubscriberCount() == 0 && time.Now().Before(deadline) {
					time.Sleep(time.Millisecond)
				}
				_ = hub.Close()
			}()
			return live
		},
		"FallbackSource (fallen back)": func(t *testing.T) source.Source {
			dir := writeRun(t, twoStepRun())
			srv, hub, sockPath := newLiveServer(t, dir, liveServerOpts{})
			live, err := source.Dial(context.Background(), sockPath)
			if err != nil {
				t.Fatalf("Dial: %v", err)
			}
			if err := srv.Close(); err != nil {
				t.Fatalf("srv.Close: %v", err)
			}
			_ = hub.Close()
			fb := source.Fallback(live, dir)
			t.Cleanup(func() { _ = fb.Close() })
			return fb
		},
	}

	for name, factory := range cases {
		t.Run(name, func(t *testing.T) {
			src := factory(t)

			type result struct {
				status api.RunStatus
				err    error
			}
			done := make(chan result, 1)
			go func() {
				status, err := render.Plain(context.Background(), src, io.Discard)
				done <- result{status, err}
			}()

			select {
			case r := <-done:
				if r.err != nil {
					t.Fatalf("render.Plain: %v", r.err)
				}
				if r.status != api.RunSucceeded {
					t.Errorf("status = %q, want %q", r.status, api.RunSucceeded)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("render.Plain never returned for a finished run — the source's Subscribe channel never closed")
			}
		})
	}
}

// --- helpers ---

// recvN Subscribes from fromSeq and reads exactly n events off the returned
// channel, bounded by timeout.
func recvN(t *testing.T, src source.Source, fromSeq uint64, n int, timeout time.Duration) []api.Event {
	t.Helper()
	ch, err := src.Subscribe(context.Background(), fromSeq)
	if err != nil {
		t.Fatalf("Subscribe(%d): %v", fromSeq, err)
	}
	return recvNEvents(t, ch, n, timeout)
}

// --- A real pipeline, through a real attach server ---
//
// Everything above proves the three implementations agree on a
// pre-recorded event sequence but never starts an actual engine. This
// section runs senro.RunPlan behind a real attach server, attaches a
// LiveSource while it runs, and compares the resulting fold field for
// field against a FileSource on the same directory and a FallbackSource
// after the engine is gone.

// isolateAttachRegistry points attach.Listen's registry lookup (and the
// AutoUnixSocket path derived from it) at a throwaway directory. Mirrors
// run_test.go's and attach_test.go's helpers; no shared test-support
// package exists to hold one copy. os.MkdirTemp with a short prefix, not
// t.TempDir(): AutoUnixSocket binds "<pid>.sock" under it, and
// t.TempDir()'s test-name nesting blows past darwin's ~104-byte unix
// socket path limit.
func isolateAttachRegistry(t *testing.T) {
	t.Helper()
	dir, err := os.MkdirTemp("", "e2e")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	t.Setenv("HOME", dir)
	t.Setenv("XDG_RUNTIME_DIR", dir)
}

// e2eScenario is one real pipeline this table builds, runs behind a real
// attach server, and reads back four ways: a plain failure, a recovering
// retry, handlers, and a cancelled run, not just the all-succeeds case.
type e2eScenario struct {
	name string
	// build constructs the plan fresh for each run: plans are immutable
	// once built, never reused across table entries.
	build func(t *testing.T) *senro.Plan
	// wantStatus sanity-checks the scenario itself, so a typo'd predicate
	// that silently changes what it exercises fails loudly here rather
	// than the real assertions agreeing on the wrong answer.
	wantStatus api.RunStatus
	// cancelAfter, if non-zero, cancels the run's own context after this
	// long: the one scenario that interrupts a step mid-flight.
	cancelAfter time.Duration
}

func e2eScenarios() []e2eScenario {
	return []e2eScenario{
		{name: "success", build: buildE2ESuccessPlan, wantStatus: api.RunSucceeded},
		{name: "failure", build: buildE2EFailurePlan, wantStatus: api.RunFailed},
		{name: "retry_recovers", build: buildE2ERetryRecoversPlan, wantStatus: api.RunSucceededWithRecovery},
		{name: "handler_on_failure_runs", build: buildE2EHandlerOnFailurePlan, wantStatus: api.RunFailed},
		{name: "always_handler_runs_on_success", build: buildE2EAlwaysHandlerPlan, wantStatus: api.RunSucceeded},
		{name: "cancelled_mid_run", build: buildE2ECancelledPlan, wantStatus: api.RunCancelled,
			cancelAfter: 300 * time.Millisecond},
	}
}

func buildE2ESuccessPlan(t *testing.T) *senro.Plan {
	t.Helper()
	pipe := senro.New("e2e")
	l := pipe.Workflow("main")
	l.Step("setup", exec.Command("true"))
	l.Step("build", exec.Command("echo", "build")).Needs("setup")
	p, err := pipe.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	return p
}

func buildE2EFailurePlan(t *testing.T) *senro.Plan {
	t.Helper()
	pipe := senro.New("e2e")
	l := pipe.Workflow("main")
	l.Step("boom", exec.Command("sh", "-c", "exit 7"))
	p, err := pipe.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	return p
}

// buildE2ERetryRecoversPlan reuses golden_test.go's marker-file trick: a
// relative ../marker lands in the directory both attempts of "flaky"
// share, so the first attempt fails and touches it, the second finds it
// and succeeds. Deterministic: no sleep, no host-specific absolute path in
// the recorded command.
func buildE2ERetryRecoversPlan(t *testing.T) *senro.Plan {
	t.Helper()
	pipe := senro.New("e2e")
	l := pipe.Workflow("main")
	l.Step("flaky", exec.Command("sh", "-c",
		`if [ -f ../marker ]; then exit 0; else touch ../marker; exit 1; fi`)).
		RetryPolicy(retry.Policy{
			MaxAttempts: 2,
			On:          retry.OnExitCode(1),
			Backoff:     retry.Backoff{Base: 10 * time.Millisecond, Max: 100 * time.Millisecond, Factor: 2},
		})
	p, err := pipe.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	return p
}

func buildE2EHandlerOnFailurePlan(t *testing.T) *senro.Plan {
	t.Helper()
	pipe := senro.New("e2e")
	l := pipe.Workflow("main")
	l.Step("deploy", exec.Command("sh", "-c", "exit 9")).
		OnFailure(senro.Handler("collect", exec.Command("echo", "evidence")))
	p, err := pipe.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	return p
}

func buildE2EAlwaysHandlerPlan(t *testing.T) *senro.Plan {
	t.Helper()
	pipe := senro.New("e2e")
	l := pipe.Workflow("main")
	l.Step("work", exec.Command("true")).
		Always(senro.Handler("cleanup", exec.Command("true")))
	p, err := pipe.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	return p
}

// buildE2ECancelledPlan's "slow" step sleeps far longer than cancelAfter
// (5s vs 300ms) so cancellation lands while it is genuinely still running,
// however loaded the box; the engine also cancels a not-yet-started node,
// so the run stays bounded well under 5s either way.
func buildE2ECancelledPlan(t *testing.T) *senro.Plan {
	t.Helper()
	pipe := senro.New("e2e")
	l := pipe.Workflow("main")
	l.Step("quick", exec.Command("true"))
	l.Step("slow", exec.Command("sleep", "5")).Needs("quick")
	p, err := pipe.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	return p
}

// e2eResult carries every fold this harness computed for one real, attached
// run: the same run, folded four independent ways, plus the directory it
// was written to, for the renderer-agreement check.
type e2eResult struct {
	dir string
	// runErr is whatever senro.RunPlan itself returned; not asserted on
	// directly, kept for a failing subtest's diagnostics.
	runErr error

	// liveReplay is folded from a LiveSource Subscribed from seq 0 BEFORE
	// senro.RunPlan is called: every event folded as the real engine
	// emits it, not read back afterward.
	liveReplay *api.RunState

	// liveResume is folded the way render.Plain and the TUI do it: one
	// State() snapshot, then Subscribe(seq+1), on a second LiveSource.
	// This exercises the server's /api/state fold, which liveReplay never
	// touches; comparing the two below is the
	// resume-must-not-change-the-answer proof.
	liveResume *api.RunState

	// file is read back from dir once the run is over: the offline
	// debugger's own view.
	file *api.RunState

	// fallback comes from a third LiveSource, dialed while the engine was
	// live but untouched until AFTER the attach server closed; see
	// runRealAttachedPipeline for why that ordering matters.
	fallback *api.RunState
}

// runRealAttachedPipeline runs p to completion behind a real attach.Listen
// server, folding the result four ways for the caller to compare. cancelAfter,
// if non-zero, cancels the run's own context after that long.
func runRealAttachedPipeline(t *testing.T, p *senro.Plan, cancelAfter time.Duration) *e2eResult {
	t.Helper()
	isolateAttachRegistry(t)
	dir := t.TempDir()

	runCtx := context.Background()
	if cancelAfter > 0 {
		var cancel context.CancelFunc
		runCtx, cancel = context.WithCancel(runCtx)
		go func() {
			time.Sleep(cancelAfter)
			cancel()
		}()
	}

	att, err := attach.Listen(context.Background(), attach.Options{
		Bind: attach.AutoUnixSocket, Dir: dir, RunID: "01E2E",
	})
	if err != nil {
		t.Fatalf("attach.Listen: %v", err)
	}

	// Four live connections, all dialed while the engine is up: the plain
	// Subscribe(0) replay, the snapshot-then-resume pairing, one held
	// untouched for the Fallback leg, and streamEndConn, which exists
	// solely to observe the terminal marker's Reason.
	replayConn, err := source.Dial(context.Background(), att.Addr())
	if err != nil {
		t.Fatalf("Dial (replay): %v", err)
	}
	resumeConn, err := source.Dial(context.Background(), att.Addr())
	if err != nil {
		t.Fatalf("Dial (resume): %v", err)
	}
	fallbackConn, err := source.Dial(context.Background(), att.Addr())
	if err != nil {
		t.Fatalf("Dial (fallback): %v", err)
	}
	streamEndConn, err := source.Dial(context.Background(), att.Addr())
	if err != nil {
		t.Fatalf("Dial (stream end): %v", err)
	}

	replayDone := make(chan *api.RunState, 1)
	go func() { replayDone <- foldSubscribeFromZero(t, replayConn) }()

	resumeDone := make(chan *api.RunState, 1)
	go func() { resumeDone <- foldSnapshotThenResume(t, resumeConn) }()

	// SubscribeStream: only its second channel exposes the marker's
	// Reason. streamEvents is drained by its own goroutine: the events
	// channel is unbuffered, and a client that stopped reading would wedge
	// the decode loop before it reached the marker line.
	streamEvents, streamEnd, err := streamEndConn.SubscribeStream(context.Background(), 0)
	if err != nil {
		t.Fatalf("SubscribeStream (stream end): %v", err)
	}
	go func() {
		for range streamEvents {
		}
	}()

	runErr := senro.RunPlan(runCtx, p, senro.WithAttach(att))

	// A real embedder's own `defer att.Close()`. Closing here is what lets
	// the still-open Subscribe calls see a clean close (nothing about
	// run.finished on the wire closes a live stream by itself) and what
	// "kills the engine" for the Fallback leg. Whether the close carries
	// the run_ended marker or is markerless is what streamEndConn pins down
	// below, through a real att.Close() rather than a direct hub.Close().
	if err := att.Close(); err != nil {
		t.Fatalf("att.Close: %v", err)
	}

	select {
	case marker, ok := <-streamEnd:
		if !ok {
			t.Fatal("a real attach.Close(), in the same shutdown sequence a real embedder uses, " +
				"produced a markerless close for this attached client instead of the run_ended " +
				"terminal marker")
		}
		if marker.Reason != "run_ended" {
			t.Errorf("StreamEnd.Reason via a real attach.Close() = %q, want %q", marker.Reason, "run_ended")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for att.Close() to deliver a terminal stream marker")
	}
	_ = streamEndConn.Close()

	liveReplay := <-replayDone
	liveResume := <-resumeDone
	_ = replayConn.Close()
	_ = resumeConn.Close()

	fs, err := source.OpenFile(dir, false)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	t.Cleanup(func() { _ = fs.Close() })
	fileState, err := fs.State(context.Background())
	if err != nil {
		t.Fatalf("FileSource.State: %v", err)
	}

	// fallbackConn was dialed while the server was up; its first real call
	// happens only now, with the server closed: a transport failure, then
	// disk. This proves Fallback's OWN transition, not merely two fresh
	// FileSources agreeing.
	fb := source.Fallback(fallbackConn, dir)
	t.Cleanup(func() { _ = fb.Close() })
	fbState, err := fb.State(context.Background())
	if err != nil {
		t.Fatalf("FallbackSource.State once the engine is gone: %v", err)
	}

	return &e2eResult{
		dir: dir, runErr: runErr,
		liveReplay: liveReplay, liveResume: liveResume,
		file: fileState, fallback: fbState,
	}
}

// foldSubscribeFromZero folds every event a fresh Subscribe(0) delivers into
// a brand new RunState, until the channel closes.
func foldSubscribeFromZero(t *testing.T, src source.Source) *api.RunState {
	t.Helper()
	ch, err := src.Subscribe(context.Background(), 0)
	if err != nil {
		t.Fatalf("Subscribe(0): %v", err)
	}
	st := api.NewRunState()
	for e := range ch {
		if err := st.Apply(e); err != nil {
			t.Fatalf("Apply: %v", err)
		}
	}
	return st
}

// foldSnapshotThenResume is render.Plain's own algorithm: one State()
// snapshot, then Subscribe(seq+1) folded onward into it until the channel
// closes. Wherever the snapshot lands, what matters is that Apply never
// sees an event twice and never misses one.
func foldSnapshotThenResume(t *testing.T, src source.Source) *api.RunState {
	t.Helper()
	st, err := src.State(context.Background())
	if err != nil {
		t.Fatalf("State: %v", err)
	}
	ch, err := src.Subscribe(context.Background(), st.Seq+1)
	if err != nil {
		t.Fatalf("Subscribe(%d): %v", st.Seq+1, err)
	}
	for e := range ch {
		if err := st.Apply(e); err != nil {
			t.Fatalf("Apply: %v", err)
		}
	}
	return st
}

// assertFoldsAgree compares two RunStates folded by different means for
// the SAME run. Indented JSON rather than reflect.DeepEqual: normalized,
// map-key-sorted, and the failure shows exactly which field diverged.
func assertFoldsAgree(t *testing.T, got, want *api.RunState) {
	t.Helper()
	g, w := *got, *want
	// ProtoMajor/ProtoMinor are populated only by decoding a live engine's
	// /api/state response (api.RunState's doc); zero on every other fold.
	// A documented difference, not a divergence, so normalized away here.
	// Everything else is compared exactly.
	g.ProtoMajor, g.ProtoMinor = 0, 0
	w.ProtoMajor, w.ProtoMinor = 0, 0

	gb, err := json.MarshalIndent(&g, "", "  ")
	if err != nil {
		t.Fatalf("marshal got: %v", err)
	}
	wb, err := json.MarshalIndent(&w, "", "  ")
	if err != nil {
		t.Fatalf("marshal want: %v", err)
	}
	if !bytes.Equal(gb, wb) {
		t.Errorf("folded state disagrees\n--- got ---\n%s\n--- want ---\n%s", gb, wb)
	}
}

func TestRealAttachedRunAgreesAcrossLiveFileFallbackAndRenderers(t *testing.T) {
	for _, sc := range e2eScenarios() {
		t.Run(sc.name, func(t *testing.T) {
			p := sc.build(t)
			result := runRealAttachedPipeline(t, p, sc.cancelAfter)

			if result.file.Run.Status != sc.wantStatus {
				t.Fatalf("scenario %s: FileSource run status = %q, want %q (runErr=%v): this "+
					"scenario is not exercising what its name claims, so nothing below is a "+
					"meaningful check of it", sc.name, result.file.Run.Status, sc.wantStatus, result.runErr)
			}

			// Requirement 1: the same run, three sources, identical folded
			// state. The whole state, not a hand-picked field.
			t.Run("live_replay_from_zero_matches_disk", func(t *testing.T) {
				assertFoldsAgree(t, result.liveReplay, result.file)
			})
			t.Run("live_snapshot_then_resume_matches_disk", func(t *testing.T) {
				assertFoldsAgree(t, result.liveResume, result.file)
			})
			t.Run("fallback_once_the_engine_is_gone_matches_disk", func(t *testing.T) {
				assertFoldsAgree(t, result.fallback, result.file)
			})

			// Requirement 3: resuming from a snapshot must not change the
			// answer relative to following from seq 0. Checked directly,
			// not just inferred from both separately matching disk above.
			t.Run("resume_from_a_snapshot_matches_full_replay_from_zero", func(t *testing.T) {
				assertFoldsAgree(t, result.liveResume, result.liveReplay)
			})

			// Requirement 4: the plain renderer and the TUI describe the
			// same outcome for this same run.
			t.Run("plain_and_tui_renderers_agree", func(t *testing.T) {
				assertRenderersAgree(t, result.dir, result.file.Order)
			})
		})
	}
}

// --- renderer agreement (requirement 4) ---
//
// render.Plain and the TUI are never byte-identical, so comparing raw
// output would be either always-false or trivially true. What IS
// meaningful: render/plain.go's printStep and tui/view.go's
// stepStatusLabel are two independently hand-written switches over the
// same api.State values, and the test above already proves both renderers
// get the identical RunState. Extracting each fact from each renderer's
// own output and comparing the strings catches a broken label; another
// state-to-state comparison would not.
func assertRenderersAgree(t *testing.T, dir string, stepIDs []string) {
	t.Helper()
	if len(stepIDs) == 0 {
		t.Fatal("no step IDs to compare: this check would be vacuous")
	}

	plainSrc, err := source.OpenFile(dir, false)
	if err != nil {
		t.Fatalf("OpenFile (plain): %v", err)
	}
	defer func() { _ = plainSrc.Close() }()
	var plainOut bytes.Buffer
	if _, err := render.Plain(context.Background(), plainSrc, &plainOut); err != nil {
		t.Fatalf("render.Plain: %v", err)
	}
	plainText := plainOut.String()

	tuiSrc, err := source.OpenFile(dir, false)
	if err != nil {
		t.Fatalf("OpenFile (tui): %v", err)
	}
	m := tui.New(tuiSrc)
	m = driveTUIToCompletion(t, m, m.Init())
	tuiText := stripANSI(m.View())

	plainRun, ok := plainRunStatusText(plainText)
	if !ok {
		t.Fatalf("render.Plain produced no final \"run <status>\" line:\n%s", plainText)
	}
	tuiRun, ok := tuiFooterRunStatusText(tuiText)
	if !ok {
		t.Fatalf("TUI View() footer has no \"run: <status>\" text:\n%s", tuiText)
	}
	if plainRun != tuiRun {
		t.Errorf("run status: Plain's own output says %q, TUI's own output says %q", plainRun, tuiRun)
	}

	for _, id := range stepIDs {
		wantStatus, ok := plainStepStatusText(plainText, id)
		if !ok {
			t.Errorf("render.Plain has no line for step %q:\n%s", id, plainText)
			continue
		}
		if !tuiContainsStepStatus(tuiText, id, wantStatus) {
			t.Errorf("step %q: Plain's own text says %q, but the TUI's own View() never contains "+
				"%q anywhere: the identical folded state, rendered two different ways, disagrees "+
				"about what happened:\n%s", id, wantStatus, id+" "+wantStatus, tuiText)
		}
	}
}

// driveTUIToCompletion mirrors internal/tui's own model_test.go driver:
// resolve cmd synchronously, without a pty. That helper is unexported in
// package tui_test, so this is a from-scratch copy of the same shape.
//
// Recognizing tui.TickMsg stops this from chasing the model's
// self-rescheduling ~30Hz tick forever: one hop is enough once fetchState
// has drained the subscription against a finished FileSource.
func driveTUIToCompletion(t *testing.T, m *tui.Model, cmd tea.Cmd) *tui.Model {
	t.Helper()
	if cmd == nil {
		return m
	}
	msg := cmd()
	switch v := msg.(type) {
	case nil:
		return m
	case tea.BatchMsg:
		for _, c := range v {
			m = driveTUIToCompletion(t, m, c)
		}
		return m
	default:
		next, follow := m.Update(v)
		nm, ok := next.(*tui.Model)
		if !ok {
			t.Fatalf("Update returned %T, want *tui.Model", next)
		}
		if _, isTick := v.(tui.TickMsg); isTick {
			return nm
		}
		return driveTUIToCompletion(t, nm, follow)
	}
}

// ansiEscapeRE strips lipgloss's SGR codes so the TUI's raw View() can be
// scanned for text; mirrors tui_test's unexported helper, which cannot be
// imported from here.
var ansiEscapeRE = regexp.MustCompile("\x1b\\[[0-9;]*m")

func stripANSI(s string) string { return ansiEscapeRE.ReplaceAllString(s, "") }

// plainStepStatusText finds render.Plain's lifecycle line for step id
// ("<id> <status>" or "<id> <status>: <error>") and returns the text after
// "<id> ". Plain also writes "<id> <stream> | <output>" log lines; those
// are skipped: this check is about the status switches agreeing.
func plainStepStatusText(text, id string) (string, bool) {
	for _, line := range strings.Split(text, "\n") {
		rest, ok := strings.CutPrefix(line, id+" ")
		if !ok || isPlainLogLine(rest) {
			continue
		}
		return rest, true
	}
	return "", false
}

// isPlainLogLine reports whether what follows "<id> " on one of Plain's
// lines is a relayed line of the step's own output rather than its status.
func isPlainLogLine(rest string) bool {
	for _, stream := range []string{api.StreamStdout, api.StreamStderr} {
		if strings.HasPrefix(rest, stream+" | ") {
			return true
		}
	}
	return false
}

// tuiContainsStepStatus reports whether View()'s raw text contains
// "<id> <status>" verbatim: the exact text printStep produced. A substring
// search, not a column-anchored parse: View() lays the steps and log panes
// side by side, so a step's row is not alone on its own line. The
// scenarios' id/status vocabulary is disjoint from the chrome, so Contains
// cannot false-positive here.
func tuiContainsStepStatus(tuiText, id, status string) bool {
	return strings.Contains(tuiText, id+" "+status)
}

// plainRunStatusText finds render.Plain's own final "run <status>" line,
// per printRun's own format, and returns the status text.
func plainRunStatusText(text string) (string, bool) {
	for _, line := range strings.Split(strings.TrimRight(text, "\n"), "\n") {
		if rest, ok := strings.CutPrefix(line, "run "); ok {
			return rest, true
		}
	}
	return "", false
}

// tuiFooterRunStatusText finds the TUI footer's own "run: <status> | ..."
// text, per renderFooter's own format, and returns just the status token.
func tuiFooterRunStatusText(text string) (string, bool) {
	const marker = "run: "
	idx := strings.Index(text, marker)
	if idx < 0 {
		return "", false
	}
	rest := text[idx+len(marker):]
	if sp := strings.IndexAny(rest, " |"); sp >= 0 {
		rest = rest[:sp]
	}
	return rest, true
}
