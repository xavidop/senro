package source_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/internal/eventlog"
	"github.com/xavidop/senro/internal/source"
)

func TestFileSourceFoldsARecordedRun(t *testing.T) {
	dir := writeRun(t, twoStepRun())

	fs, err := source.OpenFile(dir, false)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	defer func() { _ = fs.Close() }()

	st, err := fs.State(context.Background())
	if err != nil {
		t.Fatalf("State: %v", err)
	}
	if !st.Run.Done || st.Run.Status != api.RunSucceeded {
		t.Errorf("run = %+v, want a finished succeeded run", st.Run)
	}
	if len(st.Steps) != 2 || len(st.Order) != 2 {
		t.Errorf("Steps=%d Order=%d, want 2 and 2", len(st.Steps), len(st.Order))
	}
}

// Subscribe(fromSeq) must resume exactly where a snapshot left off, with no
// gap and no repeat: that pairing is what makes attach constant-time on a
// run that has already emitted a million events.
func TestSubscribeResumesFromSnapshotSeq(t *testing.T) {
	dir := writeRun(t, twoStepRun())
	fs, _ := source.OpenFile(dir, false)
	defer func() { _ = fs.Close() }()

	st, _ := fs.State(context.Background())
	ch, err := fs.Subscribe(context.Background(), st.Seq+1)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	for e := range ch {
		t.Errorf("snapshot was at seq %d but subscribe yielded seq %d — a full "+
			"snapshot must leave nothing to replay", st.Seq, e.Seq)
	}
}

func TestSubscribeFromZeroReplaysEverything(t *testing.T) {
	dir := writeRun(t, twoStepRun())
	fs, _ := source.OpenFile(dir, false)
	defer func() { _ = fs.Close() }()

	ch, _ := fs.Subscribe(context.Background(), 0)
	var n int
	last := uint64(0)
	for e := range ch {
		n++
		if e.Seq <= last {
			t.Fatalf("event %d regressed: seq %d after %d", n, e.Seq, last)
		}
		last = e.Seq
	}
	if n == 0 {
		t.Fatal("full replay yielded nothing")
	}
}

// The whole point of --follow: a run still being written must stream.
func TestFollowStreamsEventsAppendedLater(t *testing.T) {
	dir := t.TempDir()
	w := startRun(t, dir) // writes run.started, then waits

	fs, err := source.OpenFile(dir, true)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	defer func() { _ = fs.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ch, _ := fs.Subscribe(ctx, 0)

	<-ch // run.started
	w.appendStepCreated("later")

	select {
	case e := <-ch:
		if e.Type != api.StepCreated {
			t.Errorf("got %s, want step.created appended after Subscribe", e.Type)
		}
	case <-ctx.Done():
		t.Fatal("follow did not deliver an event appended after Subscribe")
	}
}

// A follow=true source must stop once run.finished has happened: nothing
// more will ever be appended, and polling on leaks the goroutine and hangs
// range-over-channel callers. Two sub-cases, exercising different paths in
// stream(): run.finished delivered live, and a resume from beyond it,
// where the "already seen" skip filters run.finished out before any
// type check in the delivery branch could see it.
func TestFollowStopsAfterRunFinished(t *testing.T) {
	t.Run("delivered live", func(t *testing.T) {
		dir := t.TempDir()
		w := startRun(t, dir) // seq 1: run.started, ledger left open
		w.appendRunFinished() // seq 2: run.finished

		fs, err := source.OpenFile(dir, true)
		if err != nil {
			t.Fatalf("OpenFile: %v", err)
		}
		defer func() { _ = fs.Close() }()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		ch, err := fs.Subscribe(ctx, 0)
		if err != nil {
			t.Fatalf("Subscribe: %v", err)
		}

		var last api.Event
		n := 0
		for e := range ch {
			n++
			last = e
		}
		if n != 2 {
			t.Fatalf("delivered %d events, want 2", n)
		}
		if last.Type != api.RunFinished {
			t.Fatalf("last event = %s, want run.finished", last.Type)
		}
	})

	t.Run("resumed from beyond run.finished", func(t *testing.T) {
		dir := writeRun(t, twoStepRun()) // ends in run.finished, seq 9

		fs, err := source.OpenFile(dir, true) // follow=true, matching FallbackSource's own disk source
		if err != nil {
			t.Fatalf("OpenFile: %v", err)
		}
		defer func() { _ = fs.Close() }()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		ch, err := fs.Subscribe(ctx, 10) // state.Seq+1, exactly render.Plain's own resume point
		if err != nil {
			t.Fatalf("Subscribe: %v", err)
		}

		select {
		case e, ok := <-ch:
			if ok {
				t.Fatalf("channel produced an event (%+v) resuming past a finished run's last seq, want it to close immediately", e)
			}
		case <-ctx.Done():
			t.Fatal("channel never closed resuming past a finished run's last seq — follow polled forever with nothing left to find")
		}
	})
}

func TestLogsServesAByteRange(t *testing.T) {
	dir := writeRun(t, twoStepRun())
	fs, _ := source.OpenFile(dir, false)
	defer func() { _ = fs.Close() }()

	rc, err := fs.Logs(context.Background(), "build", 1, api.StreamStdout, 0)
	if err != nil {
		t.Fatalf("Logs: %v", err)
	}
	defer func() { _ = rc.Close() }()
	b, _ := io.ReadAll(rc)
	if len(b) == 0 {
		t.Error("Logs returned nothing for a step that wrote output")
	}

	// A range request from an offset must skip exactly that many bytes.
	rc2, _ := fs.Logs(context.Background(), "build", 1, api.StreamStdout, 2)
	defer func() { _ = rc2.Close() }()
	b2, _ := io.ReadAll(rc2)
	if len(b2) != len(b)-2 {
		t.Errorf("from=2 returned %d bytes, want %d", len(b2), len(b)-2)
	}
}

// A file source is a view of something already decided. Refusing control is
// what lets one client render both a live and a finished run without asking
// which it has.
func TestControlIsRefused(t *testing.T) {
	dir := writeRun(t, twoStepRun())
	fs, _ := source.OpenFile(dir, false)
	defer func() { _ = fs.Close() }()

	_, err := fs.Control(context.Background(), api.Frame{
		V: api.Version, Kind: api.KindReq, ID: "c1", Type: api.OpRunCancel,
	})
	if !errors.Is(err, source.ErrReadOnly) {
		t.Errorf("Control on a file source = %v, want ErrReadOnly", err)
	}
}

// A ledger with a torn final line is what kill -9 leaves. The events before
// it are still valid and must still be served.
func TestTornLedgerStillFolds(t *testing.T) {
	dir := writeRun(t, twoStepRun())
	truncateLastLine(t, dir)

	fs, err := source.OpenFile(dir, false)
	if err != nil {
		t.Fatalf("OpenFile on a torn ledger: %v", err)
	}
	defer func() { _ = fs.Close() }()

	st, err := fs.State(context.Background())
	if err != nil {
		t.Fatalf("State on a torn ledger: %v", err)
	}
	if len(st.Steps) == 0 {
		t.Error("a torn tail discarded the whole run")
	}
}

func TestUseAfterCloseErrors(t *testing.T) {
	// Every closeable type in this package guards use-after-close and is
	// idempotent on Close. This one must match.
	dir := writeRun(t, twoStepRun())
	fs, err := source.OpenFile(dir, false)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	if err := fs.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := fs.Close(); err != nil {
		t.Errorf("second Close = %v, want nil", err)
	}

	if _, err := fs.State(context.Background()); !errors.Is(err, source.ErrClosed) {
		t.Errorf("State after Close = %v, want ErrClosed", err)
	}
	if _, err := fs.Subscribe(context.Background(), 0); !errors.Is(err, source.ErrClosed) {
		t.Errorf("Subscribe after Close = %v, want ErrClosed", err)
	}
	if _, err := fs.Logs(context.Background(), "build", 1, api.StreamStdout, 0); !errors.Is(err, source.ErrClosed) {
		t.Errorf("Logs after Close = %v, want ErrClosed", err)
	}
	// The one contract clause the conformance suite's shared table does not
	// cover: a coverage gap, not a known-different behaviour.
	if _, err := fs.Control(context.Background(), api.Frame{Type: api.OpRunCancel}); !errors.Is(err, source.ErrClosed) {
		t.Errorf("Control after Close = %v, want ErrClosed", err)
	}
}

func TestSubscribeIsInclusiveOfFromSeq(t *testing.T) {
	// fromSeq is inclusive (Source.Subscribe). Off by one means a missed
	// event or a folded duplicate, and RunState.Apply rejects a regressing
	// seq, so the duplicate is fatal, not cosmetic.
	dir := writeRun(t, twoStepRun())
	fs, _ := source.OpenFile(dir, false)
	defer func() { _ = fs.Close() }()

	all := drain(t, fs, 0)
	if len(all) < 3 {
		t.Fatalf("fixture too small: %d events", len(all))
	}
	target := all[1].Seq

	got := drain(t, fs, target)
	if len(got) == 0 || got[0].Seq != target {
		t.Fatalf("Subscribe(%d) first event = %v, want seq %d exactly", target, got, target)
	}
	if len(got) != len(all)-1 {
		t.Errorf("Subscribe(%d) yielded %d events, want %d", target, len(got), len(all)-1)
	}
}

// OpenFile is where a caller learns "there is no run recorded here",
// without paying for a full read.
func TestOpenFileRequiresEventsLedger(t *testing.T) {
	dir := t.TempDir() // no events.jsonl ever written

	if _, err := source.OpenFile(dir, false); err == nil {
		t.Error("OpenFile on a directory with no events.jsonl = nil error, want one")
	}
}

// --- helpers ---

// drain collects one Subscribe call's whole delivery into a slice, for tests
// that need to inspect what was sent rather than just that something was.
func drain(t *testing.T, src source.Source, from uint64) []api.Event {
	t.Helper()
	ch, err := src.Subscribe(context.Background(), from)
	if err != nil {
		t.Fatalf("Subscribe(%d): %v", from, err)
	}
	var events []api.Event
	for e := range ch {
		events = append(events, e)
	}
	return events
}

// twoStepRun returns the event sequence for a small, finished two-step
// run: build then test. Deliberately unstamped (no Seq, no TS): writeRun
// sends it through eventlog.Ledger, the only thing allowed to assign those.
func twoStepRun() []api.Event {
	mustBody := func(v any) []byte {
		b, err := json.Marshal(v)
		if err != nil {
			panic(err)
		}
		return b
	}

	return []api.Event{
		{Type: api.RunStarted, Run: "run1", Payload: mustBody(api.RunStartedBody{
			Pipeline:      "ci",
			EngineVersion: "test",
			PlanDigest:    "digest1",
			StartedAt:     time.Now().UTC(),
		})},
		{Type: api.StepCreated, Run: "run1", Step: "build", Payload: mustBody(api.StepCreatedBody{
			Kind: "exec",
		})},
		{Type: api.StepCreated, Run: "run1", Step: "test", Payload: mustBody(api.StepCreatedBody{
			Kind: "exec", Needs: []string{"build"},
		})},
		{Type: api.StepStarted, Run: "run1", Step: "build", Attempt: 1},
		{Type: api.StepLogAppended, Run: "run1", Step: "build", Attempt: 1, Payload: mustBody(api.StepLogAppendedBody{
			Stream: api.StreamStdout, Offset: 0, Len: 12, Lines: 1,
		})},
		{Type: api.StepFinished, Run: "run1", Step: "build", Attempt: 1, Payload: mustBody(api.StepFinishedBody{
			State: api.StateSucceeded,
		})},
		{Type: api.StepStarted, Run: "run1", Step: "test", Attempt: 1},
		{Type: api.StepFinished, Run: "run1", Step: "test", Attempt: 1, Payload: mustBody(api.StepFinishedBody{
			State: api.StateSucceeded,
		})},
		{Type: api.RunFinished, Run: "run1", Payload: mustBody(api.RunFinishedBody{
			Status: api.RunSucceeded,
		})},
	}
}

// writeRun creates a temp dir, writes events through eventlog.Ledger (the
// real engine's path, so Seq and V are stamped as a real run would), plus a
// small stdout log for build so Logs has something to serve.
func writeRun(t *testing.T, events []api.Event) string {
	t.Helper()
	dir := t.TempDir()

	l, err := eventlog.Open(dir)
	if err != nil {
		t.Fatalf("eventlog.Open: %v", err)
	}
	for _, e := range events {
		if _, err := l.Append(e); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	if err := l.Close(); err != nil {
		t.Fatalf("Close ledger: %v", err)
	}

	ls := eventlog.NewLogSet(dir)
	w, err := ls.Writer("build", 1, api.StreamStdout)
	if err != nil {
		t.Fatalf("Writer: %v", err)
	}
	if _, err := w.Write([]byte("building...\n")); err != nil {
		t.Fatalf("write log: %v", err)
	}
	if err := ls.Close(); err != nil {
		t.Fatalf("close log set: %v", err)
	}

	return dir
}

// runHandle is a live ledger a test can keep appending to, to simulate a run
// still in progress on disk while a FileSource follows it.
type runHandle struct {
	t *testing.T
	l *eventlog.Ledger
}

// startRun opens dir's ledger, appends run.started, and returns a handle
// for appending more later. The ledger stays open: exactly the state
// --follow has to cope with.
func startRun(t *testing.T, dir string) *runHandle {
	t.Helper()
	l, err := eventlog.Open(dir)
	if err != nil {
		t.Fatalf("eventlog.Open: %v", err)
	}
	t.Cleanup(func() { _ = l.Close() })

	if _, err := l.Append(api.Event{Type: api.RunStarted, Run: "run1"}); err != nil {
		t.Fatalf("Append run.started: %v", err)
	}
	return &runHandle{t: t, l: l}
}

func (h *runHandle) appendStepCreated(id string) {
	h.t.Helper()
	b, err := json.Marshal(api.StepCreatedBody{Kind: "exec"})
	if err != nil {
		h.t.Fatalf("marshal step.created body: %v", err)
	}
	if _, err := h.l.Append(api.Event{
		Type: api.StepCreated, Run: "run1", Step: id, Payload: b,
	}); err != nil {
		h.t.Fatalf("Append step.created: %v", err)
	}
}

func (h *runHandle) appendRunFinished() {
	h.t.Helper()
	b, err := json.Marshal(api.RunFinishedBody{Status: api.RunSucceeded})
	if err != nil {
		h.t.Fatalf("marshal run.finished body: %v", err)
	}
	if _, err := h.l.Append(api.Event{
		Type: api.RunFinished, Run: "run1", Payload: b,
	}); err != nil {
		h.t.Fatalf("Append run.finished: %v", err)
	}
}

// truncateLastLine chops the final newline-terminated record in dir's
// events.jsonl in half, leaving a torn line with no trailing newline: what
// kill -9 leaves mid-write.
func truncateLastLine(t *testing.T, dir string) {
	t.Helper()
	path := filepath.Join(dir, "events.jsonl")

	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read events.jsonl: %v", err)
	}

	trimmed := strings.TrimRight(string(b), "\n")
	last := strings.LastIndexByte(trimmed, '\n')
	if last < 0 {
		t.Fatalf("events.jsonl has fewer than two records, cannot tear the last one")
	}
	lastLine := trimmed[last+1:]
	torn := trimmed[:last+1] + lastLine[:len(lastLine)/2]

	if err := os.WriteFile(path, []byte(torn), 0o644); err != nil {
		t.Fatalf("write torn events.jsonl: %v", err)
	}
}
