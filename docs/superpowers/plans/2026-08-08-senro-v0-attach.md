# senro v0 Attach Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A running pipeline can be observed and controlled from a separate process. The same client code renders a live run over a unix socket and a finished run from disk, because both are one `Source` interface — and the plain renderer, the TUI and offline replay are all clients of it.

**Architecture:** Two channels, split because they have opposite requirements. The **lifecycle** channel is guaranteed, ordered and never dropped — a few thousand events even for a large run. **Log content never travels on it**: the engine already writes logs to seekable files and emits only a byte-range marker, so clients fetch content by range request or subscribe to a lossy per-step channel for the one step they are looking at. A slow client must never stall the build.

**Tech Stack:** Go 1.26. First third-party dependencies in the root module: a WebSocket implementation and bubbletea/lipgloss for the TUI. `api` stays standard-library only — that constraint is enforced by test and is what makes this protocol consumable by anything.

**Spec:** `docs/superpowers/specs/2026-08-07-senro-v0-design.md` §4.6, §4.7, and the source design's §6.

## Global Constraints

- Module path `github.com/xavidop/senro`. **`api/go.mod` must stay dependency-free.** Two tests enforce it; never resolve a build error by adding a require there.
- **One `Source` interface, two implementations.** Getting this seam right is the whole point: the offline TUI is not a separate feature, it is the same client with a different `Source`. If a renderer ever type-asserts to find out which one it has, the seam has failed.
- **The engine's correctness cannot depend on whether anyone is watching.** Everything here lives behind `sink.Sink`, whose `Emit` is non-blocking and infallible. A slow or wedged client must not stall a run.
- **Lifecycle events are never dropped.** If a client's ring overflows, close the connection with `bye{reason:"lifecycle_overflow"}` and let it reconnect with a fresh snapshot. Losing a `step.finished` silently is a worse failure than a reconnect.
- **Log content is lossy by design.** On overflow, drop and send a gap marker; the client back-fills by range request.
- `api.RunState.Apply` rejects a regressing sequence number, so anything that delivers events must preserve order.
- Comments frame senro as a **pipeline engine**; CI/CD is one thing built on it.
- gofmt-clean, `go test ./... -race` green, working tree clean at every commit. Test-first; where a test asserts an invariant, watch it fail before implementing.

---

## What already exists

The engine writes `events.jsonl` (append-only, flushed per event, `Read` recovers a torn tail), `plan.json`, and per-step per-attempt logs at `logs/<encoded-step>/<attempt>/<stream>` with byte offsets carried in `step.log.appended`. `sink.Sink` has `Emit(api.Event)` and `Control() <-chan ControlRequest`, and `ControlRequest` already carries `ID`, `Op`, `ClientID`, `Args` and a `Reply` channel — the engine side of control is built and unused. `api` provides `RunState` with `Steps`, `Handlers`, `Order` and `Group`, the frame protocol with `KindReq/Res/Evt/Bye`, the control-op name constants, `LogGap`, and `SubscribeArgs`.

**Nothing consumes any of it yet.** No `Source`, no server, no renderer, no CLI.

---

## File Structure

```
internal/source/source.go        the Source interface — the seam everything else depends on
internal/source/file.go          FileSource: events.jsonl + log files, with follow
internal/source/live.go          LiveSource: WebSocket client
internal/source/fallback.go      live → file on bye, so scrollback survives the engine

internal/attachsrv/hub.go        lifecycle ring, per-step log channels, drop + gap
internal/attachsrv/server.go     http.Server over unix or TCP; the JSON and WS endpoints
internal/attachsrv/registry.go   $XDG_RUNTIME_DIR discovery, pid reaping
internal/attachsrv/peercred_*.go SO_PEERCRED / LOCAL_PEERCRED, build-tagged

internal/render/plain.go         the plain line renderer — a Source client, not an engine path
internal/tui/                    bubbletea model, DAG pane, log pane, keys

attach/attach.go                 public: attach.Listen(ctx, Options) for embedding
cmd/senro/main.go                run · attach · ui
```

`internal/source` is its own package because three implementations and every client depend on it, and it must not import the server or the engine. `internal/render` is separate from `internal/tui` so the plain path cannot accidentally grow a terminal dependency.

---

### Task 1: The Source interface and FileSource

**Files:** Create `internal/source/source.go`, `internal/source/file.go`, `internal/source/file_test.go`

**Interfaces:**
- Consumes: `api`, `internal/eventlog`, `internal/stepid`.
- Produces: `source.Source` interface with `State(ctx) (*api.RunState, error)`, `Subscribe(ctx, fromSeq uint64) (<-chan api.Event, error)`, `Logs(ctx, step string, attempt int, stream string, from int64) (io.ReadCloser, error)`, `Control(ctx, req api.Frame) (api.Frame, error)`, `Close() error`; `source.ErrReadOnly`; `source.OpenFile(dir string, follow bool) (*FileSource, error)`.

- [ ] **Step 1: Write the failing test**

```go
package source_test

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/internal/source"
)

func TestFileSourceFoldsARecordedRun(t *testing.T) {
	dir := writeRun(t, twoStepRun())

	fs, err := source.OpenFile(dir, false)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	defer fs.Close()

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
// gap and no repeat — that pairing is what makes attach constant-time on a
// run that has already emitted a million events.
func TestSubscribeResumesFromSnapshotSeq(t *testing.T) {
	dir := writeRun(t, twoStepRun())
	fs, _ := source.OpenFile(dir, false)
	defer fs.Close()

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
	defer fs.Close()

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
	defer fs.Close()

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

func TestLogsServesAByteRange(t *testing.T) {
	dir := writeRun(t, twoStepRun())
	fs, _ := source.OpenFile(dir, false)
	defer fs.Close()

	rc, err := fs.Logs(context.Background(), "build", 1, api.StreamStdout, 0)
	if err != nil {
		t.Fatalf("Logs: %v", err)
	}
	defer rc.Close()
	b, _ := io.ReadAll(rc)
	if len(b) == 0 {
		t.Error("Logs returned nothing for a step that wrote output")
	}

	// A range request from an offset must skip exactly that many bytes.
	rc2, _ := fs.Logs(context.Background(), "build", 1, api.StreamStdout, 2)
	defer rc2.Close()
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
	defer fs.Close()

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
	defer fs.Close()

	st, err := fs.State(context.Background())
	if err != nil {
		t.Fatalf("State on a torn ledger: %v", err)
	}
	if len(st.Steps) == 0 {
		t.Error("a torn tail discarded the whole run")
	}
}
```

Write the helpers in this file: `twoStepRun()` returns a slice of `api.Event`; `writeRun(t, events)` creates a temp dir, writes them through `eventlog.Ledger`, and writes a small log file for `build`; `startRun(t, dir)` returns a handle with `appendStepCreated(id string)`; `truncateLastLine` chops the final newline-terminated record.

- [ ] **Step 2: Run to verify it fails** — `go test ./internal/source/... -v`, package does not exist.

- [ ] **Step 3: Implement.** Design notes:

- `Source` is the seam. Keep it small and keep `Control` on it: a client that must ask which implementation it holds before offering a retry key has defeated the purpose. `FileSource.Control` returns `ErrReadOnly` for every op.
- `State` folds the whole ledger through `api.RunState.Apply` and returns the result with its `Seq`. `eventlog.Read` already returns the events parsed before a torn tail along with `ErrTruncated`; treat that error as success-with-a-short-log, and say so in a comment.
- `Subscribe(fromSeq)` replays from `fromSeq` and, when `follow` is set, keeps watching the file for appends. Poll — a 50ms tick is imperceptible next to a step's runtime and avoids a filesystem-notification dependency on two platforms. Close the channel when not following and the replay is exhausted.
- `Logs` opens the file, seeks to `from`, and returns it. The caller closes.

- [ ] **Step 4: Run to verify it passes** with `-race`.

- [ ] **Step 5: Prove the resume pairing.** Make `State` return a `Seq` one lower than it folded, confirm `TestSubscribeResumesFromSnapshotSeq` fails with a duplicate. Restore. Record it.

- [ ] **Step 6: Commit**

```bash
git add internal/source && git commit -m "feat(source): the Source seam and FileSource"
```

---

### Task 2: The plain renderer

**Files:** Create `internal/render/plain.go`, `internal/render/plain_test.go`

**Interfaces:**
- Consumes: Task 1's `source.Source`, `api`.
- Produces: `render.Plain(ctx context.Context, src source.Source, w io.Writer) (api.RunStatus, error)`.

- [ ] **Step 1: Write the failing test**

```go
// The plain renderer is a Source client, never an engine code path. If it
// were a second path, a TTY run and a CI log would drift in what they report
// — and the CI log is the one nobody reads until something breaks.
func TestPlainRendersFromAFileSource(t *testing.T) {
	dir := writeRun(t, twoStepRun())
	fs, _ := source.OpenFile(dir, false)
	defer fs.Close()

	var buf bytes.Buffer
	status, err := render.Plain(context.Background(), fs, &buf)
	if err != nil {
		t.Fatalf("Plain: %v", err)
	}
	if status != api.RunSucceeded {
		t.Errorf("status = %s, want succeeded", status)
	}

	out := buf.String()
	for _, want := range []string{"setup", "build", "succeeded"} {
		if !strings.Contains(out, want) {
			t.Errorf("output missing %q:\n%s", want, out)
		}
	}
}

func TestPlainReportsAFailedRunAndNamesTheCause(t *testing.T) {
	dir := writeRun(t, failedRun()) // "deploy" exits 9
	fs, _ := source.OpenFile(dir, false)
	defer fs.Close()

	var buf bytes.Buffer
	status, _ := render.Plain(context.Background(), fs, &buf)
	if status != api.RunFailed {
		t.Errorf("status = %s, want failed", status)
	}
	out := buf.String()
	if !strings.Contains(out, "deploy") {
		t.Errorf("a failed run must name the step that failed:\n%s", out)
	}
}

// A recovered run is not a clean run, and the renderer is where an operator
// finds that out.
func TestPlainDistinguishesRecovery(t *testing.T) {
	dir := writeRun(t, recoveredRun())
	fs, _ := source.OpenFile(dir, false)
	defer fs.Close()

	var buf bytes.Buffer
	status, _ := render.Plain(context.Background(), fs, &buf)
	if status != api.RunSucceededWithRecovery {
		t.Errorf("status = %s, want succeeded_with_recovery", status)
	}
	if !strings.Contains(buf.String(), "recovered") {
		t.Errorf("a recovered run must say so:\n%s", buf.String())
	}
}

func TestPlainWritesNoAnsiEscapes(t *testing.T) {
	// This renderer exists for non-TTY output. An escape sequence in a CI log
	// is the most common way this feature ships broken.
	dir := writeRun(t, twoStepRun())
	fs, _ := source.OpenFile(dir, false)
	defer fs.Close()

	var buf bytes.Buffer
	_, _ = render.Plain(context.Background(), fs, &buf)
	if bytes.ContainsRune(buf.Bytes(), 0x1b) {
		t.Errorf("plain output contains an ANSI escape:\n%q", buf.String())
	}
}
```

- [ ] **Step 2–4:** Run, observe, implement, observe. `Plain` takes a snapshot, prints what already happened, subscribes from `Seq+1`, prints each lifecycle event as a line, and returns the final status from the folded state. It must fold every event through `api.RunState.Apply` rather than tracking its own state — one fold, four consumers.

- [ ] **Step 5: Commit**

```bash
git add internal/render && git commit -m "feat(render): the plain renderer, as a Source client"
```

---

### Task 3: The hub

**Files:** Create `internal/attachsrv/hub.go`, `internal/attachsrv/hub_test.go`

**Interfaces:**
- Consumes: `api`, `internal/sink`.
- Produces: `attachsrv.Hub`; `NewHub(ringSize int) *Hub`; `(*Hub).Emit(api.Event)` satisfying `sink.Sink`; `(*Hub).Control() <-chan sink.ControlRequest`; `(*Hub).Subscribe(fromSeq uint64) (<-chan api.Event, func(), error)`; `(*Hub).SubscribeLogs(step string) (<-chan api.LogGap, func())`; `(*Hub).State() *api.RunState`; `ErrLifecycleOverflow`.

- [ ] **Step 1: Write the failing test**

```go
// The hub is the engine's only observer. Emit must never block, whatever a
// client is doing — the engine's correctness cannot depend on who is watching.
func TestEmitNeverBlocksOnASlowSubscriber(t *testing.T) {
	h := attachsrv.NewHub(8)
	ch, cancel, _ := h.Subscribe(0)
	defer cancel()
	_ = ch // deliberately never read

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 1; i <= 1000; i++ {
			h.Emit(api.Event{V: 1, Seq: uint64(i), Type: api.StepCreated, Step: "a"})
		}
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Emit blocked on a subscriber that never reads")
	}
}

// Lifecycle events are never dropped. A client that cannot keep up is
// disconnected so it reconnects with a fresh snapshot — losing a
// step.finished silently is worse than a reconnect.
func TestSlowSubscriberIsDisconnectedNotSilentlyTruncated(t *testing.T) {
	h := attachsrv.NewHub(8)
	ch, cancel, _ := h.Subscribe(0)
	defer cancel()

	for i := 1; i <= 1000; i++ {
		h.Emit(api.Event{V: 1, Seq: uint64(i), Type: api.StepCreated, Step: "a"})
	}

	var last uint64
	var closed bool
	for e := range ch {
		if last != 0 && e.Seq != last+1 {
			t.Fatalf("a gap appeared in the lifecycle stream: %d then %d", last, e.Seq)
		}
		last = e.Seq
	}
	closed = true
	if !closed {
		t.Fatal("an overflowing subscriber must be closed")
	}
}

func TestSubscribersSeeEventsInOrder(t *testing.T) {
	h := attachsrv.NewHub(4096)
	ch, cancel, _ := h.Subscribe(0)
	defer cancel()

	const n = 500
	go func() {
		for i := 1; i <= n; i++ {
			h.Emit(api.Event{V: 1, Seq: uint64(i), Type: api.StepCreated, Step: "a"})
		}
	}()

	s := api.NewRunState()
	for i := 0; i < n; i++ {
		e := <-ch
		if err := s.Apply(e); err != nil {
			t.Fatalf("event %d: %v — the hub must preserve order", i, err)
		}
	}
}

func TestStateIsTheFoldOfEverythingEmitted(t *testing.T) {
	h := attachsrv.NewHub(4096)
	for _, e := range twoStepEvents() {
		h.Emit(e)
	}
	st := h.State()
	if !st.Run.Done || len(st.Steps) != 2 {
		t.Errorf("hub state = %+v", st.Run)
	}
}

// Attaching to a run already in flight must not replay from the beginning.
func TestSubscribeFromSeqSkipsWhatTheSnapshotCovered(t *testing.T) {
	h := attachsrv.NewHub(4096)
	for i := 1; i <= 10; i++ {
		h.Emit(api.Event{V: 1, Seq: uint64(i), Type: api.StepCreated, Step: "a"})
	}
	st := h.State()

	ch, cancel, _ := h.Subscribe(st.Seq + 1)
	defer cancel()
	h.Emit(api.Event{V: 1, Seq: 11, Type: api.StepCreated, Step: "b"})

	e := <-ch
	if e.Seq != 11 {
		t.Errorf("first delivered seq = %d, want 11", e.Seq)
	}
}
```

- [ ] **Step 2–4:** Run, observe, implement, observe under `-race`.

Design notes. The hub holds the materialized `RunState` (folding every event as it arrives), a ring of recent events for resume, and a set of subscribers each with a bounded channel. `Emit` folds, appends to the ring, then does a non-blocking send to each subscriber; a subscriber whose channel is full is **closed and removed**, not skipped. `Subscribe(fromSeq)` replays from the ring if the request is within it, and reports `ErrLifecycleOverflow` if it is older — the caller then serves a fresh snapshot instead.

- [ ] **Step 5: Prove the no-drop guarantee.** Change the full-channel case to skip the event instead of closing the subscriber, confirm `TestSlowSubscriberIsDisconnectedNotSilentlyTruncated` fails with a gap. Restore. Record it.

- [ ] **Step 6: Commit**

```bash
git add internal/attachsrv && git commit -m "feat(attachsrv): the hub, lossless on lifecycle"
```

---

### Task 4: The server

**Files:** Create `internal/attachsrv/server.go`, `internal/attachsrv/server_test.go`

**Interfaces:**
- Consumes: Task 3's `Hub`, `internal/eventlog`, `internal/plan`.
- Produces: `attachsrv.Server`; `attachsrv.Listen(ctx, Options) (*Server, error)` with `Options{Bind string; Dir string; Hub *Hub; ReadOnly bool}`; `(*Server).Addr() string`; `(*Server).Close() error`.

Endpoints: `GET /api/state`, `GET /api/plan`, `GET /api/logs/{step}?attempt=&stream=&from=`, `WS /api/stream`, `WS /api/logs/{step}`.

- [ ] **Step 1: Write the failing test** — tests over a real unix socket: `/api/state` returns a foldable snapshot carrying its `seq`; `/api/plan` returns the plan; `/api/logs` honours `from`; the stream endpoint delivers events from a requested seq in order; a `ReadOnly` server rejects control frames; and a client that subscribes, disconnects, and resubscribes from `last+1` sees no gap and no repeat.

- [ ] **Step 2–4:** Run, observe, implement, observe.

Design notes. One `http.Server` over a `net.Listener` that may be unix or TCP — same mux for JSON, WebSocket and (later) the embedded browser UI. Default to a unix socket at `<runtime-dir>/senro/<pid>.sock`, mode `0600`. **Do not implement TCP binding in this task**; the token and origin machinery it needs belongs with the discovery work in Task 6, and a TCP listener without them is a remote code execution endpoint.

- [ ] **Step 5: Commit**

```bash
git add internal/attachsrv && git commit -m "feat(attachsrv): the attach server over a unix socket"
```

---

### Task 5: LiveSource and FallbackSource

**Files:** Create `internal/source/live.go`, `internal/source/fallback.go`, and tests

**Interfaces:**
- Produces: `source.Dial(ctx, addr string) (*LiveSource, error)`; `source.Fallback(live Source, dir string) Source`.

- [ ] **Step 1: Write the failing test** — the key ones: a `LiveSource` against a real server satisfies the same assertions as `FileSource` did in Task 1 (write them as one table exercised against both, so a divergence in the seam is a test failure); `Control` succeeds against a live server and returns `ErrReadOnly` against a file; and on `bye{reason:"exit"}` a `FallbackSource` transparently continues serving state and logs from disk.

**That shared table is the most valuable test in this plan.** The claim being made is that the offline TUI is the same client with a different `Source`. A table that runs identical assertions against both implementations is the only thing that makes that claim testable rather than aspirational.

- [ ] **Step 2–4:** Run, observe, implement, observe.

- [ ] **Step 5: Commit**

```bash
git add internal/source && git commit -m "feat(source): LiveSource, and falling back to disk when the engine exits"
```

---

### Task 6: Discovery and peer credentials

**Files:** Create `internal/attachsrv/registry.go`, `internal/attachsrv/peercred_linux.go`, `internal/attachsrv/peercred_darwin.go`, and tests

**Interfaces:**
- Produces: `attachsrv.Register(entry Entry) (func(), error)`; `attachsrv.Discover() ([]Entry, error)` with `Entry{PID int; Socket, RunID, Pipeline, CWD string; StartedAt time.Time; EngineVersion string}`; `attachsrv.CheckPeer(conn net.Conn) error`.

- [ ] **Step 1: Write the failing test** — a registered entry is discoverable; an entry whose pid is dead is reaped by `Discover`; the registry file is `0600`; `CheckPeer` accepts a connection from the same uid. The dead-pid reaping is the one that matters: `senro attach` with no arguments has to pick the one live run, and a stale entry makes it pick a corpse.

- [ ] **Step 2–4:** Run, observe, implement, observe. `SO_PEERCRED` on Linux, `LOCAL_PEERCRED` via `unix.Xucred` on darwin, build-tagged. Mode `0600` on the socket is not sufficient on every platform — the uid check is the actual guard.

- [ ] **Step 5: Commit**

```bash
git add internal/attachsrv && git commit -m "feat(attachsrv): run discovery and peer-credential checks"
```

---

### Task 7: Control operations

**Files:** Modify `internal/attachsrv/server.go`, `internal/engine/engine.go`; add tests

- [ ] **Step 1: Write the failing test** — `run.cancel` over an attached client stops a running pipeline and the run reports `cancelled`; `step.retry` on a failed step runs it again and the ledger shows the new attempt; every accepted operation emits `control.applied` carrying the originating client id, so the audit trail is complete and other attached clients see who did what; an unknown op returns `ok:false` with a reason rather than closing the connection; and a `ReadOnly` server refuses both.

- [ ] **Step 2–4:** Run, observe, implement, observe.

The engine already exposes `Control() <-chan sink.ControlRequest` and `ControlRequest` already carries a `Reply` channel — wire the hub's control channel to it and have the scheduler serve requests between steps. Keep the surface to `run.cancel` and `step.retry`; breakpoints, `rerun_from`, `step.skip` and PTY are v1.

- [ ] **Step 5: Prove the attribution.** Drop the `ClientID` from the emitted `control.applied`, confirm a test fails. Restore. Record it.

- [ ] **Step 6: Commit**

```bash
git add internal/attachsrv internal/engine && git commit -m "feat(attach): run.cancel and step.retry, attributed"
```

---

### Task 8: The TUI

**Files:** Create `internal/tui/*.go` and tests

- [ ] **Step 1: Write the failing test** — model tests, not terminal tests: the model folds events into a `RunState` and its view lists every step; an expansion group renders collapsed with counts; the focused step's log pane shows that step's content; `r` sends a retry for the focused step; `c` sends a cancel; `q` detaches without cancelling. Test the model's `Update`/`View` directly; do not drive a pty.

- [ ] **Step 2–4:** Run, observe, implement, observe.

Design notes. bubbletea and lipgloss. Render on a fixed ~30 Hz tick with events coalesced between ticks — rendering per event melts the terminal at 200k lines/sec and makes the TUI the build's bottleneck. Virtualize the log view over an in-memory ring, with scrollback beyond it served by range request. **`q` detaches and the run continues; `Ctrl-C` cancels.** Quitting a UI must never be a way to silently kill a deploy.

- [ ] **Step 5: Commit**

```bash
git add internal/tui && git commit -m "feat(tui): the terminal client"
```

---

### Task 9: Embedding and the CLI

**Files:** Create `attach/attach.go`, `cmd/senro/main.go`, and tests

**Interfaces:**
- Produces: `attach.Listen(ctx, attach.Options) (*attach.Attach, error)` with `Options{Bind string; WaitForClient bool; ReadOnly bool}`; `(*Attach).Sink() sink.Sink`; `(*Attach).Close() error`. CLI: `senro run <pkg>`, `senro attach [--pid|--run|--follow]`, `senro ui`.

- [ ] **Step 1: Write the failing test** — `--ui=auto` picks plain on a non-TTY; `--ui=tui` on a non-TTY is an **error**, not a silent downgrade; exit codes are `0` success, `1` run failed, `2` usage, `130` cancelled, with `78` reserved; `attach.Listen` with `WaitForClient` blocks until a client connects; and an embedded pipeline with no attach server starts no goroutines and pays nothing.

- [ ] **Step 1b: Version negotiation (§6.6)** — added after the fact; the original plan
      omitted it. `api.Version` is already emitted on every frame and documented as "a client
      and engine must agree on the major version; a minor mismatch warns rather than failing,"
      but **nothing validates it anywhere in the codebase**. A version field that nothing checks
      is worse than no field at all: §6.6 states that a stale CLI against a new engine "should
      say `upgrade your CLI` and not fail with a JSON decode error," which is exactly what
      happens today.

      Write the failing test first: a client whose major version differs from the engine's is
      refused with a clear, machine-readable message naming the mismatch — not a decode error,
      not a generic 400; a minor mismatch warns and proceeds; an equal version is silent. Then
      implement the check on the client's first contact with the engine, and have the CLI
      surface the refusal as an actionable message rather than a stack trace.

      Both directions matter and neither is optional: a new CLI against an old engine, and an
      old CLI against a new engine. Test both. The failure this prevents is a user seeing
      `invalid character 'x' looking for beginning of value` when the real answer is "your CLI
      is a major version behind."

- [ ] **Step 2–4:** Run, observe, implement, observe.

- [ ] **Step 5: Commit**

```bash
git add attach cmd && git commit -m "feat(cli): senro run, attach and ui"
```

---

### Task 10: End-to-end and the shared-source golden

**Files:** Create `internal/source/conformance_test.go`, extend `internal/engine/golden_test.go`

- [ ] **Step 1: Write the failing test** — run a real pipeline with an attach server, attach a `LiveSource`, and assert its folded state matches what a `FileSource` over the same run directory folds to, event for event. Then kill the engine and assert a `FallbackSource` keeps serving the same state from disk.

**This is the plan's deliverable.** If a live client and an offline client can disagree about a run, the `Source` seam has not done its job, and everything built on it inherits the divergence.

- [ ] **Step 2–4:** Run, observe, implement, observe.

- [ ] **Step 5: Commit**

```bash
git add internal/source internal/engine && git commit -m "test(attach): live and file sources agree on the same run"
```

---

## Self-Review

**Spec coverage.** §4.6's server surface, hub overflow behaviour, percent-encoded log paths, `Source`/`FallbackSource` and peer credentials are Tasks 1, 3, 4, 5, 6. §4.7's plain renderer as a `Source` client, the TUI, `--ui` selection and exit codes are Tasks 2, 8, 9. Control ops limited to `run.cancel` and `step.retry` is Task 7.

**Deliberately out of scope.** TCP binding with bearer tokens and origin checks — v1, and dangerous to half-build. Breakpoints, `rerun_from`, `step.skip`, `ws.snapshot`, PTY sessions — v1. The browser UI and WASM — v1. `senro run`'s `go build` path is in Task 9, but `senro ui` is a stub that reports it needs the browser UI.

**Placeholder scan.** No TBDs. Tasks 4–9 specify behaviour in prose with tests enumerated rather than transcribed — the pattern used for the scheduler and the shutdown sequence, where a hand-written implementation beats a copied one and every named behaviour has an assertion.

**Type consistency.** `source.Source` (Task 1) is implemented by `FileSource` (1), `LiveSource` (5) and `FallbackSource` (5), consumed by `render.Plain` (2), the TUI (8) and the CLI (9). `attachsrv.Hub` (3) satisfies `sink.Sink`, so `engine.Options.Sink` takes it with no engine change. `ControlRequest` (existing) is produced by the server (7) and consumed by the scheduler.

---

## Next

Plan 5 is storage: the CAS, workspaces with normalized snapshots, and both caches. Do not begin it until `make all` passes here and Task 10's cross-source agreement test is green.
