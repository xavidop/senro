# senro v0 Engine Spine Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A Go-defined pipeline of `Exec` steps runs on the local machine and produces an `events.jsonl` that matches a golden fixture, with per-step logs written to seekable files whose byte offsets appear in the event stream.

**Architecture:** Definition → plan → execution. User Go code builds an immutable `*Line`; `Run` resolves it into a `Plan` (validated, serialized as `plan.json`); a scheduler walks the plan against a global concurrency semaphore, executing each step in a `Sandbox`. Every observable fact is an event, assigned a sequence number and appended **synchronously** to the ledger before any observer sees it.

**Tech Stack:** Go 1.26, standard library only. Consumes `github.com/xavidop/senro/api` (plan 1, on `main`).

**Spec:** `docs/superpowers/specs/2026-08-07-senro-v0-design.md`, phase 2 of its table. §-references point at `docs/design.md`.

## Global Constraints

- Module path `github.com/xavidop/senro`. Go directive `go 1.26`.
- **`api/go.mod` must stay dependency-free.** The root module may take dependencies; `api` may not. Two tests enforce this — do not "fix" a build error by adding a require to `api`.
- **The event log is a ledger, not a sink.** Events are assigned a seq and appended to `events.jsonl` synchronously; a write failure fails the run. Only after the append are they handed to `Sink`s. `Sink.Emit` is non-blocking and never returns an error.
- **`Sandbox.Run` returns `exit` and `error` separately and they mean different things.** `error` is infrastructure failure; `exit` is the workload's verdict. Never collapse them — retry predicates key off exactly this distinction.
- **Mounts and secrets are declared in `SandboxSpec`, not pushed imperatively.** Later executors restore them out of band.
- Platform is two values: `DeclaredPlatform` (plan-time, enters cache keys later) and `ObservedPlatform` (post-creation, verified).
- Terminology: railway metaphor in prose and error messages only; identifiers use `Step`, `Line`, `Plan`, `Run`.
- Code must be gofmt-clean or `make all` fails. Working tree must be clean at every commit — no `.backup`/`.orig` scratch files.
- Test-first throughout. Where a test asserts an invariant, **watch it fail** before implementing.

---

## File Structure

```
senro.go                         package senro — Line, Step, Run, options
exec/exec.go                     exec.Command — the Exec step constructor
local/local.go                   local.Host() — the local executor handle

internal/stepid/stepid.go        ID grammar: parse, format, sorted keys, attempt addressing
internal/eventlog/ledger.go      seq assignment, events.jsonl append, fsync
internal/eventlog/logfile.go     seekable per-step logs, percent-encoded paths, offset tracking
internal/sink/sink.go            Sink interface, MultiSink, nop
internal/executor/executor.go    Executor, Sandbox, SandboxSpec, Cmd, Mount, Platform
internal/executor/localexec/     the local Sandbox implementation
internal/plan/plan.go            Plan, Node, plan.json marshalling
internal/plan/validate.go        cycles, dangling Needs, duplicate IDs
internal/engine/engine.go        the scheduler
internal/engine/golden_test.go   end-to-end: run a pipeline, diff the event log
```

Split by responsibility rather than layer. `stepid` is its own package because three others parse IDs and none should own the grammar. `eventlog` holds the ledger and the log files together because they are written in lockstep — a log append and its `step.log.appended` marker must not drift.

---

### Task 1: Step ID grammar

**Files:** Create `internal/stepid/stepid.go`, `internal/stepid/stepid_test.go`

**Interfaces:**
- Consumes: nothing.
- Produces: `stepid.Format(base string, keys map[string]string) string`; `stepid.ParseAddress(s string) (id string, attempt int, err error)`; `stepid.Encode(id string) string` and `stepid.Decode(s string) (string, error)` for filesystem-safe paths.

- [ ] **Step 1: Write the failing test**

```go
package stepid_test

import (
	"testing"

	"github.com/xavidop/senro/internal/stepid"
)

func TestFormatSortsKeys(t *testing.T) {
	// Sorted keys are what make expansion child IDs deterministic. An expander
	// returning map iteration order must still produce a stable ID.
	got := stepid.Format("build/test", map[string]string{"os": "linux", "unit": "api"})
	want := "build/test[os=linux,unit=api]"
	if got != want {
		t.Errorf("Format = %q, want %q", got, want)
	}
}

func TestFormatNoKeys(t *testing.T) {
	if got := stepid.Format("build/test", nil); got != "build/test" {
		t.Errorf("Format = %q, want bare ID", got)
	}
}

func TestParseAddress(t *testing.T) {
	cases := []struct {
		in      string
		id      string
		attempt int
	}{
		{"build/test", "build/test", 0},
		{"build/test@2", "build/test", 2},
		{"build/test[unit=api]", "build/test[unit=api]", 0},
		{"build/test[unit=api]@3", "build/test[unit=api]", 3},
	}
	for _, tc := range cases {
		id, attempt, err := stepid.ParseAddress(tc.in)
		if err != nil {
			t.Fatalf("ParseAddress(%q): %v", tc.in, err)
		}
		if id != tc.id || attempt != tc.attempt {
			t.Errorf("ParseAddress(%q) = (%q, %d), want (%q, %d)", tc.in, id, attempt, tc.id, tc.attempt)
		}
	}
}

func TestParseAddressRejectsBadAttempt(t *testing.T) {
	// An attempt of 0 or negative is not addressable; attempts are 1-based.
	for _, in := range []string{"build/test@0", "build/test@-1", "build/test@x", "build/test@"} {
		if _, _, err := stepid.ParseAddress(in); err == nil {
			t.Errorf("ParseAddress(%q) should error", in)
		}
	}
}

func TestEncodeRoundTrip(t *testing.T) {
	// Step IDs contain / and [] and cannot be path segments directly. Encoding
	// must be reversible and must keep the result readable when debugging a run
	// from disk.
	for _, id := range []string{
		"build/test",
		"build/test[unit=services/api]",
		"deploy/discover/apply-west[a=1,b=2]",
	} {
		enc := stepid.Encode(id)
		if enc == "" {
			t.Fatalf("Encode(%q) empty", id)
		}
		for _, bad := range []rune{'/', '[', ']'} {
			for _, r := range enc {
				if r == bad {
					t.Errorf("Encode(%q) = %q still contains %q", id, enc, string(bad))
				}
			}
		}
		back, err := stepid.Decode(enc)
		if err != nil {
			t.Fatalf("Decode(%q): %v", enc, err)
		}
		if back != id {
			t.Errorf("round-trip: %q -> %q -> %q", id, enc, back)
		}
	}
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `go test ./internal/stepid/... -v`
Expected: FAIL — `no required module provides package .../internal/stepid`.

- [ ] **Step 3: Implement**

```go
// Package stepid owns senro's step identifier grammar.
//
//	stepID   := segment ("/" segment)*         "deploy/discover/apply-west"
//	expanded := stepID "[" k=v ("," k=v)* "]"  keys sorted
//	address  := (stepID|expanded) ["@" N]      CLI surface only, N >= 1
//
// The attempt suffix is an addressing form for the CLI and never appears in an
// event's step field, where attempt is its own routing field.
package stepid

import (
	"fmt"
	"net/url"
	"sort"
	"strconv"
	"strings"
)

// Format builds an expanded child ID. Keys are sorted so that an expander
// returning map iteration order still produces a stable, reproducible ID.
func Format(base string, keys map[string]string) string {
	if len(keys) == 0 {
		return base
	}
	pairs := make([]string, 0, len(keys))
	for k, v := range keys {
		pairs = append(pairs, k+"="+v)
	}
	sort.Strings(pairs)
	return base + "[" + strings.Join(pairs, ",") + "]"
}

// ParseAddress splits a CLI address into its step ID and attempt number.
// An absent @N yields attempt 0, meaning "unspecified".
func ParseAddress(s string) (string, int, error) {
	// Search after the bracketed key set, so a value containing @ is safe.
	search := s
	if i := strings.LastIndex(s, "]"); i >= 0 {
		search = s[i:]
	}
	at := strings.LastIndex(search, "@")
	if at < 0 {
		return s, 0, nil
	}
	cut := len(s) - len(search) + at
	id, rest := s[:cut], s[cut+1:]
	n, err := strconv.Atoi(rest)
	if err != nil {
		return "", 0, fmt.Errorf("stepid: bad attempt in %q: %w", s, err)
	}
	if n < 1 {
		return "", 0, fmt.Errorf("stepid: attempt must be >= 1 in %q", s)
	}
	return id, n, nil
}

// Encode makes a step ID safe as a single filesystem path segment while
// keeping it readable, which matters when reading a run's logs from disk.
func Encode(id string) string { return url.PathEscape(id) }

// Decode reverses Encode.
func Decode(s string) (string, error) { return url.PathUnescape(s) }
```

- [ ] **Step 4: Run to verify it passes**

Run: `go test ./internal/stepid/... -v`
Expected: PASS. If `url.PathEscape` leaves `/` unescaped, replace `Encode` with a `strings.NewReplacer`-based escape over `%`, `/`, `[`, `]` and adjust `Decode` to match — the test is the contract, not the implementation.

- [ ] **Step 5: Commit**

```bash
git add internal/stepid && git commit -m "feat(stepid): step identifier grammar"
```

---

### Task 2: The ledger

**Files:** Create `internal/eventlog/ledger.go`, `internal/eventlog/ledger_test.go`

**Interfaces:**
- Consumes: `api.Event`.
- Produces: `eventlog.Ledger`; `eventlog.Open(dir string) (*Ledger, error)`; `(*Ledger).Append(e api.Event) (api.Event, error)` returning the event with `Seq`/`TS`/`V` stamped; `(*Ledger).Seq() uint64`; `(*Ledger).Close() error`; `eventlog.Read(path string) ([]api.Event, error)`.

- [ ] **Step 1: Write the failing test**

```go
package eventlog_test

import (
	"path/filepath"
	"sync"
	"testing"

	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/internal/eventlog"
)

func TestAppendStampsSeqAndVersion(t *testing.T) {
	dir := t.TempDir()
	l, err := eventlog.Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer l.Close()

	first, err := l.Append(api.Event{Type: api.RunStarted, Run: "01JQ"})
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if first.Seq != 1 {
		t.Errorf("first Seq = %d, want 1", first.Seq)
	}
	if first.V != api.Version {
		t.Errorf("V = %d, want %d", first.V, api.Version)
	}
	if first.TS.IsZero() {
		t.Error("TS must be stamped")
	}

	second, _ := l.Append(api.Event{Type: api.StepCreated, Step: "a"})
	if second.Seq != 2 {
		t.Errorf("second Seq = %d, want 2", second.Seq)
	}
}

func TestAppendIsDurableAndReadable(t *testing.T) {
	dir := t.TempDir()
	l, _ := eventlog.Open(dir)
	for i := 0; i < 3; i++ {
		if _, err := l.Append(api.Event{Type: api.StepCreated, Step: "a"}); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	if err := l.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	got, err := eventlog.Read(filepath.Join(dir, "events.jsonl"))
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("read %d events, want 3", len(got))
	}
	for i, e := range got {
		if e.Seq != uint64(i+1) {
			t.Errorf("event %d has Seq %d", i, e.Seq)
		}
	}
}

// The ledger is written from the scheduler's goroutines. A torn line or a
// duplicated seq under concurrency is a corrupted source of truth.
func TestAppendIsConcurrencySafe(t *testing.T) {
	dir := t.TempDir()
	l, _ := eventlog.Open(dir)

	const n = 200
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = l.Append(api.Event{Type: api.StepCreated, Step: "a"})
		}()
	}
	wg.Wait()
	_ = l.Close()

	got, err := eventlog.Read(filepath.Join(dir, "events.jsonl"))
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(got) != n {
		t.Fatalf("read %d events, want %d", len(got), n)
	}
	seen := make(map[uint64]bool, n)
	for _, e := range got {
		if seen[e.Seq] {
			t.Fatalf("duplicate seq %d", e.Seq)
		}
		seen[e.Seq] = true
	}
	for i := uint64(1); i <= n; i++ {
		if !seen[i] {
			t.Errorf("missing seq %d", i)
		}
	}
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `go test ./internal/eventlog/... -v`
Expected: FAIL — package does not exist.

- [ ] **Step 3: Implement**

```go
// Package eventlog writes a run's append-only ledger.
//
// The ledger is not a Sink. An event is assigned its sequence number and
// appended here synchronously before any observer sees it, and a write failure
// fails the run. Sinks are observers and may drop; the ledger may not.
package eventlog

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/xavidop/senro/api"
)

// Ledger is the append-only event log for one run. Safe for concurrent use.
type Ledger struct {
	mu  sync.Mutex
	f   *os.File
	w   *bufio.Writer
	seq uint64
	now func() time.Time
}

// Open creates or truncates dir/events.jsonl.
func Open(dir string) (*Ledger, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("eventlog: %w", err)
	}
	f, err := os.OpenFile(filepath.Join(dir, "events.jsonl"),
		os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, fmt.Errorf("eventlog: %w", err)
	}
	return &Ledger{f: f, w: bufio.NewWriter(f), now: time.Now}, nil
}

// Append stamps the event with the next sequence number, the envelope version
// and a timestamp, writes it, and returns the stamped event for handing to
// sinks. The returned error is fatal to the run.
func (l *Ledger) Append(e api.Event) (api.Event, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.seq++
	e.Seq = l.seq
	e.V = api.Version
	if e.TS.IsZero() {
		e.TS = l.now().UTC()
	}

	b, err := json.Marshal(e)
	if err != nil {
		return api.Event{}, fmt.Errorf("eventlog: marshal seq %d: %w", e.Seq, err)
	}
	if _, err := l.w.Write(append(b, '\n')); err != nil {
		return api.Event{}, fmt.Errorf("eventlog: write seq %d: %w", e.Seq, err)
	}
	return e, nil
}

// Seq reports the highest sequence number written.
func (l *Ledger) Seq() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.seq
}

// Close flushes and fsyncs. The ledger is the run's source of truth, so the
// data must be on disk before the process exits.
func (l *Ledger) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.w.Flush(); err != nil {
		l.f.Close()
		return fmt.Errorf("eventlog: flush: %w", err)
	}
	if err := l.f.Sync(); err != nil {
		l.f.Close()
		return fmt.Errorf("eventlog: sync: %w", err)
	}
	return l.f.Close()
}

// Read loads a whole event log. Used by offline replay and by golden tests.
func Read(path string) ([]api.Event, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var out []api.Event
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 8*1024*1024)
	for line := 1; sc.Scan(); line++ {
		if len(sc.Bytes()) == 0 {
			continue
		}
		var e api.Event
		if err := json.Unmarshal(sc.Bytes(), &e); err != nil {
			return nil, fmt.Errorf("%s:%d: %w", path, line, err)
		}
		out = append(out, e)
	}
	return out, sc.Err()
}
```

- [ ] **Step 4: Run to verify it passes**

Run: `go test ./internal/eventlog/... -race -v`
Expected: PASS, including under `-race`.

- [ ] **Step 5: Commit**

```bash
git add internal/eventlog && git commit -m "feat(eventlog): the run ledger"
```

---

### Task 3: Seekable per-step log files

**Files:** Create `internal/eventlog/logfile.go`, `internal/eventlog/logfile_test.go`

**Interfaces:**
- Consumes: Task 1's `stepid.Encode`, Task 2's `Ledger`.
- Produces: `eventlog.LogSet`; `eventlog.NewLogSet(dir string) *LogSet`; `(*LogSet).Writer(step string, attempt int, stream string) (io.WriteCloser, error)` where each `Write` appends and emits nothing on its own; `(*LogSet).Path(step string, attempt int, stream string) string`; `(*LogSet).Close() error`. Each writer exposes `Offset() int64`.

- [ ] **Step 1: Write the failing test**

```go
package eventlog_test

import (
	"os"
	"strings"
	"testing"

	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/internal/eventlog"
)

func TestLogWriterTracksOffsets(t *testing.T) {
	ls := eventlog.NewLogSet(t.TempDir())
	defer ls.Close()

	w, err := ls.Writer("build/test[unit=api]", 1, api.StreamStdout)
	if err != nil {
		t.Fatalf("Writer: %v", err)
	}
	if got := w.Offset(); got != 0 {
		t.Errorf("initial offset = %d, want 0", got)
	}
	if _, err := w.Write([]byte("hello\n")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if got := w.Offset(); got != 6 {
		t.Errorf("offset after 6 bytes = %d, want 6", got)
	}
	if _, err := w.Write([]byte("world\n")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if got := w.Offset(); got != 12 {
		t.Errorf("offset = %d, want 12", got)
	}
}

// Step IDs contain / and [] and cannot be path segments. The path must stay
// readable — debugging a run from disk means finding these by eye.
func TestLogPathIsSafeAndReadable(t *testing.T) {
	ls := eventlog.NewLogSet("/runs/01JQ")
	p := ls.Path("build/test[unit=services/api]", 2, api.StreamStdout)

	if strings.Count(p, "/") != strings.Count("/runs/01JQ/logs/X/2/stdout", "/") {
		t.Errorf("path %q has unexpected depth — the step ID must be one segment", p)
	}
	if !strings.HasSuffix(p, "/2/stdout") {
		t.Errorf("path %q must end in <attempt>/<stream>", p)
	}
	if !strings.Contains(p, "build") {
		t.Errorf("path %q should stay recognisable", p)
	}
}

func TestSeparateStreamsAndAttempts(t *testing.T) {
	dir := t.TempDir()
	ls := eventlog.NewLogSet(dir)
	defer ls.Close()

	out, _ := ls.Writer("a", 1, api.StreamStdout)
	errw, _ := ls.Writer("a", 1, api.StreamStderr)
	second, _ := ls.Writer("a", 2, api.StreamStdout)

	_, _ = out.Write([]byte("out"))
	_, _ = errw.Write([]byte("err!"))
	_, _ = second.Write([]byte("retry"))

	if err := ls.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Each attempt gets its own files, per the failure taxonomy: never reuse
	// an attempt's log, or a retry inherits the output that explained the
	// original failure.
	for _, tc := range []struct{ step, want string; attempt int; stream string }{
		{"a", "out", 1, api.StreamStdout},
		{"a", "err!", 1, api.StreamStderr},
		{"a", "retry", 2, api.StreamStdout},
	} {
		b, err := os.ReadFile(ls.Path(tc.step, tc.attempt, tc.stream))
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		if string(b) != tc.want {
			t.Errorf("attempt %d %s = %q, want %q", tc.attempt, tc.stream, b, tc.want)
		}
	}
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `go test ./internal/eventlog/... -run TestLog -v`
Expected: FAIL — `undefined: eventlog.NewLogSet`.

- [ ] **Step 3: Implement**

```go
package eventlog

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"sync"

	"github.com/xavidop/senro/internal/stepid"
)

// LogSet owns a run's per-step log files.
//
// Logs are files rather than event payloads so that a client can range-request
// scrollback instead of the server retaining a replay buffer, and so a 300-node
// fan-out does not push log bodies down the lifecycle channel.
type LogSet struct {
	dir string

	mu      sync.Mutex
	writers map[string]*LogWriter
}

func NewLogSet(dir string) *LogSet {
	return &LogSet{dir: dir, writers: make(map[string]*LogWriter)}
}

// Path is the on-disk location of one stream of one attempt of one step.
// The step ID is percent-encoded into a single segment: it contains / and [],
// and it must stay readable for anyone debugging a run from disk.
func (ls *LogSet) Path(step string, attempt int, stream string) string {
	return filepath.Join(ls.dir, "logs", stepid.Encode(step), strconv.Itoa(attempt), stream)
}

// Writer returns the append-only writer for one stream, creating it on first
// use. Repeated calls with the same key return the same writer.
func (ls *LogSet) Writer(step string, attempt int, stream string) (*LogWriter, error) {
	key := step + "\x00" + strconv.Itoa(attempt) + "\x00" + stream

	ls.mu.Lock()
	defer ls.mu.Unlock()
	if w, ok := ls.writers[key]; ok {
		return w, nil
	}

	p := ls.Path(step, attempt, stream)
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		return nil, fmt.Errorf("eventlog: %w", err)
	}
	f, err := os.OpenFile(p, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, fmt.Errorf("eventlog: %w", err)
	}
	w := &LogWriter{f: f}
	ls.writers[key] = w
	return w, nil
}

func (ls *LogSet) Close() error {
	ls.mu.Lock()
	defer ls.mu.Unlock()
	var first error
	for _, w := range ls.writers {
		if err := w.Close(); err != nil && first == nil {
			first = err
		}
	}
	return first
}

// LogWriter appends to one log file and tracks the byte offset, which is what
// a step.log.appended marker carries.
type LogWriter struct {
	mu     sync.Mutex
	f      *os.File
	offset int64
}

var _ io.WriteCloser = (*LogWriter)(nil)

func (w *LogWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	n, err := w.f.Write(p)
	w.offset += int64(n)
	return n, err
}

// Offset is the number of bytes written so far — the position a subsequent
// write will start at.
func (w *LogWriter) Offset() int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.offset
}

func (w *LogWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.f.Close()
}
```

- [ ] **Step 4: Run to verify it passes**

Run: `go test ./internal/eventlog/... -race -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/eventlog && git commit -m "feat(eventlog): seekable per-step, per-attempt log files"
```

---

### Task 4: The Sink seam

**Files:** Create `internal/sink/sink.go`, `internal/sink/sink_test.go`

**Interfaces:**
- Consumes: `api.Event`.
- Produces: `sink.Sink` interface with `Emit(api.Event)` and `Control() <-chan ControlRequest`; `sink.ControlRequest`; `sink.Nop()`; `sink.Multi(...Sink) Sink`; `sink.RecordingSink` and `sink.Recording() *RecordingSink` with `Events() []api.Event`.

- [ ] **Step 1: Write the failing test**

```go
package sink_test

import (
	"testing"
	"time"

	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/internal/sink"
)

// The engine's correctness cannot depend on whether anyone is watching. A sink
// that blocks or panics must not be able to stall or kill a run.
func TestMultiIsNonBlockingAndPanicSafe(t *testing.T) {
	slow := sink.FuncSink(func(api.Event) { time.Sleep(50 * time.Millisecond) })
	boom := sink.FuncSink(func(api.Event) { panic("observer exploded") })
	rec := sink.Recording()

	m := sink.Multi(slow, boom, rec)

	done := make(chan struct{})
	go func() {
		defer close(done)
		m.Emit(api.Event{Seq: 1, Type: api.RunStarted})
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Emit blocked — a slow observer must never stall the engine")
	}

	// The healthy sink still receives it, eventually.
	deadline := time.After(2 * time.Second)
	for {
		if len(rec.Events()) == 1 {
			return
		}
		select {
		case <-deadline:
			t.Fatal("healthy sink never received the event")
		case <-time.After(5 * time.Millisecond):
		}
	}
}

func TestNopHasNoControlChannel(t *testing.T) {
	if sink.Nop().Control() != nil {
		t.Error("a no-op sink must expose a nil control channel")
	}
}

func TestRecordingCapturesOrder(t *testing.T) {
	rec := sink.Recording()
	for i := 1; i <= 3; i++ {
		rec.Emit(api.Event{Seq: uint64(i), Type: api.StepCreated})
	}
	got := rec.Events()
	if len(got) != 3 || got[0].Seq != 1 || got[2].Seq != 3 {
		t.Errorf("Events = %v", got)
	}
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `go test ./internal/sink/... -v`
Expected: FAIL — package does not exist.

- [ ] **Step 3: Implement**

```go
// Package sink defines the engine's one coupling to observers.
//
// Emit is non-blocking and never fails. Everything that could block — client
// fan-out, ring buffers, network writes — lives behind it. A pipeline with no
// observer pays nothing and starts no goroutines.
package sink

import (
	"sync"

	"github.com/xavidop/senro/api"
)

// ControlRequest is an operation an observer asks the engine to perform.
type ControlRequest struct {
	ID       string
	Op       string
	ClientID string
	Args     map[string]string
	Reply    chan<- ControlResponse
}

// ControlResponse is the engine's answer.
type ControlResponse struct {
	ID    string
	OK    bool
	Error string
}

// Sink observes a run. Implementations must not block in Emit.
type Sink interface {
	Emit(api.Event)
	Control() <-chan ControlRequest
}

// FuncSink adapts a function to Sink. It has no control channel.
type FuncSink func(api.Event)

func (f FuncSink) Emit(e api.Event)              { f(e) }
func (f FuncSink) Control() <-chan ControlRequest { return nil }

type nop struct{}

// Nop is a sink that does nothing, for runs with no observer.
func Nop() Sink                          { return nop{} }
func (nop) Emit(api.Event)               {}
func (nop) Control() <-chan ControlRequest { return nil }

type multi struct {
	sinks []Sink
}

// Multi fans an event out to several sinks, each on its own goroutine, so that
// one slow or panicking observer cannot affect the engine or its peers.
func Multi(sinks ...Sink) Sink { return &multi{sinks: sinks} }

func (m *multi) Emit(e api.Event) {
	for _, s := range m.sinks {
		go func(s Sink) {
			defer func() { _ = recover() }() // an observer must not kill a run
			s.Emit(e)
		}(s)
	}
}

// Control returns the first non-nil control channel among the sinks.
func (m *multi) Control() <-chan ControlRequest {
	for _, s := range m.sinks {
		if c := s.Control(); c != nil {
			return c
		}
	}
	return nil
}

type RecordingSink struct {
	mu     sync.Mutex
	events []api.Event
}

// Recording returns a Sink that retains what it saw, for tests.
func Recording() *RecordingSink { return &RecordingSink{} }

func (r *RecordingSink) Emit(e api.Event) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, e)
}

func (r *RecordingSink) Control() <-chan ControlRequest { return nil }

func (r *RecordingSink) Events() []api.Event {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]api.Event(nil), r.events...)
}
```

- [ ] **Step 4: Run to verify it passes**

Run: `go test ./internal/sink/... -race -v`
Expected: PASS.

**Note on `Multi`'s goroutine-per-event:** this is correct for the non-blocking contract but unbounded. The attach hub in a later plan owns a bounded ring and is the real backpressure boundary; record that here rather than optimising now.

- [ ] **Step 5: Commit**

```bash
git add internal/sink && git commit -m "feat(sink): the observer seam, non-blocking by construction"
```

---

### Task 5: Executor and Sandbox interfaces

**Files:** Create `internal/executor/executor.go`, `internal/executor/executor_test.go`

**Interfaces:**
- Consumes: nothing beyond stdlib.
- Produces: `executor.Platform{OS, Arch string}` with `String()`; `executor.Cmd{Args []string; Env []string; Dir string}`; `executor.Mount{Name, Digest, At string; RO bool}`; `executor.SecretRef{Name, Source string}`; `executor.SandboxSpec{StepID string; Attempt int; Mounts []Mount; Secrets []SecretRef; Env []string; WorkDir string}`; `executor.Executor` and `executor.Sandbox` interfaces; `executor.ErrInfra` sentinel and `executor.IsInfra(error) bool`.

- [ ] **Step 1: Write the failing test**

```go
package executor_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/xavidop/senro/internal/executor"
)

// The exit/error split is the whole basis of retry classification: infra
// failures are retryable without judgement, a non-zero exit is the workload's
// verdict and retrying it deletes information.
func TestIsInfra(t *testing.T) {
	wrapped := fmt.Errorf("dialing host: %w", executor.ErrInfra)
	if !executor.IsInfra(wrapped) {
		t.Error("a wrapped ErrInfra must classify as infrastructure failure")
	}
	if executor.IsInfra(errors.New("go test failed")) {
		t.Error("an ordinary error must not classify as infrastructure failure")
	}
	if executor.IsInfra(nil) {
		t.Error("nil is not a failure")
	}
}

func TestPlatformString(t *testing.T) {
	if got := (executor.Platform{OS: "linux", Arch: "arm64"}).String(); got != "linux/arm64" {
		t.Errorf("String = %q, want linux/arm64", got)
	}
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `go test ./internal/executor/... -v`
Expected: FAIL — package does not exist.

- [ ] **Step 3: Implement**

```go
// Package executor defines where a step runs.
//
// This is the seam that keeps the executor matrix linear: every executor
// implements the same interface, and data moves between them by content
// address rather than executor-to-executor transfer.
package executor

import (
	"context"
	"errors"
	"io"
)

// ErrInfra marks a failure of the execution substrate rather than the
// workload. Wrap it with %w. Retry predicates key off this.
var ErrInfra = errors.New("infrastructure failure")

// IsInfra reports whether err represents an infrastructure failure.
func IsInfra(err error) bool { return err != nil && errors.Is(err, ErrInfra) }

// Platform is an execution target's OS and architecture.
type Platform struct {
	OS   string `json:"os"`
	Arch string `json:"arch"`
}

func (p Platform) String() string { return p.OS + "/" + p.Arch }

// Cmd is a command to run inside a sandbox.
type Cmd struct {
	Args []string
	Env  []string
	Dir  string
}

// Mount declares a workspace the sandbox must provide. It is a declaration,
// not an instruction to push bytes: an executor may realise it however it can,
// including having the target pull from a content-addressed store itself.
type Mount struct {
	Name   string
	Digest string
	At     string
	RO     bool
}

// SecretRef names a secret the step needs. Values never appear here.
type SecretRef struct {
	Name   string
	Source string
}

// SandboxSpec is everything a sandbox must provide before the step runs.
type SandboxSpec struct {
	StepID  string
	Attempt int
	Mounts  []Mount
	Secrets []SecretRef
	Env     []string
	WorkDir string
}

// Executor creates sandboxes on one execution target.
type Executor interface {
	// Class is the cache equivalence class — deliberately not host identity,
	// or a fleet never shares cache entries.
	Class(ctx context.Context) (string, error)

	// DeclaredPlatform is resolved at plan time and is what enters a cache key.
	DeclaredPlatform(ctx context.Context) (Platform, error)

	Sandbox(ctx context.Context, spec SandboxSpec) (Sandbox, error)
}

// Sandbox is one step's execution environment.
type Sandbox interface {
	// ObservedPlatform is read after the sandbox exists and verified against
	// the declaration. It is never a cache key input.
	ObservedPlatform(ctx context.Context) (Platform, error)

	// Snapshot captures a writable workspace and returns its digest.
	Snapshot(ctx context.Context, name string) (string, error)

	// PutSecret delivers a value and returns the path the step reads it from.
	PutSecret(ctx context.Context, name string, v []byte) (string, error)

	// Run executes the command.
	//
	// exit is the workload's verdict; err is infrastructure failure. They are
	// separate because retry predicates distinguish them, and collapsing them
	// is how a pipeline ends up retrying `go test` until it passes.
	Run(ctx context.Context, c Cmd, stdout, stderr io.Writer) (exit int, err error)

	// Close tears the sandbox down. keep defers teardown so a debugging shell
	// can attach to the filesystem state of a failed step.
	Close(ctx context.Context, keep bool) error
}
```

- [ ] **Step 4: Run to verify it passes**

Run: `go test ./internal/executor/... -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/executor && git commit -m "feat(executor): Executor and Sandbox interfaces"
```

---

### Task 6: The local sandbox

**Files:** Create `internal/executor/localexec/localexec.go`, `internal/executor/localexec/localexec_test.go`

**Interfaces:**
- Consumes: Task 5's `executor` package.
- Produces: `localexec.New(root string) executor.Executor`, where `root` is the run directory under which step working directories are created.

- [ ] **Step 1: Write the failing test**

```go
package localexec_test

import (
	"bytes"
	"context"
	"runtime"
	"testing"

	"github.com/xavidop/senro/internal/executor"
	"github.com/xavidop/senro/internal/executor/localexec"
)

func newSandbox(t *testing.T) executor.Sandbox {
	t.Helper()
	ex := localexec.New(t.TempDir())
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
	p, err := localexec.New(t.TempDir()).DeclaredPlatform(context.Background())
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
```

- [ ] **Step 2: Run to verify it fails**

Run: `go test ./internal/executor/localexec/... -v`
Expected: FAIL — package does not exist.

- [ ] **Step 3: Implement**

```go
// Package localexec runs steps as child processes on the coordinator's host.
package localexec

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"

	senroexec "github.com/xavidop/senro/internal/executor"
	"github.com/xavidop/senro/internal/stepid"
)

type local struct{ root string }

// New returns an Executor that runs steps on this host, with step working
// directories created under root.
func New(root string) senroexec.Executor { return &local{root: root} }

func (l *local) Class(context.Context) (string, error) {
	return "local/" + runtime.GOOS + "/" + runtime.GOARCH, nil
}

func (l *local) DeclaredPlatform(context.Context) (senroexec.Platform, error) {
	return senroexec.Platform{OS: runtime.GOOS, Arch: runtime.GOARCH}, nil
}

func (l *local) Sandbox(_ context.Context, spec senroexec.SandboxSpec) (senroexec.Sandbox, error) {
	dir := filepath.Join(l.root, "work", stepid.Encode(spec.StepID), strconv.Itoa(spec.Attempt))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("localexec: %w: %w", senroexec.ErrInfra, err)
	}
	return &sandbox{dir: dir, spec: spec}, nil
}

type sandbox struct {
	dir  string
	spec senroexec.SandboxSpec
}

func (s *sandbox) ObservedPlatform(context.Context) (senroexec.Platform, error) {
	return senroexec.Platform{OS: runtime.GOOS, Arch: runtime.GOARCH}, nil
}

func (s *sandbox) Snapshot(context.Context, string) (string, error) {
	// Content addressing arrives with the storage plan. Until then a step's
	// output is whatever it left in the working directory.
	return "", fmt.Errorf("localexec: snapshot not implemented in this phase")
}

func (s *sandbox) PutSecret(_ context.Context, name string, v []byte) (string, error) {
	p := filepath.Join(s.dir, ".secrets", name)
	if err := os.MkdirAll(filepath.Dir(p), 0o700); err != nil {
		return "", fmt.Errorf("localexec: %w: %w", senroexec.ErrInfra, err)
	}
	if err := os.WriteFile(p, v, 0o600); err != nil {
		return "", fmt.Errorf("localexec: %w: %w", senroexec.ErrInfra, err)
	}
	return p, nil
}

func (s *sandbox) Run(ctx context.Context, c senroexec.Cmd, stdout, stderr io.Writer) (int, error) {
	if len(c.Args) == 0 {
		return 0, fmt.Errorf("localexec: %w: empty command", senroexec.ErrInfra)
	}

	dir := s.dir
	if c.Dir != "" {
		dir = c.Dir
	}

	cmd := exec.CommandContext(ctx, c.Args[0], c.Args[1:]...)
	cmd.Dir = dir
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	// Env is set explicitly and never through os.Setenv, which would leak into
	// every other subprocess and into coordinator crash dumps.
	cmd.Env = c.Env

	err := cmd.Run()
	if err == nil {
		return 0, nil
	}

	// A command that ran and exited non-zero is the workload's verdict, not an
	// infrastructure failure. Anything else — missing binary, permission
	// denied, cancelled context — is infrastructure.
	var ee *exec.ExitError
	if errors.As(err, &ee) {
		if ctx.Err() != nil {
			return ee.ExitCode(), fmt.Errorf("localexec: %w: %w", senroexec.ErrInfra, ctx.Err())
		}
		return ee.ExitCode(), nil
	}
	return 0, fmt.Errorf("localexec: %w: %w", senroexec.ErrInfra, err)
}

func (s *sandbox) Close(_ context.Context, keep bool) error {
	if keep {
		return nil
	}
	return nil // the run directory is the run's artifact; a later plan reaps it
}
```

- [ ] **Step 4: Run to verify it passes**

Run: `go test ./internal/executor/localexec/... -race -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/executor/localexec && git commit -m "feat(localexec): local sandbox with the exit/error split"
```

---

### Task 7: Plan, nodes and validation

**Files:** Create `internal/plan/plan.go`, `internal/plan/validate.go`, `internal/plan/plan_test.go`

**Interfaces:**
- Consumes: nothing beyond stdlib.
- Produces: `plan.Node{ID, Kind string; Cmd []string; WorkDir string; Env []string; Needs []string; ContinueOnError bool}`; `plan.Plan{Version int; Nodes []Node}`; `(*Plan).Marshal() ([]byte, error)`; `plan.Unmarshal([]byte) (*Plan, error)`; `(*Plan).Validate() error`; `(*Plan).Digest() string`; `(*Plan).Node(id string) (*Node, bool)`.

- [ ] **Step 1: Write the failing test**

```go
package plan_test

import (
	"strings"
	"testing"

	"github.com/xavidop/senro/internal/plan"
)

func TestValidateRejectsCycles(t *testing.T) {
	p := &plan.Plan{Nodes: []plan.Node{
		{ID: "a", Kind: "exec", Cmd: []string{"true"}, Needs: []string{"c"}},
		{ID: "b", Kind: "exec", Cmd: []string{"true"}, Needs: []string{"a"}},
		{ID: "c", Kind: "exec", Cmd: []string{"true"}, Needs: []string{"b"}},
	}}
	err := p.Validate()
	if err == nil {
		t.Fatal("a cycle must be rejected at plan time")
	}
	// The error must name the cycle — "invalid plan" sends someone hunting.
	for _, id := range []string{"a", "b", "c"} {
		if !strings.Contains(err.Error(), id) {
			t.Errorf("error %q should name the nodes in the cycle", err)
		}
	}
}

func TestValidateRejectsDanglingNeeds(t *testing.T) {
	p := &plan.Plan{Nodes: []plan.Node{
		{ID: "a", Kind: "exec", Cmd: []string{"true"}, Needs: []string{"ghost"}},
	}}
	err := p.Validate()
	if err == nil || !strings.Contains(err.Error(), "ghost") {
		t.Errorf("a dangling Needs must be rejected and named, got %v", err)
	}
}

func TestValidateRejectsDuplicateIDs(t *testing.T) {
	p := &plan.Plan{Nodes: []plan.Node{
		{ID: "a", Kind: "exec", Cmd: []string{"true"}},
		{ID: "a", Kind: "exec", Cmd: []string{"true"}},
	}}
	if err := p.Validate(); err == nil {
		t.Error("duplicate step IDs must be rejected")
	}
}

func TestValidateRejectsEmptyCommand(t *testing.T) {
	p := &plan.Plan{Nodes: []plan.Node{{ID: "a", Kind: "exec"}}}
	if err := p.Validate(); err == nil {
		t.Error("an exec node with no command must be rejected at plan time")
	}
}

func TestValidateAcceptsADAG(t *testing.T) {
	p := &plan.Plan{Nodes: []plan.Node{
		{ID: "setup", Kind: "exec", Cmd: []string{"true"}},
		{ID: "a", Kind: "exec", Cmd: []string{"true"}, Needs: []string{"setup"}},
		{ID: "b", Kind: "exec", Cmd: []string{"true"}, Needs: []string{"setup"}},
		{ID: "done", Kind: "exec", Cmd: []string{"true"}, Needs: []string{"a", "b"}},
	}}
	if err := p.Validate(); err != nil {
		t.Errorf("a valid DAG was rejected: %v", err)
	}
}

func TestMarshalRoundTrip(t *testing.T) {
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{
		{ID: "a", Kind: "exec", Cmd: []string{"go", "test"}, Needs: []string{}},
	}}
	b, err := p.Marshal()
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	got, err := plan.Unmarshal(b)
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if len(got.Nodes) != 1 || got.Nodes[0].ID != "a" {
		t.Errorf("round-trip lost data: %+v", got)
	}
}

// The digest is recorded in plan.resolved and ties a run to its timetable, so
// it must not change when nothing semantic changed.
func TestDigestIsStableAcrossNodeOrder(t *testing.T) {
	a := &plan.Plan{Version: 1, Nodes: []plan.Node{
		{ID: "x", Kind: "exec", Cmd: []string{"true"}},
		{ID: "y", Kind: "exec", Cmd: []string{"true"}},
	}}
	b := &plan.Plan{Version: 1, Nodes: []plan.Node{
		{ID: "y", Kind: "exec", Cmd: []string{"true"}},
		{ID: "x", Kind: "exec", Cmd: []string{"true"}},
	}}
	if a.Digest() != b.Digest() {
		t.Errorf("digest depends on node order: %s vs %s", a.Digest(), b.Digest())
	}
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `go test ./internal/plan/... -v`
Expected: FAIL — package does not exist.

- [ ] **Step 3: Implement `plan.go`**

```go
// Package plan is the resolved timetable: what the engine executes.
package plan

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
)

// Node is one step in a resolved plan.
type Node struct {
	ID              string   `json:"id"`
	Kind            string   `json:"kind"`
	Cmd             []string `json:"cmd,omitempty"`
	WorkDir         string   `json:"workdir,omitempty"`
	Env             []string `json:"env,omitempty"`
	Needs           []string `json:"needs,omitempty"`
	ContinueOnError bool     `json:"continue_on_error,omitempty"`
}

// Plan is the serialized artifact the engine executes.
type Plan struct {
	Version int    `json:"version"`
	Nodes   []Node `json:"nodes"`
}

func (p *Plan) Marshal() ([]byte, error) { return json.MarshalIndent(p, "", "  ") }

func Unmarshal(b []byte) (*Plan, error) {
	var p Plan
	if err := json.Unmarshal(b, &p); err != nil {
		return nil, fmt.Errorf("plan: %w", err)
	}
	return &p, nil
}

func (p *Plan) Node(id string) (*Node, bool) {
	for i := range p.Nodes {
		if p.Nodes[i].ID == id {
			return &p.Nodes[i], true
		}
	}
	return nil, false
}

// Digest identifies the plan's content. Nodes are sorted first so that a
// reordering that changes nothing semantic does not change the digest.
func (p *Plan) Digest() string {
	c := Plan{Version: p.Version, Nodes: append([]Node(nil), p.Nodes...)}
	sort.Slice(c.Nodes, func(i, j int) bool { return c.Nodes[i].ID < c.Nodes[j].ID })
	b, err := json.Marshal(c)
	if err != nil {
		return ""
	}
	sum := sha256.Sum256(b)
	return "sha256:" + hex.EncodeToString(sum[:])
}
```

- [ ] **Step 4: Implement `validate.go`**

```go
package plan

import (
	"fmt"
	"sort"
	"strings"
)

// Validate rejects a plan the engine could not faithfully execute.
//
// Everything detectable here belongs here: a failure at plan time names the
// problem once, while the same failure at run time surfaces on whichever
// target happened to schedule it.
func (p *Plan) Validate() error {
	byID := make(map[string]*Node, len(p.Nodes))
	for i := range p.Nodes {
		n := &p.Nodes[i]
		if n.ID == "" {
			return fmt.Errorf("plan: a node has an empty id")
		}
		if _, dup := byID[n.ID]; dup {
			return fmt.Errorf("plan: duplicate step id %q", n.ID)
		}
		byID[n.ID] = n
	}

	for _, n := range p.Nodes {
		if n.Kind == "exec" && len(n.Cmd) == 0 {
			return fmt.Errorf("plan: step %q is an exec step with no command", n.ID)
		}
		for _, need := range n.Needs {
			if _, ok := byID[need]; !ok {
				return fmt.Errorf("plan: step %q needs %q, which does not exist", n.ID, need)
			}
		}
	}

	return p.checkAcyclic(byID)
}

// checkAcyclic reports the first cycle it finds, naming every node on it —
// "invalid plan" would send someone hunting through a 300-node graph.
func (p *Plan) checkAcyclic(byID map[string]*Node) error {
	const (
		white = 0 // unvisited
		grey  = 1 // on the current path
		black = 2 // fully explored
	)
	colour := make(map[string]int, len(byID))

	ids := make([]string, 0, len(byID))
	for id := range byID {
		ids = append(ids, id)
	}
	sort.Strings(ids) // deterministic error messages

	var path []string
	var visit func(string) error
	visit = func(id string) error {
		switch colour[id] {
		case black:
			return nil
		case grey:
			from := 0
			for i, p := range path {
				if p == id {
					from = i
					break
				}
			}
			cycle := append(append([]string(nil), path[from:]...), id)
			return fmt.Errorf("plan: dependency cycle: %s", strings.Join(cycle, " -> "))
		}
		colour[id] = grey
		path = append(path, id)
		needs := append([]string(nil), byID[id].Needs...)
		sort.Strings(needs)
		for _, need := range needs {
			if err := visit(need); err != nil {
				return err
			}
		}
		path = path[:len(path)-1]
		colour[id] = black
		return nil
	}

	for _, id := range ids {
		if err := visit(id); err != nil {
			return err
		}
	}
	return nil
}
```

- [ ] **Step 5: Run to verify it passes**

Run: `go test ./internal/plan/... -v`
Expected: PASS.

- [ ] **Step 6: Prove the cycle detector**

Add a node to `TestValidateAcceptsADAG` creating `done -> setup -> done`, confirm the test fails naming the cycle, then remove it. Record the output.

- [ ] **Step 7: Commit**

```bash
git add internal/plan && git commit -m "feat(plan): resolved plan, digest and plan-time validation"
```

---

### Task 8: The builder API

**Files:** Create `senro.go`, `exec/exec.go`, `local/local.go`, `senro_test.go`

**Interfaces:**
- Consumes: Task 7's `plan`.
- Produces: `senro.New(name string) *Pipeline`; `(*Pipeline).Workflow(name string, opts ...WorkflowOption) *WorkflowBuilder`; `(*WorkflowBuilder).Step(id string, a Action) *StepBuilder`; `(*StepBuilder).Needs(ids ...string) *StepBuilder`; `(*StepBuilder).ContinueOnError() *StepBuilder`; `(*StepBuilder).Env(key, value string) *StepBuilder`; `(*Pipeline).Build() (*plan.Plan, error)`; `senro.Action` interface; `exec.Command(args ...string) senro.Action`; `local.Host() senro.ExecutorOption`.

- [ ] **Step 1: Write the failing test**

```go
package senro_test

import (
	"testing"

	"github.com/xavidop/senro"
	"github.com/xavidop/senro/exec"
)

func TestBuildProducesAValidPlan(t *testing.T) {
	pipe := senro.New("ci")
	l := pipe.Workflow("main")
	l.Step("setup", exec.Command("echo", "setup"))
	l.Step("test", exec.Command("go", "test", "./...")).Needs("setup")

	p, err := pipe.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if len(p.Nodes) != 2 {
		t.Fatalf("Nodes = %d, want 2", len(p.Nodes))
	}
	n, ok := p.Node("test")
	if !ok {
		t.Fatal("test node missing")
	}
	if len(n.Needs) != 1 || n.Needs[0] != "setup" {
		t.Errorf("Needs = %v, want [setup]", n.Needs)
	}
	if n.Cmd[0] != "go" {
		t.Errorf("Cmd = %v", n.Cmd)
	}
}

// Build must run validation, so a bad line fails before anything executes.
func TestBuildValidates(t *testing.T) {
	pipe := senro.New("ci")
	l := pipe.Workflow("main")
	l.Step("a", exec.Command("true")).Needs("nope")
	if _, err := pipe.Build(); err == nil {
		t.Error("Build must reject a dangling dependency")
	}
}

func TestDuplicateStepIDIsRejected(t *testing.T) {
	pipe := senro.New("ci")
	l := pipe.Workflow("main")
	l.Step("a", exec.Command("true"))
	l.Step("a", exec.Command("true"))
	if _, err := pipe.Build(); err == nil {
		t.Error("Build must reject a duplicate step id")
	}
}

// A built plan is a value; mutating the builder afterwards must not change it.
func TestBuildSnapshotsTheGraph(t *testing.T) {
	pipe := senro.New("ci")
	l := pipe.Workflow("main")
	l.Step("a", exec.Command("true"))

	p, err := pipe.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	l.Step("b", exec.Command("true"))

	if len(p.Nodes) != 1 {
		t.Errorf("the built plan changed after further building: %d nodes", len(p.Nodes))
	}
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `go test . -v`
Expected: FAIL, `undefined: senro.New`.

- [ ] **Step 3: Implement `exec/exec.go`**

```go
// Package exec constructs command steps, the step kind portable to every
// executor.
package exec

// Action is what a step does. It mirrors senro.Action; the interface lives
// here too so this package does not import the root and create a cycle.
type Action interface {
	ActionKind() string
	ActionCmd() []string
}

type command struct{ args []string }

// Command runs an executable with arguments. Nothing is shell-interpreted:
// pass a shell explicitly if you want one.
func Command(args ...string) Action { return command{args: args} }

func (c command) ActionKind() string  { return "exec" }
func (c command) ActionCmd() []string { return c.args }
```

- [ ] **Step 4: Implement `senro.go`**

```go
// Package senro defines pipelines in Go and executes them. It is a pipeline
// engine first: CI/CD is the most familiar thing to build on it, not the
// boundary of what it is for.
//
// A pipeline is built as an immutable DAG, resolved into a plan, and executed
// by the engine; user code never drives execution. Every observable fact about
// a run is an event in an append-only stream.
//
// The name is 線路 (senro), railway track. The metaphor carries through the
// documentation and error messages — steps are stations, a workflow is a line,
// a resolved plan is a timetable — but not through identifiers.
package senro

import (
	"fmt"

	"github.com/xavidop/senro/internal/plan"
)

// Action is what a step does.
type Action interface {
	ActionKind() string
	ActionCmd() []string
}

// Pipeline accumulates workflows. Build snapshots it into a plan.
type Pipeline struct {
	name      string
	workflows []*WorkflowBuilder
}

// New starts a new pipeline.
func New(name string) *Pipeline { return &Pipeline{name: name} }

// Name reports the pipeline's name.
func (p *Pipeline) Name() string { return p.name }

// WorkflowBuilder accumulates the steps of one workflow.
type WorkflowBuilder struct {
	name  string
	steps []*StepBuilder
}

// Workflow adds a named group of steps to the pipeline.
func (p *Pipeline) Workflow(name string, opts ...WorkflowOption) *WorkflowBuilder {
	w := &WorkflowBuilder{name: name}
	p.workflows = append(p.workflows, w)
	return w
}

// Step adds a station to the workflow.
func (w *WorkflowBuilder) Step(id string, a Action) *StepBuilder {
	sb := &StepBuilder{id: id, action: a}
	w.steps = append(w.steps, sb)
	return sb
}

// Build resolves and validates the pipeline. The returned plan is a
// snapshot: further building does not change it.
func (pipe *Pipeline) Build() (*plan.Plan, error) {
	p := &plan.Plan{Version: 1}
	for _, sb := range pipe.allSteps() {
		if sb.action == nil {
			return nil, fmt.Errorf("senro: step %q has no action", sb.id)
		}
		p.Nodes = append(p.Nodes, plan.Node{
			ID:              sb.id,
			Kind:            sb.action.ActionKind(),
			Cmd:             append([]string(nil), sb.action.ActionCmd()...),
			WorkDir:         sb.workDir,
			Env:             append([]string(nil), sb.env...),
			Needs:           append([]string(nil), sb.needs...),
			ContinueOnError: sb.continueOnError,
		})
	}
	if err := p.Validate(); err != nil {
		return nil, err
	}
	return p, nil
}

// StepBuilder configures one station.
type StepBuilder struct {
	id              string
	action          Action
	needs           []string
	env             []string
	workDir         string
	continueOnError bool
}

// Needs declares upstream steps that must finish first.
func (s *StepBuilder) Needs(ids ...string) *StepBuilder {
	s.needs = append(s.needs, ids...)
	return s
}

// Env sets environment entries as "KEY=value".
func (s *StepBuilder) Env(kv ...string) *StepBuilder {
	s.env = append(s.env, kv...)
	return s
}

// WorkDir sets the working directory.
func (s *StepBuilder) WorkDir(dir string) *StepBuilder {
	s.workDir = dir
	return s
}

// ContinueOnError lets dependents run even if this step fails. Use for
// advisory steps such as lint or coverage upload.
func (s *StepBuilder) ContinueOnError() *StepBuilder {
	s.continueOnError = true
	return s
}
```

Delete the placeholder `doc.go` at the repo root, since `senro.go` now carries the package documentation. Keep the terminology paragraph.

- [ ] **Step 5: Run to verify it passes**

Run: `go test . ./exec/... -v`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git rm doc.go && git add senro.go exec senro_test.go
git commit -m "feat(senro): the Line and Step builder API"
```

---

### Task 9: The scheduler

**Files:** Create `internal/engine/engine.go`, `internal/engine/engine_test.go`

**Interfaces:**
- Consumes: Tasks 2–8.
- Produces: `engine.Options{Dir string; Executor executor.Executor; Sink sink.Sink; MaxParallel int; RunID string}`; `engine.Run(ctx context.Context, p *plan.Plan, opts Options) (api.RunStatus, error)`.

- [ ] **Step 1: Write the failing test**

```go
package engine_test

import (
	"context"
	"testing"

	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/internal/engine"
	"github.com/xavidop/senro/internal/executor/localexec"
	"github.com/xavidop/senro/internal/plan"
	"github.com/xavidop/senro/internal/sink"
)

func run(t *testing.T, p *plan.Plan) (api.RunStatus, *sink.RecordingSink, string) {
	t.Helper()
	dir := t.TempDir()
	rec := sink.Recording()
	status, err := engine.Run(context.Background(), p, engine.Options{
		Dir:         dir,
		Executor:    localexec.New(dir),
		Sink:        rec,
		MaxParallel: 4,
		RunID:       "01TEST",
	})
	if err != nil {
		t.Fatalf("Run returned an engine error: %v", err)
	}
	return status, rec, dir
}

func TestRunsAChainInOrder(t *testing.T) {
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{
		{ID: "first", Kind: "exec", Cmd: []string{"echo", "1"}},
		{ID: "second", Kind: "exec", Cmd: []string{"echo", "2"}, Needs: []string{"first"}},
	}}
	status, _, dir := run(t, p)
	if status != api.RunSucceeded {
		t.Errorf("status = %s, want succeeded", status)
	}

	events := readLedger(t, dir)
	firstDone := indexOf(events, api.StepFinished, "first")
	secondStart := indexOf(events, api.StepStarted, "second")
	if firstDone < 0 || secondStart < 0 || firstDone > secondStart {
		t.Errorf("dependency order violated: first finished at %d, second started at %d",
			firstDone, secondStart)
	}
}

func TestFailurePropagatesToDependentsOnly(t *testing.T) {
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{
		{ID: "boom", Kind: "exec", Cmd: []string{"sh", "-c", "exit 1"}},
		{ID: "downstream", Kind: "exec", Cmd: []string{"echo", "x"}, Needs: []string{"boom"}},
		{ID: "unrelated", Kind: "exec", Cmd: []string{"echo", "y"}},
	}}
	status, _, dir := run(t, p)
	if status != api.RunFailed {
		t.Errorf("status = %s, want failed", status)
	}

	st := foldStates(t, dir)
	if st["boom"] != api.StateFailed {
		t.Errorf("boom = %s, want failed", st["boom"])
	}
	if st["downstream"] != api.StateSkippedUpstreamFailed {
		t.Errorf("downstream = %s, want skipped_upstream_failed", st["downstream"])
	}
	// An unrelated branch runs to completion, so one failure yields one report
	// rather than a half-explored graph.
	if st["unrelated"] != api.StateSucceeded {
		t.Errorf("unrelated = %s, want succeeded", st["unrelated"])
	}
}

func TestContinueOnErrorLetsDependentsRun(t *testing.T) {
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{
		{ID: "advisory", Kind: "exec", Cmd: []string{"sh", "-c", "exit 1"}, ContinueOnError: true},
		{ID: "after", Kind: "exec", Cmd: []string{"echo", "x"}, Needs: []string{"advisory"}},
	}}
	_, _, dir := run(t, p)
	st := foldStates(t, dir)
	if st["after"] != api.StateSucceeded {
		t.Errorf("after = %s, want succeeded — ContinueOnError must not block dependents", st["after"])
	}
}

func TestLogsAreWrittenAndMarked(t *testing.T) {
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{
		{ID: "talker", Kind: "exec", Cmd: []string{"echo", "hello"}},
	}}
	_, _, dir := run(t, p)

	events := readLedger(t, dir)
	var total int64
	for _, e := range events {
		if e.Type != api.StepLogAppended {
			continue
		}
		var b api.StepLogAppendedBody
		if err := e.Decode(&b); err != nil {
			t.Fatalf("decode: %v", err)
		}
		total += b.Len
	}
	if total != 6 {
		t.Errorf("log markers total %d bytes, want 6 for \"hello\\n\"", total)
	}
}

// The ledger is the source of truth. Every event a sink saw must be in it,
// with the same sequence number.
func TestSinkSeesExactlyWhatTheLedgerRecorded(t *testing.T) {
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{
		{ID: "a", Kind: "exec", Cmd: []string{"echo", "a"}},
	}}
	_, rec, dir := run(t, p)

	ledger := readLedger(t, dir)
	waitForSink(t, rec, len(ledger))

	seen := make(map[uint64]api.Type, len(ledger))
	for _, e := range ledger {
		seen[e.Seq] = e.Type
	}
	for _, e := range rec.Events() {
		if typ, ok := seen[e.Seq]; !ok {
			t.Errorf("sink saw seq %d which is not in the ledger", e.Seq)
		} else if typ != e.Type {
			t.Errorf("seq %d: sink saw %s, ledger has %s", e.Seq, e.Type, typ)
		}
	}
}
```

Write the helpers `readLedger`, `indexOf`, `foldStates` and `waitForSink` in the same file: `readLedger` calls `eventlog.Read(filepath.Join(dir, "events.jsonl"))`; `indexOf` returns the index of the first event of a given type and step, or -1; `foldStates` replays the ledger through `api.RunState.Apply` and returns `map[string]api.State`; `waitForSink` polls `rec.Events()` up to two seconds for the expected count, since `Multi` fans out asynchronously.

- [ ] **Step 2: Run to verify it fails**

Run: `go test ./internal/engine/... -v`
Expected: FAIL — package does not exist.

- [ ] **Step 3: Implement**

Write `internal/engine/engine.go` implementing:

- `Run` opens the ledger and log set under `Options.Dir`, emits `run.started` (with pipeline name, engine version, plan digest), `plan.resolved`, then one `step.created` per node carrying `kind` and `needs`.
- A `emit(e api.Event)` helper that appends to the ledger **first**, and only then calls `opts.Sink.Emit` with the stamped event. A ledger error aborts the run.
- Ready-set scheduling: a node is ready when every `Needs` entry has reached a terminal state, and satisfying means `succeeded`, `cached`, `recovered`, or a failed node with `ContinueOnError`. A node with any unsatisfied terminal upstream becomes `skipped_upstream_failed` and is itself terminal, propagating transitively.
- A **global** buffered-channel semaphore of `MaxParallel` (default `runtime.NumCPU()`), acquired around execution only.
- Per step: emit `step.started`, create the sandbox via `opts.Executor.Sandbox`, obtain log writers for stdout and stderr, wrap each in a writer that appends to the file and emits `step.log.appended` with the pre-write offset and byte count, call `Sandbox.Run`, then emit `step.finished` with `succeeded` on exit 0, `failed` on non-zero exit, and `failed` with `Error` set when `Run` returns an infrastructure error. Close the sandbox with `keep=false`.
- Cancellation: when `ctx` is done, stop scheduling new steps and mark unstarted nodes `cancelled`.
- Finally emit `run.finished` with the status from `api.RollUp` over every step's terminal state, close the log set, close the ledger, and return the status.

Keep `Run` under ~200 lines by extracting the ready-set walk and the single-step execution into unexported helpers.

- [ ] **Step 4: Run to verify it passes**

Run: `go test ./internal/engine/... -race -v`
Expected: PASS.

- [ ] **Step 5: Prove the ledger-before-sink ordering**

Temporarily reorder `emit` to call `Sink.Emit` before appending to the ledger, and add a temporary assertion that a sink event's `Seq` is non-zero. Confirm the test fails (the sink sees an unstamped event), then restore. Record the output.

- [ ] **Step 6: Commit**

```bash
git add internal/engine && git commit -m "feat(engine): the scheduler"
```

---

### Task 10: End-to-end golden event log

**Files:** Create `internal/engine/golden_test.go`, `internal/engine/testdata/golden/two-step.jsonl`

**Interfaces:**
- Consumes: everything above.
- Produces: the phase's deliverable — a pipeline runs and its recorded event log matches a golden file.

- [ ] **Step 1: Write the failing test**

```go
package engine_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/xavidop/senro"
	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/exec"
	"github.com/xavidop/senro/internal/engine"
	"github.com/xavidop/senro/internal/executor/localexec"
	"github.com/xavidop/senro/internal/sink"
)

// scrub removes what legitimately varies between runs, so the golden file
// pins the event stream's shape without pinning a wall clock.
func scrub(e api.Event) api.Event {
	e.TS = api.Event{}.TS
	e.Run = "RUN"
	if len(e.Payload) > 0 {
		var m map[string]any
		if err := json.Unmarshal(e.Payload, &m); err == nil {
			for _, k := range []string{"started_at", "duration_ns", "cwd", "engine_version", "plan_digest"} {
				if _, ok := m[k]; ok {
					m[k] = nil
				}
			}
			if b, err := json.Marshal(m); err == nil {
				e.Payload = b
			}
		}
	}
	return e
}

func TestGoldenTwoStepRun(t *testing.T) {
	pipe := senro.New("golden")
	l := pipe.Workflow("main")
	l.Step("setup", exec.Command("echo", "setup"))
	l.Step("build", exec.Command("echo", "build")).Needs("setup")

	p, err := pipe.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	dir := t.TempDir()
	status, err := engine.Run(t.Context(), p, engine.Options{
		Dir: dir, Executor: localexec.New(dir), Sink: sink.Nop(),
		MaxParallel: 1, RunID: "01TEST",
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if status != api.RunSucceeded {
		t.Fatalf("status = %s", status)
	}

	events := readLedger(t, dir)
	var got strings.Builder
	for _, e := range events {
		b, err := json.Marshal(scrub(e))
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		got.WriteString(string(b) + "\n")
	}

	goldenPath := filepath.Join("testdata", "golden", "two-step.jsonl")
	if os.Getenv("UPDATE_GOLDEN") == "1" {
		if err := os.MkdirAll(filepath.Dir(goldenPath), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(goldenPath, []byte(got.String()), 0o644); err != nil {
			t.Fatal(err)
		}
		t.Log("golden updated")
		return
	}

	want, err := os.ReadFile(goldenPath)
	if err != nil {
		t.Fatalf("read golden (run with UPDATE_GOLDEN=1 to create it): %v", err)
	}
	if got.String() != string(want) {
		t.Errorf("event log does not match golden.\n--- got ---\n%s\n--- want ---\n%s",
			got.String(), want)
	}
}

// The golden log must fold cleanly through the same function every client
// uses. If the engine emits something the fold cannot make sense of, the two
// halves of the system have already diverged.
func TestGoldenFoldsToASucceededRun(t *testing.T) {
	events := readGolden(t, filepath.Join("testdata", "golden", "two-step.jsonl"))
	s := api.NewRunState()
	for i, e := range events {
		if err := s.Apply(e); err != nil {
			t.Fatalf("event %d: %v", i, err)
		}
	}
	if !s.Run.Done || s.Run.Status != api.RunSucceeded {
		t.Errorf("folded run = %+v, want a finished succeeded run", s.Run)
	}
	if len(s.Steps) != 2 {
		t.Errorf("Steps = %d, want 2", len(s.Steps))
	}
	if len(s.Order) != len(s.Steps) {
		t.Errorf("Order %d vs Steps %d — each step must be recorded once", len(s.Order), len(s.Steps))
	}
}
```

Write `readGolden` as a small helper that reads the file and unmarshals each line into an `api.Event`.

- [ ] **Step 2: Run to verify it fails**

Run: `go test ./internal/engine/... -run TestGolden -v`
Expected: FAIL — golden file missing.

- [ ] **Step 3: Generate and inspect the golden**

Run: `UPDATE_GOLDEN=1 go test ./internal/engine/... -run TestGoldenTwoStepRun`

Then **read `internal/engine/testdata/golden/two-step.jsonl` and check it by eye before committing it.** A golden file accepted without reading is a snapshot of whatever the code did, including its bugs. Confirm: `run.started` first and `run.finished` last; sequence numbers contiguous from 1; a `step.created` for each step; `setup` finishing before `build` starts; `step.log.appended` markers with plausible offsets and lengths; every `step.finished` carrying `duration_ns`; no `exit_code` on the successful steps. Note anything surprising in your report.

- [ ] **Step 4: Run to verify it passes**

Run: `go test ./internal/engine/... -run TestGolden -v`
Expected: PASS, both tests.

- [ ] **Step 5: Full verification**

Run: `go clean -testcache && make all`
Expected: green across both modules.

- [ ] **Step 6: Commit**

```bash
git add internal/engine/golden_test.go internal/engine/testdata
git commit -m "test(engine): end-to-end golden event log"
```

---

## Self-Review

**Spec coverage.** Phase 2 of the spec's table — builders, plan and validation and `plan.json`, the ledger writer, seekable logs, the local `Exec` sandbox, the scheduler, and "a pipeline runs; its event log matches a golden" — is covered by Tasks 1–10. §2.5's `Executor`/`Sandbox` shape including the exit/error split and `SandboxSpec` declaration is Task 5. §2.6's ledger-before-sinks ordering is Task 9 with a dedicated proof step. §4.6's percent-encoded log paths are Task 3. §4.1's plan-time validation set is Task 7, less the checks that need subsystems not yet built (secret TTLs, `Always` handler timeouts, multi-arch platform pinning, `Pure()` input declarations) — those land with the plans that introduce them.

**Deliberately deferred.** `senro.Run` as a public wrapper over `engine.Run`, and the `cmd/senro` CLI: both need the renderer work from the attach plan to be worth shipping, and `--ui=auto` cannot be honoured before a `Source` client exists. `Sandbox.Snapshot` returns an error until the storage plan lands. Retry, handlers and the shutdown grace path are the next plan.

**Placeholder scan.** No TBDs. Task 9's Step 3 is prose rather than a code block because the scheduler is the one component where a transcribed implementation would be worse than a specified one — every behaviour it must exhibit is enumerated and every one is asserted by a test in Step 1.

**Type consistency.** `stepid.Encode` is used for both log paths (Task 3) and work directories (Task 6). `executor.ErrInfra` is wrapped in Task 6 and classified in Tasks 5 and 9. `sink.Recording()` returns `*sink.RecordingSink`, the exported type Task 9's helper signature names. `plan.Node.Kind` is `"exec"`, matching `exec.Command`'s `ActionKind()` and the `kind` field in `api.StepCreatedBody`.

---

## Next

Plan 3 (failure handling) adds the state taxonomy's remaining paths — retry with `OnInfra` and jitter, `OnFailure`/`Always` handlers, and the shutdown grace sequence — on top of this scheduler. Do not begin it until `make all` passes here and the golden file has been read by a human.
