package tui_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	tea "github.com/charmbracelet/bubbletea"

	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/internal/eventlog"
	"github.com/xavidop/senro/internal/source"
	"github.com/xavidop/senro/internal/tui"
)

// ---- a fake source.Source, entirely in-memory, no pty, no socket ----
//
// render.Plain already proves the Source seam against a real FileSource,
// so this fake exists to give these tests synchronous control over
// State/Control/Logs responses, which a real network round trip cannot.
type fakeSource struct {
	mu sync.Mutex

	state    *api.RunState
	stateErr error

	// logs is keyed by step then attempt, matching how a real engine keeps
	// one log FILE per attempt (eventlog.LogSet.Path: <step>/<attempt>/
	// <stream>) rather than one continuous stream per step: the retry
	// tests need two attempts of the same step to have genuinely
	// independent content, each starting at its own byte 0.
	logs          map[string]map[int][]byte
	logsErr       error
	logsCallCount int
	logsCalledFor []logsCall // every (step, attempt, from) requested, for assertions

	// subCh, if set, is returned by Subscribe instead of a pre-closed
	// channel: see Subscribe's own doc.
	subCh chan api.Event

	controlResp api.Frame
	controlErr  error
	controlLog  []api.Frame // every request received, for assertions

	closed bool
}

var _ source.Source = (*fakeSource)(nil)

func (f *fakeSource) State(context.Context) (*api.RunState, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.stateErr != nil {
		return nil, f.stateErr
	}
	return f.state, nil
}

// Subscribe returns a closed empty channel by default: most of these tests
// drive the fold via tui.EventMsg directly, never through a live
// subscription goroutine. A test exercising the real concurrent drain path
// sets subCh and feeds it itself.
func (f *fakeSource) Subscribe(context.Context, uint64) (<-chan api.Event, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.subCh != nil {
		return f.subCh, nil
	}
	ch := make(chan api.Event)
	close(ch)
	return ch, nil
}

// logsCall records one Logs invocation, for tests that need to assert on
// exactly which (step, attempt) a fetch targeted, the retry tests in
// particular, where a fetch landing against the WRONG attempt's file is
// precisely the bug under test.
type logsCall struct {
	step    string
	attempt int
	from    int64
}

func (f *fakeSource) Logs(_ context.Context, step string, attempt int, _ string, from int64) (io.ReadCloser, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.logsCallCount++
	f.logsCalledFor = append(f.logsCalledFor, logsCall{step: step, attempt: attempt, from: from})
	if f.logsErr != nil {
		return nil, f.logsErr
	}
	b := f.logs[step][attempt]
	if from < 0 || from > int64(len(b)) {
		from = int64(len(b))
	}
	return io.NopCloser(strings.NewReader(string(b[from:]))), nil
}

// setLogs updates what a future Logs call for (step, attempt) will return:
// the fake's way of simulating the engine appending more output to a
// step's log file, or a retry starting a brand new one, between two of the
// model's own fetches.
func (f *fakeSource) setLogs(step string, attempt int, content []byte) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.logs == nil {
		f.logs = make(map[string]map[int][]byte)
	}
	if f.logs[step] == nil {
		f.logs[step] = make(map[int][]byte)
	}
	f.logs[step][attempt] = content
}

func (f *fakeSource) logsCalls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.logsCallCount
}

// logsCallsFor returns every recorded Logs call for step, in call order.
func (f *fakeSource) logsCallsFor(step string) []logsCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []logsCall
	for _, c := range f.logsCalledFor {
		if c.step == step {
			out = append(out, c)
		}
	}
	return out
}

func (f *fakeSource) Control(ctx context.Context, req api.Frame) (api.Frame, error) {
	// A real Control is a network round trip and would fail against an
	// already-cancelled context. Checking it here lets a test prove the
	// model's ctx was torn down by observing a LATER action fail, rather
	// than by inspecting unexported state.
	if err := ctx.Err(); err != nil {
		return api.Frame{}, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.controlLog = append(f.controlLog, req)
	if f.controlErr != nil {
		return api.Frame{}, f.controlErr
	}
	resp := f.controlResp
	resp.ID = req.ID
	return resp, nil
}

func (f *fakeSource) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed = true
	return nil
}

func (f *fakeSource) requests() []api.Frame {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]api.Frame(nil), f.controlLog...)
}

// okResp builds a control response Frame reporting success.
func okResp() api.Frame {
	ok := true
	return api.Frame{V: api.Version, Kind: api.KindRes, OK: &ok}
}

// refusedResp builds a control response Frame refusing the request with the
// given machine-readable reason: the exact shape internal/engine/control.go
// sends over the wire for run_finished, already_cancelled, step_running,
// step_not_failed, etc.
func refusedResp(reason string) api.Frame {
	ok := false
	return api.Frame{V: api.Version, Kind: api.KindRes, OK: &ok, Error: reason}
}

// ---- helpers ----

var ansiEscape = regexp.MustCompile("\x1b\\[[0-9;]*m")

// plain strips lipgloss/ANSI styling so assertions can match on literal text
// regardless of the color profile the environment running `go test` happens
// to auto-detect. What renders is under test here, not how it is colored.
func plain(s string) string { return ansiEscape.ReplaceAllString(s, "") }

func stepCreated(run, step string) api.Event {
	b, _ := json.Marshal(api.StepCreatedBody{Kind: "exec"})
	return api.Event{Type: api.StepCreated, Run: run, Step: step, Payload: b}
}

func stepStarted(run, step string, attempt int) api.Event {
	// TS must be non-zero: Apply folds it into StepState.Started, and
	// Running() means "Started is non-zero and not yet terminal", so a zero
	// TS would silently make every in-flight step not count as running.
	return api.Event{Type: api.StepStarted, Run: run, Step: step, Attempt: attempt, TS: time.Now()}
}

func stepFinished(run, step string, attempt int, state api.State) api.Event {
	b, _ := json.Marshal(api.StepFinishedBody{State: state})
	return api.Event{Type: api.StepFinished, Run: run, Step: step, Attempt: attempt, Payload: b}
}

func planExpanded(run, parent string, children []string) api.Event {
	b, _ := json.Marshal(api.PlanExpandedBody{Parent: parent, Children: children, Count: len(children)})
	return api.Event{Type: api.PlanExpanded, Run: run, Step: parent, Payload: b}
}

func logAppended(run, step, stream string, offset, length int64) api.Event {
	b, _ := json.Marshal(api.StepLogAppendedBody{Stream: stream, Offset: offset, Len: length})
	return api.Event{Type: api.StepLogAppended, Run: run, Step: step, Payload: b}
}

// update is a small test helper that applies one message and returns the
// concrete *tui.Model, so a test can chain m = update(t, m, msg) without a
// type assertion at every call site.
func update(t *testing.T, m *tui.Model, msg tea.Msg) *tui.Model {
	t.Helper()
	next, cmd := m.Update(msg)
	nm, ok := next.(*tui.Model)
	if !ok {
		t.Fatalf("Update returned %T, want *tui.Model", next)
	}
	_ = cmd
	return nm
}

// run invokes cmd and feeds whatever Msg it returns back into m via
// Update: the standard way to drive an async bubbletea Cmd with no running
// Program and no pty. A tea.Batch produces a tea.BatchMsg rather than one
// Msg, which a real Program unpacks by running each sub-command; this does
// the same, recursively, so a test need not know whether Update batched.
func run(t *testing.T, m *tui.Model, cmd tea.Cmd) *tui.Model {
	t.Helper()
	if cmd == nil {
		return m
	}
	return applyMsg(t, m, cmd())
}

func applyMsg(t *testing.T, m *tui.Model, msg tea.Msg) *tui.Model {
	t.Helper()
	if msg == nil {
		return m
	}
	if batch, ok := msg.(tea.BatchMsg); ok {
		for _, c := range batch {
			if c == nil {
				continue
			}
			m = applyMsg(t, m, c())
		}
		return m
	}
	return update(t, m, msg)
}

// tickAndFetchOnly drives handleTick's Cmd the way a real bubbletea
// Program would, with tea.Batch's sub-commands CONCURRENT, rather than
// run/applyMsg's sequential replay. handleTick is the one Cmd here that
// batches a real ~33ms OS timer alongside a fast local file read, and
// resolving them sequentially serializes on the timer for no reason: each
// iteration then needs two scheduling opportunities instead of one, which
// made the one caller flaky under CPU contention. Only this test needs it.
func tickAndFetchOnly(t *testing.T, m *tui.Model, tcmd tea.Cmd) *tui.Model {
	t.Helper()
	if tcmd == nil {
		return m
	}

	// tcmd() itself can BE tea.Tick's blocking wait: when
	// followFocusedLogsCmd returns nil, compactCmds collapses handleTick's
	// Batch to the bare tick command. So even calling tcmd() has to happen
	// off this goroutine, not just the Msg it produces.
	msgCh := make(chan tea.Msg, 1)
	go func() { msgCh <- tcmd() }()

	select {
	case msg := <-msgCh:
		return applyTickBatchDiscardingReschedule(t, m, msg)
	case <-time.After(tickFetchWait):
		// Nothing resolved quickly: most likely the bare tick command
		// itself, still waiting on its own timer in the background,
		// abandoned here since nothing reads its result. The caller's own
		// outer loop will simply try again.
		return m
	}
}

// tickFetchWait bounds how long tickAndFetchOnly waits for a USEFUL
// (non-reschedule) result before giving up on this one call: generous
// next to followFocusedLogsCmd's own cost (a local file open+seek+read,
// normally sub-millisecond), short next to tea.Tick's ~33ms timer, which
// this helper never needs to wait out at all.
const tickFetchWait = 200 * time.Millisecond

// applyTickBatchDiscardingReschedule resolves msg, either a single Msg
// (the bare-tick-command case) or a tea.BatchMsg (tick plus a real fetch),
// concurrently, applying the first non-tick result and discarding a
// tui.TickMsg wherever it shows up.
func applyTickBatchDiscardingReschedule(t *testing.T, m *tui.Model, msg tea.Msg) *tui.Model {
	t.Helper()
	batch, ok := msg.(tea.BatchMsg)
	if !ok {
		if _, isTick := msg.(tui.TickMsg); isTick {
			return m
		}
		return applyMsg(t, m, msg)
	}

	result := make(chan tea.Msg, len(batch))
	for _, c := range batch {
		if c == nil {
			continue
		}
		go func(c tea.Cmd) { result <- c() }(c)
	}

	timeout := time.After(tickFetchWait)
	for {
		select {
		case r := <-result:
			if _, isTick := r.(tui.TickMsg); isTick {
				continue // never what a caller of tickAndFetchOnly wants
			}
			return applyMsg(t, m, r)
		case <-timeout:
			return m
		}
	}
}

func seedState(run string) *api.RunState {
	st := api.NewRunState()
	st.Run.ID = run
	return st
}

// ---- Step 1: the model folds events into a RunState, and its view lists every step ----

func TestModelViewListsEveryStep(t *testing.T) {
	src := &fakeSource{state: seedState("r1")}
	m := tui.New(src)

	m = update(t, m, tui.StateMsg{State: seedState("r1")})
	m = update(t, m, tui.EventMsg{Event: stepCreated("r1", "setup")})
	m = update(t, m, tui.EventMsg{Event: stepCreated("r1", "build")})
	m = update(t, m, tui.EventMsg{Event: stepStarted("r1", "setup", 1)})
	m = update(t, m, tui.EventMsg{Event: stepFinished("r1", "setup", 1, api.StateSucceeded)})

	out := plain(m.View())
	for _, want := range []string{"setup", "build"} {
		if !strings.Contains(out, want) {
			t.Errorf("View() missing step %q:\n%s", want, out)
		}
	}
}

// TestModelRunStatusReflectsTheFoldedRunFinished is what lets a caller of
// Run (cmd/senro, in particular) learn the run's rolled-up outcome (for an
// exit code) after the bubbletea program has exited, without a second
// State() round trip against a source Run already closed on its own way
// out. Zero before run.finished is folded, matching RunInfo.Status's own
// documented zero value.
func TestModelRunStatusReflectsTheFoldedRunFinished(t *testing.T) {
	src := &fakeSource{state: seedState("r1")}
	m := tui.New(src)
	m = update(t, m, tui.StateMsg{State: seedState("r1")})

	if got := m.RunStatus(); got != "" {
		t.Errorf("RunStatus() before run.finished = %q, want empty", got)
	}

	m = update(t, m, tui.EventMsg{Event: api.Event{
		Type: api.RunFinished, Run: "r1",
		Payload: mustJSON(api.RunFinishedBody{Status: api.RunFailed}),
	}})

	if got, want := m.RunStatus(), api.RunFailed; got != want {
		t.Errorf("RunStatus() = %q, want %q", got, want)
	}
}

// A single-line mutation that skipped Steps whose State is empty (e.g. "if
// s.State == \"\" { continue }") would still pass ViewListsEverySteps above
// if it happened to leave "build" out. No: build has no state either. This
// test isolates that exact case: a created-but-never-started step must still
// appear, or a plan with 300 nodes and only 4 running would show 4 rows.
func TestModelViewListsAStepThatHasNotStartedYet(t *testing.T) {
	src := &fakeSource{state: seedState("r1")}
	m := tui.New(src)
	m = update(t, m, tui.StateMsg{State: seedState("r1")})
	m = update(t, m, tui.EventMsg{Event: stepCreated("r1", "queued")})

	out := plain(m.View())
	if !strings.Contains(out, "queued") {
		t.Errorf("View() missing an unstarted step %q:\n%s", "queued", out)
	}
}

// ---- expansion groups render collapsed, with counts ----

func TestModelViewCollapsesAnExpansionGroupWithCounts(t *testing.T) {
	src := &fakeSource{state: seedState("r1")}
	m := tui.New(src)
	m = update(t, m, tui.StateMsg{State: seedState("r1")})

	children := []string{"svc[a]", "svc[b]", "svc[c]", "svc[d]", "svc[e]"}
	m = update(t, m, tui.EventMsg{Event: planExpanded("r1", "svc", children)})
	m = update(t, m, tui.EventMsg{Event: stepFinished("r1", "svc[a]", 1, api.StateFailed)})
	m = update(t, m, tui.EventMsg{Event: stepFinished("r1", "svc[b]", 1, api.StateCached)})
	m = update(t, m, tui.EventMsg{Event: stepStarted("r1", "svc[c]", 1)})

	out := plain(m.View())

	// The group's counts line: units, failed, cached, running.
	for _, want := range []string{"5 units", "1 failed", "1 cached", "1 running"} {
		if !strings.Contains(out, want) {
			t.Errorf("View() missing group summary %q:\n%s", want, out)
		}
	}
	// Collapsed means collapsed: none of the five children get their own
	// top-level row. A mutation that rendered every step in Order
	// unconditionally (ignoring Group) would pass every test above but fail
	// here, since it would print "svc[a]" as its own line too.
	for _, child := range children {
		if strings.Contains(out, child) {
			t.Errorf("View() must not list expansion child %q as its own row (it must render collapsed):\n%s", child, out)
		}
	}
}

// ---- the focused step's log pane shows that step's content ----

func TestModelFocusedStepLogPaneShowsItsContent(t *testing.T) {
	src := &fakeSource{
		state: seedState("r1"),
		logs:  map[string]map[int][]byte{"build": {1: []byte("compiling\nlinking\n")}},
	}
	m := tui.New(src)
	m = update(t, m, tui.StateMsg{State: seedState("r1")})
	m = update(t, m, tui.EventMsg{Event: stepCreated("r1", "build")})
	m = update(t, m, tui.EventMsg{Event: stepStarted("r1", "build", 1)})

	// Cursor starts on the first (only) row; enter focuses it and issues the
	// log fetch.
	next, cmd := m.Update(tea.KeyMsg{Type: tea.KeyEnter})
	m = next.(*tui.Model)
	m = run(t, m, cmd)

	out := plain(m.View())
	if !strings.Contains(out, "compiling") || !strings.Contains(out, "linking") {
		t.Errorf("View() missing focused step's log content:\n%s", out)
	}
}

// A log pane that never focused anything must say so, not silently show a
// blank pane indistinguishable from "focused but no output yet."
func TestModelLogPaneBeforeAnyFocusIsNotBlank(t *testing.T) {
	src := &fakeSource{state: seedState("r1")}
	m := tui.New(src)
	m = update(t, m, tui.StateMsg{State: seedState("r1")})
	m = update(t, m, tui.EventMsg{Event: stepCreated("r1", "build")})

	out := strings.TrimSpace(plain(m.View()))
	if out == "" {
		t.Fatal("View() is blank before any step is focused")
	}
}

// A tick must fetch new log bytes once the fold's high-water mark advances
// past what was fetched, and must NOT re-fetch when nothing changed. Both
// halves matter: never re-fetching leaves a streaming pane stale, and
// re-fetching unconditionally hammers Source.Logs at ~30Hz, exactly the
// per-tick I/O the coalescing design exists to prevent.
func TestModelTickFollowsFocusedStepLogsOnlyWhenTheWatermarkAdvances(t *testing.T) {
	src := &fakeSource{state: seedState("r1")}
	src.setLogs("build", 1, []byte("first\n"))
	m := tui.New(src)
	m = update(t, m, tui.StateMsg{State: seedState("r1")})
	m = update(t, m, tui.EventMsg{Event: stepCreated("r1", "build")})
	m = update(t, m, tui.EventMsg{Event: stepStarted("r1", "build", 1)})

	next, cmd := m.Update(tea.KeyMsg{Type: tea.KeyEnter}) // focus "build"
	m = next.(*tui.Model)
	m = run(t, m, cmd)

	if got := plain(m.View()); !strings.Contains(got, "first") {
		t.Fatalf("View() after initial focus missing %q:\n%s", "first", got)
	}
	if strings.Contains(plain(m.View()), "second") {
		t.Fatalf("View() shows content that has not been fetched yet")
	}
	callsAfterFocus := src.logsCalls()

	// A tick with no new watermark must not issue a redundant fetch.
	next, cmd = m.Update(tui.TickMsg(time.Now()))
	m = next.(*tui.Model)
	m = run(t, m, cmd)
	if got := src.logsCalls(); got != callsAfterFocus {
		t.Errorf("Logs called %d times after an idle tick, want %d (no new bytes, no fetch)", got, callsAfterFocus)
	}

	// The engine writes more output and announces it; the fake's own
	// content grows to match what the marker claims.
	src.setLogs("build", 1, []byte("first\nsecond\n"))
	m = update(t, m, tui.EventMsg{Event: logAppended("r1", "build", api.StreamStdout, 6, 7)})

	next, cmd = m.Update(tui.TickMsg(time.Now()))
	m = next.(*tui.Model)
	m = run(t, m, cmd)

	if got := src.logsCalls(); got != callsAfterFocus+1 {
		t.Errorf("Logs called %d times after the watermark advanced, want %d (exactly one follow-up fetch)", got, callsAfterFocus+1)
	}
	out := plain(m.View())
	if !strings.Contains(out, "first") || !strings.Contains(out, "second") {
		t.Errorf("View() after the follow-up fetch missing new content:\n%s", out)
	}
}

// Any async fetch that advances state, issued on a repeating tick, can
// overlap itself: bubbletea runs every Cmd as its own goroutine, and
// logOffset only advances when a result is APPLIED, so two ticks can both
// fetch the identical stale offset and applying both duplicates content.
// The guard is Model.beginFetch/endFetch, claimed inside fetchLogsCmd so
// it covers every caller rather than one call site.
func TestModelNeverIssuesTwoOverlappingTailFetchesForTheSameStep(t *testing.T) {
	src := &fakeSource{state: seedState("r1")}
	src.setLogs("build", 1, []byte("aaa\n"))
	m := tui.New(src)
	m = update(t, m, tui.StateMsg{State: seedState("r1")})
	m = update(t, m, tui.EventMsg{Event: stepCreated("r1", "build")})
	m = update(t, m, tui.EventMsg{Event: stepStarted("r1", "build", 1)})
	m = update(t, m, tui.EventMsg{Event: logAppended("r1", "build", api.StreamStdout, 0, 4)})

	next, cmd := m.Update(tea.KeyMsg{Type: tea.KeyEnter})
	m = next.(*tui.Model)
	m = run(t, m, cmd) // "aaa" shown, logOffset == 4

	// More output arrives.
	src.setLogs("build", 1, []byte("aaa\nbbb\n"))
	m = update(t, m, tui.EventMsg{Event: logAppended("r1", "build", api.StreamStdout, 4, 4)})

	// Two ticks fire back to back. Neither one's fetch resolved in
	// between. Without the guard both would see the identical stale
	// watermark (logOffset is still 4; it only moves once a fetch's
	// result is applied) and both would issue a fetch from offset 4.
	next, tick1 := m.Update(tui.TickMsg(time.Now()))
	m = next.(*tui.Model)
	next, tick2 := m.Update(tui.TickMsg(time.Now()))
	m = next.(*tui.Model)

	m = run(t, m, tick1)
	m = run(t, m, tick2)

	out := plain(m.View())
	if got := strings.Count(out, "bbb"); got != 1 {
		t.Errorf("View() shows %q %d times, want exactly 1 — two overlapping ticks each fetched "+
			"the same bytes and both were applied:\n%s", "bbb", got, out)
	}
}

// ---- the log pane follows a retried step onto its new attempt ----
//
// A retry starts a new attempt with its own log file at byte 0, not a
// continuation. Without a reset, logOffset stays seeked into the dead
// attempt's file, so even re-focusing shows the old content or seeks past
// the new file's end and shows nothing. This walks that exact sequence and
// requires attempt 2's content.
func TestModelLogPaneFollowsARetriedStepsNewAttempt(t *testing.T) {
	src := &fakeSource{state: seedState("r1")}
	src.setLogs("build", 1, []byte("attempt one output\n"))
	m := tui.New(src)
	m = update(t, m, tui.StateMsg{State: seedState("r1")})
	m = update(t, m, tui.EventMsg{Event: stepCreated("r1", "build")})
	m = update(t, m, tui.EventMsg{Event: stepStarted("r1", "build", 1)})
	m = update(t, m, tui.EventMsg{Event: logAppended("r1", "build", api.StreamStdout, 0, int64(len("attempt one output\n")))})

	next, cmd := m.Update(tea.KeyMsg{Type: tea.KeyEnter}) // focus "build" on attempt 1
	m = next.(*tui.Model)
	m = run(t, m, cmd)

	out := plain(m.View())
	if !strings.Contains(out, "attempt one output") {
		t.Fatalf("View() after focusing attempt 1 missing its content:\n%s", out)
	}

	m = update(t, m, tui.EventMsg{Event: stepFinished("r1", "build", 1, api.StateFailed)})

	// The retry: a fresh attempt, its own (shorter, unrelated) log file
	// starting at byte 0.
	src.setLogs("build", 2, []byte("attempt two\n"))
	b, _ := json.Marshal(api.StepRetriedBody{Attempt: 2})
	m = update(t, m, tui.EventMsg{Event: api.Event{
		Type: api.StepRetried, Run: "r1", Step: "build", Attempt: 2, Payload: b,
	}})

	// Immediately after the retry, before the new attempt has logged
	// anything, the stale content must be gone rather than lingering:
	// showing attempt 1's output at this point would be actively
	// misleading, not merely stale.
	out = plain(m.View())
	if strings.Contains(out, "attempt one output") {
		t.Errorf("View() still shows the retried-away attempt's content immediately after step.retried:\n%s", out)
	}

	m = update(t, m, tui.EventMsg{Event: stepStarted("r1", "build", 2)})
	m = update(t, m, tui.EventMsg{Event: logAppended("r1", "build", api.StreamStdout, 0, int64(len("attempt two\n")))})

	// The tick-driven follow (see followFocusedLogsCmd) is what notices the
	// new attempt has output and fetches it, no fresh 'enter' needed.
	next, cmd = m.Update(tui.TickMsg(time.Now()))
	m = next.(*tui.Model)
	m = run(t, m, cmd)

	out = plain(m.View())
	if !strings.Contains(out, "attempt two") {
		t.Errorf("View() after the retry and a tick does not show the new attempt's content:\n%s", out)
	}
	if strings.Contains(out, "attempt one output") {
		t.Errorf("View() after the retry still shows the dead attempt's content:\n%s", out)
	}

	// The fetch that produced this must have actually targeted attempt 2's
	// file, not attempt 1's: a fetch can land at a stale OFFSET against the
	// right (or wrong) file, so the offset matters as much as the attempt
	// number.
	calls := src.logsCallsFor("build")
	last := calls[len(calls)-1]
	if last.attempt != 2 {
		t.Errorf("last Logs call was for attempt %d, want 2: %+v", last.attempt, calls)
	}
	if last.from != 0 {
		t.Errorf("last Logs call was from offset %d, want 0 — attempt 2's file starts at byte 0, "+
			"and a stale offset from attempt 1 seeks into it wrong", last.from)
	}
}

// Re-pressing 'enter' after a retry must ALSO show the new attempt: without
// a reset, focusCursor would reuse the same stale offset. Tested separately
// from the tick-driven case above because it exercises a different code
// path (focusCursor's own fetch, not followFocusedLogsCmd's).
func TestModelReFocusingARetriedStepShowsTheNewAttempt(t *testing.T) {
	src := &fakeSource{state: seedState("r1")}
	src.setLogs("build", 1, []byte("attempt one, quite a bit longer than attempt two\n"))
	m := tui.New(src)
	m = update(t, m, tui.StateMsg{State: seedState("r1")})
	m = update(t, m, tui.EventMsg{Event: stepCreated("r1", "build")})
	m = update(t, m, tui.EventMsg{Event: stepStarted("r1", "build", 1)})
	m = update(t, m, tui.EventMsg{Event: logAppended("r1", "build", api.StreamStdout, 0,
		int64(len("attempt one, quite a bit longer than attempt two\n")))})

	next, cmd := m.Update(tea.KeyMsg{Type: tea.KeyEnter})
	m = next.(*tui.Model)
	m = run(t, m, cmd)
	m = update(t, m, tui.EventMsg{Event: stepFinished("r1", "build", 1, api.StateFailed)})

	src.setLogs("build", 2, []byte("short\n")) // deliberately SHORTER than attempt 1's offset
	b, _ := json.Marshal(api.StepRetriedBody{Attempt: 2})
	m = update(t, m, tui.EventMsg{Event: api.Event{
		Type: api.StepRetried, Run: "r1", Step: "build", Attempt: 2, Payload: b,
	}})
	m = update(t, m, tui.EventMsg{Event: stepStarted("r1", "build", 2)})
	m = update(t, m, tui.EventMsg{Event: logAppended("r1", "build", api.StreamStdout, 0, int64(len("short\n")))})

	// Re-focus via 'enter' again, WITHOUT any intervening tick.
	next, cmd = m.Update(tea.KeyMsg{Type: tea.KeyEnter})
	m = next.(*tui.Model)
	m = run(t, m, cmd)

	out := plain(m.View())
	if !strings.Contains(out, "short") {
		t.Errorf("re-focusing after a retry does not show the new attempt's content:\n%s", out)
	}
	if strings.Contains(out, "attempt one") {
		t.Errorf("re-focusing after a retry still shows the dead attempt's content:\n%s", out)
	}
}

// The narrower race handleLogChunk guards: a fetch already IN FLIGHT for
// the old attempt, resolving AFTER the retry's reset ran. Exercised by
// holding the fetch Cmd unresolved across the retry. Removing the Attempt
// guard passes every test above but fails this one.
func TestModelDropsALogFetchThatResolvesAfterARetrySupersededIt(t *testing.T) {
	src := &fakeSource{state: seedState("r1")}
	src.setLogs("build", 1, []byte("attempt one output\n"))
	m := tui.New(src)
	m = update(t, m, tui.StateMsg{State: seedState("r1")})
	m = update(t, m, tui.EventMsg{Event: stepCreated("r1", "build")})
	m = update(t, m, tui.EventMsg{Event: stepStarted("r1", "build", 1)})

	// Focus issues the attempt-1 fetch; deliberately do NOT run it yet.
	next, cmd := m.Update(tea.KeyMsg{Type: tea.KeyEnter})
	m = next.(*tui.Model)
	if cmd == nil {
		t.Fatal("focusing did not issue a fetch Cmd")
	}

	m = update(t, m, tui.EventMsg{Event: stepFinished("r1", "build", 1, api.StateFailed)})
	b, _ := json.Marshal(api.StepRetriedBody{Attempt: 2})
	m = update(t, m, tui.EventMsg{Event: api.Event{
		Type: api.StepRetried, Run: "r1", Step: "build", Attempt: 2, Payload: b,
	}})

	// The stale, attempt-1 fetch resolves only now, after the reset.
	m = run(t, m, cmd)

	out := plain(m.View())
	if strings.Contains(out, "attempt one output") {
		t.Errorf("View() applied a log fetch that resolved for an attempt the retry already superseded:\n%s", out)
	}
}

// ---- scrollback beyond the ring, served by range request ('pgup') ----
//
// The ring on its own only ever keeps the tail; scrollback beyond it is
// served by a range request against the log file. These tests are what
// prove that other half (a path back to whatever the ring dropped)
// actually exists.

func TestModelPgUpLoadsOlderLogHistoryPastTheRingsCap(t *testing.T) {
	var buf strings.Builder
	const total = 2500 // comfortably past defaultLogRingCap (2000)
	for i := range total {
		fmt.Fprintf(&buf, "line%04d\n", i)
	}

	src := &fakeSource{state: seedState("r1")}
	src.setLogs("build", 1, []byte(buf.String()))
	m := tui.New(src)
	m = update(t, m, tui.StateMsg{State: seedState("r1")})
	m = update(t, m, tui.EventMsg{Event: stepCreated("r1", "build")})
	m = update(t, m, tui.EventMsg{Event: stepStarted("r1", "build", 1)})

	next, cmd := m.Update(tea.KeyMsg{Type: tea.KeyEnter})
	m = next.(*tui.Model)
	m = run(t, m, cmd)

	out := plain(m.View())
	if strings.Contains(out, "line0000") {
		t.Fatalf("precondition: the ring should have trimmed the earliest lines past its cap:\n%s", out)
	}
	if !strings.Contains(out, "line2499") {
		t.Fatalf("precondition: the most recent line must still be visible:\n%s", out)
	}
	if !strings.Contains(out, "more history above") {
		t.Errorf("View() does not hint that scrollback is available:\n%s", out)
	}

	next, cmd = m.Update(tea.KeyMsg{Type: tea.KeyPgUp})
	m = next.(*tui.Model)
	m = run(t, m, cmd)

	out = plain(m.View())
	if !strings.Contains(out, "line0000") {
		t.Errorf("View() after pgup does not show the file's true first line:\n%s", out)
	}
	if strings.Contains(out, "more history above") {
		t.Error("View() still hints at more history after pgup already fetched back to the true start of the file")
	}
}

// A 'pgup' with nothing earlier to fetch (nothing was ever trimmed) must
// not silently do nothing: a key press must say what happened, the same
// requirement r and c are held to.
func TestModelPgUpWithNoMoreHistoryIsANoOpThatSaysSo(t *testing.T) {
	src := &fakeSource{state: seedState("r1")}
	src.setLogs("build", 1, []byte("only one line\n"))
	m := tui.New(src)
	m = update(t, m, tui.StateMsg{State: seedState("r1")})
	m = update(t, m, tui.EventMsg{Event: stepCreated("r1", "build")})

	next, cmd := m.Update(tea.KeyMsg{Type: tea.KeyEnter})
	m = next.(*tui.Model)
	m = run(t, m, cmd)

	callsBefore := src.logsCalls()
	next, cmd = m.Update(tea.KeyMsg{Type: tea.KeyPgUp})
	m = next.(*tui.Model)
	m = run(t, m, cmd)

	if got := src.logsCalls(); got != callsBefore {
		t.Errorf("Logs called again on pgup with nothing earlier to fetch: %d calls, want %d (no-op)", got, callsBefore)
	}
	out := plain(m.View())
	if !strings.Contains(out, "no earlier log history") {
		t.Errorf("View() does not explain why pgup did nothing:\n%s", out)
	}
}

// A 'pgup' before anything is even focused must not panic or issue a
// request against an empty step name.
func TestModelPgUpWithNothingFocusedIsANoOp(t *testing.T) {
	src := &fakeSource{state: seedState("r1")}
	m := tui.New(src)
	m = update(t, m, tui.StateMsg{State: seedState("r1")})

	next, cmd := m.Update(tea.KeyMsg{Type: tea.KeyPgUp})
	m = next.(*tui.Model)
	m = run(t, m, cmd)

	if got := src.logsCalls(); got != 0 {
		t.Errorf("Logs called %d times with nothing focused, want 0", got)
	}
	_ = m.View() // must not panic
}

// The scrollback half of the overlap proof its tail-fetch sibling gives:
// two 'pgup' presses before the first resolves must issue exactly one
// fetch. Both paths claim a slot through the same beginFetch/endFetch.
func TestModelNeverIssuesTwoOverlappingScrollbackFetchesForTheSameStep(t *testing.T) {
	var buf strings.Builder
	for i := range 2500 {
		fmt.Fprintf(&buf, "line%04d\n", i)
	}
	src := &fakeSource{state: seedState("r1")}
	src.setLogs("build", 1, []byte(buf.String()))
	m := tui.New(src)
	m = update(t, m, tui.StateMsg{State: seedState("r1")})
	m = update(t, m, tui.EventMsg{Event: stepCreated("r1", "build")})
	m = update(t, m, tui.EventMsg{Event: stepStarted("r1", "build", 1)})

	next, cmd := m.Update(tea.KeyMsg{Type: tea.KeyEnter})
	m = next.(*tui.Model)
	m = run(t, m, cmd)

	callsBefore := src.logsCalls()

	next, pgup1 := m.Update(tea.KeyMsg{Type: tea.KeyPgUp})
	m = next.(*tui.Model)
	if pgup1 == nil {
		t.Fatal("the first pgup did not issue a fetch")
	}
	// A second pgup fires before the first one's result has been applied;
	// the actual shape of the race: nothing in Update ever blocks waiting
	// for a Cmd to resolve.
	next, pgup2 := m.Update(tea.KeyMsg{Type: tea.KeyPgUp})
	m = next.(*tui.Model)
	if pgup2 != nil {
		t.Error("a second pgup issued a fetch while the first was still in flight")
	}

	run(t, m, pgup1)
	if got := src.logsCalls() - callsBefore; got != 1 {
		t.Errorf("Logs called %d times across two overlapping pgup presses, want exactly 1", got)
	}
}

// loadOlderLogsCmd captures StartOffset() as the fetch's boundary before
// the round trip; a live tail that advances and trims meanwhile makes it
// stale. Applying it anyway splices a range that no longer abuts the
// ring's front, corrupting StartOffset and misdirecting a later pgup, so
// handleLogHistory re-validates at apply time.
//
// Deliberately not covered by the in-flight guard: a tail fetch and a
// history fetch are independent operations claiming separate slots, and
// the guard stops a fetch racing ITSELF, not being invalidated by a
// different kind of fetch.
func TestModelDropsAScrollbackFetchWhoseBoundaryMovedUnderIt(t *testing.T) {
	build := func(n int) string {
		var b strings.Builder
		for i := range n {
			fmt.Fprintf(&b, "line%04d\n", i)
		}
		return b.String()
	}

	src := &fakeSource{state: seedState("r1")}
	src.setLogs("build", 1, []byte(build(2100))) // 100 lines over the 2000 cap
	m := tui.New(src)
	m = update(t, m, tui.StateMsg{State: seedState("r1")})
	m = update(t, m, tui.EventMsg{Event: stepCreated("r1", "build")})
	m = update(t, m, tui.EventMsg{Event: stepStarted("r1", "build", 1)})
	m = update(t, m, tui.EventMsg{Event: logAppended("r1", "build", api.StreamStdout, 0, int64(len(build(2100))))})

	next, cmd := m.Update(tea.KeyMsg{Type: tea.KeyEnter})
	m = next.(*tui.Model)
	m = run(t, m, cmd)

	// Issue the pgup fetch, but hold it "in flight": do not resolve it yet.
	next, pgupCmd := m.Update(tea.KeyMsg{Type: tea.KeyPgUp})
	m = next.(*tui.Model)
	if pgupCmd == nil {
		t.Fatal("pgup with earlier history available did not issue a fetch")
	}

	// While that fetch is outstanding, the live tail grows and the ring
	// trims further, genuinely independent of the pgup fetch (separate
	// guard slot), moving StartOffset() out from under the boundary the
	// pgup fetch already captured.
	src.setLogs("build", 1, []byte(build(2600)))
	m = update(t, m, tui.EventMsg{Event: logAppended("r1", "build", api.StreamStdout,
		int64(len(build(2100))), int64(len(build(2600))-len(build(2100))))})
	next, tickCmd := m.Update(tui.TickMsg(time.Now()))
	m = next.(*tui.Model)
	m = run(t, m, tickCmd)

	if out := plain(m.View()); !strings.Contains(out, "line2599") {
		t.Fatalf("precondition: the live tail growth did not apply:\n%s", out)
	}

	// NOW resolve the stale pgup fetch, whose captured boundary no longer
	// matches the ring's current StartOffset().
	m = run(t, m, pgupCmd)

	out := plain(m.View())
	// "line0000" is exactly what the STALE fetch targeted (it fetched back
	// toward the true start of the file, from the boundary captured before
	// the tail's growth). If it were applied anyway, it would show up
	// spliced in front of whatever the ring's now-later front is.
	if strings.Contains(out, "line0000") {
		t.Errorf("View() applied a scrollback fetch whose boundary had moved (a stale-boundary splice):\n%s", out)
	}
	if !strings.Contains(out, "moved while fetching") {
		t.Errorf("View() does not explain that the stale fetch was dropped because history moved:\n%s", out)
	}
}

// ---- 'r' sends a retry for the focused step ----

func TestModelRSendsRetryForTheFocusedStep(t *testing.T) {
	src := &fakeSource{state: seedState("r1"), controlResp: okResp()}
	m := tui.New(src)
	m = update(t, m, tui.StateMsg{State: seedState("r1")})
	m = update(t, m, tui.EventMsg{Event: stepCreated("r1", "build")})
	m = update(t, m, tui.EventMsg{Event: stepFinished("r1", "build", 1, api.StateFailed)})

	next, cmd := m.Update(tea.KeyMsg{Type: tea.KeyEnter}) // focus "build"
	m = next.(*tui.Model)
	m = run(t, m, cmd)

	next, cmd = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("r")})
	m = next.(*tui.Model)
	run(t, m, cmd)

	reqs := src.requests()
	if len(reqs) != 1 {
		t.Fatalf("Control called %d times, want 1: %+v", len(reqs), reqs)
	}
	if reqs[0].Type != api.OpStepRetry {
		t.Errorf("Type = %q, want %q", reqs[0].Type, api.OpStepRetry)
	}
	var args map[string]string
	if err := json.Unmarshal(reqs[0].Payload, &args); err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	if args["step"] != "build" {
		t.Errorf("payload step = %q, want %q", args["step"], "build")
	}
}

// 'r' with nothing focused must not silently do nothing: a key press that
// appears to succeed but changes nothing is exactly the bug to avoid.
func TestModelRWithNoFocusDoesNotSendAControlRequestAndSaysWhy(t *testing.T) {
	src := &fakeSource{state: seedState("r1")}
	m := tui.New(src)
	m = update(t, m, tui.StateMsg{State: seedState("r1")})
	m = update(t, m, tui.EventMsg{Event: stepCreated("r1", "build")})

	next, cmd := m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("r")})
	m = next.(*tui.Model)
	m = run(t, m, cmd)

	if len(src.requests()) != 0 {
		t.Fatalf("Control called with nothing focused: %+v", src.requests())
	}
	if strings.TrimSpace(plain(m.View())) == "" {
		t.Fatal("View() blank after a no-op 'r'; must explain nothing happened")
	}
}

// ---- 'c' sends a cancel ----

func TestModelCSendsRunCancel(t *testing.T) {
	src := &fakeSource{state: seedState("r1"), controlResp: okResp()}
	m := tui.New(src)
	m = update(t, m, tui.StateMsg{State: seedState("r1")})

	next, cmd := m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("c")})
	m = next.(*tui.Model)
	run(t, m, cmd)

	reqs := src.requests()
	if len(reqs) != 1 || reqs[0].Type != api.OpRunCancel {
		t.Fatalf("requests = %+v, want exactly one run.cancel", reqs)
	}
}

// Ctrl-C cancels the run; it must never be a silent quit-and-abandon.
func TestModelCtrlCSendsRunCancelNotQuit(t *testing.T) {
	src := &fakeSource{state: seedState("r1"), controlResp: okResp()}
	m := tui.New(src)
	m = update(t, m, tui.StateMsg{State: seedState("r1")})

	next, cmd := m.Update(tea.KeyMsg{Type: tea.KeyCtrlC})
	m = next.(*tui.Model)
	if src.closed {
		t.Fatal("ctrl+c must cancel the run, not detach — Close was called")
	}
	run(t, m, cmd)

	reqs := src.requests()
	if len(reqs) != 1 || reqs[0].Type != api.OpRunCancel {
		t.Fatalf("requests = %+v, want exactly one run.cancel", reqs)
	}
}

// ---- 'q' detaches without cancelling ----

func TestModelQDetachesWithoutSendingAnyControlRequest(t *testing.T) {
	src := &fakeSource{state: seedState("r1"), controlResp: okResp()}
	m := tui.New(src)
	m = update(t, m, tui.StateMsg{State: seedState("r1")})

	next, cmd := m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("q")})
	m = next.(*tui.Model)
	if cmd == nil {
		t.Fatal("'q' must return tea.Quit")
	}
	msg := cmd()
	if _, ok := msg.(tea.QuitMsg); !ok {
		t.Errorf("'q' returned %T, want tea.QuitMsg — quitting the TUI must not silently continue running something else instead", msg)
	}
	if len(src.requests()) != 0 {
		t.Fatalf("'q' sent a control request: %+v — quitting a UI must never be a way to cancel a run", src.requests())
	}
	if !src.closed {
		t.Error("'q' did not release the source (Close was not called)")
	}
	_ = m
}

// ---- '/' filter and '?' help ----

// '/' narrows the step list to IDs containing the typed text; enter
// commits it. The cursor must be bounded by the FILTERED list, not the
// full one: 'enter' after filtering has to focus what is actually on
// screen, never a step the operator cannot see.
func TestModelSlashFiltersTheStepListBySubstring(t *testing.T) {
	src := &fakeSource{state: seedState("r1")}
	m := tui.New(src)
	m = update(t, m, tui.StateMsg{State: seedState("r1")})
	m = update(t, m, tui.EventMsg{Event: stepCreated("r1", "build-api")})
	m = update(t, m, tui.EventMsg{Event: stepCreated("r1", "build-web")})
	m = update(t, m, tui.EventMsg{Event: stepCreated("r1", "deploy")})

	next, cmd := m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("/")})
	m = next.(*tui.Model)
	_ = cmd
	next, cmd = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("build")})
	m = next.(*tui.Model)
	_ = cmd
	next, cmd = m.Update(tea.KeyMsg{Type: tea.KeyEnter})
	m = next.(*tui.Model)
	_ = cmd

	out := plain(m.View())
	if !strings.Contains(out, "build-api") || !strings.Contains(out, "build-web") {
		t.Errorf("View() after filtering to %q missing a matching step:\n%s", "build", out)
	}
	if strings.Contains(out, "deploy") {
		t.Errorf("View() after filtering to %q still shows a non-matching step:\n%s", "build", out)
	}

	// 'enter' now must focus one of the VISIBLE (filtered) steps.
	next, cmd = m.Update(tea.KeyMsg{Type: tea.KeyEnter})
	m = next.(*tui.Model)
	m = run(t, m, cmd)
	out = plain(m.View())
	if !strings.Contains(out, "build-api") {
		t.Errorf("focusing after a filter did not target a visible step:\n%s", out)
	}
}

// Typing while filtering must not ALSO trigger ordinary key commands:
// typing "r" into a filter must not retry anything, or every filter
// containing the letters used by a command key would be a minefield.
func TestModelTypingWhileFilteringDoesNotTriggerCommands(t *testing.T) {
	src := &fakeSource{state: seedState("r1"), controlResp: okResp()}
	m := tui.New(src)
	m = update(t, m, tui.StateMsg{State: seedState("r1")})
	m = update(t, m, tui.EventMsg{Event: stepCreated("r1", "run-tests")})

	next, cmd := m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("/")})
	m = next.(*tui.Model)
	_ = cmd
	for _, r := range "run" {
		next, cmd = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{r}})
		m = next.(*tui.Model)
		_ = cmd
	}
	next, cmd = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("c")}) // would cancel, if not swallowed
	m = next.(*tui.Model)
	m = run(t, m, cmd)

	if len(src.requests()) != 0 {
		t.Fatalf("a key typed while filtering triggered a control request: %+v", src.requests())
	}
	out := plain(m.View())
	if !strings.Contains(out, "filter: runc") {
		t.Errorf("View() does not show the in-progress filter text:\n%s", out)
	}
}

// esc cancels an in-progress filter EDIT without touching whatever filter
// was already active: a cancelled edit is not the same action as
// clearing the filter.
func TestModelEscCancelsAFilterEditWithoutClearingTheActiveFilter(t *testing.T) {
	src := &fakeSource{state: seedState("r1")}
	m := tui.New(src)
	m = update(t, m, tui.StateMsg{State: seedState("r1")})
	m = update(t, m, tui.EventMsg{Event: stepCreated("r1", "build")})
	m = update(t, m, tui.EventMsg{Event: stepCreated("r1", "deploy")})

	// Commit an active filter first.
	next, _ := m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("/")})
	m = next.(*tui.Model)
	next, _ = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("build")})
	m = next.(*tui.Model)
	next, _ = m.Update(tea.KeyMsg{Type: tea.KeyEnter})
	m = next.(*tui.Model)

	// Start editing again, type something else, then esc.
	next, _ = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("/")})
	m = next.(*tui.Model)
	next, _ = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("deploy")})
	m = next.(*tui.Model)
	next, _ = m.Update(tea.KeyMsg{Type: tea.KeyEsc})
	m = next.(*tui.Model)

	out := plain(m.View())
	if !strings.Contains(out, "build") {
		t.Errorf("esc during a filter edit lost the previously active filter:\n%s", out)
	}
	if strings.Contains(out, "deploy") {
		t.Errorf("esc during a filter edit applied the abandoned edit instead of cancelling it:\n%s", out)
	}
}

// '?' shows the key-binding help; the same key (or esc) closes it. While
// it is open, other keys must not fall through to their normal action:
// an operator reading the help screen who accidentally presses 'c' must
// not cancel the run.
func TestModelQuestionMarkTogglesHelpAndSwallowsOtherKeysWhileOpen(t *testing.T) {
	src := &fakeSource{state: seedState("r1"), controlResp: okResp()}
	m := tui.New(src)
	m = update(t, m, tui.StateMsg{State: seedState("r1")})

	next, cmd := m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("?")})
	m = next.(*tui.Model)
	_ = cmd
	out := plain(m.View())
	if !strings.Contains(out, "keys") || !strings.Contains(out, "retry") {
		t.Errorf("View() with help open does not look like a key-binding help screen:\n%s", out)
	}

	next, cmd = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("c")})
	m = next.(*tui.Model)
	m = run(t, m, cmd)
	if len(src.requests()) != 0 {
		t.Fatalf("'c' while help was open sent a control request: %+v", src.requests())
	}

	next, cmd = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("?")})
	m = next.(*tui.Model)
	_ = cmd
	out = plain(m.View())
	if strings.Contains(out, "press ? or esc to close") {
		t.Errorf("'?' a second time did not close the help screen:\n%s", out)
	}
}

// ---- refusals are surfaced, not swallowed ----

func TestModelSurfacesARefusedRetryByReason(t *testing.T) {
	src := &fakeSource{state: seedState("r1"), controlResp: refusedResp("step_not_failed")}
	m := tui.New(src)
	m = update(t, m, tui.StateMsg{State: seedState("r1")})
	m = update(t, m, tui.EventMsg{Event: stepCreated("r1", "build")})

	next, cmd := m.Update(tea.KeyMsg{Type: tea.KeyEnter})
	m = next.(*tui.Model)
	m = run(t, m, cmd)

	next, cmd = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("r")})
	m = next.(*tui.Model)
	m = run(t, m, cmd)

	out := plain(m.View())
	if !strings.Contains(out, "step_not_failed") {
		t.Errorf("View() does not surface the refusal reason %q:\n%s", "step_not_failed", out)
	}
}

func TestModelSurfacesAReadOnlySourceErrorRatherThanAppearingToSucceed(t *testing.T) {
	src := &fakeSource{state: seedState("r1"), controlErr: fmt.Errorf("control: %w", source.ErrReadOnly)}
	m := tui.New(src)
	m = update(t, m, tui.StateMsg{State: seedState("r1")})

	next, cmd := m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("c")})
	m = next.(*tui.Model)
	m = run(t, m, cmd)

	out := plain(m.View())
	if !strings.Contains(out, "read-only") && !strings.Contains(out, source.ErrReadOnly.Error()) {
		t.Errorf("View() does not surface the read-only control failure:\n%s", out)
	}
}

// A FileSource behaves exactly like this fake configured with controlErr =
// source.ErrReadOnly: the model must not crash and must not claim success.
func TestModelAgainstARealFileSourceControlFailsGracefully(t *testing.T) {
	dir := writeFinishedRun(t)
	fs, err := source.OpenFile(dir, false)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	defer func() { _ = fs.Close() }()

	m := tui.New(fs)
	m = run(t, m, m.Init())

	next, cmd := m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("c")})
	m = next.(*tui.Model)
	m = run(t, m, cmd)

	out := plain(m.View())
	if !strings.Contains(out, "read-only") && !strings.Contains(out, source.ErrReadOnly.Error()) {
		t.Errorf("View() does not surface FileSource's read-only refusal:\n%s", out)
	}
}

// ---- a regressed Seq is surfaced, never silently swallowed ----
//
// Apply rejects a regressing Seq on purpose, and every other consumer
// treats that as the stream-ending error it is. Both of the model's fold
// paths must surface it rather than discard it: silently discarding would
// make a lost step.finished (survivable, the log channel is lossy by
// design) and a broken ordering guarantee (not survivable) look identical
// to the operator.

func TestModelSurfacesARegressedSeqViaEventMsg(t *testing.T) {
	src := &fakeSource{state: seedState("r1")}
	m := tui.New(src)
	m = update(t, m, tui.StateMsg{State: seedState("r1")})

	m = update(t, m, tui.EventMsg{Event: api.Event{Seq: 5, Type: api.StepCreated, Run: "r1", Step: "preserved-step",
		Payload: mustJSON(api.StepCreatedBody{Kind: "exec"})}})
	m = update(t, m, tui.EventMsg{Event: api.Event{Seq: 3, Type: api.StepCreated, Run: "r1", Step: "regressed-step",
		Payload: mustJSON(api.StepCreatedBody{Kind: "exec"})}})

	out := plain(m.View())
	if !strings.Contains(out, "out-of-order") {
		t.Errorf("View() does not surface the regressed-seq error:\n%s", out)
	}
}

// The subscription drain treats a regression as fatal to itself, matching
// render.Plain, rather than folding on through a broken ordering
// guarantee. Driven over a REAL channel on its own goroutine, not
// EventMsg, because that is the path a live run's regression takes.
func TestModelSubscriptionStopsAndSurfacesARegressedSeq(t *testing.T) {
	events := make(chan api.Event)
	src := &fakeSource{state: seedState("r1")}
	src.subCh = events

	m := tui.New(src)
	next, cmd := m.Update(tui.StateMsg{State: seedState("r1")})
	m = next.(*tui.Model)

	done := make(chan tea.Msg, 1)
	go func() { done <- cmd() }()

	events <- api.Event{Seq: 5, Type: api.StepCreated, Run: "r1", Step: "preserved-step",
		Payload: mustJSON(api.StepCreatedBody{Kind: "exec"})}
	events <- api.Event{Seq: 3, Type: api.StepCreated, Run: "r1", Step: "regressed-step", // regresses
		Payload: mustJSON(api.StepCreatedBody{Kind: "exec"})}

	var msg tea.Msg
	select {
	case msg = <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("subscription did not stop after a regressed Seq")
	}
	closed, ok := msg.(tui.SubscribeClosedMsg)
	if !ok {
		t.Fatalf("subscribe Cmd produced %T, want tui.SubscribeClosedMsg", msg)
	}
	if closed.Err == nil {
		t.Fatal("SubscribeClosedMsg.Err is nil after a regressed Seq, want the out-of-order error")
	}

	m = update(t, m, closed)
	out := plain(m.View())
	if !strings.Contains(out, "out-of-order") {
		t.Errorf("View() does not surface the regressed-seq error from the subscription:\n%s", out)
	}
	// What DID fold before the regression must not be lost: a regression
	// stops the stream, it does not corrupt or discard what was already
	// correctly applied.
	if !strings.Contains(out, "preserved-step") {
		t.Errorf("View() lost a step that was correctly applied before the regression:\n%s", out)
	}
	if strings.Contains(out, "regressed-step") {
		t.Errorf("View() shows the step from the REJECTED event — Apply must not have partially applied it:\n%s", out)
	}
}

// Stopping the subscription's consumer loop on a regression is not enough
// on its own: whatever is on the other end of
// Subscribe (a file poller, a websocket reader) is left blocked forever
// trying to send the next event to nobody, holding its file handle or
// connection open for the rest of the session: a half-shutdown. The
// whole session must tear down: the Source is released, and the model's
// own ctx is cancelled so every OTHER Cmd this model could still issue
// (retry, cancel, a log fetch) fails too, rather than quietly continuing
// to act on a session whose local fold is already known to be wrong.
func TestModelTearsDownOnARegressedSeqReleasingTheSource(t *testing.T) {
	events := make(chan api.Event)
	src := &fakeSource{state: seedState("r1"), controlResp: okResp()}
	src.subCh = events

	m := tui.New(src)
	next, cmd := m.Update(tui.StateMsg{State: seedState("r1")})
	m = next.(*tui.Model)

	done := make(chan tea.Msg, 1)
	go func() { done <- cmd() }()

	events <- api.Event{Seq: 5, Type: api.StepCreated, Run: "r1", Step: "a",
		Payload: mustJSON(api.StepCreatedBody{Kind: "exec"})}
	events <- api.Event{Seq: 3, Type: api.StepCreated, Run: "r1", Step: "b", // regresses
		Payload: mustJSON(api.StepCreatedBody{Kind: "exec"})}

	var msg tea.Msg
	select {
	case msg = <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("subscription did not stop after a regressed Seq")
	}
	if _, ok := msg.(tui.SubscribeClosedMsg); !ok {
		t.Fatalf("subscribe Cmd produced %T, want tui.SubscribeClosedMsg", msg)
	}

	if !src.closed {
		t.Error("Source was not released after a regressed Seq — its producer goroutine " +
			"(a file poller, an HTTP/websocket reader) is left blocked forever on the channel send")
	}

	// The model's own context must also be cancelled: a further action
	// must fail rather than silently proceeding against a session that has
	// already torn itself down. fakeSource.Control checks ctx.Err() for
	// exactly this reason.
	next, ccmd := m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("c")})
	m = next.(*tui.Model)
	m = run(t, m, ccmd)

	out := plain(m.View())
	if !strings.Contains(out, "context canceled") {
		t.Errorf("View() after the regression does not show that a further action was refused "+
			"(ctx was not cancelled):\n%s", out)
	}
}

// Routing the regression error through m.status instead of the dedicated,
// sticky streamErr field would still pass both regression tests above: they
// only check the error appears once, immediately after the regression. This
// is what actually pins the "sticky" property the doc comments claim: a
// LATER, unrelated status update (an accepted control result, here) must
// not clear it.
func TestModelStreamErrIsStickyAcrossLaterStatusUpdates(t *testing.T) {
	src := &fakeSource{state: seedState("r1"), controlResp: okResp()}
	m := tui.New(src)
	m = update(t, m, tui.StateMsg{State: seedState("r1")})

	m = update(t, m, tui.EventMsg{Event: api.Event{Seq: 5, Type: api.StepCreated, Run: "r1", Step: "a",
		Payload: mustJSON(api.StepCreatedBody{Kind: "exec"})}})
	m = update(t, m, tui.EventMsg{Event: api.Event{Seq: 3, Type: api.StepCreated, Run: "r1", Step: "b", // regresses
		Payload: mustJSON(api.StepCreatedBody{Kind: "exec"})}})

	if out := plain(m.View()); !strings.Contains(out, "out-of-order") {
		t.Fatalf("precondition: regression not surfaced:\n%s", out)
	}

	next, cmd := m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("c")})
	m = next.(*tui.Model)
	m = run(t, m, cmd)

	out := plain(m.View())
	if !strings.Contains(out, "run.cancel accepted") {
		t.Fatalf("precondition: the later status update was not applied:\n%s", out)
	}
	if !strings.Contains(out, "out-of-order") {
		t.Errorf("View() lost the regression error after a later, unrelated status update — "+
			"streamErr must be sticky, not routed through the same field ordinary status uses:\n%s", out)
	}
}

// ---- rendering is coalesced on tick, bounded regardless of event volume ----

// A burst of events must not cost one render per event, or a build emitting
// 200k lines/sec makes the TUI the bottleneck for the whole run. A mutation
// that incremented Model.Frame() inside the EventMsg case (instead of only
// TickMsg) would fail the first assertion; one that never incremented it on
// TickMsg would fail the second.
func TestModelCoalescesEventsBetweenTicksAndBoundsRenders(t *testing.T) {
	src := &fakeSource{state: seedState("r1")}
	m := tui.New(src)
	m = update(t, m, tui.StateMsg{State: seedState("r1")})

	if got := m.Frame(); got != 0 {
		t.Fatalf("Frame() = %d before any tick, want 0", got)
	}

	const burst = 5000
	for i := range burst {
		m = update(t, m, tui.EventMsg{Event: stepCreated("r1", fmt.Sprintf("step-%d", i))})
	}
	if got := m.Frame(); got != 0 {
		t.Errorf("Frame() = %d after %d events with no tick, want 0 (events must not drive rendering)", got, burst)
	}

	m = update(t, m, tui.TickMsg(time.Now()))
	if got := m.Frame(); got != 1 {
		t.Errorf("Frame() = %d after one tick, want 1", got)
	}

	// The burst must still have been folded: coalesced, not dropped.
	out := plain(m.View())
	if !strings.Contains(out, "step-0") || !strings.Contains(out, "step-4999") {
		t.Errorf("View() after the tick is missing events from the coalesced burst:\n%s", out)
	}

	m = update(t, m, tui.TickMsg(time.Now()))
	if got := m.Frame(); got != 2 {
		t.Errorf("Frame() = %d after a second tick, want 2", got)
	}
}

// tickCmd (returned by Init and rescheduled by every TickMsg) must actually
// keep ticking: a mutation that stopped rescheduling after the first tick
// would still pass the bounded-renders test above.
func TestModelTickReschedulesItself(t *testing.T) {
	src := &fakeSource{state: seedState("r1")}
	m := tui.New(src)
	m = update(t, m, tui.StateMsg{State: seedState("r1")})

	_, cmd := m.Update(tui.TickMsg(time.Now()))
	if cmd == nil {
		t.Fatal("handling TickMsg returned a nil Cmd; the tick loop would stop after one frame")
	}
	msg := cmd()
	if _, ok := msg.(tui.TickMsg); !ok {
		t.Errorf("rescheduled Cmd produced %T, want tui.TickMsg", msg)
	}
}

// Every coalescing test above (including the one directly above) feeds a
// hand-built TickMsg and never waits on the real timer tickCmd wraps: they
// prove the coalescing LOGIC, but a mutation to tickInterval itself would
// pass every one of them, since none ever measures how long a tick actually
// takes to fire.
//
// This drives the real tea.Tick timer and times it. bubbletea's own
// Tick(d, fn) (commands.go) starts a time.NewTimer(d) the INSTANT it is
// called (here, synchronously inside Update(TickMsg)) not when the
// returned Cmd is later invoked, so `start` is taken immediately before
// that call and `cmd()` genuinely blocks in real time until the timer
// fires.
func TestModelTickFiresAtApproximatelyThirtyHertz(t *testing.T) {
	src := &fakeSource{state: seedState("r1")}
	m := tui.New(src)
	m = update(t, m, tui.StateMsg{State: seedState("r1")})

	start := time.Now()
	_, cmd := m.Update(tui.TickMsg(time.Now()))
	if cmd == nil {
		t.Fatal("handling TickMsg returned a nil Cmd")
	}
	msg := cmd() // blocks for real until the timer fires
	elapsed := time.Since(start)
	if _, ok := msg.(tui.TickMsg); !ok {
		t.Fatalf("cmd produced %T, want tui.TickMsg", msg)
	}

	// "~30Hz" is time.Second/30 ≈ 33ms. The window below is wide enough to
	// absorb ordinary scheduler jitter on a loaded machine while still
	// failing hard on a mutation an order of magnitude off in either
	// direction (1s: melts nothing, just goes stale; 1ms: melts the
	// terminal for real).
	const wantHz = 30
	if elapsed < 15*time.Millisecond || elapsed > 500*time.Millisecond {
		t.Errorf("tick fired after %s, want roughly %s (~%dHz)", elapsed, time.Second/wantHz, wantHz)
	}
}

// The subscription drain runs on its own goroutine, concurrently with
// Update/View: that concurrency is the whole point of newSubscribeCmd, so
// View() must be safe to call while events fold in the background. Proven
// under -race rather than by inspecting mutex placement: one unlocked read
// into RunState's maps from View() is enough to fail it.
func TestModelViewIsRaceSafeWhileEventsStreamConcurrently(t *testing.T) {
	events := make(chan api.Event)
	src := &fakeSource{state: seedState("r1")}
	src.subCh = events

	m := tui.New(src)
	next, cmd := m.Update(tui.StateMsg{State: seedState("r1")})
	m = next.(*tui.Model)
	if cmd == nil {
		t.Fatal("StateMsg handling returned a nil Cmd; expected the subscribe Cmd")
	}
	go cmd() // the real drain loop, running for real, on its own goroutine

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := range 2000 {
			events <- stepCreated("r1", fmt.Sprintf("s-%d", i))
		}
		close(events)
	}()
	go func() {
		defer wg.Done()
		for range 500 {
			_ = m.View()
		}
	}()
	wg.Wait()
}

// ---- Init wiring against a real FileSource: State -> Subscribe -> fold ----

func TestModelAgainstARealFileSourceFoldsTheWholeReplay(t *testing.T) {
	dir := writeFinishedRun(t)
	fs, err := source.OpenFile(dir, false)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	defer func() { _ = fs.Close() }()

	m := tui.New(fs)
	cmd := m.Init()
	// Init returns tea.Batch(fetchState, tick); run each sub-command to
	// completion synchronously, exactly as bubbletea's own runtime would
	// via goroutines, just without the concurrency: no pty required.
	m = runBatch(t, m, cmd)

	out := plain(m.View())
	for _, want := range []string{"setup", "build", "succeeded"} {
		if !strings.Contains(out, want) {
			t.Errorf("View() against a real FileSource missing %q:\n%s", want, out)
		}
	}
}

// TestModelAgainstARealFileSourceFollowsARetriedAttempt reproduces the
// retry-follow behaviour against the real stack: real per-attempt log files
// under logs/<step>/<attempt>/<stream>, a real events.jsonl, real
// follow-mode polling (source.OpenFile(dir, true)), rather than the fake.
// This confirms the fix holds against the real thing too, not just the
// model's own idea of what a Source does.
func TestModelAgainstARealFileSourceFollowsARetriedAttempt(t *testing.T) {
	dir := t.TempDir()
	ls := eventlog.NewLogSet(dir)

	writeAttempt := func(attempt int, content string) {
		t.Helper()
		w, err := ls.Writer("build", attempt, api.StreamStdout)
		if err != nil {
			t.Fatalf("Writer attempt %d: %v", attempt, err)
		}
		if _, err := w.Write([]byte(content)); err != nil {
			t.Fatalf("write attempt %d: %v", attempt, err)
		}
		if err := w.Close(); err != nil {
			t.Fatalf("close attempt %d: %v", attempt, err)
		}
	}
	writeAttempt(1, "attempt one\n")

	l, err := eventlog.Open(dir)
	if err != nil {
		t.Fatalf("eventlog.Open: %v", err)
	}
	defer func() { _ = l.Close() }()
	mustAppend := func(e api.Event) {
		t.Helper()
		if _, err := l.Append(e); err != nil {
			t.Fatalf("append %s: %v", e.Type, err)
		}
	}
	mustAppend(api.Event{Type: api.RunStarted, Run: "r1", Payload: mustJSON(api.RunStartedBody{
		Pipeline: "ci", EngineVersion: "test", PlanDigest: "d1", StartedAt: time.Now().UTC(),
	})})
	mustAppend(stepCreated("r1", "build"))
	mustAppend(stepStarted("r1", "build", 1))
	mustAppend(logAppended("r1", "build", api.StreamStdout, 0, int64(len("attempt one\n"))))
	mustAppend(stepFinished("r1", "build", 1, api.StateFailed))

	fs, err := source.OpenFile(dir, true) // follow=true: this is the live-attach case
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	defer func() { _ = fs.Close() }()

	st, err := fs.State(context.Background())
	if err != nil {
		t.Fatalf("State: %v", err)
	}

	m := tui.New(fs)
	next, subCmd := m.Update(tui.StateMsg{State: st})
	m = next.(*tui.Model)

	done := make(chan tea.Msg, 1)
	go func() { done <- subCmd() }()

	next, kcmd := m.Update(tea.KeyMsg{Type: tea.KeyEnter}) // focus "build" on attempt 1
	m = next.(*tui.Model)
	m = run(t, m, kcmd)

	out := plain(m.View())
	if !strings.Contains(out, "attempt one") {
		t.Fatalf("View() before the retry missing attempt 1's content:\n%s", out)
	}

	// The retry, written before we start waiting so one follow-poll cycle
	// (internal/source's 50ms pollInterval) picks all of it up together:
	// attempt 2's own log file, then step.retried, step.started and its
	// first step.log.appended marker.
	writeAttempt(2, "attempt two\n")
	retriedBody, err := json.Marshal(api.StepRetriedBody{Attempt: 2})
	if err != nil {
		t.Fatalf("marshal StepRetriedBody: %v", err)
	}
	mustAppend(api.Event{Type: api.StepRetried, Run: "r1", Step: "build", Attempt: 2, Payload: retriedBody})
	mustAppend(stepStarted("r1", "build", 2))
	mustAppend(logAppended("r1", "build", api.StreamStdout, 0, int64(len("attempt two\n"))))

	// Tick repeatedly until the pane follows, or time out generously
	// beyond the poll interval. tickAndFetchOnly, not run(): see its own
	// doc for why driving this specific Cmd through run()'s sequential
	// replay made this loop's real-time budget collapse under CPU
	// contention for a reason that had nothing to do with the follow
	// mechanism this test actually exercises.
	deadline := time.Now().Add(3 * time.Second)
	for {
		next, tcmd := m.Update(tui.TickMsg(time.Now()))
		m = next.(*tui.Model)
		m = tickAndFetchOnly(t, m, tcmd)

		out = plain(m.View())
		if strings.Contains(out, "attempt two") {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for the pane to follow the retried attempt; last View():\n%s", out)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if strings.Contains(out, "attempt one") {
		t.Errorf("View() after following the retry still shows the dead attempt's content:\n%s", out)
	}

	// Let the run finish so the follow-mode drain goroutine started above
	// exits cleanly rather than leaking past this test: FileSource's own
	// follow loop stops once it sees run.finished (see file_test.go's
	// TestFollowStopsAfterRunFinished).
	mustAppend(api.Event{Type: api.RunFinished, Run: "r1", Payload: mustJSON(api.RunFinishedBody{Status: api.RunFailed})})
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("subscribe goroutine did not exit after run.finished")
	}
}

// runBatch resolves a (possibly batched) Cmd fully: it invokes cmd(), and
// for every resulting Msg (unwrapping tea.BatchMsg) feeds it through
// Update, chasing any further Cmd that produces (e.g. StateMsg's own
// follow-up Subscribe) until nothing more is pending. It never sleeps and
// never opens a terminal.
func runBatch(t *testing.T, m *tui.Model, cmd tea.Cmd) *tui.Model {
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
			m = runBatch(t, m, c)
		}
		return m
	default:
		next, follow := m.Update(v)
		nm, ok := next.(*tui.Model)
		if !ok {
			t.Fatalf("Update returned %T, want *tui.Model", next)
		}
		// Do not chase a rescheduled TickMsg forever; one hop is enough to
		// prove the wiring without looping for the test's own lifetime.
		if _, isTick := v.(tui.TickMsg); isTick {
			return nm
		}
		return runBatch(t, nm, follow)
	}
}

func writeFinishedRun(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	l, err := eventlog.Open(dir)
	if err != nil {
		t.Fatalf("open ledger: %v", err)
	}
	events := []api.Event{
		{Type: api.RunStarted, Run: "r1", Payload: mustJSON(api.RunStartedBody{
			Pipeline: "ci", EngineVersion: "test", PlanDigest: "d1", StartedAt: time.Now().UTC(),
		})},
		stepCreated("r1", "setup"),
		stepCreated("r1", "build"),
		stepStarted("r1", "setup", 1),
		stepFinished("r1", "setup", 1, api.StateSucceeded),
		stepStarted("r1", "build", 1),
		stepFinished("r1", "build", 1, api.StateSucceeded),
		{Type: api.RunFinished, Run: "r1", Payload: mustJSON(api.RunFinishedBody{Status: api.RunSucceeded})},
	}
	for _, e := range events {
		if _, err := l.Append(e); err != nil {
			t.Fatalf("append: %v", err)
		}
	}
	if err := l.Close(); err != nil {
		t.Fatalf("close ledger: %v", err)
	}
	return dir
}

func mustJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}

// ---- 'x' sends a skip for the focused step ----

// TestModelXSendsSkipForTheFocusedStep is the assertion that keeps 'x' from
// becoming the thing helpText's reserved list exists to prevent: a key
// documented as real that sends nothing. It is deliberately the mirror of
// TestModelRSendsRetryForTheFocusedStep, since both keys now go through one
// shared sendStepOp.
func TestModelXSendsSkipForTheFocusedStep(t *testing.T) {
	src := &fakeSource{state: seedState("r1"), controlResp: okResp()}
	m := tui.New(src)
	m = update(t, m, tui.StateMsg{State: seedState("r1")})
	m = update(t, m, tui.EventMsg{Event: stepCreated("r1", "build")})

	next, cmd := m.Update(tea.KeyMsg{Type: tea.KeyEnter}) // focus "build"
	m = next.(*tui.Model)
	m = run(t, m, cmd)

	next, cmd = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("x")})
	m = next.(*tui.Model)
	run(t, m, cmd)

	reqs := src.requests()
	if len(reqs) != 1 {
		t.Fatalf("Control called %d times, want 1: %+v", len(reqs), reqs)
	}
	if reqs[0].Type != api.OpStepSkip {
		t.Errorf("Type = %q, want %q", reqs[0].Type, api.OpStepSkip)
	}
	var args map[string]string
	if err := json.Unmarshal(reqs[0].Payload, &args); err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	if args["step"] != "build" {
		t.Errorf("payload step = %q, want %q", args["step"], "build")
	}
}

func TestModelXWithNoFocusDoesNotSendAControlRequestAndSaysWhy(t *testing.T) {
	src := &fakeSource{state: seedState("r1")}
	m := tui.New(src)
	m = update(t, m, tui.StateMsg{State: seedState("r1")})
	m = update(t, m, tui.EventMsg{Event: stepCreated("r1", "build")})

	next, cmd := m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("x")})
	m = next.(*tui.Model)
	m = run(t, m, cmd)

	if len(src.requests()) != 0 {
		t.Fatalf("Control called with nothing focused: %+v", src.requests())
	}
	if !strings.Contains(plain(m.View()), "skip") {
		t.Fatalf("View() does not explain why 'x' did nothing:\n%s", plain(m.View()))
	}
}

// The mechanical guard against helpText advertising a working key as
// inert, so an operator never tries it. It reads the rendered help rather
// than a second hand-maintained list.
//
// The reserved list is currently EMPTY, so the check is inverted: it
// asserts there is no reserved block at all and that every key is
// documented by what it does. If a key is ever reserved-but-inert again,
// the block and the old form of this test come back.
func TestModelHelpDoesNotListARealKeyAsReserved(t *testing.T) {
	src := &fakeSource{state: seedState("r1"), controlResp: okResp()}
	m := tui.New(src)
	m = update(t, m, tui.StateMsg{State: seedState("r1")})
	next, cmd := m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("?")})
	m = next.(*tui.Model)
	m = run(t, m, cmd)

	help := plain(m.View())

	// The two phrasings every wording of the reserved block has used. Either
	// one appearing means a key is being advertised as doing nothing, and
	// there is no longer any such key.
	for _, marker := range []string{"reserved for", "this key does nothing"} {
		if strings.Contains(help, marker) {
			t.Errorf("the help screen still advertises a key as inert (%q), and none are:\n%s", marker, help)
		}
	}

	// Every key the help exists to teach, named by what it DOES. Each of
	// these was reserved-but-inert at some point, and each has to be
	// documented for real now.
	for _, documented := range []string{
		"skip the focused step",
		"open a shell on the focused step",
		"set a breakpoint on the focused step",
		"rerun the focused step",
		"approve the analyzer's proposal",
	} {
		if !strings.Contains(help, documented) {
			t.Errorf("the help screen does not document %q at all:\n%s", documented, help)
		}
	}
}

// ---- 'b' / 'B' arm and clear a breakpoint on the focused step ----

func TestModelBAndShiftBSendTheTwoBreakpointOps(t *testing.T) {
	for _, tc := range []struct {
		key string
		op  string
	}{
		{"b", api.OpBreakpointSet},
		{"B", api.OpBreakpointClear},
	} {
		t.Run(tc.key, func(t *testing.T) {
			src := &fakeSource{state: seedState("r1"), controlResp: okResp()}
			m := tui.New(src)
			m = update(t, m, tui.StateMsg{State: seedState("r1")})
			m = update(t, m, tui.EventMsg{Event: stepCreated("r1", "build")})

			next, cmd := m.Update(tea.KeyMsg{Type: tea.KeyEnter})
			m = next.(*tui.Model)
			m = run(t, m, cmd)

			next, cmd = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune(tc.key)})
			m = next.(*tui.Model)
			run(t, m, cmd)

			reqs := src.requests()
			if len(reqs) != 1 {
				t.Fatalf("Control called %d times, want 1: %+v", len(reqs), reqs)
			}
			if reqs[0].Type != tc.op {
				t.Errorf("Type = %q, want %q", reqs[0].Type, tc.op)
			}
			var args map[string]string
			if err := json.Unmarshal(reqs[0].Payload, &args); err != nil {
				t.Fatalf("decode payload: %v", err)
			}
			if args["step"] != "build" {
				t.Errorf("payload step = %q, want %q", args["step"], "build")
			}
		})
	}
}

// TestModelRendersAHeldStepAsPaused: a step held at a breakpoint has no
// Started, no State and no Error, so without a label of its own it renders
// as a blank row, exactly like one still waiting on a dependency. An
// operator staring at a stopped run would have nothing on screen saying so.
func TestModelRendersAHeldStepAsPaused(t *testing.T) {
	src := &fakeSource{state: seedState("r1")}
	m := tui.New(src)
	m = update(t, m, tui.StateMsg{State: seedState("r1")})
	m = update(t, m, tui.EventMsg{Event: stepCreated("r1", "build")})
	m = update(t, m, tui.EventMsg{Event: api.Event{
		V: 1, Seq: 20, Type: api.BreakpointHit, Run: "r1", Step: "build",
	}})

	if out := plain(m.View()); !strings.Contains(out, "paused") {
		t.Errorf("View() does not show that build is held at a breakpoint:\n%s", out)
	}
}

// ---- 'R' rerun-from ----

func TestModelShiftRSendsRerunFromForTheFocusedStep(t *testing.T) {
	src := &fakeSource{state: seedState("r1"), controlResp: okResp()}
	m := tui.New(src)
	m = update(t, m, tui.StateMsg{State: seedState("r1")})
	m = update(t, m, tui.EventMsg{Event: stepCreated("r1", "build")})
	m = update(t, m, tui.EventMsg{Event: stepFinished("r1", "build", 1, api.StateSucceeded)})

	next, cmd := m.Update(tea.KeyMsg{Type: tea.KeyEnter})
	m = next.(*tui.Model)
	m = run(t, m, cmd)

	next, cmd = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("R")})
	m = next.(*tui.Model)
	run(t, m, cmd)

	reqs := src.requests()
	if len(reqs) != 1 {
		t.Fatalf("Control called %d times, want 1: %+v", len(reqs), reqs)
	}
	if reqs[0].Type != api.OpRunRerunFrom {
		t.Errorf("Type = %q, want %q", reqs[0].Type, api.OpRunRerunFrom)
	}
	var args map[string]string
	if err := json.Unmarshal(reqs[0].Payload, &args); err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	if args["step"] != "build" {
		t.Errorf("payload step = %q, want %q", args["step"], "build")
	}
}

// TestModelHelpListsNoKeyThatIsActuallyWired is the mechanical version of
// the drift the reserved list exists to prevent, checked in the direction
// that hurts: a key that WORKS but is still advertised as inert, so an
// operator never tries it. It drives every key the help screen calls
// reserved and requires each to send nothing at all.
func TestModelHelpListsNoKeyThatIsActuallyWired(t *testing.T) {
	for _, key := range []string{"s", "a"} {
		t.Run(key, func(t *testing.T) {
			src := &fakeSource{state: seedState("r1"), controlResp: okResp()}
			m := tui.New(src)
			m = update(t, m, tui.StateMsg{State: seedState("r1")})
			m = update(t, m, tui.EventMsg{Event: stepCreated("r1", "build")})
			next, cmd := m.Update(tea.KeyMsg{Type: tea.KeyEnter})
			m = next.(*tui.Model)
			m = run(t, m, cmd)

			next, cmd = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune(key)})
			m = next.(*tui.Model)
			run(t, m, cmd)

			if reqs := src.requests(); len(reqs) != 0 {
				t.Errorf("%q is listed on the help screen as reserved and inert, but it sent %+v", key, reqs)
			}
		})
	}
}

// analysisProposed is an analysis.proposed event for a step, as the engine
// would emit it.
func analysisProposed(run, step, id, summary string, remedy api.Remedy) api.Event {
	body, err := json.Marshal(api.AnalysisProposedBody{
		ID: id, Analyzer: "fake",
		Proposal: api.Proposal{Summary: summary, Remedy: remedy},
	})
	if err != nil {
		panic(err)
	}
	return api.Event{Type: api.AnalysisProposed, Run: run, Step: step, Attempt: 1, Payload: body}
}

// TestModelAAndShiftASendTheTwoAnalysisOps: 'a' was the last key on the help
// screen's reserved list, and this is the test that made it leave.
//
// The id, not the step, is what goes on the wire, which is what stops a
// client from approving a proposal about one step into a retry of another.
func TestModelAAndShiftASendTheTwoAnalysisOps(t *testing.T) {
	for _, tc := range []struct {
		key string
		op  string
	}{
		{"a", api.OpAnalysisAccept},
		{"A", api.OpAnalysisReject},
	} {
		t.Run(tc.key, func(t *testing.T) {
			src := &fakeSource{state: seedState("r1"), controlResp: okResp()}
			m := tui.New(src)
			m = update(t, m, tui.StateMsg{State: seedState("r1")})
			m = update(t, m, tui.EventMsg{Event: stepCreated("r1", "build")})
			m = update(t, m, tui.EventMsg{
				Event: analysisProposed("r1", "build", "build@1", "the module proxy timed out", api.RemedyRetry)})

			next, cmd := m.Update(tea.KeyMsg{Type: tea.KeyEnter})
			m = next.(*tui.Model)
			m = run(t, m, cmd)

			// The operator can read what they are approving before they
			// approve it. A gate whose subject is invisible is a prompt.
			if view := plain(m.View()); !strings.Contains(view, "the module proxy timed out") {
				t.Errorf("the proposal's summary is not on screen for the focused step:\n%s", view)
			}

			next, cmd = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune(tc.key)})
			m = next.(*tui.Model)
			run(t, m, cmd)

			reqs := src.requests()
			if len(reqs) != 1 {
				t.Fatalf("Control called %d times, want 1: %+v", len(reqs), reqs)
			}
			if reqs[0].Type != tc.op {
				t.Errorf("Type = %q, want %q", reqs[0].Type, tc.op)
			}
			var args map[string]string
			if err := json.Unmarshal(reqs[0].Payload, &args); err != nil {
				t.Fatalf("decode payload: %v", err)
			}
			if args["id"] != "build@1" {
				t.Errorf("payload id = %q, want build@1", args["id"])
			}
			if _, named := args["step"]; named {
				t.Errorf("the request names a step (%v); the operation takes an id so that a client "+
					"cannot approve a proposal about one step into a retry of another", args)
			}
		})
	}
}

// TestModelASaysSoWhenThereIsNothingToApprove: the engine would answer
// missing_proposal, which is correct and useless. An operator who pressed 'a'
// on a step nothing has proposed anything about needs to be told that.
func TestModelASaysSoWhenThereIsNothingToApprove(t *testing.T) {
	src := &fakeSource{state: seedState("r1"), controlResp: okResp()}
	m := tui.New(src)
	m = update(t, m, tui.StateMsg{State: seedState("r1")})
	m = update(t, m, tui.EventMsg{Event: stepCreated("r1", "build")})

	next, cmd := m.Update(tea.KeyMsg{Type: tea.KeyEnter})
	m = next.(*tui.Model)
	m = run(t, m, cmd)

	next, cmd = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("a")})
	m = next.(*tui.Model)
	run(t, m, cmd)

	if reqs := src.requests(); len(reqs) != 0 {
		t.Fatalf("Control called with nothing proposed: %+v", reqs)
	}
	if view := plain(m.View()); !strings.Contains(view, "nothing proposed") {
		t.Errorf("View() does not explain why 'a' did nothing:\n%s", view)
	}
}

// TestModelForgetsAProposalSomebodyElseDecided: the fold is driven by the
// stream, not by what this client sent, so another operator's accept clears
// the key here too.
func TestModelForgetsAProposalSomebodyElseDecided(t *testing.T) {
	decided, err := json.Marshal(api.AnalysisDecisionBody{ID: "build@1", ClientID: "tui-other", Remedy: api.RemedyRetry})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	src := &fakeSource{state: seedState("r1"), controlResp: okResp()}
	m := tui.New(src)
	m = update(t, m, tui.StateMsg{State: seedState("r1")})
	m = update(t, m, tui.EventMsg{Event: stepCreated("r1", "build")})
	m = update(t, m, tui.EventMsg{
		Event: analysisProposed("r1", "build", "build@1", "flaky network", api.RemedyRetry)})
	m = update(t, m, tui.EventMsg{Event: api.Event{
		Type: api.AnalysisApplied, Run: "r1", Step: "build", Attempt: 1, Payload: decided}})

	next, cmd := m.Update(tea.KeyMsg{Type: tea.KeyEnter})
	m = next.(*tui.Model)
	m = run(t, m, cmd)

	next, cmd = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("a")})
	m = next.(*tui.Model)
	run(t, m, cmd)

	if reqs := src.requests(); len(reqs) != 0 {
		t.Fatalf("Control called for a proposal another client already decided: %+v", reqs)
	}
}

// ---- 'p' / 'P' pause and resume ----

// TestModelPAndShiftPSendRunPauseAndResume: both are run-scoped, so neither
// needs a focused step and neither may send one. A pause carrying a step
// argument would be refused outright by the attach server's own allowlist
// before it reached the engine.
func TestModelPAndShiftPSendRunPauseAndResume(t *testing.T) {
	for _, tc := range []struct {
		key string
		op  string
	}{
		{"p", api.OpRunPause},
		{"P", api.OpRunResume},
	} {
		t.Run(tc.key, func(t *testing.T) {
			src := &fakeSource{state: seedState("r1"), controlResp: okResp()}
			m := tui.New(src)
			m = update(t, m, tui.StateMsg{State: seedState("r1")})
			m = update(t, m, tui.EventMsg{Event: stepCreated("r1", "build")})

			// Deliberately no enter: nothing is focused, and a run-scoped
			// operation must work anyway.
			next, cmd := m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune(tc.key)})
			m = next.(*tui.Model)
			run(t, m, cmd)

			reqs := src.requests()
			if len(reqs) != 1 {
				t.Fatalf("Control called %d times, want 1: %+v", len(reqs), reqs)
			}
			if reqs[0].Type != tc.op {
				t.Errorf("Type = %q, want %q", reqs[0].Type, tc.op)
			}
			if len(reqs[0].Payload) != 0 {
				t.Errorf("payload = %s, want none: %s takes no arguments", reqs[0].Payload, tc.op)
			}
		})
	}
}

// TestModelRendersAPausedRunAsPaused is the run-level companion to
// TestModelRendersAHeldStepAsPaused, and the gap it closes is the same one:
// every step of a paused run has no Started and no State, so the rows are
// indistinguishable from a run whose steps are all still waiting. The footer
// saying "running" over them is the actively misleading part.
func TestModelRendersAPausedRunAsPaused(t *testing.T) {
	src := &fakeSource{state: seedState("r1")}
	m := tui.New(src)
	m = update(t, m, tui.StateMsg{State: seedState("r1")})
	m = update(t, m, tui.EventMsg{Event: stepCreated("r1", "build")})

	if out := plain(m.View()); !strings.Contains(out, "run: running") {
		t.Fatalf("View() does not start out reporting a running run:\n%s", out)
	}

	body, err := json.Marshal(api.ControlAppliedBody{Op: api.OpRunPause, ClientID: "someone-else"})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	m = update(t, m, tui.EventMsg{Event: api.Event{
		V: 1, Seq: 30, Type: api.ControlApplied, Run: "r1", Payload: body,
	}})

	if out := plain(m.View()); !strings.Contains(out, "run: paused") {
		t.Errorf("View() does not show that the run is paused:\n%s", out)
	}
}

// ---- 'w' sends a forced workspace snapshot for the focused step ----

// The same assertion 'x' and 'r' carry, for the key helpText advertises as
// real: a documented key that sends nothing is exactly what that file's
// reserved list exists to prevent.
func TestModelWSendsAWorkspaceSnapshotForTheFocusedStep(t *testing.T) {
	src := &fakeSource{state: seedState("r1"), controlResp: okResp()}
	m := tui.New(src)
	m = update(t, m, tui.StateMsg{State: seedState("r1")})
	m = update(t, m, tui.EventMsg{Event: stepCreated("r1", "build")})

	next, cmd := m.Update(tea.KeyMsg{Type: tea.KeyEnter}) // focus "build"
	m = next.(*tui.Model)
	m = run(t, m, cmd)

	next, cmd = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("w")})
	m = next.(*tui.Model)
	run(t, m, cmd)

	reqs := src.requests()
	if len(reqs) != 1 {
		t.Fatalf("Control called %d times, want 1: %+v", len(reqs), reqs)
	}
	if reqs[0].Type != api.OpWSSnapshot {
		t.Errorf("Type = %q, want %q", reqs[0].Type, api.OpWSSnapshot)
	}
	var args map[string]string
	if err := json.Unmarshal(reqs[0].Payload, &args); err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	if args["step"] != "build" {
		t.Errorf("payload step = %q, want %q", args["step"], "build")
	}
}

func TestModelWWithNoFocusDoesNotSendAControlRequestAndSaysWhy(t *testing.T) {
	src := &fakeSource{state: seedState("r1")}
	m := tui.New(src)
	m = update(t, m, tui.StateMsg{State: seedState("r1")})
	m = update(t, m, tui.EventMsg{Event: stepCreated("r1", "build")})

	next, cmd := m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("w")})
	m = next.(*tui.Model)
	m = run(t, m, cmd)

	if len(src.requests()) != 0 {
		t.Fatalf("Control called with nothing focused: %+v", src.requests())
	}
	// The whole phrase, not the bare label: the footer's key hints carry
	// the word "snapshot" on every frame, so a substring of one would be a
	// test that cannot fail.
	if !strings.Contains(plain(m.View()), "snapshot: no step focused") {
		t.Fatalf("View() does not explain why 'w' did nothing:\n%s", plain(m.View()))
	}
}
