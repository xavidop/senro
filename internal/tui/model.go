package tui

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	tea "github.com/charmbracelet/bubbletea"

	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/internal/source"
)

// tickInterval is the model's fixed render cadence: ~30Hz. Rendering per
// event (the textbook way to consume a channel in a bubbletea program)
// melts the terminal at the volumes a real build produces; see
// newSubscribeCmd's own doc for the mechanism this avoids.
const tickInterval = time.Second / 30

// maxLogFetch bounds a single Source.Logs forward-tail read. The ring
// itself is already bounded (defaultLogRingCap lines); this just stops one
// fetch from paging in an unreasonable amount of a step's history in one
// shot when a user focuses a step with gigabytes of output.
const maxLogFetch = 1 << 20 // 1 MiB

// scrollbackChunkSize is how much history one 'pgup' fetches. Large enough
// that a scrollback session is not all round trips, small enough that
// repeated presses stay responsive.
const scrollbackChunkSize = 64 * 1024

// Fetch kinds, used to build pending's keys. A forward-tail fetch and a
// scrollback fetch for the same step are independent operations: an
// operator can page back through history while the live tail keeps
// advancing, so they claim separate slots; see pending's own doc.
const (
	fetchKindTail    = "tail"
	fetchKindHistory = "history"
)

// fetchKey builds the pending-map key for one (step, kind) pair.
func fetchKey(step, kind string) string { return step + "\x00" + kind }

// Model is the TUI's bubbletea model. It is a source.Source client and
// nothing more (see doc.go), so it works identically against a live
// engine, a finished run on disk, or a FallbackSource that switched between
// the two mid-run.
type Model struct {
	src source.Source
	ctx context.Context
	// cancel stops every in-flight Cmd this model started (the subscription
	// drain in particular), called on quit so that goroutine does not leak
	// past the program's lifetime.
	cancel context.CancelFunc

	// mu guards state, logs, logOffset and pending: the subscription-drain
	// Cmd folds from its own goroutine (and resets logs/logOffset on a
	// retry), every fetch Cmd runs on a goroutine of its own, and
	// Update/View run on bubbletea's loop. Every access to any of the
	// four, from any goroutine, must hold mu.
	mu    sync.Mutex
	state *api.RunState
	logs  map[string]*logRing
	// logOffset is, per step, the byte offset stdout has been fetched to:
	// the high-water mark a follow-up range request resumes from. Deleted
	// (not merely reset) the moment StepRetried lands: a retry's log is a
	// brand new file starting at byte 0, and the old offset describes a
	// file that no longer exists at that position.
	logOffset map[string]int64

	// pending tracks in-flight log fetches, keyed by fetchKey(step, kind):
	// the ONE mechanism that makes an overlapping fetch impossible. Every
	// path that fetches claims a slot via beginFetch before returning a
	// Cmd and releases it via endFetch when the result (success, error, or
	// dropped stale) is handled; a future path added the same way is
	// guarded for free.
	//
	// Needed because bubbletea runs every Cmd as its own goroutine, and
	// offsets only advance when a fetch's result is APPLIED, so two ticks
	// firing before one round trip completes would both fetch the same
	// stale range, and applying both duplicates content.
	pending map[string]bool

	cursor  int    // index into rows(): the highlighted-but-not-yet-focused row
	focused string // the step whose log pane is showing; "" before 'enter'

	status string // last control result or error, shown in the footer

	// proposals is the live, undecided proposal per step: step id to the
	// api.AnalysisProposedBody.ID the 'a' key would accept. Folded from
	// the stream, not asked for, so a late-attaching client knows about a
	// proposal made before it connected; entries are removed when the
	// engine records a decision, so 'a' on a settled proposal says there
	// is nothing to approve. One per step, not a list: a later proposal is
	// about a later attempt and is the only one still actionable.
	proposals map[string]string

	// summaries is what each of those proposals actually said, so the footer
	// can show an operator what they are about to approve. Approving text
	// you have not seen is not approval.
	summaries map[string]string
	// streamErr is sticky, unlike status: once Apply rejects a regressing
	// Seq the local fold can no longer be trusted, and that must stay
	// visible in the footer rather than be overwritten by the next status
	// update. See newSubscribeCmd for why a regression is fatal.
	streamErr error

	frame int // advanced only by TickMsg; see Frame's own doc

	// filtering is true while '/' input is being typed; filter is the last
	// committed (enter) value, applied to the step list until changed or
	// cleared. filterInput is the in-progress edit, shown in the footer
	// while filtering.
	filtering   bool
	filterInput string
	filter      string

	showHelp bool

	width, height int

	reqSeq uint64 // monotonic source of control frame IDs

	quitting bool
}

// Option configures a Model constructed by New.
type Option func(*Model)

// WithContext derives the model's own (cancellable) context from ctx,
// rather than context.Background(). Run uses this to make the model's
// subscription and control requests respect the caller's context.
func WithContext(ctx context.Context) Option {
	return func(m *Model) {
		m.ctx, m.cancel = context.WithCancel(ctx)
	}
}

// New builds a Model over src. src is never touched until Init runs (or a
// caller drives the model directly in a test, per this package's own
// tests), construction alone must not perform I/O.
func New(src source.Source, opts ...Option) *Model {
	m := &Model{
		src:       src,
		state:     api.NewRunState(),
		logs:      make(map[string]*logRing),
		logOffset: make(map[string]int64),
		pending:   make(map[string]bool),
	}
	m.ctx, m.cancel = context.WithCancel(context.Background())
	for _, opt := range opts {
		opt(m)
	}
	return m
}

var _ tea.Model = (*Model)(nil)

// Frame reports how many render cycles the model has processed, advanced
// only by TickMsg, never by EventMsg. It exists so a caller (this package's
// own tests, in particular) can observe the tick coalescing guarantee
// directly: however many events arrive between two ticks, this counter
// advances by exactly one per tick, not one per event.
func (m *Model) Frame() int { return m.frame }

// RunStatus reports the run's rolled-up status as folded so far: empty
// until run.finished lands. It lets a caller of Run learn the final
// outcome for an exit code after the program exits, without a second round
// trip against a Source Run has already closed. Guarded by mu, though
// nothing else is writing by then.
func (m *Model) RunStatus() api.RunStatus {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.state == nil {
		return ""
	}
	return m.state.Run.Status
}

// Init fetches the run's initial snapshot and starts the tick loop. The
// subscription itself does not start until the snapshot's Seq is known
// (see StateMsg's own doc), so it is not started here.
func (m *Model) Init() tea.Cmd {
	return tea.Batch(m.fetchStateCmd(), m.tickCmd())
}

func (m *Model) fetchStateCmd() tea.Cmd {
	src, ctx := m.src, m.ctx
	return func() tea.Msg {
		st, err := src.State(ctx)
		return StateMsg{State: st, Err: err}
	}
}

func (m *Model) tickCmd() tea.Cmd {
	return tea.Tick(tickInterval, func(t time.Time) tea.Msg { return TickMsg(t) })
}

// newSubscribeCmd starts the lifecycle subscription and drains it directly
// into the fold via applyEvent for the lifetime of the Cmd, NOT by sending
// one bubbletea Msg per event.
//
// This is what delivers "coalesced between ticks": bubbletea's event loop
// calls View() after EVERY message it routes through Update, and its
// renderer only throttles the terminal WRITE, not the View() computation.
// One Msg per event (the textbook channel-consuming pattern) would call
// View() once per event regardless of tick rate, melting the terminal at
// real build volumes. Folding here means View() is driven only by TickMsg
// and input, while this goroutine drains the channel as fast as Apply can
// run: what "never applies backpressure to the engine" requires.
//
// A regressing Seq is fatal to the subscription, matching render.Plain.
// And stopping the read loop alone is a half-shutdown: the producer would
// block forever sending to a consumer that stopped reading. So a
// regression cancels ctx (releasing every other Cmd, which all share it)
// and closes src (so the producer releases what it holds): the whole
// session tears down, not just this read loop.
func (m *Model) newSubscribeCmd(fromSeq uint64) tea.Cmd {
	src, ctx, cancel := m.src, m.ctx, m.cancel
	return func() tea.Msg {
		ch, err := src.Subscribe(ctx, fromSeq)
		if err != nil {
			return SubscribeClosedMsg{Err: err}
		}
		for e := range ch {
			if err := m.applyEvent(e); err != nil {
				cancel()
				_ = src.Close()
				return SubscribeClosedMsg{Err: err}
			}
		}
		return SubscribeClosedMsg{}
	}
}

// beginFetch claims the in-flight slot for key (see pending's own doc),
// returning false if one is already claimed: the caller must not issue a
// fetch in that case, and must instead let the outstanding one resolve.
func (m *Model) beginFetch(key string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.pending[key] {
		return false
	}
	m.pending[key] = true
	return true
}

// endFetch releases the slot a prior successful beginFetch claimed. Called
// exactly once per claim, when that fetch's result (whatever it turns
// out to be, including one dropped as stale) is handled, so the step can
// be fetched again.
func (m *Model) endFetch(key string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.pending, key)
}

// applyEvent folds one event into state under mu: the single fold path
// both EventMsg and newSubscribeCmd's drain goroutine use.
//
// A StepRetried that Apply accepts also deletes this model's log cache for
// that step, in the same locked section: the next attempt writes its own
// file from byte 0, so the cached lines and offset describe a file
// Source.Logs will never hand back; left in place, the pane shows dead
// content or seeks past the new file's end. Deleting (not zeroing) means
// the next focus or follow starts a clean fetch of the NEW file. This runs
// on whichever goroutine called applyEvent, which is why logs/logOffset
// live under mu: a retry does not wait for the operator to be looking.
func (m *Model) applyEvent(e api.Event) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.state == nil {
		m.state = api.NewRunState()
	}
	if err := m.state.Apply(e); err != nil {
		return err
	}
	if e.Type == api.StepRetried && e.Step != "" {
		delete(m.logs, e.Step)
		delete(m.logOffset, e.Step)
	}
	m.foldProposal(e)
	return nil
}

// foldProposal keeps the 'a' key's target in step with the engine's own
// view of which proposals are still open. Called under m.mu from
// applyEvent: View ranges over these maps while the drain goroutine folds
// into them.
func (m *Model) foldProposal(e api.Event) {
	if e.Step == "" {
		return
	}
	switch e.Type {
	case api.AnalysisProposed:
		var b api.AnalysisProposedBody
		if e.Decode(&b) != nil || b.ID == "" {
			return
		}
		if m.proposals == nil {
			m.proposals = map[string]string{}
			m.summaries = map[string]string{}
		}
		m.proposals[e.Step] = b.ID
		m.summaries[e.Step] = b.Summary
	case api.AnalysisApplied, api.AnalysisRejected:
		// Decided by somebody, not necessarily by this client: another
		// operator's accept clears it here too, which is the point of
		// folding it from the stream rather than from what this client sent.
		delete(m.proposals, e.Step)
		delete(m.summaries, e.Step)
	}
}

// Update implements tea.Model.
func (m *Model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case StateMsg:
		return m.handleState(msg)
	case EventMsg:
		// A regression here gets the identical treatment newSubscribeCmd
		// gives the live path: surfaced, sticky, never silently dropped.
		// See streamErr's own doc.
		if err := m.applyEvent(msg.Event); err != nil {
			m.streamErr = err
		}
		return m, nil
	case TickMsg:
		return m.handleTick()
	case tea.KeyMsg:
		return m.handleKey(msg)
	case ControlResultMsg:
		return m.handleControlResult(msg)
	case ShellFinishedMsg:
		return m.handleShellFinished(msg)
	case LogChunkMsg:
		return m.handleLogChunk(msg)
	case LogHistoryMsg:
		return m.handleLogHistory(msg)
	case SubscribeClosedMsg:
		// Only a rejected regression sets Err (a drained replay or 'q'
		// leaves it nil), and that is the case that must not be
		// swallowed; see newSubscribeCmd.
		if msg.Err != nil {
			m.streamErr = msg.Err
		}
		return m, nil
	case tea.WindowSizeMsg:
		m.width, m.height = msg.Width, msg.Height
		return m, nil
	}
	return m, nil
}

func (m *Model) handleState(msg StateMsg) (tea.Model, tea.Cmd) {
	if msg.Err != nil {
		m.status = fmt.Sprintf("state: %v", msg.Err)
		return m, nil
	}
	m.mu.Lock()
	m.state = msg.State
	seq := msg.State.Seq
	m.mu.Unlock()
	return m, m.newSubscribeCmd(seq + 1)
}

func (m *Model) handleTick() (tea.Model, tea.Cmd) {
	m.frame++
	cmds := []tea.Cmd{m.tickCmd()}
	if c := m.followFocusedLogsCmd(); c != nil {
		cmds = append(cmds, c)
	}
	return m, tea.Batch(cmds...)
}

// followFocusedLogsCmd issues a forward fetch for the focused step's new
// stdout bytes when the fold's high-water mark (StepState.LogBytes) has
// advanced past what has been fetched. Tied to the tick: many log-append
// markers between two ticks coalesce into at most one fetch.
func (m *Model) followFocusedLogsCmd() tea.Cmd {
	if m.focused == "" {
		return nil
	}
	// The watermark and offset must be read under the SAME locked section:
	// the drain goroutine mutates the StepState in place and deletes
	// logOffset on a retry, and two separate lock pairs could observe a
	// torn view (a pre-retry watermark with a post-retry offset).
	m.mu.Lock()
	st, ok := m.state.Steps[m.focused]
	var watermark int64
	if ok {
		watermark = st.LogBytes[api.StreamStdout]
	}
	have := m.logOffset[m.focused]
	m.mu.Unlock()
	if !ok || watermark <= have {
		return nil
	}
	return m.fetchLogsCmd(m.focused, have)
}

// fetchLogsCmd issues a forward-tail fetch for step starting at from: the
// single choke point both focusCursor and followFocusedLogsCmd go through,
// so beginFetch guards both callers with one claim/release pair. Returns
// nil when a tail fetch for this step is already in flight.
//
// Attempt is captured at issue time and travels with the result so
// handleLogChunk can tell whether a retry landed while this fetch was in
// flight; see its doc.
func (m *Model) fetchLogsCmd(step string, from int64) tea.Cmd {
	if !m.beginFetch(fetchKey(step, fetchKindTail)) {
		return nil
	}
	src, ctx := m.src, m.ctx
	attempt := m.stepAttempt(step)
	return func() tea.Msg {
		rc, err := src.Logs(ctx, step, attempt, api.StreamStdout, from)
		if err != nil {
			return LogChunkMsg{Step: step, Err: err}
		}
		defer func() { _ = rc.Close() }()
		data, err := io.ReadAll(io.LimitReader(rc, maxLogFetch))
		if err != nil {
			return LogChunkMsg{Step: step, Err: err}
		}
		return LogChunkMsg{Step: step, Data: data, From: from, NextOffset: from + int64(len(data)), Attempt: attempt}
	}
}

// loadOlderLogsCmd issues one scrollback fetch ('pgup') for the focused
// step: a chunk ending exactly at its ring's current StartOffset(), so the
// result prepends with no gap and no overlap. That boundary travels with
// the result so handleLogHistory can tell whether it is still valid when
// the fetch resolves. A no-op, with a status message, when nothing has
// been fetched or scrollback already reaches the file's start; and via
// beginFetch when a scrollback fetch is already in flight.
func (m *Model) loadOlderLogsCmd() (tea.Model, tea.Cmd) {
	if m.focused == "" {
		return m, nil
	}
	step := m.focused
	m.mu.Lock()
	r, ok := m.logs[step]
	var end int64
	if ok {
		end = r.StartOffset()
	}
	m.mu.Unlock()
	if !ok || end <= 0 {
		m.status = "no earlier log history for " + step
		return m, nil
	}
	if !m.beginFetch(fetchKey(step, fetchKindHistory)) {
		return m, nil
	}

	from := end - scrollbackChunkSize
	atStart := from <= 0
	if from < 0 {
		from = 0
	}
	length := end - from

	src, ctx := m.src, m.ctx
	attempt := m.stepAttempt(step)
	return m, func() tea.Msg {
		rc, err := src.Logs(ctx, step, attempt, api.StreamStdout, from)
		if err != nil {
			return LogHistoryMsg{Step: step, Boundary: end, Err: err}
		}
		defer func() { _ = rc.Close() }()
		data, err := io.ReadAll(io.LimitReader(rc, length))
		if err != nil {
			return LogHistoryMsg{Step: step, Boundary: end, Err: err}
		}
		return LogHistoryMsg{Step: step, Data: data, AtStart: atStart, Attempt: attempt, Boundary: end}
	}
}

func (m *Model) stepAttempt(step string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.state == nil {
		return 0
	}
	if s, ok := m.state.Steps[step]; ok && s != nil {
		return s.Attempt
	}
	return 0
}

func (m *Model) handleKey(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	// The help overlay and the filter-input line both swallow keys that
	// would otherwise be commands ('r' typed into a filter must not retry
	// anything); see handleFilterKey's own doc.
	if m.showHelp {
		switch msg.String() {
		case "?", "esc":
			m.showHelp = false
		}
		return m, nil
	}
	if m.filtering {
		return m.handleFilterKey(msg)
	}

	switch msg.String() {
	case "up", "k":
		m.moveCursor(-1)
		return m, nil
	case "down", "j":
		m.moveCursor(1)
		return m, nil
	case "enter":
		return m.focusCursor()
	case "r":
		return m.sendRetry()
	case "s":
		return m.openShell()
	case "x":
		return m.sendSkip()
	case "b":
		return m.sendStepOp(api.OpBreakpointSet, "breakpoint")
	case "B":
		return m.sendStepOp(api.OpBreakpointClear, "release")
	case "R":
		return m.sendStepOp(api.OpRunRerunFrom, "rerun-from")
	case "w":
		return m.sendStepOp(api.OpWSSnapshot, "snapshot")
	case "p":
		return m.sendRunOp(api.OpRunPause)
	case "P":
		return m.sendRunOp(api.OpRunResume)
	case "a":
		return m.sendAnalysisDecision(api.OpAnalysisAccept, "approve")
	case "A":
		return m.sendAnalysisDecision(api.OpAnalysisReject, "reject")
	case "c", "ctrl+c":
		return m.sendCancel()
	case "q":
		return m.quit()
	case "/":
		m.filtering = true
		m.filterInput = m.filter
		return m, nil
	case "?":
		m.showHelp = true
		return m, nil
	case "pgup":
		return m.loadOlderLogsCmd()
	}
	return m, nil
}

// handleFilterKey edits filterInput while '/' input is active. enter
// commits it as the active filter (and resets the cursor, since the
// visible row set just changed under it); esc discards the edit and
// leaves whatever filter was active before unchanged: a cancelled edit is
// not the same as clearing the filter.
func (m *Model) handleFilterKey(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.Type {
	case tea.KeyEnter:
		m.filter = m.filterInput
		m.filtering = false
		m.cursor = 0
	case tea.KeyEsc:
		m.filtering = false
	case tea.KeyBackspace:
		if len(m.filterInput) > 0 {
			m.filterInput = m.filterInput[:len(m.filterInput)-1]
		}
	case tea.KeySpace:
		m.filterInput += " "
	case tea.KeyRunes:
		m.filterInput += string(msg.Runes)
	}
	return m, nil
}

// quit detaches: it releases the Source and stops this model's own
// goroutines, and sends NO control request. The run (if it is a live one)
// continues; Ctrl-C cancels it instead. Quitting a UI must never be a way
// to silently kill a deploy.
func (m *Model) quit() (tea.Model, tea.Cmd) {
	m.quitting = true
	m.cancel()
	_ = m.src.Close()
	return m, tea.Quit
}

func (m *Model) sendRetry() (tea.Model, tea.Cmd) {
	return m.sendStepOp(api.OpStepRetry, "retry")
}

// sendSkip asks the engine to take the focused step out of the run: it and
// every step that needs it settle as skipped_manual without running. It is
// 'x', not 's': 's' opens a shell on the focused step, and putting an
// operation that permanently removes work one key from the one an operator
// reaches for to LOOK at a step is how a deploy step gets skipped by
// mistake.
func (m *Model) sendSkip() (tea.Model, tea.Cmd) {
	return m.sendStepOp(api.OpStepSkip, "skip")
}

// sendStepOp issues one step-scoped control request against the focused
// step, or explains that there is not one. Shared by every such key: each
// copy would be another chance to send a request naming an empty step and
// leave the operator staring at missing_step.
func (m *Model) sendStepOp(op, label string) (tea.Model, tea.Cmd) {
	if m.focused == "" {
		m.status = label + ": no step focused, press enter on a step first"
		return m, nil
	}
	step := m.focused
	return m, m.controlCmd(op, step, map[string]string{"step": step})
}

// sendAnalysisDecision accepts or rejects the focused step's open
// proposal. It sends an id, never a step: the step an accepted proposal
// retries is the one the ENGINE's record names, so a client cannot approve
// one step's proposal into a retry of another.
//
// 'a' and 'A' are two keys, matching b/B: one wire operation each, the
// engine's refusal shown in the footer. The one local refusal is a step
// with no open proposal: the engine's missing_proposal answer is correct
// and useless to an operator.
func (m *Model) sendAnalysisDecision(op, label string) (tea.Model, tea.Cmd) {
	if m.focused == "" {
		m.status = label + ": no step focused, press enter on a step first"
		return m, nil
	}
	m.mu.Lock()
	id := m.proposals[m.focused]
	m.mu.Unlock()
	if id == "" {
		m.status = label + ": nothing proposed for " + m.focused
		return m, nil
	}
	return m, m.controlCmd(op, m.focused, map[string]string{"id": id})
}

func (m *Model) sendCancel() (tea.Model, tea.Cmd) {
	return m, m.controlCmd(api.OpRunCancel, "", nil)
}

// sendRunOp issues one run-scoped control request: no focused step, no
// arguments. Separate from sendStepOp because its "press enter on a step
// first" check would refuse a perfectly valid pause.
//
// 'p' and 'P' are two keys, not a toggle: whether the run is paused is the
// ENGINE's answer, and another client may have changed it a moment ago; a
// toggle keyed off local memory would resume a pause somebody else already
// released.
func (m *Model) sendRunOp(op string) (tea.Model, tea.Cmd) {
	return m, m.controlCmd(op, "", nil)
}

func (m *Model) controlCmd(op, step string, args map[string]string) tea.Cmd {
	src, ctx := m.src, m.ctx
	id := fmt.Sprintf("tui-%d", atomic.AddUint64(&m.reqSeq, 1))
	var payload []byte
	if args != nil {
		payload, _ = json.Marshal(args)
	}
	req := api.Frame{V: api.Version, Kind: api.KindReq, ID: id, Type: op, Payload: payload}
	return func() tea.Msg {
		resp, err := src.Control(ctx, req)
		return ControlResultMsg{Op: op, Step: step, Resp: resp, Err: err}
	}
}

// handleControlResult turns a control response into a footer status line:
// a refusal's machine-readable reason or a transport error, surfaced
// verbatim, never swallowed into silence or a false "done".
func (m *Model) handleControlResult(msg ControlResultMsg) (tea.Model, tea.Cmd) {
	switch {
	case msg.Err != nil:
		m.status = fmt.Sprintf("%s: %v", msg.Op, msg.Err)
	case msg.Resp.OK == nil || !*msg.Resp.OK:
		reason := msg.Resp.Error
		if reason == "" {
			reason = "refused"
		}
		m.status = fmt.Sprintf("%s refused: %s", msg.Op, reason)
	case msg.Step != "":
		m.status = fmt.Sprintf("%s accepted for %s", msg.Op, msg.Step)
	default:
		m.status = fmt.Sprintf("%s accepted", msg.Op)
	}
	return m, nil
}

// handleLogChunk applies a forward-tail fetch's result to the step's ring.
//
// endFetch runs FIRST and unconditionally, on every return path: skipping
// it on any path would permanently wedge that step's fetches for the rest
// of the session.
//
// msg.Attempt is checked against the step's CURRENT attempt before
// applying: a retry can land, and applyEvent's reset delete the ring,
// WHILE this fetch was in flight for the old attempt, and applying the
// stale result would recreate the ring with dead content. A mismatch is
// dropped silently: the next tick or focus fetches fresh.
func (m *Model) handleLogChunk(msg LogChunkMsg) (tea.Model, tea.Cmd) {
	m.endFetch(fetchKey(msg.Step, fetchKindTail))
	if msg.Err != nil {
		m.status = fmt.Sprintf("logs %s: %v", msg.Step, msg.Err)
		return m, nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if s, ok := m.state.Steps[msg.Step]; !ok || s.Attempt != msg.Attempt {
		return m, nil
	}
	r, ok := m.logs[msg.Step]
	if !ok {
		r = newLogRing(defaultLogRingCap)
		m.logs[msg.Step] = r
	}
	r.Append(msg.From, msg.Data)
	m.logOffset[msg.Step] = msg.NextOffset
	return m, nil
}

// handleLogHistory prepends a scrollback fetch's result to the step's
// ring. endFetch runs first and unconditionally, as in handleLogChunk.
//
// Guarded against the world moving on since the fetch was issued: the ring
// no longer exists or the attempt moved on (handleLogChunk's check); or
// the ring's CURRENT StartOffset() no longer matches msg.Boundary, meaning
// a concurrent Append/trim moved it and the fetched range no longer abuts
// the ring's front. Applying it anyway would corrupt StartOffset and
// misdirect a later pgup; the next pgup re-reads StartOffset fresh.
//
// Prepend returning false (no line boundary; folded in as one raw line) is
// surfaced as its own status message rather than left unexplained.
func (m *Model) handleLogHistory(msg LogHistoryMsg) (tea.Model, tea.Cmd) {
	m.endFetch(fetchKey(msg.Step, fetchKindHistory))
	if msg.Err != nil {
		m.status = fmt.Sprintf("logs %s: %v", msg.Step, msg.Err)
		return m, nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if s, ok := m.state.Steps[msg.Step]; !ok || s.Attempt != msg.Attempt {
		return m, nil
	}
	r, ok := m.logs[msg.Step]
	if !ok {
		return m, nil
	}
	if r.StartOffset() != msg.Boundary {
		m.status = "log history for " + msg.Step + " moved while fetching, press pgup again"
		return m, nil
	}
	if !r.Prepend(msg.Data, msg.AtStart) {
		m.status = "no line breaks in older history for " + msg.Step + ", showing raw bytes"
	}
	return m, nil
}

// rows returns the current top-level step IDs, narrowed by the active
// filter. Used both to build the left pane and to bound the cursor, which
// must agree on exactly the same set: otherwise 'enter' and 'r' could
// target a step the operator cannot even see.
func (m *Model) rows() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return filterRows(topLevelRows(m.state), m.filter)
}

// filterRows narrows rows to those whose ID contains filter as a
// case-insensitive substring. An empty filter is a no-op: returns rows
// itself, not a copy, so the common (unfiltered) case allocates nothing.
func filterRows(rows []string, filter string) []string {
	if filter == "" {
		return rows
	}
	f := strings.ToLower(filter)
	out := make([]string, 0, len(rows))
	for _, id := range rows {
		if strings.Contains(strings.ToLower(id), f) {
			out = append(out, id)
		}
	}
	return out
}

// topLevelRows lists what the left pane shows one row per: every step in
// creation order whose Group is empty (an expansion child is represented,
// collapsed, by its parent's row, never on its own), plus, sorted for
// determinism, any expansion parent that never got its own step.created
// (a pure fan-out declaration with no StepState in Steps at all).
func topLevelRows(st *api.RunState) []string {
	if st == nil {
		return nil
	}
	seen := make(map[string]bool, len(st.Order))
	rows := make([]string, 0, len(st.Order))
	for _, id := range st.Order {
		s := st.Steps[id]
		if s == nil || s.Group != "" {
			continue
		}
		rows = append(rows, id)
		seen[id] = true
	}
	var orphans []string
	for parent := range st.Expansions {
		if !seen[parent] {
			orphans = append(orphans, parent)
		}
	}
	sort.Strings(orphans)
	return append(rows, orphans...)
}

func (m *Model) moveCursor(delta int) {
	n := len(m.rows())
	if n == 0 {
		m.cursor = 0
		return
	}
	m.cursor += delta
	if m.cursor < 0 {
		m.cursor = 0
	}
	if m.cursor >= n {
		m.cursor = n - 1
	}
}

func (m *Model) focusCursor() (tea.Model, tea.Cmd) {
	rows := m.rows()
	if m.cursor < 0 || m.cursor >= len(rows) {
		return m, nil
	}
	step := rows[m.cursor]
	m.mu.Lock()
	from := m.logOffset[step]
	m.mu.Unlock()
	m.focused = step
	return m, m.fetchLogsCmd(step, from)
}
