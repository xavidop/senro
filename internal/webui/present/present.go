// Package present turns a folded api.RunState into the handful of strings
// a renderer draws, and nothing else.
//
// It reads STATE, never events: what an event means is
// api.RunState.Apply's business, with exactly one implementation shared by
// every client. What a state LOOKS like is this package's business, held
// here rather than in the client so it can be tested on the host, where a
// mistake fails a test rather than a person's screen. Every function is a
// pure function of the state it is given.
package present

import (
	"fmt"
	"sort"
	"time"

	"github.com/xavidop/senro/api"
)

// Tone names the four colours the site's own stylesheet reserves for
// describing a run's state, and is deliberately a small closed set: a
// renderer picks a class from it and never invents one, which is what keeps
// the browser UI inside the palette rule global.css states (status colours
// describe a run, and are never the interface's own accents).
type Tone string

const (
	ToneOK      Tone = "ok"
	ToneFailed  Tone = "failed"
	ToneRunning Tone = "running"
	ToneNeutral Tone = "neutral"
)

// Badge is a label and the tone to draw it in.
type Badge struct {
	Label string
	Tone  Tone
}

// StepBadge describes one step's current condition. The order of the
// checks is the whole content: Paused first, because a step held at a
// breakpoint has no state, start or error and would otherwise look like
// one still waiting on its dependencies; a terminal state next because it
// is a fact; running is derived; "pending" is what is left.
func StepBadge(s *api.StepState) Badge {
	switch {
	case s == nil:
		return Badge{Label: "unknown", Tone: ToneNeutral}
	case s.Paused:
		return Badge{Label: "paused", Tone: ToneRunning}
	case s.State != "":
		return Badge{Label: string(s.State), Tone: StateTone(s.State)}
	case s.Running():
		return Badge{Label: "running", Tone: ToneRunning}
	default:
		return Badge{Label: "pending", Tone: ToneNeutral}
	}
}

// StateTone maps a terminal state to its tone, through api.State's own
// predicates rather than a list, so a state added to api is toned
// correctly without editing this. An unknown state comes out neutral, the
// fold's own forward-compatible stance.
func StateTone(s api.State) Tone {
	switch {
	case s.Failed():
		return ToneFailed
	case s == api.StateSucceeded, s == api.StateRecovered:
		return ToneOK
	case s.Terminal():
		// Cached, cancelled and the three kinds of skip. None of them is a
		// failure and none of them is work that ran: neutral is exactly
		// what they are.
		return ToneNeutral
	default:
		return ToneNeutral
	}
}

// RunBadge describes the run as a whole. RunInfo.Status is preferred over
// anything derived: it is the engine's own verdict, taken verbatim from
// run.finished, and authoritative where the two disagree.
func RunBadge(st *api.RunState) Badge {
	if st == nil {
		return Badge{Label: "loading", Tone: ToneNeutral}
	}
	if st.Run.Status != "" {
		return Badge{Label: string(st.Run.Status), Tone: RunStatusTone(st.Run.Status)}
	}
	if st.Run.Done {
		return Badge{Label: "done", Tone: ToneNeutral}
	}
	if len(st.Steps) == 0 {
		return Badge{Label: "waiting", Tone: ToneNeutral}
	}
	return Badge{Label: "running", Tone: ToneRunning}
}

// RunStatusTone maps a run's rolled-up outcome to its tone.
func RunStatusTone(s api.RunStatus) Tone {
	switch s {
	case api.RunSucceeded:
		return ToneOK
	case api.RunSucceededWithRecovery:
		return ToneOK
	case api.RunFailed, api.RunPartial:
		return ToneFailed
	default:
		return ToneNeutral
	}
}

// Count is one figure in the summary strip.
type Count struct {
	Label string
	N     int
	Tone  Tone
}

// Counts summarises the run. The categories are deliberately not mutually
// exclusive and do not sum to the total (as api.GroupCounts documents): a
// cached step is done, a recovered step succeeded after failing, and a pie
// would be a claim the fold never made.
func Counts(st *api.RunState) []Count {
	if st == nil {
		return nil
	}
	var total, running, failed, cached, done int
	for _, id := range st.Order {
		s := st.Steps[id]
		if s == nil {
			continue
		}
		total++
		switch {
		case s.State.Failed():
			failed++
		case s.State == api.StateCached:
			cached++
		case s.Running():
			running++
		}
		if s.State.Terminal() {
			done++
		}
	}
	out := []Count{
		{Label: "steps", N: total, Tone: ToneNeutral},
		{Label: "done", N: done, Tone: ToneNeutral},
		{Label: "running", N: running, Tone: ToneRunning},
		{Label: "cached", N: cached, Tone: ToneNeutral},
		{Label: "failed", N: failed, Tone: ToneFailed},
	}
	return out
}

// Row is one line in the step list.
type Row struct {
	ID string
	// Child reports whether this step was created by an expansion, and is
	// what a renderer indents on. Taken from StepState.Group, which the
	// fold sets both from plan.expanded and from step.created.
	Child bool
	Badge Badge
	// Duration is a human-readable elapsed time, empty for a step that has
	// not started.
	Duration string
	// Needs is the step's dependencies, joined, empty when it has none.
	Needs string
}

// Rows lists the run's steps in the order the fold recorded them:
// RunState.Order, not a sort, so every renderer lays a run out the same
// way. Handlers are not rows: they are not plan nodes, nothing depends on
// them, and counting them as steps would make every count wrong; a step's
// handlers show in its detail instead.
func Rows(st *api.RunState, now time.Time) []Row {
	if st == nil {
		return nil
	}
	rows := make([]Row, 0, len(st.Order))
	for _, id := range st.Order {
		s := st.Steps[id]
		if s == nil {
			continue
		}
		rows = append(rows, Row{
			ID:       id,
			Child:    s.Group != "",
			Badge:    StepBadge(s),
			Duration: Elapsed(s.Started, s.Finished, now),
			Needs:    joinNeeds(s.Needs),
		})
	}
	return rows
}

func joinNeeds(needs []string) string {
	if len(needs) == 0 {
		return ""
	}
	out := "needs " + needs[0]
	for _, n := range needs[1:] {
		out += ", " + n
	}
	return out
}

// Elapsed renders how long a step took, or has been taking. A step with no
// start renders empty rather than "0s": pending work must not look
// instantaneous. now is a parameter rather than a time.Now call so this is
// a pure function.
func Elapsed(started, finished, now time.Time) string {
	if started.IsZero() {
		return ""
	}
	end := finished
	if end.IsZero() {
		end = now
	}
	d := end.Sub(started)
	if d < 0 {
		// The engine never finishes before it starts, but clock skew
		// between engine and client could show it; a negative duration
		// would be a claim about causality.
		return ""
	}
	switch {
	case d < time.Second:
		return fmt.Sprintf("%dms", d.Milliseconds())
	case d < time.Minute:
		return fmt.Sprintf("%.1fs", d.Seconds())
	case d < time.Hour:
		return fmt.Sprintf("%dm%02ds", int(d.Minutes()), int(d.Seconds())%60)
	default:
		return fmt.Sprintf("%dh%02dm", int(d.Hours()), int(d.Minutes())%60)
	}
}

// Field is one label and value in the detail pane.
type Field struct {
	Label string
	Value string
}

// Detail describes one step, for the pane beside the list.
//
// Empty values are omitted rather than rendered blank: a pane full of dashes
// says less than a short pane, and every field here is genuinely absent
// rather than zero for some step somewhere.
func Detail(st *api.RunState, id string, now time.Time) []Field {
	if st == nil {
		return nil
	}
	s := st.Steps[id]
	if s == nil {
		return nil
	}
	fields := []Field{{Label: "state", Value: StepBadge(s).Label}}
	if s.Kind != "" {
		fields = append(fields, Field{Label: "kind", Value: s.Kind})
	}
	if s.Attempt > 0 {
		fields = append(fields, Field{Label: "attempt", Value: fmt.Sprint(s.Attempt)})
	}
	if d := Elapsed(s.Started, s.Finished, now); d != "" {
		fields = append(fields, Field{Label: "took", Value: d})
	}
	if s.Cached {
		fields = append(fields, Field{Label: "cached", Value: "restored from cache"})
	}
	// Zero is a real exit code, so this is shown only when the step ended
	// with a non-zero one; a successful step reporting "exit 0" is noise
	// and a running step reporting it would be wrong.
	if s.ExitCode != 0 {
		fields = append(fields, Field{Label: "exit", Value: fmt.Sprint(s.ExitCode)})
	}
	if s.Group != "" {
		fields = append(fields, Field{Label: "group", Value: s.Group})
	}
	if n := joinNeeds(s.Needs); n != "" {
		fields = append(fields, Field{Label: "deps", Value: n[len("needs "):]})
	}
	if s.Error != "" {
		fields = append(fields, Field{Label: "error", Value: s.Error})
	}
	for _, name := range logStreams(s) {
		fields = append(fields, Field{
			Label: name,
			Value: fmt.Sprintf("%d bytes", s.LogBytes[name]),
		})
	}
	for _, h := range s.Handlers {
		hs := st.Handlers[h]
		if hs == nil {
			continue
		}
		fields = append(fields, Field{Label: handlerLabel(hs), Value: handlerValue(hs)})
	}
	if g := st.Group(id); g.Total > 0 {
		fields = append(fields, Field{
			Label: "expanded",
			Value: fmt.Sprintf("%d children, %d done, %d running, %d cached, %d failed",
				g.Total, g.Done, g.Running, g.Cached, g.Failed),
		})
	}
	return fields
}

// logStreams lists a step's log streams in a stable order, so the detail
// pane does not reshuffle itself between frames on map iteration order.
func logStreams(s *api.StepState) []string {
	if len(s.LogBytes) == 0 {
		return nil
	}
	names := make([]string, 0, len(s.LogBytes))
	for k := range s.LogBytes {
		names = append(names, k)
	}
	sort.Strings(names)
	return names
}

func handlerLabel(h *api.HandlerState) string {
	if h.Kind != "" {
		return h.Kind
	}
	return "handler"
}

// handlerValue describes one handler, and says the thing that matters:
// a handler that started, never finished, and whose run is over is cleanup
// that was abandoned, which may mean a lock is still held somewhere. That
// is a different fact from a handler that succeeded quietly, and it is the
// reason the handler events exist at all.
func handlerValue(h *api.HandlerState) string {
	switch {
	case h.State != "":
		if h.Error != "" {
			return h.ID + ": " + string(h.State) + " (" + h.Error + ")"
		}
		return h.ID + ": " + string(h.State)
	case h.Running():
		return h.ID + ": running"
	default:
		return h.ID + ": never reported finishing"
	}
}

// Subtitle is the line under the run's name: what it is and where it came
// from, in the order somebody scanning it needs.
func Subtitle(st *api.RunState) string {
	if st == nil {
		return "connecting"
	}
	out := st.Run.Pipeline
	if out == "" {
		out = "pipeline"
	}
	if st.Run.EngineVersion != "" {
		out += " · engine " + st.Run.EngineVersion
	}
	if st.Run.CleanupAbandoned {
		// Surfaced here rather than buried in a step's detail because it is
		// a run-level fact with consequences outside the run: cleanup did
		// not finish, so something it holds may still be held.
		out += " · cleanup abandoned"
	}
	return out
}

// Action is one control the page offers, already decided: an op name, the
// step it names (empty for a run-scoped op), and how to label it. WHICH
// actions to offer is decided here, not in the DOM code: a pure function
// of folded state, and a browser is a bad place to find a mistake in it.
// view.go draws whatever this returns.
type Action struct {
	// Op is the api control operation name, sent verbatim.
	Op string
	// Step is the argument for a step-scoped op, and "" for a run-scoped
	// one. The renderer passes it back untouched.
	Step string
	// Label is what the button says.
	Label string
	// Tone picks the button's colour from the same four the rest of this
	// package uses.
	Tone Tone
	// Confirm marks an action that ends something rather than adjusting it.
	// The page asks before sending these.
	Confirm bool
}

// RunActions lists the run-scoped controls to offer. Nothing at all once
// the run is done: a Cancel button the engine will refuse is worse than no
// button. Derived from RunInfo.Paused and Done, so the buttons follow a
// pause applied from the TUI or CLI without this page being told
// separately.
func RunActions(st *api.RunState) []Action {
	if st == nil || st.Run.ID == "" || st.Run.Done {
		return nil
	}
	out := make([]Action, 0, 2)
	if st.Run.Paused {
		out = append(out, Action{Op: api.OpRunResume, Label: "Resume", Tone: ToneOK})
	} else {
		out = append(out, Action{Op: api.OpRunPause, Label: "Pause", Tone: ToneNeutral})
	}
	out = append(out, Action{Op: api.OpRunCancel, Label: "Cancel run", Tone: ToneFailed, Confirm: true})
	return out
}

// StepActions lists the controls to offer for one selected step. Every
// entry is gated on the engine actually accepting it in that condition: a
// button that produces a refusal teaches an operator to distrust the
// buttons.
//
//   - A step held at a breakpoint gets Release and nothing else: it has
//     not run, and it is already stopped.
//   - A finished step gets Retry and Rerun from here (OpRunRerunFrom is
//     refused for a step that never ran).
//   - A step that has not started gets Skip and a breakpoint.
//   - A running step gets nothing: retry and rerun_from are refused
//     mid-flight, and "stop this one step" does not exist; cancel is
//     run-wide.
func StepActions(st *api.RunState, id string) []Action {
	if st == nil || id == "" || st.Run.Done {
		return nil
	}
	s := st.Steps[id]
	if s == nil {
		return nil
	}

	if s.Paused {
		return []Action{{Op: api.OpBreakpointClear, Step: id, Label: "Release", Tone: ToneOK}}
	}
	if s.Running() {
		return nil
	}
	if s.State.Terminal() {
		return []Action{
			{Op: api.OpStepRetry, Step: id, Label: "Retry", Tone: ToneNeutral},
			{Op: api.OpRunRerunFrom, Step: id, Label: "Rerun from here", Tone: ToneNeutral, Confirm: true},
		}
	}
	return []Action{
		{Op: api.OpBreakpointSet, Step: id, Label: "Break before", Tone: ToneNeutral},
		{Op: api.OpStepSkip, Step: id, Label: "Skip", Tone: ToneNeutral, Confirm: true},
	}
}
