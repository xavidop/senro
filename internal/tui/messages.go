package tui

import (
	"time"

	"github.com/xavidop/senro/api"
)

// StateMsg carries the result of the initial State() snapshot fetch Init
// kicks off. Subscribe cannot start until the snapshot's Seq is known:
// subscribing from Seq+1 is what makes attaching to a long-running run an
// O(1) snapshot plus a tail rather than a full replay.
type StateMsg struct {
	State *api.RunState
	Err   error
}

// EventMsg carries one lifecycle event into the model's fold, exactly as
// Update would fold it from anywhere else; see applyEvent. Tests drive the
// fold through it, as would a caller replaying a hand-built sequence. The
// live subscription does NOT use it: see newSubscribeCmd for why one
// EventMsg per event would defeat the ~30Hz coalescing.
type EventMsg struct{ Event api.Event }

// TickMsg drives the model's fixed ~30Hz render cadence. Events keep
// folding into state as they arrive (see newSubscribeCmd); this is the
// only thing that advances Model.Frame().
type TickMsg time.Time

// ControlResultMsg carries the outcome of one control request. Op and Step
// (Step empty for run-scoped ops) identify which request this answers,
// since more than one can be in flight and the model must not attribute
// one request's answer to another.
type ControlResultMsg struct {
	Op   string
	Step string
	Resp api.Frame
	Err  error
}

// LogChunkMsg carries newly fetched forward (tail) bytes for one step's
// stdout, the result of a range request starting at From. NextOffset is
// where the next forward fetch resumes: the byte after the last delivered,
// so a follow-up never re-fetches or skips.
//
// Attempt is the step's attempt when the fetch was issued. handleLogChunk
// compares it against the CURRENT attempt: a retry can land mid-flight,
// and applying a stale attempt's bytes would resurrect the "pane frozen on
// the dead attempt" bug the retry-reset exists to fix.
type LogChunkMsg struct {
	Step       string
	Data       []byte
	From       int64
	NextOffset int64
	Attempt    int
	Err        error
}

// LogHistoryMsg carries an older chunk of a step's log: scrollback, fetched
// to end exactly where the ring's resident content begins, issued by
// loadOlderLogsCmd on 'pgup'. AtStart reports whether Data begins at the
// file's true offset 0, which tells the ring whether its first line is
// genuine or an artifact of a byte-unaligned boundary. Attempt guards a
// mid-flight retry as it does for LogChunkMsg.
//
// Boundary is the ring's StartOffset() this fetch was issued against, and
// may be stale by arrival: a concurrent Append/trim can move the real one.
// handleLogHistory compares before applying, since a mismatched range no
// longer abuts the ring's front and would corrupt StartOffset itself.
type LogHistoryMsg struct {
	Step     string
	Data     []byte
	AtStart  bool
	Attempt  int
	Boundary int64
	Err      error
}

// SubscribeClosedMsg reports that the lifecycle subscription ended: a
// finished run's replay drained, the model's context was cancelled
// (typically 'q'), or Apply rejected a regressing Seq. Only the last sets
// Err, and newSubscribeCmd treats it as fatal to the subscription,
// matching render.Plain.
type SubscribeClosedMsg struct{ Err error }
