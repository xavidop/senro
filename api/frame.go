package api

import (
	"encoding/json"
	"sort"
)

// Kind distinguishes the two frame shapes on the attach socket's control
// channel: a client-initiated request and the server's correlated response.
//
// The shipped server speaks control operations this way and nothing else:
// lifecycle events are not wrapped in frames (GET /api/stream emits bare
// Event NDJSON; see StreamEndMarker), and no frame announces a connection
// close. Only symbols an implementation exists for are declared here; see
// doc.go's stability note.
type Kind string

const (
	KindReq Kind = "req" // client → server control request
	KindRes Kind = "res" // server → client response, correlated by ID
)

// Control operation names. These are wire protocol: renaming one breaks every
// deployed client.
//
// Only ops the engine actually serves are declared: a constant a client can
// reference but no engine will accept is worse than no constant at all.
// Adding one when its handler lands is additive; reserved names live in
// prose in the docs until then.
const (
	OpRunCancel = "run.cancel"
	OpStepRetry = "step.retry"

	// OpStepSkip marks a step as deliberately not run. It takes one
	// argument, "step". The step settles as StateSkippedManual, and so does
	// everything that needs it, directly or transitively: see
	// StateSkippedManual's own doc for why a skip propagates itself rather
	// than blaming its dependents with StateSkippedUpstreamFailed.
	OpStepSkip = "step.skip"

	// OpBreakpointSet arms a breakpoint before a step, and OpBreakpointClear
	// disarms it. Both take one argument, "step".
	//
	// An armed breakpoint stops the scheduler from DISPATCHING that step;
	// the run makes whatever other progress it can. When the scheduler first
	// withholds the step, and only then, the engine emits BreakpointHit
	// once. Clearing makes the step dispatchable again on the next
	// scheduling pass.
	//
	// Clearing is the ONLY release; OpRunCancel is the only other way out. A
	// held run waits indefinitely, on purpose, and nothing inside the engine
	// blocks while it waits. See internal/engine/control.go.
	OpBreakpointSet   = "breakpoint.set"
	OpBreakpointClear = "breakpoint.clear"

	// OpRunPause stops a live run dispatching anything new, and OpRunResume
	// lets it dispatch again. Neither takes an argument: a pause is run-wide.
	//
	// A pause is NOT a breakpoint (which withholds one nominated step) and
	// NOT a cancel: it stops the scheduler STARTING work and does nothing to
	// work already started. A step mid-attempt runs to completion and
	// settles while the run is paused, along with anything its outcome
	// decides. senro cannot suspend a command; a "pause" that killed running
	// work would be a cancel that lied about being reversible.
	//
	// It is not a veto on explicit per-step requests: OpStepRetry dispatches
	// directly rather than through the scheduler and still works while
	// paused. OpRunRerunFrom composes the other way: it hands its nodes back
	// to the scheduler, so a rerun requested while paused starts on resume.
	//
	// Resuming is the ONLY release; OpRunCancel is the only other way out. A
	// paused run waits indefinitely, and nothing inside the engine blocks
	// while it waits. See internal/engine/control.go.
	//
	// A paused run is distinguishable from a hung one by the control.applied
	// event recording the pause: a pause takes effect the instant it is
	// accepted, so the event recording the request IS the event recording
	// the stop. Clients fold it to RunInfo.Paused.
	OpRunPause  = "run.pause"
	OpRunResume = "run.resume"

	// OpRunRerunFrom re-runs a step and everything downstream of it, in a
	// run that is still live. One argument, "step": the step to start from.
	//
	// The nominated step and every step that needs it, directly or
	// transitively, go back to pending and are scheduled again under fresh
	// attempt numbers; nothing outside that set is touched. Each re-run step
	// announces itself with a step.retried carrying its new attempt number,
	// which tells a client's fold to stop rendering it as finished.
	//
	// Refused, never partially applied, if any step in that set is still
	// running, or if the nominated step has not run at all yet.
	OpRunRerunFrom = "run.rerun_from"

	// OpAnalysisAccept accepts a proposal an analyzer made, and
	// OpAnalysisReject declines it. Both take one argument, "id", the ID an
	// analysis.proposed event carried.
	//
	// These two are the gate: an analyzer proposes and can do nothing else.
	// A proposal becomes an action only via one of these from an attached
	// client, or via senro.AcceptWithoutHumanApproval. Accepting emits
	// AnalysisApplied and performs the remedy; rejecting emits
	// AnalysisRejected and performs nothing. Either way the proposal is
	// settled and a second decision is refused, so two operators cannot
	// retry one step twice.
	//
	// Accepting grants an analyzer no power a client did not already have:
	// the only applicable remedy, RemedyRetry, is served by exactly the code
	// OpStepRetry is served by, refusals included.
	OpAnalysisAccept = "analysis.accept"
	OpAnalysisReject = "analysis.reject"

	// OpWSSnapshot captures every workspace a step mounts, on demand, so an
	// operator can look at one mid-run. One argument, "step".
	//
	// The same string as the WSSnapshot EVENT type, deliberately: this
	// operation causes exactly that event, the way breakpoint.set causes
	// breakpoint.hit, and the two vocabularies never share a channel (a
	// Frame carries an op, GET /api/stream carries an event), so one name
	// cannot be ambiguous on the wire.
	//
	// Answerable only for a step that has neither started nor settled, the
	// useful case being one held at a breakpoint: a step mid-attempt is
	// being written while it is read, and a settled step already has the
	// authoritative snapshot its own settling emitted.
	//
	// What it captures is never evidence. WSSnapshotBody.Forced marks it, no
	// cache key and no later mount ever sees its digest, and the step's own
	// snapshot at settle time is unaffected.
	OpWSSnapshot = "ws.snapshot"
)

// declaredOps mirrors the const block above; TestDeclaredOpsMatchesTheConstants
// checks the two against each other by reading this file's source, because a
// hand-mirrored list quietly diverges.
//
// It exists so a consumer can enumerate the ops rather than hard-coding
// them. internal/webui forwards a deliberate subset, and its test fails when
// api declares an op nobody has ruled on, so a new op cannot become
// browser-reachable, or silently unreachable, by default.
var declaredOps = map[string]bool{
	OpRunCancel:       true,
	OpStepRetry:       true,
	OpStepSkip:        true,
	OpBreakpointSet:   true,
	OpBreakpointClear: true,
	OpRunPause:        true,
	OpRunResume:       true,
	OpRunRerunFrom:    true,
	OpAnalysisAccept:  true,
	OpAnalysisReject:  true,
	OpWSSnapshot:      true,
}

// DeclaredOps returns every control operation this build declares, sorted
// for a stable order. Same argument as DeclaredTypes: tooling checks itself
// against the code, not a hand-kept list.
func DeclaredOps() []string {
	out := make([]string, 0, len(declaredOps))
	for op := range declaredOps {
		out = append(out, op)
	}
	sort.Strings(out)
	return out
}

// OpDeclared reports whether this build declares the named control
// operation. A diagnostic aid, never a gate: the engine's own boundary
// decides what it will serve.
func OpDeclared(op string) bool { return declaredOps[op] }

// Frame is one message on the attach socket's control channel.
//
// JSON rather than a binary encoding, so the whole protocol is debuggable with
// websocat. Only per-step log chunks are binary, on their own channel, because
// they are the only volume worth optimising.
type Frame struct {
	V       int             `json:"v"`
	Kind    Kind            `json:"kind"`
	ID      string          `json:"id,omitempty"` // correlation ID for req/res
	Type    string          `json:"type,omitempty"`
	OK      *bool           `json:"ok,omitempty"` // res only
	Error   string          `json:"error,omitempty"`
	Payload json.RawMessage `json:"payload,omitempty"`
}
