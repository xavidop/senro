package api

import (
	"encoding/json"
	"sort"
	"time"
)

// Version is the current envelope version. A client and engine must agree on
// the major version; a minor mismatch warns rather than failing.
const Version = 1

// Type identifies an event's kind.
//
// The set is additive within a major version. Clients MUST ignore types they
// do not recognise: a newer engine will emit types this build has never
// heard of, and treating that as an error breaks forward compatibility.
type Type string

// The event types this build declares.
const (
	RunStarted  Type = "run.started"
	RunFinished Type = "run.finished"

	PlanResolved         Type = "plan.resolved"
	PlanExpanded         Type = "plan.expanded"
	PlanGenerated        Type = "plan.generated"
	PlanExpansionSkipped Type = "plan.expansion_skipped"

	StepCreated     Type = "step.created"
	StepStarted     Type = "step.started"
	StepFinished    Type = "step.finished"
	StepRetried     Type = "step.retried"
	StepLogAppended Type = "step.log.appended"

	CacheHit   Type = "cache.hit"
	CacheMiss  Type = "cache.miss"
	CacheSaved Type = "cache.saved"

	// CacheDegraded says a SHARED cache stopped being used and the run
	// carried on from local disk alone: the store was unreachable, refused
	// the credentials, or returned something other than what it promised.
	// Neither a failure nor a miss; the run is slower than it should have
	// been and correct regardless.
	//
	// Run-scoped rather than step-scoped: the store is shared by every step,
	// and which step held the connection when it broke is an accident of
	// scheduling.
	//
	// A run with no shared cache configured emits none of these.
	CacheDegraded Type = "cache.degraded"

	WSSnapshot Type = "ws.snapshot"
	WSRestored Type = "ws.restored"

	// WSEvicted says a persistent workspace was emptied because it hit one of
	// its bounds: unused longer than its MaxAge, or content grown past its
	// MaxSize.
	//
	// Run-scoped rather than step-scoped: eviction only ever happens outside
	// every step, at workspace lease or release, so there is never a step to
	// attribute it to.
	//
	// It explains an otherwise silent cold cache: the body names the bound
	// that was hit. Emitted only on eviction, never as a per-run heartbeat; a
	// run with no persistent workspace emits none.
	//
	// Not folded into RunState, for the reason BinaryStaged is not: a fact
	// about a directory on this machine, not a step's outcome. WSSnapshot and
	// WSRestored are not folded either; `senro ws` reads the ledger directly.
	WSEvicted Type = "ws.evicted"

	// BinaryStaged says a copy of the engine's own binary is on an execution
	// target at a content-addressed path, ready to be re-entered there as a
	// step. A registered function's body is compiled into the binary, so
	// running a func step anywhere but the coordinator means staging the
	// binary there first.
	//
	// One per staging attempt. BinaryStagedBody.Reused says whether the
	// upload actually happened: a run whose every func step reports
	// Reused=false is paying the transfer once per step rather than once per
	// host.
	//
	// Not folded into RunState: a fact about a host, not a step's outcome.
	BinaryStaged Type = "binary.staged"

	SecretResolved Type = "secret.resolved"
	SecretRedacted Type = "secret.redacted"

	ControlApplied Type = "control.applied"

	// BreakpointHit says the scheduler has withheld a step it would otherwise
	// have dispatched, because a client armed a breakpoint on it
	// (OpBreakpointSet); the run makes no further progress through that step
	// until the breakpoint is cleared.
	//
	// Emitted once per arming, when the step is first withheld, not once per
	// scheduling pass: a held step is re-examined every time anything else
	// settles, and a per-pass event would be an unbounded stream of identical
	// ones.
	//
	// It is the only thing in the stream that distinguishes a run paused at a
	// breakpoint from a run that has hung: the held step otherwise looks
	// exactly like one still waiting on its dependencies.
	BreakpointHit Type = "breakpoint.hit"

	// A handler emits started and then exactly one of succeeded or failed.
	//
	// Three types rather than one handler.finished carrying a state:
	// started/failed shipped first, and adding a type is additive while
	// replacing one is breaking.
	//
	// handler.succeeded is not redundant with "started and never failed": a
	// handler abandoned when the cleanup grace ran out also leaves started
	// with no failed, and the difference between "cleanup ran" and "a lock
	// may still be held" is why these events exist.
	HandlerStarted   Type = "handler.started"
	HandlerSucceeded Type = "handler.succeeded"
	HandlerFailed    Type = "handler.failed"

	// HandlerSuperseded marks that a step's already-completed OnFailure
	// and/or Always handler run no longer describes the step's final outcome:
	// a later attempt (step.retry) superseded the one those handlers ran
	// against. The handler events themselves are never rewritten or removed;
	// they stay anchored to the attempt that triggered them. This event is
	// what lets a stream reader tell, without inferring it from
	// step.retried's timing, that a prior handler pass no longer speaks for
	// how the step ended.
	HandlerSuperseded Type = "handler.superseded"

	// ShellOpened and ShellClosed bracket one interactive session on a step's
	// workspaces: `senro shell`, or the TUI's 's' key. See ShellOpenedBody,
	// ShellClosedBody, and internal/engine/shell.go.
	//
	// Unlike client.attached/detached, which the attach server observes and
	// so cannot reach the ledger only the engine writes, a shell is hosted by
	// the engine, so these are emitted through runCore.emit like any other
	// engine event.
	//
	// Neither is folded into RunState: a session is a fact about a person and
	// a socket, not a step's outcome.
	ShellOpened Type = "shell.opened"
	ShellClosed Type = "shell.closed"

	// The outcome of one outbound notification, emitted by a notifier the
	// caller wired in (see senro.WithSink and the notify package). A run
	// that configures no notifier emits none.
	//
	// Each describes a delivery, never the event being delivered:
	// NotifyBody.Event names that. A notifier never notifies about a
	// notify.* event, so these three cannot feed themselves.
	//
	// The outcome of delivering run.finished can never appear here:
	// run.finished seals the stream in the same critical section that
	// appends it, so an outcome decided after that has nowhere left to go. A
	// notifier reports that one on standard error at shutdown instead; see
	// the notify package's doc.
	NotifyDelivered Type = "notify.delivered"
	NotifyFailed    Type = "notify.failed"
	NotifyDropped   Type = "notify.dropped"

	// One failed step explained, and the decision somebody made about the
	// explanation. Emitted by a run that wired an analyzer in; see
	// senro.WithAnalyzer and AnalysisProposedBody. A run that configures no
	// analyzer emits none.
	//
	// AnalysisProposed is a SUGGESTION and never means anything happened.
	// AnalysisApplied means somebody accepted one and the engine performed
	// its remedy; AnalysisRejected means somebody declined it, or the engine
	// refused to perform it. A proposal is applied only after an attached
	// client said so or the caller configured
	// senro.AcceptWithoutHumanApproval; AnalysisDecisionBody records the
	// latter as Policy, so a run no human watched can be identified from the
	// ledger alone.
	AnalysisProposed Type = "analysis.proposed"
	AnalysisApplied  Type = "analysis.applied"
	AnalysisRejected Type = "analysis.rejected"
)

// Event types reserved for features that are not built yet. Declared now so
// that emitting them later is an additive change rather than a schema
// revision.
const (
	// ClientAttached and ClientDetached are reserved rather than declared
	// because they cannot yet be emitted from where they occur: the attach
	// server (internal/attachsrv) observes connections, but only the
	// engine's Sink.Emit path writes the ledger, and carrying the event
	// across is work that has not been done.
	ClientAttached Type = "client.attached"
	ClientDetached Type = "client.detached"
)

// declaredTypes are the event types this build declares; every one has at
// least one live emit site in this build. A type naming something unbuilt
// goes in reservedTypes below instead.
//
// Kept as its own set so this package and its tests can answer "what is
// declared" mechanically, from the code, rather than hand-maintaining a
// second list that can silently drift. DeclaredTypes() is what schema_test.go
// checks event.schema.json's type.examples against.
var declaredTypes = map[Type]bool{
	RunStarted: true, RunFinished: true,
	PlanResolved: true, PlanExpanded: true, PlanExpansionSkipped: true,
	PlanGenerated: true,
	StepCreated:   true, StepStarted: true, StepFinished: true,
	StepRetried: true, StepLogAppended: true,
	CacheHit: true, CacheMiss: true, CacheSaved: true, CacheDegraded: true,
	WSSnapshot: true, WSRestored: true, WSEvicted: true, BinaryStaged: true,
	SecretResolved: true, SecretRedacted: true,
	ControlApplied: true, BreakpointHit: true,
	HandlerStarted: true, HandlerSucceeded: true, HandlerFailed: true,
	HandlerSuperseded: true,
	ShellOpened:       true, ShellClosed: true,
	NotifyDelivered: true, NotifyFailed: true, NotifyDropped: true,
	AnalysisProposed: true, AnalysisApplied: true, AnalysisRejected: true,
}

// reservedTypes are declared now (see their own const block's doc) but
// emitted by nothing yet. Known() still recognises them, since a client on
// this build should treat a newer engine's early adoption of one as
// unsurprising, not unknown.
var reservedTypes = map[Type]bool{
	ClientAttached: true, ClientDetached: true,
}

// knownTypes is the union of declaredTypes and reservedTypes, built from the
// two sets above so a type added to either is automatically known.
var knownTypes = func() map[Type]bool {
	m := make(map[Type]bool, len(declaredTypes)+len(reservedTypes))
	for t := range declaredTypes {
		m[t] = true
	}
	for t := range reservedTypes {
		m[t] = true
	}
	return m
}()

// Known reports whether this build recognises the type. It is a diagnostic
// aid, never a gate. Consumers must tolerate unknown types regardless.
func (t Type) Known() bool { return knownTypes[t] }

// DeclaredTypes returns every event Type this build declares, sorted for a
// stable order. It lets tooling and this package's tests check published
// documentation (event.schema.json's type.examples) against the code rather
// than a second hand-maintained list.
func DeclaredTypes() []Type {
	out := make([]Type, 0, len(declaredTypes))
	for t := range declaredTypes {
		out = append(out, t)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// Event is one entry in a run's append-only ledger.
//
// Routing fields are flat so a client can filter without decoding the body.
// The type-specific body lives under Payload, which lets each event type
// evolve additively under its own schema.
type Event struct {
	V       int       `json:"v"`
	Seq     uint64    `json:"seq"`
	TS      time.Time `json:"ts"`
	Type    Type      `json:"type"`
	Run     string    `json:"run,omitempty"`
	Step    string    `json:"step,omitempty"`    // stable base ID, never "id@2"
	Attempt int       `json:"attempt,omitempty"` // 0 when not step-scoped
	Group   string    `json:"group,omitempty"`   // expansion parent, for aggregation

	// TraceID is the W3C trace this run belongs to: 32 lowercase hex
	// characters, never the all-zero value, IDENTICAL on every event of the
	// run and different for every run. See ValidTraceID and NewTraceID.
	//
	// Repeated on every event, rather than stated once in run.started, so a
	// consumer holding one event knows its trace without replaying the
	// stream.
	//
	// Taken from an inbound traceparent when there was a valid one (a CI
	// job, a webhook, a deploy tool that ran senro), and a fresh random ID
	// otherwise.
	//
	// Span IDs live in the payloads (RunStartedBody.SpanID,
	// StepStartedBody.SpanID), because unlike this field they are not
	// constant across the run.
	//
	// Empty on events written by builds older than this field; never since.
	TraceID string `json:"trace_id,omitempty"`

	Payload json.RawMessage `json:"payload,omitempty"`
}

// Decode unmarshals the payload into v. A nil payload is a no-op, so callers
// can decode unconditionally.
func (e Event) Decode(v any) error {
	if len(e.Payload) == 0 {
		return nil
	}
	return json.Unmarshal(e.Payload, v)
}
