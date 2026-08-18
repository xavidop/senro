package engine

import (
	"context"
	"sync"
	"time"

	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/internal/eventlog"
	"github.com/xavidop/senro/internal/plan"
	"github.com/xavidop/senro/internal/sink"
)

// A control request is a request, not a command: this file is where the
// engine decides whether to honour it. Every refusal gets its own short,
// machine-readable reason (Frame.Error / ControlResponse.Error) so a client
// can show something specific and a test can assert which case fired.
//
// Every ACCEPTED operation, never a refused one, also lands in the stream
// as control.applied (api.ControlAppliedBody) with the requesting client's
// id: a refusal changes nothing about the run, so the ledger has nothing to
// say about it.
//
// reasonRunFinished lives in package sink, not here: attachsrv answers with
// the identical string from a different code path (Hub.Done()); see
// sink.ReasonRunFinished.
const (
	reasonUnknownOp        = "unknown_op"
	reasonAlreadyCancelled = "already_cancelled"
	reasonRunNotActive     = "run_not_active"
	reasonMissingStep      = "missing_step"
	reasonUnknownStep      = "unknown_step"
	reasonStepRunning      = "step_running"
	reasonStepNotFailed    = "step_not_failed"

	// reasonStepSettled refuses a step.skip for a step already terminal: a
	// step that ran cannot be un-run, and rewriting its outcome to
	// skipped_manual would make the ledger disagree with the log files, the
	// cache entry and the artifacts it produced. It refuses a ws.snapshot for
	// the same step for the neighbouring reason: that step's own snapshot at
	// settle time IS the record of what it produced.
	reasonStepSettled = "step_settled"

	// reasonBreakpointExists and reasonNoBreakpoint refuse a breakpoint
	// request that disagrees with what is already armed. Refusals rather
	// than silent no-ops: "clear" IS the release, and ok:true for clearing a
	// breakpoint never set tells a client the run is moving again when
	// nothing changed (the reasoning run.cancel's already_cancelled is
	// built on).
	reasonBreakpointExists = "breakpoint_exists"
	reasonNoBreakpoint     = "no_breakpoint"

	// reasonAlreadyPaused and reasonNotPaused refuse a run.pause/run.resume
	// that disagrees with what the run is already doing; refusals rather
	// than silent no-ops for the same reason the breakpoint pair are.
	reasonAlreadyPaused = "already_paused"
	reasonNotPaused     = "not_paused"

	// reasonStepNotSettled refuses a run.rerun_from for a step that has not
	// run yet: it is already going to run, and ok:true would claim a change
	// where nothing changed. The mirror image of reasonStepSettled.
	reasonStepNotSettled = "step_not_settled"

	// reasonMissingProposal, reasonUnknownProposal and reasonProposalSettled
	// are the three ways an analysis decision can name nothing this run can
	// decide: no id, an unknown id, an id already decided. The third is a
	// refusal rather than silent success on purpose: two operators pressing
	// 'a' on the same proposal must not retry the step twice or put two
	// analysis.applied events in the ledger.
	reasonMissingProposal = "missing_proposal"
	reasonUnknownProposal = "unknown_proposal"
	reasonProposalSettled = "proposal_settled"

	// reasonNoRemedy refuses accepting a proposal with nothing to apply (an
	// advisory or out-of-vocabulary remedy; see api.Remedy). Refused rather
	// than accepted as a no-op, because analysis.applied has to mean
	// something happened. An advisory proposal can still be rejected, which
	// is how an operator clears it.
	reasonNoRemedy = "no_remedy"

	// reasonNoWorkspace refuses a ws.snapshot for a step with nothing this
	// coordinator can capture: no workspace mount at all, or only
	// claim-backed ones, whose content lives in the cluster (see
	// capturableWorkspaces). Refused rather than accepted as a no-op, for
	// reasonNoRemedy's reason: ws.snapshot has to mean a snapshot happened.
	reasonNoWorkspace = "no_workspace"

	// reasonSnapshotFailed reports a forced snapshot that could not be
	// taken. Unlike every reason above, nothing was wrong with the REQUEST;
	// it is shell.go's reasonSandbox in this file's vocabulary, and like a
	// refusal it leaves the ledger untouched.
	reasonSnapshotFailed = "snapshot_failed"
)

// breakpoint is one armed pause: a step the scheduler must not dispatch
// until a client clears it.
//
// The map of these (schedHandle.breakpoints) is owned by the scheduler
// goroutine ALONE and carries no lock: serveControl and readySet, its only
// touchers, both run on schedule's own loop. Anything that starts touching
// these from anywhere else needs one.
type breakpoint struct {
	// clientID is who armed it, carried into breakpoint.hit so every
	// attached client can see who stopped the run.
	clientID string
	// hit records that breakpoint.hit was already emitted for THIS arming:
	// a held node is re-examined every pass, so without this one pause
	// would become an unbounded stream of identical events.
	hit bool
}

// schedHandle is what a control handler needs from schedule's own locals:
// the plan, the shared states/running maps and the ONE mutex guarding both
// them and rc.oc.finished/attempts (mu is literally &rc.oc.mu, see
// schedule), the breakpoints and pause flag (unlocked; see breakpoint), the
// semaphore pair, the wait group a dispatched retry must join, signal, and
// what runAttempt needs (logs, opts).
type schedHandle struct {
	ctx         context.Context
	p           *plan.Plan
	byID        map[string]*plan.Node
	mu          *sync.Mutex
	states      map[string]api.State
	running     map[string]bool
	breakpoints map[string]*breakpoint
	// paused points at schedule's own pause flag so handleRunPause writes
	// the value the very next pass reads: a fresh schedHandle is built per
	// request, and a copied bool would let a client pause a run that
	// carried on regardless. Unlocked for the reason the breakpoints map
	// is: one goroutine.
	paused  *bool
	wg      *sync.WaitGroup
	acquire func(context.Context) error
	release func()
	signal  func()
	logs    *eventlog.LogSet
	opts    Options
}

// serveControl dispatches one request to its op-specific handler, or
// refuses an op this build does not recognise. Called from exactly one
// place (schedule's own loop), so two requests are never handled
// concurrently, which is what makes run.cancel's idempotency (see
// handleRunCancel) fall out of sequential logic rather than its own
// locking.
func (rc *runCore) serveControl(h schedHandle, req sink.ControlRequest) {
	switch req.Op {
	case api.OpRunCancel:
		rc.handleRunCancel(h, req)
	case api.OpStepRetry:
		rc.handleStepRetry(h, req)
	case api.OpStepSkip:
		rc.handleStepSkip(h, req)
	case api.OpBreakpointSet, api.OpBreakpointClear:
		rc.handleBreakpoint(h, req)
	case api.OpRunPause, api.OpRunResume:
		rc.handleRunPause(h, req)
	case api.OpRunRerunFrom:
		rc.handleRerunFrom(h, req)
	case api.OpAnalysisAccept:
		rc.handleAnalysisAccept(h, req, false)
	case api.OpAnalysisReject:
		rc.handleAnalysisReject(h, req, false)
	case api.OpWSSnapshot:
		rc.handleWSSnapshot(h, req)
	default:
		refuse(req, reasonUnknownOp)
	}
}

// handleAnalysisAccept is the gate: the one place an analyzer's proposal
// turns into something the run actually does.
//
// policy says the decision came from the caller's configured policy
// (senro.AcceptWithoutHumanApproval) rather than an attached client. A
// parameter rather than an inference from an empty ClientID: "no human
// decided this" is too important to be spelled as the absence of something
// else. It travels into api.AnalysisDecisionBody.Policy and stops there.
//
// The remedy is performed by exactly the code that serves api.OpStepRetry,
// refusals included: accepting a proposal grants an analyzer nothing an
// attached operator could not already do with one keystroke, and everything
// senro will not do on an analyzer's say-so is simply not in api.Remedy's
// vocabulary.
//
// Event order is deliberate: control.applied, then step.retried, then
// analysis.applied last, because it alone asserts the remedy was carried
// out, and by then it has been.
func (rc *runCore) handleAnalysisAccept(h schedHandle, req sink.ControlRequest, policy bool) {
	p, ok := rc.resolveProposal(req)
	if !ok {
		return
	}
	if !p.body.Remedy.Applicable() {
		refuse(req, reasonNoRemedy)
		return
	}

	// Args is rewritten, never merged: the step handed to the retry path is
	// taken from this engine's own record of the proposal, never from the
	// client, which could otherwise retry any step by calling the proposal
	// something else. req.Op is left alone, so control.applied records
	// analysis.accept.
	req.Args = map[string]string{"step": p.step}
	if !rc.handleStepRetry(h, req) {
		// Refused, and handleStepRetry already said why on req.Reply. The
		// proposal stays undecided: an operator who tries again later should
		// find it still there.
		return
	}

	rc.analysis.settle(p.id)
	rc.emitAnalysisDecision(api.AnalysisApplied, p, req, policy, "")
}

// handleAnalysisReject declines a proposal. It performs nothing, so the
// step's state is irrelevant and it can never be refused for a step-related
// reason. The schedHandle is unused but taken so servePolicy and
// serveControl dispatch both analysis operations through one shape.
func (rc *runCore) handleAnalysisReject(_ schedHandle, req sink.ControlRequest, policy bool) {
	p, ok := rc.resolveProposal(req)
	if !ok {
		return
	}
	rc.analysis.settle(p.id)
	rc.emitControlApplied(req, map[string]string{"id": p.id})
	rc.emitAnalysisDecision(api.AnalysisRejected, p, req, policy, "declined")
	req.Reply <- sink.ControlResponse{ID: req.ID, OK: true}
}

// resolveProposal runs the checks both analysis operations share and
// refuses req with the matching reason if any fails. The mirror of
// resolveStep: it is what guarantees control.applied's Args come from an id
// VALIDATED against this run's pending set rather than client-supplied
// bytes.
func (rc *runCore) resolveProposal(req sink.ControlRequest) (*proposal, bool) {
	id := req.Args["id"]
	if id == "" {
		refuse(req, reasonMissingProposal)
		return nil, false
	}
	if rc.analysis == nil {
		// No analyzer, so no proposal exists. Unknown rather than a
		// distinct "no analyzer" reason: the answer is the same whatever
		// the reason the proposal does not exist.
		refuse(req, reasonUnknownProposal)
		return nil, false
	}
	p, ok := rc.analysis.take(id)
	if !ok {
		// take reports "never existed" and "already decided" both as false;
		// the two are told apart here, once.
		if rc.analysis.known(id) {
			refuse(req, reasonProposalSettled)
		} else {
			refuse(req, reasonUnknownProposal)
		}
		return nil, false
	}
	return p, true
}

// emitAnalysisDecision records that somebody decided about a proposal.
// Never called for a refusal, matching emitControlApplied.
func (rc *runCore) emitAnalysisDecision(
	typ api.Type, p *proposal, req sink.ControlRequest, policy bool, reason string,
) {
	rc.emit(api.Event{
		Type: typ, Step: p.step, Attempt: p.attempt,
		Payload: mustMarshal(api.AnalysisDecisionBody{
			ID: p.id, ClientID: req.ClientID, Policy: policy,
			Remedy: p.body.Remedy, Reason: reason,
		}),
	})
}

// resolveStep runs the three checks EVERY step-scoped control operation
// shares, refusing req with the matching reason on failure: the request
// names a step (reasonMissingStep), the step exists in the plan
// (reasonUnknownStep), and the run is not already cancelling
// (reasonRunNotActive: acting during teardown would race shutdown.go's
// sequence).
//
// One function so the checks cannot drift per handler, and so
// control.applied's Args are reconstructed from a step id VALIDATED against
// the plan rather than client-supplied bytes. The checks live in
// resolveShellStep (shell.go), SHARED with the shell path, which needs the
// identical three on a different channel.
func (rc *runCore) resolveStep(h schedHandle, req sink.ControlRequest) (*plan.Node, bool) {
	n, reason := resolveShellStep(h.ctx, h.byID, req.Args["step"])
	if reason != "" {
		refuse(req, reason)
		return nil, false
	}
	return n, true
}

// emitControlApplied records one ACCEPTED control operation in the ledger,
// with the requesting client's id. Never called for a refusal (see this
// file's doc).
//
// args is reconstructed by the caller from data already validated against
// the plan, NEVER req.Args: control.applied is a permanent event and
// req.Args is client-supplied JSON. attachsrv's controlArgAllowlist is the
// first layer; this is the second, independent one, which holds even if
// that check is bypassed or a different Sink skips it. run.cancel passes
// nil: it takes no arguments.
func (rc *runCore) emitControlApplied(req sink.ControlRequest, args map[string]string) {
	rc.emit(api.Event{
		Type: api.ControlApplied, Run: rc.runID,
		Payload: mustMarshal(api.ControlAppliedBody{
			Op: req.Op, ClientID: req.ClientID, Args: args,
		}),
	})
}

// unsettleLocked takes one node back out of the settled world so it can run
// again, returning the next attempt number and whatever pending handler
// evidence this consumed.
//
// Shared by step.retry and run.rerun_from: the transition out of settled is
// the same transition, and it is the part with the invariants. Every map it
// touches is covered by the ONE lock (h.mu IS &rc.oc.mu, see schedule), so
// this is a single atomic transition rather than four separate races.
//
// The anchor is TAKEN, read and cleared: whoever unsettles a node consumes
// its pending handler evidence, so nothing is left for a second unsettle to
// falsely supersede again (see outcomes.handlerAnchors).
//
// The caller must hold h.mu.
func (rc *runCore) unsettleLocked(h schedHandle, id string) (nextAttempt int, anchor handlerAnchor, hadAnchor bool) {
	delete(h.states, id)
	delete(rc.oc.finished, id)
	anchor, hadAnchor = rc.oc.handlerAnchors[id]
	delete(rc.oc.handlerAnchors, id)
	// attempts is deliberately NOT cleared: it is the high-water mark of
	// every attempt's events and log files, and resetting it would point
	// the next execution's output at a file the previous one is using.
	return rc.oc.attempts[id] + 1, anchor, hadAnchor
}

// emitHandlerSuperseded marks a completed OnFailure/Always pass as no longer
// describing its step's final outcome, because attempt nextAttempt has
// superseded the one it ran against. See api.HandlerSuperseded's own doc.
func (rc *runCore) emitHandlerSuperseded(id string, nextAttempt int, a handlerAnchor) {
	rc.emit(api.Event{
		Type: api.HandlerSuperseded, Step: id, Attempt: nextAttempt,
		Payload: mustMarshal(api.HandlerSupersededBody{
			SupersededAttempt: a.attempt,
			OnFailure:         a.onFailure,
			Always:            a.always,
		}),
	})
}

// emitStepRetried announces that a step is about to run again under a new
// attempt number. step.retried rather than a new type for both unsettle
// operations: it already means "fresh attempt, stop rendering the last
// one", and api.RunState.Apply already folds it that way (see fold.go's
// StepRetried case). A new type would re-implement all of that to say the
// same thing.
func (rc *runCore) emitStepRetried(id string, attempt int, reason string) {
	rc.emit(api.Event{
		Type: api.StepRetried, Step: id, Attempt: attempt,
		Payload: mustMarshal(api.StepRetriedBody{Attempt: attempt, Reason: reason}),
	})
}

// dependentsClosure returns root and every node that needs it, directly or
// transitively, in plan order. Plan order, not discovery order, so a
// rerun's events land in a deterministic sequence, exactly as schedule
// sorts the ids of nodes that settle in one pass.
func dependentsClosure(nodes []plan.Node, root string) []string {
	in := map[string]bool{root: true}
	// Repeated passes rather than a reverse index plus a queue: a node's
	// dependents can appear before it in p.Nodes, so one pass can miss a
	// chain. The loop runs at most depth times.
	for grew := true; grew; {
		grew = false
		for i := range nodes {
			if in[nodes[i].ID] {
				continue
			}
			for _, need := range nodes[i].Needs {
				if in[need] {
					in[nodes[i].ID] = true
					grew = true
					break
				}
			}
		}
	}
	out := make([]string, 0, len(in))
	for i := range nodes {
		if in[nodes[i].ID] {
			out = append(out, nodes[i].ID)
		}
	}
	return out
}

// handleRerunFrom re-runs a step and everything downstream of it, in a run
// that is still live.
//
// It dispatches nothing itself: it unsettles the nominated step and its
// transitive dependents and hands them back to the scheduler, which runs
// them exactly as it ran them the first time (same readySet, permits,
// runStep with retries, timeouts, cache and handlers). That is the
// difference from step.retry, which dispatches one bare attempt itself.
//
// Beyond resolveStep's shared checks: nothing in the closure may be running
// (reasonStepRunning), checked across the WHOLE closure before anything is
// modified, since unsettling a node whose goroutine is still alive would
// put two executions of one step in flight; and the nominated step must
// have settled (reasonStepNotSettled). Only nodes that actually settled are
// unsettled: a dependent that never ran was already going to run, and a
// step.retried for something never tried would be false.
//
// The cache is not bypassed and does not need to be: only a Pure step
// consults it, and serving an unchanged Pure step's cached result IS
// re-running it as far as anything downstream can tell. The cache.hit stays
// in the stream either way.
//
// Handlers do run again: a rerun is a real second execution through
// runStep, so outcomes.alwaysRun is cleared for the reset nodes and
// claimAlways's "exactly once per node" becomes "exactly once per
// execution" (see its doc). The previous pass is not rewritten;
// handler.superseded marks it stale, exactly as step.retry does.
func (rc *runCore) handleRerunFrom(h schedHandle, req sink.ControlRequest) {
	n, ok := rc.resolveStep(h, req)
	if !ok {
		return
	}
	closure := dependentsClosure(h.p.Nodes, n.ID)

	// One critical section for the check and every unsettle: validating
	// under one lock and mutating under another would let a closure step
	// start in between, precisely the state the check refuses.
	type reset struct {
		id        string
		attempt   int
		anchor    handlerAnchor
		hadAnchor bool
	}
	h.mu.Lock()
	for _, id := range closure {
		if h.running[id] {
			h.mu.Unlock()
			refuse(req, reasonStepRunning)
			return
		}
	}
	if _, settled := h.states[n.ID]; !settled {
		h.mu.Unlock()
		refuse(req, reasonStepNotSettled)
		return
	}
	revived := make([]reset, 0, len(closure))
	for _, id := range closure {
		if _, settled := h.states[id]; !settled {
			continue
		}
		// Cleared so this execution's Always can be claimed and run; see this
		// function's own doc.
		delete(rc.oc.alwaysRun, id)
		attempt, anchor, hadAnchor := rc.unsettleLocked(h, id)
		revived = append(revived, reset{id: id, attempt: attempt, anchor: anchor, hadAnchor: hadAnchor})
	}
	h.mu.Unlock()

	rc.emitControlApplied(req, map[string]string{"step": n.ID})
	reason := "rerun from " + n.ID + " requested via control by client " + req.ClientID
	for _, r := range revived {
		if r.hadAnchor {
			rc.emitHandlerSuperseded(r.id, r.attempt, r.anchor)
		}
		rc.emitStepRetried(r.id, r.attempt, reason)
	}
	req.Reply <- sink.ControlResponse{ID: req.ID, OK: true}

	// No dispatch and no signal: serveControl runs on schedule's own loop,
	// which re-derives its ready set at the top of the very next pass.
}

// handleBreakpoint serves both breakpoint.set and breakpoint.clear: the two
// ops differ in exactly one line, and two copies would be two places to
// keep the refusal codes and the ledger record in step.
//
// Nothing here blocks, and that is the design: a scheduler parked on a
// client's next move would be a deadlock with a nice name. Arming writes
// one map entry and returns; readySet then declines to put that node in
// `ready`, and the scheduler goes back to its ordinary idle wait, the same
// select that reads the control channel. Nothing is held: no goroutine, no
// MaxParallel slot, no lock, no ledger.
//
// A breakpoint stops the scheduler dispatching that node; it does not
// intercept step.retry, which dispatches an attempt directly, and an
// operator explicitly asking for a retry should get one.
//
// A breakpoint may be armed on a step in any state, settled or running
// included: it fires when, and only when, the scheduler next considers
// dispatching the node. Refusing those cases would make "arm the breakpoint
// first, then ask for the work" impossible.
func (rc *runCore) handleBreakpoint(h schedHandle, req sink.ControlRequest) {
	n, ok := rc.resolveStep(h, req)
	if !ok {
		return
	}
	_, armed := h.breakpoints[n.ID]
	switch {
	case req.Op == api.OpBreakpointSet && armed:
		refuse(req, reasonBreakpointExists)
		return
	case req.Op == api.OpBreakpointClear && !armed:
		refuse(req, reasonNoBreakpoint)
		return
	case req.Op == api.OpBreakpointSet:
		h.breakpoints[n.ID] = &breakpoint{clientID: req.ClientID}
	default:
		delete(h.breakpoints, n.ID)
	}
	rc.emitControlApplied(req, map[string]string{"step": n.ID})
	req.Reply <- sink.ControlResponse{ID: req.ID, OK: true}
}

// handleRunPause serves both run.pause and run.resume: it flips one bool
// the scheduler reads at the top of every pass, and nothing else. One
// function for the reason handleBreakpoint is one.
//
// A breakpoint withholds ONE step; a pause withholds the whole plan: from
// the next pass, readySet's answer is computed as usual and simply not
// dispatched (see schedule).
//
// A step mid-flight when the pause lands runs to completion, settles, and
// whatever its outcome decides for the nodes below it still applies,
// because settling is not dispatching. That is a decision, not an omission:
// senro cannot suspend a running command (no checkpoint, live sandbox, open
// logs), so "pause the running step" could only mean kill it, and a pause
// that killed work would be a cancel that lied about being reversible. A
// step's own automatic retry loop keeps going for the same reason: it is
// that execution continuing. The promise is exactly the narrow one: no NEW
// work is dispatched.
//
// Nothing here blocks, exactly as in handleBreakpoint: this writes one bool
// and returns, and the release arrives on the select the scheduler already
// reads between passes.
//
// A pause deliberately does NOT stop step.retry, step.skip or
// run.rerun_from: pausing is how an operator makes a run quiet enough to
// intervene, and those are the interventions. A rerun asked for while
// paused is queued (it goes back through the scheduler) and starts on the
// resume.
func (rc *runCore) handleRunPause(h schedHandle, req sink.ControlRequest) {
	// The same cancelling-run check resolveStep makes for step-scoped ops.
	// run.cancel answers already_cancelled here instead, because cancelling
	// twice is a question about the cancel itself; this is not.
	if h.ctx.Err() != nil {
		refuse(req, reasonRunNotActive)
		return
	}
	want := req.Op == api.OpRunPause
	if *h.paused == want {
		if want {
			refuse(req, reasonAlreadyPaused)
		} else {
			refuse(req, reasonNotPaused)
		}
		return
	}
	*h.paused = want
	// nil args, never req.Args: neither op takes an argument (see
	// emitControlApplied).
	//
	// This control.applied is also what distinguishes a paused run from a
	// hung one in the stream. Breakpoints needed a type of their own
	// (breakpoint.hit) because arming and withholding are separated in time
	// and identity; a pause takes effect the instant it is accepted, so the
	// event recording the request IS the event recording the stop. Clients
	// fold it to api.RunInfo.Paused.
	rc.emitControlApplied(req, nil)
	req.Reply <- sink.ControlResponse{ID: req.ID, OK: true}
}

// announceBreakpoints emits breakpoint.hit for every node just withheld and
// not already announced, in the plan order readySet returned them so two
// runs of the same plan produce the same ledger. Called from schedule's
// loop, on the goroutine that owns the map (see breakpoint.hit).
func (rc *runCore) announceBreakpoints(breakpoints map[string]*breakpoint, held []string) {
	for _, id := range held {
		bp := breakpoints[id]
		if bp == nil || bp.hit {
			continue
		}
		bp.hit = true
		rc.emit(api.Event{
			Type: api.BreakpointHit, Step: id,
			Payload: mustMarshal(api.BreakpointHitBody{ClientID: bp.clientID}),
		})
	}
}

// handleStepSkip takes a step out of the run: it settles as
// api.StateSkippedManual without ever being dispatched, and so does
// everything that needs it, transitively.
//
// Beyond resolveStep's checks: the step must not be running
// (reasonStepRunning: a mid-attempt step cannot be un-started) and must not
// have settled (reasonStepSettled).
//
// Dependents settle as StateSkippedManual too, not
// StateSkippedUpstreamFailed, and ContinueOnError does not rescue them:
// this is readySet's existing rule for a condition-skipped upstream,
// reached through the SAME skipPropagation table. The upstream never ran
// and nothing broke, so RollUp treats it as clean; blaming dependents would
// report a partial run for a run in which every step did exactly what the
// operator asked. Only transitive dependents are affected; unrelated
// branches run to completion.
//
// The states map and the settle claim move together in one critical section
// under h.mu (which IS &rc.oc.mu, see schedule): writing states and then
// claiming through oc.settle would leave a window in which teardown's
// settleAbandoned claims first and emits `cancelled` for a node already
// recorded as skipped, leaving the ledger and the map permanently
// disagreeing. Refuse rather than half-apply.
func (rc *runCore) handleStepSkip(h schedHandle, req sink.ControlRequest) {
	n, ok := rc.resolveStep(h, req)
	if !ok {
		return
	}
	stepID := n.ID

	h.mu.Lock()
	if h.running[stepID] {
		h.mu.Unlock()
		refuse(req, reasonStepRunning)
		return
	}
	// Both maps, not just states: a step settled but still inside its own
	// uncancellable Always handler is in oc.finished and not yet in
	// states, and that step has run (see outcomes.finished).
	_, settled := h.states[stepID]
	if _, done := rc.oc.finished[stepID]; settled || done {
		h.mu.Unlock()
		refuse(req, reasonStepSettled)
		return
	}
	h.states[stepID] = api.StateSkippedManual
	rc.oc.finished[stepID] = api.StateSkippedManual
	h.mu.Unlock()

	rc.emitControlApplied(req, map[string]string{"step": stepID})
	// stepFinishedEvent, not emitStepFinished: the claim oc.settle would take
	// has already been taken above, atomically with h.states. See its doc.
	rc.stepFinishedEvent(stepID, api.StateSkippedManual, "skipped manually by client "+req.ClientID)
	req.Reply <- sink.ControlResponse{ID: req.ID, OK: true}
}

// handleWSSnapshot captures every workspace a step mounts, on demand, so an
// operator can look at what the step is about to run against.
//
// Beyond resolveStep's shared checks, three. The step must mount something
// this coordinator can capture (reasonNoWorkspace, a property of the plan
// and so decided before anything is claimed). It must not be running
// (reasonStepRunning: a step mid-attempt is writing the very directories
// this reads, which is what narrows the design's "mid-step" framing). And
// it must not have settled (reasonStepSettled: its own snapshot at settle
// time is the authoritative record of what it produced, and a later capture
// under the same step id would describe a directory other steps have since
// written).
//
// So the accepted case is a step that has not run yet, and the useful one
// is a step held at a breakpoint: the run has stopped there, and the
// workspaces are what the step will be given.
//
// Claiming h.running is what makes that guarantee mechanical rather than
// conventional. handleStepRetry claims it for the same reason: while the
// node is running the scheduler will not dispatch it (readySet skips it),
// so the step's own writer cannot start mid-capture, and a second
// ws.snapshot, a step.skip and a step.retry are all refused with
// step_running until this one is done.
//
// The capture itself runs on a goroutine and NOT on schedule's loop, which
// serves every control request one at a time: a workspace is exactly the
// thing that can be gigabytes, and a scheduler parked on a tar of one would
// stall every other step in the run. The reply is sent when the capture
// finishes, as sink.ShellRequest's is, so ok:true means the snapshot is in
// the ledger rather than merely accepted.
func (rc *runCore) handleWSSnapshot(h schedHandle, req sink.ControlRequest) {
	n, ok := rc.resolveStep(h, req)
	if !ok {
		return
	}
	if rc.ws == nil || len(capturableWorkspaces(n)) == 0 {
		refuse(req, reasonNoWorkspace)
		return
	}
	stepID := n.ID

	h.mu.Lock()
	if h.running[stepID] {
		h.mu.Unlock()
		refuse(req, reasonStepRunning)
		return
	}
	// Both maps, exactly as handleStepSkip reads them: a step settled but
	// still inside its uncancellable Always handler is in oc.finished and
	// not yet in states, and that step has run.
	_, settled := h.states[stepID]
	if _, done := rc.oc.finished[stepID]; settled || done {
		h.mu.Unlock()
		refuse(req, reasonStepSettled)
		return
	}
	h.running[stepID] = true
	h.mu.Unlock()

	h.wg.Add(1)
	go func() {
		defer h.wg.Done()
		// Released whatever happens: a node left marked running would never
		// be dispatched and the run would end declaring it cancelled.
		defer func() {
			h.mu.Lock()
			delete(h.running, stepID)
			h.mu.Unlock()
			h.signal()
		}()
		rc.captureForced(h.ctx, n, req)
	}()
}

// captureForced takes the snapshot and puts it in the ledger, or answers
// that it could not and leaves no trace.
//
// All or nothing: every workspace is captured before a single event is
// emitted, so a control.applied never describes a capture that half
// happened. The unreferenced objects a partial capture wrote are simply
// never pinned, and the next sweep reclaims them.
func (rc *runCore) captureForced(ctx context.Context, n *plan.Node, req sink.ControlRequest) {
	snaps, err := rc.ws.forceSnapshot(ctx, n)
	if err != nil {
		refuse(req, reasonSnapshotFailed)
		return
	}
	// Args reconstructed from the validated node, never req.Args (see
	// emitControlApplied). Emitted here rather than on acceptance so
	// control.applied keeps meaning the operation was carried out.
	rc.emitControlApplied(req, map[string]string{"step": n.ID})
	for _, s := range snaps {
		// No Attempt on the envelope: the step has not run one, and a
		// confident 1 would file this under an attempt that never existed.
		// Forced is what keeps `senro ws` reading the run's real snapshots.
		rc.emit(api.Event{
			Type: api.WSSnapshot, Step: n.ID,
			Payload: mustMarshal(api.WSSnapshotBody{
				Name: s.Name, Digest: string(s.Digest), Index: string(s.Index),
				Bytes: s.Bytes, Files: s.Files, Forced: true,
			}),
		})
	}
	req.Reply <- sink.ControlResponse{ID: req.ID, OK: true}
}

// startRefusingControl hands controlCh over, PERMANENTLY, from schedule's
// loop to a persistent goroutine that answers every request with
// sink.ReasonRunFinished until controlStop closes.
//
// "Permanently" matters: a one-shot sweep would leave anything arriving
// during the teardown Always/OnFailure pass (up to 2.5x CleanupGrace)
// hanging until attachsrv's 30s controlTimeout and coming back as a
// bare-text 504, not a refusal Frame.
//
// Called AT MOST ONCE per run (the done and stuck exits are mutually
// exclusive and both stop the loop immediately after), so this never races
// schedule's OWN reads of controlCh: ownership transfers exactly once, at a
// single point in program order. A nil controlCh (no attach server) starts
// nothing: no one can ever send on it.
func (rc *runCore) startRefusingControl(controlCh <-chan sink.ControlRequest, controlStop <-chan struct{}) {
	if controlCh == nil {
		return
	}
	go func() {
		for {
			select {
			case req, ok := <-controlCh:
				if !ok {
					return
				}
				refuse(req, sink.ReasonRunFinished)
			case <-controlStop:
				// Run() is about to return: answer whatever is already
				// queued, non-blocking, then exit. Nothing reads controlCh
				// after this; attachsrv's Hub.Done() precheck closes the
				// window from then on by answering without touching the
				// channel.
				rc.drainControl(controlCh)
				return
			}
		}
	}()
}

// drainControl answers every request already queued on ch with
// sink.ReasonRunFinished, without blocking. Called only from
// startRefusingControl's exit.
func (rc *runCore) drainControl(ch <-chan sink.ControlRequest) {
	for {
		select {
		case req, ok := <-ch:
			if !ok {
				return
			}
			refuse(req, sink.ReasonRunFinished)
		default:
			return
		}
	}
}

func refuse(req sink.ControlRequest, reason string) {
	req.Reply <- sink.ControlResponse{ID: req.ID, OK: false, Error: reason}
}

// handleRunCancel accepts req unless the run is already cancelling:
// ctx.Err() is the one question, since ctx is the run's own context.
//
// Idempotency (two run.cancels, exactly one cancellation and one
// control.applied) falls directly out of serveControl's single-threaded
// dispatch: rc.cancel() marks ctx done synchronously, so a second request
// is refused deterministically, every time. See
// TestControlRunCancelRaceIsSafe.
func (rc *runCore) handleRunCancel(h schedHandle, req sink.ControlRequest) {
	if h.ctx.Err() != nil {
		refuse(req, reasonAlreadyCancelled)
		return
	}
	// nil args, never req.Args: run.cancel takes no arguments (see
	// emitControlApplied).
	rc.emitControlApplied(req, nil)
	rc.cancel()
	req.Reply <- sink.ControlResponse{ID: req.ID, OK: true}
}

// handleStepRetry accepts req only when, beyond resolveStep's three checks,
// the step is not currently running (reasonStepRunning: two attempts of one
// step alive at once could not say whose log lines and exit code are whose)
// and its last recorded state is a FAILED one per api.State.Failed
// (reasonStepNotFailed).
//
// Accepted, it schedules exactly ONE new attempt: not attempt.go's full
// backoff/predicate loop, and deliberately not another OnFailure/Always
// pass, since outcomes.claimAlways is built on "exactly once per node". If
// the superseded attempt's handlers already ran, handler.superseded marks
// that evidence stale (see the event type). The prior attempt's events and
// log files are untouched: this only ADDS a fresh attempt number
// (rc.oc.attempts[id]+1), it never rewrites what is on disk or in the
// ledger.
//
// The new attempt runs on its own goroutine, tracked by h.wg and respecting
// h.acquire/h.release, exactly like an ordinary scheduled step. The bool
// reports whether the retry was ACCEPTED: serveControl ignores it;
// handleAnalysisAccept needs it, and must not reply on req.Reply a second
// time when this function has already refused.
func (rc *runCore) handleStepRetry(h schedHandle, req sink.ControlRequest) bool {
	n, ok := rc.resolveStep(h, req)
	if !ok {
		return false
	}
	stepID := n.ID

	h.mu.Lock()
	if h.running[stepID] {
		h.mu.Unlock()
		refuse(req, reasonStepRunning)
		return false
	}
	st, settled := h.states[stepID]
	if !settled || !st.Failed() {
		h.mu.Unlock()
		refuse(req, reasonStepNotFailed)
		return false
	}
	// Accepted: unsettle so the node can be scheduled again. running is set
	// here, not inside unsettleLocked, because it is the one part
	// rerun_from does NOT share: this function dispatches the new attempt
	// immediately, so the node is busy from this instant.
	h.running[stepID] = true
	nextAttempt, anchor, hadAnchor := rc.unsettleLocked(h, stepID)
	h.mu.Unlock()

	// Args reconstructed from the validated stepID, never req.Args (see
	// emitControlApplied).
	rc.emitControlApplied(req, map[string]string{"step": stepID})

	// hadAnchor is true only when a handler pass GENUINELY RAN against an
	// earlier, not-yet-superseded attempt, taken from
	// outcomes.handlerAnchors, never derived from the static
	// n.OnFailure/n.Always declaration: a manually retried attempt bypasses
	// runStep and never runs handlers, so keying off the declaration made a
	// SECOND step.retry emit a second, false handler.superseded.
	// unsettleLocked's take-and-clear guarantees this fires at most once
	// per real handler pass.
	if hadAnchor {
		rc.emitHandlerSuperseded(stepID, nextAttempt, anchor)
	}

	// Reason names the requesting ClientID (server-assigned; see attachsrv)
	// rather than implying a retry predicate that was never evaluated.
	rc.emitStepRetried(stepID, nextAttempt, "requested via control by client "+req.ClientID)
	req.Reply <- sink.ControlResponse{ID: req.ID, OK: true}

	h.wg.Add(1)
	go func() {
		defer h.wg.Done()
		if err := h.acquire(h.ctx); err != nil {
			// The run was cancelled while this retry waited for a slot:
			// recorded exactly like a queued-but-never-dispatched step (see
			// schedule's cancellation short-circuit), cancelled rather than
			// failed, via emitStepFinished since nothing here opened a
			// sandbox.
			h.mu.Lock()
			h.states[stepID] = api.StateCancelled
			delete(h.running, stepID)
			h.mu.Unlock()
			rc.emitStepFinished(stepID, api.StateCancelled, "")
			h.signal()
			return
		}
		defer h.release()

		start := time.Now()
		res := rc.runAttempt(h.ctx, n, h.opts, h.logs, nextAttempt)
		// See the same call in attempt.go: the attempt's log files are final
		// once runAttempt has returned, and not before.
		rc.archiveAttempt(h.logs, n.ID, nextAttempt)
		finalState := res.state
		if finalState == api.StateSucceeded {
			// Matches the automatic retry loop's rule (runStep): a step
			// that failed at least once and then succeeded is recovered,
			// not succeeded outright.
			finalState = api.StateRecovered
		}
		// cached is always false: this path bypasses runStep and never
		// consults the cache. A manual retry is a request to actually run
		// the step again.
		state := rc.finishStep(n, start, nextAttempt, finalState, res.exitCode, errText(res), false)

		// A manually retried attempt that failed again is offered to the
		// analyzer like any other failure; leaving it out would make the
		// one attempt somebody intervened on the one nothing explained.
		if finalState.Failed() {
			rc.offerAnalysis(n, nextAttempt, finalState, res, time.Since(start), h.opts.Pipeline)
		}

		h.mu.Lock()
		h.states[stepID] = state
		delete(h.running, stepID)
		h.mu.Unlock()
		h.signal()
	}()
	return true
}
