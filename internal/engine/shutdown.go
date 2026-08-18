package engine

import (
	"context"
	"sync"
	"time"

	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/internal/eventlog"
	"github.com/xavidop/senro/internal/plan"
)

// defaultCleanupGrace bounds teardown when Options.CleanupGrace is zero:
// longer than any cleanup a pipeline plausibly declares, short enough that
// one wedged handler cannot hold a terminal or CI worker indefinitely.
const defaultCleanupGrace = 60 * time.Second

// cleanupGrace is opts.CleanupGrace, or the default when unset. Negative is
// treated as unset rather than "no grace at all": zero grace kills every
// Always handler before it can do anything, precisely the failure this file
// exists to prevent. A caller who really wants that can pass a millisecond.
func (o Options) cleanupGrace() time.Duration {
	if o.CleanupGrace <= 0 {
		return defaultCleanupGrace
	}
	return o.CleanupGrace
}

// outcomes is the scheduler's shared record of what has settled so far,
// plus the evidence each settled node's final attempt produced. On runCore
// rather than schedule's locals because teardown may read it while schedule
// is still running (see waitForSchedule).
type outcomes struct {
	mu      sync.Mutex
	states  map[string]api.State
	running map[string]bool

	// finished is the terminal state of every node that reached one,
	// recorded AT THE MOMENT IT SETTLED, unlike states, which schedule
	// writes only after runStep returns. The gap between those instants is
	// where a succeeded step used to be recorded as cancelled: settle-time
	// Always is uncancellable, so a step goroutine routinely outlives the
	// abandonment in waitForSchedule, and settleAbandoned then fabricated
	// `cancelled` for a node that had in fact succeeded.
	//
	// Kept separate from states rather than published early into it: states
	// is what readySet reads, and writing an outcome there before handlers
	// finish would let dependents start alongside cleanup. Nothing outside
	// teardown reads this map.
	finished map[string]api.State

	// alwaysRun records which nodes have had their Always handlers run, so
	// the two places able to run them (the node settling, and teardown for
	// nodes that never settled) cannot both run the same list. See
	// claimAlways.
	alwaysRun map[string]bool

	// alwaysInFlight counts settle-time Always handlers running right now.
	// They ignore cancellation, so teardown cannot assume abandonment
	// stopped them: it waits for the count to reach zero before closing
	// the ledger and log set (see waitForSettleTimeCleanup).
	alwaysInFlight int

	// attempts is the highest attempt number each node has actually run,
	// written only by finishStep. Exists for control.go's step.retry,
	// which must continue numbering without reusing a number an earlier
	// attempt's events and logs are already filed under.
	attempts map[string]int

	// handlerAnchors records, per node, the attempt a currently PENDING
	// (not yet superseded) OnFailure/Always run is anchored to: written
	// only from the places that actually invoke those handlers, never from
	// the plan node's static declaration.
	//
	// handleStepRetry used to decide handler.superseded from the
	// DECLARATION, which says nothing about whether the superseded attempt
	// ever ran a handler: a manually retried attempt bypasses runStep and
	// runs none, so a second step.retry emitted a false handler.superseded.
	// handleStepRetry takes (reads and clears) this map atomically under
	// the same lock as states/running/finished/attempts, so the event fires
	// only for a genuine handler pass and never twice for the same one.
	handlerAnchors map[string]handlerAnchor
}

// handlerAnchor is one node's currently pending handler evidence: the
// attempt it is anchored to, and which of OnFailure/Always actually ran
// against it. See outcomes.handlerAnchors' own doc for why this exists.
type handlerAnchor struct {
	attempt   int
	onFailure bool
	always    bool
}

func newOutcomes(nodes int) *outcomes {
	return &outcomes{
		states:         make(map[string]api.State, nodes),
		running:        make(map[string]bool, nodes),
		finished:       make(map[string]api.State, nodes),
		alwaysRun:      make(map[string]bool),
		attempts:       make(map[string]int, nodes),
		handlerAnchors: make(map[string]handlerAnchor),
	}
}

// settle records id's terminal state and reports whether the caller is the
// one that gets to emit its step.finished.
//
// Every step.finished in this package goes through here, which is what
// makes "exactly one step.finished per node" a property of the code. Three
// places settle a node: the scheduler (skip cascade and cancellation
// short-circuit), a step's own finishStep, and teardown's settleAbandoned,
// which runs concurrently with the other two by construction. The first
// caller wins, and that is right in both directions: a step that reached a
// terminal state keeps it, and a node teardown already settled gets no
// second, later event.
func (oc *outcomes) settle(id string, state api.State) bool {
	oc.mu.Lock()
	defer oc.mu.Unlock()
	if _, done := oc.finished[id]; done {
		return false
	}
	oc.finished[id] = state
	return true
}

// recordAttempt notes that id's most recent attempt used number attempt,
// so a later step.retry knows where to continue numbering. Monotonic: a
// stale or out-of-order caller must never move the counter backwards.
func (oc *outcomes) recordAttempt(id string, attempt int) {
	oc.mu.Lock()
	defer oc.mu.Unlock()
	if attempt > oc.attempts[id] {
		oc.attempts[id] = attempt
	}
}

// recordHandlerRan notes that id's OnFailure and/or Always list genuinely
// ran against attempt. Called only from the three places that invoke
// rc.runHandlers, immediately after the call, never from the static
// declaration. The booleans are OR'd in, not overwritten: OnFailure and
// Always are recorded from two call sites that can both fire for one
// settling attempt.
func (oc *outcomes) recordHandlerRan(id string, attempt int, onFailure, always bool) {
	oc.mu.Lock()
	defer oc.mu.Unlock()
	a := oc.handlerAnchors[id]
	a.attempt = attempt
	a.onFailure = a.onFailure || onFailure
	a.always = a.always || always
	oc.handlerAnchors[id] = a
}

// claimAlways reports whether the caller is the one that gets to run id's
// Always handlers, claiming them in the same breath. Teardown's form; a
// step settling uses claimAlwaysAtSettle. Both ask exactly once per node
// and the loser does nothing, making "exactly once" a property of this
// function. Cleanup is not required to be idempotent: the `rm -rf` of a
// scratch directory something else has since reused does not survive twice.
func (oc *outcomes) claimAlways(id string) bool { return oc.claim(id, false) }

// claimAlwaysAtSettle claims id and counts the run in as in-flight, in ONE
// critical section. Claiming first and incrementing afterwards leaves a
// window in which a node is spoken for but invisible to
// waitForSettleTimeCleanup: an abandoned step goroutine arriving there
// starts its handler against a LogSet Run has already closed, the cleanup
// command never runs (ErrClosed), and the handler.failed that would say so
// is dropped by the seal. Atomicity closes it in both directions, given
// teardown claims everything it intends to run before it starts waiting.
func (oc *outcomes) claimAlwaysAtSettle(id string) bool { return oc.claim(id, true) }

func (oc *outcomes) claim(id string, countIn bool) bool {
	oc.mu.Lock()
	defer oc.mu.Unlock()
	if oc.alwaysRun[id] {
		return false
	}
	oc.alwaysRun[id] = true
	if countIn {
		oc.alwaysInFlight++
	}
	return true
}

// alwaysDone closes out one settle-time Always run claimed through
// claimAlwaysAtSettle.
func (oc *outcomes) alwaysDone() {
	oc.mu.Lock()
	defer oc.mu.Unlock()
	oc.alwaysInFlight--
}

func (oc *outcomes) inFlightAlways() int {
	oc.mu.Lock()
	defer oc.mu.Unlock()
	return oc.alwaysInFlight
}

// settledSnapshot copies the finished map under the lock, so teardown can
// read a consistent view of a scheduler that may still be running. It reads
// finished, not states: a step settled but still inside its uncancellable
// Always handler is in finished only (see the field's doc).
func (oc *outcomes) settledSnapshot() map[string]api.State {
	oc.mu.Lock()
	defer oc.mu.Unlock()
	states := make(map[string]api.State, len(oc.finished))
	for id, st := range oc.finished {
		states[id] = st
	}
	return states
}

// waitForSchedule runs the scheduler and returns once every node has
// settled. If the run's context is cancelled first, in-flight steps get
// grace/2 to unwind and whatever has not returned is abandoned. The bound
// is not redundant with cancellation: a killed step whose children left an
// orphan holding the stdout pipe keeps its goroutine inside Sandbox.Run
// until the executor's own grace elapses (localexec.waitDelay). Half the
// budget, so a hung step can never eat the Always handlers' half.
//
// controlStop is closed exactly once, by Run right before it returns, and
// tells startRefusingControl's goroutine when it may stop reading; nothing
// owns the control channel after that, which is why attachsrv's Hub.Done()
// precheck exists on the other side of the wire.
func (rc *runCore) waitForSchedule(
	ctx context.Context,
	p *plan.Plan,
	opts Options,
	logs *eventlog.LogSet,
	grace time.Duration,
	controlStop <-chan struct{},
) (map[string]api.State, error) {
	type result struct {
		states map[string]api.State
		err    error
	}
	// Buffered: on the abandonment path below nothing is left to receive
	// this, and an unbuffered send would pin the scheduler goroutine, and
	// everything it still holds, for the life of the process.
	done := make(chan result, 1)
	go func() {
		states, err := rc.schedule(ctx, p, opts, logs, controlStop)
		done <- result{states: states, err: err}
	}()

	select {
	case r := <-done:
		return r.states, r.err
	case <-ctx.Done():
	}

	timer := time.NewTimer(grace / 2)
	defer timer.Stop()
	select {
	case r := <-done:
		return r.states, r.err
	case <-timer.C:
	}

	// Kill whatever remains. The run's context is already cancelled, so
	// this is belt-and-braces that also covers any future path reaching
	// abandonment without cancellation. A step goroutine still blocked here
	// cannot be killed (Go has no such operation): it is abandoned, its
	// node settled as cancelled below, and rc.seal keeps anything it emits
	// from landing after run.finished.
	//
	// "Kill" is weaker than it reads: there is no SIGTERM-then-SIGKILL
	// escalation (exec.CommandContext's cancellation is a single Kill), and
	// only the direct child is signalled, so anything a step backgrounded
	// survives the run. Process groups are a later executor change
	// (localexec would set Setpgid and signal -pgid), not a scheduling one.
	rc.cancel()
	return rc.settleAbandoned(p), nil
}

// settleAbandoned records every node that never reached a terminal state as
// cancelled, in the ledger as well as in the map Run rolls a status up
// from: a run.finished saying "cancelled" while three steps sit forever
// started-but-never-finished would be a worse artifact.
//
// "Never reached a terminal state" is read from oc.finished, not states,
// and the distinction is the whole point: states is only written once the
// step goroutine returns, after its uncancellable Always handlers, so
// judging by states meant a step that had SUCCEEDED was overwritten with
// `cancelled`, telling a resume or rerun_from tool to re-run a migration
// that already applied.
//
// emitStepFinished claims each node through oc.settle, so a node this loop
// settles cannot also be settled by the goroutine still running it.
func (rc *runCore) settleAbandoned(p *plan.Plan) map[string]api.State {
	states := rc.oc.settledSnapshot()
	for i := range p.Nodes {
		id := p.Nodes[i].ID
		if _, ok := states[id]; ok {
			continue
		}
		// Losing the claim means a step goroutine settled it in the gap
		// since the snapshot; its state, not a fabricated cancellation, is
		// the truth.
		if !rc.emitStepFinished(id, api.StateCancelled, "") {
			states[id] = rc.oc.settledState(id)
			continue
		}
		states[id] = api.StateCancelled
	}
	return states
}

// settledState reads one node's recorded terminal state.
func (oc *outcomes) settledState(id string) api.State {
	oc.mu.Lock()
	defer oc.mu.Unlock()
	return oc.finished[id]
}

// attemptOf is the highest attempt id has actually run, or 0 for a node
// that never ran one. Zero is the honest answer for a skipped or
// never-dispatched node, and SENRO_FAILURE_ATTEMPT=0 tells a cleanup
// handler exactly that: no attempt, so no log or workspace state to look
// for. It reads the same map recordAttempt writes.
func (oc *outcomes) attemptOf(id string) int {
	oc.mu.Lock()
	defer oc.mu.Unlock()
	return oc.attempts[id]
}

// runAlwaysAtSettle runs one node's Always handlers the moment that node
// settles: the primary path. Deferring to teardown would mean a
// release-lock on a step that succeeds in minute one of an hour-long run
// holds the lock for the hour; teardown's runAlways is the fallback for
// nodes that never settled at all.
//
// The context is fresh here too (context.WithoutCancel, exactly as at
// teardown), and that is the most important line in the function: Always is
// not work, it is the thing that must happen regardless, and a release-lock
// killed by Ctrl-C leaves the lock held. Deriving from the run context
// looks harmless because the run "is healthy" when the handler starts, but
// cancellation arriving mid-handler would kill every cleanup in flight on
// every cancelled run. See
// TestAlwaysSurvivesCancellationArrivingMidHandler.
//
// The ctx.Err() guard is about ownership, not the handler's survival: a
// cancelled run is in teardown, and teardown's own pass runs the cleanup of
// everything that did not get there first, in one sequence with one budget.
//
// The grace bounds one node here (the run is healthy; there is no global
// clock to divide); see Options.CleanupGrace. Because these handlers ignore
// cancellation they can outlive the step goroutine's abandonment, so they
// are counted in and out and teardown waits for zero before closing the
// ledger and log set (waitForSettleTimeCleanup).
func (rc *runCore) runAlwaysAtSettle(
	ctx context.Context,
	n *plan.Node,
	fail Failure,
	opts Options,
	logs *eventlog.LogSet,
) {
	// Claimed and counted in as one step: see claimAlwaysAtSettle for the
	// window that splitting them opens, and for what falls into it.
	if ctx.Err() != nil || !rc.oc.claimAlwaysAtSettle(n.ID) {
		return
	}
	defer rc.oc.alwaysDone()

	alwaysCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), opts.cleanupGrace())
	defer cancel()
	rc.runHandlers(alwaysCtx, n, n.Always, "always", fail, opts, logs)
	// Recorded after the call that actually ran Always, never from the
	// declaration (see outcomes.handlerAnchors); handleStepRetry reads it.
	rc.oc.recordHandlerRan(n.ID, fail.Attempt, false, true)
}

// waitForSettleTimeCleanup blocks until no settle-time Always handler is
// still running, or until limit elapses, reporting whether the former.
// Without it a handler that ignored cancellation and outlived its abandoned
// step goroutine would write into a LogSet Run has already closed, losing
// exactly the cleanup record this file exists to keep. It normally returns
// immediately; only the abandonment path has anything to wait for.
//
// Polling rather than a sync.WaitGroup: an already-abandoned goroutine can
// still increment the count while this waits, precisely the "Add called
// concurrently with Wait" case WaitGroup panics on. A millisecond poll once
// per run costs nothing.
func (rc *runCore) waitForSettleTimeCleanup(limit time.Duration) bool {
	deadline := time.Now().Add(limit)
	for rc.oc.inFlightAlways() > 0 {
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(time.Millisecond)
	}
	return true
}

// runAlways is the fallback half: it runs the Always handlers of every node
// that never got to run them itself (cancelled mid-flight, skipped, never
// dispatched, or abandoned) after the scheduler has stopped and before
// run.finished. It reports whether the shared budget ran out with handlers
// still running, which run.finished carries as CleanupAbandoned.
//
// Nodes run CONCURRENTLY, up to opts.MaxParallel, each node's own list
// still sequential in declaration order. Not an optimisation: on a Ctrl-C
// of a wide plan this pass covers nearly the whole plan, and one shared
// budget spent one node at a time is a budget divided by the number of
// nodes, leaving most locks held. MaxParallel rather than unbounded: these
// are real processes, and a plan capped at 4 did not consent to 400
// concurrent teardown handlers. The cost is that handler events no longer
// reach the ledger in plan order; within one node, declaration order holds.
//
// A node that settled normally already ran its own (runAlwaysAtSettle);
// claimAlways prevents a second run.
//
// The Failure carries the node's terminal state and attempt number and no
// other evidence: exit code and log tail exist only where an attempt ran,
// and retaining them charged every Always-declaring node up to logTailCap
// bytes for the run's life. The attempt comes from attemptOf, not a literal
// 1: a node that failed its way to a later attempt and was then cancelled
// settles with a real number, and SENRO_FAILURE_ATTEMPT is how a handler
// finds that attempt's log and workspace state.
//
// The defining property of this file is
// context.WithTimeout(context.WithoutCancel(ctx), grace): WithoutCancel
// keeps the run context's values and drops its cancellation. Deriving from
// ctx compiles and produces cleanup that is dead on arrival, every handler
// killed as it starts with handler.started recorded for each, which is why
// TestAlwaysRunsAfterCancellationOnAFreshContext checks the handler's
// effect on the filesystem rather than that it was attempted. ONE budget
// for the whole pass, not one each: a per-handler budget would scale the
// grace with the size of the plan.
//
// Always means always: handlers run whatever became of the node,
// succeeded through never-dispatched. A release-lock that does not run
// because its step was skipped is the same "cleanup you believed you had"
// failure; running cleanup for something that never started is at worst a
// no-op, and SENRO_FAILURE_STATE tells the handler which case it is in.
func (rc *runCore) runAlways(
	ctx context.Context,
	p *plan.Plan,
	opts Options,
	logs *eventlog.LogSet,
	grace time.Duration,
	states map[string]api.State,
) bool {
	// Claim first, then run: a run whose every node settled normally has
	// nothing left to do here, and must not pay for a context and a snapshot
	// to discover it.
	pending := make([]*plan.Node, 0, len(p.Nodes))
	for i := range p.Nodes {
		n := &p.Nodes[i]
		if len(n.Always) == 0 || !rc.oc.claimAlways(n.ID) {
			continue
		}
		pending = append(pending, n)
	}
	if len(pending) == 0 {
		return false
	}

	alwaysCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), grace)
	defer cancel()

	parallel := opts.MaxParallel
	if parallel <= 0 {
		// Run resolves this before teardown; the guard is for a direct
		// caller, so a zero cannot become a pass that dispatches nothing
		// and reports cleanup as abandoned.
		parallel = 1
	}
	sem := make(chan struct{}, parallel)
	var wg sync.WaitGroup
	for _, n := range pending {
		wg.Add(1)
		go func() {
			defer wg.Done()
			select {
			case sem <- struct{}{}:
			case <-alwaysCtx.Done():
				// The budget is gone; a handler dispatched now would be killed
				// before its first instruction, and handler.started for a
				// command that never ran is worse than nothing.
				return
			}
			defer func() { <-sem }()

			fail := Failure{
				Run: rc.runID, Step: n.ID, State: states[n.ID],
				Attempt: rc.oc.attemptOf(n.ID),
			}
			rc.runHandlers(alwaysCtx, n, n.Always, "always", fail, opts, logs)
			// A node on this fallback pass can never be step.retry's target
			// (every control request is already refused by now), but
			// recording keeps "recordHandlerRan fires everywhere a handler
			// genuinely runs" true by construction rather than by accident.
			rc.oc.recordHandlerRan(n.ID, fail.Attempt, false, true)
		}()
	}
	wg.Wait()

	// Checked before the deferred cancel turns every context error into
	// "context canceled". A live deadline means the pass finished within
	// budget; an expired one means at least one handler was killed by the
	// clock or never started, and a lock this run was supposed to release
	// may still be held.
	return alwaysCtx.Err() != nil
}
