package engine

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand"
	"sync"
	"time"

	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/internal/cache"
	"github.com/xavidop/senro/internal/eventlog"
	"github.com/xavidop/senro/internal/executor"
	"github.com/xavidop/senro/internal/plan"
	"github.com/xavidop/senro/internal/redact"
	"github.com/xavidop/senro/internal/testparse"
	"github.com/xavidop/senro/retry"
	"github.com/xavidop/senro/testreport"
)

// logTailCap bounds how much of an attempt's combined output is kept in
// memory for a retry predicate to inspect (see retry.Attempt.LogTail):
// 4 KiB is enough for a log_match pattern without letting a chatty step
// exhaust the coordinator.
const logTailCap = 4 * 1024

// attemptResult is what one try of a step produced, in the form the retry
// loop needs: enough to classify the attempt, to feed retry.Predicate, and
// to fill in the step's final step.finished payload once the loop ends.
type attemptResult struct {
	state     api.State
	exitCode  int
	err       error // nil for a plain non-zero exit or a clean success
	logTail   string
	snapshots []wsSnapshot
}

// runStep executes one node to a terminal state, retrying it according to
// n.Retry and bounding each attempt with n.TimeoutMS.
//
// release and acquire are the scheduler's MaxParallel semaphore as
// closures, so this function only knows it can give its slot back for a
// backoff sleep and must reacquire before the next attempt.
//
// The caller has already acquired this step's slot, and from here runStep
// alone owns releasing it, exactly once, however it returns: the retry
// loop's release/acquire around the backoff sleep means whether the slot is
// held toggles mid-function, so a plain call-site release would
// double-release on the cancelled-during-backoff path. holding tracks the
// toggle and the deferred giveBack is idempotent.
//
// Every event this emits goes through rc.emit: a second emission path would
// reintroduce out-of-order delivery, since the ledger allocates the
// sequence number and the sink must receive events in that order.
func (rc *runCore) runStep(ctx context.Context, n *plan.Node, opts Options, logs *eventlog.LogSet, release func(), acquire func(context.Context) error) api.State {
	stepStart := time.Now()

	// The first attempt number this execution may use, read from the node's
	// high-water mark rather than assumed to be 1. It IS 1 for every
	// ordinary run; after a run.rerun_from unsettled a node that already
	// ran, starting over at 1 would write output into the first execution's
	// log file and leave the ledger with two step.started/step.finished
	// pairs both claiming attempt 1.
	first := rc.oc.attemptOf(n.ID) + 1

	holding := true
	giveBack := func() {
		if holding {
			release()
			holding = false
		}
	}
	defer giveBack()

	var dec cacheDecision
	if rc.cacheable(n) {
		var err error
		dec, err = rc.cacheLookup(ctx, n, opts)
		if err != nil {
			// A key that cannot be built is a step that cannot be trusted
			// to run correctly: an undeclared or missing input is the
			// author's to declare. It settles as an ordinary failure, so
			// its handlers still run.
			return rc.finishStep(n, stepStart, first, api.StateFailed, 0, err.Error(), false)
		}
		if dec.hit && rc.forcedRegen(n) {
			// Refused on purpose, and recorded as a miss with its own
			// reason, so `cache explain` does not report a hit that was
			// never served.
			dec.hit = false
			dec.reason = "regenerate"
		}
		if dec.hit {
			if rc.serveFromCache(ctx, n, opts, logs, dec) {
				rc.recordDecision(opts.Dir, n, dec)
				return rc.finishStep(n, stepStart, first, api.StateCached, dec.result.ExitCode, "", true)
			}
			// The entry could not be reproduced; serveFromCache already
			// emitted cache.miss and forgot the broken entry
			// (degradeToMiss). Update the decision before it is recorded so
			// `cache explain` reflects a step that ran, then fall through
			// as an ordinary miss.
			dec.hit = false
			dec.reason = cache.ReasonEntryIncomplete
		} else {
			rc.emitMiss(n, dec)
		}
		rc.recordDecision(opts.Dir, n, dec)
	}

	maxAttempts := 1
	var backoff retry.Backoff
	var pred retry.Predicate
	// predErr skips the attempt loop without skipping what comes after it:
	// returning from inside the parse would skip the handler block below,
	// making this the one failure in the engine that fires no OnFailure
	// handler. Reachable through plan.Unmarshal, which does not validate.
	var predErr error
	if n.Retry != nil {
		maxAttempts = n.Retry.MaxAttempts
		backoff = retry.Backoff{
			Base:   time.Duration(n.Retry.BackoffBaseMS) * time.Millisecond,
			Max:    time.Duration(n.Retry.BackoffMaxMS) * time.Millisecond,
			Factor: n.Retry.BackoffFactor,
		}

		// senro.Build guarantees this string round-trips, so a failure here
		// is a plan-time error surfacing at run time (a hand-assembled
		// *plan.Plan). It fails the step before any attempt, but through
		// the same finishStep and handlers as any other failure.
		p, err := retry.Parse(n.Retry.Predicate)
		if err != nil {
			predErr = fmt.Errorf("engine: retry predicate %q: %v", n.Retry.Predicate, err)
		}
		pred = p
	}

	recovered := false
	attempt := first
	// The highest attempt number this execution may reach. n.Retry counts
	// attempts, not attempt NUMBERS, so a rerun gets the same budget as a
	// first run.
	last := first + maxAttempts - 1
	// The unparseable-predicate outcome, settled without running anything:
	// no sandbox is created, so no step.started either, which would make it
	// look like a step that started and then failed. Apply's StepFinished
	// handler creates the step's state itself, so nothing downstream needs
	// a step.started first.
	res := attemptResult{state: api.StateFailed, err: predErr}
	if predErr == nil {
	retryLoop:
		for attempt = first; attempt <= last; attempt++ {
			res = rc.runAttempt(ctx, n, opts, logs, attempt)
			// Here, not inside runAttempt: its deferred writer closes have
			// run by the time it returns, so the two files are final.
			// Archiving an open file would upload a prefix, and on a CI
			// runner the archive is the only copy that outlives the job.
			rc.archiveAttempt(logs, n.ID, attempt)

			if res.state != api.StateFailed && res.state != api.StateTimedOut {
				// Succeeded, or the run itself was cancelled mid-attempt: neither
				// is a candidate for another try.
				break retryLoop
			}
			if attempt == last {
				break retryLoop
			}
			if res.state == api.StateTimedOut {
				// A timeout is never retried, whatever the predicate says:
				// the deadline was declared by the author, so it is a
				// verdict about the workload, and retrying only spends the
				// same budget again. (OnInfra matches it today only because
				// localexec wraps the deadline's ctx.Err in ErrInfra.)
				break retryLoop
			}

			a := retry.Attempt{Number: attempt, ExitCode: res.exitCode, Err: res.err, LogTail: res.logTail}
			if !pred.Match(a) {
				break retryLoop
			}

			// A run cancelled while the attempt was held (or in the instant
			// after it failed) gets no next attempt, so announcing one with
			// step.retried would put a retry in the ledger that never runs.
			// Settled cancelled, as the backoff select below settles it: the
			// predicate wanted another try and the run's cancel is what
			// stopped it. The failed attempt's tail and snapshots are kept.
			if err := ctx.Err(); err != nil {
				res = attemptResult{state: api.StateCancelled, err: err, logTail: res.logTail, snapshots: res.snapshots}
				break retryLoop
			}

			recovered = true
			next := attempt + 1
			delay := backoff.Delay(next, rand.Float64())
			rc.emit(api.Event{
				Type: api.StepRetried, Step: n.ID, Attempt: next,
				Payload: mustMarshal(api.StepRetriedBody{
					Attempt: next, Reason: retryReason(res), Predicate: pred.Serial(),
					BackoffMS: delay.Milliseconds(),
				}),
			})

			// Sleeping is not working: give the slot back so another step
			// can run (holding it stalled every ready step for the length
			// of the sleep) and take it again before the next attempt.
			giveBack()
			select {
			case <-time.After(delay):
				// proceed to reacquire, then the next attempt
			case <-ctx.Done():
				// The run was cancelled while this step waited to retry. The
				// next attempt never starts (no sandbox, no step.started),
				// so this is the run's decision: report it cancelled, not a
				// failure the predicate happened not to reach. The slot was
				// already given back and is not reacquired; holding stays
				// false, so the deferred giveBack correctly does nothing.
				res = attemptResult{state: api.StateCancelled, err: ctx.Err()}
				break retryLoop
			}
			// Reacquiring respects cancellation too: a run being torn down must
			// not block here waiting for a slot that may never free up.
			if err := acquire(ctx); err != nil {
				res = attemptResult{state: api.StateCancelled, err: err}
				break retryLoop
			}
			holding = true
		}
	}

	finalState := res.state
	if finalState == api.StateSucceeded && recovered {
		// A step that failed at least once and then succeeded is recovered,
		// not succeeded: collapsing the two is how flaky infrastructure
		// stays invisible in a run's rolled-up status.
		finalState = api.StateRecovered
	}

	// A generator splices what it produced BEFORE this step settles: its
	// dependents gain the fragment's boundary as new needs, and that is only
	// safe while they are provably still pending, which they are until
	// states[n.ID] is written after runStep returns. Before cacheSave too, so
	// the entry a generator saves can carry the fragment it produced.
	var fragment string
	if finalState == api.StateSucceeded && n.Generate != nil {
		var err error
		if fragment, err = rc.splice(ctx, n, opts); err != nil {
			finalState = api.StateFailed
			res.err = err
		}
	}

	// Saved only on an outright success, never StateRecovered: a step that
	// failed before passing is not evidence its declared inputs describe
	// it, and saving would serve the one lucky attempt to every future run.
	if rc.cacheable(n) && finalState == api.StateSucceeded {
		if err := rc.cacheSave(ctx, n, opts, logs, dec, res, attempt, time.Since(stepStart), fragment); err != nil {
			// A step that ran correctly and could not be stored still ran
			// correctly: the next run misses, a slower build rather than a
			// broken one.
			rc.emit(api.Event{
				Type: api.CacheMiss, Step: n.ID, Attempt: attempt,
				Payload: mustMarshal(api.CacheMissBody{
					Key: string(dec.digest), Reason: "save_failed", Differing: err.Error(),
				}),
			})
		}
	}

	state := rc.finishStep(n, stepStart, attempt, finalState, res.exitCode, errText(res), false)

	// After finishStep, never before: an analyzer's proposal refers to a
	// failure that must be in the ledger before anything explaining it.
	// Offering is a bounded-queue send that cannot block; a run with no
	// analyzer does one nil check. See offerAnalysis.
	if finalState.Failed() {
		rc.offerAnalysis(n, attempt, finalState, res, time.Since(stepStart), opts.Pipeline)
	}

	// OnFailure handlers run after step.finished, deliberately still under
	// this step's own MaxParallel slot (giveBack has not run yet): a
	// handler is real work on the same executor, and reusing the slot
	// bounds it by MaxParallel without a second acquire that could queue
	// behind unrelated ready steps. A handler's outcome never reaches
	// `state`; see runHandlers.
	if finalState.Failed() && len(n.OnFailure) > 0 {
		fail := newFailure(rc.runID, n, attempt, finalState, res)
		rc.runHandlers(ctx, n, n.OnFailure, "on_failure", fail, opts, logs)
		// Recorded AFTER the call, not gated only by the len() check: this
		// is what makes handleStepRetry's handler.superseded reflect a pass
		// that genuinely ran against THIS attempt (see
		// outcomes.handlerAnchors).
		rc.oc.recordHandlerRan(n.ID, attempt, true, false)
	}

	// Always runs after OnFailure, so evidence is collected before cleanup
	// dismantles the environment (see TestOnFailureRunsBeforeAlways). This
	// is Always' primary path, under this step's own slot for the same
	// reasons. It gets the same full Failure an OnFailure handler does, but
	// NOT the run's context: see runAlwaysAtSettle.
	if len(n.Always) > 0 {
		fail := newFailure(rc.runID, n, attempt, finalState, res)
		rc.runAlwaysAtSettle(ctx, n, fail, opts, logs)
	}

	return state
}

// runAttempt runs one try of n and classifies its outcome. It owns exactly
// one attempt's resources (sandbox, log writers, timeout context) and
// releases all of them before returning: a retry inheriting the previous
// attempt's sandbox inherits what caused the failure, and appending to the
// previous attempt's log destroys the evidence explaining it.
//
// A timed-out attempt's process is not guaranteed gone by the time this
// returns: localexec kills only the direct child, so a step that
// backgrounds work leaves an orphan writing to this attempt's log for up to
// localexec's waitDelay, and a retry can briefly run alongside it. Fixing
// that belongs to a later executor change (process groups), not here.
func (rc *runCore) runAttempt(ctx context.Context, n *plan.Node, opts Options, logs *eventlog.LogSet, attempt int) attemptResult {
	attemptCtx := ctx
	if n.TimeoutMS > 0 {
		var cancel context.CancelFunc
		attemptCtx, cancel = context.WithTimeout(ctx, time.Duration(n.TimeoutMS)*time.Millisecond)
		defer cancel()
	}

	s, err := rc.prepareAttempt(ctx, attemptCtx, n, opts, logs, attempt)
	if err != nil {
		return attemptResult{state: api.StateFailed, err: err}
	}
	defer s.close()

	// A step's own executor, and no failure: a step is not cleaning up
	// after anything, which is what makes ctx.Failure report ok=false.
	//
	// attemptStart is taken immediately before the command runs so a JUnit
	// report older than it is recognisably not this attempt's: a workspace
	// mounted at WorkDir outlives attempts and runs, and can still hold the
	// report a previous one wrote.
	attemptStart := time.Now()
	exit, runErr := rc.invoke(attemptCtx, n, invocation{key: n.ExecutorKey(), ex: s.ex}, s.sb,
		executor.Cmd{Args: n.Cmd, Env: s.cmdEnv, Dir: cmdDirFor(n.WorkDir, s.mounts)},
		s.mounts, s.secretPaths, attempt, s.stdout, s.stderr, opts)

	if err := rc.flushAttempt(n, attempt, s); err != nil && runErr == nil {
		runErr = err
	}

	// Snapshotting happens while the sandbox is still open and on EVERY
	// path: failure is when the workspace matters most, and a snapshot
	// taken after the sandbox closed would capture whatever teardown left.
	// Each attempt snapshots for itself, so a retry does not erase the
	// evidence of the attempt before it. WithoutCancel: a cancelled run's
	// workspace still has to be captured.
	snaps, snapErr := rc.snapshotMounts(context.WithoutCancel(ctx), s.sb, n, s.mounts, attempt)
	if snapErr != nil && runErr == nil && exit == 0 {
		// A step whose workspace could not be captured produced a result
		// nothing downstream can key on; reported as a step failure, but
		// only when the step itself passed, so a genuine workload failure
		// is not masked.
		return attemptResult{state: api.StateFailed, err: snapErr, logTail: s.tail.String(), snapshots: snaps}
	}
	if rc.ws != nil {
		rc.ws.record(snaps)
	}

	res := classifyAttempt(ctx, attemptCtx, exit, runErr, s.tail, snaps)

	// A JUnit report is read on two outcomes, with different stakes.
	//
	// A step that SUCCEEDED but left no readable report has not shown its
	// tests passed, so a missing, malformed or stale report fails it with an
	// error naming the path. Replacing res also skips the scratch read-back
	// below, which keys on the same succeeded state.
	//
	// A step that FAILED with a plain non-zero exit is the case the report
	// matters most for: a red test run is exactly when someone needs to know
	// which test. So it is read best-effort: whatever parses is emitted, and
	// a missing or malformed file is ignored, leaving the step's state, exit
	// code and (empty) error as the exit made them, because the step may have
	// died before writing a report and "report missing" would bury the real
	// story under a consequence of it. A timed-out, cancelled or panicked
	// step, or one that failed with an error of its own, is not read: its
	// command did not run to an exit, so any report is partial at best.
	if n.Tests != nil && n.Tests.Format == testreport.FormatJUnit {
		switch {
		case res.state == api.StateSucceeded:
			if err := rc.readJUnit(n, s.sb, attempt, attemptStart); err != nil {
				res = attemptResult{state: api.StateFailed, err: err, logTail: s.tail.String(), snapshots: snaps}
			}
		case res.state == api.StateFailed && res.err == nil && res.exitCode != 0:
			_ = rc.readJUnit(n, s.sb, attempt, attemptStart)
		}
	}

	// --break-on-failure holds the attempt HERE, with the sandbox still open
	// and the workspace mount locks still held by s.close's deferred
	// release: a shell on this step mounts the workspaces as "whatever is in
	// them now", so the hold has to come before anything a step's teardown
	// or a sibling's cache restore could do to them.
	//
	// The cost, for as long as an operator keeps the step held: a sibling's
	// ordinary mount of the same workspace is a shared hold and does not
	// wait, but a cache hit restoring into it is exclusive and does, and
	// once that restore is queued Go's RWMutex makes every LATER mount of
	// the workspace wait behind it too. (The shell on this step is the one
	// exception; see runShellSession.) The held step also keeps its
	// MaxParallel slot, so with MaxParallel 1 a hold stalls the whole run.
	//
	// After the snapshot and the JUnit read, so the ledger already records
	// what the failure left, and before the scratch read-back, which only a
	// succeeded attempt does anyway. Not on a cancelled run: nobody is left
	// to release it. The hold changes nothing about res; the retry loop
	// carries on as if it had not happened, and holds again if the next
	// attempt fails too.
	if opts.BreakOnFailure && (res.state == api.StateFailed || res.state == api.StateTimedOut) && ctx.Err() == nil {
		rc.holds.hold(ctx, n.ID, func() {
			rc.emit(api.Event{
				Type: api.BreakpointHit, Step: n.ID, Attempt: attempt,
				Payload: mustMarshal(api.BreakpointHitBody{Reason: api.HoldReasonFailure}),
			})
		})
	}

	// A scratch cache on a target that does not share the coordinator's
	// filesystem comes back HERE or not at all: the sandbox is still open,
	// and by the time this function returns it is closed and the pod or the
	// attempt directory is gone. Only on a SUCCEEDED attempt, because that is
	// the only kind saveScratch can ever store from, and a read-back is a
	// full transfer of the whole cache. WithoutCancel, as the snapshot above
	// is: what a step left is still worth having.
	if rc.ws != nil && res.state == api.StateSucceeded && len(s.scratch) > 0 {
		rc.ws.readScratch(context.WithoutCancel(ctx), s.sb, s.scratch)
	}
	return res
}

// attemptSetup is everything one attempt of a node runs with, built by
// prepareAttempt: the sandbox, its mounts and delivered secrets, the
// command's environment, and the log writers wrapped for redaction, markers,
// the tail buffer and a streaming test parser.
//
// close releases all of it, in the order runAttempt's own defers used to:
// the writers, then the sandbox, then the workspace hold. It is safe to call
// exactly once, after flushAttempt.
type attemptSetup struct {
	ex          executor.Executor
	sb          executor.Sandbox
	mounts      []executor.Mount
	scratch     []executor.Mount
	secretPaths map[string]string
	cmdEnv      []string
	stdout      *redact.Writer
	stderr      *redact.Writer
	tests       testparse.Stream
	tail        *tailBuffer
	close       func()
}

// prepareAttempt opens one attempt of n: it emits step.started, takes the
// workspace hold, realizes the mounts, opens the sandbox, delivers secrets,
// builds the environment and opens the log writers.
//
// Extracted from runAttempt so a service (runService) is started in exactly
// the sandbox, environment and log plumbing a step is; two copies of this
// setup are two places for a secret's delivery or the redactor's position to
// drift.
//
// On error everything already acquired is released before returning, so the
// caller has nothing to close. step.started has been emitted either way: a
// step whose sandbox could not be opened still started and then failed.
//
// Two contexts, used exactly where runAttempt used them before the
// extraction: ctx is the run's, for step.started and the sandbox's
// uncancellable close; attemptCtx carries the attempt's own deadline, which
// bounds the setup (scratch realization, the sandbox, secret delivery) as it
// bounds the command. A service passes the run's context for both: its
// TimeoutMS bounds readiness, not setup.
func (rc *runCore) prepareAttempt(
	ctx, attemptCtx context.Context, n *plan.Node, opts Options, logs *eventlog.LogSet, attempt int,
) (s attemptSetup, err error) {
	// Releases run in reverse of acquisition; on success they become close,
	// on failure they run before this returns.
	var releases []func()
	defer func() {
		if err != nil {
			for i := len(releases) - 1; i >= 0; i-- {
				releases[i]()
			}
		}
	}()
	s.close = func() {
		for i := len(releases) - 1; i >= 0; i-- {
			releases[i]()
		}
	}

	// The span this attempt runs under, minted by the event announcing it
	// and taken as a return value so there is exactly one answer to "which
	// span is this attempt": the one the ledger published. The step's own
	// command is launched inside it; see cmdEnv below.
	span := rc.emitStepStarted(ctx, n, attempt)

	// Shared hold on every workspace n mounts, for this attempt's entire
	// span (mount realization, the process running, the post-run snapshot):
	// while held, no sibling's cache hit can RemoveAll-then-Rename the same
	// directory out from under this attempt. Released by close, so every
	// return path of the caller must release it too.
	//
	// Deliberately NOT held across the retry loop's backoff sleep (sleeping
	// is not working), nor across OnFailure/Always handlers: a handler
	// takes its OWN hold for its own span (see handlerMounts), since
	// settle-time Always can hold the full cleanup grace and stretching
	// this hold that far would keep the workspace exclusive to one step for
	// the whole of its cleanup.
	if rc.ws != nil {
		releases = append(releases, rc.ws.lockMounts(workspaceMountNames(n)))
	}

	// rc.ws is nil only when the plan declares nothing needing storage;
	// Run's upfront check (planNeedsStorage) guarantees a node with a
	// workspace mount never reaches here with rc.ws == nil.
	if rc.ws != nil {
		s.mounts, err = rc.ws.mounts(n)
		if err != nil {
			return s, err
		}
	}

	// Scratch mounts are kept out of mounts itself: they must never reach
	// snapshotMounts, since a scratch cache is not a workspace and its
	// digest must never enter a ws.snapshot event or a cache key.
	if rc.ws != nil {
		s.scratch, err = rc.ws.scratchMounts(attemptCtx, n)
		if err != nil {
			return s, err
		}
	}

	// SandboxSpec.Secrets carries identities only, never a value: the shape
	// a future executor that provisions secrets itself would need.
	// Populated, not yet consumed (localexec never reads spec.Secrets);
	// forward-looking API surface rather than a working delegation path.
	var specSecrets []executor.SecretRef
	for _, sec := range n.Secrets {
		specSecrets = append(specSecrets, executor.SecretRef{Name: sec.Name, Source: sec.Source})
	}

	s.ex, err = rc.executorFor(n)
	if err != nil {
		return s, err
	}

	// Read once, here, so the environment below and the sandbox's resolver
	// agree on which services this attempt was given.
	serviceEnv := rc.serviceEnv(n)

	// Node.Env verbatim: the step gets its declared environment and nothing
	// else.
	spec := executor.SandboxSpec{
		StepID: n.ID, Attempt: attempt, Env: n.Env, WorkDir: n.WorkDir,
		Secrets: specSecrets,
		Mounts:  append(append([]executor.Mount(nil), s.mounts...), s.scratch...),
	}
	if n.IsService() {
		spec.Ports = n.Service.Ports
	}
	if len(serviceEnv) > 0 && consumesFromContainer(n) {
		// A container consumer of a LOCAL service is given
		// host.docker.internal, which resolves only with this entry on
		// Linux; Docker Desktop has it built in, and adding it there (or for
		// a consumer of a container service only, which is given an IP) is
		// harmless. See serviceTable.envFor.
		spec.ExtraHosts = []string{dockerHostGateway}
	}
	s.sb, err = s.ex.Sandbox(attemptCtx, spec)
	if err != nil {
		return s, err
	}
	sb := s.sb
	releases = append(releases, func() { _ = sb.Close(context.WithoutCancel(ctx), false) })

	// Secret delivery: one FILE per declared secret, with its PATH in the
	// environment. Nothing here ever puts a VALUE in the environment, which
	// keeps two promises at once: the cache key's env component can never
	// reach a credential, and /proc/<pid>/environ holds paths. Built as a
	// copy of n.Env so the plan's slice is never mutated and a retry starts
	// from the declared environment, not the previous attempt's paths.
	cmdEnv := n.Env
	if len(n.Secrets) > 0 && delegatesSecrets(sb) {
		// The executor fetches its own secrets with its own identity; senro
		// resolves nothing and only the SOURCE crosses. See
		// executor/k8s.DelegateSecrets for the costs, including that the
		// redactor never sees a value it did not resolve.
		cmdEnv = append([]string(nil), n.Env...)
		for _, sec := range n.Secrets {
			cmdEnv = append(cmdEnv, plan.SecretSourceEnvVar(sec.Name)+"="+sec.Source)
		}
	} else if len(n.Secrets) > 0 {
		cmdEnv = append([]string(nil), n.Env...)
		s.secretPaths = make(map[string]string, len(n.Secrets))
		for _, sec := range n.Secrets {
			v, ok := rc.secrets.Value(sec.Name)
			if !ok {
				// checkSecretRefs already refused this at run start;
				// reaching here means a hand-assembled *plan.Plan. Failing
				// keeps the invariant that a step never runs believing it
				// has a credential it does not.
				return s, fmt.Errorf(
					"engine: step %q needs secret %q, which was not resolved", n.ID, sec.Name)
			}
			path, err := sb.PutSecret(attemptCtx, sec.Name, v)
			if err != nil {
				return s, err
			}
			s.secretPaths[sec.Name] = path
			cmdEnv = append(cmdEnv, plan.SecretEnvVar(sec.Name)+"="+path)
			if sec.Env != "" {
				cmdEnv = append(cmdEnv, sec.Env+"="+path)
			}
		}
	}

	// This attempt's own trace context, so a traced tool the step runs
	// joins the run's trace as a child of THIS attempt (see
	// spanTable.outboundEnv; a step's own declared traceparent wins).
	// Applied last, after the secret block, because a step may name its
	// trace context through any mechanism above, including a secret's Env.
	//
	// It never enters the cache key: the key's env component digests only
	// CacheEnv-declared names from the node's DECLARED environment (see
	// cacheLookup), so a value that necessarily differs every run cannot
	// make a pure step miss every run
	// (TestTwoRunsInDifferentTracesShareACacheKey).
	cmdEnv = rc.spans.outboundEnv(cmdEnv, span)

	// Where every service this node needs answers (SENRO_SERVICE_*), after
	// the trace context. Outside the cache key for the same reason the
	// trace context is: the key digests CacheEnv-declared names from the
	// DECLARED environment only, and a published port differs every run, so
	// digesting it would make a pure consumer miss every run
	// (TestServiceEnvNeverEntersTheCacheKey). A copy, never an append onto
	// cmdEnv in place: with no secrets and no trace it is still n.Env, and
	// an append into its spare capacity would write into the plan.
	if len(serviceEnv) > 0 {
		cmdEnv = append(append([]string(nil), cmdEnv...), serviceEnv...)
	}
	s.cmdEnv = cmdEnv

	// Both writers are scoped to (step, attempt): LogSet.Path folds the
	// attempt number into the file's path, so a fresh attempt number is
	// automatically a fresh file. Closing them when the attempt ends
	// bounds descriptor use per attempt in flight.
	stdoutW, err := logs.Writer(n.ID, attempt, api.StreamStdout)
	if err != nil {
		return s, err
	}
	releases = append(releases, func() { _ = stdoutW.Close() })

	stderrW, err := logs.Writer(n.ID, attempt, api.StreamStderr)
	if err != nil {
		return s, err
	}
	releases = append(releases, func() { _ = stderrW.Close() })

	s.tail = &tailBuffer{}
	// The redactor sits UPSTREAM of logMarker and the tail buffer, so every
	// downstream consumer sees redacted bytes and exactly one place can be
	// wrong. Upstream of logMarker because the marker records byte offsets
	// a client range-requests back out of the file, and redaction changes
	// byte counts: the marker must describe what landed on disk (see
	// TestLogMarkersDescribeTheRedactedFile). Upstream of the tail buffer
	// because a log_match pattern must not match a value in memory the same
	// bytes on disk no longer contain; the consequence is a retry predicate
	// cannot match a secret, and that is the correct trade.
	//
	// One Writer per stream, never one shared: the rolling-buffer state is
	// per stream, and interleaving stdout and stderr through one buffer
	// would splice a match out of bytes that were never adjacent.
	//
	// A streaming test parser sits in the same downstream position as the
	// tail buffer, and for the same reason: a test.finished Output is a copy
	// of log bytes that lands in the ledger, so it must never carry a value
	// the log file no longer contains. Stdout only, because go test -json and
	// TAP both report on stdout, and stderr's chatter interleaved into a
	// line-oriented parser would split lines that were never broken.
	stdoutSinks := []io.Writer{
		&logMarker{rc: rc, w: stdoutW, step: n.ID, attempt: attempt, stream: api.StreamStdout}, s.tail,
	}
	if n.TestsStreaming() {
		s.tests, err = testparse.NewStream(n.Tests.Format, rc.testEmitter(n.ID, attempt))
		if err != nil {
			return s, fmt.Errorf("engine: step %q: %w", n.ID, err)
		}
		stdoutSinks = append(stdoutSinks, s.tests)
	}
	s.stdout = rc.redact.Writer(io.MultiWriter(stdoutSinks...))
	s.stderr = rc.redact.Writer(io.MultiWriter(
		&logMarker{rc: rc, w: stderrW, step: n.ID, attempt: attempt, stream: api.StreamStderr}, s.tail))
	return s, nil
}

// flushAttempt drains one attempt's output once its process has ended, and
// returns the first flush error.
//
// Flush both streams before anything else, so every step.log.appended
// marker precedes this attempt's step.finished, and a partial match's
// held-back tail reaches the file. Explicit rather than left to close: the
// writer closes would run first, and a flush into a closed LogWriter
// returns ErrClosed with the bytes lost. A backgrounded child can still be
// writing at this moment (localexec's waitDelay); redact.Writer is
// mutex-guarded for exactly that race.
func (rc *runCore) flushAttempt(n *plan.Node, attempt int, s attemptSetup) error {
	var first error
	if err := s.stdout.Flush(); err != nil {
		first = err
	}
	if err := s.stderr.Flush(); err != nil && first == nil {
		first = err
	}
	// After both flushes, so the redactor's held-back tail has reached the
	// parser, and before step.finished, so a trailing line with no newline
	// still settles its test in time. Close's error is ignored because its
	// contract is that it has none: a malformed line was already dropped.
	if s.tests != nil {
		_ = s.tests.Close()
	}
	// One event per attempt carrying both streams' counts:
	// api.SecretRedactedBody has only a Count, so two events would be
	// indistinguishable to a reader.
	if c := s.stdout.Redacted() + s.stderr.Redacted(); c > 0 {
		rc.emit(api.Event{
			Type: api.SecretRedacted, Step: n.ID, Attempt: attempt,
			Payload: mustMarshal(api.SecretRedactedBody{Count: c}),
		})
	}
	return first
}

// classifyAttempt turns one attempt's outcome into its result.
//
// Extracted from runAttempt so there is exactly ONE statement of "this
// attempt succeeded": the scratch read-back needs the same answer, and a
// second copy of these conditions is how the two would drift.
func classifyAttempt(
	ctx, attemptCtx context.Context, exit int, runErr error, tail *tailBuffer, snaps []wsSnapshot,
) attemptResult {
	switch {
	case runErr != nil && ctx.Err() != nil:
		// ctx is the run's own context, not this attempt's timeout-bounded
		// derivative: done means the run was cancelled.
		return attemptResult{state: api.StateCancelled, exitCode: exit, err: runErr, logTail: tail.String(), snapshots: snaps}
	case runErr != nil && attemptCtx.Err() != nil:
		// Only the attempt's own deadline fired; the run is still live. See
		// TestTimeoutIsNotCancellation.
		return attemptResult{state: api.StateTimedOut, exitCode: exit, err: runErr, logTail: tail.String(), snapshots: snaps}
	case runErr != nil && isPanic(runErr):
		// A panicked step is not a failed one (api.StatePanicked), and is
		// not retried: the retry loop only reconsiders StateFailed and
		// StateTimedOut.
		return attemptResult{state: api.StatePanicked, exitCode: exit, err: runErr,
			logTail: tail.String(), snapshots: snaps}
	case runErr != nil:
		return attemptResult{state: api.StateFailed, exitCode: exit, err: runErr, logTail: tail.String(), snapshots: snaps}
	case ctx.Err() != nil:
		// runErr == nil but the run's context is done: only a func step
		// whose function ignored its context reaches this (exec-backed
		// executors guarantee a non-nil error when their context is done).
		// Falling through to StateSucceeded would report a cancelled run's
		// func step as succeeded and, if Pure(), write a cache entry from a
		// cancelled run. See TestACancelledFuncStepDoesNotWriteTheCache.
		return attemptResult{state: api.StateCancelled, exitCode: exit, logTail: tail.String(), snapshots: snaps}
	case attemptCtx.Err() != nil:
		// Same for the attempt's own timeout: a func step whose function
		// outruns Timeout must not settle succeeded (see
		// TestTimeoutAppliesToAFuncStepToo). Nothing can force a Go
		// function to return; only the classification is bounded.
		return attemptResult{state: api.StateTimedOut, exitCode: exit, logTail: tail.String(), snapshots: snaps}
	case exit != 0:
		return attemptResult{state: api.StateFailed, exitCode: exit, logTail: tail.String(), snapshots: snaps}
	default:
		return attemptResult{state: api.StateSucceeded, logTail: tail.String(), snapshots: snaps}
	}
}

// cmdDirFor is executor.CmdDir, shared by runAttempt, execHandler and
// `senro verify` so the step/handler pairing cannot drift again (it has,
// three times); the alias keeps this file's call sites reading as before.
func cmdDirFor(workDir string, mounts []executor.Mount) string {
	return executor.CmdDir(workDir, mounts)
}

// emitStepStarted records the start of one attempt of n and returns the
// span it minted, so the attempt's command is launched inside the span the
// ledger published; asking the table again later could disagree for a step
// whose next attempt began in between.
func (rc *runCore) emitStepStarted(ctx context.Context, n *plan.Node, attempt int) string {
	var class, plat string
	if ex, err := rc.executorFor(n); err == nil {
		c, _ := ex.Class(ctx)
		p, _ := ex.DeclaredPlatform(ctx)
		class, plat = c, p.String()
	}
	body := api.StepStartedBody{
		Cmd: n.Cmd, WorkDir: n.WorkDir, ExecutorClass: class, Platform: plat,
	}
	if n.Func != nil {
		body.Func = n.Func.Name
	}
	// One span per ATTEMPT, minted here because this is the only place an
	// attempt begins; its parent comes from the dependency graph (see
	// spanTable.parentLocked).
	body.SpanID, body.ParentSpanID, body.LinkedSpanIDs = rc.spans.begin(n.ID)
	rc.emit(api.Event{
		Type: api.StepStarted, Step: n.ID, Attempt: attempt,
		Payload: mustMarshal(body),
	})
	return body.SpanID
}

// finishStep emits the one step.finished event for a step's overall outcome
// and returns state for schedule to record. attempt names the last attempt
// that actually concluded.
//
// The state is published to rc.oc HERE, before the caller's OnFailure and
// Always handlers run. That ordering is load-bearing: settle-time Always is
// uncancellable, so runStep stays on the stack long after its step settled,
// and teardown used to read "not in the map yet" as "never finished" and
// record a succeeded step as cancelled (see outcomes.finished and
// settleAbandoned).
//
// oc.settle also decides whether this event is emitted at all: losing the
// claim means teardown got there first and the ledger already carries this
// node's step.finished.
func (rc *runCore) finishStep(
	n *plan.Node, start time.Time, attempt int, state api.State, exitCode int, errMsg string, cached bool,
) api.State {
	if !rc.oc.settle(n.ID, state) {
		return state
	}
	rc.oc.recordAttempt(n.ID, attempt)
	body := api.StepFinishedBody{
		State: state, ExitCode: exitCode,
		Duration: time.Since(start), Error: errMsg, Cached: cached,
	}
	// Usually this closes the span emitStepStarted opened, and only the
	// span ID goes on the wire (step.started already said where it hangs).
	// A step restored from cache reaches here having never started, so the
	// span is minted now and its parentage stated here or nowhere;
	// finishSpan tells the two apart.
	body.SpanID, body.ParentSpanID, body.LinkedSpanIDs = rc.spans.finishSpan(n.ID)
	rc.emit(api.Event{
		Type: api.StepFinished, Step: n.ID, Attempt: attempt,
		Payload: mustMarshal(body),
	})
	return state
}

// retryReason renders why an attempt failed, for step.retried's Reason
// field: the error for an infrastructure-style failure, or "exit status N"
// for a plain non-zero exit: localexec deliberately reports that case as
// (exitCode, nil), so there is no error to render otherwise.
func retryReason(res attemptResult) string {
	if res.err != nil {
		return res.err.Error()
	}
	return fmt.Sprintf("exit status %d", res.exitCode)
}

// errText is res.err's message, or "" when the attempt had none, matching
// StepFinishedBody.Error's existing convention that a plain non-zero exit
// carries no error text, only a state and a code.
func errText(res attemptResult) string {
	if res.err != nil {
		return res.err.Error()
	}
	return ""
}

// logMarker wraps one attempt's log writer so every write both appends to
// the file and emits step.log.appended, carrying the byte offset the write
// started at and how many bytes landed: a byte-range marker, not the log
// content itself.
type logMarker struct {
	rc      *runCore
	w       *eventlog.LogWriter
	step    string
	attempt int
	stream  string
	mu      sync.Mutex
}

func (m *logMarker) Write(p []byte) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	pre := m.w.Offset()
	n, err := m.w.Write(p)
	if n > 0 {
		m.rc.emit(api.Event{
			Type: api.StepLogAppended, Step: m.step, Attempt: m.attempt,
			Payload: mustMarshal(api.StepLogAppendedBody{
				Stream: m.stream, Offset: pre, Len: int64(n),
				Lines: bytes.Count(p[:n], []byte{'\n'}),
			}),
		})
	}
	return n, err
}

// tailBuffer keeps only the most recent logTailCap bytes written to it,
// possibly from stdout and stderr concurrently, so a retry predicate can be
// fed a LogTail without the engine holding a whole log in memory.
type tailBuffer struct {
	mu  sync.Mutex
	buf []byte
}

func (t *tailBuffer) Write(p []byte) (int, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.buf = append(t.buf, p...)
	if len(t.buf) > logTailCap {
		// Copying the window down, rather than reslicing, is what releases
		// the discarded prefix's backing array: a plain reslice keeps the
		// whole buffer alive, growing without bound across a chatty step.
		trimmed := make([]byte, logTailCap)
		copy(trimmed, t.buf[len(t.buf)-logTailCap:])
		t.buf = trimmed
	}
	return len(p), nil
}

func (t *tailBuffer) String() string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return string(t.buf)
}

// archiveAttempt queues one finished attempt's log files for the shared
// store. Called only where runAttempt has RETURNED: its deferred writer
// closes have run by then, so the bytes on disk are final. A file archived
// while open would upload a prefix, and on a CI runner the archive is the
// only copy that survives the job.
//
// Enqueuing is a non-blocking send; a run with no shared store has a nil
// archiver whose every method is a no-op. Both streams unconditionally: a
// stream never written to has no file, which the archiver treats as nothing
// to do.
//
// Nothing is redacted here, deliberately: every byte reaching a log file
// already passed through rc.redact.Writer at all three producing call sites
// (runAttempt, replayLog, runHandler), and a second implementation of the
// guarantee is the one that would drift.
func (rc *runCore) archiveAttempt(logs *eventlog.LogSet, step string, attempt int) {
	if rc.archive == nil {
		return
	}
	for _, stream := range []string{api.StreamStdout, api.StreamStderr} {
		rc.archive.Stream(step, attempt, stream, logs.Path(step, attempt, stream))
	}
}

// secretDelegator is implemented by a sandbox that fetches its own secrets
// rather than receiving them through PutSecret. An optional interface for
// the reason workspaceLocker is one: true of one executor, only when
// configured for it.
type secretDelegator interface {
	DelegatesSecrets() bool
}

// delegatesSecrets reports whether this sandbox resolves its own.
func delegatesSecrets(sb executor.Sandbox) bool {
	d, ok := sb.(secretDelegator)
	return ok && d.DelegatesSecrets()
}
