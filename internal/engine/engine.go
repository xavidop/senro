// Package engine is senro's scheduler: it turns a resolved plan into a
// running set of steps and an append-only event stream.
package engine

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/internal/binprov"
	"github.com/xavidop/senro/internal/cache"
	"github.com/xavidop/senro/internal/cond"
	"github.com/xavidop/senro/internal/eventlog"
	"github.com/xavidop/senro/internal/executor"
	"github.com/xavidop/senro/internal/plan"
	"github.com/xavidop/senro/internal/redact"
	"github.com/xavidop/senro/internal/remotecache"
	"github.com/xavidop/senro/internal/secrets"
	"github.com/xavidop/senro/internal/sink"
	"github.com/xavidop/senro/internal/storage"
)

// engineVersion is what run.started carries as engine_version.
//
// Read from the running binary's own build info rather than injected at
// link time: `senro run` compiles the user's pipeline package and execs the
// result, so the version that matters is the senro module version that
// binary was built against, which only its own build info knows. An -X flag
// naming an unlinked symbol would be a silent no-op. Reports "dev" for a
// binary built from a senro checkout (tests, examples/).
var engineVersion = readEngineVersion()

func readEngineVersion() string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return "dev"
	}
	return engineVersionFrom(info)
}

// engineVersionFrom is readEngineVersion's logic, split out so every branch
// can be tested against a synthetic BuildInfo: a test binary's real build
// info only ever reports itself.
func engineVersionFrom(info *debug.BuildInfo) string {
	const mod = "github.com/xavidop/senro"

	if info == nil {
		return "dev"
	}
	// Built from senro's own module (tests, examples/, `go run` in a
	// checkout); Go reports "(devel)" as the version.
	if info.Main.Path == mod {
		return normalizeVersion(info.Main.Version)
	}
	for _, d := range info.Deps {
		if d.Path != mod {
			continue
		}
		// A replace directive (a local checkout) describes the code actually
		// linked in; d.Version still names the version being replaced.
		if d.Replace != nil {
			return normalizeVersion(d.Replace.Version)
		}
		return normalizeVersion(d.Version)
	}
	return "dev"
}

// normalizeVersion maps Go's placeholders ("" and "(devel)") to "dev" so a
// reader need not know Go's spelling of "not built from a released tag".
func normalizeVersion(v string) string {
	if v == "" || v == "(devel)" {
		return "dev"
	}
	return v
}

// Options configures one call to Run.
type Options struct {
	Dir string
	// Executor is the DEFAULT executor: the one a node that names none runs
	// on. senro.Run always supplies the local executor here.
	Executor    executor.Executor
	Sink        sink.Sink
	MaxParallel int
	RunID       string

	// Pipeline is what run.started publishes as api.RunStartedBody.Pipeline.
	// An option rather than a plan field because a plan carries no name of
	// its own: senro.Run reads it from the *Pipeline, while senro.RunPlan
	// leaves it empty rather than inventing one. Empty is a real answer.
	Pipeline string

	// TraceParent and TraceState are the inbound W3C Trace Context this run
	// should continue, raw and unvalidated (senro.WithTraceContext, or the
	// TRACEPARENT/TRACESTATE environment variables). Empty produces a fresh
	// trace, as does a TraceParent that does not parse (see newSpanTable).
	// Raw strings rather than api.TraceParent so exactly one place decides
	// what a valid inbound context is.
	TraceParent string
	TraceState  string

	// Executors holds one executor per distinct plan.ExecutorSpec.Key() the
	// plan names; empty when every node runs on the default. Keyed by spec
	// rather than by node so two workflows naming the same image share one
	// executor, one resolved digest and one pull, which is what keeps the
	// digest stable across a run.
	Executors map[string]executor.Executor

	// CleanupGrace is how much time cleanup is allowed; zero or negative
	// means defaultCleanupGrace (60s). It is four separate budgets, not one
	// shared one:
	//
	//   - grace/2 bounds teardown's wait for in-flight steps to unwind after
	//     cancellation, before abandoning them.
	//   - the full grace is teardown's Always pass budget, one wall clock
	//     shared across the whole pass (up to MaxParallel nodes at once).
	//   - the full grace bounds teardown's wait for settle-time cleanup still
	//     running, keeping the ledger and log set open until it is recorded.
	//   - the full grace bounds each node's Always handlers when they run at
	//     settle time, PER NODE: while the run is healthy one slow cleanup
	//     must not eat another's budget.
	//
	// So worst-case teardown is 2.5 × CleanupGrace (150s at the default),
	// and a run's TOTAL cleanup time is unbounded: the last budget is per
	// node. See shutdown.go for the sequence these are spent in.
	CleanupGrace time.Duration

	// Storage is the run's content-addressed store, action cache and
	// workspace snapshotter. nil is legal and means no cache and no
	// workspaces; a plan that declares a workspace or a Pure() step is then
	// rejected by Run rather than executed with the declaration silently
	// ignored. senro.Run always supplies one.
	Storage *storage.Storage

	// Secrets is the run's credentials, resolved once on the coordinator
	// before the first step. nil means none: no redactor, no
	// secret.resolved, no delivery; call sites treat the nil set as empty.
	// The engine never resolves anything itself: mamori supplies the whole
	// resolution layer (see internal/secrets).
	Secrets *secrets.Set

	// Params are the run's parameters (senro.WithParams), read by every
	// node's When conditions (see pruned). nil behaves as empty:
	// senro.Branch and senro.ParamIs simply never match.
	Params map[string]string

	// Binaries provisions the binary a func step running off the coordinator
	// is re-entered as; see internal/binprov. nil means the identity
	// strategy alone: a same-platform target gets os.Executable(), any other
	// platform is refused with an error naming what to configure. That is a
	// real default (a Linux coordinator driving Linux hosts needs nothing
	// more). senro.Run always supplies one; a plan with no remote func step
	// never reaches it.
	Binaries *binprov.Provisioner

	// Analyze is the analyzer senro.WithAnalyzer configured, or nil. Nil is
	// the free path: no goroutine, no queue, one nil check when a step
	// settles failed. See analyze.go.
	Analyze *AnalyzeOptions
}

// Run executes p to completion: it opens the run's ledger and log set under
// Options.Dir, schedules every node respecting Needs and ContinueOnError, and
// returns the run's rolled-up status.
//
// The returned error reports an ENGINE failure (unexecutable plan, ledger
// could not be opened or written), never a step failure: a run full of
// failed steps returns a nil error, with the outcome in the RunStatus and
// the event stream. A run that executed nothing must never come back as a
// silent RunSucceeded; see the "stuck" handling in schedule.
func Run(ctx context.Context, p *plan.Plan, opts Options) (api.RunStatus, error) {
	// Run accepts any *plan.Plan, not only ones senro.Build validated; a
	// cyclic or dangling-need plan left unchecked leaves the scheduler stuck
	// with zero ready nodes. ValidateWithGrace, not Validate: Run is the one
	// caller that knows the cleanup budget, so it can catch an Always
	// handler timeout certain to be killed mid-cleanup (see its doc).
	if err := p.ValidateWithGrace(opts.cleanupGrace()); err != nil {
		return "", fmt.Errorf("engine: %w", err)
	}

	// A plan declaring a workspace, scratch cache or Pure() step cannot be
	// faithfully executed without a store; running it anyway would report
	// success over a run that quietly did less than the plan describes.
	if opts.Storage == nil && planNeedsStorage(p) {
		return "", fmt.Errorf(
			"engine: this plan declares workspaces, scratch caches or Pure() steps but no storage was " +
				"configured; running it anyway would drop every workspace and every cache result silently")
	}

	if opts.MaxParallel <= 0 {
		opts.MaxParallel = runtime.NumCPU()
	}
	if opts.Sink == nil {
		opts.Sink = sink.Nop()
	}

	if err := checkSecretRefs(p, opts.Secrets); err != nil {
		return "", err
	}

	// A run-start refusal like checkSecretRefs: a plan naming an executor
	// this run has no instance of should fail before the first step, not on
	// step forty.
	if err := checkExecutors(p, opts); err != nil {
		return "", err
	}

	// A condition nothing can parse fails closed at second zero rather than
	// mid-run when its node happens to become ready.
	if err := checkConditions(p); err != nil {
		return "", err
	}

	// The redactor is built before anything is opened or emitted: every
	// event passes through it, and redaction runs before the hub, so a
	// client never receives values regardless of its permissions.
	red := redact.New(opts.Secrets.RedactValues()...)
	if skipped := red.Skipped(); len(skipped) > 0 {
		return "", fmt.Errorf(
			"engine: secret(s) %s resolved to a value shorter than %d bytes; senro cannot "+
				"redact a value that short without redacting unrelated output, so it refuses "+
				"to run rather than deliver a credential it cannot protect. "+
				"Use a longer credential, or stop declaring this field as a secret",
			strings.Join(skipped, ", "), redact.MinLength)
	}

	// checkSecretChannels refuses, before eventlog.Open, a plan that would
	// put a resolved secret into a command argument or environment value:
	// neither channel is reachable from this process once the child starts.
	// It must come after red is built, since it consumes the same automaton
	// the ledger's own redaction uses.
	if err := checkSecretChannels(p, red); err != nil {
		return "", err
	}

	ledger, err := eventlog.Open(opts.Dir)
	if err != nil {
		return "", fmt.Errorf("engine: %w", err)
	}
	logs := eventlog.NewLogSet(opts.Dir)

	// ws is nil whenever opts.Storage is nil, which the check above limits
	// to plans declaring nothing that needs it; every mount and snapshot
	// call below is guarded by rc.ws != nil.
	var ws *wsManager
	if opts.Storage != nil {
		ws, err = newWSManager(
			opts.Dir, p, opts.Storage.Snapshotter, opts.Storage.Scratch,
			opts.Storage.Persist, opts.RunID, lockerFor(opts.Executors))
		if err != nil {
			_ = ledger.Close()
			return "", err
		}

		// Leased persistent workspaces are given back on every path out of
		// Run, including pre-step failures. abandonPersistent is idempotent
		// and records nothing; the ordinary path below still releases them
		// with measured sizes first. A leaked lease would block every other
		// process on this machine from the workspace until this one exited.
		defer ws.abandonPersistent()

		// Between a step's CAS Put and either its cacheSave or the
		// end-of-run pin, this run's snapshots are referenced by nothing,
		// and a GC sweep in that window would correctly delete them (see
		// cache.MarkRunInFlight). Marked before any step can run, cleared
		// on every exit by the deferred call below. Failing to WRITE the
		// marker is not fatal, matching the pin's "protection, not a
		// result" treatment.
		inFlightDir := filepath.Join(opts.Storage.Root, "inflight")
		if err := cache.MarkRunInFlight(inFlightDir, opts.RunID); err != nil {
			_ = err
		}
		defer func() { _ = cache.ClearRunInFlight(inFlightDir, opts.RunID) }()
	}

	// The run directory holds what happened (events.jsonl, logs/) and what
	// was meant to happen (plan.json). Without plan.json the run cannot be
	// reproduced and the API has nothing to serve, so its write failure is
	// as fatal as a ledger write. Written before the first event so a
	// reader that sees plan.resolved can rely on the file being there.
	planJSON, err := p.Marshal()
	if err != nil {
		_ = ledger.Close()
		return "", fmt.Errorf("engine: marshal plan: %w", err)
	}
	if err := os.WriteFile(filepath.Join(opts.Dir, "plan.json"), planJSON, 0o644); err != nil {
		_ = ledger.Close()
		return "", fmt.Errorf("engine: write plan.json: %w", err)
	}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	rc := &runCore{
		ledger: ledger, sink: opts.Sink, runID: opts.RunID, cancel: cancel,
		oc: newOutcomes(len(p.Nodes)), ws: ws,
		redact: red, secrets: opts.Secrets,
		execs: opts.Executors, defaultExec: opts.Executor,
		groups:   buildGroupIndex(p),
		scope:    conditionScope(opts),
		spans:    newSpanTable(p, opts.TraceParent, opts.TraceState),
		binaries: opts.Binaries,
	}
	// Started before the run's first event so a step failing on the first
	// scheduling pass has somewhere to be offered; stopped by drain below
	// on every path out of Run.
	if opts.Analyze != nil && opts.Analyze.Analyzer != nil {
		rc.analysis = newAnalysis(rc, *opts.Analyze)
	}
	if rc.binaries == nil {
		// The identity strategy alone; see Options.Binaries.
		rc.binaries = binprov.New(binprov.Options{})
	}
	if opts.Storage != nil {
		rc.cache = opts.Storage.ActionCache
	}

	// checkFuncIdentity fails a run that cannot identify its own binary at
	// second zero rather than on the step that needed it. It runs against
	// rc, unlike the earlier refusals, so the memoized binOnce it triggers
	// is the one cacheLookup reuses: one hash of a possibly
	// hundred-megabyte binary, not two. Nothing has reached the ledger yet,
	// so a failure here looks exactly like the earlier refusals.
	if err := checkFuncIdentity(rc, p); err != nil {
		_ = ledger.Close()
		return "", err
	}

	// checkRemoteFunc provisions the binary every remote func step is
	// re-entered as, before the run emits anything, priming the provisioner
	// those steps reuse: twelve func steps on two architectures
	// cross-compile twice at second zero, not twelve times mid-run. This is
	// also where a cgo-tainted dependency graph is caught (see
	// internal/cgocheck).
	if err := checkRemoteFunc(runCtx, rc, p); err != nil {
		_ = ledger.Close()
		return "", err
	}

	// controlStop tells startRefusingControl (control.go) when it may stop
	// reading the control channel: right before Run returns, on every path.
	// defer, because every return below must close it exactly once.
	controlStop := make(chan struct{})
	defer close(controlStop)

	// Interactive sessions, if the observer hosts any (sink.ShellHost).
	// Started before the first event because a client may stand in a step
	// at the very first breakpoint; stopped by the same controlStop as
	// every other client-facing reader. The sessions themselves are ended
	// and waited for by closeShells far below, strictly before
	// run.finished seals the stream.
	rc.startShellServer(runCtx, p, controlStop)

	// A Sink that reports its own events (sink.Reporter; see notify) gets
	// the ledger appender before the first emit, so no outcome it produces
	// can arrive before it has somewhere to put it.
	if r, ok := opts.Sink.(sink.Reporter); ok {
		r.SetAppender(rc.report)
	}

	// The log archive runs for the length of this run: started after the
	// ledger exists and before the first step can produce output, drained
	// below once the ledger is sealed.
	if opts.Storage != nil {
		rc.archive = opts.Storage.Remote.Archive(opts.RunID)
	}

	// A shared cache that stops working reports it into this run's ledger:
	// "the cache went away" is the one fact that explains an inexplicably
	// slow build. Subscribed for this run only, since an embedder may reuse
	// one Storage across runs and a degradation after the seal must not
	// append. Through rc.emit, not rc.report: report's allowlist is
	// deliberately the notify types alone.
	if opts.Storage != nil && opts.Storage.Remote != nil {
		defer opts.Storage.Remote.Observe(func(d remotecache.Degradation) {
			msg := ""
			if d.Err != nil {
				msg = d.Err.Error()
			}
			rc.emit(api.Event{
				Type: api.CacheDegraded, Run: opts.RunID,
				Payload: mustMarshal(api.CacheDegradedBody{
					Store: d.Store, Op: d.Op, Error: msg, Disabled: d.Disabled,
				}),
			})
		})()
	}

	digest := p.Digest()
	started := time.Now().UTC()
	rc.emit(api.Event{
		Type: api.RunStarted, Run: opts.RunID,
		Payload: mustMarshal(api.RunStartedBody{
			Pipeline:      opts.Pipeline,
			EngineVersion: engineVersion,
			PlanDigest:    digest,
			StartedAt:     started,
			SpanID:        rc.spans.runSpan,
			ParentSpanID:  rc.spans.parentSpan,
			TraceFlags:    api.FormatTraceFlags(rc.spans.flags),
			TraceState:    rc.spans.state,
		}),
	})
	rc.emit(api.Event{
		Type: api.PlanResolved, Run: opts.RunID,
		Payload: mustMarshal(api.PlanResolvedBody{Digest: digest, Nodes: len(p.Nodes)}),
	})

	// One secret.resolved per credential: identity only, never a value
	// (see api.SecretResolvedBody).
	for _, id := range opts.Secrets.Identities() {
		rc.emit(api.Event{
			Type: api.SecretResolved, Run: opts.RunID,
			Payload: mustMarshal(api.SecretResolvedBody{
				Name: id.Name, Source: id.Source, Version: id.Version,
			}),
		})
	}

	// One event per declared expansion, BEFORE the step.created loop:
	// api.RunState.Apply materialises a group's children from plan.expanded,
	// and a client that saw the children first would flash them ungrouped.
	// Children are listed in plan order so a re-run reconstitutes the same
	// list; Count is len(Children) so the two cannot disagree.
	for _, g := range p.Groups {
		children := p.GroupMembers(g.Name)
		if len(children) == 0 {
			rc.emit(api.Event{
				Type: api.PlanExpansionSkipped, Step: g.Name, Group: g.Name,
				Payload: mustMarshal(api.PlanExpansionSkippedBody{
					Parent: g.Name,
					Reason: "the unit graph matched nothing, so this expansion produced no steps",
				}),
			})
			continue
		}
		rc.emit(api.Event{
			Type: api.PlanExpanded, Step: g.Name, Group: g.Name,
			Payload: mustMarshal(api.PlanExpandedBody{
				Parent: g.Name, Children: children, Count: len(children),
			}),
		})
	}

	for _, n := range p.Nodes {
		rc.emit(api.Event{
			Type: api.StepCreated, Step: n.ID,
			Payload: mustMarshal(api.StepCreatedBody{Kind: n.Kind, Group: n.Group, Needs: n.Needs}),
		})
	}

	// What leasing this run's persistent workspaces evicted, reported now
	// that there is a stream. The eviction itself already happened in
	// newWSManager: the measurement below must describe the workspace a
	// step will actually see, not one an eviction is about to empty.
	if ws != nil {
		for _, ev := range ws.pendingEvictions() {
			rc.emitEviction(ev, evictAtAcquire)
		}
		// Every persistent workspace is measured HERE: after the ledger can
		// carry the result, strictly before the scheduler can start a step
		// whose cache key would mention one (see openPersistent). A failure
		// is fatal, not a degradation: running with a workspace whose
		// recorded state does not describe its content is the one outcome
		// this measurement exists to prevent.
		opening, err := ws.openPersistent(runCtx)
		if err != nil {
			rc.seal()
			_ = logs.Close()
			_ = ledger.Close()
			return "", err
		}
		for _, s := range opening {
			// No Step: an opening measurement belongs to the run, and there
			// is no step it could honestly be attributed to. `senro ws ls`
			// folds it in with every other ws.snapshot (see cmd_ws.go).
			rc.emit(api.Event{
				Type: api.WSSnapshot,
				Payload: mustMarshal(api.WSSnapshotBody{
					Name: s.Name, Digest: string(s.Digest), Index: string(s.Index),
					Bytes: s.Bytes, Files: s.Files,
				}),
			})
		}
	}

	// The teardown sequence, in order (see shutdown.go for each stage):
	//
	//	run context cancelled (or scheduling finished)
	//	  → wait for in-flight steps to exit, up to grace/2
	//	  → kill whatever remains
	//	  → run Always handlers on a FRESH context with the full grace budget,
	//	    for every node that did not already run its own when it settled
	//	  → wait for any settle-time Always handler still in flight
	//	  → emit run.finished, sealing the stream in the same breath
	//	  → flush and close the ledger and the log set
	grace := opts.cleanupGrace()
	states, schedErr := rc.waitForSchedule(runCtx, p, opts, logs, grace, controlStop)
	teardownAbandoned := rc.runAlways(runCtx, p, opts, logs, grace, states)
	// Settle-time cleanup ignores cancellation by design and can outlive an
	// abandoned step goroutine; closing the ledger and log set under a live
	// handler would lose the record of the very cleanup this package
	// guarantees. Whether the wait ran out is reported in run.finished: a
	// cleanup the engine gave up on leaves a handler.started with no
	// terminal event, which reads like one that finished quietly but means
	// something very different to whoever must decide whether a lock is
	// still held.
	cleanupFinished := rc.waitForSettleTimeCleanup(grace)
	// Every open session ends and is waited for here, the last point its
	// shell.closed can still reach the ledger: run.finished seals the
	// stream in the same critical section that appends it. A session must
	// not hold a run open, and a run must not end with somebody standing
	// inside it.
	rc.closeShells()

	// The analyzer is stopped before run.finished, the one place a run ever
	// waits on one: unlike a notification, its answer is about something
	// that already happened, so a proposal arriving while the stream is
	// open is an ordinary event (notify, by contrast, flushes after the
	// seal). Bounded by grace then cancellation, so a wedged analyzer
	// delays exit by at most the grace. No scheduling decision above ever
	// waited on it.
	rc.analysis.drain()

	var status api.RunStatus
	if schedErr == nil {
		histogram := make(map[api.State]int, len(states))
		stateList := make([]api.State, 0, len(states))
		for _, st := range states {
			histogram[st]++
			stateList = append(stateList, st)
		}
		status = api.RollUp(stateList)

		// Scratch caches are saved after the outcome is known, before
		// run.finished. The directory is run-scoped, so the "only on step
		// success" rule becomes "only when the run succeeded" at this
		// granularity (see saveScratch for why a per-step save would race).
		// WithoutCancel: saving is cleanup, and a cancelled run still wants
		// the module cache it warmed.
		if ws != nil {
			ws.saveScratch(context.WithoutCancel(ctx), opts.Dir,
				status == api.RunSucceeded || status == api.RunSucceededWithRecovery)
			// Persistent workspaces are given back in the same window, so a
			// MaxSize eviction still has somewhere to be reported.
			// Unconditionally: a workspace released only on success would
			// never refresh its age on a machine whose builds were broken,
			// and the dependency cache would be the first thing to age out.
			for _, ev := range ws.releasePersistent() {
				rc.emitEviction(ev, evictAtRelease)
			}
		}

		// A run that ended badly is about to be debugged, and its
		// workspaces are the evidence; unpinned, a size-budget GC sweep
		// could delete exactly the snapshot being looked at. Both digests
		// go in per snapshot: the body so it can be restored, the index so
		// `ws ls` still works (see internal/cache/gc.go's references doc).
		if opts.Storage != nil && ws != nil && status != api.RunSucceeded && status != api.RunSucceededWithRecovery {
			if err := cache.WritePin(filepath.Join(opts.Storage.Root, "pins"), cache.Pin{
				RunID: opts.RunID, Status: string(status), Finished: time.Now().UTC(),
				Digests: ws.allSnapshotDigests(),
			}); err != nil {
				// A pin is protection, not a result: the run still produced
				// its output, and only a later GC sweep pays.
				_ = err
			}
		}

		// emitFinal, not emit: the append and the seal are one critical
		// section, so nothing can land after run.finished. See emitFinal.
		rc.emitFinal(api.Event{
			Type: api.RunFinished, Run: opts.RunID,
			Payload: mustMarshal(api.RunFinishedBody{
				Status: status, Steps: histogram, Duration: time.Since(started),
				// Either loss mode counts: an operator asking "might a lock
				// still be held?" does not care which pass ran out of time.
				CleanupAbandoned: teardownAbandoned || !cleanupFinished,
				SpanID:           rc.spans.runSpan,
			}),
		})
	} else {
		// Engine-failure path: no status was computed. Scratch records are
		// still written, with succeeded forced false, so a declared scratch
		// cache does not silently end up with no record at all.
		if ws != nil {
			ws.saveScratch(context.WithoutCancel(ctx), opts.Dir, false)
			// Released BEFORE the seal, so a MaxSize eviction on a run the
			// scheduler could not finish is recorded rather than happening
			// silently in the deferred abandon.
			for _, ev := range ws.releasePersistent() {
				rc.emitEviction(ev, evictAtRelease)
			}
		}
		// No run.finished on this path; seal anyway so nothing lands
		// between here and the close.
		rc.seal()
	}

	// Anything the analyzer could not get into the now-sealed stream is
	// permanent; report it rather than swallow it. Silent when there is
	// nothing to say.
	rc.analysis.report()

	closeErr := logs.Close()
	if err := ledger.Close(); err != nil && closeErr == nil {
		closeErr = err
	}

	// Queued AFTER the close: an events file uploaded while the run was
	// still appending would be a prefix of the run. Then drain with a
	// bounded grace: a run that cannot exit because an object store is slow
	// is worse than unarchived logs. Failures here are degradations and
	// never touch closeErr.
	rc.archive.Ledger(filepath.Join(opts.Dir, "events.jsonl"))
	rc.archive.Close(opts.CleanupGrace)

	// Precedence: a ledger write failure wins; a stuck scheduler is next
	// (its states are not trustworthy enough to report a status over); a
	// close error last, usually a symptom of the above.
	if fatal := rc.Fatal(); fatal != nil {
		return "", fatal
	}
	if schedErr != nil {
		return "", schedErr
	}
	if closeErr != nil {
		return "", fmt.Errorf("engine: %w", closeErr)
	}
	return status, nil
}

// runCore is the state every scheduling helper needs to record an event or
// abort the run. It is deliberately small: schedule and runStep close over
// it instead of threading the ledger, sink and run ID through every call.
type runCore struct {
	ledger *eventlog.Ledger
	sink   sink.Sink
	runID  string
	cancel context.CancelFunc

	// oc is what the scheduler has settled. On runCore rather than locals
	// in schedule because teardown reads it after the scheduler may have
	// been abandoned (see shutdown.go's outcomes).
	oc *outcomes

	// ws owns this run's workspace directories; nil when the plan needs no
	// storage and Options.Storage was nil, which Run's planNeedsStorage
	// check keeps in agreement, so readers only guard against nil.
	ws *wsManager

	// archive uploads this run's logs and ledger to the shared store, off
	// the execution path. nil with no shared store; every method tolerates
	// nil, so call sites do not branch.
	archive *remotecache.Archiver

	// cache is this run's action cache handle, taken from Options.Storage;
	// nil under exactly the same condition ws is nil (see cacheable).
	cache cache.ActionCache

	// redact is this run's pattern set, built before the first event; nil
	// for a run with no secrets. Immutable once Run assigns it, which is
	// why append reads it with no lock and scans OUTSIDE emitMu: Sink.Emit
	// must never block, and a scan inside the ledger's critical section
	// would put every other emitter behind it.
	redact *redact.Set

	// secrets is the same set the redactor was built from, kept so the
	// delivery path (attempt.go) can look up a value by name.
	secrets *secrets.Set

	// execs and defaultExec are Options.Executors and Options.Executor,
	// captured at Run and immutable afterwards.
	execs       map[string]executor.Executor
	defaultExec executor.Executor

	// redactedPayloads counts replacements made in event payloads. Not a
	// secret.redacted event: that one is step-scoped, and emitting from
	// inside the emit path would recurse. Read by internal tests.
	redactedPayloads atomic.Int64

	// groups maps a step id to its expansion group, so append stamps
	// api.Event.Group in one place. Built once in Run, immutable, read
	// lock-free.
	groups map[string]string

	// scope is this run's condition scope (caller Params plus coordinator
	// environment). Built once in Run, immutable; pruned reads it
	// lock-free.
	scope cond.Scope

	// spans is this run's trace context and per-step spans, write-once
	// before the first event (see spanTable), so append stamps the trace ID
	// lock-free. Never nil for a run started through Run; append reads only
	// the trace ID, so a hand-assembled runCore in tests still emits,
	// without a trace.
	spans *spanTable

	// emitMu makes one ledger-append and its sink-delivery a single atomic
	// unit. The ledger's own mutex only covers seq allocation: releasing it
	// between Append and Sink.Emit lets a second emitter deliver seq N+1 to
	// the sink first, and api.RunState.Apply rejects a regressing seq.
	// Holding this lock across Sink.Emit is safe because Emit is
	// non-blocking by the Sink contract.
	emitMu sync.Mutex

	// sealed closes the run's event stream for good. Guarded by emitMu and
	// set in the same critical section as the final append (emitFinal), so
	// "nothing after run.finished" holds by construction.
	sealed bool

	// shellCancel ends every open interactive session; shellWG is how
	// closeShells knows they have ended. Set up by startShellServer before
	// any session can exist; nil/empty for a run whose observer hosts no
	// shells. See shell.go.
	shellCancel context.CancelFunc
	shellWG     sync.WaitGroup

	mu    sync.Mutex
	fatal error

	// binOnce, binDigest and binErr memoize this binary's content digest, a
	// func step's cache identity. Computed at most once per run, and only
	// when a plan has a Pure() func step.
	binOnce   sync.Once
	binDigest string
	binErr    error

	// binaries provisions the binary a remote func step is re-entered as;
	// one per run, so repeated targets cross-compile once. Never nil: Run
	// substitutes an identity-only provisioner for a nil Options.Binaries.
	binaries *binprov.Provisioner

	// analysis runs the caller's analyzer, or is nil when none is
	// configured. Every method on a nil *analysis is a no-op, so call sites
	// do not branch. See analyze.go.
	analysis *analysis
}

// binaryDigest is sha256 of this process's own executable, part of a func
// step's cache key: the function's BODY is compiled into this binary and
// invisible to every other key component, so without it editing a
// registered function and re-running would serve the old result forever.
func (rc *runCore) binaryDigest() (string, error) {
	rc.binOnce.Do(func() {
		exe, err := os.Executable()
		if err != nil {
			rc.binErr = fmt.Errorf("engine: locating this binary for a func step's cache identity: %w", err)
			return
		}
		f, err := os.Open(exe)
		if err != nil {
			rc.binErr = fmt.Errorf("engine: reading %s for a func step's cache identity: %w", exe, err)
			return
		}
		defer func() { _ = f.Close() }()
		h := sha256.New()
		if _, err := io.Copy(h, f); err != nil {
			rc.binErr = fmt.Errorf("engine: hashing %s: %w", exe, err)
			return
		}
		rc.binDigest = "sha256:" + hex.EncodeToString(h.Sum(nil))
	})
	return rc.binDigest, rc.binErr
}

// seal stops this run's event stream; later emits are dropped silently, not
// recorded as fatal, since a dropped event during teardown is an expected
// part of shutting down. Only the engine-failure path calls this directly;
// the ordinary path seals through emitFinal, keeping the last append and
// the seal in one critical section.
func (rc *runCore) seal() {
	rc.emitMu.Lock()
	defer rc.emitMu.Unlock()
	rc.sealed = true
}

// emit appends e to the ledger and only then hands the stamped copy to the
// sink, both under emitMu. Append-before-emit is load-bearing: the ledger
// is the source of truth, and a sink must never observe an event that does
// not yet exist on disk. A ledger error is fatal (first one wins) and
// cancels the run; Run surfaces it as the returned error. After seal, every
// call is a silent no-op.
func (rc *runCore) emit(e api.Event) { rc.append(e, false) }

// notifyTypes are the only event types a Sink may append through report:
// the ones describing an observer's OWN behaviour, the only thing an
// observer is authoritative about. Without this allowlist arbitrary
// observer code could append, say, a step.finished for a step that never
// ran, and every downstream reader would believe it.
var notifyTypes = map[api.Type]bool{
	api.NotifyDelivered: true,
	api.NotifyFailed:    true,
	api.NotifyDropped:   true,
}

// report is what a sink.Reporter is handed (see Run). It appends one of the
// observer's own events and reports whether it landed.
//
// False means the type is not in notifyTypes, or the stream is sealed. The
// seal case is by design, not a bug to unseal around: run.finished is
// appended and sealed in one critical section, so the outcome of delivering
// run.finished itself can never be an event. A notifier that gets false
// reports the outcome somewhere that still exists (standard error, at
// shutdown: see notify.Notifier.Flush).
//
// Never called from inside Emit: see sink.Reporter's doc for why that would
// deadlock.
func (rc *runCore) report(e api.Event) bool {
	if !notifyTypes[e.Type] {
		return false
	}
	return rc.append(e, false)
}

// emitFinal appends e and closes the stream behind it under one hold of
// emitMu, making e the last event in the ledger by construction.
// Emit-then-seal is two critical sections, and a concurrent emitter (an
// abandoned step goroutine whose orphan is still producing output) can take
// emitMu in the gap and append after run.finished.
func (rc *runCore) emitFinal(e api.Event) { rc.append(e, true) }

// redactPayload removes every registered secret value from one event's
// body. Redaction happens before the hub, so a client never receives values
// regardless of authorization; a filter on the way out to a client would be
// too late, since a FileSource reader opens events.jsonl with no server in
// the loop.
//
// It runs OUTSIDE emitMu: rc.redact is immutable so needs no lock, and a
// scan inside the ledger's critical section would put every emitter behind
// it. A no-secrets run pays one nil check.
//
// A replacement can only break the JSON when the secret itself contains
// JSON punctuation (the placeholder has no quote or backslash); the answer
// is a body a fold can skip, with the routing fields outside Payload
// untouched.
func (rc *runCore) redactPayload(p json.RawMessage) json.RawMessage {
	if rc.redact == nil || len(p) == 0 {
		return p
	}
	out, n := rc.redact.Redact(p)
	if n == 0 {
		return p
	}
	rc.redactedPayloads.Add(int64(n))
	if !json.Valid(out) {
		return json.RawMessage(`{"redacted":true}`)
	}
	return out
}

// append is emit and emitFinal's shared body. final seals the stream in the
// same critical section as the append, whether or not the append itself
// succeeded. It reports whether e reached the ledger; emit and emitFinal
// ignore that (an engine emit site has nothing useful to do about it),
// report needs it.
func (rc *runCore) append(e api.Event, final bool) bool {
	e.Run = rc.runID

	// One place, so no emit site can forget. An event that already carries
	// a group (plan.expanded) keeps it.
	if e.Group == "" && e.Step != "" && rc.groups != nil {
		e.Group = rc.groups[e.Step]
	}

	// The trace ID goes on EVERY event here: events with differing trace
	// IDs would be as many single-event traces as the run has events. One
	// string copy, no lock (rc.spans is write-once), which matters because
	// step.log.appended comes through once per output write.
	if rc.spans != nil {
		e.TraceID = rc.spans.traceID
	}

	// Before rc.ledger.Append, not only before rc.sink.Emit: "secret values
	// never in cache keys, events, or logs" is unconditional. Outside
	// emitMu; see redactPayload.
	e.Payload = rc.redactPayload(e.Payload)

	rc.emitMu.Lock()
	defer rc.emitMu.Unlock()

	if rc.sealed {
		return false
	}

	stamped, err := rc.ledger.Append(e)
	if final {
		rc.sealed = true
	}
	if err != nil {
		rc.mu.Lock()
		first := rc.fatal == nil
		if first {
			rc.fatal = err
		}
		rc.mu.Unlock()
		if first {
			rc.cancel()
		}
		return false
	}
	rc.sink.Emit(stamped)
	return true
}

// emitStepFinished is the step.finished shape for outcomes the scheduler
// itself decides (skip-cascade and cancellation), as opposed to the richer
// one runStep emits. reason names the condition or upstream need behind a
// skip, never a resolved value; every other call site passes "". It reports
// whether the event was emitted: false means another path already settled
// this node (see outcomes.settle: one step.finished per node).
func (rc *runCore) emitStepFinished(id string, state api.State, reason string) bool {
	if !rc.oc.settle(id, state) {
		return false
	}
	rc.stepFinishedEvent(id, state, reason)
	return true
}

// stepFinishedEvent emits the event alone, for a caller that has ALREADY
// taken the node's claim. The only such caller is control.go's step.skip,
// which writes oc.finished and the scheduler's states in one critical
// section (holding &rc.oc.mu, both maps' lock) and so cannot go back
// through oc.settle without losing its own claim. Never call this without
// holding the node's claim.
func (rc *runCore) stepFinishedEvent(id string, state api.State, reason string) {
	body := api.StepFinishedBody{State: state, Reason: reason}
	// Every caller here is a step that never started, so the span is minted
	// here or nowhere; a step missing from the trace entirely is worse than
	// one recorded as skipped.
	body.SpanID, body.ParentSpanID, body.LinkedSpanIDs = rc.spans.finishSpan(id)
	rc.emit(api.Event{
		Type: api.StepFinished, Step: id, Attempt: 1,
		Payload: mustMarshal(body),
	})
}

// Fatal reports the first ledger error seen, if any.
func (rc *runCore) Fatal() error {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	return rc.fatal
}

// schedule runs every node in p to a terminal state, respecting Needs and
// ContinueOnError, under a single global concurrency limit, and returns
// each node's terminal state keyed by ID.
//
// It is one control loop plus per-step goroutines: the loop alone decides
// what becomes ready or skipped; each step goroutine reports back and wakes
// the loop, so a node starts the instant its own dependencies clear.
//
// A non-nil error means the loop found no ready, settled, or in-flight work
// while nodes remained unresolved (a cycle or dangling need that slipped
// past validation); Run must not report a status computed from the returned
// states in that case.
//
// The states and running maps live on rc.oc, not here, because teardown may
// read them while this loop is still going (see waitForSchedule). Returning
// rc.oc.states directly is safe only on paths that have already waited for
// every goroutine.
func (rc *runCore) schedule(ctx context.Context, p *plan.Plan, opts Options, logs *eventlog.LogSet, controlStop <-chan struct{}) (map[string]api.State, error) {
	byID := make(map[string]*plan.Node, len(p.Nodes))
	for i := range p.Nodes {
		byID[p.Nodes[i].ID] = &p.Nodes[i]
	}

	mu := &rc.oc.mu
	states := rc.oc.states
	running := rc.oc.running
	sem := make(chan struct{}, opts.MaxParallel) // global across the whole run
	// release and acquire let a step give back its slot for a retry's
	// backoff sleep: sleeping is not working, and holding the slot would
	// stall every other ready step for the length of the backoff. Closures
	// rather than the channel, so attempt.go never needs to know how
	// MaxParallel is enforced.
	release := func() { <-sem }
	acquire := func(ctx context.Context) error {
		select {
		case sem <- struct{}{}:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	// One semaphore per group that declares a limit, held IN ADDITION to
	// the global one. Acquisition order is group first, then global,
	// released in reverse. The order matters: the opposite one deadlocks
	// nothing but starves the whole plan behind a MaxParallel(1) group,
	// since a child waiting its group's turn would sit on a global slot.
	groupSem := make(map[string]chan struct{}, len(p.Groups))
	for _, g := range p.Groups {
		if g.MaxParallel > 0 {
			groupSem[g.Name] = make(chan struct{}, g.MaxParallel)
		}
	}
	permits := func(n *plan.Node) (acquire func(context.Context) error, release func()) {
		g := groupSem[n.Group]
		acquire = func(ctx context.Context) error {
			if g != nil {
				select {
				case g <- struct{}{}:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			select {
			case sem <- struct{}{}:
				return nil
			case <-ctx.Done():
				if g != nil {
					<-g
				}
				return ctx.Err()
			}
		}
		release = func() {
			<-sem
			if g != nil {
				<-g
			}
		}
		return acquire, release
	}

	var wg sync.WaitGroup
	wake := make(chan struct{}, 1)
	signal := func() {
		select {
		case wake <- struct{}{}:
		default:
		}
	}

	// controlCh is nil whenever the Sink carries no control channel, the
	// ordinary case with no attach server. A nil channel in a select never
	// fires, so every select below behaves correctly with no
	// special-casing.
	controlCh := opts.Sink.Control()
	// Armed breakpoints, owned by this goroutine alone: serve and readySet
	// are the only things that touch them and both run here, which is what
	// makes the map lock-free (see breakpoint). A nil control channel
	// means no client can arm one, so the map stays empty.
	breakpoints := make(map[string]*breakpoint)
	// The run-wide pause (api.OpRunPause), owned by this goroutine for the
	// same reason the breakpoints map is: handleRunPause writes it only
	// from serve, and only this loop reads it.
	paused := false
	handle := func() schedHandle {
		return schedHandle{
			ctx: ctx, p: p, byID: byID, mu: mu, states: states, running: running,
			breakpoints: breakpoints, paused: &paused,
			wg: &wg, acquire: acquire, release: release, signal: signal,
			logs: logs, opts: opts,
		}
	}
	serve := func(req sink.ControlRequest) { rc.serveControl(handle(), req) }

	// policyCh carries decisions the caller's analyze policy made (see
	// senro.AcceptWithoutHumanApproval). A SECOND channel rather than more
	// traffic on controlCh: controlCh belongs to the attach hub and does
	// not exist with nothing attached, so a policy heard only there would
	// silently stop working in CI; and a policy decision must stay
	// distinguishable from a client's all the way to
	// api.AnalysisDecisionBody.Policy, a fact about where the request came
	// from. Nil (never fires) for a run with no analyzer or no policy.
	var policyCh <-chan sink.ControlRequest
	if rc.analysis != nil {
		policyCh = rc.analysis.decisions
	}
	servePolicy := func(req sink.ControlRequest) {
		// Only the two analysis operations, not the whole op switch: an
		// auto-accept policy must not become a way to cancel a run.
		switch req.Op {
		case api.OpAnalysisAccept:
			rc.handleAnalysisAccept(handle(), req, true)
		case api.OpAnalysisReject:
			rc.handleAnalysisReject(handle(), req, true)
		default:
			refuse(req, reasonUnknownOp)
		}
	}

	for {
		mu.Lock()
		// Read ONCE per pass and reused below: the idle wait's decision to
		// watch ctx.Done() must describe the same instant readySet was
		// given. Two reads let one pass both decline to settle a cancelled
		// run and decline to watch for the cancellation, leaving no wake
		// source at all (surfaced under load as
		// TestControlRunPauseStillNoticesAnExternalCancel ending at exactly
		// grace/2).
		runCancelled := ctx.Err() != nil
		ready, settled, reasons, held := readySet(p.Nodes, byID, states, running, runCancelled, rc.pruned, breakpoints)
		if paused {
			// A paused run dispatches nothing new: ready nodes are DROPPED,
			// stay pending, and are reclassified after the resume. Dropped
			// here rather than inside readySet: readySet answers "what would
			// this run do next", a question about the graph; pause is a
			// decision about acting on that answer. Crucially `settled` is
			// left alone, which a pause must NOT suppress: a dependent whose
			// upstream failed while paused is still skipped, because
			// settling is not dispatching.
			ready = nil
		}
		for _, n := range ready {
			running[n.ID] = true
		}
		for id, st := range settled {
			states[id] = st
		}
		// done, idle and stuck are derived under the same lock that produced
		// ready/settled, so they describe one consistent snapshot.
		// Re-reading `running` in a later critical section was a real
		// defect: a healthy plan got aborted as "dependency cycle or
		// dangling need" when the last in-flight step completed in the gap.
		done := len(states) == len(p.Nodes)
		idle := len(ready) == 0 && len(settled) == 0
		// held and paused separate "nothing can ever happen" from "nothing
		// can happen until a client says so": a run held at a breakpoint,
		// or paused, has exactly the shape stuck detects, and without these
		// terms the scheduler aborted a valid plan with a false
		// dependency-cycle message (see
		// TestControlBreakpointHeldRunIsNotDeclaredStuck and
		// TestControlRunPausedRunIsNotDeclaredStuck). paused staying true
		// after a cancellation costs nothing: readySet settles every
		// non-running node once ctx is done, so a cancelled run reaches
		// `done` without needing stuck.
		stuck := idle && len(running) == 0 && len(held) == 0 && !paused
		unresolved := len(p.Nodes) - len(states)
		mu.Unlock()

		if len(settled) > 0 {
			// Sorted so nodes settling in the same pass are recorded in a
			// deterministic order; map iteration would make two runs of one
			// plan byte-different.
			ids := make([]string, 0, len(settled))
			for id := range settled {
				ids = append(ids, id)
			}
			sort.Strings(ids)
			for _, id := range ids {
				rc.emitStepFinished(id, settled[id], reasons[id])
			}
		}

		// After this pass's step.finished events, so the ledger reads in the
		// order things happened: the upstream settled, then the scheduler
		// declined to dispatch what it unblocked.
		rc.announceBreakpoints(breakpoints, held)

		if done {
			// A configured accept policy may still have something to say: an
			// analyzer answering about the last failure can apply a retry,
			// and only this loop can dispatch it. So the run waits, bounded
			// by the same grace drain uses, until outstanding answers are
			// served. Terminates because a policy applies at most once per
			// step (see analysis.applied). A run with no policy skips this
			// on a nil check.
			if rc.analysis.settlePolicy(servePolicy) {
				continue
			}
			// Nothing left to schedule: this loop will never read controlCh
			// again. startRefusingControl takes over reading it, durably,
			// for the rest of Run's lifetime, teardown included (see its
			// doc).
			rc.startRefusingControl(controlCh, controlStop)
			break
		}
		if stuck {
			rc.startRefusingControl(controlCh, controlStop)
			wg.Wait()
			return states, fmt.Errorf(
				"engine: scheduler stuck: %d of %d nodes never became ready or "+
					"cancelled (a dependency cycle or dangling need slipped past validation)",
				unresolved, len(p.Nodes))
		}
		if idle {
			// Nothing changed this pass: wait for an in-flight step to
			// report back or a control request. Every dispatched goroutine
			// calls signal() exactly once on its way out, so an ordinary
			// idle pass is guaranteed to wake without polling ctx.Done().
			//
			// A pass holding a node at a breakpoint, or paused, is the
			// exception: with nothing running, no step goroutine is left to
			// signal and an external cancellation arrives on neither
			// channel, so the loop would sit here until teardown abandoned
			// it at grace/2. Watching ctx.Done() lets the next pass settle
			// held nodes as cancelled through readySet's cancelled branch.
			// A pause is the bigger hazard: a breakpoint holds one node, a
			// pause the whole plan.
			//
			// Gated rather than unconditional, because ctx.Done() stays
			// ready once closed: a pass idle merely because steps are still
			// running would spin at full CPU (see
			// TestControlBreakpointStillNoticesAnExternalCancel and
			// TestControlRunPauseStillNoticesAnExternalCancel).
			//
			// The two gates differ by necessity. len(held) disarms itself:
			// readySet settles a cancelled run's nodes before consulting
			// breakpoints, so held empties once the run is cancelled.
			// `paused` does not disarm, so it checks runCancelled
			// explicitly; without that, a run cancelled while paused with a
			// step still running would spin until the step finished.
			// runCancelled, never a fresh ctx.Err(): see the read-once
			// comment at the top of this pass. Safe because the cases are
			// exhaustive: a pass that did not observe the cancellation
			// watches ctx.Done(); a pass that did has already settled every
			// non-running node, so it has a non-empty `settled`, or is done,
			// or waits on a goroutine guaranteed to signal.
			var cancelled <-chan struct{}
			if len(held) > 0 || (paused && !runCancelled) {
				cancelled = ctx.Done()
			}
			select {
			case <-wake:
			case req, ok := <-controlCh:
				if ok {
					serve(req)
				}
			case req := <-policyCh:
				servePolicy(req)
			case <-cancelled:
			}
			continue
		}

		// A control request may have arrived while this pass was busy
		// dispatching; served non-blocking so it does not wait an entire
		// extra pass.
		select {
		case req, ok := <-controlCh:
			if ok {
				serve(req)
			}
		case req := <-policyCh:
			servePolicy(req)
		default:
		}

		for _, n := range ready {
			n := n
			wg.Add(1)
			go func() {
				defer wg.Done()
				acquire, release := permits(n)
				if err := acquire(context.Background()); err != nil {
					// Unreachable: acquire only fails on a cancelled context,
					// and this one cannot be. A dispatched node must always
					// take its slot so the ctx.Err() check below decides its
					// fate.
					return
				}
				var state api.State
				if ctx.Err() != nil {
					// Cancellation arrived after this step was marked ready.
					// A step that never ran must never be recorded as
					// started-and-failed, so this bypasses runStep (and its
					// step.started). runStep was never called, so nothing
					// else owns the slot: release it here.
					state = api.StateCancelled
					rc.emitStepFinished(n.ID, state, "")
					release()
				} else {
					// From here runStep owns releasing the slot, exactly
					// once, however it returns: its retry loop gives the
					// slot back during a backoff sleep, so an unconditional
					// release here would double-release when that loop
					// returns having just given it back (a cancelled backoff
					// wait). See runStep.
					state = rc.runStep(ctx, n, opts, logs, release, acquire)
				}
				mu.Lock()
				states[n.ID] = state
				delete(running, n.ID)
				mu.Unlock()
				signal()
			}()
		}
	}
	wg.Wait()
	return states, nil
}

// skipPropagation is the set of upstream states a dependent INHERITS rather
// than treats as a failure, mapped to the clause that finishes its
// step.finished reason ("upstream <id> " + this). One table rather than
// adjacent if-branches in readySet, so a new skip state gets every answer
// (ContinueOnError, resulting state, reason shape) at once.
//
// StateSkippedUpstreamFailed is deliberately NOT here: it means something
// broke, ContinueOnError is the escape hatch for it, and RollUp maps it to
// RunPartial. See api.StateSkippedManual.
var skipPropagation = map[api.State]string{
	api.StateSkippedCondition: "was skipped by a condition",
	api.StateSkippedManual:    "was skipped manually",
}

// readySet classifies every node not yet terminal and not already running.
//
// cancelled means the run's context is done: every such node is settled as
// StateCancelled without ever being weighed against its Needs. A node
// dispatched before the cancellation was observed is in `running` and
// skipped here; schedule's own goroutine handles it.
//
// prune reports whether a node otherwise ready is gated out by its When
// conditions, and why (see runCore.pruned); called only once a node's Needs
// are satisfied.
//
// A node with an armed breakpoint (see control.go) that would otherwise be
// ready is returned in held: neither dispatched nor settled, it stays put
// until the breakpoint clears. held is what tells the caller "waiting on a
// client" apart from "nothing left to do". The check sits AFTER prune and
// after the Needs evaluation, so a breakpoint fires when the step is
// genuinely about to be dispatched, and never for a node that was not going
// to run anyway.
func readySet(
	nodes []plan.Node,
	byID map[string]*plan.Node,
	states map[string]api.State,
	running map[string]bool,
	cancelled bool,
	prune func(*plan.Node) (bool, string),
	breakpoints map[string]*breakpoint,
) (ready []*plan.Node, settled map[string]api.State, reasons map[string]string, held []string) {
	settled = make(map[string]api.State)
	reasons = make(map[string]string)
	for i := range nodes {
		n := &nodes[i]
		if _, terminal := states[n.ID]; terminal {
			continue
		}
		if running[n.ID] {
			continue
		}
		if cancelled {
			settled[n.ID] = api.StateCancelled
			continue
		}

		blocked, waiting := false, false
		var inherited api.State
		var blockingNeed string
		for _, need := range n.Needs {
			st, ok := states[need]
			if !ok {
				waiting = true
				break
			}
			if _, propagates := skipPropagation[st]; propagates {
				// A skipped upstream is not a failed one: dependents are
				// skipped, not blamed. ContinueOnError deliberately does not
				// apply; it promises surviving a FAILURE, not running against
				// output that was never produced.
				inherited = st
				blockingNeed = need
				break
			}
			if !satisfies(st, byID[need].ContinueOnError) {
				blocked = true
				break
			}
		}
		switch {
		case inherited != "":
			settled[n.ID] = inherited
			reasons[n.ID] = "upstream " + blockingNeed + " " + skipPropagation[inherited]
		case blocked:
			settled[n.ID] = api.StateSkippedUpstreamFailed
		case waiting:
			// Still pending on an in-flight or unresolved dependency.
		default:
			// Pruned only after its dependencies settled, so the reason a
			// node did not run reads in the order a person reads the graph.
			if skip, because := prune(n); skip {
				settled[n.ID] = api.StateSkippedCondition
				reasons[n.ID] = because
				continue
			}
			if breakpoints[n.ID] != nil {
				held = append(held, n.ID)
				continue
			}
			ready = append(ready, n)
		}
	}
	return ready, settled, reasons, held
}

// satisfies reports whether a Need in state st (whose own node declared
// continueOnError) lets a dependent proceed as though it had succeeded.
// Keyed off State.Failed() rather than enumerating StateFailed alone: a
// dependent surviving a plain non-zero exit under ContinueOnError must
// equally survive a timed-out or panicked upstream, and State.Failed() is
// senro's one definition of "fails".
func satisfies(st api.State, continueOnError bool) bool {
	switch {
	case st == api.StateSucceeded, st == api.StateCached, st == api.StateRecovered:
		return true
	case st.Failed():
		return continueOnError
	default:
		return false
	}
}

// runStep executes one node to a terminal state, retrying it and bounding
// each attempt with a timeout as its plan.Node declares. See attempt.go.

// buildGroupIndex maps every routing id an event can carry to its group.
// Node ids are the obvious half; handler log step ids (handlerLogStep) are
// impossible to derive downstream, because a child's own id already
// contains "/" inside its unit ("lint[unit=apps/web]"), so nothing can
// split a joined id back apart. Registering them here, from the plan, is
// exact.
func buildGroupIndex(p *plan.Plan) map[string]string {
	var idx map[string]string
	for i := range p.Nodes {
		n := &p.Nodes[i]
		if n.Group == "" {
			continue
		}
		if idx == nil {
			idx = make(map[string]string)
		}
		idx[n.ID] = n.Group
		for kind, list := range map[string][]plan.Node{
			"on_failure": n.OnFailure,
			"always":     n.Always,
		} {
			for _, h := range list {
				idx[handlerLogStep(n.ID, kind, h.ID)] = n.Group
			}
		}
	}
	return idx
}

// mustMarshal encodes a payload body; a failure can only be a programming
// error, since every body type in package api is a plain JSON-safe struct.
func mustMarshal(v any) json.RawMessage {
	b, err := json.Marshal(v)
	if err != nil {
		panic(fmt.Sprintf("engine: marshal payload: %v", err))
	}
	return b
}
