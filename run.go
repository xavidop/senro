package senro

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/attach"
	"github.com/xavidop/senro/internal/binprov"
	"github.com/xavidop/senro/internal/engine"
	"github.com/xavidop/senro/internal/executor"
	"github.com/xavidop/senro/internal/executor/containerexec"
	"github.com/xavidop/senro/internal/executor/k8sexec"
	"github.com/xavidop/senro/internal/executor/localexec"
	"github.com/xavidop/senro/internal/executor/sshexec"
	"github.com/xavidop/senro/internal/plan"
	"github.com/xavidop/senro/internal/secrets"
	"github.com/xavidop/senro/internal/sink"
	"github.com/xavidop/senro/internal/stepchild"
	"github.com/xavidop/senro/internal/storage"
	"github.com/xavidop/senro/trigger"
)

// Plan is the resolved, immutable result of (*Pipeline).Build: a type alias
// for the wire-identical internal/plan type, not a copy or a wrapper. The
// alias exists so code outside this module can NAME Build's return type
// (internal/plan cannot be imported), which is what RunPlan's parameter and
// everything attach, replay and the golden fixtures build on require. An
// alias rather than a defined type, so *Plan and *plan.Plan are identical
// and there is nothing to convert.
type Plan = plan.Plan

// Option configures Run.
type Option func(*runConfig)

type runConfig struct {
	attach       *attach.Attach
	dir          string
	runID        string
	cacheDir     string
	localClass   string
	secrets      any
	hasSecrets   bool
	params       Params
	sinks        []Sink
	triggerEvent *trigger.Event
	triggers     []trigger.Trigger
	remoteCache  RemoteCache
	traceParent  string
	traceState   string
	hasTrace     bool
	funcPkg      string
	hasFuncPkg   bool
	analyze      *engine.AnalyzeOptions
	maxDepth     int
	maxNodes     int
	regenerate   bool
	only         map[string]bool
}

// Params are a run's parameters: the small, flat, string-valued facts a run
// is started with, which conditions read (senro.Branch, senro.ParamIs).
//
// A map rather than a struct because a trigger produces them and a CLI
// passes them through, neither of which knows the pipeline's Go types.
// Values are never recorded in an event or a cache key, so a credential
// passed here leaks into nothing durable; passing one into a step's Env or
// argv still gets the refusal WithSecrets produces.
type Params = map[string]string

// WithParams supplies the run's parameters. See Params and senro.When.
func WithParams(p Params) Option {
	return func(c *runConfig) { c.params = p }
}

// WithAttach hands Run the Sink of an already-listening attach server; see
// attach.Listen. Every event Run emits fans out to whatever is attached, and
// Run adopts att's run directory and RunID unless WithDir/WithRunID override
// them, so the attach server and the engine agree on exactly one run; see
// TestRunWithAttachSharesDirectoryAndRunID.
//
// Only *attach.Attach is accepted, not a bind-address string: a string sugar
// would make the wildcard TCP bind (which attach.Listen refuses without a
// certificate) the easiest thing to type, and would have nowhere to hand the
// bearer token back. Call attach.Listen yourself, read att.Token() if it
// bound TCP, and hand the result here.
func WithAttach(att *attach.Attach) Option {
	return func(c *runConfig) { c.attach = att }
}

// WithDir overrides the run directory Run uses. Unnecessary when
// WithAttach is also given (Run adopts att.Dir() by default), and
// unnecessary when neither is given, since Run then generates one under
// runs/<id> the same way attach.Listen does (attach.NewRunID). Set this to
// pin a specific, known path instead.
func WithDir(dir string) Option {
	return func(c *runConfig) { c.dir = dir }
}

// WithRunID overrides the run's ID, the same way WithDir overrides its
// directory and for the same reasons.
func WithRunID(id string) Option {
	return func(c *runConfig) { c.runID = id }
}

// WithLocalClass overrides the local executor's cache equivalence class,
// e.g. "local/darwin/arm64/go1.26" instead of the bare "local/darwin/arm64":
// the executor has no generic way to know what toolchain a step invokes, and
// two machines differing only in an unfingerprinted toolchain would
// otherwise silently share cache entries. Unset, Class() reports exactly
// what it always has.
func WithLocalClass(class string) Option {
	return func(c *runConfig) { c.localClass = class }
}

// WithCacheDir overrides where the content-addressed store, the action
// cache and the scratch cache live. Unset, Run uses storage.DefaultRoot:
// $SENRO_CACHE_DIR when set, and os.UserCacheDir()/senro otherwise.
//
// Unlike WithDir, this is deliberately NOT per-run. A run directory is one
// run's record; a cache root is shared by every run on the machine, and
// that sharing is the entire point of a cache.
func WithCacheDir(dir string) Option {
	return func(c *runConfig) { c.cacheDir = dir }
}

// WithMaxDepth bounds how deep generators may NEST: a generated step can
// itself be a generator, and without a bound that recurses until the machine
// gives out. Zero, the default, means three.
//
// Raise it for a pipeline that genuinely discovers work in layers; lower it
// to one to allow generation but forbid a generator producing generators.
func WithMaxDepth(n int) Option {
	return func(c *runConfig) { c.maxDepth = n }
}

// WithRegenerate makes generator steps ignore the action cache, so each one
// runs and produces a FRESH fragment instead of replaying the recorded one.
//
// Reach for it when the world has changed and the recorded graph describes a
// fleet that no longer exists. It is a separate switch, and not the default,
// because silently re-deriving a graph during what looked like a retry is a
// genuinely confusing failure: the run would do different work than the one
// it claims to repeat.
func WithRegenerate() Option {
	return func(c *runConfig) { c.regenerate = true }
}

// WithOnlySteps restricts the run to these steps. Everything else is skipped
// with a reason, exactly as an unmet When condition is.
//
// The set is taken literally: senro does not add dependents for you, because
// "and everything below it" and "only this" are both things a caller
// legitimately wants and only the caller knows which.
func WithOnlySteps(ids ...string) Option {
	return func(c *runConfig) {
		if c.only == nil {
			c.only = make(map[string]bool, len(ids))
		}
		for _, id := range ids {
			c.only[id] = true
		}
	}
}

// WithMaxNodes bounds how many nodes the whole run may hold, the plan's own
// included, and every splice is checked against it. Zero, the default, means
// five thousand.
//
// Run-wide rather than per-fragment: a hundred generators producing fifty
// nodes each is the same runaway as one producing five thousand, and only a
// run-wide count sees it.
func WithMaxNodes(n int) Option {
	return func(c *runConfig) { c.maxNodes = n }
}

// WithTraceContext continues an inbound W3C trace, making this run a child
// of whatever started it.
//
// Both arguments are header values exactly as W3C Trace Context defines them
// (https://www.w3.org/TR/trace-context/), which is also how they arrive
// everywhere else: as an HTTP header, as a CI job's environment variable, as
// a field in a webhook delivery. A caller that already holds a span in a
// context.Context renders one from it:
//
//	sc := trace.SpanContextFromContext(ctx)
//	senro.Run(ctx, pipeline, senro.WithTraceContext(
//		fmt.Sprintf("00-%s-%s-%02x", sc.TraceID(), sc.SpanID(), sc.TraceFlags()),
//		sc.TraceState().String(),
//	))
//
// tracestate may be empty, and usually is. senro never parses it: it is
// opaque vendor routing data that only has to reach downstream unchanged.
//
// Without this option, Run reads TRACEPARENT and TRACESTATE from its own
// environment (lowercase spellings too): a senro run is almost always a
// child of a CI job, webhook delivery or deploy tool, and every one of those
// exports the variables already. This option WINS over the environment when
// given, including when given empty strings: WithTraceContext("", "") is how
// an embedder says "this run is a root, ignore the ambient variables".
//
// A malformed value is ignored and the run starts a fresh trace: never
// propagated (a salvaged link to a nonexistent trace is indistinguishable
// from a real one whose other half was lost), and never a reason to refuse
// to run (an unset shell variable must not break a build).
//
// The trace context reaches every event as api.Event.TraceID; see
// api.RunStartedBody and api.StepStartedBody. senro emits no spans and
// depends on no OpenTelemetry package: an exporter is a Sink in the caller's
// own program; see WithSink and examples/otelexport.
//
// The trace also goes back out: every step's command is launched with
// TRACEPARENT set to that ATTEMPT's own span (and TRACESTATE beside it), on
// every executor, so a traced tool inside a step becomes a child of the step
// rather than the root of a disconnected trace. Handlers get their own span;
// a step declaring its own TRACEPARENT keeps it. It never enters a cache
// key: the key digests only the names a step declared in CacheEnv, from the
// step's declared environment.
func WithTraceContext(traceparent, tracestate string) Option {
	return func(c *runConfig) {
		c.traceParent, c.traceState, c.hasTrace = traceparent, tracestate, true
	}
}

// funcPkgEnv is the environment variable WithFuncBuild falls back to. It
// exists so `senro run ./ci` needs no option at all: that command knows the
// package it just built and sets this on the pipeline it execs. A CI job can
// set it too.
const funcPkgEnv = "SENRO_FUNC_PKG"

// envTraceContext reads an inbound trace context out of the process
// environment, in both spellings (uppercase first): CI systems export
// TRACEPARENT, but plenty of tooling exports the lowercase header name, and
// accepting one spelling only would silently lose the trace.
//
// The two are read as a PAIR, the tracestate from the same case as the
// traceparent that was found: pairing an uppercase parent with a lowercase
// state would attach one vendor's routing data to a trace it never saw.
func envTraceContext() (traceparent, tracestate string) {
	for _, names := range [][2]string{
		{"TRACEPARENT", "TRACESTATE"},
		{"traceparent", "tracestate"},
	} {
		if tp := os.Getenv(names[0]); tp != "" {
			return tp, os.Getenv(names[1])
		}
	}
	return "", ""
}

// WithFuncBuild names the package this program was built from, so senro can
// cross-compile it for a target that does not share the coordinator's
// platform. A func step runs a function compiled into THIS binary; when the
// target's platform matches, senro ships os.Executable() and needs nothing
// from you, but a Go program does not record the package it was compiled
// from, so a cross-build needs this. The common surprise is a func step in a
// CONTAINER: an image is linux, so a macOS coordinator cross-compiles for
// every one, however local the daemon is.
//
// The value is anything `go build` accepts, resolved as `go build` would:
//
//	senro.Run(ctx, pipeline(cfg), senro.WithFuncBuild("./ci"))
//
// A run with no remote func step is completely unaffected. A run that needs
// a cross-build and was not given this reads SENRO_FUNC_PKG (which `senro
// run` sets to the package it just built); with neither, the run fails at
// second zero naming both, rather than on the step that needed it.
//
// The cross-build is CGO_ENABLED=0 with -tags netgo,osusergo, so no
// cgo-dependent package may appear in the module's transitive closure;
// `senro func check` answers that in advance, and a failing build names the
// offending import path and the chain that pulled it in. The result is
// cached under the cache root, keyed by this binary's digest and the target
// platform, so a release compiles once per architecture.
//
// Explicit wins over the environment, for the reason WithTraceContext gives.
// Passing "" means "no package": a run that will refuse to cross-build
// rather than fall back to the environment.
func WithFuncBuild(pkg string) Option {
	return func(c *runConfig) { c.funcPkg, c.hasFuncPkg = pkg, true }
}

// WithSecrets hands Run the resolved configuration struct mamori.Load
// returned:
//
//	cfg, err := mamori.Load[Config](ctx, mamori.WithProvider(awssm.New()))
//	if err != nil { return err }
//	senro.Run(ctx, pipeline(cfg), senro.WithSecrets(cfg))
//
// Load, not Watch: a run lasts minutes and reads each value once, so senro
// takes a snapshot of the struct, and a value that rotates mid-run is a step
// that fails on use rather than a re-delivery path.
//
// What senro does with it, once:
//
//   - Every self-redacting field (mamori's secret.String and secret.Bytes,
//     or any type internal/secrets/reveal.go recognizes) has its value read
//     exactly once, in that one file. A plain string or int field with a
//     source tag is configuration, not a credential, and is left alone.
//   - Those values seed the run's redactor, which sits in front of the event
//     ledger and every log file.
//   - Their identities (field name, source URI with userinfo removed) are
//     emitted as one secret.resolved event each. A value is never in an
//     event.
//   - A step that declared SecretEnv receives its value as a file, with the
//     path in the environment. The value never enters a command argument, an
//     environment variable, a cache key, or plan.json.
//
// Run refuses to start if a resolved value is shorter than six bytes (it
// could not be redacted without redacting unrelated output), or if any step
// or handler would put a resolved value into a command argument, an
// environment variable, WorkDir, an Inputs/Outputs pattern, or a mount's
// names or path: the first two are visible in ps(1) and /proc/<pid>/environ,
// the rest are recorded verbatim in plan.json and the cache root, all beyond
// the redactor's reach. See the secrets section of the README.
//
// Passing anything that is not a struct or a pointer to one is an error Run
// returns rather than an empty set it silently proceeds with.
//
// hasSecrets, not a nil check on the stored value: an `any` holding a nil
// pointer of a concrete type is not itself == nil, so a nil check would not
// even catch the common form of the WithSecrets(nil) mistake.
func WithSecrets(cfg any) Option {
	return func(c *runConfig) { c.secrets, c.hasSecrets = cfg, true }
}

// RunError reports that a run reached a terminal state other than success
// (failed, partially failed, or cancelled), as opposed to an ENGINE failure
// (an invalid plan, a ledger write failure), which Run returns as a plain
// wrapped error, since a run that never really happened has no status to
// report.
//
// Run's signature stays a plain `error`; a caller that needs more than the
// one-line summary recovers this with errors.As(err, &runErr) and reads
// Status, Dir and Steps off it.
type RunError struct {
	// Status is the run's rolled-up outcome; see api.RollUp and
	// api.RunInfo.Status. Never api.RunSucceeded or
	// api.RunSucceededWithRecovery: Run returns nil for both of those.
	Status api.RunStatus

	// Dir is the run directory Run wrote to: events.jsonl and every
	// step's per-attempt logs live there. Empty only when a caller builds
	// a RunError itself rather than receiving one from Run.
	Dir string

	// Steps names the steps behind Status, in the order Run created
	// them, capped at a handful (see StepsOmitted). Which State qualifies
	// depends on Status: State.Failed() for RunFailed, StateCancelled for
	// RunCancelled, StateSkippedUpstreamFailed for RunPartial. Empty when
	// the fold has no step to blame; Error still names Dir rather than
	// inventing a cause.
	Steps []RunErrorStep

	// StepsOmitted is how many further qualifying steps exist beyond
	// Steps, so Error can say "and N more" instead of naming dozens.
	StepsOmitted int
}

// Error renders one line: the run's status, which step(s) are behind it
// (named, not dumped: a step's error text or command line can carry values
// that must not be repeated here), and where to look next. For example:
//
//	senro: run failed: step "test" failed (exit 1); see runs/20260810T073610-64c2f4b40c/events.jsonl
func (e *RunError) Error() string {
	var b strings.Builder
	b.WriteString("senro: run ")
	b.WriteString(string(e.Status))
	if len(e.Steps) > 0 {
		b.WriteString(": ")
		b.WriteString(e.stepsClause())
	}
	if e.Dir != "" {
		b.WriteString("; see ")
		b.WriteString(filepath.Join(e.Dir, "events.jsonl"))
	}
	return b.String()
}

// Run builds pipe and executes the result to completion, with no Build()
// visible at the call site:
// `senro.Run(ctx, pipeline(cfg), senro.WithSecrets(cfg), ...)`.
//
// Build runs first, before anything here touches disk, and its error is
// returned unwrapped and never as a *RunError: a pipeline that failed to
// build produced no run, so there is no status to report; see
// TestRunReturnsTheBuildErrorDirectlyForAnInvalidPipeline.
//
// A caller that already holds a *Plan calls RunPlan instead. Building the
// same *Pipeline twice is not guaranteed to reproduce an already-inspected
// plan: a *StepBuilder can still be mutated after Build returns, and a
// second Build picks up whatever was added since. RunPlan takes the resolved
// Plan itself, so nothing added afterward can reach the run it executes.
//
// With no options, Run costs exactly what the engine costs: no attach
// server, no extra goroutines (TestRunWithNoOptionsStartsNoGoroutines proves
// it by counting). A run directory and ID are still generated under
// runs/<id> via attach.NewRunID, so an option-less Run still produces a
// real, inspectable run on disk.
//
// The DEFAULT executor is local (internal/executor/localexec). A workflow
// targeted with On runs on the executor buildExecutors constructs, one per
// distinct target the plan names; resolving it here from the plan keeps
// Option additive rather than making every caller name an executor.
func Run(ctx context.Context, pipe *Pipeline, opts ...Option) error {
	if handled, err := StepChild(ctx); handled {
		return err
	}
	p, err := pipe.Build()
	if err != nil {
		return err
	}
	return runPlan(ctx, p, pipe.Name(), pipe.generators(), opts...)
}

// StepChild runs this process as a remote step child, if that is what a
// coordinator launched it as, and reports whether it did.
//
// Run and RunPlan call it first, so an ordinary pipeline needs nothing:
//
//	func main() {
//		if err := senro.Run(context.Background(), pipeline()); err != nil {
//			log.Fatal(err)
//		}
//	}
//
// really does run a func step on an ssh host, with no line about re-entry
// anywhere in it.
//
// Call it yourself when main does something before Run and might not get
// there: a main that parses flags with a package that exits on an
// unrecognised one never reaches Run when launched as `<binary> __step
// --state-fd 0`. Calling this first is the fix:
//
//	func main() {
//		if handled, err := senro.StepChild(context.Background()); handled {
//			if err != nil { log.Fatal(err) }
//			return
//		}
//		flag.Parse()
//		...
//	}
//
// handled is false, instantly and with no side effect, for every ordinary
// invocation: it is decided by os.Args[1] alone. Deliberately no environment
// variable: a marker in the environment is inherited by every child process
// a step launches, and a pipeline that ran itself would re-enter as a step
// child of a step.
//
// The error it returns is the CHILD's failure, never the step's: a
// function's error, panic or timeout is a verdict that travels back in the
// protocol. An error from here means the protocol did not happen at all,
// which is why it is worth exiting non-zero on.
//
// It never calls os.Exit on the ordinary path. It does end the process on
// the step's own deadline, because a registered function that never selects
// on its context cannot be made to return by anything less. See
// internal/stepchild.
func StepChild(ctx context.Context) (handled bool, err error) {
	argv := os.Args[1:]
	if !stepchild.Invoked(argv) {
		return false, nil
	}
	return true, stepchild.Run(ctx, argv, os.Stdin, os.Stdout, os.Stderr)
}

// RunPlan executes an already-built plan directly, without calling Build:
// the EXACT plan a caller already validated or inspected, not a plan
// re-resolved from whatever a *Pipeline looks like now. See Run.
//
// The run's first event names no pipeline: a plan carries no name, and
// run.started asserting one this call was never given would be a guess. A
// caller who wants the ledger to carry a name has Run.
func RunPlan(ctx context.Context, p *Plan, opts ...Option) error {
	if handled, err := StepChild(ctx); handled {
		return err
	}
	return runPlan(ctx, p, "", &generatorRegistry{}, opts...)
}

// runPlan is Run and RunPlan's shared body. pipeline is the name run.started
// publishes, which only Run has: it is the one entry point holding the
// *Pipeline the name belongs to.
func runPlan(ctx context.Context, p *Plan, pipeline string, gens *generatorRegistry, opts ...Option) error {
	var cfg runConfig
	for _, opt := range opts {
		opt(&cfg)
	}

	// Decided before anything touches disk, starts a goroutine or opens the
	// cache: a no-match has to be genuinely inert, with no partial state for
	// something to misread later as a run that died. Only Build, which is
	// pure, runs ahead of this. The error is wrapped so it reads like every
	// other error here; errors.Is(err, trigger.ErrNoMatch) still recovers it.
	match, err := trigger.Select(cfg.triggerEvent, cfg.triggers...)
	if err != nil {
		return fmt.Errorf("senro: %w", err)
	}

	dir, runID := cfg.dir, cfg.runID

	// One list, built in one place, so "who observes this run" has a single
	// answer. The attach hub goes in as itself, not behind a sink.Multi
	// queue: it has its own ring buffers and overflow accounting
	// (api.StreamEndOverflowed), and a queue in front would drop events
	// before that accounting saw them.
	var observers []sink.Sink
	if cfg.attach != nil {
		observers = append(observers, cfg.attach.Sink())
		if dir == "" {
			dir = cfg.attach.Dir()
		}
		if runID == "" {
			runID = cfg.attach.RunID()
		}
	}
	for _, s := range cfg.sinks {
		observers = append(observers, externalSink{s: s})
	}

	snk := teeSink(observers).collapse()
	if runID == "" {
		runID = attach.NewRunID()
	}
	if dir == "" {
		dir = filepath.Join("runs", runID)
	}

	cacheDir := cfg.cacheDir
	if cacheDir == "" {
		cacheDir, err = storage.DefaultRoot()
		if err != nil {
			return fmt.Errorf("senro: %w", err)
		}
	}
	// No WithRemoteCache reads the environment, exactly as no WithCacheDir
	// reads SENRO_CACHE_DIR: a CI job sets variables, it does not edit Go
	// source. An explicit WithRemoteCache wins.
	remoteCache := cfg.remoteCache
	if !remoteCache.configured() {
		remoteCache, _, err = RemoteCacheFromEnv()
		if err != nil {
			return err
		}
	}
	// Opened BEFORE the storage root, so a configuration that cannot work
	// refuses the run before a directory tree is created for it. See
	// WithRemoteCache on why this class of problem fails rather than degrades.
	remote, err := remoteCache.open()
	if err != nil {
		return err
	}
	store, err := storage.Open(cacheDir,
		storage.WithRemote(remote), storage.WithScratchNamespace(pipeline))
	if err != nil {
		return fmt.Errorf("senro: %w", err)
	}
	defer func() { _ = store.Close() }()

	// secretSet stays nil for a run with no WithSecrets, which is the free
	// path every call site downstream (engine.Options.Secrets, runCore.redact,
	// runCore.secrets) treats as empty rather than branching on.
	var secretSet *secrets.Set
	if cfg.hasSecrets {
		secretSet, err = secrets.FromConfig(cfg.secrets)
		if err != nil {
			return fmt.Errorf("senro: %w", err)
		}
	}

	// folded records every event the engine emits, so a non-success status
	// can be reported with which step and where to look (see newRunError).
	folded := newFoldingSink(snk)

	local := localexec.New(dir, store.Snapshotter, localexec.WithClass(cfg.localClass))
	execs, err := buildExecutors(p, dir, runID, store, secretSet)
	if err != nil {
		return fmt.Errorf("senro: %w", err)
	}
	defer closeExecutors(execs)

	// Flush every sink that asked to be flushed, on every path out of here. A
	// defer, because the run.finished notification is sent at the moment
	// everything is shutting down and is therefore the likeliest to be
	// abandoned in flight.
	//
	// context.WithoutCancel: the notification that matters most often belongs
	// to a run whose context was just cancelled (Ctrl-C, a CI timeout). A
	// Flusher bounds its own wait, so this cannot hang; see senro.Flusher.
	//
	// The error is dropped, deliberately, the only place in this package that
	// drops one: a run that did what it was asked did not fail because a
	// webhook was down. A Flusher reports its own trouble on a channel of its
	// own choosing; see senro.Flusher.
	defer func() {
		for _, s := range cfg.sinks {
			if f, ok := s.(Flusher); ok {
				_ = f.Flush(context.WithoutCancel(ctx))
			}
		}
	}()

	// Written before the run's first event, so a watcher can read what
	// triggered the run while it is still going, and a run the engine then
	// refuses still says what was attempted. See RunManifest.
	if err := writeManifest(dir, RunManifest{
		RunID:     runID,
		Pipeline:  pipeline,
		StartedAt: time.Now().UTC(),
		Trigger:   newTriggerRecord(match),
	}); err != nil {
		return err
	}

	traceParent, traceState := cfg.traceParent, cfg.traceState
	if !cfg.hasTrace {
		traceParent, traceState = envTraceContext()
	}

	// hasFuncPkg rather than a non-empty check, for the same reason hasTrace
	// is: WithFuncBuild("") really does mean "no package", not "fall through
	// to whatever this process was launched with". See WithFuncBuild.
	funcPkg := cfg.funcPkg
	if !cfg.hasFuncPkg {
		funcPkg = os.Getenv(funcPkgEnv)
	}

	status, err := engine.Run(ctx, p, engine.Options{
		Dir:       dir,
		Executor:  local,
		Executors: execs,
		Sink:      folded,
		RunID:     runID,
		Pipeline:  pipeline,
		Storage:   store,
		Secrets:   secretSet,
		Params:    runParams(match, cfg.params),

		// Go generators, carried from the pipeline because a closure cannot
		// be in a plan. Nil for a run started from a plan on disk.
		Generators: gens.lookup,

		// Zero means the engine's own defaults; see DefaultMaxDepth and
		// DefaultMaxNodes.
		MaxDepth: cfg.maxDepth,
		MaxNodes: cfg.maxNodes,

		Regenerate: cfg.regenerate,
		Only:       cfg.only,

		// Not validated here: engine.Options.TraceParent takes the raw
		// header, so exactly one place in senro decides what a valid
		// inbound context is.
		TraceParent: traceParent,
		TraceState:  traceState,

		// Built for every run, and costs a struct: it compiles nothing until
		// a plan has a func step on a foreign platform. Its cache lives under
		// the cache root, not the run directory, because a cross-built binary
		// is shared by every run on the machine.
		Binaries: binprov.New(binprov.Options{
			Dir: filepath.Join(cacheDir, "binaries"),
			Pkg: funcPkg,
		}),

		// Nil for a run with no WithAnalyzer: no goroutine, no queue, one
		// nil check when a step settles failed. See senro.WithAnalyzer.
		Analyze: cfg.analyze,
	})
	if err != nil {
		return fmt.Errorf("senro: %w", err)
	}

	switch status {
	case api.RunSucceeded, api.RunSucceededWithRecovery:
		return nil
	default:
		return newRunError(status, dir, folded.state)
	}
}

// buildExecutors constructs one executor per distinct non-local target the
// plan names: one instance per plan.ExecutorSpec.Key(), so two workflows on
// the same image share a resolved image digest and a single pull.
//
// This is the one place in senro that knows which executor packages exist:
// Option stays additive, and the engine never learns that Docker exists.
// The construction switch lives in newExecutorFor so the map bookkeeping
// here stays independent of how many executor kinds exist (and to avoid an
// ineffassign false positive on a one-case switch).
//
// runID labels every container this run's executors create (see
// containerexec.WithRunID), so an orphaned container left by a killed
// coordinator can still be attributed to the run that started it.
//
// secrets is the run's resolved set, needed because one target declares a
// credential of its own: a container image in a private registry. It is nil
// for a run with no WithSecrets, which is the empty set.
func buildExecutors(
	p *Plan, dir, runID string, store *storage.Storage, secrets *secrets.Set,
) (map[string]executor.Executor, error) {
	var out map[string]executor.Executor
	for i := range p.Nodes {
		spec := p.Nodes[i].Executor
		if spec == nil || spec.Kind == plan.ExecutorLocal {
			continue
		}
		key := spec.Key()
		if _, done := out[key]; done {
			continue
		}
		ex, err := newExecutorFor(*spec, p.Nodes[i].ID, runID, store, secrets)
		if err != nil {
			return nil, err
		}
		if out == nil {
			out = make(map[string]executor.Executor)
		}
		out[key] = ex
	}
	return out, nil
}

// closeExecutors gives back what an executor holds for the whole run, on
// every path out including a failed or cancelled one: the ssh executor's
// control master is an authenticated session per host, and the run is the
// only thing that knows it is over.
//
// After engine.Run has returned, so nothing is taken away under a running
// step. The error is dropped, as the Flusher's above is and for the same
// class of reason: the run's verdict is already decided, and every executor
// that holds anything here also arms its own reaper for the coordinator that
// never got this far (see sshexec.DefaultMasterIdleTTL).
func closeExecutors(execs map[string]executor.Executor) {
	for _, ex := range execs {
		if c, ok := ex.(io.Closer); ok {
			_ = c.Close()
		}
	}
}

// newExecutorFor constructs the executor a single plan.ExecutorSpec names.
// stepID is only for the error message: it names the first step that
// referenced this spec, which is enough for a caller to find it in the plan.
func newExecutorFor(
	spec plan.ExecutorSpec, stepID, runID string, store *storage.Storage, secrets *secrets.Set,
) (executor.Executor, error) {
	switch spec.Kind {
	case plan.ExecutorContainer:
		opts := []containerexec.Option{containerexec.WithRunID(runID)}
		if ra := spec.RegistryAuth; ra != nil {
			// Resolved here, where the run's secrets live, and handed over as
			// bytes: an executor is given values, never the set to look them
			// up in, the same division Sandbox.PutSecret keeps.
			password, ok := secrets.Value(ra.Secret)
			if !ok {
				return nil, registryAuthRefusal(stepID, spec.Image, ra.Secret, secrets)
			}
			opts = append(opts, containerexec.WithRegistryAuth(ra.Username, password))
		}
		ex, err := containerexec.New(spec, store.Snapshotter, opts...)
		if err != nil {
			return nil, fmt.Errorf("step %q: %w", stepID, err)
		}
		return ex, nil
	case plan.ExecutorK8s:
		// The snapshotter lets this executor carry a workspace in and out of
		// a pod: a tar over the apiserver's exec subresource, in both
		// directions, with the digest computed from the copy that came back.
		// See k8sexec/transfer.go.
		ex, err := k8sexec.New(spec, store.Snapshotter, k8sexec.WithRunID(runID))
		if err != nil {
			return nil, fmt.Errorf("step %q: %w", stepID, err)
		}
		return ex, nil
	case plan.ExecutorSSH:
		// The same snapshotter, for the same reason: the executor carries a
		// workspace in both directions and computes its digest through the
		// same code the local and container executors use. See sshexec's doc.
		ex, err := sshexec.New(spec, store.Snapshotter, sshexec.WithRunID(runID))
		if err != nil {
			return nil, fmt.Errorf("step %q: %w", stepID, err)
		}
		return ex, nil
	default:
		return nil, fmt.Errorf(
			"step %q runs on the %q executor, which this build cannot construct",
			stepID, spec.Kind)
	}
}

// registryAuthRefusal names a container.RegistryAuth that points at a
// configuration field the run has no value for, at second zero rather than
// on the first step that needed the image.
//
// The same shape engine.checkSecretRefs uses for a step's own secrets,
// including listing what DID resolve, because the mistake is nearly always a
// typo or a password typed where a field name belongs: a literal credential
// never matches a field name, so it lands here rather than in plan.json.
func registryAuthRefusal(stepID, image, field string, set *secrets.Set) error {
	available := "none were resolved"
	if names := set.Names(); len(names) > 0 {
		available = "resolved: " + strings.Join(names, ", ")
	}
	return fmt.Errorf(
		"step %q pulls image %q with a registry credential from configuration field %q, which the "+
			"struct passed to senro.WithSecrets does not provide (%s). container.RegistryAuth takes "+
			"the registry account name and the NAME of a mamori-resolved field, never a password",
		stepID, image, field, available)
}
