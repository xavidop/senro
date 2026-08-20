// Package senro defines pipelines in Go and executes them locally, in
// containers, as Kubernetes pods, or on a remote host over SSH (senro.Local,
// container.Image, k8s.Pod, ssh.Host; anything else is refused by Build). It
// is a pipeline engine first: CI/CD is the most familiar thing to build on
// it, but data pipelines, batch jobs and release automation are equally in
// scope.
//
// A pipeline is built as an immutable DAG, resolved into a plan, and executed
// by the engine; user code never drives execution. Every observable fact
// about a run is an event in an append-only stream, which realtime UI,
// attach, replay, re-run and audit all read. The wire contract lives in
// github.com/xavidop/senro/api, a package of this module that depends only on
// the standard library (enforced by api/nodeps_test.go).
//
// # Terminology
//
// The name is 線路 (senro), railway track. The metaphor carries through the
// documentation and error messages: steps are stations, a workflow is a line,
// a resolved plan is a timetable. The API itself says Pipeline, Workflow,
// Step and Plan.
//
// # The three levels
//
// A pipeline holds workflows; a workflow holds steps:
//
//	p := senro.New("monorepo")
//	setup := p.Workflow("setup")
//	setup.Step("install", exec.Command("pnpm", "install"))
//	verify := p.Workflow("verify", senro.Needs("setup"))
//	verify.Step("test", exec.Command("pnpm", "test"))
//
// A workflow carries Needs and On: groups of steps depend on other groups,
// and a group is targeted at one executor. Steps carry their own,
// finer-grained Needs within a workflow.
package senro

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/xavidop/senro/artifact"
	"github.com/xavidop/senro/change"
	"github.com/xavidop/senro/internal/cond"
	"github.com/xavidop/senro/internal/funcs"
	"github.com/xavidop/senro/internal/plan"
	"github.com/xavidop/senro/internal/stepid"
	"github.com/xavidop/senro/internal/unit"
	"github.com/xavidop/senro/retry"
)

// Action is what a step does.
type Action interface {
	ActionKind() string
	ActionCmd() []string
}

// Ctx is what a registered function receives: a context.Context that also
// knows the run, the step, its mounted workspaces and its delivered secrets.
type Ctx = funcs.Ctx

// WorkspacePath is a mounted workspace's path, as Ctx.Workspace reports it.
type WorkspacePath = funcs.WorkspacePath

// RegisterFunc registers a Go function as a step kind, under a stable name:
//
//	type DeployParams struct {
//		App       string `json:"app"`
//		Namespace string `json:"namespace"`
//	}
//
//	func init() { senro.RegisterFunc("deploy/helm", HelmUpgrade) }
//
//	func HelmUpgrade(ctx senro.Ctx, p DeployParams) error {
//		kubeconfig := ctx.Secret("kubeconfig")
//		chart, _ := ctx.Workspace("charts")
//		return helm.Upgrade(ctx, p.App, chart.Path("apps", p.App), kubeconfig)
//	}
//
// The name is API: it is the step's cache key, its name in plan.json, and its
// address for `senro rerun --step`, which a closure's identity could be none
// of. Changing it invalidates the cache for every step that used it and
// breaks any recorded plan that names it, exactly as renaming a command
// would. Registering the same name twice panics.
//
// P must be JSON-serializable and is decoded strictly: a recorded parameter
// field that P does not have is an error, not a silent zero value.
//
// A func step runs on every executor: the coordinator, an ssh host, a
// container and a pod. Its body is compiled into this binary and no plan can
// describe it, so running it elsewhere means putting THIS BINARY over there
// and re-entering it as a step child: senro ships its own executable when the
// target's platform matches the coordinator's, and cross-compiles
// (CGO_ENABLED=0, -tags netgo,osusergo) when it does not. Over ssh the binary
// is transferred once per host; in a container it is bind-mounted read-only;
// in a pod it is sent over the apiserver's exec subresource once per pod, and
// the image must carry sh and tar for it. See WithFuncBuild and `senro func
// check`.
func RegisterFunc[P any](name string, fn func(Ctx, P) error) {
	funcs.Register(name, func(ctx Ctx, raw json.RawMessage) error {
		var p P
		if len(raw) > 0 {
			dec := json.NewDecoder(bytes.NewReader(raw))
			dec.DisallowUnknownFields()
			if err := dec.Decode(&p); err != nil {
				return fmt.Errorf("senro: func %q: decoding parameters into %T: %w", name, p, err)
			}
		}
		return fn(ctx, p)
	})
}

// funcAction is what Func produces. It is an Action like exec.Command's, so a
// func step is built, validated, scheduled, retried, cached and handled by
// exactly the same code an exec step is.
type funcAction struct {
	name   string
	params json.RawMessage
	err    error
}

func (f funcAction) ActionKind() string  { return "func" }
func (f funcAction) ActionCmd() []string { return nil }

// Func makes a step out of a registered function and its parameters:
//
//	deploy.Step("apply", senro.Func("deploy/helm", DeployParams{App: "web"}))
//
// The parameters are canonicalised here, at Build time, so an unserializable
// value is an error where it was written rather than a failure on the
// twentieth step of a run.
func Func(name string, params any) Action {
	canon, err := plan.CanonicalParams(params)
	return funcAction{name: name, params: canon, err: err}
}

// Pipeline accumulates workflows: the top of the three-level hierarchy
// (pipeline, workflow, step). Build snapshots it into the *Plan the engine
// executes.
type Pipeline struct {
	name      string
	workflows []*WorkflowBuilder
}

// New starts a new pipeline.
func New(name string) *Pipeline { return &Pipeline{name: name} }

// Name reports the pipeline's name.
func (p *Pipeline) Name() string { return p.name }

// Workflow adds a named group of steps to the pipeline: the unit that
// carries cross-group dependencies (Needs) and executor targeting (On). Two
// workflows may not share a name, and no step id may repeat anywhere in the
// pipeline: Build refuses both, naming the workflows involved.
func (p *Pipeline) Workflow(name string, opts ...WorkflowOption) *WorkflowBuilder {
	var cfg workflowConfig
	for _, o := range opts {
		o(&cfg)
	}
	w := &WorkflowBuilder{
		name:  name,
		needs: append([]string(nil), cfg.needs...),
		on:    cfg.on,
		when:  append([]Condition(nil), cfg.when...),
	}
	p.workflows = append(p.workflows, w)
	return w
}

// WorkflowBuilder accumulates the steps of one workflow.
type WorkflowBuilder struct {
	name       string
	needs      []string
	on         ExecutorTarget
	when       []Condition
	steps      []*StepBuilder
	expansions []*ExpandBuilder
}

// Name reports the workflow's name.
func (w *WorkflowBuilder) Name() string { return w.name }

// Step adds a station to the workflow.
func (w *WorkflowBuilder) Step(id string, a Action) *StepBuilder {
	sb := &StepBuilder{id: id, action: a}
	w.steps = append(w.steps, sb)
	return sb
}

// Unit is what an expansion's Template is called with: one thing a unit
// graph discovered, such as a directory glob.Dirs matched. An alias, not a
// copy, so a caller passing one to a helper has nothing to convert.
type Unit = unit.Unit

// Shard is what a PARTITIONED expansion's TemplateShard is called with: one
// bucket of units, and where it sits among its siblings. An alias for the
// same reason Unit is.
type Shard = unit.Shard

// DurationHistory reports how long each unit's step took in previous runs,
// which is what Partition balances its buckets by.
//
// The shipped implementation is github.com/xavidop/senro/duration
// (FromFile, Record, and None for "no history"). The history file belongs IN
// THE REPOSITORY: a partition derived from timing is a plan that depends on
// the timing, so a per-machine history would give two machines on one commit
// two different plans, digests and cache keys.
//
// WRITE YOUR OWN, the same way UnitGraph invites one; nothing needs
// registering. Two obligations: report the SAME durations on every machine
// building one commit, or the plan moves; and report an empty map with no
// error when nothing has been recorded, because that is the first run of
// every pipeline that uses it.
type DurationHistory = unit.DurationHistory

// UnitGraph discovers the units an expansion fans out over.
//
// Eight implementations ship, under github.com/xavidop/senro/unit: glob
// matches paths; gowork asks the Go toolchain; cargo, jswork, maven and
// gradle read the manifests of a Cargo, npm/pnpm/Yarn, Maven or Gradle
// workspace; pyproject discovers Python distributions; bazel discovers Bazel
// packages. All but glob, pyproject and bazel also implement UnitAffector.
//
// WRITE YOUR OWN: a graph in your own module satisfies this and UnitAffector
// by having the methods, with nothing to register and nothing under
// internal/ to import. See https://senro.dev/docs/unit-graphs/ and
// examples/customgraph.
//
// Units must be DETERMINISTICALLY ORDERED. Child step ids derive from the
// unit set in this order, so an order that varies between builds varies the
// plan and the digest every cache entry hangs off. Sort before returning;
// map iteration is the usual way this goes wrong.
type UnitGraph = unit.Graph

// UnitAffector is a UnitGraph that can also say which unit owns a changed
// file and which units break when a unit changes, which is what
// ExpandBuilder.Affected needs to narrow a fan-out to what a change reaches.
//
// A SEPARATE interface, because growing the published UnitGraph would break
// every implementation outside this repository. A graph without it gets
// ErrNoAffectedSet from Affected at build time rather than quietly covering
// everything.
//
// Implement it only when you can answer HONESTLY: a wrong affected set skips
// the unit a change broke and reports a green build for a tree that does not
// build. Where an answer is unclear the answer is "affected". unit/glob,
// unit/pyproject and unit/bazel deliberately do not implement this.
type UnitAffector = unit.Affector

// ErrNoAffectedSet reports that ExpandBuilder.Affected was used over a graph
// that does not implement UnitAffector. Build wraps it, so errors.Is can
// tell "this graph cannot narrow" apart from "narrowing failed".
var ErrNoAffectedSet = unit.ErrNoAffectedSet

// ChangeSource answers what a run is asked to build, for
// ExpandBuilder.Affected. See package change
// (github.com/xavidop/senro/change), whose FromTrigger reads the mode and the
// base that the event which started this run already recorded.
type ChangeSource = change.Source

type workflowConfig struct {
	needs []string
	on    ExecutorTarget
	when  []Condition
}

// WorkflowOption configures a workflow. See Needs, On and When.
type WorkflowOption func(*workflowConfig)

// Condition gates a node on something known at run start. See When.
type Condition = cond.Condition

// Branch runs a node only on a named branch, read from the run's "branch"
// parameter (see WithParams).
func Branch(name string) Condition { return cond.Branch(name) }

// ParamIs runs a node only when a run parameter has a given value.
func ParamIs(name, value string) Condition { return cond.ParamIs(name, value) }

// EnvIs runs a node only when a coordinator environment variable has a given
// value.
func EnvIs(name, value string) Condition { return cond.EnvIs(name, value) }

// When gates every step of a workflow on a condition:
//
//	deploy := p.Workflow("deploy",
//		senro.Needs("build"),
//		senro.On(deployer),
//		senro.When(senro.Branch("main")))
//
// A step whose conditions are not all true is SKIPPED, not failed: it settles
// as skipped_condition, its dependents settle the same way, and the run's
// status is unaffected, so a main-only deploy workflow leaves a pull request
// run green rather than partial.
//
// Two When calls, or a workflow-level When plus a step-level one, are ANDed.
func When(c Condition) WorkflowOption {
	return func(cfg *workflowConfig) { cfg.when = append(cfg.when, c) }
}

// Needs declares WORKFLOWS this workflow waits for: a barrier, the only form
// of workflow dependency there is. Every step of this workflow starts only
// once every step of each named workflow has settled.
//
// It names WORKFLOWS; the step-level (*StepBuilder).Needs names STEPS. A
// name matching no workflow declared on the same pipeline is refused by
// Build, naming both sides, rather than being read as a step id.
//
// Build lowers the barrier onto step edges: each of this workflow's entry
// steps gains a dependency on each named workflow's exit steps. A workflow
// with no steps satisfies the barrier immediately.
func Needs(names ...string) WorkflowOption {
	return func(c *workflowConfig) { c.needs = append(c.needs, names...) }
}

// ExecutorSpec is where a workflow's steps run, as declared: the type an
// ExecutorTarget hands to Build. An alias for the wire-identical internal
// type, not a copy, so a caller can name it with nothing to convert.
type ExecutorSpec = plan.ExecutorSpec

// ExecutorTarget is where a workflow's steps run: a value produced by an
// executor package, which On carries into the pipeline.
//
// One method returning a struct, rather than one method per property: a new
// executor family adds a FIELD to ExecutorSpec, which is additive for every
// existing implementation, instead of a METHOD to this interface, which is
// not.
//
// This build ships four implementations: Local, container.Image, k8s.Pod and
// ssh.Host (under github.com/xavidop/senro/executor/...). Build refuses any
// other kind rather than ignoring it.
type ExecutorTarget interface {
	ExecutorSpec() ExecutorSpec
}

type localTarget struct{}

func (localTarget) ExecutorSpec() ExecutorSpec { return ExecutorSpec{Kind: plan.ExecutorLocal} }

// Local is the coordinator's own machine (internal/executor/localexec), and
// the executor every workflow gets by saying nothing.
func Local() ExecutorTarget { return localTarget{} }

// On targets a workflow at an executor: Local, container.Image, k8s.Pod or
// ssh.Host. Build refuses a target this build cannot honour rather than
// silently running the steps on the coordinator instead.
//
// A target other than Local is recorded as plan.Node.Executor on every step
// of the workflow: the executor decides where a step runs and its cache
// equivalence class, so a plan that did not record it could not be re-run
// faithfully. A workflow targeted at Local, or at nothing, records nothing,
// so every plan built before executors existed keeps its exact digest.
func On(target ExecutorTarget) WorkflowOption {
	return func(c *workflowConfig) { c.on = target }
}

// Handler builds a node for use as a step's OnFailure or Always handler. It
// is a *StepBuilder with the same Env, WorkDir and Timeout methods, but it
// is deliberately not appended to any workflow: passing its result to
// OnFailure or Always is what makes it reachable, and a handler that also
// became a step would run twice. OnFailure and Always refuse a *StepBuilder
// returned by Step instead of Handler; see StepBuilder.handler.
//
// The step methods a handler has no meaning for are refused by Build rather
// than accepted and dropped: Needs and When (it runs because its parent
// settled), its own Executor or mounts (it inherits its parent's), cache
// settings, handlers of its own, and Retry (the engine runs a handler
// exactly once, with no attempt loop; retry the step instead, which retries
// before any handler runs).
//
// Inheritance is literal: the handler's sandbox is given the parent's
// workspace mounts at the same paths, and it starts in the parent's WorkDir
// unless it sets one of its own. A step that wrote build.log into a
// workspace mounted at /repo has a handler that can `cat build.log`, on the
// same executor the step ran on.
//
// The inherited view is READ-ONLY: the step's workspace snapshot is taken
// before any handler starts, so a handler write would move bytes the run's
// event log already describes. As with a step's own senro.RO mount, only the
// container executor enforces this. A handler that needs to write has its
// own sandbox working directory.
//
// Not inherited: anything the step wrote outside a declared workspace (the
// executor's private sandbox, which a container removes before the handler
// starts), and scratch caches.
func Handler(id string, a Action) *StepBuilder {
	return &StepBuilder{id: id, action: a, handler: true}
}

// NewStep builds a step that is not attached to any workflow, which is what
// an expansion's Template returns:
//
//	verify.Expand("lint", glob.Dirs("apps/*")).
//		Template(func(u senro.Unit) *senro.StepBuilder {
//			return senro.NewStep(exec.Command("pnpm", "--filter", u.Name, "lint")).
//				Pure().Inputs(u.Sources()...)
//		})
//
// It has no id: the expansion assigns one from the unit
// ("lint[unit=apps/web]"), because a template-chosen id could not be
// guaranteed unique across units. It is not a handler either, so OnFailure
// and Always refuse it exactly as they refuse a workflow's step.
func NewStep(a Action) *StepBuilder { return &StepBuilder{action: a} }

// ExpandBuilder configures one expansion: one template, one unit graph, and
// one node per unit, all resolved when Build runs.
//
// Expansion happens at PLAN time, not mid-run: definition, plan and
// execution stay distinct phases, child ids are deterministic, a re-run
// reconstitutes exactly the same children because they are IN the plan, and
// the UI knows the whole node set before anything starts. What it gives up
// is expanding over a list only a running step could produce; that is not
// supported yet.
type ExpandBuilder struct {
	id        string
	graph     UnitGraph
	tmpl      func(Unit) *StepBuilder
	shardTmpl func(Shard) *StepBuilder
	buckets   int
	history   DurationHistory
	changes   ChangeSource
	parallel  int
	maxNodes  int
	needs     []string
	needsEach []string
	when      []Condition
	errs      []error
}

// Expand adds one step per unit the graph discovers.
func (w *WorkflowBuilder) Expand(id string, g UnitGraph) *ExpandBuilder {
	e := &ExpandBuilder{id: id, graph: g, maxNodes: plan.DefaultMaxNodes}
	w.expansions = append(w.expansions, e)
	return e
}

// Template builds the step for one unit. It is called once per unit, in unit
// order, and must return a fresh builder each time: two units sharing one
// builder would produce one node, with whichever unit's command was applied
// last.
func (e *ExpandBuilder) Template(fn func(Unit) *StepBuilder) *ExpandBuilder {
	if e.tmpl != nil {
		e.errs = append(e.errs, fmt.Errorf("senro: expansion %q has two templates", e.id))
	}
	e.tmpl = fn
	return e
}

// Partition groups the units into at most n buckets and makes ONE STEP PER
// BUCKET rather than one per unit, balancing the buckets by how long each
// unit's step took in previous runs.
//
//	verify.Expand("test", gowork.Modules()).
//		Partition(8, duration.FromFile(".senro/durations.json")).
//		TemplateShard(func(sh senro.Shard) *senro.StepBuilder {
//			return senro.NewStep(exec.Command(append([]string{"go", "test"}, sh.Dirs()...)...))
//		})
//
// It takes TemplateShard rather than Template: a bucket is several units, and
// the per-unit template has nowhere to put the others. Declaring one without
// the other, or both templates at once, is refused at Build.
//
// Balancing by duration exists because an alphabetical or round-robin split
// puts the slowest units together often enough that the fan-out takes as
// long as that one shard. No history (the first run) is not an error: every
// unit weighs the same and the fill degenerates to a round robin over the
// sorted unit set. A unit missing from a non-empty history gets the median.
// See internal/unit.Partition.
//
// A child is "test[shard=0]", numbered, never named after its contents, and
// the NUMBER of shards is min(n, number of units): the id set is a function
// of the repository alone, so two machines holding two different histories
// build the same step ids, and the cache keys hanging off them stay put.
// What the history does move is which unit is in which bucket, and so each
// shard's command, inputs, cache key and the plan digest. That is correct: a
// step that runs three modules is not the step that ran two of them. It is
// also why the history has to be a committed file; see DurationHistory.
//
// A shard's time cannot be attributed to the units that spent it, so
// duration.Record ignores shard steps; record from a run of the same
// expansion UNPARTITIONED when the numbers go stale.
//
// MaxNodes is still checked against the whole graph, before any of this:
// partitioning is not a way around the guard, and a glob matching forty
// thousand directories is a mistake whether or not it would have collapsed
// into eight steps.
func (e *ExpandBuilder) Partition(n int, h DurationHistory) *ExpandBuilder {
	if e.buckets != 0 {
		e.errs = append(e.errs, fmt.Errorf("senro: expansion %q is partitioned twice", e.id))
		return e
	}
	if n <= 0 {
		e.errs = append(e.errs, fmt.Errorf(
			"senro: expansion %q has Partition(%d), which is not a number of steps", e.id, n))
		return e
	}
	if h == nil {
		e.errs = append(e.errs, fmt.Errorf(
			"senro: expansion %q was given a nil duration history; use duration.None() to say "+
				"there is no history to balance by", e.id))
		return e
	}
	e.buckets = n
	e.history = h
	return e
}

// TemplateShard builds the step for one bucket of a partitioned expansion.
// It is called once per shard, in shard order, and must return a fresh
// builder each time, for the same reason Template must. It replaces Template
// rather than joining it: Build refuses an expansion declaring both, one
// partitioned with only a per-unit Template, and one declaring this with no
// Partition.
func (e *ExpandBuilder) TemplateShard(fn func(Shard) *StepBuilder) *ExpandBuilder {
	if e.shardTmpl != nil {
		e.errs = append(e.errs, fmt.Errorf("senro: expansion %q has two shard templates", e.id))
	}
	e.shardTmpl = fn
	return e
}

// MaxParallel bounds how many of this expansion's children run at once, on
// top of the run's own global limit. Unset, only the global limit applies.
func (e *ExpandBuilder) MaxParallel(n int) *ExpandBuilder {
	if n < 0 {
		e.errs = append(e.errs, fmt.Errorf("senro: expansion %q has MaxParallel %d", e.id, n))
		return e
	}
	e.parallel = n
	return e
}

// MaxNodes refuses an expansion wider than n, defaulting to
// plan.DefaultMaxNodes. This guards against a bad glob turning into 40k
// pods: the refusal happens at Build, with the count and the pattern named,
// rather than at run time with a scheduler already holding hundreds of
// sandboxes.
func (e *ExpandBuilder) MaxNodes(n int) *ExpandBuilder {
	if n <= 0 {
		e.errs = append(e.errs, fmt.Errorf("senro: expansion %q has MaxNodes %d", e.id, n))
		return e
	}
	e.maxNodes = n
	return e
}

// Needs declares upstream steps every child waits for, the same step-level
// dependency (*StepBuilder).Needs declares.
func (e *ExpandBuilder) Needs(ids ...string) *ExpandBuilder {
	e.needs = append(e.needs, ids...)
	return e
}

// NeedsEach declares a PER-UNIT dependency on another expansion: the child
// for a unit waits on that expansion's child for the SAME unit, and on
// nothing else.
//
//	verify.Expand("build", gowork.Modules()).Template(...)
//	verify.Expand("test", gowork.Modules()).
//		NeedsEach("build").
//		Template(...)
//
// It takes EXPANSION ids, the id given to Expand, not step ids: Needs takes
// those. A name matching no expansion in the pipeline is refused at Build,
// because a NeedsEach that quietly did nothing would be a fan-out with no
// ordering at all.
//
// Without it, ordering one fan-out after another means the whole-expansion
// barrier: no child of "test" starts until every child of "build" has
// settled, so one slow module holds up every other module's tests. With it,
// test[unit=web] starts the moment build[unit=web] finishes. Beware that two
// expansions in two workflows joined by the workflow-level senro.Needs get
// entry-to-exit edges ON TOP of these, which are the barrier again; put both
// expansions in ONE workflow when the point is to pipeline them.
//
// The two unit sets will not always match (a module with no tests, two
// different graphs), and both easy answers are wrong: dropping the edge lets
// a step run before its input exists, dropping the step silently skips work.
// So neither, in either direction:
//
//   - A unit HERE with no counterpart THERE keeps its step and falls back to
//     the whole-expansion barrier: that child waits for EVERY child of the
//     named expansion. That can only order more, never less.
//   - A unit THERE with no counterpart HERE is ordinary and is not an error;
//     it simply has no per-unit dependent.
//
// Naming an empty expansion (a glob that matched nothing) gains no edges,
// the same nothing an empty group already means everywhere else.
func (e *ExpandBuilder) NeedsEach(expansions ...string) *ExpandBuilder {
	e.needsEach = append(e.needsEach, expansions...)
	return e
}

// When gates every child of an expansion, the same way the workflow-level
// When gates every step of a workflow: a condition declared here is appended
// to every materialized child's own When, in addition to (and ANDed with)
// anything the child's own Template call declares.
func (e *ExpandBuilder) When(c Condition) *ExpandBuilder {
	e.when = append(e.when, c)
	return e
}

// Affected narrows the expansion to the units a change actually reaches: the
// units that own a changed file, and every unit that depends on one of those,
// at any depth. This is the point of fanning out over a monorepo at all:
// without it a push that touched one package builds every package.
//
//	verify.Expand("test", gowork.Packages()).
//		Affected(change.FromTrigger(ev)).
//		Template(func(u senro.Unit) *senro.StepBuilder {
//			return senro.NewStep(exec.Command("go", "test", "./...")).WorkDir(u.Dir)
//		})
//
// It is a plan-time filter, not a run-time skip: unaffected children are NOT
// in the plan, deliberately unlike When, which prunes a node the plan
// contains. Two runs of the same commit against the same base produce the
// same plan, and a re-run reconstitutes exactly the children the first run
// had. An empty affected set materializes no children, the same ordinary
// empty expansion (plan.expansion_skipped) a glob that matched nothing
// produces.
//
// The unit graph has to know which unit imports which (see UnitAffector).
// gowork does; glob does not, and Build REFUSES an Affected over a glob
// graph rather than quietly running everything: a silent run-everything
// would be indistinguishable in a plan or a log from a computed answer.
//
// It deliberately runs too much at the edges: a changed file that belongs to
// no unit (a root Makefile, a CI workflow) affects every unit, and a change
// source that cannot tell what changed says "everything" rather than
// "nothing". Running an unneeded unit costs minutes; skipping a needed one
// costs trust. See package change and gowork's Owns.
//
// MaxNodes is still checked against the WHOLE graph, not the narrowed set: a
// 40k-unit graph is a mistake whether or not today's pull request touched
// three of them.
func (e *ExpandBuilder) Affected(src ChangeSource) *ExpandBuilder {
	if src == nil {
		e.errs = append(e.errs, fmt.Errorf(
			"senro: expansion %q was given a nil change source; use change.Everything() to say "+
				"every unit runs", e.id))
		return e
	}
	if e.changes != nil {
		e.errs = append(e.errs, fmt.Errorf("senro: expansion %q has two change sources", e.id))
		return e
	}
	e.changes = src
	return e
}

// unitIDNeedsEscaping reports whether id contains a character that would
// corrupt the expanded step id grammar stepid.Format builds
// ("base[k=v,k=v]"), including "@", the CLI address's attempt separator
// (stepid.ParseAddress). Not hypothetical: glob.Dirs matches directory names
// verbatim, and a directory such as "app=v1" would otherwise silently build
// a child id nothing downstream can parse back apart.
func unitIDNeedsEscaping(id string) bool {
	return strings.ContainsAny(id, "[]=,@")
}

// resolve materializes this expansion's children. It is called once per
// Build, never at run time, and never mutates the builder: Build must be
// repeatable, and an expansion that appended to its own state would double
// its children on a second call.
func (e *ExpandBuilder) resolve(ctx context.Context, root string) ([]*StepBuilder, plan.GroupSpec, error) {
	if len(e.errs) > 0 {
		return nil, plan.GroupSpec{}, e.errs[0]
	}
	if e.id == "" {
		return nil, plan.GroupSpec{}, fmt.Errorf("senro: an expansion has an empty id")
	}
	if e.graph == nil {
		return nil, plan.GroupSpec{}, fmt.Errorf("senro: expansion %q has no unit graph", e.id)
	}
	if err := e.checkTemplates(); err != nil {
		return nil, plan.GroupSpec{}, err
	}
	units, total, err := e.units(ctx, root)
	if err != nil {
		return nil, plan.GroupSpec{}, fmt.Errorf("senro: expansion %q: %w", e.id, err)
	}
	// The WHOLE graph, not the narrowed set, and before partitioning: the
	// guard is against a bad pattern, and collapsing forty thousand
	// directories into eight steps does not make the pattern good.
	if total > e.maxNodes {
		return nil, plan.GroupSpec{}, fmt.Errorf(
			"senro: expansion %q (%s) found %d units, more than MaxNodes(%d); narrow the pattern or "+
				"raise MaxNodes deliberately", e.id, e.graph.Describe(), total, e.maxNodes)
	}
	// Checked over the whole unit set whether or not this expansion
	// partitions: shard children carry a shard number, but unit ids still key
	// NeedsEach's pairing, and one restriction everywhere beats two that
	// nearly hold.
	for _, u := range units {
		if unitIDNeedsEscaping(u.ID) {
			return nil, plan.GroupSpec{}, fmt.Errorf(
				"senro: expansion %q: unit id %q contains a character (one of \"[]=,@\") that would "+
					"corrupt its own child step id; rename the unit or narrow the pattern", e.id, u.ID)
		}
	}
	group := plan.GroupSpec{Name: e.id, MaxParallel: e.parallel}
	if e.buckets > 0 {
		out, err := e.resolveShards(ctx, root, units)
		return out, group, err
	}
	out := make([]*StepBuilder, 0, len(units))
	for _, u := range units {
		sb := e.tmpl(u)
		if sb == nil {
			return nil, plan.GroupSpec{}, fmt.Errorf(
				"senro: expansion %q returned no step for unit %q", e.id, u.ID)
		}
		if sb.handler {
			return nil, plan.GroupSpec{}, fmt.Errorf(
				"senro: expansion %q returned a handler for unit %q; build the template with "+
					"senro.NewStep, not senro.Handler", e.id, u.ID)
		}
		sb.id = stepid.Format(e.id, map[string]string{"unit": u.ID})
		sb.group = e.id
		sb.units = []string{u.ID}
		sb.needs = append(sb.needs, e.needs...)
		sb.when = append(sb.when, e.when...)
		out = append(out, sb)
	}
	return out, group, nil
}

// checkTemplates refuses every combination of Template, TemplateShard and
// Partition that does not describe one expansion.
func (e *ExpandBuilder) checkTemplates() error {
	switch {
	case e.tmpl != nil && e.shardTmpl != nil:
		return fmt.Errorf(
			"senro: expansion %q has both a Template and a TemplateShard; an expansion makes "+
				"either one step per unit or one step per shard", e.id)
	case e.buckets > 0 && e.shardTmpl == nil:
		return fmt.Errorf(
			"senro: expansion %q is partitioned and has no TemplateShard; a shard is several "+
				"units and the per-unit Template has nowhere to put them", e.id)
	case e.buckets == 0 && e.shardTmpl != nil:
		return fmt.Errorf(
			"senro: expansion %q has a TemplateShard and no Partition, so nothing would ever "+
				"call it; add Partition(n, history) or use Template", e.id)
	case e.tmpl == nil && e.shardTmpl == nil:
		return fmt.Errorf(
			"senro: expansion %q has no Template, so there is nothing to make one node per unit from",
			e.id)
	}
	return nil
}

// resolveShards materializes a partitioned expansion's children: one step per
// bucket, numbered, rather than one per unit. See Partition.
func (e *ExpandBuilder) resolveShards(ctx context.Context, root string, units []Unit) ([]*StepBuilder, error) {
	durations, err := e.history.Durations(ctx, root, e.id)
	if err != nil {
		// A history that cannot be read is a fault, not a cold start: reading
		// a corrupt committed file as empty would silently switch a fleet
		// back to balancing by count.
		return nil, fmt.Errorf("senro: expansion %q: reading %s: %w", e.id, e.history.Describe(), err)
	}
	buckets := unit.Partition(units, e.buckets, durations)
	out := make([]*StepBuilder, 0, len(buckets))
	for i, b := range buckets {
		sh := Shard{Index: i, Total: len(buckets), Units: b}
		sb := e.shardTmpl(sh)
		if sb == nil {
			return nil, fmt.Errorf("senro: expansion %q returned no step for shard %d", e.id, i)
		}
		if sb.handler {
			return nil, fmt.Errorf(
				"senro: expansion %q returned a handler for shard %d; build the template with "+
					"senro.NewStep, not senro.Handler", e.id, i)
		}
		sb.id = stepid.Format(e.id, map[string]string{"shard": strconv.Itoa(i)})
		sb.group = e.id
		sb.units = sh.IDs()
		sb.needs = append(sb.needs, e.needs...)
		sb.when = append(sb.when, e.when...)
		out = append(out, sb)
	}
	return out, nil
}

// units is the expansion's unit set and the size of the whole graph it came
// out of. With no Affected the two are the same; with one, the first is the
// narrowed set and the second is what MaxNodes is judged against.
//
// A change source reporting All takes the plain Units path without asking
// for an affected set: a push to the default branch covers everything by
// definition and must not require a graph capable of narrowing. That lets a
// pipeline declare Affected once and still build on a graph, or on a day,
// where narrowing is not possible.
func (e *ExpandBuilder) units(ctx context.Context, root string) ([]Unit, int, error) {
	if e.changes == nil {
		us, err := e.graph.Units(ctx, root)
		return us, len(us), err
	}
	set, err := e.changes.Changed(ctx, root)
	if err != nil {
		return nil, 0, err
	}
	if set.All {
		us, err := e.graph.Units(ctx, root)
		return us, len(us), err
	}
	res, err := unit.Affected(ctx, e.graph, root, set.Files)
	if err != nil {
		if errors.Is(err, unit.ErrNoAffectedSet) {
			return nil, 0, fmt.Errorf("%w. Fan out over a graph that knows which unit depends on "+
				"which (gowork, cargo, jswork, maven or gradle, under "+
				"github.com/xavidop/senro/unit), or drop Affected and run every unit", err)
		}
		return nil, 0, err
	}
	return res.Units, res.Total, nil
}

// resolvedExpansion is one expansion and the children it materialized, which
// is what lowering NeedsEach needs and nothing else keeps: a plan node
// records its group but not the unit set behind it, and resolved[w] has
// already flattened children in among the workflow's own steps.
type resolvedExpansion struct {
	spec     *ExpandBuilder
	children []*StepBuilder
}

// lowerNeedsEach turns each expansion's NeedsEach into ordinary step edges,
// so nothing downstream (the workflow barrier, plan.Validate, the scheduler)
// has to know this feature exists.
//
// The rule is stated over unit SETS because a child need not be one unit: a
// downstream child waits on every upstream child covering at least one of
// its units, and, if any of its units is covered by no upstream child at
// all, on every upstream child instead. See NeedsEach for why the mismatch
// resolves that way.
//
// Every list is built in resolve order and deduplicated while keeping it:
// the order reaches plan.json and the step.created event, not just the
// digest (which sorts Needs anyway).
func (p *Pipeline) lowerNeedsEach(expansions []resolvedExpansion) error {
	if len(expansions) == 0 {
		return nil
	}
	at := make(map[string][]int, len(expansions))
	for i, rx := range expansions {
		at[rx.spec.id] = append(at[rx.spec.id], i)
	}
	for _, rx := range expansions {
		for _, target := range rx.spec.needsEach {
			if target == rx.spec.id {
				return fmt.Errorf(
					"senro: expansion %q declares NeedsEach(%q), which is itself; a child cannot "+
						"wait for its own unit's step", rx.spec.id, target)
			}
			idx, ok := at[target]
			if !ok {
				return fmt.Errorf(
					"senro: expansion %q declares NeedsEach(%q), and pipeline %q declares no "+
						"expansion with that id. NeedsEach names EXPANSIONS, the id given to Expand; "+
						"use Needs for a dependency on a step", rx.spec.id, target, p.name)
			}
			if len(idx) > 1 {
				return fmt.Errorf(
					"senro: expansion %q declares NeedsEach(%q), and pipeline %q declares %d "+
						"expansions with that id, so there is no one set of children to pair with",
					rx.spec.id, target, p.name, len(idx))
			}
			pairEach(rx.children, expansions[idx[0]].children)
		}
	}
	return nil
}

// pairEach adds the per-unit edges from one expansion's children to another's.
func pairEach(children, upstream []*StepBuilder) {
	owner := make(map[string][]string, len(upstream))
	all := make([]string, 0, len(upstream))
	for _, up := range upstream {
		all = append(all, up.id)
		for _, u := range up.units {
			owner[u] = append(owner[u], up.id)
		}
	}
	for _, sb := range children {
		var matched []string
		seen := make(map[string]bool, len(sb.units))
		unpaired := false
		for _, u := range sb.units {
			ids, ok := owner[u]
			if !ok {
				unpaired = true
				break
			}
			for _, id := range ids {
				if !seen[id] {
					seen[id] = true
					matched = append(matched, id)
				}
			}
		}
		if unpaired {
			// The whole-expansion barrier; see NeedsEach on why this is not a
			// dropped edge and not a dropped step.
			matched = all
		}
		sb.needs = append(sb.needs, matched...)
	}
}

// Build resolves and validates the pipeline. The returned plan is a
// snapshot: further building does not change it.
//
// The workflow layer is resolved away here: a plan is a flat set of step
// nodes and edges, each workflow-level Needs is lowered into step-level
// edges (see Needs), and a pipeline of one workflow builds into exactly the
// plan its steps alone describe. That keeps a plan's digest, and every cache
// entry keyed under it, a function of the steps a pipeline declares rather
// than of how they happen to be grouped.
//
// A node's Env is exactly what the caller declared; Build supplies no
// default PATH. A default would put the host's $PATH into plan.Digest(),
// giving two developers on one commit two plan identities. A search path is
// a property of the host, which belongs in executor.Executor.Class(), not in
// the timetable; the default lives in the local executor. A PATH set
// explicitly through StepBuilder.Env is part of the pipeline's definition,
// belongs in the digest, and no executor overrides it.
//
// Expansions resolve here, exactly once, into ordinary []*StepBuilder: from
// this point a child of Expand is a step like any other. Resolving in a
// helper that might run twice could see two different filesystem trees and
// would double every expansion's children. Build passes context.Background()
// to Units because changing Build's signature would break every caller,
// senro.Run included; the cost is that a slow Build (gowork shells out to
// `go list -deps -json` over a whole workspace) cannot be cancelled. A
// BuildContext variant would be the fix, added rather than changed.
func (p *Pipeline) Build() (*Plan, error) {
	resolved := make(map[*WorkflowBuilder][]*StepBuilder, len(p.workflows))
	var groups []plan.GroupSpec
	root, err := os.Getwd()
	if err != nil {
		return nil, fmt.Errorf("senro: resolving the expansion root: %w", err)
	}
	var expansions []resolvedExpansion
	for _, w := range p.workflows {
		steps := append([]*StepBuilder(nil), w.steps...)
		for _, e := range w.expansions {
			children, group, err := e.resolve(context.Background(), root)
			if err != nil {
				return nil, err
			}
			steps = append(steps, children...)
			groups = append(groups, group)
			expansions = append(expansions, resolvedExpansion{spec: e, children: children})
		}
		resolved[w] = steps
	}
	// Before anything reads a step's dependencies: a per-unit edge added
	// after the entry/exit sets or node conversion would be an edge the
	// workflow barrier never saw.
	if err := p.lowerNeedsEach(expansions); err != nil {
		return nil, err
	}

	if err := p.checkWorkflowNames(resolved); err != nil {
		return nil, err
	}
	if err := p.checkWorkflowNeeds(); err != nil {
		return nil, err
	}
	if err := p.checkExecutorTargets(); err != nil {
		return nil, err
	}

	steps := p.allSteps(resolved)
	topLevel := make(map[string]bool, len(steps))
	for _, sb := range steps {
		topLevel[sb.id] = true
	}

	// spec is shared by every node of one workflow deliberately: it is never
	// mutated after Build, and sharing makes one workflow's steps compare
	// equal by pointer as well as by value.
	pl := &plan.Plan{Version: 1, Groups: groups}
	for _, w := range p.workflows {
		var spec *plan.ExecutorSpec
		if w.on != nil {
			s := w.on.ExecutorSpec()
			// A local target records nothing: recording "local" on every node
			// would move every existing plan_digest for no gain.
			if s.Kind != "" && s.Kind != plan.ExecutorLocal {
				spec = &s
			}
		}
		for _, sb := range resolved[w] {
			n, err := toNode(sb, nil)
			if err != nil {
				return nil, err
			}
			n.Executor = spec
			// Workflow conditions precede the step's own; Digest sorts them,
			// so the order is provenance, not semantics.
			if len(w.when) > 0 {
				whenSerials := make([]string, len(w.when))
				for i, c := range w.when {
					whenSerials[i] = c.Serial()
				}
				n.When = append(whenSerials, n.When...)
			}
			if err := checkHandlerIDsAreDistinctFromSteps(sb.id, n.OnFailure, topLevel); err != nil {
				return nil, err
			}
			if err := checkHandlerIDsAreDistinctFromSteps(sb.id, n.Always, topLevel); err != nil {
				return nil, err
			}
			pl.Nodes = append(pl.Nodes, n)
		}
	}
	p.lowerWorkflowNeeds(pl, resolved)
	if err := collectDeclarations(pl, steps); err != nil {
		return nil, err
	}
	if err := pl.Validate(); err != nil {
		return nil, err
	}
	return pl, nil
}

// allSteps is every step in the pipeline, in workflow declaration order
// then step declaration order (a workflow's own steps, then each expansion's
// children in unit order), which is what the plan's node order becomes.
func (p *Pipeline) allSteps(resolved map[*WorkflowBuilder][]*StepBuilder) []*StepBuilder {
	var out []*StepBuilder
	for _, w := range p.workflows {
		out = append(out, resolved[w]...)
	}
	return out
}

// checkWorkflowNames refuses two workflows sharing a name, and any step id
// declared twice, including two expansion children that resolved to the same
// id (a UnitGraph reporting two units with one Unit.ID: stepid.Format builds
// a child's whole identity from that ID alone).
//
// plan.Validate rejects a duplicate step id too, but by then the workflow
// layer is gone and the message could only say which id repeated, not where
// either copy came from.
func (p *Pipeline) checkWorkflowNames(resolved map[*WorkflowBuilder][]*StepBuilder) error {
	seenWF := make(map[string]bool, len(p.workflows))
	stepOwner := make(map[string]string)
	for _, w := range p.workflows {
		if w.name == "" {
			return fmt.Errorf("senro: pipeline %q has a workflow with an empty name", p.name)
		}
		if seenWF[w.name] {
			return fmt.Errorf("senro: pipeline %q declares workflow %q twice; "+
				"declare it once and keep the returned builder", p.name, w.name)
		}
		seenWF[w.name] = true

		for _, sb := range resolved[w] {
			if prev, dup := stepOwner[sb.id]; dup {
				if prev == w.name {
					return fmt.Errorf("senro: workflow %q declares step %q twice", w.name, sb.id)
				}
				return fmt.Errorf("senro: step id %q is declared in both workflow %q and workflow %q; "+
					"step ids are unique across the whole pipeline", sb.id, prev, w.name)
			}
			stepOwner[sb.id] = w.name
		}
	}
	return nil
}

// checkWorkflowNeeds refuses a workflow-level Needs naming a workflow that
// does not exist, a workflow that needs itself, and a cycle between
// workflows. The unknown-name message names both sides because the usual
// mistake is a step id passed to the workflow-level Needs, which looks
// identical at the call site.
func (p *Pipeline) checkWorkflowNeeds() error {
	byName := make(map[string]*WorkflowBuilder, len(p.workflows))
	for _, w := range p.workflows {
		byName[w.name] = w
	}
	for _, w := range p.workflows {
		for _, need := range w.needs {
			if need == w.name {
				return fmt.Errorf("senro: workflow %q needs itself", w.name)
			}
			if _, ok := byName[need]; !ok {
				return fmt.Errorf("senro: workflow %q needs workflow %q, which pipeline %q does not "+
					"declare. senro.Needs names workflows, not steps; use (*senro.StepBuilder).Needs "+
					"for a dependency on a step", w.name, need, p.name)
			}
		}
	}
	return p.checkWorkflowsAcyclic(byName)
}

// checkWorkflowsAcyclic reports the first cycle among workflows, naming
// every workflow on it. plan.checkAcyclic would catch the lowered step
// edges, but the step names it prints are not what anybody wrote.
func (p *Pipeline) checkWorkflowsAcyclic(byName map[string]*WorkflowBuilder) error {
	const (
		white = 0
		grey  = 1
		black = 2
	)
	colour := make(map[string]int, len(byName))
	var path []string

	var visit func(string) error
	visit = func(name string) error {
		switch colour[name] {
		case black:
			return nil
		case grey:
			from := 0
			for i, n := range path {
				if n == name {
					from = i
					break
				}
			}
			cycle := append(append([]string(nil), path[from:]...), name)
			return fmt.Errorf("senro: workflow dependency cycle: %s", strings.Join(cycle, " -> "))
		}
		colour[name] = grey
		path = append(path, name)
		for _, need := range byName[name].needs {
			if err := visit(need); err != nil {
				return err
			}
		}
		path = path[:len(path)-1]
		colour[name] = black
		return nil
	}

	// Declaration order, not map order, so the same broken pipeline reports
	// the same cycle every time.
	for _, w := range p.workflows {
		if err := visit(w.name); err != nil {
			return err
		}
	}
	return nil
}

// checkExecutorTargets refuses an On target this build cannot run. See On.
func (p *Pipeline) checkExecutorTargets() error {
	for _, w := range p.workflows {
		if w.on == nil {
			continue
		}
		switch kind := w.on.ExecutorSpec().Kind; kind {
		case plan.ExecutorLocal:
		case plan.ExecutorContainer:
		case plan.ExecutorK8s:
		case plan.ExecutorSSH:
		default:
			return fmt.Errorf(
				"senro: workflow %q is targeted with senro.On at the %q executor, and this build "+
					"has the local executor (senro.Local), the container executor "+
					"(container.Image), the Kubernetes executor (k8s.Pod) and the SSH executor "+
					"(ssh.Host)",
				w.name, kind)
		}
	}
	return nil
}

// lowerWorkflowNeeds turns each workflow-level Needs into step edges: every
// entry step of the dependent workflow gains a dependency on every exit step
// of each workflow it needs. Entry-to-exit edges alone give the barrier;
// an edge for every pair would state the same ordering |A| × |B| times.
// Edges are appended in declaration order, so the same pipeline lowers to
// the same plan every time.
//
// resolved is each workflow's steps AFTER expansion: a workflow whose only
// steps come from Expand has an empty w.steps, and computing entry and exit
// from those would leave its barrier a no-op. See
// TestAWorkflowBarrierWaitsForEveryChildOfAnExpansion.
func (p *Pipeline) lowerWorkflowNeeds(pl *plan.Plan, resolved map[*WorkflowBuilder][]*StepBuilder) {
	byName := make(map[string]*WorkflowBuilder, len(p.workflows))
	for _, w := range p.workflows {
		byName[w.name] = w
	}
	nodes := make(map[string]*plan.Node, len(pl.Nodes))
	for i := range pl.Nodes {
		nodes[pl.Nodes[i].ID] = &pl.Nodes[i]
	}

	for _, w := range p.workflows {
		if len(w.needs) == 0 {
			continue
		}
		entries := entrySteps(resolved[w])
		if len(entries) == 0 {
			continue
		}
		for _, need := range w.needs {
			for _, exit := range exitSteps(resolved[byName[need]]) {
				for _, entry := range entries {
					n := nodes[entry]
					n.Needs = append(n.Needs, exit)
				}
			}
		}
	}
}

// entrySteps are the ids of the steps that start a workflow's resolved step
// set: those that depend on no other step IN THAT SET. A dependency on a
// step in another workflow is its own edge and says nothing about where this
// workflow begins.
func entrySteps(steps []*StepBuilder) []string {
	mine := stepIDsOf(steps)
	var out []string
	for _, sb := range steps {
		internal := false
		for _, need := range sb.needs {
			if mine[need] {
				internal = true
				break
			}
		}
		if !internal {
			out = append(out, sb.id)
		}
	}
	return out
}

// exitSteps are the ids of the steps that finish a workflow's resolved step
// set: those no other step IN THAT SET depends on.
func exitSteps(steps []*StepBuilder) []string {
	mine := stepIDsOf(steps)
	depended := make(map[string]bool, len(steps))
	for _, sb := range steps {
		for _, need := range sb.needs {
			if mine[need] {
				depended[need] = true
			}
		}
	}
	var out []string
	for _, sb := range steps {
		if !depended[sb.id] {
			out = append(out, sb.id)
		}
	}
	return out
}

func stepIDsOf(steps []*StepBuilder) map[string]bool {
	ids := make(map[string]bool, len(steps))
	for _, sb := range steps {
		ids[sb.id] = true
	}
	return ids
}

// checkHandlerIDsAreDistinctFromSteps rejects a handler whose id matches a
// top-level step's id. Nothing would run twice (the handler flag guards
// that), but the plan would read as one node appearing both on its own and
// nested inside another's handlers, which is confusing enough to reject.
func checkHandlerIDsAreDistinctFromSteps(parentID string, handlers []plan.Node, topLevel map[string]bool) error {
	for _, h := range handlers {
		if topLevel[h.ID] {
			return fmt.Errorf("senro: step %q has a handler %q with the same id as a top-level step: "+
				"they are distinct nodes that happen to share a name; rename one", parentID, h.ID)
		}
		if err := checkHandlerIDsAreDistinctFromSteps(parentID, h.OnFailure, topLevel); err != nil {
			return err
		}
		if err := checkHandlerIDsAreDistinctFromSteps(parentID, h.Always, topLevel); err != nil {
			return err
		}
	}
	return nil
}

// toNode converts one StepBuilder into a plan.Node: the one conversion,
// called recursively for handlers, so a handler and a step cannot drift in
// how their fields are copied.
//
// path is the set of builders already on this recursive descent, nil at the
// top-level call. h.OnFailure(h) passes checkHandlers (h.handler is true)
// and would otherwise recurse unboundedly: a typo becoming a stack overflow
// instead of an error Build can report. Checking by pointer identity, not
// id, also catches a cycle between two distinct handler builders.
//
// Every slice is copied, never aliased: a caller may keep mutating sb, or a
// slice passed into Env/Needs/OnFailure/Always, after Build returns, and the
// returned plan must not change underneath them.
func toNode(sb *StepBuilder, path map[*StepBuilder]bool) (plan.Node, error) {
	if path[sb] {
		return plan.Node{}, fmt.Errorf(
			"senro: handler %q refers back to itself through its own OnFailure/Always chain", sb.id)
	}
	if path == nil {
		path = make(map[*StepBuilder]bool)
	}
	path[sb] = true
	defer delete(path, sb)

	if len(sb.errs) > 0 {
		return plan.Node{}, sb.errs[0]
	}
	if sb.action == nil {
		return plan.Node{}, fmt.Errorf("senro: step %q has no action", sb.id)
	}
	n := plan.Node{
		ID:              sb.id,
		Kind:            sb.action.ActionKind(),
		Cmd:             append([]string(nil), sb.action.ActionCmd()...),
		WorkDir:         sb.workDir,
		Env:             append([]string(nil), sb.env...),
		Needs:           append([]string(nil), sb.needs...),
		ContinueOnError: sb.continueOnError,
		Group:           sb.group,
	}
	if fa, ok := sb.action.(funcAction); ok {
		if fa.err != nil {
			return plan.Node{}, fmt.Errorf("senro: step %q: %w", sb.id, fa.err)
		}
		if _, registered := funcs.Lookup(fa.name); !registered {
			return plan.Node{}, fmt.Errorf(
				"senro: step %q names function %q, which nothing registered; registered names are %v. "+
					"Call senro.RegisterFunc in an init function of the package that defines it",
				sb.id, fa.name, funcs.Names())
		}
		n.Func = &plan.FuncSpec{Name: fa.name, Params: fa.params}
	} else if n.Kind == "func" {
		return plan.Node{}, fmt.Errorf(
			"senro: step %q has an Action reporting kind \"func\" that senro.Func did not build; "+
				"a func step carries a registered name and parameters, which only senro.Func supplies",
			sb.id)
	}
	for _, c := range sb.when {
		n.When = append(n.When, c.Serial())
	}
	if sb.hasRetry {
		spec, err := toRetrySpec(sb)
		if err != nil {
			return plan.Node{}, err
		}
		n.Retry = spec
	}
	if sb.timeout > 0 {
		n.TimeoutMS = sb.timeout.Milliseconds()
	}
	if sb.generate != nil {
		n.Generate = &plan.GenerateSpec{Path: sb.generate.path}
	}
	n.Pure = sb.pure
	n.NoSnapshot = sb.noSnapshot
	n.CacheEnv = append([]string(nil), sb.cacheEnv...)
	for _, se := range sb.secretEnvs {
		n.Secrets = append(n.Secrets, plan.SecretSpec{Name: se.field, Env: se.env})
	}
	for _, sel := range sb.inputs {
		n.Inputs = append(n.Inputs, sel.Serial())
	}
	for _, sel := range sb.outputs {
		n.Outputs = append(n.Outputs, sel.Serial())
	}
	for _, m := range sb.mounts {
		spec, err := toMountSpec(sb.id, m)
		if err != nil {
			return plan.Node{}, err
		}
		n.Mounts = append(n.Mounts, spec)
	}
	for _, h := range sb.onFailure {
		hn, err := toNode(h, path)
		if err != nil {
			return plan.Node{}, err
		}
		n.OnFailure = append(n.OnFailure, hn)
	}
	for _, h := range sb.always {
		hn, err := toNode(h, path)
		if err != nil {
			return plan.Node{}, err
		}
		n.Always = append(n.Always, hn)
	}
	return n, nil
}

// toRetrySpec serializes sb's retry policy into a plan.RetrySpec. The
// predicate carries its own serialized form (see retry.Predicate); an empty
// form means retry.Func or the zero Predicate, which no plan can carry.
// Build refuses rather than leaving RetrySpec.Predicate empty, which would
// silently become retry-on-every-failure.
func toRetrySpec(sb *StepBuilder) (*plan.RetrySpec, error) {
	serial := sb.retryPred.Serial()
	if serial == "" {
		return nil, fmt.Errorf("senro: step %q retry predicate has no serialized form and cannot be "+
			"built into a plan, which is what retry.Func produces; use retry.OnInfra, retry.OnExitCode, "+
			"retry.OnLogMatch or retry.Any instead", sb.id)
	}
	return &plan.RetrySpec{
		MaxAttempts:   sb.retryMax,
		Predicate:     serial,
		BackoffBaseMS: sb.retryBackoff.Base.Milliseconds(),
		BackoffMaxMS:  sb.retryBackoff.Max.Milliseconds(),
		BackoffFactor: sb.retryBackoff.Factor,
	}, nil
}

// toMountSpec converts one Mount. A zero Mount names nothing, which is what
// a caller writing senro.Mount{} by hand produces; catching it here means the
// error names the step rather than surfacing later as an unresolvable name.
func toMountSpec(stepID string, m Mount) (plan.MountSpec, error) {
	switch {
	case m.ws != nil:
		mode := string(m.mode)
		if mode == "" {
			mode = string(RW)
		}
		return plan.MountSpec{Workspace: m.ws.name, At: m.at, Mode: mode}, nil
	case m.scratch != nil:
		return plan.MountSpec{Scratch: m.scratch.name, At: m.at}, nil
	default:
		return plan.MountSpec{}, fmt.Errorf(
			"senro: step %q has a mount that names neither a workspace nor a scratch cache; "+
				"build one with Workspace(...).At(...) or ScratchCache(...).At(...)", stepID)
	}
}

// ScopeKind is a workspace's lifetime.
type ScopeKind string

const (
	// ScopeStep is one fresh directory PER STEP, shared with nobody and
	// discarded with the run.
	//
	// It buys isolation a run-scoped workspace cannot: every step mounting a
	// ScopeRun workspace mounts the same directory, so a step sees what its
	// siblings left there and can stamp on what they are still using. Reach
	// for this when a step wants a clean tree to work in and nothing
	// downstream reads what it produced. Nothing is snapshotted from one, for
	// the same reason: there is no later step to hand it to.
	ScopeStep ScopeKind = "step"
	// ScopeRun is shared across the steps of one run. The common case, and
	// the default.
	ScopeRun ScopeKind = "run"
	// ScopePersistent survives runs: one directory on this machine, named by
	// the workspace's own name, that every later run mounting the same name
	// starts from. For expensive trees (a dependency cache, a checkout) not
	// worth rebuilding every run.
	//
	// It requires an explicit MaxAge and MaxSize, both: an unbounded
	// persistent workspace fills the disk silently.
	//
	// It is MACHINE-GLOBAL and keyed by name alone, so two pipelines that
	// both declare a workspace called "cache" share one directory. Name it
	// for what is IN it ("go-mod-cache"), not for the role it plays.
	//
	// Its content is digested at the start of every run and that digest
	// enters the cache key of every step mounting it, so a Pure() step whose
	// persistent workspace changed between runs misses, correctly. The
	// measurement is a full snapshot per run, before the first step, and is
	// visible on trees big enough for this scope to be worth using.
	//
	// One run at a time holds a given persistent workspace; a second run is
	// refused before any step has run, naming the holder. See senro.MaxAge.
	ScopePersistent ScopeKind = "persistent"
)

// MountMode is whether a step may write through a mount. See RO for what
// "read-only" actually means, which differs by executor.
type MountMode string

const (
	// RO marks a mount read-only. Enforcement depends on the executor: the
	// container and Kubernetes executors enforce it (a read-only bind; a pod
	// spec with ReadOnly set), while the local and ssh executors cannot (no
	// unprivileged bind mounts; a transferred directory carries no per-step
	// mode), so a write there succeeds. The backstop for those two is
	// detection: a read-only mount whose content digest changed while a step
	// ran fails that step, naming the workspace, because a workspace digest
	// that does not describe what the step read makes every cache key
	// computed from it wrong.
	RO MountMode = "ro"
	// RW marks a mount read-write.
	RW MountMode = "rw"
)

type workspaceConfig struct {
	scope            ScopeKind
	exclude          []string
	preserveSymlinks bool
	maxAge           time.Duration
	maxSize          int64
}

// WorkspaceOption configures a workspace.
type WorkspaceOption func(*workspaceConfig)

// Scope sets a workspace's lifetime.
func Scope(k ScopeKind) WorkspaceOption { return func(c *workspaceConfig) { c.scope = k } }

// MaxAge is how long a ScopePersistent workspace survives without being
// used. A run that finds one older than this evicts it and starts from an
// empty directory: a cold cache, never a failure.
//
// "Used" means "leased by a run", recorded at release, so a workspace a
// nightly build touches every night never ages out however old the tree is.
//
// Mandatory for ScopePersistent and refused for every other scope: how long
// a cache should outlive its last use is a property of the pipeline, not of
// senro. Evaluated only at the START of a run: deleting a directory a
// running step is reading is the exact hazard the workspace locks exist to
// prevent.
func MaxAge(d time.Duration) WorkspaceOption {
	return func(c *workspaceConfig) { c.maxAge = d }
}

// MaxSize is how large a ScopePersistent workspace's content may grow, in
// bytes, measured the way a snapshot measures it: the regular files that
// survive the workspace's excludes.
//
// A workspace over the bound is evicted whole, not trimmed: half a
// dependency tree is a broken one. A workspace evicted on every run has a
// MaxSize set below what the tree actually needs; the ws.evicted event says
// which bound fired and by how much.
//
// Enforced when a run releases the workspace and again when the next run
// leases it (so a run killed before releasing cannot leave an unbounded tree
// behind), never mid-run: see MaxAge. Mandatory for ScopePersistent and
// refused for every other scope.
func MaxSize(bytes int64) WorkspaceOption {
	return func(c *workspaceConfig) { c.maxSize = bytes }
}

// Exclude keeps paths out of the workspace's snapshots. Patterns use the
// same syntax everywhere in senro: "*" and "?" within a segment, "**" across
// segments, and a trailing "/" for a directory and everything under it.
func Exclude(patterns ...string) WorkspaceOption {
	return func(c *workspaceConfig) { c.exclude = append(c.exclude, patterns...) }
}

// PreserveSymlinks declares that this workspace's own directories literally
// named "node_modules" must survive a snapshot, not just the symlinks that
// point into them.
//
// Every workspace excludes ".git" and "node_modules" by default, which is
// wrong for a workspace that IS a tree of symlinks, such as pnpm's
// node_modules: pnpm realizes each package under a directory ALSO named
// "node_modules", one level down, and the default would strip exactly what
// every symlink points at. Restoring such a snapshot would still produce the
// symlinks, every one pointing at nothing.
//
// It widens this one workspace's default excludes to keep its
// node_modules-shaped directories; ".git" stays excluded regardless. It
// changes what a snapshot includes, so declaring it moves the plan's digest;
// omitting it does not.
func PreserveSymlinks() WorkspaceOption {
	return func(c *workspaceConfig) { c.preserveSymlinks = true }
}

// WorkspaceRef names a workspace. It is a declaration, not a directory: what
// directory it becomes is the executor's business.
type WorkspaceRef struct {
	name             string
	scope            ScopeKind
	exclude          []string
	preserveSymlinks bool
	maxAge           time.Duration
	maxSize          int64
}

// Workspace declares a named, versioned directory with a content digest.
func Workspace(name string, opts ...WorkspaceOption) *WorkspaceRef {
	cfg := workspaceConfig{scope: ScopeRun}
	for _, o := range opts {
		o(&cfg)
	}
	return &WorkspaceRef{
		name: name, scope: cfg.scope,
		exclude:          append([]string(nil), cfg.exclude...),
		preserveSymlinks: cfg.preserveSymlinks,
		maxAge:           cfg.maxAge,
		maxSize:          cfg.maxSize,
	}
}

// At mounts the workspace into a step at a path.
func (w *WorkspaceRef) At(at string, mode MountMode) Mount {
	return Mount{ws: w, at: at, mode: mode}
}

type scratchConfig struct {
	key         string
	restoreKeys []string
}

// ScratchOption configures a scratch cache.
type ScratchOption func(*scratchConfig)

// Key sets the scratch cache's lookup key. The value is a template evaluated
// once per run, with one function available: hashFiles, which takes globs
// relative to the pipeline process's working directory.
func Key(template string) ScratchOption { return func(c *scratchConfig) { c.key = template } }

// RestoreKeys are prefixes tried, in order, when the exact key misses. The
// newest entry under the first matching prefix wins.
func RestoreKeys(prefixes ...string) ScratchOption {
	return func(c *scratchConfig) { c.restoreKeys = append(c.restoreKeys, prefixes...) }
}

// ScratchRef names a scratch cache: a mutable directory restored
// best-effort, such as a module cache. Distinct from a workspace because a
// miss is not an error and a stale hit only costs time, and because it is
// NEVER an input to an action cache key.
type ScratchRef struct {
	name        string
	key         string
	restoreKeys []string
}

// ScratchCache declares one.
func ScratchCache(name string, opts ...ScratchOption) *ScratchRef {
	var cfg scratchConfig
	for _, o := range opts {
		o(&cfg)
	}
	return &ScratchRef{name: name, key: cfg.key, restoreKeys: append([]string(nil), cfg.restoreKeys...)}
}

// At mounts the scratch cache into a step. There is no mode: a scratch cache
// is always writable, since the point is that the step fills it.
func (c *ScratchRef) At(at string) Mount { return Mount{scratch: c, at: at} }

// Mount is one workspace or scratch cache realized into one step. Whether
// an RO mount is actually enforced read-only depends on the executor; see RO.
type Mount struct {
	ws      *WorkspaceRef
	scratch *ScratchRef
	at      string
	mode    MountMode
}

// StepBuilder configures one station.
type StepBuilder struct {
	id              string
	action          Action
	needs           []string
	env             []string
	workDir         string
	continueOnError bool
	when            []Condition

	// group names the expansion this step was materialized from, or "" for
	// an ordinary step declared with Step. Set only by ExpandBuilder.resolve;
	// toNode copies it into plan.Node.Group.
	group string

	// units are the unit ids this step COVERS, in unit order, or nil for an
	// ordinary step. Set only by ExpandBuilder.resolve. A slice because a
	// partitioned child covers a bucket of units; NeedsEach pairs children by
	// intersecting these sets. It stays out of the plan deliberately: the
	// pairing is lowered into ordinary Needs edges at Build, and putting it
	// in plan.Node would move the digest of every expanded plan ever built.
	units []string

	// handler is set by Handler and left false by WorkflowBuilder.Step.
	// OnFailure and Always check it (a workflow's step passed to them would
	// run twice, once in each role); they check this flag rather than the
	// workflow because a builder does not know which workflow it joined.
	handler bool

	// errs accumulates problems found by chaining methods, which return
	// *StepBuilder and so cannot report an error directly; Build surfaces
	// the first one.
	errs []error

	hasRetry     bool // Retry/RetryPolicy was called at all
	retryMax     int
	retryPred    retry.Predicate
	retryBackoff retry.Backoff

	timeout time.Duration

	onFailure []*StepBuilder
	always    []*StepBuilder

	// generate is set by Generates and nil for every ordinary step; toNode
	// lowers it into plan.Node.Generate.
	generate *Generator

	mounts     []Mount
	pure       bool
	inputs     []artifact.Selector
	outputs    []artifact.Selector
	cacheEnv   []string
	noSnapshot bool

	secretEnvs []secretEnv
}

// secretEnv is one SecretEnv declaration: which configuration field, and
// which environment variable receives its file's path.
type secretEnv struct {
	env   string
	field string
}

// ID is the step's declared identifier.
//
// Exists for building a fragment in a loop, where a step depends on the
// sibling it just created (see Fragment.Step). Without it the id is written
// twice, once to declare and once to depend on, and the second copy is where
// a typo lives: a mistyped need is caught at splice time, mid-run, rather
// than by the compiler.
func (s *StepBuilder) ID() string { return s.id }

// Needs declares upstream STEPS that must finish first: the step-level
// dependency, naming step ids, distinct from the package-level Needs, which
// names whole workflows. Naming a step in another workflow is allowed (step
// ids are unique across the pipeline) and expresses a single edge rather
// than the barrier the workflow-level Needs gives you.
func (s *StepBuilder) Needs(ids ...string) *StepBuilder {
	s.needs = append(s.needs, ids...)
	return s
}

// Env sets one environment variable, as a key and a value:
//
//	Env("PNPM_HOME", "/pnpm-store").Env("CI", "1")
//
// One pair per call, rather than a variadic list of pairs, whose arity the
// compiler cannot check: Env("A", "1", "B") would build, and the missing
// value could only be reported by Build, some distance from the typo.
//
// The key must be non-empty and must not contain "=": Env("A=1", "2") would
// quietly produce the entry "A=1=2". Build reports either mistake.
//
// Env on a FUNC step is refused at Build: the function receives a Ctx, not
// an environment, so the variable has no way to arrive, yet it would still
// move the step's cache key on the way to being dropped. Read the value in a
// closure where you call RegisterFunc, or pass it in the parameters Func
// records.
func (s *StepBuilder) Env(key, value string) *StepBuilder {
	switch {
	case key == "":
		s.errs = append(s.errs, fmt.Errorf(
			"senro: step %q sets an environment variable with an empty name (value %q)", s.id, value))
	case strings.Contains(key, "="):
		s.errs = append(s.errs, fmt.Errorf(
			"senro: step %q sets an environment variable named %q, which contains \"=\"; "+
				"Env takes a name and a value as two arguments", s.id, key))
	default:
		s.env = append(s.env, key+"="+value)
	}
	return s
}

// SecretEnv delivers a resolved secret to this step as a FILE, and puts that
// file's PATH into the named environment variable:
//
//	setup.Step("install", exec.Command("pnpm", "install")).
//		SecretEnv("NPM_TOKEN", "NPMToken")
//
// field names a field of the struct handed to senro.WithSecrets. A field
// inside a named nested struct is spelled with a dot ("Registry.Token"); a
// field promoted from an embedded struct keeps its bare name.
//
// The variable holds a PATH, not a value: a value in an environment variable
// is readable through /proc/<pid>/environ for the whole life of the process,
// where senro's redactor cannot reach. The value goes to a 0600 file in a
// tmpfs-preferring directory:
//
//	SecretEnv("NPM_TOKEN", "NPMToken")   // NPM_TOKEN=/run/user/1000/senro-secret-xyz/NPMToken
//	// in the step:  npm config set //registry.npmjs.org/:_authToken="$(cat "$NPM_TOKEN")"
//
// Every declared secret ALSO arrives under the uniform name
// SENRO_SECRET_<NAME> (the field name uppercased, characters outside A-Z,
// 0-9 and _ replaced by _); SecretEnv's variable is the ergonomic second
// name for the same path. A run whose plan puts a resolved value into a
// command argument or an environment variable is refused before the first
// step starts, because both are visible outside the process (ps(1),
// /proc/<pid>/environ, shell history) where redaction cannot follow; see the
// secrets section of the README.
//
// The secret's IDENTITY (its source URI, and a digest of its value salted
// with that URI) enters the step's cache key; the value never does. Naming
// one variable in both SecretEnv and CacheEnv is refused at build time: the
// path is per-attempt, so the key would change on every run and never hit.
func (s *StepBuilder) SecretEnv(envName, field string) *StepBuilder {
	switch {
	case envName == "":
		s.errs = append(s.errs, fmt.Errorf(
			"senro: step %q declares a SecretEnv with an empty variable name (for secret %q)",
			s.id, field))
	case strings.Contains(envName, "="):
		s.errs = append(s.errs, fmt.Errorf(
			"senro: step %q declares a SecretEnv named %q, which contains \"=\"; SecretEnv takes "+
				"a variable name and a configuration field name as two arguments", s.id, envName))
	case field == "":
		s.errs = append(s.errs, fmt.Errorf(
			"senro: step %q declares SecretEnv(%q, \"\") with no configuration field to read",
			s.id, envName))
	default:
		s.secretEnvs = append(s.secretEnvs, secretEnv{env: envName, field: field})
	}
	return s
}

// WorkDir sets the working directory the step's command runs in.
//
// Refused at Build on a FUNC step: a func runs in the coordinator's own
// process, where the working directory is process-global, so honouring one
// would move every step running alongside it. Reach files through
// Ctx.Workspace instead.
func (s *StepBuilder) WorkDir(dir string) *StepBuilder {
	s.workDir = dir
	return s
}

// ContinueOnError lets dependents run even if this step fails. Use for
// advisory steps such as lint or coverage upload.
func (s *StepBuilder) ContinueOnError() *StepBuilder {
	s.continueOnError = true
	return s
}

// When gates one step on a condition, the same way the workflow-level When
// gates a whole workflow. A step in a workflow that also declares a
// workflow-level When must satisfy both; see the package-level When.
func (s *StepBuilder) When(c Condition) *StepBuilder {
	s.when = append(s.when, c)
	return s
}

// Retry lets this step run again, up to maxAttempts total tries, when p
// judges a failed attempt worth retrying. Backoff between attempts uses
// retry.Backoff's zero-value defaults; use RetryPolicy to set it explicitly.
//
// Build records p.Serial(): a Plan is JSON and cannot carry a func, so a
// predicate built with retry.Func fails Build rather than silently becoming
// a policy that retries on every failure. Use retry.OnInfra,
// retry.OnExitCode, retry.OnLogMatch or retry.Any.
//
// Retry on a HANDLER is refused: the engine runs a handler exactly once, so
// a policy declared on one would be recorded and never honoured. Retry the
// step instead, which exhausts its attempts before any handler runs.
func (s *StepBuilder) Retry(maxAttempts int, p retry.Predicate) *StepBuilder {
	s.hasRetry = true
	s.retryMax = maxAttempts
	s.retryPred = p
	s.retryBackoff = retry.Backoff{}
	return s
}

// RetryPolicy is Retry for a caller that also wants to control backoff: the
// same declaration, so everything Retry documents (the predicate and the
// refusal on a handler alike) applies here too.
func (s *StepBuilder) RetryPolicy(policy retry.Policy) *StepBuilder {
	s.hasRetry = true
	s.retryMax = policy.MaxAttempts
	s.retryPred = policy.On
	s.retryBackoff = policy.Backoff
	return s
}

// Timeout bounds how long a single attempt of this step may run.
func (s *StepBuilder) Timeout(d time.Duration) *StepBuilder {
	s.timeout = d
	return s
}

// OnFailure runs handlers, in order, when this step's attempts are
// exhausted and it still failed. Build handlers with Handler, not Step.
// Passing a *StepBuilder returned by Step is rejected at Build, since that
// step would then run twice: once on its own, once as this handler.
func (s *StepBuilder) OnFailure(handlers ...*StepBuilder) *StepBuilder {
	s.checkHandlers("OnFailure", handlers)
	s.onFailure = append(s.onFailure, handlers...)
	return s
}

// Always runs handlers, in order, after this step settles, whether it
// succeeded or failed. Build handlers with Handler, not Step. Passing a
// *StepBuilder returned by Step is rejected at Build, since that step would
// then run twice: once on its own, once as this handler.
func (s *StepBuilder) Always(handlers ...*StepBuilder) *StepBuilder {
	s.checkHandlers("Always", handlers)
	s.always = append(s.always, handlers...)
	return s
}

// checkHandlers records an error on s for each of handlers that was
// returned by Step, not Handler. It cannot return the error itself:
// OnFailure and Always return *StepBuilder so calls chain, and Build
// surfaces the error later.
func (s *StepBuilder) checkHandlers(method string, handlers []*StepBuilder) {
	for _, h := range handlers {
		if !h.handler {
			s.errs = append(s.errs, fmt.Errorf(
				"senro: step %q was given %q as an %s handler, but %q is a step on a "+
					"workflow, so it would run twice. Use senro.Handler(%q, ...) instead",
				s.id, h.id, method, h.id, h.id))
		}
	}
}

// Mount realizes workspaces and scratch caches into this step. Whether an
// RO mount is actually enforced read-only depends on the executor; see RO.
func (s *StepBuilder) Mount(m ...Mount) *StepBuilder {
	s.mounts = append(s.mounts, m...)
	return s
}

// Pure declares this step eligible for the action cache.
//
// Steps are impure by default (never cached, never skipped), the correct
// default for a tool that can ssh into production and restart a service.
// Pure() is trusted rather than enforced: nothing sandboxes the network in
// this build, so it is a claim a reviewer can see, and `senro verify
// --recheck-pure` is how the claim is CHECKED: it re-runs a cached step
// against the exact input its key records and reports digests that do not
// come back the same.
//
// A Pure() step must declare Inputs. Build refuses one that does not,
// because a key that cannot change when the sources change is worse than no
// cache at all.
func (s *StepBuilder) Pure() *StepBuilder {
	s.pure = true
	return s
}

// Inputs declares the files this step reads. They are hashed into its cache
// key: you cannot hash what you have not declared.
func (s *StepBuilder) Inputs(sel ...artifact.Selector) *StepBuilder {
	s.inputs = append(s.inputs, sel...)
	return s
}

// Outputs declares the files this step produces. They are stored when the
// step's result is saved and restored when it is served from cache, so a
// cached step still leaves behind what an uncached one would have.
func (s *StepBuilder) Outputs(sel ...artifact.Selector) *StepBuilder {
	s.outputs = append(s.outputs, sel...)
	return s
}

// CacheEnv names environment variables that belong in this step's cache key.
// Only a digest of each value enters the key, so a credential that reached
// the environment by mistake cannot reach a cache entry, which outlives the
// run directory.
//
// Nothing is allowlisted by default, on purpose: keys built from the whole
// environment would differ between machines, and the cache would never hit
// for a reason nobody could see.
func (s *StepBuilder) CacheEnv(names ...string) *StepBuilder {
	s.cacheEnv = append(s.cacheEnv, names...)
	return s
}

// NoSnapshot suppresses the workspace snapshot this step would otherwise
// take when it settles. For a step whose filesystem output nobody consumes.
func (s *StepBuilder) NoSnapshot() *StepBuilder {
	s.noSnapshot = true
	return s
}

// collectDeclarations records every workspace and scratch cache any step
// mounts, once each, in a deterministic order. Two steps mounting the same
// workspace declare it once; two DIFFERENT declarations under one name are
// refused, because a workspace whose excludes depended on which step was
// built first would snapshot differently from run to run.
func collectDeclarations(p *plan.Plan, steps []*StepBuilder) error {
	seenWS := make(map[string]*WorkspaceRef)
	seenSC := make(map[string]*ScratchRef)
	for _, sb := range steps {
		for _, m := range sb.mounts {
			switch {
			case m.ws != nil:
				prev, ok := seenWS[m.ws.name]
				if !ok {
					seenWS[m.ws.name] = m.ws
					p.Workspaces = append(p.Workspaces, plan.WorkspaceSpec{
						Name:             m.ws.name,
						Scope:            string(m.ws.scope),
						MaxAgeMS:         m.ws.maxAge.Milliseconds(),
						MaxSizeBytes:     m.ws.maxSize,
						Exclude:          append([]string(nil), m.ws.exclude...),
						PreserveSymlinks: m.ws.preserveSymlinks,
					})
					continue
				}
				if prev.scope != m.ws.scope || !equalStrings(prev.exclude, m.ws.exclude) ||
					prev.preserveSymlinks != m.ws.preserveSymlinks ||
					prev.maxAge != m.ws.maxAge || prev.maxSize != m.ws.maxSize {
					return fmt.Errorf(
						"senro: workspace %q is declared twice with different options; "+
							"declare it once and share the value", m.ws.name)
				}
			case m.scratch != nil:
				prev, ok := seenSC[m.scratch.name]
				if !ok {
					seenSC[m.scratch.name] = m.scratch
					p.Scratch = append(p.Scratch, plan.ScratchSpec{
						Name:        m.scratch.name,
						Key:         m.scratch.key,
						RestoreKeys: append([]string(nil), m.scratch.restoreKeys...),
					})
					continue
				}
				if prev.key != m.scratch.key || !equalStrings(prev.restoreKeys, m.scratch.restoreKeys) {
					return fmt.Errorf(
						"senro: scratch cache %q is declared twice with different options; "+
							"declare it once and share the value", m.scratch.name)
				}
			}
		}
	}
	return nil
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
