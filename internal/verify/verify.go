// Package verify re-executes cached Pure() steps and compares what they
// produce against what the action cache recorded for them.
//
// Pure() is trusted, not enforced: nothing sandboxes a step's network
// access, and a wrong cache hit is silent, persistent, and inherited by
// every downstream result. Rather than per-executor network sandboxing,
// this is the empirical answer: put the step back in front of the exact
// input it saw, run it again, and compare digests.
//
// A mismatch means the step depended on something outside its key, which is
// narrower than "under-declared inputs": a mounted workspace's whole content
// is already digested into the key (cache.Key.WorkspaceDigests), so what the
// key cannot see is the network, a file elsewhere on the host, an
// undeclared environment variable, or the clock.
package verify

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"

	"github.com/xavidop/senro/internal/cache"
	"github.com/xavidop/senro/internal/cas"
	"github.com/xavidop/senro/internal/executor"
	"github.com/xavidop/senro/internal/executor/localexec"
	"github.com/xavidop/senro/internal/plan"
	"github.com/xavidop/senro/internal/stepid"
	"github.com/xavidop/senro/internal/storage"
	"github.com/xavidop/senro/internal/workspace"
)

// Verdict is one step's outcome. "impure" is deliberately not a value: this
// command cannot see a step's reason, only its bytes.
type Verdict string

const (
	// Verified means the re-run reproduced the cached result exactly: same
	// declared outputs, same workspace content, same exit code.
	Verified Verdict = "verified"

	// Mismatch means the re-run differed from the cached result AND a second
	// re-run agreed with the first: deterministic, yet not reproducing the
	// cache, so the step depended on something the key does not cover.
	Mismatch Verdict = "mismatch"

	// NonDeterministic means the re-run differed from the cached result and
	// ALSO from a second re-run of itself (an embedded timestamp, a PID), so
	// the disagreement says nothing about purity. Kept separate from
	// Mismatch to avoid alarms a user learns to ignore; not a pass either,
	// since the entry serves bytes no re-run would produce.
	NonDeterministic Verdict = "nondeterministic"

	// Planned means this step WOULD be re-run and was not, because the caller
	// did not ask for execution. See Options.Execute.
	Planned Verdict = "planned"

	// Skipped means this step cannot be checked, for a reason the report
	// names: a step nobody could check must not read as one that passed.
	Skipped Verdict = "skipped"

	// Errored means the check itself broke: a sandbox that could not be
	// built, a workspace that could not be restored. Distinct from Mismatch
	// because nothing was learned about the step.
	Errored Verdict = "error"
)

// Options configures one Check.
type Options struct {
	// Plan is the run's own resolved plan, read from <run>/plan.json. A cache
	// key hashes a step's arguments rather than storing them (see
	// cache.CommandComponent), so an entry alone cannot say what to re-run.
	Plan *plan.Plan

	// Records are the run's own cache decisions, read from <run>/cache. One
	// per Pure() step the run actually attempted.
	Records []cache.Record

	// Storage holds the entries and workspace bodies. Check reads from it
	// and adds CAS objects; it never writes an action cache entry.
	Storage *storage.Storage

	// WorkRoot is where re-runs happen: one throwaway directory tree per step
	// per round, never the run's own workspaces and never the caller's
	// checkout. The caller owns creating and removing it.
	WorkRoot string

	// LocalClass mirrors senro.WithLocalClass: a step is only comparable
	// against an entry recorded on the same executor equivalence class.
	LocalClass string

	// Steps limits the check to these step IDs. Empty means every step in
	// Records.
	Steps []string

	// Limit bounds how many steps are checked, in plan order. Zero means no
	// bound: one run's worth of Pure() steps.
	Limit int

	// Execute is the consent to actually run anything. False (the default a
	// caller gets by not asking) plans the work and executes none of it.
	Execute bool

	// Classify asks for the second re-run that separates Mismatch from
	// NonDeterministic, and is only ever spent on a step that already
	// disagreed with its entry.
	Classify bool

	// Now stamps the report. Injected so a test does not have to match a
	// clock.
	Now time.Time
}

// Difference is one thing that did not come back the same.
type Difference struct {
	// Kind is "output", "workspace" or "exit_code". An output is a declared
	// product (the strong signal); a workspace is everything the step left
	// behind, declared or not (the weaker one).
	Kind string `json:"kind"`
	// Name is the declared output's path, the workspace's name, or empty for
	// an exit code.
	Name string `json:"name"`
	// Cached and Rerun are the two digests (or exit codes) compared. Either
	// may be "absent": a declared output never produced is a difference
	// worth its own word rather than an empty string.
	Cached string `json:"cached"`
	Rerun  string `json:"rerun"`
	// RerunAgain is the second re-run's value, present only when Classify
	// spent one; it is what separates the two verdicts.
	RerunAgain string `json:"rerun_again,omitempty"`
}

// Step is one step's result.
type Step struct {
	Step    string  `json:"step"`
	Verdict Verdict `json:"verdict"`
	// Key is the entry this step was checked against.
	Key string `json:"key"`
	// Hermeticity is what the entry recorded about how it was produced:
	// "trusted" for every entry this build writes. Reported so an entry
	// produced under real enforcement can one day be told apart without a
	// migration. See cache.HermeticityTrusted.
	Hermeticity string `json:"hermeticity,omitempty"`
	// FromRun is the run that actually produced the entry, which is not
	// necessarily the run being verified: a hit serves an older run's result.
	FromRun string `json:"from_run,omitempty"`
	// Inputs and Outputs are the step's declared selectors, verbatim: a
	// mismatch is only actionable next to them.
	Inputs  []string `json:"declared_inputs,omitempty"`
	Outputs []string `json:"declared_outputs,omitempty"`
	// Reason explains a Skipped or Errored verdict. Always set for those two
	// and always empty otherwise.
	Reason string `json:"reason,omitempty"`
	// Differences is what did not match, empty for Verified.
	Differences []Difference `json:"differences,omitempty"`
	// NotCompared names anything deliberately left out of the comparison and
	// why: a check that quietly narrows its scope has a green that means
	// less than a reader thinks.
	NotCompared []string `json:"not_compared,omitempty"`
	// WorkDir is where the re-run happened, so a mismatch can be opened
	// rather than only read about. Empty when nothing executed.
	WorkDir string `json:"work_dir,omitempty"`
}

// Report is the whole pass. It is `senro verify --json`'s wire contract, so
// its field names are additive-only.
type Report struct {
	// Executed says whether anything actually ran. A report with this false
	// is a plan, and every step in it is Planned.
	Executed bool   `json:"executed"`
	Checked  int    `json:"checked"`
	Steps    []Step `json:"steps"`
	// Truncated is how many eligible steps Limit left unchecked, so a bounded
	// sample can never be mistaken for the whole set.
	Truncated int `json:"truncated,omitempty"`
}

// Findings is every step whose re-run did not reproduce its entry, both
// Mismatch and NonDeterministic: a caller gating CI wants one number.
func (r Report) Findings() int {
	n := 0
	for _, s := range r.Steps {
		if s.Verdict == Mismatch || s.Verdict == NonDeterministic {
			n++
		}
	}
	return n
}

// Unchecked is every step that could not be checked at all.
func (r Report) Unchecked() int {
	n := 0
	for _, s := range r.Steps {
		if s.Verdict == Skipped || s.Verdict == Errored {
			n++
		}
	}
	return n
}

// Check re-executes the cached Pure() steps opts names and reports what came
// back.
//
// It never writes an action cache entry: saving over the entry being
// compared would make a second invocation pass silently and destroy the
// evidence. TestCheckNeverWritesTheActionCache pins it. It DOES add CAS
// objects when snapshotting a re-run's workspace; those are immutable,
// cannot create a cache hit, and `senro cache gc` reclaims them.
//
// The returned error means the PASS could not be made (no plan, no
// storage). A step that could not be checked is a Skipped step with a
// reason, never an error, so one unreadable entry cannot suppress a finding
// next to it.
func Check(ctx context.Context, opts Options) (Report, error) {
	if opts.Plan == nil {
		return Report{}, errors.New("verify: no plan to check against")
	}
	if opts.Storage == nil {
		return Report{}, errors.New("verify: no storage to read entries from")
	}

	eligible := selectRecords(opts)
	reach := newReachability(opts.Plan)
	rep := Report{Executed: opts.Execute}
	if opts.Limit > 0 && len(eligible) > opts.Limit {
		rep.Truncated = len(eligible) - opts.Limit
		eligible = eligible[:opts.Limit]
	}

	for _, rec := range eligible {
		rep.Steps = append(rep.Steps, checkOne(ctx, opts, reach, rec))
	}
	rep.Checked = len(rep.Steps)
	return rep, nil
}

// selectRecords is which of the run's cache records this pass considers, in
// PLAN order rather than cache.ReadRecords' alphabetical order, so
// `--limit 3` checks the first three steps a reader would name, not the
// three whose IDs sort first.
func selectRecords(opts Options) []cache.Record {
	byStep := make(map[string]cache.Record, len(opts.Records))
	for _, r := range opts.Records {
		byStep[r.Step] = r
	}
	want := make(map[string]bool, len(opts.Steps))
	for _, s := range opts.Steps {
		want[s] = true
	}
	var out []cache.Record
	for i := range opts.Plan.Nodes {
		id := opts.Plan.Nodes[i].ID
		rec, ok := byStep[id]
		if !ok {
			continue
		}
		if len(want) > 0 && !want[id] {
			continue
		}
		out = append(out, rec)
	}
	return out
}

// checkOne is one step, from eligibility through to a verdict.
func checkOne(ctx context.Context, opts Options, reach reachability, rec cache.Record) Step {
	s := Step{Step: rec.Step, Key: string(rec.Digest)}

	node, ok := opts.Plan.Node(rec.Step)
	if !ok {
		return skip(s, "the run recorded a cache decision for this step and the run's own plan has no such "+
			"node, so there is nothing to re-run")
	}
	s.Inputs, s.Outputs = node.Inputs, node.Outputs

	if reason := refuse(node); reason != "" {
		return skip(s, reason)
	}

	res, found, err := opts.Storage.Action.Lookup(ctx, rec.Step, rec.Key)
	if err != nil {
		return skip(s, fmt.Sprintf("reading this step's cache entry: %v", err))
	}
	if !found {
		return skip(s, "no entry is stored under this key any more, so there is nothing to compare a re-run "+
			"against; a `senro cache gc` sweep evicts entries by least recent use")
	}
	s.Hermeticity, s.FromRun = res.Hermeticity, res.RunID

	pre, ok := rec.Key.Workspaces()
	if !ok {
		return skip(s, "this entry's key does not record the workspace state the step started from in a form "+
			"this build can read, which is how an entry written by an older senro looks; re-run the pipeline "+
			"once to write a current one")
	}
	if len(pre) == 0 {
		return skip(s, "this step mounts no workspace, so its Inputs resolve against the working directory "+
			"the pipeline ran in rather than against content this command can reconstitute; re-running it "+
			"would mean running it against your checkout, which this command will not do")
	}
	if reason := missingBodies(ctx, opts.Storage, pre); reason != "" {
		return skip(s, reason)
	}

	if !opts.Execute {
		s.Verdict = Planned
		return s
	}

	first, err := rerun(ctx, opts, node, pre, 1)
	if err != nil {
		s.Verdict, s.Reason = Errored, err.Error()
		s.WorkDir = roundDir(opts.WorkRoot, node.ID, 1)
		return s
	}
	s.WorkDir = first.dir

	diffs, notCompared := compareToEntry(opts.Plan, reach, node, res, first)
	s.NotCompared = notCompared
	if len(diffs) == 0 {
		s.Verdict = Verified
		return s
	}

	// The second round is spent only on a step that already disagreed, so a
	// pass over steps that all verify costs one execution each.
	if !opts.Classify {
		s.Verdict, s.Differences = Mismatch, diffs
		return s
	}
	second, err := rerun(ctx, opts, node, pre, 2)
	if err != nil {
		// Classification failed; report the unclassified result, not a guess.
		s.Verdict, s.Differences = Mismatch, diffs
		s.Reason = fmt.Sprintf("a second re-run, which would have separated a non-deterministic step from an "+
			"impure one, could not be made: %v", err)
		return s
	}
	s.Verdict, s.Differences = classify(diffs, first, second)
	return s
}

// refuse is every reason a step is not re-runnable by this command, in one
// place so none is a silent pass. Each is a refusal rather than a best
// effort: checking something other than what the run did would produce a
// verdict nobody can act on.
func refuse(n *plan.Node) string {
	if !n.Pure {
		return "this step is not Pure(), so it was never cached and there is nothing to verify"
	}
	if len(n.Secrets) > 0 {
		return "this step declares secrets, and this command cannot resolve them: they come from the " +
			"configuration struct the pipeline binary handed senro.WithSecrets, which is not in the run " +
			"directory and must not be. Re-running it without them would test a different step"
	}
	if n.ExecutorKey() != plan.ExecutorLocal {
		return fmt.Sprintf("this step runs on the %q executor, and this command only re-runs steps on the "+
			"local one: building the others would mean pulling an image or creating a pod, which is more "+
			"than a verification pass should do behind your back", n.Executor.Kind)
	}
	if n.Kind == "func" {
		return "this step is a Func step, whose body is compiled into the pipeline binary rather than " +
			"described by the plan, so this command has nothing to execute"
	}
	if len(n.Cmd) == 0 {
		return "this step has no command in the plan, so there is nothing to execute"
	}
	return ""
}

func skip(s Step, reason string) Step {
	s.Verdict, s.Reason = Skipped, reason
	return s
}

// missingBodies reports the first pre-step workspace body no longer in the
// store: unverifiable, not failed, since only a failed run's workspaces are
// pinned against a sweep.
func missingBodies(ctx context.Context, store *storage.Storage, pre []cache.WorkspaceDigest) string {
	for _, w := range pre {
		if w.Digest == "" {
			// No content yet when the step ran: the re-run starts from an
			// empty directory, exactly as the step did.
			continue
		}
		ok, err := store.CAS.Has(ctx, w.Digest)
		if err != nil {
			return fmt.Sprintf("checking the stored body of workspace %q: %v", w.Name, err)
		}
		if !ok {
			return fmt.Sprintf("the body of workspace %q as this step saw it (%s) is no longer in the store, "+
				"most likely collected by a `senro cache gc` sweep, so the step cannot be put back in front "+
				"of the input it actually had", w.Name, w.Digest.Short())
		}
	}
	return ""
}

// observation is what one re-run produced.
type observation struct {
	dir        string
	exit       int
	workspaces map[string]cas.Digest
	outputs    map[string]cas.Digest
}

// rerun executes one round of a step in a throwaway sandbox built from the
// workspace state its own cache key records.
//
// This isolation is the command's safety: nothing here touches the run's
// workspaces, the caller's checkout, or the action cache; every mount is a
// fresh directory restored from a content address. A step that reaches the
// network is the step this command exists to find, and its side effects are
// not containable here; that is what Options.Execute is for.
func rerun(ctx context.Context, opts Options, n *plan.Node, pre []cache.WorkspaceDigest, round int) (observation, error) {
	dir := roundDir(opts.WorkRoot, n.ID, round)
	wsRoot := filepath.Join(dir, "ws")
	if err := os.MkdirAll(wsRoot, 0o755); err != nil {
		return observation{}, fmt.Errorf("verify: %w", err)
	}

	specs := make(map[string]plan.WorkspaceSpec, len(opts.Plan.Workspaces))
	for _, w := range opts.Plan.Workspaces {
		specs[w.Name] = w
	}
	digests := make(map[string]cas.Digest, len(pre))
	for _, w := range pre {
		digests[w.Name] = w.Digest
	}

	var mounts []executor.Mount
	for _, ms := range n.Mounts {
		if ms.Workspace == "" {
			continue
		}
		spec := specs[ms.Workspace]
		path := filepath.Join(wsRoot, ms.Workspace)
		d := digests[ms.Workspace]
		if d == "" {
			if err := os.MkdirAll(path, 0o755); err != nil {
				return observation{}, fmt.Errorf("verify: workspace %q: %w", ms.Workspace, err)
			}
		} else if err := opts.Storage.Snapshotter.Restore(ctx, d, path); err != nil {
			return observation{}, fmt.Errorf("verify: workspace %q: %w", ms.Workspace, err)
		}
		mounts = append(mounts, executor.Mount{
			Name: ms.Workspace, Digest: string(d), Path: path, At: ms.At,
			RO: ms.Mode == "ro", Exclude: spec.Exclude, PreserveSymlinks: spec.PreserveSymlinks,
		})
	}

	// A scratch cache is realized COLD: it is never an input to a cache key
	// (see plan.ScratchSpec), so a step whose result depends on one is
	// impure, and handing the re-run a warmed copy would hide exactly that.
	all := append([]executor.Mount(nil), mounts...)
	for _, ms := range n.Mounts {
		if ms.Scratch == "" {
			continue
		}
		path := filepath.Join(dir, "scratch", ms.Scratch)
		if err := os.MkdirAll(path, 0o755); err != nil {
			return observation{}, fmt.Errorf("verify: scratch cache %q: %w", ms.Scratch, err)
		}
		all = append(all, executor.Mount{Name: ms.Scratch, Path: path, At: ms.At, Scratch: true})
	}

	ex := localexec.New(dir, opts.Storage.Snapshotter, localexec.WithClass(opts.LocalClass))
	sb, err := ex.Sandbox(ctx, executor.SandboxSpec{
		StepID: n.ID, Attempt: round, Env: n.Env, WorkDir: n.WorkDir, Mounts: all,
	})
	if err != nil {
		return observation{}, fmt.Errorf("verify: %w", err)
	}
	defer func() { _ = sb.Close(context.WithoutCancel(ctx), false) }()

	runCtx := ctx
	if n.TimeoutMS > 0 {
		var cancel context.CancelFunc
		runCtx, cancel = context.WithTimeout(ctx, time.Duration(n.TimeoutMS)*time.Millisecond)
		defer cancel()
	}

	stdout, err := os.Create(filepath.Join(dir, "stdout"))
	if err != nil {
		return observation{}, fmt.Errorf("verify: %w", err)
	}
	defer func() { _ = stdout.Close() }()
	stderr, err := os.Create(filepath.Join(dir, "stderr"))
	if err != nil {
		return observation{}, fmt.Errorf("verify: %w", err)
	}
	defer func() { _ = stderr.Close() }()

	// The step's output goes to files, never this process's streams, and is
	// never compared: logs legitimately carry timestamps, durations and PIDs.
	//
	// n.Env verbatim, deliberately WITHOUT the TRACEPARENT the engine exports
	// into every attempt: inventing one per round would give the two rounds
	// different environments, and a step that writes its environment into
	// its workspace would then read as nondeterministic.
	obs := observation{dir: dir, workspaces: map[string]cas.Digest{}, outputs: map[string]cas.Digest{}}
	exit, runErr := sb.Run(runCtx, executor.Cmd{
		Args: n.Cmd, Env: n.Env, Dir: executor.CmdDir(n.WorkDir, all),
	}, stdout, stderr)
	if runErr != nil {
		// Infrastructure failure, not a workload verdict (see
		// executor.Sandbox.Run): a step that could not launch told us nothing.
		return observation{}, fmt.Errorf("verify: re-running step %q: %w", n.ID, runErr)
	}
	obs.exit = exit

	if !n.NoSnapshot {
		for _, mt := range mounts {
			snap, err := sb.Snapshot(context.WithoutCancel(ctx), mt.Name)
			if err != nil {
				return observation{}, fmt.Errorf("verify: snapshot workspace %q: %w", mt.Name, err)
			}
			obs.workspaces[mt.Name] = cas.Digest(snap.Digest)
		}
	}

	if len(n.Outputs) > 0 {
		root, exc, err := outputRoot(n, specs, wsRoot)
		if err != nil {
			return observation{}, err
		}
		// A declared Output matching nothing is an ERROR in a real run
		// (cache.Resolve), but here it is precisely a finding, so the miss
		// is recorded as an absent output and compared as one.
		files, err := cache.Resolve(root, n.Outputs, exc)
		if err == nil {
			for _, f := range files {
				obs.outputs[f.Path] = f.Digest
			}
		}
	}
	return obs, nil
}

// outputRoot is the directory a re-run's declared Outputs resolve against
// and its excluder, answered exactly as the engine answers them for a live
// step (plan.Node.InputWorkspace and workspace.ExcluderFor, shared not
// copied).
func outputRoot(n *plan.Node, specs map[string]plan.WorkspaceSpec, wsRoot string) (string, *workspace.Excluder, error) {
	name, ok := n.InputWorkspace()
	if !ok {
		return "", nil, fmt.Errorf("verify: step %q declares Outputs and no unambiguous workspace to "+
			"resolve them against", n.ID)
	}
	root := filepath.Join(wsRoot, name)
	spec := specs[name]
	ex, err := workspace.ExcluderFor(root, spec.Exclude, spec.PreserveSymlinks)
	if err != nil {
		return "", nil, fmt.Errorf("verify: step %q: %w", n.ID, err)
	}
	return root, ex, nil
}

// roundDir is where one round of one step happens. stepid.Encode, so a step
// ID containing a path separator cannot name a directory outside the work
// root.
func roundDir(workRoot, step string, round int) string {
	return filepath.Join(workRoot, stepid.Encode(step), strconv.Itoa(round))
}

// absent is what a comparison prints for a side with no value at all; an
// empty string would read as "no digest", which is not what happened.
const absent = "absent"

// compareToEntry is what "the same" means: declared outputs, soundly
// comparable workspaces, and the exit code, plus what it declined to
// compare and why.
//
// Outputs come first: a declared product is the strong signal, a workspace
// (everything left behind, declared or not) the weaker one, and the report
// keeps them apart. Logs are never compared: they legitimately carry
// timestamps, durations and PIDs, and the resulting false alarms would
// teach a user to ignore the tool.
func compareToEntry(p *plan.Plan, reach reachability, n *plan.Node, res *cache.Result, obs observation) ([]Difference, []string) {
	var out []Difference
	var notCompared []string

	cachedOutputs := make(map[string]cas.Digest, len(res.Outputs))
	for _, o := range res.Outputs {
		cachedOutputs[o.Path] = o.Digest
	}
	for _, path := range union(cachedOutputs, obs.outputs) {
		c, r := cachedOutputs[path], obs.outputs[path]
		if c == r {
			continue
		}
		out = append(out, Difference{Kind: "output", Name: path, Cached: shortOr(c), Rerun: shortOr(r)})
	}

	if !n.NoSnapshot {
		cachedWS := make(map[string]cas.Digest, len(res.Workspaces))
		for _, w := range res.Workspaces {
			cachedWS[w.Name] = w.Digest
		}
		for _, name := range union(cachedWS, obs.workspaces) {
			if other, shared := concurrentWriter(p, reach, n, name); shared {
				// The entry's digest for a shared ScopeRun workspace is not a
				// function of this step alone: an unordered sibling's writes
				// are inside the snapshot, and the re-run has no siblings.
				// The reason is reported, and the step's declared outputs,
				// which no sibling writes, still decide the verdict.
				notCompared = append(notCompared, fmt.Sprintf(
					"workspace %q: step %q mounts it read-write and is unordered with respect to this "+
						"step, so what this step's entry recorded for it includes whatever that step had "+
						"written by then, and an isolated re-run has no such sibling",
					name, other))
				continue
			}
			c, r := cachedWS[name], obs.workspaces[name]
			if c == r {
				continue
			}
			out = append(out, Difference{Kind: "workspace", Name: name, Cached: shortOr(c), Rerun: shortOr(r)})
		}
	}

	if res.ExitCode != obs.exit {
		out = append(out, Difference{
			Kind:   "exit_code",
			Cached: strconv.Itoa(res.ExitCode), Rerun: strconv.Itoa(obs.exit),
		})
	}
	sort.Strings(notCompared)
	return out, notCompared
}

// concurrentWriter names another node that mounts ws read-write and is
// unordered with respect to n through Needs, so the scheduler may run them
// concurrently and each one's snapshot can contain the other's writes.
//
// A read-only sibling does not count: the engine fails a step that writes
// through an RO mount (snapshotMounts' breach check). Handlers do not count
// either: they inherit mounts read-only and run after the parent's
// snapshot.
func concurrentWriter(p *plan.Plan, reach reachability, n *plan.Node, ws string) (string, bool) {
	for i := range p.Nodes {
		other := &p.Nodes[i]
		if other.ID == n.ID || !mountsRW(other, ws) {
			continue
		}
		if reach[n.ID][other.ID] || reach[other.ID][n.ID] {
			continue
		}
		return other.ID, true
	}
	return "", false
}

func mountsRW(n *plan.Node, ws string) bool {
	for _, ms := range n.Mounts {
		if ms.Workspace == ws && cache.CanonicalMode(ms.Mode) == "rw" {
			return true
		}
	}
	return false
}

// reachability is the transitive closure of a plan's Needs graph:
// reach[a][b] is true when a depends on b, directly or transitively, which is
// the only thing that ORDERS two nodes. Anything else may run concurrently.
type reachability map[string]map[string]bool

// newReachability computes it by repeated relaxation rather than a
// topological sort: a plan off disk (plan.Unmarshal does not validate) may
// contain a cycle, and relaxation terminates on one regardless. Computed
// once per pass.
func newReachability(p *plan.Plan) reachability {
	reach := make(reachability, len(p.Nodes))
	for i := range p.Nodes {
		reach[p.Nodes[i].ID] = make(map[string]bool, len(p.Nodes[i].Needs))
		for _, need := range p.Nodes[i].Needs {
			reach[p.Nodes[i].ID][need] = true
		}
	}
	for changed := true; changed; {
		changed = false
		for id, direct := range reach {
			for need := range direct {
				for indirect := range reach[need] {
					if !reach[id][indirect] {
						reach[id][indirect] = true
						changed = true
					}
				}
			}
		}
	}
	return reach
}

// classify decides between Mismatch and NonDeterministic by asking whether
// the two re-runs agreed with each other, annotating every difference with
// the second round's value so the reasoning is visible.
func classify(diffs []Difference, first, second observation) (Verdict, []Difference) {
	verdict := Mismatch
	for i, d := range diffs {
		var a, b cas.Digest
		switch d.Kind {
		case "output":
			a, b = first.outputs[d.Name], second.outputs[d.Name]
		case "workspace":
			a, b = first.workspaces[d.Name], second.workspaces[d.Name]
		case "exit_code":
			diffs[i].RerunAgain = strconv.Itoa(second.exit)
			if first.exit != second.exit {
				verdict = NonDeterministic
			}
			continue
		}
		diffs[i].RerunAgain = shortOr(b)
		if a != b {
			verdict = NonDeterministic
		}
	}
	return verdict, diffs
}

// union is every key in either map, sorted, so a report reads the same twice
// running.
func union(a, b map[string]cas.Digest) []string {
	seen := make(map[string]bool, len(a)+len(b))
	out := make([]string, 0, len(a)+len(b))
	for _, m := range []map[string]cas.Digest{a, b} {
		for k := range m {
			if !seen[k] {
				seen[k] = true
				out = append(out, k)
			}
		}
	}
	sort.Strings(out)
	return out
}

func shortOr(d cas.Digest) string {
	if d == "" {
		return absent
	}
	return d.Short()
}
