package engine

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/internal/cas"
	"github.com/xavidop/senro/internal/executor"
	"github.com/xavidop/senro/internal/persist"
	"github.com/xavidop/senro/internal/plan"
	"github.com/xavidop/senro/internal/scratch"
	"github.com/xavidop/senro/internal/workspace"
)

// wsSnapshot is one workspace captured after one attempt.
type wsSnapshot struct {
	Name   string
	Digest cas.Digest
	Index  cas.Digest
	Bytes  int64
	Files  int
	// RO records how the step mounted it, for callers of digests(). It does
	// NOT feed the read-only breach check: snapshotMounts computes that
	// inline from executor.Mount.RO and never reads this field back.
	RO bool
}

// wsManager owns a run's workspace directories and their current digests.
//
// ScopeRun means one directory per workspace for the whole run, under
// <run>/ws/<name>, shared by every step that mounts it; nothing restores
// between local steps because the directory is already there. Snapshots
// exist to feed the next step's cache key, to let a cache hit put back what
// the skipped step would have produced, and to make a failed step's
// filesystem addressable afterwards.
type wsManager struct {
	dir   string
	snap  *workspace.Snapshotter
	specs map[string]plan.WorkspaceSpec

	mu    sync.Mutex
	state map[string]cas.Digest

	// wsLocks is one sync.RWMutex per workspace name, created lazily under
	// mu and kept for the run. It guards a real race: a ScopeRun workspace
	// is one directory shared by every step mounting it, and restoring a
	// cache hit into it is destructive (RemoveAll then Rename; see
	// workspace.Snapshotter.Restore).
	//
	// The rule: a step's ordinary use of a workspace takes a SHARED hold;
	// restoring a cache hit takes an EXCLUSIVE hold. RWMutex's writer
	// priority means a restore is never starved by a stream of fresh shared
	// holds. See lockMounts and lockRestore.
	wsLocks map[string]*sync.RWMutex

	// produced is every digest record has recorded for this run, body and
	// index, across every attempt, never overwritten: it feeds
	// allSnapshotDigests, which the end-of-run pin protects, and a retried
	// step's earlier failed attempt is evidence too.
	produced []cas.Digest

	// runDir is the run's own directory (opts.Dir), the parent of dir. It is
	// also where a run's scratch caches live, at <runDir>/scratch/<name>, and
	// where their records are written, at <runDir>/cache/scratch.json.
	runDir string

	// scratchSpecs, scratchKeys, scratchDone, scratchRecs and scratchExact
	// are keyed by scratch cache name. scratchKeys is resolved ONCE per
	// run, at construction: expanding mid-run would let two steps mounting
	// the same cache compute different keys depending on what an earlier
	// step wrote. scratchDone stops a second mount restoring over what the
	// first step put there. scratchRecs is what saveScratch stores and
	// `senro cache explain` reads (no event type; see scratch.Record).
	// scratchExact records whether a restore hit the run's own key EXACTLY,
	// as opposed to a restore-keys prefix fallback; see saveScratch for why
	// that distinction must not collapse into "anything was restored".
	scratchSpecs map[string]plan.ScratchSpec
	scratchKeys  map[string]string
	scratchDone  map[string]bool
	scratchRecs  map[string]scratch.Record
	scratchExact map[string]bool
	scratchStore scratch.Cache

	// scratchRemote marks a cache a step whose target does not share the
	// coordinator's filesystem mounted, and scratchRead is the directory
	// holding what such a step actually left in it (readScratch). Together
	// they are the whole rule saveScratch enforces: once a remote step has
	// mounted a cache, the coordinator's own directory is no longer
	// evidence of anything, so the run stores what came BACK or nothing at
	// all. A scratch entry is written once under its key and never
	// rewritten, so a stale copy stored there is the answer every later run
	// gets.
	scratchRemote map[string]bool
	scratchRead   map[string]string

	// leases holds one lease per ScopePersistent workspace, taken in
	// newWSManager and given back by releasePersistent or
	// abandonPersistent. A leased workspace lives at the lease's directory
	// rather than under dir: path() answers differently, nothing else
	// changes.
	//
	// Every lease is held for the WHOLE run: the digest seeded into state
	// at run start is what every cache key mentioning the workspace is
	// computed from, and it is only honest while no other run can write to
	// the directory it describes.
	leases map[string]*persist.Lease
	// acquireEvictions is what leasing evicted, held until run.started
	// gives a ws.evicted somewhere to go; it exists so an eviction is
	// reported rather than performed silently.
	acquireEvictions []persist.Eviction
}

// newWSManager prepares one directory per declared workspace, up front, so
// a step never races another into creating the same one.
//
// Every scratch cache's key is expanded here too, once, from the process's
// working directory (a workspace may not exist yet, which would make the
// key depend on scheduling order). A key that fails to expand is a
// run-level error: guessing a substitute key would poison a shared cache
// with an entry nothing can distinguish from a correctly keyed one.
func newWSManager(
	runDir string, p *plan.Plan, snap *workspace.Snapshotter, scratchStore scratch.Cache,
	pstore *persist.Store, runID string, lockerFor func(workspace string) persist.Locker,
) (*wsManager, error) {
	m := &wsManager{
		dir:    filepath.Join(runDir, "ws"),
		snap:   snap,
		specs:  make(map[string]plan.WorkspaceSpec, len(p.Workspaces)),
		state:  make(map[string]cas.Digest, len(p.Workspaces)),
		runDir: runDir,

		scratchSpecs:  make(map[string]plan.ScratchSpec, len(p.Scratch)),
		scratchKeys:   make(map[string]string, len(p.Scratch)),
		scratchDone:   make(map[string]bool, len(p.Scratch)),
		scratchRecs:   make(map[string]scratch.Record, len(p.Scratch)),
		scratchExact:  make(map[string]bool, len(p.Scratch)),
		scratchRemote: make(map[string]bool, len(p.Scratch)),
		scratchRead:   make(map[string]string, len(p.Scratch)),
		scratchStore:  scratchStore,
	}
	for _, w := range p.Workspaces {
		m.specs[w.Name] = w
		if w.Scope == scopePersistent {
			// The lease is what makes a persistent directory safe to use.
			// Taken before the run emits anything, so a refusal (another
			// run holds it) reads like plan.Validate's own refusals: no
			// partial event stream.
			if err := m.lease(pstore, w, runID, lockerFor); err != nil {
				m.abandonPersistent()
				return nil, err
			}
			continue
		}
		if err := os.MkdirAll(m.path(w.Name), 0o755); err != nil {
			return nil, fmt.Errorf("engine: create workspace %q: %w", w.Name, err)
		}
	}
	if len(p.Scratch) > 0 {
		cwd, err := os.Getwd()
		if err != nil {
			// Getwd fails only when the working directory was removed under
			// the process. inputRoot can fall back to "."; a scratch key
			// cannot, since a key computed over the wrong root names the
			// wrong files.
			return nil, fmt.Errorf("engine: resolve scratch cache key root: %w", err)
		}
		for _, sc := range p.Scratch {
			m.scratchSpecs[sc.Name] = sc
			key, err := scratch.ExpandKey(sc.Key, cwd)
			if err != nil {
				return nil, fmt.Errorf("engine: scratch cache %q: %w", sc.Name, err)
			}
			m.scratchKeys[sc.Name] = key
		}
	}
	return m, nil
}

// scopePersistent is plan.WorkspaceSpec.Scope for a workspace that outlives
// the run. Spelled once: a scope compared against a typo is a persistent
// workspace silently treated as run-scoped.
const scopePersistent = "persistent"

// scopeStep is plan.WorkspaceSpec.Scope for a workspace realised once per
// STEP: a fresh directory nobody else mounts, discarded with the run. Spelled
// once, for the reason scopePersistent is.
const scopeStep = "step"

// path is a workspace's directory for this run. For a persistent workspace
// that is the leased directory, the same path on every run and outside the
// run directory entirely; this is the one place that decides which. A
// persistent workspace with no lease cannot happen through Run; the
// fallback to the run-scoped path lets a wsManager built directly by a test
// degrade to a run-scoped directory instead of "/".
func (m *wsManager) path(name string) string {
	if l, ok := m.leases[name]; ok {
		return l.Dir()
	}
	return filepath.Join(m.dir, name)
}

// lease takes w's persistent directory for this run, recording whatever the
// acquisition evicted.
func (m *wsManager) lease(
	pstore *persist.Store, w plan.WorkspaceSpec, runID string,
	lockerFor func(workspace string) persist.Locker,
) error {
	if pstore == nil {
		return fmt.Errorf(
			"engine: workspace %q is persistent but this run has no persistent-workspace store; "+
				"a persistent workspace needs somewhere on this machine to live between runs", w.Name)
	}
	sp := persist.Spec{
		Name:    w.Name,
		MaxAge:  time.Duration(w.MaxAgeMS) * time.Millisecond,
		MaxSize: w.MaxSizeBytes,
	}
	// A workspace whose storage an executor owns is excluded by that
	// executor, not by a file lock on this machine (see
	// k8sexec.WorkspaceLocker). Nil for every ordinary workspace.
	if lockerFor != nil {
		sp.Locker = lockerFor(w.Name)
	}
	l, err := pstore.Acquire(sp, runID)
	if err != nil {
		// Unwrapped, so a caller can errors.As it for *persist.HeldError;
		// the message already names the workspace, and an engine: prefix
		// adds nothing.
		return err
	}
	if m.leases == nil {
		m.leases = make(map[string]*persist.Lease)
	}
	m.leases[w.Name] = l
	if ev, ok := l.Eviction(); ok {
		m.acquireEvictions = append(m.acquireEvictions, ev)
	}
	return nil
}

// openPersistent measures every persistent workspace this run leased and
// makes that measurement the workspace's recorded state, so the first cache
// key that mentions it describes the bytes actually on disk.
//
// A run-scoped workspace starts empty, so its recorded state is honest by
// construction. A persistent one starts from whatever the last run left:
// leaving its state empty would make two runs whose workspaces differed
// compute identical keys, and the second would be served a result computed
// against different bytes.
//
// The cost is a full snapshot per persistent workspace per run, before the
// first step, and it is not optional: a cheaper fingerprint would be a
// second definition of the workspace's identity, and two definitions that
// disagree by one file is precisely the failure this measurement prevents.
//
// The digests are recorded through record, so the end-of-run pin protects
// them from a GC sweep exactly as it protects a step's own snapshots.
func (m *wsManager) openPersistent(ctx context.Context) ([]wsSnapshot, error) {
	names := make([]string, 0, len(m.leases))
	for name := range m.leases {
		names = append(names, name)
	}
	// Sorted, so a run's opening events arrive in the same order every
	// time.
	sort.Strings(names)

	out := make([]wsSnapshot, 0, len(names))
	for _, name := range names {
		ex, err := m.excluderForWorkspace(name)
		if err != nil {
			return nil, err
		}
		snap, err := m.snap.Snapshot(ctx, m.path(name), ex)
		if err != nil {
			return nil, fmt.Errorf("engine: measure persistent workspace %q: %w", name, err)
		}
		out = append(out, wsSnapshot{
			Name: name, Digest: snap.Digest, Index: snap.Index,
			Bytes: snap.Bytes, Files: snap.Files,
		})
	}
	m.record(out)
	return out, nil
}

// releasePersistent gives every lease back, recording what each workspace
// held so the next run can enforce MaxSize against it, and evicting the
// ones already over.
//
// Measured with workspace.Measure rather than a second Snapshot: only a
// size is needed. It uses the workspace's OWN excluder, so the number
// matches what the opening snapshot reported for the same content.
//
// Nothing here can fail a run: a workspace whose size could not be measured
// is released with size unknown, which the next acquisition does not evict
// on, so the bound is enforced a run later rather than a transient
// filesystem error turning a green build red.
func (m *wsManager) releasePersistent() []persist.Eviction {
	names := make([]string, 0, len(m.leases))
	for name := range m.leases {
		names = append(names, name)
	}
	sort.Strings(names)

	var evictions []persist.Eviction
	for _, name := range names {
		l := m.leases[name]
		var bytes int64
		if ex, err := m.excluderForWorkspace(name); err == nil {
			if n, err := workspace.Measure(l.Dir(), ex); err == nil {
				bytes = n
			}
		}
		if ev, ok, err := l.Release(bytes); err == nil && ok {
			evictions = append(evictions, ev)
		}
		delete(m.leases, name)
	}
	return evictions
}

// abandonPersistent gives every lease still held back without recording a
// use. Idempotent, so Run can defer it unconditionally after
// releasePersistent has already emptied the map. Recording a use here would
// refresh the age of a workspace nothing used, keeping a dead tree alive
// across enough refused or aborted runs.
func (m *wsManager) abandonPersistent() {
	for name, l := range m.leases {
		l.Abandon()
		delete(m.leases, name)
	}
}

// pendingEvictions is what leasing evicted, drained once. Drained rather
// than merely read so a caller cannot emit the same eviction twice.
func (m *wsManager) pendingEvictions() []persist.Eviction {
	out := m.acquireEvictions
	m.acquireEvictions = nil
	return out
}

// Where in a run an eviction happened, as ws.evicted's When field reports
// it: at acquire the last run left something too old or too large, at
// release this run itself built something too large to keep.
const (
	evictAtAcquire = "acquire"
	evictAtRelease = "release"
)

// emitEviction reports one persistent workspace being emptied. No Step: an
// eviction happens before the first step or after the last, never while one
// is reading the directory. See api.WSEvicted.
func (rc *runCore) emitEviction(ev persist.Eviction, when string) {
	rc.emit(api.Event{
		Type: api.WSEvicted,
		Payload: mustMarshal(api.WSEvictedBody{
			Name: ev.Name, Reason: ev.Reason, When: when,
			Bytes: ev.Bytes, MaxBytes: ev.Limit,
			AgeMS: ev.Age.Milliseconds(), MaxAgeMS: ev.MaxAge.Milliseconds(),
		}),
	})
}

// mounts turns a node's declared mounts into the executor's form. Scratch
// mounts are skipped here and handled separately, because their lifetime and
// their semantics are different: a scratch cache is best-effort and never an
// input to a cache key.
// pathFor is path for one STEP: the same directory for every scope but
// "step", which gets one of its own per step.
//
// Step id and not merely a counter, so a directory left behind by a failed
// run says which step owned it, and so a handler inheriting its parent's
// mounts lands in the parent's tree rather than a new one.
func (m *wsManager) pathFor(name, stepID string) string {
	if m.specs[name].Scope != scopeStep {
		return m.path(name)
	}
	return filepath.Join(m.dir, name, stepDirName(stepID))
}

// stepDirName turns a step id into one path segment: ids are hierarchical and
// contain "/", which would otherwise make a step-scoped workspace a tree
// shaped like the pipeline.
func stepDirName(id string) string {
	return strings.ReplaceAll(strings.ReplaceAll(id, "/", "_"), string(filepath.Separator), "_")
}

func (m *wsManager) mounts(n *plan.Node) ([]executor.Mount, error) {
	var out []executor.Mount
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, ms := range n.Mounts {
		if ms.Workspace == "" {
			continue
		}
		spec, ok := m.specs[ms.Workspace]
		if !ok {
			return nil, fmt.Errorf("engine: step %q mounts unknown workspace %q", n.ID, ms.Workspace)
		}
		path := m.pathFor(ms.Workspace, n.ID)
		if spec.Scope == scopeStep {
			// Created here rather than at run start: there is one per step,
			// and the set of steps is not known up front once a generator can
			// add some.
			if err := os.MkdirAll(path, 0o755); err != nil {
				return nil, fmt.Errorf("engine: create step workspace %q for %q: %w",
					ms.Workspace, n.ID, err)
			}
		}
		out = append(out, executor.Mount{
			Name: ms.Workspace,
			// A step-scoped workspace carries no shared digest: m.state is
			// keyed by NAME, and one step's tree is not another's.
			Digest:           stepScopedDigest(m, spec, ms.Workspace),
			Path:             path,
			At:               ms.At,
			RO:               ms.Mode == "ro",
			Exclude:          spec.Exclude,
			PreserveSymlinks: spec.PreserveSymlinks,
			// Empty unless this node's target backs the workspace with a
			// claim (Kubernetes only). Read off the node's own executor
			// spec: two workflows can target two clusters and only one may
			// have the claim.
			Claim: claimFor(n, ms.Workspace),
		})
	}
	return out, nil
}

// claimFor reports the PersistentVolumeClaim this node's target backs the
// named workspace with, or "" when it backs it with nothing, which is every
// executor other than Kubernetes and the ordinary case even there.
func claimFor(n *plan.Node, workspace string) string {
	if n == nil || n.Executor == nil {
		return ""
	}
	return n.Executor.Claims[workspace]
}

// handlerMounts is what a handler of parent inherits: parent's OWN
// workspace mounts, at the same sandbox paths, carrying whatever the parent
// left in them, and read-only.
//
// It lives here rather than at the executor seam on purpose: by handler
// time the parent's sandbox is gone, so nothing an executor derives can be
// inheritance (the container executor's Close removes the container, and a
// fresh one inherits nothing). A MOUNT is a declaration the coordinator
// makes about a directory it owns, and that is the only thing that
// survives.
//
// It is m.mounts, not a second walk over parent.Mounts, so "the handler saw
// what the step saw" is a property of the code rather than of two walks
// staying in step. That reuse also means Digest carries the CURRENT
// recorded state (see record): what the step left behind.
//
// Read-only, unconditionally: the parent's ws.snapshot was taken before any
// handler starts, so a handler that writes moves bytes the ledger's digest,
// and every cache key computed from it, already claims to describe. There
// is no second snapshot after handlers; adding one would let a diagnostic
// script change what the run says its steps produced. On the local executor
// RO is a statement of intent a determined handler can still violate,
// exactly as for a step's own read-only mount (see executor.Mount.RO).
//
// Scratch caches are deliberately not inherited: they are best-effort
// caches rather than evidence, and saveScratch stores them at run end from
// the same shared directory, so a live handle would put a cleanup script
// inside something a later run reads.
//
// The caller must hold lockMounts for parent's workspace names across the
// handler's whole run: a handler is a reader like any other, and a sibling
// step's cache hit restoring into one is still a RemoveAll then a Rename.
func (m *wsManager) handlerMounts(parent *plan.Node) ([]executor.Mount, error) {
	mounts, err := m.mounts(parent)
	if err != nil {
		return nil, err
	}
	for i := range mounts {
		mounts[i].RO = true
	}
	return mounts, nil
}

// capturableWorkspaces is every workspace name n mounts that this
// coordinator can capture on demand, sorted and deduplicated by
// workspaceMountNames, minus the claim-backed ones.
//
// A claim-backed workspace lives in the cluster with no coordinator-side
// copy to walk, which is exactly why snapshotMounts skips it at settle time
// too; a snapshot of the empty directory standing in for it here would be a
// confident wrong answer rather than a missing one.
func capturableWorkspaces(n *plan.Node) []string {
	names := workspaceMountNames(n)
	out := make([]string, 0, len(names))
	for _, name := range names {
		if claimFor(n, name) == "" {
			out = append(out, name)
		}
	}
	return out
}

// forceSnapshot captures every workspace n mounts, right now, for somebody
// to look at. It is the body of the ws.snapshot control operation.
//
// Coordinator-side, through the same Snapshotter openPersistent measures a
// leased directory with, and deliberately NOT through a sandbox: the
// operation is answerable only for a step that has not been dispatched, so
// on every executor there is no sandbox of that step's to ask. That also
// makes the answer exact rather than approximate on a target sharing no
// filesystem with the coordinator: k8s and ssh stage THIS directory into
// the pod or onto the host when the step starts (see their transfer.go and
// mountxfer), so what is here is what that step will see. The one mount
// whose content lives on the target and was never staged from here is the
// claim-backed one, which capturableWorkspaces excludes.
//
// Nothing here calls record, and that is the whole guarantee that a forced
// snapshot cannot become evidence: record is the ONLY writer of m.state,
// the map digests() turns into a cache key's workspaceDigests component and
// mounts() turns into executor.Mount.Digest. Skipping it means this capture
// cannot enter a key, cannot replace what the step's own settle-time
// snapshot will record, and cannot reach the plan, which was fixed before
// the run began. The digests are still pinned (pinForced), or a GC sweep
// could collect the very objects the operator asked for.
//
// The hold is SHARED, like a handler's and a session's: it excludes a
// sibling's cache-hit restore, the RemoveAll-then-Rename that would make
// this read explode rather than merely age, and nothing else. A sibling
// step writing the same ScopeRun workspace is the last-writer-wins rule
// every mount of one already lives under.
func (m *wsManager) forceSnapshot(ctx context.Context, n *plan.Node) ([]wsSnapshot, error) {
	names := capturableWorkspaces(n)
	unlock := m.lockMounts(names)
	defer unlock()

	out := make([]wsSnapshot, 0, len(names))
	for _, name := range names {
		ex, err := m.excluderForWorkspace(name)
		if err != nil {
			return nil, err
		}
		snap, err := m.snap.Snapshot(ctx, m.path(name), ex)
		if err != nil {
			return nil, fmt.Errorf("engine: step %q: force-snapshot workspace %q: %w", n.ID, name, err)
		}
		out = append(out, wsSnapshot{
			Name: name, Digest: snap.Digest, Index: snap.Index,
			Bytes: snap.Bytes, Files: snap.Files,
		})
	}
	m.pinForced(out)
	return out, nil
}

// pinForced records a forced snapshot's digests for the end-of-run pin and
// NOTHING else, unlike record, which also makes them the workspace's
// current state. Without it the objects a forced capture created would be
// unreferenced, and the next sweep could collect the snapshot an operator
// asked for before they pulled it.
func (m *wsManager) pinForced(snaps []wsSnapshot) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, s := range snaps {
		m.produced = append(m.produced, s.Digest, s.Index)
	}
}

// record stores the digests an attempt produced, so a later step's cache key
// and a later mount's Digest field see them.
func (m *wsManager) record(snaps []wsSnapshot) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, s := range snaps {
		m.state[s.Name] = s.Digest
		m.produced = append(m.produced, s.Digest, s.Index)
	}
}

// allSnapshotDigests is every digest this run's snapshots produced, body
// and index alike, in a deterministic order, deduplicated for the pin that
// reads it.
func (m *wsManager) allSnapshotDigests() []cas.Digest {
	m.mu.Lock()
	defer m.mu.Unlock()
	seen := make(map[cas.Digest]bool, len(m.produced))
	out := make([]cas.Digest, 0, len(m.produced))
	for _, d := range m.produced {
		if d == "" || seen[d] {
			continue
		}
		seen[d] = true
		out = append(out, d)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// wsLock returns the RWMutex guarding name's directory, creating it on
// first use. One mutex per workspace name for the run's lifetime, which is
// why it lives on m rather than being constructed per call.
func (m *wsManager) wsLock(name string) *sync.RWMutex {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.wsLocks == nil {
		m.wsLocks = make(map[string]*sync.RWMutex)
	}
	l, ok := m.wsLocks[name]
	if !ok {
		l = &sync.RWMutex{}
		m.wsLocks[name] = l
	}
	return l
}

// lockMounts takes a SHARED hold on every workspace in names, in the order
// given (workspaceMountNames sorts it, so steps mounting overlapping sets
// always acquire in the same order and cannot deadlock), and returns a func
// releasing every hold.
//
// This is what a step's own use of a workspace takes. Multiple steps can
// hold it at once for one workspace: that is the intentional concurrency a
// ScopeRun workspace already has. It excludes exactly one thing: a
// concurrent lockRestore for the same name, the destructive
// RemoveAll-then-Rename a cache hit performs. Hold it for the shortest span
// that touches the directory's content, never across a retry's backoff
// sleep, which is why callers each acquire and release it themselves.
//
// A handler IS such a span and takes its own hold (see execHandler): since
// handlers inherit their parent's mounts (handlerMounts), a handler reading
// the failed step's workspace is a reader like any other.
func (m *wsManager) lockMounts(names []string) func() {
	locks := make([]*sync.RWMutex, len(names))
	for i, name := range names {
		locks[i] = m.wsLock(name)
	}
	for _, l := range locks {
		l.RLock()
	}
	return func() {
		for _, l := range locks {
			l.RUnlock()
		}
	}
}

// lockRestore takes an EXCLUSIVE hold on every workspace in names (in
// practice one; a slice for symmetry with lockMounts and so a future
// multi-workspace restore cannot deadlock against itself).
//
// While held, no lockMounts call for the same name can proceed, and it
// cannot be acquired while any shared hold on the name is outstanding: the
// moment between RemoveAll and Rename in workspace.Snapshotter.Restore,
// when the directory is briefly gone, provably has no other goroutine
// inside it.
//
// Two steps whose cache hits both restore the same workspace serialize
// through this lock; which runs first is undefined and the last writer
// wins, the same rule two ordinary uncached siblings already live under.
//
// Cost: a restore waits for the slowest step currently using the workspace,
// and new steps wanting the SAME workspace queue behind a waiting restore.
// The exclusion is per workspace name, not per run.
func (m *wsManager) lockRestore(names []string) func() {
	locks := make([]*sync.RWMutex, len(names))
	for i, name := range names {
		locks[i] = m.wsLock(name)
	}
	for _, l := range locks {
		l.Lock()
	}
	return func() {
		for _, l := range locks {
			l.Unlock()
		}
	}
}

// workspaceMountNames is every distinct workspace name n mounts, sorted and
// deduplicated: sorted so overlapping sets always lock in the same order
// (see lockMounts), deduplicated so a node mounting one workspace at two
// paths cannot lock its own mutex twice and deadlock against itself.
func workspaceMountNames(n *plan.Node) []string {
	seen := make(map[string]bool, len(n.Mounts))
	names := make([]string, 0, len(n.Mounts))
	for _, ms := range n.Mounts {
		if ms.Workspace == "" || seen[ms.Workspace] {
			continue
		}
		seen[ms.Workspace] = true
		names = append(names, ms.Workspace)
	}
	sort.Strings(names)
	return names
}

// digests reports the current digest of every workspace a node mounts, in
// the node's declared order. This is the workspaceDigests component of the
// cache key, read BEFORE the step runs.
func (m *wsManager) digests(n *plan.Node) []wsSnapshot {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []wsSnapshot
	for _, ms := range n.Mounts {
		if ms.Workspace == "" {
			continue
		}
		out = append(out, wsSnapshot{
			Name: ms.Workspace, Digest: m.state[ms.Workspace], RO: ms.Mode == "ro",
		})
	}
	return out
}

// restore materializes a digest into a workspace's directory, replacing
// what is there, and records it as the workspace's current state.
//
// Coordinator-side, because a cache hit skips the step and there is no
// sandbox to ask. The coordinator holds the canonical copy whatever the
// executor: k8s and ssh stage whatever this function last put on disk (see
// their transfer.go), and executor.Mount.Digest lets an executor skip a
// transfer it already has.
//
// The caller MUST already hold lockRestore(name): not taken here, so a
// caller that also places restored output files (see serveFromCache) can
// cover both under one continuous exclusive hold; RWMutex's Lock is not
// reentrant anyway.
func (m *wsManager) restore(ctx context.Context, name string, d cas.Digest) error {
	if _, ok := m.specs[name]; !ok {
		return fmt.Errorf("engine: cannot restore unknown workspace %q", name)
	}
	if err := m.snap.Restore(ctx, d, m.path(name)); err != nil {
		return fmt.Errorf("engine: restore workspace %q: %w", name, err)
	}
	m.mu.Lock()
	m.state[name] = d
	m.mu.Unlock()
	return nil
}

// inputWorkspace is inputRoot's decision, one level short of a path: which
// declared workspace (if any) a node's Inputs and Outputs resolve against.
// It is plan.Node.InputWorkspace, not a walk of its own: `senro verify`
// needs the identical answer for a node reconstituted outside any run, and
// two walks would only have to drift once.
func (m *wsManager) inputWorkspace(n *plan.Node) (name string, ok bool) {
	return n.InputWorkspace()
}

// inputRoot is the directory a node's declared Inputs and Outputs resolve
// against. plan.Validate has already refused every ambiguous case, so this
// resolves rather than guesses, mirroring plan.mountsAtWorkDir exactly: the
// mount at the step's WorkDir wins, then the single mounted workspace, then
// the coordinator's working directory (which can carry Inputs only;
// Validate refuses Outputs there).
func (m *wsManager) inputRoot(n *plan.Node) string {
	if name, ok := m.inputWorkspace(n); ok {
		return m.pathFor(name, n.ID)
	}
	cwd, err := os.Getwd()
	if err != nil {
		// Getwd fails only when the working directory was removed under the
		// process; "." is the same directory by definition.
		return "."
	}
	return cwd
}

// excluderFor is what "part of this workspace" means for n's Inputs and
// Outputs, deliberately the SAME excluder localexec's Snapshot builds for
// the identical mount. It has to stay in sync with the snapshot's excluder:
// if cache.Resolve applied only the two mandatory defaults instead, a
// workspace excluding dist/ from its snapshot would still have dist/ hashed
// into input_digests, two views of the workspace disagreeing invisibly, and
// the step would miss every run.
//
// Falls back to nil (Resolve's own mandatory-defaults fallback) when n
// resolves against no declared workspace: the coordinator's working
// directory has no WorkspaceSpec to read from.
func (m *wsManager) excluderFor(n *plan.Node) (*workspace.Excluder, error) {
	name, ok := m.inputWorkspace(n)
	if !ok {
		return nil, nil
	}
	ex, err := m.excluderForWorkspace(name)
	if err != nil {
		return nil, fmt.Errorf("engine: step %q: %w", n.ID, err)
	}
	return ex, nil
}

// excluderForWorkspace is excluderFor without a node, for the two callers
// with no step to name (a persistent workspace's opening measurement and
// its closing MaxSize check): a separate excluder for either would enforce
// the bound over a different file set than the digest describes.
func (m *wsManager) excluderForWorkspace(name string) (*workspace.Excluder, error) {
	spec, ok := m.specs[name]
	if !ok {
		return nil, fmt.Errorf("engine: unknown workspace %q", name)
	}
	ex, err := workspace.ExcluderFor(m.path(name), spec.Exclude, spec.PreserveSymlinks)
	if err != nil {
		return nil, fmt.Errorf("engine: workspace %q: %w", name, err)
	}
	return ex, nil
}

// planNeedsStorage reports whether p declares anything that cannot work
// without a store. Run refuses such a plan when Options.Storage is nil,
// rather than executing it with every workspace and every cache silently
// absent, which would look exactly like a working run.
func planNeedsStorage(p *plan.Plan) bool {
	if len(p.Workspaces) > 0 || len(p.Scratch) > 0 {
		return true
	}
	for i := range p.Nodes {
		if p.Nodes[i].Pure {
			return true
		}
	}
	return false
}

// scratchPath is a scratch cache's directory for this run.
func (m *wsManager) scratchPath(name string) string {
	return filepath.Join(m.runDir, "scratch", name)
}

// scratchMounts realizes a node's scratch caches, restoring each one the
// first time any step in the run mounts it. Once per run, not per step: a
// scratch cache is one directory shared by every step mounting it, and
// restoring again mid-run would throw away what an earlier step put there.
//
// A node whose target does not share the coordinator's filesystem marks
// every cache it mounts, HERE, before its sandbox exists: from this moment
// the coordinator's directory is only what was sent out, and saveScratch
// must not store it. Marking at read-back time would be too late for a step
// that never got that far.
func (m *wsManager) scratchMounts(ctx context.Context, n *plan.Node) ([]executor.Mount, error) {
	var out []executor.Mount
	remote := n.RemoteMounts()
	for _, ms := range n.Mounts {
		if ms.Scratch == "" {
			continue
		}
		spec, ok := m.scratchSpecs[ms.Scratch]
		if !ok {
			return nil, fmt.Errorf("engine: step %q mounts unknown scratch cache %q", n.ID, ms.Scratch)
		}
		dir := m.scratchPath(ms.Scratch)
		if err := m.ensureScratch(ctx, spec, dir); err != nil {
			return nil, err
		}
		if remote {
			m.mu.Lock()
			m.scratchRemote[ms.Scratch] = true
			m.mu.Unlock()
		}
		out = append(out, executor.Mount{Name: ms.Scratch, Path: dir, At: ms.At, Scratch: true})
	}
	return out, nil
}

// readScratch brings back what a remote step actually left in each scratch
// cache it mounted, so the run stores those bytes rather than the
// coordinator's own copy of what it sent out.
//
// The read-back is Sandbox.Snapshot's, minus the two halves a workspace
// needs and a scratch cache must not have: no digest, because a scratch
// cache is never evidence and never a cache key input, and no replacement of
// the coordinator's directory, because a sibling step may be tarring that
// directory out to its own target at this very moment. What came back is
// kept ASIDE and saveScratch stores it; the newest one wins, which is the
// same last-writer rule two steps sharing any mount already live under.
//
// The cost is a second full transfer of the cache per step, on top of the one
// that sent it out, and on Kubernetes every byte crosses the SHARED
// apiserver both ways: a dependency tree big enough to be worth caching is
// often big enough for this to cost more than the download it saves. Not
// suppressed by NoSnapshot, which is about a workspace's digest.
//
// A read-back that fails leaves scratchRead empty, which is exactly what
// stops saveScratch storing anything for that cache: a miss costs time, and
// a wrong entry costs every later run.
func (m *wsManager) readScratch(ctx context.Context, sb executor.Sandbox, mounts []executor.Mount) {
	r, ok := sb.(executor.MountReader)
	if !ok {
		// Every executor that shares the coordinator's filesystem, where the
		// step wrote the shared directory itself and there is nothing to
		// fetch. A sandbox from a plan.Node.RemoteMounts target that did not
		// implement it would save nothing, which is the safe half of the
		// wrong answer.
		return
	}
	for _, mt := range mounts {
		// The cache's own name is deliberately not in the directory name: it
		// is an arbitrary Go string, this is a path, and the run directory
		// already holds one directory per cache under its real name.
		dest, err := os.MkdirTemp(filepath.Join(m.runDir, "scratch"), ".senro-read-")
		if err != nil {
			continue
		}
		if err := r.ReadMount(ctx, mt.Name, dest); err != nil {
			// Swallowed, like every other scratch failure: a correct step is
			// not made incorrect by a cache that could not be read back, and
			// the record says the save was skipped.
			_ = os.RemoveAll(dest)
			continue
		}
		m.mu.Lock()
		if prev := m.scratchRead[mt.Name]; prev != "" {
			_ = os.RemoveAll(prev)
		}
		m.scratchRead[mt.Name] = dest
		m.mu.Unlock()
	}
}

// ensureScratch restores a scratch cache once, and never fails a step
// because of it: a miss, a swept entry, or a restore that could not complete
// all leave an empty directory the step repopulates itself. That is what
// "best effort" means.
func (m *wsManager) ensureScratch(ctx context.Context, spec plan.ScratchSpec, dir string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.scratchDone[spec.Name] {
		return nil
	}
	m.scratchDone[spec.Name] = true

	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("engine: create scratch cache %q: %w", spec.Name, err)
	}
	key := m.scratchKeys[spec.Name]
	rec := scratch.Record{Name: spec.Name, Key: key}
	match, ok, err := m.scratchStore.Restore(ctx, key, spec.RestoreKeys, dir)
	if err == nil && ok {
		rec.Restored = true
		rec.RestoredFrom = match.Key
		// match.Exact is true only when the restore hit the run's own key,
		// not a restore-keys prefix; see saveScratch.
		m.scratchExact[spec.Name] = match.Exact
	}
	// A restore error is recorded and swallowed. The alternative is a
	// pipeline that cannot run because a module cache was unreadable.
	m.scratchRecs[spec.Name] = rec
	return nil
}

// saveScratch stores every scratch cache the run touched, once, at run end.
//
// Gated on the RUN's outcome, not each step's: the directory is shared by
// every step that mounts it, so per-step saves would race inside one run
// and store intermediate states under a key that names none of them.
//
// The per-cache condition is "only when the run's own key was not already
// an EXACT hit", not "only when nothing was restored": a restore-keys
// fallback restores SOME entry, but under an older key, and skipping the
// save then would mean a changing scratch cache never converges, every
// later run falling back to the same aging prefix match forever. See
// TestAScratchCacheSavesAFreshEntryAfterARestoreKeyFallback.
//
// A cache any step on a machine of its own mounted is saved from what
// readScratch brought BACK, or not at all: see the branch below.
func (m *wsManager) saveScratch(ctx context.Context, runDir string, succeeded bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	recs := make([]scratch.Record, 0, len(m.scratchRecs))
	for name, rec := range m.scratchRecs {
		if succeeded && !m.scratchExact[name] {
			// Nothing to store when the exact key was already there: entries
			// are immutable, so a save would lose the race with itself.
			dir := filepath.Join(runDir, "scratch", name)
			if m.scratchRemote[name] {
				// A step on a machine of its own mounted this cache, so the
				// coordinator's directory is only what was SENT OUT. What
				// came back is the only honest source, and when nothing did
				// this run stores nothing: the entry would be written once
				// and never rewritten, so a stale copy under this key is
				// what every later run would be served.
				dir = m.scratchRead[name]
				if dir == "" {
					rec.Unread = true
					recs = append(recs, rec)
					continue
				}
			}
			if saved, err := m.scratchStore.Save(ctx, rec.Key, dir); err == nil {
				rec.Saved = saved
			}
			// A save error, like a restore error, is swallowed: a correct
			// step is not made incorrect by a cache this run could not
			// persist. rec.Saved stays false, which `senro cache explain`
			// reports honestly.
		}
		recs = append(recs, rec)
	}
	// The read-back copies are this function's inputs and nothing else's:
	// removed here rather than left doubling the run directory's size.
	for name, dir := range m.scratchRead {
		_ = os.RemoveAll(dir)
		delete(m.scratchRead, name)
	}
	if err := scratch.WriteRecords(filepath.Join(runDir, "cache"), recs); err != nil {
		// A record is a diagnostic; losing one must not change a run's
		// outcome.
		_ = err
	}
}

// snapshotMounts captures every workspace a node mounted, emits one
// ws.snapshot per workspace, and reports a read-only mount whose content
// changed.
//
// Read-only mounts are snapshotted too, and that is the point: the local
// executor cannot enforce read-only, and a mutated read-only input leaves a
// digest that does not describe what the step saw, making every later cache
// key silently wrong. One extra snapshot turns that into a step failure
// naming the workspace.
func (rc *runCore) snapshotMounts(
	ctx context.Context, sb executor.Sandbox, n *plan.Node, mounts []executor.Mount, attempt int,
) ([]wsSnapshot, error) {
	if rc.ws == nil || n.NoSnapshot || len(mounts) == 0 {
		return nil, nil
	}
	out := make([]wsSnapshot, 0, len(mounts))
	var violation error
	for _, mt := range mounts {
		if mt.Claim != "" {
			// A claim-backed workspace has no coordinator-side copy to
			// walk: nothing to capture, no digest that would be true, so no
			// ws.snapshot, and the read-only breach check (a before/after
			// digest comparison) is skipped with it. Nothing downstream
			// needs the digest: data reaches the next step through the
			// claim, and plan.Validate refuses Pure() on a step mounting
			// one.
			continue
		}
		if rc.ws.specs[mt.Name].Scope == scopeStep {
			// A step-scoped tree has no reader: it belongs to this step and
			// is discarded with the run, so there is no later step to hand a
			// digest to and nothing a cache entry could usefully restore.
			// Skipped for the reason a claim-backed workspace is: no digest
			// here would be a fact anyone uses.
			continue
		}
		snap, err := sb.Snapshot(ctx, mt.Name)
		if err != nil {
			return out, fmt.Errorf("engine: step %q: %w", n.ID, err)
		}
		s := wsSnapshot{
			Name: mt.Name, Digest: cas.Digest(snap.Digest), Index: cas.Digest(snap.Index),
			Bytes: snap.Bytes, Files: snap.Files, RO: mt.RO,
		}
		out = append(out, s)
		rc.emit(api.Event{
			Type: api.WSSnapshot, Step: n.ID, Attempt: attempt,
			Payload: mustMarshal(api.WSSnapshotBody{
				Name: s.Name, Digest: string(s.Digest), Index: string(s.Index),
				Bytes: s.Bytes, Files: s.Files,
			}),
		})
		if mt.RO && mt.Digest != "" && mt.Digest != snap.Digest && violation == nil {
			violation = fmt.Errorf(
				"engine: step %q wrote through its read-only mount of workspace %q (%s became %s); "+
					"a read-only mount that changes makes every cache key computed from it wrong",
				n.ID, mt.Name, cas.Digest(mt.Digest).Short(), cas.Digest(snap.Digest).Short())
		}
	}
	return out, violation
}

// workspaceLocker is implemented by an executor that owns the storage
// behind a workspace, and therefore has to own its exclusion too. An
// asserted interface rather than an executor.Executor method: exactly one
// executor implements it, and only for claim-backed workspaces.
type workspaceLocker interface {
	WorkspaceLocker(workspace string) persist.Locker
}

// lockerFor builds the lookup newWSManager uses to decide how each
// persistent workspace is excluded: it asks every executor and takes the
// first non-nil answer. Two executors claiming one workspace is a pipeline
// saying two contradictory things senro cannot reconcile here, so the first
// wins and the conflict stays visible in the plan. Returns nil when nothing
// implements the interface, so the ordinary run allocates nothing.
func lockerFor(executors map[string]executor.Executor) func(string) persist.Locker {
	var owners []workspaceLocker
	for _, ex := range executors {
		if wl, ok := ex.(workspaceLocker); ok {
			owners = append(owners, wl)
		}
	}
	if len(owners) == 0 {
		return nil
	}
	return func(workspace string) persist.Locker {
		for _, o := range owners {
			if l := o.WorkspaceLocker(workspace); l != nil {
				return l
			}
		}
		return nil
	}
}

// stepScopedDigest is the digest a mount carries: none for a step-scoped
// workspace, whose tree is its step's alone, and the run's shared state for
// every other scope.
func stepScopedDigest(m *wsManager, spec plan.WorkspaceSpec, name string) string {
	if spec.Scope == scopeStep {
		return ""
	}
	return string(m.state[name])
}
