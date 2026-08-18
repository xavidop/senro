// Package persist owns the directories a ScopePersistent workspace lives in
// between runs: one tree per workspace name, on this machine, leased to one
// run at a time and bounded by an age and a size.
//
// Deliberately not internal/scratch: a scratch cache is content-addressed
// and immutable, while a persistent workspace is a MUTABLE directory
// identified by name, and an input to a cache key, which a scratch cache is
// forbidden from being. The key includes the tree's digest measured at run
// start (see engine.wsManager), which is only sound while the tree cannot
// change under the run that measured it; the lease makes that true.
//
// Acquire takes an exclusive advisory lock, held for the whole run; a
// second run is REFUSED immediately, naming the holder. Not a wait: the
// lock spans somebody else's entire pipeline, an unbounded duration no
// timeout could be chosen for. Not a private copy per run either: that is a
// ScopeRun workspace with extra steps, ending in a cold cache or silent
// overwrites. The kernel releases the lock when the holder dies, so there
// is no stale-claim heuristic to need.
//
// MaxAge is checked at lease time. MaxSize is checked at release, and again
// at the next acquisition against the last recorded size, the only check a
// SIGKILLed run cannot skip. Nothing is evicted mid-run: a vanished
// dependency cache is worse than one run's disk overshoot. An eviction
// removes the tree WHOLE; half a node_modules is not a smaller one.
package persist

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"net/url"
	"os"
	"path/filepath"
	"time"
)

// The two reasons a workspace is evicted, as they appear in the ws.evicted
// event. Constants because the engine emits them and a test asserts on them.
const (
	// ReasonMaxAge is a workspace that went unused for longer than its
	// MaxAge.
	ReasonMaxAge = "max_age"
	// ReasonMaxSize is a workspace whose content grew past its MaxSize.
	ReasonMaxSize = "max_size"
)

// Spec is one persistent workspace as its pipeline declared it. Both bounds
// are mandatory upstream (plan.Validate), so a zero or negative bound here
// means "never evict for that reason" rather than a re-derived refusal.
type Spec struct {
	// Locker overrides how this ONE workspace is excluded; nil means the
	// store's advisory file lock, right whenever the tree is local. Per
	// workspace rather than per store because one run can hold both kinds
	// at once (a PVC build cache and an on-disk workspace).
	Locker Locker

	Name    string
	MaxAge  time.Duration
	MaxSize int64
}

// Eviction is one workspace being emptied, and why.
type Eviction struct {
	// Name is the workspace's declared name, not its directory.
	Name string
	// Reason is ReasonMaxAge or ReasonMaxSize.
	Reason string
	// Bytes is what the workspace held, and Limit the MaxSize it was
	// measured against. Both zero for a ReasonMaxAge eviction.
	Bytes int64
	Limit int64
	// Age is how long the workspace had gone unused, and MaxAge the bound it
	// was measured against. Both zero for a ReasonMaxSize eviction.
	Age    time.Duration
	MaxAge time.Duration
}

// HeldError is Acquire's refusal: another run holds this workspace.
//
// It carries the holder's identity because the next question is always
// "held by what". The fields come from a file the holder wrote after taking
// the lock, so they can be one acquisition stale; the refusal itself comes
// from the lock, never from this file.
type HeldError struct {
	Name  string
	RunID string
	PID   int
	Since time.Time
}

func (e *HeldError) Error() string {
	who := "another run"
	if e.RunID != "" {
		who = fmt.Sprintf("run %s (pid %d, since %s)", e.RunID, e.PID, e.Since.Format(time.RFC3339))
	}
	return fmt.Sprintf(
		"persist: persistent workspace %q is held by %s; one run at a time may use a persistent "+
			"workspace, because two runs writing one mutable tree produce a tree neither of them "+
			"describes and every cache key computed from it is then wrong. Wait for that run to "+
			"finish, or give this pipeline a persistent workspace of its own name",
		e.Name, who)
}

// Store is an opened persistent-workspace root.
type Store struct {
	root string
	// lock is how one run becomes the holder. Nil means the default advisory
	// file lock beside the tree; see Locker and WithLocker.
	lock Locker
}

// Open prepares root.
func Open(root string, opts ...Option) (*Store, error) {
	if err := os.MkdirAll(root, 0o755); err != nil {
		return nil, fmt.Errorf("persist: open %s: %w", root, err)
	}
	s := &Store{root: root}
	for _, o := range opts {
		o(s)
	}
	if s.lock == nil {
		s.lock = &fileLocker{store: s}
	}
	return s, nil
}

// Option configures Open.
type Option func(*Store)

// WithLocker replaces the exclusion, for a workspace in a
// PersistentVolumeClaim: two coordinators can reach that tree and a file
// lock on one of them excludes nothing. See internal/persist/kubelock.
func WithLocker(l Locker) Option { return func(s *Store) { s.lock = l } }

// slot is everything belonging to one workspace name, in one directory. The
// name is percent-encoded into a single path segment (as internal/scratch
// encodes its keys): trusting a name containing "/" as a filename is how
// two distinct workspaces quietly become one.
func (s *Store) slot(name string) string {
	return filepath.Join(s.root, url.PathEscape(name))
}

// treePath is the workspace's content directory: a subdirectory of the slot
// so the lock file, metadata and holder record sit beside the content, not
// inside it, where a snapshot would fold them into the digest.
func (s *Store) treePath(name string) string { return filepath.Join(s.slot(name), "tree") }

func (s *Store) lockPath(name string) string   { return filepath.Join(s.slot(name), "lock") }
func (s *Store) metaPath(name string) string   { return filepath.Join(s.slot(name), "meta.json") }
func (s *Store) holderPath(name string) string { return filepath.Join(s.slot(name), "holder.json") }

// meta is what one run tells the next about a workspace.
type meta struct {
	// LastUsed is when a run last RELEASED this workspace: use refreshes the
	// age, so a nightly build never ages its tree out.
	LastUsed time.Time `json:"last_used"`
	// Bytes is what that release measured: the only size the next
	// acquisition can check, which is the check a SIGKILLed run cannot skip.
	Bytes int64 `json:"bytes"`
	// RunID is the run that wrote this record, for a human reading the
	// directory.
	RunID string `json:"run_id,omitempty"`
}

type holder struct {
	RunID string    `json:"run_id"`
	PID   int       `json:"pid"`
	Since time.Time `json:"since"`
}

// Lease is one run's exclusive hold on one persistent workspace. It is
// released exactly once, by Release or by Abandon.
type Lease struct {
	store *Store
	spec  Spec
	lock  Unlocker

	// evicted is what Acquire did before handing this lease over, if
	// anything; reported by Eviction rather than returned from Acquire.
	evicted Eviction
	didEvit bool
}

// Acquire leases name for runID, enforcing MaxAge and the recorded size
// before handing back a directory.
//
// Lock first, then check, then evict: checking before locking would let two
// runs make the same eviction decision and the loser delete a tree the
// winner had started using.
func (s *Store) Acquire(sp Spec, runID string) (*Lease, error) {
	slot := s.slot(sp.Name)
	if err := os.MkdirAll(slot, 0o755); err != nil {
		return nil, fmt.Errorf("persist: prepare workspace %q: %w", sp.Name, err)
	}
	// The exclusion comes first; everything below assumes it holds.
	lock := sp.Locker
	if lock == nil {
		lock = s.lock
	}
	held, err := lock.TryAcquire(context.Background(), sp.Name, runID)
	if err != nil {
		return nil, err
	}

	l := &Lease{store: s, spec: sp, lock: held}
	if err := os.MkdirAll(s.treePath(sp.Name), 0o755); err != nil {
		l.Abandon()
		return nil, fmt.Errorf("persist: prepare workspace %q: %w", sp.Name, err)
	}
	if err := s.writeHolder(sp.Name, holder{RunID: runID, PID: os.Getpid(), Since: time.Now().UTC()}); err != nil {
		l.Abandon()
		return nil, err
	}

	m, hasMeta, err := s.readMeta(sp.Name)
	if err != nil {
		l.Abandon()
		return nil, err
	}
	// No record means nothing has finished using it yet (first run, or a
	// killed predecessor). Not evictable: nothing to measure against.
	if hasMeta {
		if age := time.Since(m.LastUsed); sp.MaxAge > 0 && age > sp.MaxAge {
			if err := l.evict(Eviction{
				Name: sp.Name, Reason: ReasonMaxAge, Age: age, MaxAge: sp.MaxAge,
			}); err != nil {
				l.Abandon()
				return nil, err
			}
		} else if sp.MaxSize > 0 && m.Bytes > sp.MaxSize {
			if err := l.evict(Eviction{
				Name: sp.Name, Reason: ReasonMaxSize, Bytes: m.Bytes, Limit: sp.MaxSize,
			}); err != nil {
				l.Abandon()
				return nil, err
			}
		}
	}
	return l, nil
}

// heldBy builds the refusal from whatever the holder recorded about itself.
// An unreadable or absent holder record is not an error: the lock already
// decided the exclusion, and a refusal naming nobody is still correct.
func (s *Store) heldBy(name string) error {
	e := &HeldError{Name: name}
	b, err := os.ReadFile(s.holderPath(name))
	if err != nil {
		return e
	}
	var h holder
	if err := json.Unmarshal(b, &h); err != nil {
		return e
	}
	e.RunID, e.PID, e.Since = h.RunID, h.PID, h.Since
	return e
}

// Dir is the directory this run mounts: the same path across runs, which is
// what lets the engine hand it straight to an executor with no copy.
func (l *Lease) Dir() string { return l.store.treePath(l.spec.Name) }

// Eviction reports what Acquire emptied before handing this lease over, if
// anything.
func (l *Lease) Eviction() (Eviction, bool) { return l.evicted, l.didEvit }

// Release records this run's use of the workspace, enforces MaxSize against
// the size the caller measured, and gives the lease up.
//
// bytes is measured by the CALLER: the workspace's own excludes define the
// one correct size (workspace.Measure), and a second walk here would
// enforce the bound against a different set of files than the digest
// describes. The lease is given up even on failure: a run that could not
// write its metadata must not hold a workspace nobody can take.
func (l *Lease) Release(bytes int64) (Eviction, bool, error) {
	defer l.Abandon()

	var ev Eviction
	var evicted bool
	if l.spec.MaxSize > 0 && bytes > l.spec.MaxSize {
		ev = Eviction{Name: l.spec.Name, Reason: ReasonMaxSize, Bytes: bytes, Limit: l.spec.MaxSize}
		if err := l.evict(ev); err != nil {
			return Eviction{}, false, err
		}
		evicted = true
		// Recording the pre-eviction size would make every future acquisition
		// evict an already empty workspace and report it.
		bytes = 0
	}
	if err := l.store.writeMeta(l.spec.Name, meta{
		LastUsed: time.Now().UTC(), Bytes: bytes,
	}); err != nil {
		return ev, evicted, err
	}
	return ev, evicted, nil
}

// Abandon gives the lease up without recording anything: what a run that
// never used the workspace does. Recording a use would refresh an age
// nothing actually used, keeping a dead tree alive indefinitely. Safe to
// call twice, and called by Release itself.
func (l *Lease) Abandon() {
	if l.lock == nil {
		return
	}
	// The error is deliberately dropped: this runs on every teardown path,
	// and a failed release is discovered by the next acquisition anyway (a
	// file lock dies with the process, a cluster lease expires).
	_ = l.lock.Release(context.Background())
	l.lock = nil
}

// evict empties the workspace's tree, leaving the directory itself in
// place. RemoveAll then MkdirAll, not a rename aside: a discarded tree
// parked next to the live one is a second copy of exactly the bytes the
// bound said there was no room for.
func (l *Lease) evict(ev Eviction) error {
	tree := l.store.treePath(l.spec.Name)
	if err := os.RemoveAll(tree); err != nil {
		return fmt.Errorf("persist: evict workspace %q: %w", l.spec.Name, err)
	}
	if err := os.MkdirAll(tree, 0o755); err != nil {
		return fmt.Errorf("persist: evict workspace %q: %w", l.spec.Name, err)
	}
	l.evicted, l.didEvit = ev, true
	return nil
}

func (s *Store) readMeta(name string) (meta, bool, error) {
	b, err := os.ReadFile(s.metaPath(name))
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return meta{}, false, nil
		}
		return meta{}, false, fmt.Errorf("persist: read workspace %q record: %w", name, err)
	}
	var m meta
	if err := json.Unmarshal(b, &m); err != nil {
		// Treated as absent, not an error: a corrupt byte in a bookkeeping
		// file must not stop every future run that mounts this workspace.
		return meta{}, false, nil
	}
	return m, true, nil
}

func (s *Store) writeMeta(name string, m meta) error {
	if err := writeJSONAtomic(s.metaPath(name), m); err != nil {
		return fmt.Errorf("persist: record workspace %q: %w", name, err)
	}
	return nil
}

func (s *Store) writeHolder(name string, h holder) error {
	if err := writeJSONAtomic(s.holderPath(name), h); err != nil {
		return fmt.Errorf("persist: record holder of workspace %q: %w", name, err)
	}
	return nil
}

// writeJSONAtomic writes v through a temp file and a rename, so a reader
// not holding the lock (the refused run reading holder.json) sees either
// the previous record or the new one, never half of each.
func writeJSONAtomic(p string, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	f, err := os.CreateTemp(filepath.Dir(p), ".senro-persist-*")
	if err != nil {
		return err
	}
	tmp := f.Name()
	defer func() { _ = os.Remove(tmp) }()
	if _, err := f.Write(b); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, p)
}
