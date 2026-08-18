// Package storage is the one handle the engine holds on everything
// content-addressed: the CAS, the action cache, the scratch cache and the
// workspace snapshotter, all rooted in one directory.
//
// It exists so engine.Options grows one field rather than four, and so the
// question "where does this run's cache live" has exactly one answer.
package storage

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/xavidop/senro/internal/cache"
	"github.com/xavidop/senro/internal/cas"
	"github.com/xavidop/senro/internal/persist"
	"github.com/xavidop/senro/internal/remotecache"
	"github.com/xavidop/senro/internal/scratch"
	"github.com/xavidop/senro/internal/workspace"
)

// Storage is an opened storage root.
//
// Two pairs of fields name the same stores on purpose. CAS and Action are
// the stores on THIS MACHINE'S DISK, which the GC budgets against and the
// `senro cache` commands walk: questions a shared store cannot answer.
// Objects and ActionCache are what a RUN reads and writes through, the same
// two stores when nothing else is configured and a tier in front of them
// when a remote is. The engine uses the second pair, disk management the
// first.
type Storage struct {
	// Root is the directory everything below lives in.
	Root string
	// CAS holds every content-addressed object on this machine: workspace
	// tarballs, file indexes, cached logs, cached output artifacts.
	CAS *cas.Dir
	// Snapshotter turns a run's workspace directories into digests and back.
	// It is built on Objects, so a workspace snapshotted by one run is
	// restorable by the next, on this machine or on another one.
	Snapshotter *workspace.Snapshotter
	// Action is the correctness-critical cache on this machine: a hit skips a
	// step entirely.
	Action *cache.Dir
	// Scratch is the best-effort cache: a mutable directory restored by key
	// with prefix fallbacks. A miss is not an error, and its contents are
	// never an input to a cache key.
	//
	// Backed by the LOCAL CAS alone, not Objects, and NOT because the key
	// could not travel: ExpandKey renders from repository content alone, so
	// a second machine computes the identical one. Three other things block
	// a tier. An entry is one whole-tree tarball, often gigabytes, whose key
	// churns on every lock-file edit, to save a download the toolchain
	// already does incrementally. A key carries no repository namespace, so
	// on a shared bucket one project's RestoreKeys prefix would match
	// another's entries. And the prefix fallback picks the newest match by
	// the entry file's local mtime, which two machines cannot agree on; the
	// OCI backend cannot list by prefix at all (see internal/oci).
	Scratch *scratch.Dir
	// Persist holds the directories ScopePersistent workspaces live in
	// between runs, leased to one run at a time and bounded by an age and a
	// size (see internal/persist).
	//
	// It holds no content-addressed object: a persistent workspace IS a
	// directory on this machine, and the CAS carries only the measurement a
	// run takes of it. The tree itself does not travel, which is the whole
	// difference from a scratch cache.
	Persist *persist.Store

	// Objects is every object this run reads and writes: the local CAS, with
	// the remote one behind it when there is a remote.
	Objects cas.Store
	// ActionCache is every action-cache entry this run reads and writes: the
	// local cache, with the remote one behind it when there is a remote.
	ActionCache cache.ActionCache
	// Remote is the shared cache, or nil when none is configured. Held so a
	// caller can ask whether it is still live, and so Close can release it.
	Remote *remotecache.Remote
}

// Option configures Open.
type Option func(*options)

type options struct{ remote *remotecache.Remote }

// WithRemote puts a shared, remote cache behind the local one.
//
// A nil remote is the same as not passing anything at all, so a caller that
// resolves its configuration into a possibly-nil remote needs no branch here.
func WithRemote(r *remotecache.Remote) Option {
	return func(o *options) { o.remote = r }
}

// DefaultRoot is where a cache lives when nobody says otherwise:
// $SENRO_CACHE_DIR, else os.UserCacheDir()/senro. One environment variable
// rather than a flag on every command, because a cache root is switched for
// everything at once.
func DefaultRoot() (string, error) {
	if v := os.Getenv("SENRO_CACHE_DIR"); v != "" {
		return v, nil
	}
	base, err := os.UserCacheDir()
	if err != nil {
		return "", fmt.Errorf("storage: no cache directory available: %w", err)
	}
	return filepath.Join(base, "senro"), nil
}

// Open prepares root, creating whatever is missing. Every subdirectory is
// created up front, even ones a run will never write to, so a listing reads
// the same on every machine and a missing directory is a real anomaly.
func Open(root string, opts ...Option) (*Storage, error) {
	var o options
	for _, fn := range opts {
		fn(&o)
	}

	for _, sub := range []string{"action", "scratch", "pins", "persist"} {
		if err := os.MkdirAll(filepath.Join(root, sub), 0o755); err != nil {
			return nil, fmt.Errorf("storage: open %s: %w", root, err)
		}
	}
	store, err := cas.Open(filepath.Join(root, "cas"))
	if err != nil {
		return nil, err
	}
	action, err := cache.Open(filepath.Join(root, "action"))
	if err != nil {
		return nil, err
	}

	// Without a remote these are the local stores themselves: a run that
	// configures nothing pays nothing, not even an interface hop.
	var objects cas.Store = store
	var entries cache.ActionCache = action
	if o.remote != nil {
		objects = o.remote.TierObjects(store)
		entries = o.remote.TierEntries(action)
	}

	snap := workspace.NewSnapshotter(objects)
	// The scratch cache gets its own snapshotter over the local store alone.
	// See Storage.Scratch's own doc.
	sc, err := scratch.Open(filepath.Join(root, "scratch"), workspace.NewSnapshotter(store))
	if err != nil {
		return nil, err
	}
	// No snapshotter, and the absence is the point: a persistent workspace's
	// content never becomes an object this store hands back, though what a
	// run measures of it does, through snap above.
	ps, err := persist.Open(filepath.Join(root, "persist"))
	if err != nil {
		return nil, err
	}
	return &Storage{
		Root: root, CAS: store, Action: action, Scratch: sc, Persist: ps,
		Snapshotter: snap,
		Objects:     objects, ActionCache: entries, Remote: o.remote,
	}, nil
}

// Close releases whatever the store holds: today, the remote cache's
// connections, and nothing else.
func (s *Storage) Close() error {
	if s.Remote != nil {
		return s.Remote.Close()
	}
	return nil
}
