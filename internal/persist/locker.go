package persist

import (
	"context"
	"time"
)

// This file is the exclusion, separated from what it excludes. There are
// two backends: a local tree is excluded by an advisory file lock, and a
// PersistentVolumeClaim tree, reachable by two coordinators, by a
// coordination.k8s.io Lease. An interface with two implementations rather
// than two Acquire functions, so the reasoned-about ORDER (exclusion, then
// bounds, then evict, then lease) stays in one function and only the taking
// and releasing differ.

// Locker is what makes one run the holder of one workspace name.
//
// TryAcquire never waits (waiting would span somebody else's whole
// pipeline) and returns a *HeldError naming the holder when the workspace
// belongs to somebody else.
type Locker interface {
	TryAcquire(ctx context.Context, name, runID string) (Unlocker, error)
	// Kind names this locker for an error message: a file lock on this
	// machine, or a lease in a cluster.
	Kind() string
}

// Unlocker releases one hold. Release must be safe after the process has
// lost contact with whatever backs it: a coordinator that cannot reach the
// apiserver still finishes its teardown. Errors are for logging, never
// control flow.
type Unlocker interface {
	Release(ctx context.Context) error
}

// LeaseDuration is how long a cluster lease is good for before another run
// may take it over. Too short and a live run's lease expires mid-step,
// letting a second run in on the same tree; too long and a dead machine
// locks the workspace out until it elapses. A minute, renewed every
// LeaseRenewInterval, keeps a healthy holder many renewals from expiry.
var LeaseDuration = 60 * time.Second

// LeaseRenewInterval is how often a holder says it is still alive. A third
// of LeaseDuration, so two consecutive renewals can fail without the lease
// expiring.
var LeaseRenewInterval = LeaseDuration / 3

// StoreLocker exposes a store's own exclusion (the advisory file lock) for
// callers wanting senro's locking without its workspaces. contrib/dispatcher
// is the one such caller: inventing its own lock there would be a second
// answer to a question answered here once.
func StoreLocker(s *Store) Locker { return s.lock }
