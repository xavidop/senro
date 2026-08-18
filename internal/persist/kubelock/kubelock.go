// Package kubelock excludes two runs from one PersistentVolumeClaim-backed
// workspace, using a coordination.k8s.io Lease.
//
// A file lock only excludes while the coordinator owns the tree; a
// claim-backed workspace is reachable by two coordinators on two machines,
// where two file locks exclude nothing. A Lease is Kubernetes' own
// one-holder-at-a-time primitive (its controllers' leader election). It
// gives atomic take-over of an expired lease via optimistic concurrency; it
// does NOT give fencing, so a holder partitioned from the apiserver can
// keep writing after its lease is taken, as with every unfenced lease.
//
// A separate package on purpose: persist must not depend on an apiserver
// client, kubeapi must not depend on senro's workspace rules; this is the
// one place that knows both.
package kubelock

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/xavidop/senro/internal/kubeapi"
	"github.com/xavidop/senro/internal/persist"
)

// Locker takes a Lease in one namespace.
type Locker struct {
	cli *kubeapi.Client
	ns  string
	// now is the clock, injectable so a test can expire a lease without
	// waiting a minute for it.
	now func() time.Time
}

var _ persist.Locker = (*Locker)(nil)

// New returns a Locker that leases in ns.
func New(cli *kubeapi.Client, ns string) *Locker {
	return &Locker{cli: cli, ns: ns, now: time.Now}
}

// Kind names this exclusion for a refusal message.
func (*Locker) Kind() string { return "a Lease in the cluster" }

// TryAcquire takes the lease for name, or reports who holds it.
//
// Three cases: no lease object, create one (a lost create races back 409, a
// refusal not a retry); held and unexpired, refuse naming the holder; held
// but expired, take over with an update carrying the resourceVersion it was
// read at, so of two coordinators seeing the expiry the apiserver lets
// exactly one through. Without that resourceVersion this would be
// last-write-wins and both would believe they held it.
func (l *Locker) TryAcquire(ctx context.Context, name, runID string) (persist.Unlocker, error) {
	leaseName, err := LeaseName(name)
	if err != nil {
		return nil, err
	}

	existing, err := l.cli.GetLease(ctx, l.ns, leaseName)
	switch {
	case kubeapi.IsNotFound(err):
		created, cerr := l.cli.CreateLease(ctx, l.ns, l.newLease(leaseName, name, runID))
		if kubeapi.IsConflict(cerr) {
			// Somebody created it between the read and the create.
			return nil, heldError(name, "", time.Time{})
		}
		if cerr != nil {
			return nil, fmt.Errorf("kubelock: creating lease %s/%s: %w", l.ns, leaseName, cerr)
		}
		return l.hold(created, name, runID), nil

	case err != nil:
		return nil, fmt.Errorf("kubelock: reading lease %s/%s: %w", l.ns, leaseName, err)
	}

	if !existing.Expired(l.now()) {
		var since time.Time
		if existing.Spec.AcquireTime != nil {
			since = existing.Spec.AcquireTime.Time
		}
		return nil, heldError(name, existing.Holder(), since)
	}

	// Expired: take over, conditional on nothing changed since the read.
	takeover := l.newLease(leaseName, name, runID)
	takeover.Metadata.ResourceVersion = existing.Metadata.ResourceVersion
	updated, uerr := l.cli.UpdateLease(ctx, l.ns, takeover)
	if kubeapi.IsConflict(uerr) {
		// Another coordinator took it over first.
		return nil, heldError(name, "", time.Time{})
	}
	if uerr != nil {
		return nil, fmt.Errorf("kubelock: taking over expired lease %s/%s: %w", l.ns, leaseName, uerr)
	}
	return l.hold(updated, name, runID), nil
}

func (l *Locker) newLease(leaseName, workspace, runID string) kubeapi.Lease {
	now := kubeapi.NewMicroTime(l.now())
	secs := int32(persist.LeaseDuration / time.Second)
	holder := runID
	return kubeapi.Lease{
		Metadata: kubeapi.ObjectMeta{
			Name:      leaseName,
			Namespace: l.ns,
			Labels:    map[string]string{"senro.dev/persistent-workspace": "true"},
			// The real name, which a label (63-byte cap, narrow charset)
			// cannot hold; same split k8sexec makes for a step id.
			Annotations: map[string]string{"senro.dev/workspace": workspace},
		},
		Spec: kubeapi.LeaseSpec{
			HolderIdentity:       &holder,
			LeaseDurationSeconds: &secs,
			AcquireTime:          now,
			RenewTime:            now,
		},
	}
}

// hold starts the renewal loop and returns the release.
func (l *Locker) hold(lease kubeapi.Lease, workspace, runID string) *unlocker {
	u := &unlocker{
		l: l, lease: lease, workspace: workspace, runID: runID,
		stop: make(chan struct{}),
		done: make(chan struct{}),
	}
	go u.renew()
	return u
}

type unlocker struct {
	l         *Locker
	workspace string
	runID     string

	mu    sync.Mutex
	lease kubeapi.Lease

	stop chan struct{}
	done chan struct{}
	once sync.Once
}

// renew says "still alive" until the lease is released.
//
// A failed renewal is retried, and a lost lease is deliberately NOT a step
// failure: by the time the loss is discovered the damage is done, and
// failing a step for a reason unrelated to its work is worse than finishing
// and finding the overlap. Prevention is the lease duration being many
// renewals long; this loop keeps a healthy holder holding.
func (u *unlocker) renew() {
	defer close(u.done)
	t := time.NewTicker(persist.LeaseRenewInterval)
	defer t.Stop()
	for {
		select {
		case <-u.stop:
			return
		case <-t.C:
			ctx, cancel := context.WithTimeout(context.Background(), persist.LeaseRenewInterval)
			u.renewOnce(ctx)
			cancel()
		}
	}
}

func (u *unlocker) renewOnce(ctx context.Context) {
	u.mu.Lock()
	cur := u.lease
	u.mu.Unlock()

	cur.Spec.RenewTime = kubeapi.NewMicroTime(u.l.now())
	updated, err := u.l.cli.UpdateLease(ctx, u.l.ns, cur)
	if err != nil {
		// A conflict on a lease this run believes it holds means a take-over.
		// Re-read on the next tick rather than fighting for it.
		if kubeapi.IsConflict(err) {
			if fresh, ferr := u.l.cli.GetLease(ctx, u.l.ns, cur.Metadata.Name); ferr == nil {
				u.mu.Lock()
				u.lease = fresh
				u.mu.Unlock()
			}
		}
		return
	}
	u.mu.Lock()
	u.lease = updated
	u.mu.Unlock()
}

// Release stops renewing and deletes the lease, so the next run does not wait
// out the full duration for a workspace this one has finished with.
func (u *unlocker) Release(ctx context.Context) error {
	var err error
	u.once.Do(func() {
		close(u.stop)
		<-u.done

		u.mu.Lock()
		name := u.lease.Metadata.Name
		u.mu.Unlock()

		// Deleted rather than left to expire: a leftover lease blocks the
		// next run (often the same pipeline) for up to LeaseDuration.
		if derr := u.l.cli.DeleteLease(ctx, u.l.ns, name); derr != nil {
			err = fmt.Errorf("kubelock: releasing lease %s/%s: %w", u.l.ns, name, derr)
		}
	})
	return err
}

// LeaseName is the object name for a workspace's lease. Hashed rather than
// sanitized: sanitizing maps "my/ws" and "my-ws" onto one name, silently
// making two workspaces exclude each other; the annotation carries the real
// name for anyone reading the object.
func LeaseName(workspace string) (string, error) {
	if workspace == "" {
		return "", errors.New("kubelock: a workspace with no name cannot be leased")
	}
	return "senro-ws-" + strings.ToLower(shortHash(workspace)), nil
}

func heldError(workspace, holder string, since time.Time) error {
	return &persist.HeldError{Name: workspace, RunID: holder, Since: since}
}
