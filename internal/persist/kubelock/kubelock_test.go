package kubelock_test

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/xavidop/senro/internal/kubeapi"
	"github.com/xavidop/senro/internal/kubeapi/kindtest"
	"github.com/xavidop/senro/internal/persist"
	"github.com/xavidop/senro/internal/persist/kubelock"
)

// TestMain owns the kind cluster's lifetime for this package, exactly as the
// kubeapi and k8sexec packages' own do.
func TestMain(m *testing.M) {
	code := m.Run()
	kindtest.Cleanup()
	os.Exit(code)
}

func ctx(t *testing.T) context.Context {
	t.Helper()
	c, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)
	return c
}

// The headline property, against a real apiserver: two runs cannot hold one
// workspace, where a file lock on either machine would have let both through.
func TestASecondRunIsRefusedTheSameWorkspace(t *testing.T) {
	c := kindtest.Require(t)
	l := kubelock.New(c.Client, kindtest.Namespace)
	const ws = "build-cache"

	first, err := l.TryAcquire(ctx(t), ws, "run-one")
	if err != nil {
		t.Fatalf("first acquire: %v", err)
	}
	t.Cleanup(func() { _ = first.Release(context.Background()) })

	_, err = l.TryAcquire(ctx(t), ws, "run-two")
	if err == nil {
		t.Fatal("a second run acquired a workspace the first one holds")
	}
	var held *persist.HeldError
	if !errors.As(err, &held) {
		t.Fatalf("refusal is not a *persist.HeldError: %v", err)
	}
	if held.RunID != "run-one" {
		t.Errorf("refusal names holder %q, want %q", held.RunID, "run-one")
	}
	if held.Name != ws {
		t.Errorf("refusal names workspace %q, want %q", held.Name, ws)
	}
}

// Releasing hands it straight over rather than leaving the next run (often
// the same pipeline on the next commit) to wait out the lease duration.
func TestReleasingLetsTheNextRunStraightIn(t *testing.T) {
	c := kindtest.Require(t)
	l := kubelock.New(c.Client, kindtest.Namespace)
	const ws = "handover"

	first, err := l.TryAcquire(ctx(t), ws, "run-one")
	if err != nil {
		t.Fatalf("first acquire: %v", err)
	}
	if err := first.Release(ctx(t)); err != nil {
		t.Fatalf("release: %v", err)
	}

	second, err := l.TryAcquire(ctx(t), ws, "run-two")
	if err != nil {
		t.Fatalf("the next run was refused a released workspace: %v", err)
	}
	t.Cleanup(func() { _ = second.Release(context.Background()) })

	// The lease says who holds it now: what a refusal would report.
	name, err := kubelock.LeaseName(ws)
	if err != nil {
		t.Fatalf("LeaseName: %v", err)
	}
	got, err := c.Client.GetLease(ctx(t), kindtest.Namespace, name)
	if err != nil {
		t.Fatalf("GetLease: %v", err)
	}
	if got.Holder() != "run-two" {
		t.Errorf("lease holder = %q, want %q", got.Holder(), "run-two")
	}
}

// A killed coordinator's lease must expire, or one power cut locks a shared
// workspace out forever. The abandoned lease is written directly: exactly
// the state a SIGKILLed coordinator leaves, with no goroutine timing and no
// sleeping out a real duration.
func TestAnAbandonedLeaseIsTakenOver(t *testing.T) {
	c := kindtest.Require(t)
	l := kubelock.New(c.Client, kindtest.Namespace)
	const ws = "abandoned"

	name, err := kubelock.LeaseName(ws)
	if err != nil {
		t.Fatalf("LeaseName: %v", err)
	}
	holder := "run-killed"
	secs := int32(30)
	stale := kubeapi.NewMicroTime(time.Now().Add(-10 * time.Minute))
	if _, err := c.Client.CreateLease(ctx(t), kindtest.Namespace, kubeapi.Lease{
		Metadata: kubeapi.ObjectMeta{Name: name, Namespace: kindtest.Namespace},
		Spec: kubeapi.LeaseSpec{
			HolderIdentity: &holder, LeaseDurationSeconds: &secs,
			AcquireTime: stale, RenewTime: stale,
		},
	}); err != nil {
		t.Fatalf("planting an abandoned lease: %v", err)
	}
	t.Cleanup(func() { _ = c.Client.DeleteLease(context.Background(), kindtest.Namespace, name) })

	u, err := l.TryAcquire(ctx(t), ws, "run-two")
	if err != nil {
		t.Fatalf("an abandoned lease was not taken over: %v", err)
	}
	t.Cleanup(func() { _ = u.Release(context.Background()) })

	got, err := c.Client.GetLease(ctx(t), kindtest.Namespace, name)
	if err != nil {
		t.Fatalf("GetLease: %v", err)
	}
	if got.Holder() != "run-two" {
		t.Errorf("holder after takeover = %q, want %q", got.Holder(), "run-two")
	}
}

// A lease that has not expired is NOT taken over; otherwise the test above
// would pass against a locker that always took what it found.
func TestALeaseThatHasNotExpiredIsNotTakenOver(t *testing.T) {
	c := kindtest.Require(t)
	l := kubelock.New(c.Client, kindtest.Namespace)
	const ws = "still-live"

	name, err := kubelock.LeaseName(ws)
	if err != nil {
		t.Fatalf("LeaseName: %v", err)
	}
	holder := "run-one"
	secs := int32(600)
	recent := kubeapi.NewMicroTime(time.Now())
	if _, err := c.Client.CreateLease(ctx(t), kindtest.Namespace, kubeapi.Lease{
		Metadata: kubeapi.ObjectMeta{Name: name, Namespace: kindtest.Namespace},
		Spec: kubeapi.LeaseSpec{
			HolderIdentity: &holder, LeaseDurationSeconds: &secs,
			AcquireTime: recent, RenewTime: recent,
		},
	}); err != nil {
		t.Fatalf("planting a live lease: %v", err)
	}
	t.Cleanup(func() { _ = c.Client.DeleteLease(context.Background(), kindtest.Namespace, name) })

	if _, err := l.TryAcquire(ctx(t), ws, "run-two"); err == nil {
		t.Fatal("a live lease was taken over")
	}
}

// The renewal loop must actually renew, or a run longer than the lease
// duration has its workspace taken while a step is still writing to it.
func TestALiveHolderKeepsTheLeaseBeyondItsDuration(t *testing.T) {
	c := kindtest.Require(t)

	restoreD, restoreR := persist.LeaseDuration, persist.LeaseRenewInterval
	persist.LeaseDuration = 3 * time.Second
	persist.LeaseRenewInterval = time.Second
	t.Cleanup(func() {
		persist.LeaseDuration, persist.LeaseRenewInterval = restoreD, restoreR
	})

	l := kubelock.New(c.Client, kindtest.Namespace)
	const ws = "long-run"

	holder, err := l.TryAcquire(ctx(t), ws, "run-long")
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	t.Cleanup(func() { _ = holder.Release(context.Background()) })

	// Well past the duration: an unrenewed lease would have expired.
	time.Sleep(5 * time.Second)

	if _, err := l.TryAcquire(ctx(t), ws, "run-two"); err == nil {
		t.Fatal("a live holder's lease was taken: the renewal loop is not renewing")
	}
}

// Two workspaces are two leases: hashing rather than sanitizing keeps names
// differing only in an unrepresentable character from collapsing onto one
// lease.
func TestTwoWorkspacesDoNotExcludeEachOther(t *testing.T) {
	c := kindtest.Require(t)
	l := kubelock.New(c.Client, kindtest.Namespace)

	a, err := l.TryAcquire(ctx(t), "apps/web", "run-one")
	if err != nil {
		t.Fatalf("acquire apps/web: %v", err)
	}
	t.Cleanup(func() { _ = a.Release(context.Background()) })

	b, err := l.TryAcquire(ctx(t), "apps-web", "run-one")
	if err != nil {
		t.Fatalf("acquire apps-web was refused; it collapsed onto the same lease as apps/web: %v", err)
	}
	t.Cleanup(func() { _ = b.Release(context.Background()) })

	an, _ := kubelock.LeaseName("apps/web")
	bn, _ := kubelock.LeaseName("apps-web")
	if an == bn {
		t.Errorf("two workspace names produced one lease name: %s", an)
	}
}

// The lease carries the workspace's real name in an annotation: the object
// name is a hash, useless to somebody reading `kubectl get leases`.
func TestTheLeaseSaysWhichWorkspaceItIsFor(t *testing.T) {
	c := kindtest.Require(t)
	l := kubelock.New(c.Client, kindtest.Namespace)
	const ws = "apps/web:cache"

	u, err := l.TryAcquire(ctx(t), ws, "run-one")
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	t.Cleanup(func() { _ = u.Release(context.Background()) })

	name, _ := kubelock.LeaseName(ws)
	got, err := c.Client.GetLease(ctx(t), kindtest.Namespace, name)
	if err != nil {
		t.Fatalf("GetLease: %v", err)
	}
	if got.Metadata.Annotations["senro.dev/workspace"] != ws {
		t.Errorf("annotation = %q, want %q", got.Metadata.Annotations["senro.dev/workspace"], ws)
	}
}

// An update with no resourceVersion is refused outright: that token is what
// makes an expired-lease take-over a race exactly one coordinator wins.
func TestAnUpdateWithoutAResourceVersionIsRefused(t *testing.T) {
	c := kindtest.Require(t)
	_, err := c.Client.UpdateLease(ctx(t), kindtest.Namespace, kubeapi.Lease{
		Metadata: kubeapi.ObjectMeta{Name: "senro-ws-whatever", Namespace: kindtest.Namespace},
	})
	if err == nil {
		t.Fatal("an update with no resourceVersion was accepted")
	}
}
