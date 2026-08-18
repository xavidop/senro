package engine

// Package-internal test for the exclusion primitive C1's fix is built on:
// lockMounts (a step's ordinary shared use of a workspace) and lockRestore
// (a cache hit's exclusive replacement of one). Deterministic, channel-based
// ordering rather than timing, so it proves the primitive itself rather than
// merely making the race in cache_c1_test.go unlikely to land wrong.

import (
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/xavidop/senro/internal/plan"
)

// TestLockRestoreWaitsForAnOutstandingLockMounts is the mutation proof for
// the primitive: a restore must not proceed while a step's shared hold on
// the same workspace is outstanding, and must proceed the moment that hold
// is released. If lockMounts/lockRestore were reduced to no-ops (the
// pre-fix state), "restore:acquired" would appear in events immediately,
// before releaseMount is ever closed, and the first assertion below would
// catch it.
func TestLockRestoreWaitsForAnOutstandingLockMounts(t *testing.T) {
	p := &plan.Plan{Version: 1, Workspaces: []plan.WorkspaceSpec{{Name: "src", Scope: "run"}}}
	m := newTestWSManager(t, p, nil)

	var mu sync.Mutex
	var events []string
	record := func(s string) {
		mu.Lock()
		events = append(events, s)
		mu.Unlock()
	}
	snapshot := func() []string {
		mu.Lock()
		defer mu.Unlock()
		return append([]string(nil), events...)
	}

	mountHeld := make(chan struct{})
	releaseMount := make(chan struct{})
	restoreDone := make(chan struct{})

	go func() {
		unlock := m.lockMounts([]string{"src"})
		record("mount:acquired")
		close(mountHeld)
		<-releaseMount
		record("mount:released")
		unlock()
	}()

	go func() {
		<-mountHeld
		unlock := m.lockRestore([]string{"src"})
		record("restore:acquired")
		unlock()
		close(restoreDone)
	}()

	// The mount side is confirmed held; give the restore goroutine every
	// opportunity to (wrongly) proceed before it is released.
	<-mountHeld
	time.Sleep(50 * time.Millisecond)
	if got := snapshot(); !reflect.DeepEqual(got, []string{"mount:acquired"}) {
		t.Fatalf("restore proceeded while lockMounts was still held: %v", got)
	}

	close(releaseMount)
	<-restoreDone

	want := []string{"mount:acquired", "mount:released", "restore:acquired"}
	if got := snapshot(); !reflect.DeepEqual(got, want) {
		t.Fatalf("order = %v, want %v", got, want)
	}
}

// TestLockMountsWaitsForAnOutstandingLockRestore is the reverse direction:
// once a restore has started, a step that wants to begin (or continue)
// mounting the same workspace must wait for it, not interleave with it.
// Go's sync.RWMutex gives a pending writer priority over new readers, which
// is what makes this hold without a step already in flight being able to
// starve the restore forever, the property lockRestore's own doc names.
func TestLockMountsWaitsForAnOutstandingLockRestore(t *testing.T) {
	p := &plan.Plan{Version: 1, Workspaces: []plan.WorkspaceSpec{{Name: "src", Scope: "run"}}}
	m := newTestWSManager(t, p, nil)

	var mu sync.Mutex
	var events []string
	record := func(s string) {
		mu.Lock()
		events = append(events, s)
		mu.Unlock()
	}
	snapshot := func() []string {
		mu.Lock()
		defer mu.Unlock()
		return append([]string(nil), events...)
	}

	restoreHeld := make(chan struct{})
	releaseRestore := make(chan struct{})
	mountDone := make(chan struct{})

	go func() {
		unlock := m.lockRestore([]string{"src"})
		record("restore:acquired")
		close(restoreHeld)
		<-releaseRestore
		record("restore:released")
		unlock()
	}()

	go func() {
		<-restoreHeld
		unlock := m.lockMounts([]string{"src"})
		record("mount:acquired")
		unlock()
		close(mountDone)
	}()

	<-restoreHeld
	time.Sleep(50 * time.Millisecond)
	if got := snapshot(); !reflect.DeepEqual(got, []string{"restore:acquired"}) {
		t.Fatalf("a new mount proceeded while a restore was in flight: %v", got)
	}

	close(releaseRestore)
	<-mountDone

	want := []string{"restore:acquired", "restore:released", "mount:acquired"}
	if got := snapshot(); !reflect.DeepEqual(got, want) {
		t.Fatalf("order = %v, want %v", got, want)
	}
}

// TestLockMountsAllowsConcurrentSiblings pins the concurrency this change
// must NOT remove: two ordinary steps mounting the same ScopeRun workspace
// at once (the existing, documented behaviour, see wsManager's package
// doc) must both be able to hold lockMounts at the same time. If lockMounts
// were mistakenly implemented with Lock instead of RLock, this would
// deadlock and the test would time out.
func TestLockMountsAllowsConcurrentSiblings(t *testing.T) {
	p := &plan.Plan{Version: 1, Workspaces: []plan.WorkspaceSpec{{Name: "src", Scope: "run"}}}
	m := newTestWSManager(t, p, nil)

	firstHeld := make(chan struct{})
	bothHeld := make(chan struct{})

	unlock1 := m.lockMounts([]string{"src"})
	close(firstHeld)

	done := make(chan struct{})
	go func() {
		unlock2 := m.lockMounts([]string{"src"})
		close(bothHeld)
		unlock2()
		close(done)
	}()

	select {
	case <-bothHeld:
	case <-time.After(2 * time.Second):
		t.Fatal("a second concurrent lockMounts on the same workspace deadlocked; " +
			"two siblings sharing a ScopeRun workspace must both be able to hold it at once")
	}
	unlock1()
	<-done
}
