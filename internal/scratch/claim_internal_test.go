package scratch

// Package-internal tests: a save killed mid-way must not dead-end its key
// forever, and a GC sharing this cache's CAS must be able to tell an
// in-flight save from an idle one.

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/xavidop/senro/internal/cas"
	"github.com/xavidop/senro/internal/workspace"
)

func newTestDir(t *testing.T) *Dir {
	t.Helper()
	store, err := cas.Open(filepath.Join(t.TempDir(), "cas"))
	if err != nil {
		t.Fatalf("cas.Open: %v", err)
	}
	d, err := Open(filepath.Join(t.TempDir(), "scratch"), workspace.NewSnapshotter(store))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return d
}

func srcDirWith(t *testing.T, name, body string) string {
	t.Helper()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o644); err != nil {
		t.Fatalf("seed: %v", err)
	}
	return dir
}

// backdate makes p look staleClaimAge old without an actual wait.
func backdate(t *testing.T, p string, age time.Duration) {
	t.Helper()
	old := time.Now().Add(-age)
	if err := os.Chtimes(p, old, old); err != nil {
		t.Fatalf("backdate %s: %v", p, err)
	}
}

// TestSaveRecoversAStaleEmptyPlaceholder is the crash-recovery proof: an
// empty placeholder older than staleClaimAge must not dead-end the key.
// Reverting claim to a bare OpenFile(O_EXCL) fails this forever.
func TestSaveRecoversAStaleEmptyPlaceholder(t *testing.T) {
	d := newTestDir(t)
	ctx := context.Background()
	key := "gomod-abc"

	if err := os.WriteFile(d.entryPath(key), nil, 0o644); err != nil {
		t.Fatalf("simulate crash placeholder: %v", err)
	}
	backdate(t, d.entryPath(key), staleClaimAge)

	saved, err := d.Save(ctx, key, srcDirWith(t, "mod.txt", "downloaded\n"))
	if err != nil {
		t.Fatalf("Save: %v", err)
	}
	if !saved {
		t.Fatal("Save did not recover a stale empty placeholder; the key is dead-ended forever")
	}

	dest := filepath.Join(t.TempDir(), "dest")
	m, ok, err := d.Restore(ctx, key, nil, dest)
	if err != nil || !ok {
		t.Fatalf("Restore after recovery = %v, %v, %v", m, ok, err)
	}
	b, err := os.ReadFile(filepath.Join(dest, "mod.txt"))
	if err != nil || string(b) != "downloaded\n" {
		t.Errorf("restored content = %q, %v", b, err)
	}
}

// TestSaveDoesNotReclaimARecentEmptyPlaceholder is the negative half: a
// young placeholder is plausibly a running save, and reclaiming it would
// let a second Save silently race a still-in-progress snapshot.
func TestSaveDoesNotReclaimARecentEmptyPlaceholder(t *testing.T) {
	d := newTestDir(t)
	ctx := context.Background()
	key := "gomod-abc"

	if err := os.WriteFile(d.entryPath(key), nil, 0o644); err != nil {
		t.Fatalf("simulate an active claim: %v", err)
	}
	backdate(t, d.entryPath(key), staleClaimAge-time.Minute)

	saved, err := d.Save(ctx, key, srcDirWith(t, "mod.txt", "downloaded\n"))
	if err != nil {
		t.Fatalf("Save: %v", err)
	}
	if saved {
		t.Error("Save reclaimed a placeholder that is not yet stale")
	}
	info, err := os.Stat(d.entryPath(key))
	if err != nil || info.Size() != 0 {
		t.Errorf("the active claim was disturbed: size=%d err=%v", info.Size(), err)
	}
}

// TestSaveUnderAnExistingCompleteEntryStillLosesTheRace pins that the
// staleness reclaim is scoped to EMPTY placeholders only: a completed
// entry, however old, must never be reclaimed, or immutability would break
// for any entry older than staleClaimAge.
func TestSaveUnderAnExistingCompleteEntryStillLosesTheRace(t *testing.T) {
	d := newTestDir(t)
	ctx := context.Background()
	key := "k"
	if _, err := d.Save(ctx, key, srcDirWith(t, "v.txt", "first\n")); err != nil {
		t.Fatalf("Save: %v", err)
	}
	backdate(t, d.entryPath(key), 10*staleClaimAge)

	saved, err := d.Save(ctx, key, srcDirWith(t, "v.txt", "second\n"))
	if err != nil {
		t.Fatalf("Save: %v", err)
	}
	if saved {
		t.Fatal("a complete, old entry was overwritten")
	}
	dest := filepath.Join(t.TempDir(), "dest")
	if _, _, err := d.Restore(ctx, key, nil, dest); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	b, _ := os.ReadFile(filepath.Join(dest, "v.txt"))
	if string(b) != "first\n" {
		t.Errorf("the entry changed: %q", b)
	}
}

// TestInFlightReportsARecentEmptyPlaceholder is the GC-race half: telling
// "a save is claiming this key now" from "nothing is happening" is what
// lets GC hold off deleting the object that save has Put but not yet
// pointed anything at.
func TestInFlightReportsARecentEmptyPlaceholder(t *testing.T) {
	d := newTestDir(t)
	if ok, err := d.InFlight(); err != nil || ok {
		t.Fatalf("InFlight over an empty scratch cache = %v, %v, want false", ok, err)
	}
	if err := os.WriteFile(d.entryPath("gomod-race"), nil, 0o644); err != nil {
		t.Fatalf("simulate an active claim: %v", err)
	}
	ok, err := d.InFlight()
	if err != nil {
		t.Fatalf("InFlight: %v", err)
	}
	if !ok {
		t.Error("InFlight did not see a fresh empty placeholder")
	}
}

// TestInFlightIgnoresAStalePlaceholder is InFlight's negative half: an
// abandoned claim past staleClaimAge must not permanently block every
// future GC sweep.
func TestInFlightIgnoresAStalePlaceholder(t *testing.T) {
	d := newTestDir(t)
	if err := os.WriteFile(d.entryPath("gomod-abandoned"), nil, 0o644); err != nil {
		t.Fatalf("simulate an abandoned claim: %v", err)
	}
	backdate(t, d.entryPath("gomod-abandoned"), staleClaimAge)

	ok, err := d.InFlight()
	if err != nil {
		t.Fatalf("InFlight: %v", err)
	}
	if ok {
		t.Error("InFlight reported a stale, abandoned placeholder as in-flight")
	}
}

// TestInFlightIgnoresACompleteEntry: an ordinary, successfully saved entry
// (non-empty) must never register as in-flight.
func TestInFlightIgnoresACompleteEntry(t *testing.T) {
	d := newTestDir(t)
	ctx := context.Background()
	if _, err := d.Save(ctx, "k", srcDirWith(t, "v.txt", "x\n")); err != nil {
		t.Fatalf("Save: %v", err)
	}
	ok, err := d.InFlight()
	if err != nil {
		t.Fatalf("InFlight: %v", err)
	}
	if ok {
		t.Error("InFlight reported a complete entry as in-flight")
	}
}
