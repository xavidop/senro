package persist_test

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/xavidop/senro/internal/persist"
)

func open(t *testing.T) *persist.Store {
	t.Helper()
	s, err := persist.Open(filepath.Join(t.TempDir(), "persist"))
	if err != nil {
		t.Fatalf("persist.Open: %v", err)
	}
	return s
}

func spec(name string) persist.Spec {
	return persist.Spec{Name: name, MaxAge: time.Hour, MaxSize: 1 << 20}
}

func acquire(t *testing.T, s *persist.Store, sp persist.Spec, runID string) *persist.Lease {
	t.Helper()
	l, err := s.Acquire(sp, runID)
	if err != nil {
		t.Fatalf("Acquire(%q) as %q: %v", sp.Name, runID, err)
	}
	return l
}

func writeFile(t *testing.T, p, body string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
		t.Fatalf("write %s: %v", p, err)
	}
}

// The property the whole scope exists for: what one run leaves behind, the
// next run starts from.
func TestWhatOneLeaseLeavesBehindTheNextLeaseStartsFrom(t *testing.T) {
	s := open(t)

	first := acquire(t, s, spec("mods"), "run-1")
	writeFile(t, filepath.Join(first.Dir(), "pkg", "dep.txt"), "expensive")
	if _, _, err := first.Release(9); err != nil {
		t.Fatalf("Release: %v", err)
	}

	second := acquire(t, s, spec("mods"), "run-2")
	defer func() { _, _, _ = second.Release(9) }()
	b, err := os.ReadFile(filepath.Join(second.Dir(), "pkg", "dep.txt"))
	if err != nil {
		t.Fatalf("the second lease did not see what the first left: %v", err)
	}
	if string(b) != "expensive" {
		t.Errorf("content = %q, want %q", b, "expensive")
	}
}

// Two runs mutating one directory produce a tree neither describes. senro
// refuses the second, and the refusal names the holder: "busy" is not
// actionable.
func TestASecondLeaseOnALiveOneIsRefusedAndNamesTheHolder(t *testing.T) {
	s := open(t)

	held := acquire(t, s, spec("mods"), "run-1")
	defer func() { _, _, _ = held.Release(0) }()

	_, err := s.Acquire(spec("mods"), "run-2")
	if err == nil {
		t.Fatal("a second lease on a live one was granted; two runs now share one mutable tree")
	}
	var busy *persist.HeldError
	if !errors.As(err, &busy) {
		t.Fatalf("error is %T, want a *persist.HeldError a caller can inspect: %v", err, err)
	}
	if busy.RunID != "run-1" {
		t.Errorf("HeldError.RunID = %q, want run-1", busy.RunID)
	}
	if busy.Name != "mods" {
		t.Errorf("HeldError.Name = %q, want mods", busy.Name)
	}
	if busy.PID != os.Getpid() {
		t.Errorf("HeldError.PID = %d, want %d", busy.PID, os.Getpid())
	}
}

// A released lease is available again, in the same process; otherwise the
// refusal above is a lock never given back.
func TestAReleasedLeaseIsAvailableAgain(t *testing.T) {
	s := open(t)

	first := acquire(t, s, spec("mods"), "run-1")
	if _, _, err := first.Release(0); err != nil {
		t.Fatalf("Release: %v", err)
	}
	second, err := s.Acquire(spec("mods"), "run-2")
	if err != nil {
		t.Fatalf("Acquire after Release: %v", err)
	}
	if _, _, err := second.Release(0); err != nil {
		t.Fatalf("Release: %v", err)
	}
}

// The exclusion is per name, not per store, or one persistent workspace
// anywhere would serialize every run on the machine.
func TestTwoDifferentWorkspacesDoNotExcludeEachOther(t *testing.T) {
	s := open(t)

	a := acquire(t, s, spec("mods"), "run-1")
	b, err := s.Acquire(spec("build"), "run-2")
	if err != nil {
		t.Fatalf("a lease on a different workspace was refused: %v", err)
	}
	if _, _, err := a.Release(0); err != nil {
		t.Fatalf("Release a: %v", err)
	}
	if _, _, err := b.Release(0); err != nil {
		t.Fatalf("Release b: %v", err)
	}
}

// MaxAge is enforced at acquisition, and the eviction is reported: a cold
// cache with no explanation reads as a broken workspace.
func TestAWorkspaceOlderThanMaxAgeIsEvictedAtAcquisition(t *testing.T) {
	s := open(t)
	sp := persist.Spec{Name: "mods", MaxAge: time.Hour, MaxSize: 1 << 20}

	first := acquire(t, s, sp, "run-1")
	writeFile(t, filepath.Join(first.Dir(), "dep.txt"), "expensive")
	if _, _, err := first.Release(9); err != nil {
		t.Fatalf("Release: %v", err)
	}
	if err := s.SetLastUsedForTest(sp.Name, time.Now().Add(-3*time.Hour)); err != nil {
		t.Fatalf("SetLastUsedForTest: %v", err)
	}

	second := acquire(t, s, sp, "run-2")
	defer func() { _, _, _ = second.Release(0) }()

	ev, ok := second.Eviction()
	if !ok {
		t.Fatal("a workspace three hours past a one hour MaxAge was leased without eviction")
	}
	if ev.Reason != persist.ReasonMaxAge {
		t.Errorf("eviction reason = %q, want %q", ev.Reason, persist.ReasonMaxAge)
	}
	if _, err := os.Stat(filepath.Join(second.Dir(), "dep.txt")); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("the evicted workspace still holds dep.txt: %v", err)
	}
	entries, err := os.ReadDir(second.Dir())
	if err != nil {
		t.Fatalf("an eviction must leave an empty directory, not no directory: %v", err)
	}
	if len(entries) != 0 {
		t.Errorf("the evicted workspace holds %d entries, want 0", len(entries))
	}
}

// Using a workspace is what keeps it alive: a nightly build must never age
// its own tree out, however old the content.
func TestReleasingAWorkspaceRefreshesItsAge(t *testing.T) {
	s := open(t)
	sp := persist.Spec{Name: "mods", MaxAge: time.Hour, MaxSize: 1 << 20}

	first := acquire(t, s, sp, "run-1")
	writeFile(t, filepath.Join(first.Dir(), "dep.txt"), "expensive")
	if err := s.SetLastUsedForTest(sp.Name, time.Now().Add(-3*time.Hour)); err != nil {
		t.Fatalf("SetLastUsedForTest: %v", err)
	}
	// Release stamps "used now", saving an already stale workspace from the
	// next run's check.
	if _, _, err := first.Release(9); err != nil {
		t.Fatalf("Release: %v", err)
	}

	second := acquire(t, s, sp, "run-2")
	defer func() { _, _, _ = second.Release(9) }()
	if ev, ok := second.Eviction(); ok {
		t.Fatalf("a workspace released moments ago was evicted as %s", ev.Reason)
	}
	if _, err := os.Stat(filepath.Join(second.Dir(), "dep.txt")); err != nil {
		t.Errorf("a workspace in continuous use lost its content: %v", err)
	}
}

// MaxSize is enforced at release, so the machine does not carry the excess
// between runs; the run that overshot still finishes against its tree.
func TestAWorkspaceOverMaxSizeIsEvictedAtRelease(t *testing.T) {
	s := open(t)
	sp := persist.Spec{Name: "mods", MaxAge: time.Hour, MaxSize: 100}

	first := acquire(t, s, sp, "run-1")
	writeFile(t, filepath.Join(first.Dir(), "big.bin"), "x")
	ev, ok, err := first.Release(4096)
	if err != nil {
		t.Fatalf("Release: %v", err)
	}
	if !ok {
		t.Fatal("a workspace releasing 4096 bytes against a 100 byte MaxSize was not evicted")
	}
	if ev.Reason != persist.ReasonMaxSize {
		t.Errorf("eviction reason = %q, want %q", ev.Reason, persist.ReasonMaxSize)
	}
	if ev.Bytes != 4096 || ev.Limit != 100 {
		t.Errorf("eviction = %d bytes against a %d limit, want 4096 against 100", ev.Bytes, ev.Limit)
	}

	second := acquire(t, s, sp, "run-2")
	defer func() { _, _, _ = second.Release(0) }()
	if _, err := os.Stat(filepath.Join(second.Dir(), "big.bin")); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("the next run still sees the evicted content: %v", err)
	}
}

// A killed run never runs the release-time check, so acquisition enforces
// the same bound against the last recorded size; without this, one SIGKILL
// leaves an unbounded tree forever.
func TestAWorkspaceLeftOverMaxSizeByACrashedRunIsEvictedAtTheNextAcquisition(t *testing.T) {
	s := open(t)
	sp := persist.Spec{Name: "mods", MaxAge: time.Hour, MaxSize: 100}

	first := acquire(t, s, sp, "run-1")
	writeFile(t, filepath.Join(first.Dir(), "big.bin"), "x")
	// What a crashed run leaves: a recorded size and no release of its own.
	// Abandon drops the lock the way a dead process would.
	if err := s.SetRecordedBytesForTest(sp.Name, 4096); err != nil {
		t.Fatalf("SetRecordedBytesForTest: %v", err)
	}
	first.Abandon()

	second := acquire(t, s, sp, "run-2")
	defer func() { _, _, _ = second.Release(0) }()
	ev, ok := second.Eviction()
	if !ok {
		t.Fatal("a workspace recorded at 4096 bytes against a 100 byte MaxSize was leased without eviction")
	}
	if ev.Reason != persist.ReasonMaxSize {
		t.Errorf("eviction reason = %q, want %q", ev.Reason, persist.ReasonMaxSize)
	}
	if _, err := os.Stat(filepath.Join(second.Dir(), "big.bin")); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("the crashed run's oversize tree survived: %v", err)
	}
}

// A workspace within both bounds is left alone: a policy that fires on
// everything bounds the disk and delivers no cache.
func TestAWorkspaceWithinBothBoundsIsNotEvicted(t *testing.T) {
	s := open(t)
	sp := persist.Spec{Name: "mods", MaxAge: time.Hour, MaxSize: 1 << 20}

	first := acquire(t, s, sp, "run-1")
	writeFile(t, filepath.Join(first.Dir(), "dep.txt"), "expensive")
	if _, ok, err := first.Release(9); err != nil {
		t.Fatalf("Release: %v", err)
	} else if ok {
		t.Fatal("a 9 byte workspace was evicted against a 1 MiB MaxSize")
	}

	second := acquire(t, s, sp, "run-2")
	defer func() { _, _, _ = second.Release(9) }()
	if ev, ok := second.Eviction(); ok {
		t.Fatalf("a workspace within both bounds was evicted as %s", ev.Reason)
	}
}

// Two names that differ only in a character a filesystem would mangle must
// stay two workspaces.
func TestAWorkspaceNameThatIsNotAFilenameStillGetsItsOwnDirectory(t *testing.T) {
	s := open(t)

	a := acquire(t, s, spec("group/mods"), "run-1")
	b, err := s.Acquire(spec("group%2Fmods"), "run-2")
	if err != nil {
		t.Fatalf("two distinct names collided on one lease: %v", err)
	}
	if a.Dir() == b.Dir() {
		t.Errorf("two distinct workspace names share one directory %q", a.Dir())
	}
	writeFile(t, filepath.Join(a.Dir(), "a.txt"), "a")
	if _, err := os.Stat(filepath.Join(b.Dir(), "a.txt")); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("a write to one workspace was visible in the other: %v", err)
	}
	if _, _, err := a.Release(1); err != nil {
		t.Fatalf("Release a: %v", err)
	}
	if _, _, err := b.Release(0); err != nil {
		t.Fatalf("Release b: %v", err)
	}
}
