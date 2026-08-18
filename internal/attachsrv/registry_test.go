package attachsrv_test

import (
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/xavidop/senro/internal/attachsrv"
)

// isolateRegistry points registry discovery at a throwaway directory
// instead of the operator's real runtime dir, by controlling exactly the
// env vars attachsrv's resolution reads: $HOME (os.UserCacheDir's source)
// and $XDG_RUNTIME_DIR (linux's first choice). t.Setenv restores both and
// forbids t.Parallel, correctly: these tests share process-global state.
func isolateRegistry(t *testing.T) {
	t.Helper()
	dir := t.TempDir()
	t.Setenv("HOME", dir)
	t.Setenv("XDG_RUNTIME_DIR", dir)
}

func testEntry(t *testing.T) attachsrv.Entry {
	t.Helper()
	return attachsrv.Entry{
		PID:           os.Getpid(),
		Socket:        filepath.Join(t.TempDir(), "engine.sock"),
		RunID:         "run-1",
		Pipeline:      "ci",
		CWD:           "/repo",
		StartedAt:     time.Now().UTC(),
		EngineVersion: "v0.0.0-test",
	}
}

func TestRegisteredEntryIsDiscoverable(t *testing.T) {
	isolateRegistry(t)
	entry := testEntry(t)

	unregister, err := attachsrv.Register(entry)
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	defer unregister()

	entries, err := attachsrv.Discover()
	if err != nil {
		t.Fatalf("Discover: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("Discover returned %d entries, want 1: %+v", len(entries), entries)
	}
	got := entries[0]
	if got.PID != entry.PID || got.Socket != entry.Socket || got.RunID != entry.RunID ||
		got.Pipeline != entry.Pipeline || got.CWD != entry.CWD || got.EngineVersion != entry.EngineVersion {
		t.Errorf("Discover()[0] = %+v, want %+v", got, entry)
	}
	if !got.StartedAt.Equal(entry.StartedAt) {
		t.Errorf("StartedAt = %v, want %v", got.StartedAt, entry.StartedAt)
	}
}

// Dead-pid reaping matters most in this file: `senro attach` with no
// arguments has to pick the one live run, so a stale entry from a crashed
// engine must not look live. This pid is larger than either darwin or
// linux allows, so Kill(pid, 0) is guaranteed ESRCH rather than
// coincidentally hitting a live process, which is the pid-reuse flakiness
// a "spawn and wait for it to exit" approach would risk.
const definitelyDeadPID = 1 << 30

func TestDiscoverReapsAnEntryWhosePIDIsDead(t *testing.T) {
	isolateRegistry(t)
	entry := testEntry(t)
	entry.PID = definitelyDeadPID

	unregister, err := attachsrv.Register(entry)
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	defer unregister()

	entries, err := attachsrv.Discover()
	if err != nil {
		t.Fatalf("Discover: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("Discover returned %d entries for a dead pid, want 0 (reaped): %+v", len(entries), entries)
	}

	// Reaped means removed, not just filtered on the way out: a second
	// Discover must not still be paying to skip it, and nothing should be
	// left on disk for a future scan to trip over.
	entries2, err := attachsrv.Discover()
	if err != nil {
		t.Fatalf("second Discover: %v", err)
	}
	if len(entries2) != 0 {
		t.Fatalf("second Discover returned %d entries, want 0", len(entries2))
	}
}

func TestDiscoverReturnsALiveEntryAlongsideAReapedOne(t *testing.T) {
	isolateRegistry(t)

	live := testEntry(t)
	dead := testEntry(t)
	dead.PID = definitelyDeadPID

	unregisterLive, err := attachsrv.Register(live)
	if err != nil {
		t.Fatalf("Register(live): %v", err)
	}
	defer unregisterLive()
	unregisterDead, err := attachsrv.Register(dead)
	if err != nil {
		t.Fatalf("Register(dead): %v", err)
	}
	defer unregisterDead()

	entries, err := attachsrv.Discover()
	if err != nil {
		t.Fatalf("Discover: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("Discover returned %d entries, want exactly the live one: %+v", len(entries), entries)
	}
	if entries[0].PID != live.PID {
		t.Errorf("Discover()[0].PID = %d, want %d (the live entry, not the reaped one)", entries[0].PID, live.PID)
	}
}

// Guards against a socket file outliving the engine that created it: a
// graceful Close unlinks its own socket, but SIGKILL, os.Exit and an
// unrecovered panic all skip that, and the file would then persist under
// the registry directory indefinitely (on darwin, past reboot). A file at
// entry.Socket for a pid Discover independently proves dead is exactly the
// signature a hard exit leaves, so this needs a real file, not a real
// listener.
func TestDiscoverReapsTheDeadEntrysSocketFileToo(t *testing.T) {
	isolateRegistry(t)
	entry := testEntry(t)
	entry.PID = definitelyDeadPID

	unregister, err := attachsrv.Register(entry)
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	defer unregister()

	if err := os.WriteFile(entry.Socket, nil, 0o600); err != nil {
		t.Fatalf("write fake socket file: %v", err)
	}

	entries, err := attachsrv.Discover()
	if err != nil {
		t.Fatalf("Discover: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("Discover returned %d entries for a dead pid, want 0 (reaped): %+v", len(entries), entries)
	}

	if _, err := os.Stat(entry.Socket); !os.IsNotExist(err) {
		t.Errorf("leaked socket file %s still exists after Discover reaped its dead entry (stat err: %v)",
			entry.Socket, err)
	}
}

// TestDiscoverLeavesALiveEntrysSocketFileAlone is Discover's safety
// boundary: it must only ever remove a socket belonging to an entry it has
// independently proven dead, never merely because the socket happens to
// exist.
func TestDiscoverLeavesALiveEntrysSocketFileAlone(t *testing.T) {
	isolateRegistry(t)
	live := testEntry(t)

	unregister, err := attachsrv.Register(live)
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	defer unregister()

	if err := os.WriteFile(live.Socket, nil, 0o600); err != nil {
		t.Fatalf("write fake socket file: %v", err)
	}

	entries, err := attachsrv.Discover()
	if err != nil {
		t.Fatalf("Discover: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("Discover returned %d entries, want 1 (the live one)", len(entries))
	}

	if _, err := os.Stat(live.Socket); err != nil {
		t.Errorf("a live entry's socket file was removed by Discover: %v", err)
	}
}

func TestDiscoverReturnsNothingWhenNoRegistryDirExistsYet(t *testing.T) {
	isolateRegistry(t)

	entries, err := attachsrv.Discover()
	if err != nil {
		t.Fatalf("Discover on a never-created registry dir: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("Discover = %+v, want empty", entries)
	}
}

func TestUnregisterRemovesTheEntry(t *testing.T) {
	isolateRegistry(t)
	entry := testEntry(t)

	unregister, err := attachsrv.Register(entry)
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	unregister()

	entries, err := attachsrv.Discover()
	if err != nil {
		t.Fatalf("Discover: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("Discover after unregister = %+v, want empty", entries)
	}

	// Idempotent, matching every other closeable/undo-able thing in this
	// codebase (eventlog.Ledger.Close, Hub.Close, FileSource.Close, ...).
	unregister()
}

func TestRegistryEntryFileIsMode0600(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("unix file permission bits do not apply on windows")
	}
	isolateRegistry(t)
	entry := testEntry(t)

	unregister, err := attachsrv.Register(entry)
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	defer unregister()

	path := filepath.Join(os.Getenv("HOME"), "Library", "Caches", "senro", entryFileName(entry.PID))
	if runtime.GOOS == "linux" {
		path = filepath.Join(os.Getenv("XDG_RUNTIME_DIR"), "senro", entryFileName(entry.PID))
	}
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat registry entry file: %v", err)
	}
	if mode := fi.Mode().Perm(); mode != 0o600 {
		t.Errorf("registry entry file mode = %o, want 0600", mode)
	}
}

func TestRegistryDirIsMode0700(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("unix file permission bits do not apply on windows")
	}
	isolateRegistry(t)
	entry := testEntry(t)

	unregister, err := attachsrv.Register(entry)
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	defer unregister()

	dir := filepath.Join(os.Getenv("HOME"), "Library", "Caches", "senro")
	if runtime.GOOS == "linux" {
		dir = filepath.Join(os.Getenv("XDG_RUNTIME_DIR"), "senro")
	}
	fi, err := os.Stat(dir)
	if err != nil {
		t.Fatalf("stat registry dir: %v", err)
	}
	if mode := fi.Mode().Perm(); mode != 0o700 {
		t.Errorf("registry dir mode = %o, want 0700", mode)
	}
}

// Dir()'s chmod must apply to a directory that ALREADY exists, since
// os.MkdirAll is a no-op there. TestRegistryDirIsMode0700 only covers the
// freshly-created case, where MkdirAll's own 0700 makes the chmod
// redundant, so a Dir() that dropped the chmod would still pass it.
func TestDirTightensAnAlreadyLooseDirectory(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("unix file permission bits do not apply on windows")
	}
	isolateRegistry(t)

	dir := filepath.Join(os.Getenv("HOME"), "Library", "Caches", "senro")
	if runtime.GOOS == "linux" {
		dir = filepath.Join(os.Getenv("XDG_RUNTIME_DIR"), "senro")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("pre-create a loose registry dir: %v", err)
	}
	if fi, err := os.Stat(dir); err != nil || fi.Mode().Perm() != 0o755 {
		t.Fatalf("setup: pre-created dir mode = %v, err = %v, want exactly 0755 before Dir() ever runs", fi, err)
	}

	got, err := attachsrv.Dir()
	if err != nil {
		t.Fatalf("Dir: %v", err)
	}
	if got != dir {
		t.Fatalf("Dir() = %q, want %q", got, dir)
	}

	fi, err := os.Stat(dir)
	if err != nil {
		t.Fatalf("stat registry dir: %v", err)
	}
	if mode := fi.Mode().Perm(); mode != 0o700 {
		t.Errorf("registry dir mode after Dir() = %o, want 0700 (tightened down from the pre-existing 0755)", mode)
	}
}

func TestRegisterRequiresASocketPath(t *testing.T) {
	isolateRegistry(t)
	entry := testEntry(t)
	entry.Socket = ""

	_, err := attachsrv.Register(entry)
	if err == nil {
		t.Fatal("Register with no Socket path succeeded, want an error — there is nothing to discover without one")
	}
}

// A remove-then-symlink update of "latest" has a window where it points at
// nothing, and lets two concurrent Registers race each other's
// Remove/Symlink pair. Register builds each symlink under a per-pid temp
// name and installs it with an atomic os.Rename: several engines starting
// at once must always leave "latest" resolving to some real, currently
// registered entry, never missing, never dangling.
func TestConcurrentRegistersLeaveALatestSymlinkPointingAtOneOfThem(t *testing.T) {
	isolateRegistry(t)

	const n = 8
	entries := make([]attachsrv.Entry, n)
	for i := range entries {
		e := testEntry(t)
		e.PID = 10_000 + i
		entries[i] = e
	}

	var wg sync.WaitGroup
	unregisters := make([]func(), n)
	errs := make([]error, n)
	for i, e := range entries {
		wg.Add(1)
		go func(i int, e attachsrv.Entry) {
			defer wg.Done()
			u, err := attachsrv.Register(e)
			unregisters[i] = u
			errs[i] = err
		}(i, e)
	}
	wg.Wait()
	defer func() {
		for _, u := range unregisters {
			if u != nil {
				u()
			}
		}
	}()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("Register(entries[%d]): %v", i, err)
		}
	}

	dir := filepath.Join(os.Getenv("HOME"), "Library", "Caches", "senro")
	if runtime.GOOS == "linux" {
		dir = filepath.Join(os.Getenv("XDG_RUNTIME_DIR"), "senro")
	}
	latest := filepath.Join(dir, "latest")

	target, err := os.Readlink(latest)
	if err != nil {
		t.Fatalf("readlink latest: %v — it must always resolve to something after N concurrent Registers, never be missing or dangling", err)
	}

	found := false
	for _, e := range entries {
		if target == entryFileName(e.PID) {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("latest -> %q, want it to name one of the %d concurrently registered entries", target, n)
	}

	// And the target it names must itself actually exist: a dangling
	// symlink left mid-race would still "resolve" (Readlink succeeds) while
	// pointing at nothing real.
	if _, err := os.Stat(filepath.Join(dir, target)); err != nil {
		t.Errorf("latest -> %q, but that entry file does not exist: %v", target, err)
	}
}

func TestDiscoverSkipsACorruptEntryRatherThanFailingEntirely(t *testing.T) {
	isolateRegistry(t)
	good := testEntry(t)

	unregister, err := attachsrv.Register(good)
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	defer unregister()

	// Plant a corrupt sibling entry directly, bypassing Register; this is
	// what a process killed mid-write (before Register's atomic rename
	// completes) would leave behind.
	dir := filepath.Join(os.Getenv("HOME"), "Library", "Caches", "senro")
	if runtime.GOOS == "linux" {
		dir = filepath.Join(os.Getenv("XDG_RUNTIME_DIR"), "senro")
	}
	if err := os.WriteFile(filepath.Join(dir, "999999998.json"), []byte("{not json"), 0o600); err != nil {
		t.Fatalf("plant corrupt entry: %v", err)
	}

	entries, err := attachsrv.Discover()
	if err != nil {
		t.Fatalf("Discover: %v", err)
	}
	if len(entries) != 1 || entries[0].PID != good.PID {
		t.Fatalf("Discover = %+v, want only the one good entry", entries)
	}
}

func entryFileName(pid int) string {
	return strconv.Itoa(pid) + ".json"
}
