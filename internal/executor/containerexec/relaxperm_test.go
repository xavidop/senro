package containerexec

import (
	"os"
	"path/filepath"
	"testing"
)

func mustMode(t *testing.T, path string) os.FileMode {
	t.Helper()
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat(%q): %v", path, err)
	}
	return fi.Mode().Perm()
}

// TestRelaxMountForDeclaredUserWidensAWritableMount. This is what a step's
// workspace needs: a declared uid that owns nothing in the tree must still
// be able to create a file in it.
func TestRelaxMountForDeclaredUserWidensAWritableMount(t *testing.T) {
	root := t.TempDir()
	sub := filepath.Join(root, "sub")
	if err := os.Mkdir(sub, 0o755); err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(sub, "input.txt")
	if err := os.WriteFile(file, []byte("seeded\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	if err := relaxMountForDeclaredUser(root, false); err != nil {
		t.Fatalf("relaxMountForDeclaredUser: %v", err)
	}

	for _, dir := range []string{root, sub} {
		if mode := mustMode(t, dir); mode&0o007 != 0o007 {
			t.Errorf("%s: mode %o does not grant other rwx", dir, mode)
		}
	}
	if mode := mustMode(t, file); mode&0o006 != 0o006 {
		t.Errorf("%s: mode %o does not grant other rw", file, mode)
	}
}

// TestRelaxMountForDeclaredUserOnAReadOnlyMountNeverAddsWrite. A read-only
// mount must stay unwritable from the declared user's side too: widening it
// to rw would let a "read-only" step write through a mount senro promised
// it could not.
func TestRelaxMountForDeclaredUserOnAReadOnlyMountNeverAddsWrite(t *testing.T) {
	root := t.TempDir()
	file := filepath.Join(root, "input.txt")
	if err := os.WriteFile(file, []byte("seeded\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	if err := relaxMountForDeclaredUser(root, true); err != nil {
		t.Fatalf("relaxMountForDeclaredUser: %v", err)
	}

	if mode := mustMode(t, root); mode&0o002 != 0 {
		t.Errorf("root %s: mode %o grants other-write on a read-only mount", root, mode)
	}
	if mode := mustMode(t, file); mode&0o002 != 0 {
		t.Errorf("file %s: mode %o grants other-write on a read-only mount", file, mode)
	}
}

// TestRelaxOtherBitsKeepsExistingBitsAndIsIdempotent. The fix must only ADD
// bits: an executable script must stay executable, and running it twice
// (a retried step reuses the same mount) must not fail or change anything
// further.
func TestRelaxOtherBitsKeepsExistingBitsAndIsIdempotent(t *testing.T) {
	root := t.TempDir()
	script := filepath.Join(root, "run.sh")
	if err := os.WriteFile(script, []byte("#!/bin/sh\n"), 0o750); err != nil {
		t.Fatal(err)
	}

	if err := relaxOtherBits(root, 0o007, 0o006); err != nil {
		t.Fatalf("first pass: %v", err)
	}
	first := mustMode(t, script)
	if first&0o100 == 0 {
		t.Fatalf("owner-execute bit was lost: mode %o", first)
	}
	if first&0o006 != 0o006 {
		t.Fatalf("other rw was not added: mode %o", first)
	}

	if err := relaxOtherBits(root, 0o007, 0o006); err != nil {
		t.Fatalf("second pass: %v", err)
	}
	if second := mustMode(t, script); second != first {
		t.Errorf("mode changed on a repeat pass: %o -> %o", first, second)
	}
}

// TestRelaxOtherBitsLeavesSymlinksAlone. A symlink's own mode is not a
// meaningful permission on Linux, and os.Chmod on one follows it to a
// target that may sit outside the tree senro is allowed to widen.
func TestRelaxOtherBitsLeavesSymlinksAlone(t *testing.T) {
	root := t.TempDir()
	outside := filepath.Join(t.TempDir(), "target")
	if err := os.WriteFile(outside, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(root, "link")
	if err := os.Symlink(outside, link); err != nil {
		t.Fatal(err)
	}

	if err := relaxOtherBits(root, 0o007, 0o006); err != nil {
		t.Fatalf("relaxOtherBits: %v", err)
	}

	if mode := mustMode(t, outside); mode != 0o600 {
		t.Errorf("the symlink's target was widened: mode %o", mode)
	}
}

// TestRelaxSecretDirAndFileForDeclaredUser mirrors secretdir's own scheme:
// 0700 around a 0600 file. Only the execute bit on the directory and the
// read bit on the file may be added, so the value stays unlistable and
// unwritable to anything that is not the coordinator.
func TestRelaxSecretDirAndFileForDeclaredUser(t *testing.T) {
	dir := t.TempDir()
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(dir, "token")
	if err := os.WriteFile(file, []byte("value-1234"), 0o600); err != nil {
		t.Fatal(err)
	}

	if err := relaxSecretDirForDeclaredUser(dir); err != nil {
		t.Fatalf("relaxSecretDirForDeclaredUser: %v", err)
	}
	if err := relaxSecretFileForDeclaredUser(file); err != nil {
		t.Fatalf("relaxSecretFileForDeclaredUser: %v", err)
	}

	if mode := mustMode(t, dir); mode != 0o701 {
		t.Errorf("dir mode = %o, want 701 (traversable, not listable)", mode)
	}
	if mode := mustMode(t, file); mode != 0o604 {
		t.Errorf("file mode = %o, want 604 (readable, not writable)", mode)
	}
}
