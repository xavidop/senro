package secretdir_test

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/xavidop/senro/internal/executor/secretdir"
)

func TestEnsureIsIdempotentAndCreatesA0700Directory(t *testing.T) {
	t.Setenv("XDG_RUNTIME_DIR", t.TempDir())
	var d secretdir.Dir
	first, err := d.Ensure()
	if err != nil {
		t.Fatalf("Ensure: %v", err)
	}
	second, err := d.Ensure()
	if err != nil || second != first {
		t.Fatalf("Ensure twice = %q, %v; want %q", second, err, first)
	}
	fi, err := os.Stat(first)
	if err != nil {
		t.Fatal(err)
	}
	if perm := fi.Mode().Perm(); perm != 0o700 {
		t.Errorf("directory mode = %o, want 700", perm)
	}
}

func TestRemoveIsSafeTwiceAndLeavesNothingBehind(t *testing.T) {
	t.Setenv("XDG_RUNTIME_DIR", t.TempDir())
	var d secretdir.Dir
	p, err := d.Put("Token", []byte("hunter2hunter2"))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := d.Remove(); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	if err := d.Remove(); err != nil {
		t.Fatalf("second Remove: %v", err)
	}
	if _, err := os.Stat(p); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("the secret file survived Remove: %v", err)
	}
}

// Put must make a separator-bearing name a single safe path element rather
// than let it escape the directory, and write the value verbatim at 0600.
func TestPutSanitizesTheNameAndWritesA0600File(t *testing.T) {
	t.Setenv("XDG_RUNTIME_DIR", t.TempDir())
	var d secretdir.Dir
	p, err := d.Put("Registry.Token", []byte("value-aaaaaaaa"))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if filepath.Base(p) != "Registry_Token" {
		t.Errorf("file name %q; a dot in a field name must not become a path separator",
			filepath.Base(p))
	}
	fi, err := os.Stat(p)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if fi.Mode().Perm() != 0o600 {
		t.Errorf("file mode %v, want 0600", fi.Mode().Perm())
	}
	body, err := os.ReadFile(p)
	if err != nil || string(body) != "value-aaaaaaaa" {
		t.Fatalf("the file does not hold the value: (%q, %v)", body, err)
	}
}

// The zero-value case: Remove with nothing created must not try to delete
// anything.
func TestRemoveWithNothingCreatedCostsNothing(t *testing.T) {
	var d secretdir.Dir
	if err := d.Remove(); err != nil {
		t.Fatalf("Remove on an untouched Dir: %v", err)
	}
}

// Path is a pure accessor: empty before anything exists, exactly what
// Ensure returned afterward.
func TestPathReflectsWhatEnsureCreated(t *testing.T) {
	t.Setenv("XDG_RUNTIME_DIR", t.TempDir())
	var d secretdir.Dir
	if p := d.Path(); p != "" {
		t.Fatalf("Path on an untouched Dir = %q, want empty", p)
	}
	created, err := d.Ensure()
	if err != nil {
		t.Fatalf("Ensure: %v", err)
	}
	if p := d.Path(); p != created {
		t.Fatalf("Path() = %q, want %q", p, created)
	}
}
