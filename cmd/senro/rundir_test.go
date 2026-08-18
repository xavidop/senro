package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestResolveRunDirAcceptsAPath(t *testing.T) {
	dir := t.TempDir()
	got, err := resolveRunDir(dir)
	if err != nil {
		t.Fatalf("resolveRunDir: %v", err)
	}
	if got != dir {
		t.Errorf("resolveRunDir(%q) = %q", dir, got)
	}
}

func TestResolveRunDirAcceptsARunIDUnderRuns(t *testing.T) {
	base := t.TempDir()
	t.Chdir(base)
	want := filepath.Join(base, "runs", "r7")
	if err := os.MkdirAll(want, 0o755); err != nil {
		t.Fatalf("seed: %v", err)
	}
	got, err := resolveRunDir("r7")
	if err != nil {
		t.Fatalf("resolveRunDir: %v", err)
	}
	if got != want {
		t.Errorf("resolveRunDir(\"r7\") = %q, want %q", got, want)
	}
}

func TestResolveRunDirDefaultsToTheNewestRun(t *testing.T) {
	base := t.TempDir()
	t.Chdir(base)
	older := filepath.Join(base, "runs", "a")
	newer := filepath.Join(base, "runs", "b")
	for _, d := range []string{older, newer} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}
	past := time.Now().Add(-time.Hour)
	if err := os.Chtimes(older, past, past); err != nil {
		t.Fatalf("age: %v", err)
	}
	got, err := resolveRunDir("")
	if err != nil {
		t.Fatalf("resolveRunDir: %v", err)
	}
	if got != newer {
		t.Errorf("resolveRunDir(\"\") = %q, want the newest run %q", got, newer)
	}
}

// The negative half. Silently picking a directory that does not exist would
// turn every later error into a confusing one about a missing file.
func TestResolveRunDirFailsWhenThereIsNoRun(t *testing.T) {
	t.Chdir(t.TempDir())
	if _, err := resolveRunDir(""); err == nil {
		t.Error("resolveRunDir found a run in an empty directory")
	}
	if _, err := resolveRunDir("nope"); err == nil {
		t.Error("resolveRunDir accepted a run ID with no directory")
	}
}
