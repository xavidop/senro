package workspace_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/xavidop/senro/internal/workspace"
)

// The test is the equivalence itself: Measure and Snapshot.Bytes agree for
// the same tree and excluder. If they stop agreeing, a size bound is
// enforced against a different file set than the digest describes, which is
// invisible from either side.
func TestMeasureAgreesWithASnapshotOfTheSameTree(t *testing.T) {
	snap, _ := snapshotter(t)
	root := t.TempDir()

	write(t, filepath.Join(root, "a.txt"), "hello")
	write(t, filepath.Join(root, "sub", "b.bin"), "0123456789")
	write(t, filepath.Join(root, "sub", "skip.tmp"), "excluded from both")
	if err := os.Symlink("a.txt", filepath.Join(root, "link")); err != nil {
		t.Fatalf("symlink: %v", err)
	}

	ex, err := workspace.ExcluderFor(root, []string{"**/*.tmp"}, false)
	if err != nil {
		t.Fatalf("ExcluderFor: %v", err)
	}

	s, err := snap.Snapshot(context.Background(), root, ex)
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	got, err := workspace.Measure(root, ex)
	if err != nil {
		t.Fatalf("Measure: %v", err)
	}
	if got != s.Bytes {
		t.Errorf("Measure = %d, snapshot reported %d; a size bound would be enforced against a "+
			"different set of files than the digest describes", got, s.Bytes)
	}
	if got != 15 {
		t.Errorf("Measure = %d, want 15 (5 + 10, with the .tmp excluded)", got)
	}
}

func TestMeasureOfAnEmptyDirectoryIsZero(t *testing.T) {
	got, err := workspace.Measure(t.TempDir(), workspace.NewExcluder())
	if err != nil {
		t.Fatalf("Measure: %v", err)
	}
	if got != 0 {
		t.Errorf("Measure of an empty directory = %d, want 0", got)
	}
}

// A persistent workspace's directory is created before its first run and
// removed by an eviction, so Measure meets a missing root in the ordinary
// course of events rather than only when something is wrong. It reports
// zero, which is what an evicted workspace holds.
func TestMeasureOfAMissingDirectoryIsZero(t *testing.T) {
	got, err := workspace.Measure(filepath.Join(t.TempDir(), "gone"), workspace.NewExcluder())
	if err != nil {
		t.Fatalf("Measure of a missing directory must not be an error: %v", err)
	}
	if got != 0 {
		t.Errorf("Measure of a missing directory = %d, want 0", got)
	}
}

func write(t *testing.T, p, body string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
		t.Fatalf("write %s: %v", p, err)
	}
}
