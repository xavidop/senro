package engine

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/xavidop/senro/internal/cas"
	"github.com/xavidop/senro/internal/executor"
	"github.com/xavidop/senro/internal/workspace"
)

func driftEntry(path, digest string) workspace.Entry {
	return workspace.Entry{Path: path, Mode: 0o644, Size: 1, Digest: cas.Digest("sha256:" + strings.Repeat(digest, 64))}
}

func TestGeneratedDriftReportsEveryChangedPathSorted(t *testing.T) {
	before := workspace.Index{Version: 1, Entries: []workspace.Entry{
		driftEntry("keep.go", "a"), driftEntry("edit.go", "b"), driftEntry("gone.go", "c"),
	}}
	after := workspace.Index{Version: 1, Entries: []workspace.Entry{
		driftEntry("keep.go", "a"), driftEntry("edit.go", "d"), driftEntry("added.go", "e"),
	}}
	got := generatedDrift(before, after)
	want := []string{"added.go", "edit.go", "gone.go"}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("generatedDrift = %v, want %v", got, want)
	}
	if d := generatedDrift(before, before); len(d) != 0 {
		t.Errorf("generatedDrift of an index against itself = %v, want none", d)
	}
}

func TestDriftSentenceListsTenPathsThenCountsTheRest(t *testing.T) {
	paths := make([]string, 13)
	for i := range paths {
		paths[i] = fmt.Sprintf("f%02d.go", i)
	}
	got := driftSentence("gen", paths)
	want := "generated files are out of date in workspace gen: 13 changed: " +
		"f00.go, f01.go, f02.go, f03.go, f04.go, f05.go, f06.go, f07.go, f08.go, f09.go and 3 more"
	if got != want {
		t.Errorf("driftSentence =\n %q\nwant\n %q", got, want)
	}
	if got := driftSentence("gen", paths[:2]); got !=
		"generated files are out of date in workspace gen: 2 changed: f00.go, f01.go" {
		t.Errorf("driftSentence for two paths = %q", got)
	}
}

// A body put back by a cache hit has no recorded index, so mountedIndex
// rebuilds it from the body; the answer must equal the index stored with the
// snapshot, and the scratch extraction must not outlive the call.
func TestMountedIndexRebuildsTheIndexOfABodyWithNoRecordedOne(t *testing.T) {
	ctx := context.Background()
	store, err := cas.Open(filepath.Join(t.TempDir(), "cas"))
	if err != nil {
		t.Fatalf("cas.Open: %v", err)
	}
	snap := workspace.NewSnapshotter(store)
	src := t.TempDir()
	if err := os.MkdirAll(filepath.Join(src, "sub"), 0o755); err != nil {
		t.Fatal(err)
	}
	for _, f := range []string{"a.txt", "sub/b.txt"} {
		if err := os.WriteFile(filepath.Join(src, f), []byte(f), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	taken, err := snap.Snapshot(ctx, src, nil)
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	want, err := snap.LoadIndex(ctx, taken.Index)
	if err != nil {
		t.Fatalf("LoadIndex: %v", err)
	}

	runDir := filepath.Join(t.TempDir(), "run")
	m := &wsManager{snap: snap, runDir: runDir, indexOf: map[cas.Digest]cas.Digest{}}
	got, err := m.mountedIndex(ctx, taken.Digest)
	if err != nil {
		t.Fatalf("mountedIndex: %v", err)
	}
	if len(got.Entries) == 0 {
		t.Fatal("rebuilt index is empty")
	}
	if d := workspace.Diff(want, got); len(d) != 0 {
		t.Errorf("rebuilt index differs from the stored one: %+v", d)
	}
	if left, _ := os.ReadDir(runDir); len(left) != 0 {
		t.Errorf("mountedIndex left %d entries in the run directory", len(left))
	}

	// The recorded path and the empty-body path.
	m.state = map[string]cas.Digest{}
	m.record([]wsSnapshot{{Name: "w", Digest: taken.Digest, Index: taken.Index}})
	if got, err := m.mountedIndex(ctx, taken.Digest); err != nil || len(workspace.Diff(want, got)) != 0 {
		t.Errorf("mountedIndex via the recorded index = %v, %v", got, err)
	}
	if got, err := m.mountedIndex(ctx, ""); err != nil || len(got.Entries) != 0 {
		t.Errorf("mountedIndex of an empty body = %v, %v, want an empty index", got, err)
	}

	// The rebuild honours cancellation: it only reads, so a cancelled run
	// abandons it rather than extracting a body nobody will look at.
	fresh := &wsManager{snap: snap, runDir: runDir, indexOf: map[cas.Digest]cas.Digest{}}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := fresh.mountedIndex(cancelled, taken.Digest); !errors.Is(err, context.Canceled) {
		t.Errorf("mountedIndex under a cancelled ctx = %v, want context.Canceled", err)
	}
	if left, _ := os.ReadDir(runDir); len(left) != 0 {
		t.Errorf("a cancelled rebuild left %d entries in the run directory", len(left))
	}
}

// An index that cannot be read says nothing about drift: the error is
// infrastructure, so retry predicates and the analyzer read it as such.
func TestCheckGeneratedIndexFailureIsInfrastructure(t *testing.T) {
	err := checkGeneratedInfra("codegen", "gen", "read the file index of the post-step snapshot",
		fmt.Errorf("cas: %w", cas.ErrNotFound))
	if !executor.IsInfra(err) {
		t.Errorf("err = %v, want it to wrap executor.ErrInfra", err)
	}
	if !errors.Is(err, cas.ErrNotFound) {
		t.Errorf("err = %v, want it to keep the cause", err)
	}
	for _, s := range []string{`step "codegen"`, `workspace "gen"`, "infrastructure", "not drift"} {
		if !strings.Contains(err.Error(), s) {
			t.Errorf("err = %q, want it to contain %q", err, s)
		}
	}
}
