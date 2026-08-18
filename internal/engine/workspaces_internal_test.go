package engine

// Package-internal (white-box) tests for wsManager methods that
// workspaces_test.go, an external test in engine_test, cannot reach directly:
// digests, restore and inputRoot. These feed the cache key and cache-hit
// restoration, but that does not make their own logic exempt from
// verification here: a method with no caller yet is still worth testing
// directly rather than only through whatever eventually calls it.

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/xavidop/senro/internal/cas"
	"github.com/xavidop/senro/internal/plan"
	"github.com/xavidop/senro/internal/workspace"
)

func newTestWSManager(t *testing.T, p *plan.Plan, snap *workspace.Snapshotter) *wsManager {
	t.Helper()
	if snap == nil {
		store, err := cas.Open(t.TempDir())
		if err != nil {
			t.Fatalf("cas.Open: %v", err)
		}
		snap = workspace.NewSnapshotter(store)
	}
	// nil is fine for the scratch store here: none of these tests exercise
	// scratchMounts, ensureScratch or saveScratch, only the workspace-facing
	// methods this file tests directly.
	m, err := newWSManager(t.TempDir(), p, snap, nil, nil, "r1", nil)
	if err != nil {
		t.Fatalf("newWSManager: %v", err)
	}
	return m
}

// mounts refusing a node that names a workspace the plan never declared is
// unreachable through engine.Run: plan.Validate already refuses that plan
// before Run ever builds a wsManager. But the same shape of gap has shown
// up before in this codebase at exactly this layer, reachable directly, not
// reachable through the builder above it. Tested here rather than assumed.
func TestWSManagerMountsRefusesAnUndeclaredWorkspace(t *testing.T) {
	p := &plan.Plan{Version: 1, Workspaces: []plan.WorkspaceSpec{{Name: "known", Scope: "run"}}}
	m := newTestWSManager(t, p, nil)
	n := &plan.Node{ID: "step", Mounts: []plan.MountSpec{{Workspace: "ghost", At: "/x"}}}
	_, err := m.mounts(n)
	if err == nil {
		t.Fatal("mounts of an undeclared workspace must be refused")
	}
	if !strings.Contains(err.Error(), "ghost") {
		t.Errorf("error %q does not name the undeclared workspace", err)
	}
}

// digests must report each mounted workspace's CURRENT recorded digest, in
// the node's own declared order, skip a scratch mount entirely (it is not a
// workspace and never enters a cache key), and carry RO exactly as the
// mount declared it.
func TestWSManagerDigestsReportsCurrentStateInDeclaredOrder(t *testing.T) {
	p := &plan.Plan{Version: 1, Workspaces: []plan.WorkspaceSpec{
		{Name: "a", Scope: "run"}, {Name: "b", Scope: "run"},
	}, Scratch: []plan.ScratchSpec{{Name: "cache", Key: "k"}}}
	m := newTestWSManager(t, p, nil)
	m.record([]wsSnapshot{{Name: "a", Digest: "sha256:aaaa"}, {Name: "b", Digest: "sha256:bbbb"}})

	n := &plan.Node{ID: "step", Mounts: []plan.MountSpec{
		{Workspace: "b", At: "/b", Mode: "ro"},
		{Scratch: "cache", At: "/cache"},
		{Workspace: "a", At: "/a"},
	}}
	got := m.digests(n)
	if len(got) != 2 {
		t.Fatalf("digests returned %d entries, want 2 (the scratch mount must be skipped): %+v", len(got), got)
	}
	if got[0].Name != "b" || got[0].Digest != "sha256:bbbb" || !got[0].RO {
		t.Errorf("first entry = %+v, want workspace b, its recorded digest, and RO true", got[0])
	}
	if got[1].Name != "a" || got[1].Digest != "sha256:aaaa" || got[1].RO {
		t.Errorf("second entry = %+v, want workspace a, its recorded digest, and RO false", got[1])
	}
}

// digests reads BEFORE record: a workspace nobody has snapshotted yet, the
// first mount of a fresh run, must report a zero-value digest rather than
// panicking on a missing map entry, since Run's own upfront check does not
// distinguish a never-snapshotted workspace from one that has been.
func TestWSManagerDigestsOfANeverSnapshottedWorkspaceIsEmpty(t *testing.T) {
	p := &plan.Plan{Version: 1, Workspaces: []plan.WorkspaceSpec{{Name: "fresh", Scope: "run"}}}
	m := newTestWSManager(t, p, nil)
	got := m.digests(&plan.Node{ID: "step", Mounts: []plan.MountSpec{{Workspace: "fresh", At: "/w"}}})
	if len(got) != 1 || got[0].Digest != "" {
		t.Errorf("digests of a never-snapshotted workspace = %+v, want one entry with an empty digest", got)
	}
}

// restore must materialize the digest's content into the workspace's own
// directory and record it as that workspace's current state, so a mount
// realized right afterward sees it without a second round trip through the
// snapshotter.
func TestWSManagerRestoreMaterializesAndRecordsState(t *testing.T) {
	ctx := context.Background()
	store, err := cas.Open(t.TempDir())
	if err != nil {
		t.Fatalf("cas.Open: %v", err)
	}
	snap := workspace.NewSnapshotter(store)

	src := t.TempDir()
	if err := os.WriteFile(filepath.Join(src, "f.txt"), []byte("payload"), 0o644); err != nil {
		t.Fatalf("write source file: %v", err)
	}
	taken, err := snap.Snapshot(ctx, src, workspace.NewExcluder())
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}

	p := &plan.Plan{Version: 1, Workspaces: []plan.WorkspaceSpec{{Name: "build", Scope: "run"}}}
	m := newTestWSManager(t, p, snap)

	if err := m.restore(ctx, "build", taken.Digest); err != nil {
		t.Fatalf("restore: %v", err)
	}
	got, err := os.ReadFile(filepath.Join(m.path("build"), "f.txt"))
	if err != nil {
		t.Fatalf("read restored file: %v", err)
	}
	if string(got) != "payload" {
		t.Errorf("restored file = %q, want %q", got, "payload")
	}
	if m.state["build"] != taken.Digest {
		t.Errorf("state[build] = %q after restore, want %q", m.state["build"], taken.Digest)
	}
}

// restore of a workspace the plan never declared must fail and name it:
// there is no directory for it and nothing to record state under.
func TestWSManagerRestoreOfAnUnknownWorkspaceIsAnError(t *testing.T) {
	p := &plan.Plan{Version: 1, Workspaces: []plan.WorkspaceSpec{{Name: "build", Scope: "run"}}}
	m := newTestWSManager(t, p, nil)
	err := m.restore(context.Background(), "ghost", cas.Digest("sha256:"+strings.Repeat("0", 64)))
	if err == nil {
		t.Fatal("restore of an undeclared workspace must be rejected")
	}
	if got := err.Error(); !strings.Contains(got, "ghost") {
		t.Errorf("error %q does not name the unknown workspace", got)
	}
}

// A malformed digest must surface as an error rather than a partially
// restored directory. Snapshotter.Restore's own Valid() check is what this
// relies on, and this pins that the wiring here does not swallow it.
func TestWSManagerRestoreOfAMalformedDigestIsAnError(t *testing.T) {
	p := &plan.Plan{Version: 1, Workspaces: []plan.WorkspaceSpec{{Name: "build", Scope: "run"}}}
	m := newTestWSManager(t, p, nil)
	if err := m.restore(context.Background(), "build", cas.Digest("not-a-digest")); err == nil {
		t.Error("restore of a malformed digest must be rejected")
	}
}

// inputRoot's first rule: an explicit mount at the step's own WorkDir wins,
// even with another workspace mounted alongside it.
func TestWSManagerInputRootPrefersTheMountAtWorkDir(t *testing.T) {
	p := &plan.Plan{Version: 1, Workspaces: []plan.WorkspaceSpec{
		{Name: "src", Scope: "run"}, {Name: "other", Scope: "run"},
	}}
	m := newTestWSManager(t, p, nil)
	n := &plan.Node{ID: "step", WorkDir: "/src", Mounts: []plan.MountSpec{
		{Workspace: "other", At: "/other"},
		{Workspace: "src", At: "/src"},
	}}
	if got, want := m.inputRoot(n), m.path("src"); got != want {
		t.Errorf("inputRoot = %q, want %q (the mount at WorkDir)", got, want)
	}
}

// The fallback rule: no WorkDir set, but exactly one workspace mount. Its
// path is the only sensible root even though its At is not one of the
// special "." / "/" / "" literals.
func TestWSManagerInputRootFallsBackToTheSoleWorkspaceMount(t *testing.T) {
	p := &plan.Plan{Version: 1, Workspaces: []plan.WorkspaceSpec{{Name: "src", Scope: "run"}}}
	m := newTestWSManager(t, p, nil)
	n := &plan.Node{ID: "step", Mounts: []plan.MountSpec{{Workspace: "src", At: "/src"}}}
	if got, want := m.inputRoot(n), m.path("src"); got != want {
		t.Errorf("inputRoot = %q, want %q (the sole workspace mount)", got, want)
	}
}

// The last resort: a step with no workspace mount at all resolves against
// the coordinator's own working directory, which is where a repository's
// sources are: the case Validate restricts to Inputs-only, never Outputs.
func TestWSManagerInputRootFallsBackToTheCoordinatorCWDWithNoWorkspaceMount(t *testing.T) {
	p := &plan.Plan{Version: 1}
	m := newTestWSManager(t, p, nil)
	cwd, err := os.Getwd()
	if err != nil {
		t.Fatalf("os.Getwd: %v", err)
	}
	if got := m.inputRoot(&plan.Node{ID: "step"}); got != cwd {
		t.Errorf("inputRoot = %q, want the coordinator's cwd %q", got, cwd)
	}
}

// allSnapshotDigests feeds the pin a run writes when it ends badly (see
// engine.go): it must carry every digest record ever recorded, not just
// the latest per workspace, since a step that retries
// takes a fresh snapshot each attempt and an earlier attempt's evidence
// matters as much as the last one's. It must also carry BOTH halves of each
// snapshot, body and index, since a Result only ever stores a workspace's
// body digest and the index is otherwise unprotected by anything (see
// references' doc in internal/cache/gc.go).
func TestWSManagerAllSnapshotDigestsAccumulatesBodyAndIndexAcrossEveryRecordCall(t *testing.T) {
	p := &plan.Plan{Version: 1, Workspaces: []plan.WorkspaceSpec{{Name: "a", Scope: "run"}}}
	m := newTestWSManager(t, p, nil)

	// Two separate record calls, as two attempts of the same step would
	// produce: attempt 1 fails and snapshots once, attempt 2 succeeds and
	// snapshots again with a different digest.
	m.record([]wsSnapshot{{Name: "a",
		Digest: cas.Digest("sha256:" + strings.Repeat("1", 64)),
		Index:  cas.Digest("sha256:" + strings.Repeat("2", 64))}})
	m.record([]wsSnapshot{{Name: "a",
		Digest: cas.Digest("sha256:" + strings.Repeat("3", 64)),
		Index:  cas.Digest("sha256:" + strings.Repeat("4", 64))}})

	got := m.allSnapshotDigests()
	want := map[cas.Digest]bool{
		cas.Digest("sha256:" + strings.Repeat("1", 64)): true,
		cas.Digest("sha256:" + strings.Repeat("2", 64)): true,
		cas.Digest("sha256:" + strings.Repeat("3", 64)): true,
		cas.Digest("sha256:" + strings.Repeat("4", 64)): true,
	}
	if len(got) != len(want) {
		t.Fatalf("allSnapshotDigests returned %d digests, want %d: %v", len(got), len(want), got)
	}
	for _, d := range got {
		if !want[d] {
			t.Errorf("allSnapshotDigests returned an unexpected digest %q", d)
		}
		delete(want, d)
	}
	if len(want) != 0 {
		t.Errorf("allSnapshotDigests is missing %v (a retry's earlier snapshot, or an index, went unrecorded)", want)
	}
}

// A forced snapshot (the ws.snapshot control operation) captures real
// content and pins it, and leaves untouched the ONE map a cache key and a
// later mount are computed from. record() is what would change that map,
// and forceSnapshot deliberately never calls it: that is the whole reason a
// capture an operator asked for cannot become evidence.
func TestWSManagerForceSnapshotNeverBecomesTheWorkspacesRecordedState(t *testing.T) {
	p := &plan.Plan{Version: 1, Workspaces: []plan.WorkspaceSpec{{Name: "src", Scope: "run"}}}
	m := newTestWSManager(t, p, nil)
	settled := cas.Digest("sha256:" + strings.Repeat("a", 64))
	m.record([]wsSnapshot{{Name: "src", Digest: settled}})

	if err := os.WriteFile(filepath.Join(m.path("src"), "clue.txt"), []byte("evidence\n"), 0o644); err != nil {
		t.Fatalf("write into the workspace: %v", err)
	}
	n := &plan.Node{ID: "held", Mounts: []plan.MountSpec{{Workspace: "src", At: "/src"}}}

	snaps, err := m.forceSnapshot(context.Background(), n)
	if err != nil {
		t.Fatalf("forceSnapshot: %v", err)
	}
	if len(snaps) != 1 {
		t.Fatalf("forceSnapshot captured %d workspaces, want 1: %+v", len(snaps), snaps)
	}
	if snaps[0].Name != "src" || snaps[0].Digest == "" || snaps[0].Index == "" {
		t.Errorf("captured %+v, want the named workspace with both digests", snaps[0])
	}
	if snaps[0].Files != 1 || snaps[0].Bytes != 9 {
		t.Errorf("captured %+v, want the one nine-byte file actually on disk", snaps[0])
	}
	if snaps[0].Digest == settled {
		t.Fatal("the capture returned the recorded digest, so this test cannot tell the two apart")
	}

	// The cache key input, unchanged.
	if got := m.digests(n); len(got) != 1 || got[0].Digest != settled {
		t.Errorf("digests() = %+v after a forced snapshot, want the recorded %q: "+
			"a forced capture must never enter a cache key", got, settled)
	}
	// And the digest a later mount of the same workspace carries.
	mounts, err := m.mounts(n)
	if err != nil {
		t.Fatalf("mounts: %v", err)
	}
	if len(mounts) != 1 || mounts[0].Digest != string(settled) {
		t.Errorf("mounts() = %+v after a forced snapshot, want the recorded %q", mounts, settled)
	}

	// Pinned regardless, or the end-of-run sweep could collect exactly what
	// the operator asked to look at.
	pinned := map[cas.Digest]bool{}
	for _, d := range m.allSnapshotDigests() {
		pinned[d] = true
	}
	if !pinned[snaps[0].Digest] || !pinned[snaps[0].Index] {
		t.Errorf("allSnapshotDigests() = %v, missing the forced capture's body or index", m.allSnapshotDigests())
	}
}

// A claim-backed workspace lives in the cluster and has no coordinator-side
// copy, so there is nothing here to capture and a snapshot of the empty
// stand-in directory would be a confident wrong answer. snapshotMounts
// skips it at settle time for the same reason.
func TestWSManagerForceSnapshotSkipsAClaimBackedWorkspace(t *testing.T) {
	p := &plan.Plan{Version: 1, Workspaces: []plan.WorkspaceSpec{
		{Name: "claimed", Scope: "run"}, {Name: "local", Scope: "run"},
	}}
	m := newTestWSManager(t, p, nil)
	n := &plan.Node{
		ID: "held",
		Mounts: []plan.MountSpec{
			{Workspace: "claimed", At: "/claimed"},
			{Workspace: "local", At: "/local"},
		},
		Executor: &plan.ExecutorSpec{Kind: "k8s", Claims: map[string]string{"claimed": "pvc-1"}},
	}

	if got := capturableWorkspaces(n); len(got) != 1 || got[0] != "local" {
		t.Fatalf("capturableWorkspaces = %v, want only the workspace with a coordinator-side copy", got)
	}
	snaps, err := m.forceSnapshot(context.Background(), n)
	if err != nil {
		t.Fatalf("forceSnapshot: %v", err)
	}
	if len(snaps) != 1 || snaps[0].Name != "local" {
		t.Errorf("forceSnapshot captured %+v, want only the workspace it can honestly capture", snaps)
	}
}
