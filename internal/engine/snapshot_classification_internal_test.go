package engine

// The exit-versus-error distinction: retry.OnInfra keys off
// errors.Is(err, executor.ErrInfra), and snapshotMounts produces two
// differently-shaped errors through the same attemptResult.err field:
//
//   - A genuine capture failure (the workspace vanished, a disk error) is
//     infrastructure, and retrying the attempt is right.
//   - A read-only mount whose content changed is a pipeline authoring
//     error, and retrying reproduces the same violation, so it is
//     deliberately NOT wrapped in executor.ErrInfra.
//
// Pinned against executor.IsInfra directly: the event log carries only
// text, which cannot exercise errors.Is at all.

import (
	"context"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/xavidop/senro/internal/cas"
	"github.com/xavidop/senro/internal/eventlog"
	"github.com/xavidop/senro/internal/executor"
	"github.com/xavidop/senro/internal/plan"
	"github.com/xavidop/senro/internal/sink"
)

// snapshotFakeSandbox reports a fixed Snapshot result (or error) regardless
// of which mount it is asked about, so a test can control exactly what
// snapshotMounts sees without a real filesystem or CAS underneath it.
type snapshotFakeSandbox struct {
	snap executor.Snapshot
	err  error
}

func (s *snapshotFakeSandbox) ObservedPlatform(context.Context) (executor.Platform, error) {
	return executor.Platform{}, nil
}
func (s *snapshotFakeSandbox) Snapshot(context.Context, string) (executor.Snapshot, error) {
	return s.snap, s.err
}
func (s *snapshotFakeSandbox) PutSecret(context.Context, string, []byte) (string, error) {
	return "", nil
}
func (s *snapshotFakeSandbox) Run(context.Context, executor.Cmd, io.Writer, io.Writer) (int, error) {
	return 0, nil
}
func (s *snapshotFakeSandbox) Close(context.Context, bool) error { return nil }

func newTestRunCore(t *testing.T, ws *wsManager) *runCore {
	t.Helper()
	ledger, err := eventlog.Open(t.TempDir())
	if err != nil {
		t.Fatalf("eventlog.Open: %v", err)
	}
	t.Cleanup(func() { _ = ledger.Close() })
	return &runCore{ledger: ledger, sink: sink.Nop(), runID: "r1", ws: ws}
}

// A workspace a step deleted mid-run: Sandbox.Snapshot fails for a reason
// that has nothing to do with the pipeline's own correctness, and the
// wrapping here must preserve the ErrInfra chain localexec already attaches
// (see localexec.go's Snapshot), not flatten it into an opaque string.
func TestSnapshotMountsWrapsAGenuineCaptureFailureAsInfra(t *testing.T) {
	p := &plan.Plan{Version: 1, Workspaces: []plan.WorkspaceSpec{{Name: "w", Scope: "run"}}}
	m := newTestWSManager(t, p, nil)
	rc := newTestRunCore(t, m)

	n := &plan.Node{ID: "vanish", Mounts: []plan.MountSpec{{Workspace: "w", At: "/w"}}}
	mounts, err := m.mounts(n)
	if err != nil {
		t.Fatalf("mounts: %v", err)
	}

	sb := &snapshotFakeSandbox{err: fmt.Errorf("localexec: %w: workspace vanished", executor.ErrInfra)}
	_, gotErr := rc.snapshotMounts(context.Background(), sb, n, mounts, 1)
	if gotErr == nil {
		t.Fatal("a genuine capture failure must be reported as an error")
	}
	if !executor.IsInfra(gotErr) {
		t.Errorf("errors.Is(err, executor.ErrInfra) = false for %v, want true: "+
			"a capture failure is infrastructure, and retry.OnInfra must be able to see that", gotErr)
	}
}

// A read-only mount whose content changed is the opposite case: the step
// itself is what broke its own contract, so this must NOT match
// executor.IsInfra, or retry.OnInfra would retry an attempt certain to
// reproduce the exact same violation.
func TestSnapshotMountsReadOnlyViolationIsNotInfra(t *testing.T) {
	p := &plan.Plan{Version: 1, Workspaces: []plan.WorkspaceSpec{{Name: "src", Scope: "run"}}}
	m := newTestWSManager(t, p, nil)
	baseline := "sha256:" + strings.Repeat("1", 64)
	changed := "sha256:" + strings.Repeat("2", 64)
	m.record([]wsSnapshot{{Name: "src", Digest: cas.Digest(baseline)}})
	rc := newTestRunCore(t, m)

	n := &plan.Node{ID: "sneaky", Mounts: []plan.MountSpec{{Workspace: "src", At: "/src", Mode: "ro"}}}
	mounts, err := m.mounts(n)
	if err != nil {
		t.Fatalf("mounts: %v", err)
	}
	if mounts[0].Digest != baseline {
		t.Fatalf("test setup: mount digest = %q, want the recorded baseline %q", mounts[0].Digest, baseline)
	}

	sb := &snapshotFakeSandbox{snap: executor.Snapshot{Digest: changed, Index: "sha256:" + strings.Repeat("3", 64)}}
	_, gotErr := rc.snapshotMounts(context.Background(), sb, n, mounts, 1)
	if gotErr == nil {
		t.Fatal("a read-only mount whose digest changed must be reported as an error")
	}
	if executor.IsInfra(gotErr) {
		t.Errorf("errors.Is(err, executor.ErrInfra) = true for %v, want false: "+
			"a read-only violation is a pipeline authoring error, not infrastructure, "+
			"and retry.OnInfra must not retry it", gotErr)
	}
}
