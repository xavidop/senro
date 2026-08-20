package senro_test

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/xavidop/senro"
	"github.com/xavidop/senro/exec"
)

// A step-scoped workspace is a fresh tree per step, shared with nobody.
//
// That is the isolation a run-scoped workspace cannot offer: every step
// mounting a ScopeRun workspace mounts ONE directory, so a step sees whatever
// its siblings left there and can stamp on what they are still using. The
// second step here succeeds only if it got its own empty tree.
func TestAStepScopedWorkspaceIsFreshForEachStep(t *testing.T) {
	dir := t.TempDir()
	ws := senro.Workspace("pad", senro.Scope(senro.ScopeStep))

	pipe := senro.New("ci")
	l := pipe.Workflow("main")
	l.Step("first", exec.Command("sh", "-c", "echo hi > left-behind.txt")).
		WorkDir("/w").Mount(ws.At("/w", senro.RW))
	l.Step("second", exec.Command("sh", "-c", "test ! -f left-behind.txt")).
		Needs("first").WorkDir("/w").Mount(ws.At("/w", senro.RW))

	if err := senro.Run(context.Background(), pipe,
		senro.WithDir(filepath.Join(dir, "run"))); err != nil {
		t.Fatalf("Run: %v; the second step saw what the first left behind, so the tree was shared", err)
	}
}

// It is writable and usable, not merely present: a step gets a real directory
// it can work in.
func TestAStepScopedWorkspaceIsWritable(t *testing.T) {
	dir := t.TempDir()
	ws := senro.Workspace("pad", senro.Scope(senro.ScopeStep))

	pipe := senro.New("ci")
	l := pipe.Workflow("main")
	l.Step("work", exec.Command("sh", "-c", "echo hi > f.txt && test -f f.txt")).
		WorkDir("/w").Mount(ws.At("/w", senro.RW))

	if err := senro.Run(context.Background(), pipe,
		senro.WithDir(filepath.Join(dir, "run"))); err != nil {
		t.Fatalf("Run: %v", err)
	}
}
