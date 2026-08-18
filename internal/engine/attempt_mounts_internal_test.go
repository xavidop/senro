package engine

// A white-box test for runAttempt's own cmdDir logic, decoupled from
// localexec's compensating guard: localexec repairs a mount-realized
// WorkDir sent back as Cmd.Dir on its own, so an engine-level test through
// it cannot tell the engine's fix from its absence (confirmed: reverting
// the fix left workspaces_test.go green). This runs runAttempt against a
// fake Executor with no such guard, verifying the engine's OWN contract; a
// future executor is not guaranteed to be as forgiving.

import (
	"context"
	"io"
	"testing"

	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/internal/eventlog"
	"github.com/xavidop/senro/internal/executor"
	"github.com/xavidop/senro/internal/plan"
	"github.com/xavidop/senro/internal/sink"
)

// recordingSandbox is a minimal executor.Sandbox that does nothing but
// record every Cmd it was asked to Run, with no WorkDir-vs-Cmd.Dir
// reconciliation of its own, unlike localexec's sandbox.
type recordingSandbox struct{ dirs []string }

func (s *recordingSandbox) ObservedPlatform(context.Context) (executor.Platform, error) {
	return executor.Platform{OS: "fake", Arch: "fake"}, nil
}

func (s *recordingSandbox) Snapshot(context.Context, string) (executor.Snapshot, error) {
	return executor.Snapshot{Digest: "sha256:" + zeros64, Index: "sha256:" + zeros64}, nil
}

func (s *recordingSandbox) PutSecret(context.Context, string, []byte) (string, error) {
	return "", nil
}

func (s *recordingSandbox) Run(_ context.Context, c executor.Cmd, _, _ io.Writer) (int, error) {
	s.dirs = append(s.dirs, c.Dir)
	return 0, nil
}

func (s *recordingSandbox) Close(context.Context, bool) error { return nil }

// recordingExecutor always hands back the same *recordingSandbox, so the
// test can inspect what it recorded after runAttempt returns.
type recordingExecutor struct{ sb *recordingSandbox }

func (e *recordingExecutor) Class(context.Context) (string, error) { return "fake", nil }

func (e *recordingExecutor) DeclaredPlatform(context.Context) (executor.Platform, error) {
	return executor.Platform{OS: "fake", Arch: "fake"}, nil
}

func (e *recordingExecutor) EffectiveEnv(_ context.Context, declared []string) ([]string, error) {
	return declared, nil
}

func (e *recordingExecutor) Sandbox(context.Context, executor.SandboxSpec) (executor.Sandbox, error) {
	return e.sb, nil
}

const zeros64 = "0000000000000000000000000000000000000000000000000000000000000000"

func TestRunAttemptDoesNotSendARealizedWorkDirBackAsCmdDir(t *testing.T) {
	p := &plan.Plan{Version: 1, Workspaces: []plan.WorkspaceSpec{{Name: "w", Scope: "run"}}}
	m := newTestWSManager(t, p, nil)

	ledger, err := eventlog.Open(t.TempDir())
	if err != nil {
		t.Fatalf("eventlog.Open: %v", err)
	}
	defer func() { _ = ledger.Close() }()
	logs := eventlog.NewLogSet(t.TempDir())
	sb := &recordingSandbox{}
	rc := &runCore{ledger: ledger, sink: sink.Nop(), runID: "r1", ws: m, defaultExec: &recordingExecutor{sb: sb}}

	n := &plan.Node{
		ID: "s", Kind: "exec", Cmd: []string{"true"}, WorkDir: "/w",
		Mounts: []plan.MountSpec{{Workspace: "w", At: "/w"}},
	}
	res := rc.runAttempt(context.Background(), n, Options{}, logs, 1)

	if res.state != api.StateSucceeded {
		t.Fatalf("runAttempt state = %s, err = %v, want succeeded", res.state, res.err)
	}
	if len(sb.dirs) != 1 {
		t.Fatalf("Run was called %d times, want 1", len(sb.dirs))
	}
	if sb.dirs[0] != "" {
		t.Errorf("Cmd.Dir = %q, want empty: the mount at WorkDir already realizes the sandbox's own working "+
			"directory there, and sending the same path back as Cmd.Dir would ask the executor to chdir into "+
			"a host path that may not exist", sb.dirs[0])
	}
}

// The neighbouring case: a step with NO mount at its WorkDir must still get
// its WorkDir passed through as Cmd.Dir exactly as before. The fix must not
// blank out every step's Dir, only the one case a mount already realized.
func TestRunAttemptStillSendsAnOrdinaryWorkDirAsCmdDir(t *testing.T) {
	p := &plan.Plan{Version: 1}
	m := newTestWSManager(t, p, nil)

	ledger, err := eventlog.Open(t.TempDir())
	if err != nil {
		t.Fatalf("eventlog.Open: %v", err)
	}
	defer func() { _ = ledger.Close() }()
	logs := eventlog.NewLogSet(t.TempDir())
	sb := &recordingSandbox{}
	rc := &runCore{ledger: ledger, sink: sink.Nop(), runID: "r1", ws: m, defaultExec: &recordingExecutor{sb: sb}}

	n := &plan.Node{ID: "s", Kind: "exec", Cmd: []string{"true"}, WorkDir: "/some/plain/dir"}
	res := rc.runAttempt(context.Background(), n, Options{}, logs, 1)

	if res.state != api.StateSucceeded {
		t.Fatalf("runAttempt state = %s, err = %v, want succeeded", res.state, res.err)
	}
	if len(sb.dirs) != 1 || sb.dirs[0] != "/some/plain/dir" {
		t.Errorf("Cmd.Dir = %q, want %q unchanged", sb.dirs, "/some/plain/dir")
	}
}
