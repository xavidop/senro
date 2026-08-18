package engine

// White-box tests for execHandler's mount contract, for the reason
// attempt_mounts_internal_test.go exists for runAttempt's: localexec
// repairs a mount-realized Cmd.Dir and cannot enforce RO at all, so an
// end-to-end mutation test against it cannot tell the engine's contract
// from its absence. Both are checked here against a fake executor that
// papers over nothing; the container half lives in the repository root's
// TestAHandlerInAContainerReadsTheFailedStepsWorkspace.

import (
	"context"
	"io"
	"sync"
	"testing"

	"github.com/xavidop/senro/internal/eventlog"
	"github.com/xavidop/senro/internal/executor"
	"github.com/xavidop/senro/internal/plan"
	"github.com/xavidop/senro/internal/sink"
)

// specSandbox records the SandboxSpec it was built from and every Cmd it ran,
// and optionally blocks inside Run so a test can observe what the engine holds
// while a handler is executing.
type specSandbox struct {
	spec    executor.SandboxSpec
	dirs    []string
	entered chan struct{}
	release chan struct{}
}

func (s *specSandbox) ObservedPlatform(context.Context) (executor.Platform, error) {
	return executor.Platform{OS: "fake", Arch: "fake"}, nil
}

func (s *specSandbox) Snapshot(context.Context, string) (executor.Snapshot, error) {
	return executor.Snapshot{Digest: "sha256:" + zeros64, Index: "sha256:" + zeros64}, nil
}

func (s *specSandbox) PutSecret(context.Context, string, []byte) (string, error) { return "", nil }

func (s *specSandbox) Run(_ context.Context, c executor.Cmd, _, _ io.Writer) (int, error) {
	s.dirs = append(s.dirs, c.Dir)
	if s.entered != nil {
		close(s.entered)
		<-s.release
	}
	return 0, nil
}

func (s *specSandbox) Close(context.Context, bool) error { return nil }

// specExecutor hands back one *specSandbox and remembers the spec it was
// asked for, which is the whole point: the spec is what inheritance is made
// of, and no executor in this build is obliged to reveal it afterwards.
type specExecutor struct{ sb *specSandbox }

func (e *specExecutor) Class(context.Context) (string, error) { return "fake", nil }

func (e *specExecutor) DeclaredPlatform(context.Context) (executor.Platform, error) {
	return executor.Platform{OS: "fake", Arch: "fake"}, nil
}

func (e *specExecutor) EffectiveEnv(_ context.Context, declared []string) ([]string, error) {
	return declared, nil
}

func (e *specExecutor) Sandbox(_ context.Context, spec executor.SandboxSpec) (executor.Sandbox, error) {
	e.sb.spec = spec
	return e.sb, nil
}

// handlerFixture is one wsManager, one ledger and one runCore wired together,
// which is everything execHandler needs and nothing it does not.
func handlerFixture(t *testing.T, sb *specSandbox) (*runCore, *wsManager, *eventlog.LogSet) {
	t.Helper()
	p := &plan.Plan{Version: 1, Workspaces: []plan.WorkspaceSpec{{Name: "w", Scope: "run"}}}
	m := newTestWSManager(t, p, nil)

	ledger, err := eventlog.Open(t.TempDir())
	if err != nil {
		t.Fatalf("eventlog.Open: %v", err)
	}
	t.Cleanup(func() { _ = ledger.Close() })

	rc := &runCore{
		ledger: ledger, sink: sink.Nop(), runID: "r1", ws: m,
		defaultExec: &specExecutor{sb: sb},
	}
	return rc, m, eventlog.NewLogSet(t.TempDir())
}

func parentWithMount() *plan.Node {
	return &plan.Node{
		ID: "deploy", Kind: "exec", Cmd: []string{"true"}, WorkDir: "/w",
		Mounts: []plan.MountSpec{{Workspace: "w", At: "/w", Mode: "rw"}},
	}
}

// TestExecHandlerGivesAHandlerItsParentsMountsReadOnly is the engine-side
// contract, read straight off the SandboxSpec rather than inferred from
// what an executor did with it. The parent mounts w read-write; the handler
// must get the same workspace, path and directory, READ-ONLY, since the
// parent's ws.snapshot is already taken and there is no second snapshot to
// correct the record. RW would still pass every localexec test, that
// executor being unable to enforce RO either way.
func TestExecHandlerGivesAHandlerItsParentsMountsReadOnly(t *testing.T) {
	sb := &specSandbox{}
	rc, m, logs := handlerFixture(t, sb)

	parent := parentWithMount()
	h := &plan.Node{ID: "collect", Kind: "exec", Cmd: []string{"true"}}
	fail := Failure{Run: "r1", Step: parent.ID, Attempt: 1}

	if err := rc.execHandler(context.Background(), parent, h, "deploy/on_failure/collect", "", fail, Options{}, logs); err != nil {
		t.Fatalf("execHandler: %v", err)
	}

	if len(sb.spec.Mounts) != 1 {
		t.Fatalf("the handler was given %d mount(s), want 1: a handler inherits its parent's "+
			"workspaces, and nothing else can put the failed step's files in front of it",
			len(sb.spec.Mounts))
	}
	got := sb.spec.Mounts[0]
	if got.Name != "w" || got.At != "/w" {
		t.Errorf("the handler's mount is %q at %q, want %q at %q", got.Name, got.At, "w", "/w")
	}
	if want := m.path("w"); got.Path != want {
		t.Errorf("the handler's mount points at %q, want %q: the same directory the parent "+
			"step wrote into, not a fresh one", got.Path, want)
	}
	if !got.RO {
		t.Error("the handler's inherited mount is writable; it must be read-only, or a " +
			"diagnostic script can change the evidence the run's own ws.snapshot digest " +
			"already claims to describe")
	}

	// The sandbox is still the HANDLER's own. Inheriting the parent's step id
	// instead is the tempting fix that looks right on localexec and hands back
	// a fresh, empty container everywhere else.
	if sb.spec.StepID != "deploy/on_failure/collect" {
		t.Errorf("the handler's sandbox is keyed %q, want the composite handler id", sb.spec.StepID)
	}
}

// TestExecHandlerStartsWhereTheStepStartedWithoutResendingTheDir is the
// working-directory half, and the one localexec would hide: it reconciles a
// Cmd.Dir that equals SandboxSpec.WorkDir back to the realized mount itself
// (see localexec.Run's own comment naming that case), so a handler that
// wrongly sent both would still land in the right place there and nowhere
// else.
func TestExecHandlerStartsWhereTheStepStartedWithoutResendingTheDir(t *testing.T) {
	sb := &specSandbox{}
	rc, _, logs := handlerFixture(t, sb)

	parent := parentWithMount()
	h := &plan.Node{ID: "collect", Kind: "exec", Cmd: []string{"true"}}
	fail := Failure{Run: "r1", Step: parent.ID, Attempt: 1}

	if err := rc.execHandler(context.Background(), parent, h, "deploy/on_failure/collect", "", fail, Options{}, logs); err != nil {
		t.Fatalf("execHandler: %v", err)
	}

	if sb.spec.WorkDir != "/w" {
		t.Errorf("the handler's SandboxSpec.WorkDir = %q, want %q: a handler that declares "+
			"none of its own starts where its parent started, which is what makes an "+
			"unqualified `cat build.log` mean the same file it means in the step",
			sb.spec.WorkDir, "/w")
	}
	if len(sb.dirs) != 1 {
		t.Fatalf("Run was called %d times, want 1", len(sb.dirs))
	}
	if sb.dirs[0] != "" {
		t.Errorf("Cmd.Dir = %q, want empty: the mount at that path already realizes the "+
			"sandbox's working directory, and sending it back as Cmd.Dir asks the executor "+
			"to chdir into a host path that need not exist", sb.dirs[0])
	}
}

// TestExecHandlerLeavesAHandlerWithNoInheritedMountsWhereItWas is the
// neighbouring case, and it is a REGRESSION guard rather than a nicety. A
// parent with no workspace has a WorkDir that names a path inside its own
// sandbox, which the handler does not have. Inheriting it unconditionally
// would send such a handler chasing a directory that does not exist and fail
// it before its first instruction, where it used to run in its own sandbox
// root.
func TestExecHandlerLeavesAHandlerWithNoInheritedMountsWhereItWas(t *testing.T) {
	sb := &specSandbox{}
	rc, _, logs := handlerFixture(t, sb)

	parent := &plan.Node{ID: "deploy", Kind: "exec", Cmd: []string{"true"}, WorkDir: "build"}
	h := &plan.Node{ID: "collect", Kind: "exec", Cmd: []string{"true"}}
	fail := Failure{Run: "r1", Step: parent.ID, Attempt: 1}

	if err := rc.execHandler(context.Background(), parent, h, "deploy/on_failure/collect", "", fail, Options{}, logs); err != nil {
		t.Fatalf("execHandler: %v", err)
	}

	if len(sb.spec.Mounts) != 0 {
		t.Fatalf("the handler was given %d mount(s) for a parent that declared none", len(sb.spec.Mounts))
	}
	if sb.spec.WorkDir != "" {
		t.Errorf("the handler's SandboxSpec.WorkDir = %q, want empty: with nothing mounted "+
			"there, the parent's working directory is a path in the parent's own sandbox "+
			"and means nothing to the handler", sb.spec.WorkDir)
	}
}

// TestExecHandlerHoldsTheWorkspaceLockWhileTheHandlerRuns is the one
// assertion here no end-to-end test can make: a handler reading a shared
// ScopeRun directory is a reader like any other, and a sibling's cache hit
// restores into it with a RemoveAll then a Rename. The hold became
// necessary when handlers began inheriting their parent's mounts. TryLock,
// not a timing window: an exclusive acquisition either fails while the
// handler is inside Run or the hold is not being taken at all.
func TestExecHandlerHoldsTheWorkspaceLockWhileTheHandlerRuns(t *testing.T) {
	sb := &specSandbox{entered: make(chan struct{}), release: make(chan struct{})}
	rc, m, logs := handlerFixture(t, sb)

	parent := parentWithMount()
	h := &plan.Node{ID: "collect", Kind: "exec", Cmd: []string{"true"}}
	fail := Failure{Run: "r1", Step: parent.ID, Attempt: 1}

	var wg sync.WaitGroup
	wg.Add(1)
	var execErr error
	go func() {
		defer wg.Done()
		execErr = rc.execHandler(context.Background(), parent, h, "deploy/on_failure/collect", "", fail, Options{}, logs)
	}()

	<-sb.entered
	if m.wsLock("w").TryLock() {
		m.wsLock("w").Unlock()
		close(sb.release)
		wg.Wait()
		t.Fatal("a restore could take the workspace exclusively while a handler was running " +
			"inside it: RemoveAll-then-Rename would unlink the directory the handler is " +
			"reading, out from under the one process in the run that exists to collect " +
			"evidence from it")
	}
	close(sb.release)
	wg.Wait()

	if execErr != nil {
		t.Fatalf("execHandler: %v", execErr)
	}
	// And released afterwards. A hold that outlives the handler is a workspace
	// no cache hit can ever restore into again.
	if !m.wsLock("w").TryLock() {
		t.Error("the workspace is still held exclusively after the handler returned")
	} else {
		m.wsLock("w").Unlock()
	}
}
