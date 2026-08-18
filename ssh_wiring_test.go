package senro

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/xavidop/senro/internal/executor"
	"github.com/xavidop/senro/internal/plan"
	"github.com/xavidop/senro/internal/storage"
)

// TestAnSSHTargetReachesARealExecutor closes the gap between "the plan
// accepts an ssh target" and "a run can construct one": a family that
// validated at plan time but fell through newExecutorFor's switch would be
// refused only after the run directory existed.
//
// It contacts nothing: sshexec.New resolves the host lazily, so constructing
// an executor for a host that does not exist is not an error and must not
// become one.
func TestAnSSHTargetReachesARealExecutor(t *testing.T) {
	store, err := storage.Open(filepath.Join(t.TempDir(), "store"))
	if err != nil {
		t.Fatalf("storage.Open: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	spec := plan.ExecutorSpec{
		Kind: plan.ExecutorSSH, Host: "build-07.internal", Class: "ubuntu-24.04/amd64",
	}
	ex, err := newExecutorFor(spec, "deploy", "run-1", store, nil)
	if err != nil {
		t.Fatalf("newExecutorFor refused an ssh spec the plan accepts: %v", err)
	}
	if ex == nil {
		t.Fatal("newExecutorFor returned no executor and no error")
	}
	// The declared class is reported without a connection, which is the whole
	// point of declaring one.
	class, err := ex.Class(t.Context())
	if err != nil {
		t.Fatalf("Class contacted the host: %v", err)
	}
	if class != "ubuntu-24.04/amd64" {
		t.Errorf("class = %q, want the declared value", class)
	}
}

// The construction-time check is the plan-time check, so a plan assembled by
// hand cannot reach a connection with a specification Validate would have
// refused.
func TestAnSSHSpecWithNoHostIsRefusedAtConstruction(t *testing.T) {
	store, err := storage.Open(filepath.Join(t.TempDir(), "store"))
	if err != nil {
		t.Fatalf("storage.Open: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	_, err = newExecutorFor(plan.ExecutorSpec{Kind: plan.ExecutorSSH}, "deploy", "run-1", store, nil)
	if err == nil {
		t.Fatal("newExecutorFor built an ssh executor with no destination")
	}
	if !strings.Contains(err.Error(), "ssh.Host") {
		t.Errorf("the refusal does not say how to declare one: %v", err)
	}
}

// An ssh executor holds an authenticated control master per host for the
// length of the run, so a run that never handed it back would leave one open
// on every host it touched. The other executors hold nothing and must not be
// asked to.
func TestARunGivesBackWhatItsExecutorsHoldForItsWholeLength(t *testing.T) {
	held := &holdingExecutor{}
	closeExecutors(map[string]executor.Executor{
		"holds":  held,
		"holds2": nothingExecutor{},
	})
	if held.closed != 1 {
		t.Errorf("Close was called %d times on an executor holding something for the run, want 1",
			held.closed)
	}
}

// holdingExecutor is an executor that holds a resource for the run, as
// sshexec's control master is; nothingExecutor is one that holds nothing, as
// every other executor in this build does.
type holdingExecutor struct {
	nothingExecutor
	closed int
}

func (h *holdingExecutor) Close() error { h.closed++; return nil }

type nothingExecutor struct{}

func (nothingExecutor) Class(context.Context) (string, error) { return "test", nil }
func (nothingExecutor) DeclaredPlatform(context.Context) (executor.Platform, error) {
	return executor.Platform{}, nil
}
func (nothingExecutor) EffectiveEnv(_ context.Context, declared []string) ([]string, error) {
	return declared, nil
}
func (nothingExecutor) Sandbox(context.Context, executor.SandboxSpec) (executor.Sandbox, error) {
	return nil, errors.New("this executor builds no sandbox")
}
