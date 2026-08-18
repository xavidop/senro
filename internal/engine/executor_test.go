package engine_test

import (
	"context"
	"io"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/internal/engine"
	"github.com/xavidop/senro/internal/executor"
	"github.com/xavidop/senro/internal/plan"
	"github.com/xavidop/senro/internal/sink"
)

// countingExecutor records every sandbox it is asked for, so a test can prove
// WHICH executor a node ran on rather than merely that it ran.
type countingExecutor struct {
	class    string
	sandbox  atomic.Int64
	stepIDs  chan string
	exitCode int
}

func (c *countingExecutor) Class(context.Context) (string, error) { return c.class, nil }

func (c *countingExecutor) DeclaredPlatform(context.Context) (executor.Platform, error) {
	return executor.Platform{OS: "linux", Arch: "amd64"}, nil
}

func (c *countingExecutor) EffectiveEnv(_ context.Context, declared []string) ([]string, error) {
	return declared, nil
}

func (c *countingExecutor) Sandbox(_ context.Context, spec executor.SandboxSpec) (executor.Sandbox, error) {
	c.sandbox.Add(1)
	select {
	case c.stepIDs <- spec.StepID:
	default:
	}
	return &countingSandbox{owner: c}, nil
}

type countingSandbox struct{ owner *countingExecutor }

func (s *countingSandbox) ObservedPlatform(context.Context) (executor.Platform, error) {
	return executor.Platform{OS: "linux", Arch: "amd64"}, nil
}
func (s *countingSandbox) Snapshot(context.Context, string) (executor.Snapshot, error) {
	return executor.Snapshot{}, nil
}
func (s *countingSandbox) PutSecret(context.Context, string, []byte) (string, error) {
	return "/dev/null", nil
}
func (s *countingSandbox) Run(context.Context, executor.Cmd, io.Writer, io.Writer) (int, error) {
	return s.owner.exitCode, nil
}
func (s *countingSandbox) Close(context.Context, bool) error { return nil }

// TestAStepAndItsHandlerBothRunOnTheNodesOwnExecutor is the both-legs test
// proving runAttempt and execHandler always agree on which executor a
// node's handlers run on. The two are a pair that has diverged three times
// in this project (secret delivery, redaction, and the secret.redacted
// event), always because one was taught something the other was not. A
// handler that inherited the run's DEFAULT executor rather than its
// parent's would collect its evidence from the wrong machine, which is
// precisely what this guarantees against.
func TestAStepAndItsHandlerBothRunOnTheNodesOwnExecutor(t *testing.T) {
	def := &countingExecutor{class: "default", stepIDs: make(chan string, 8)}
	other := &countingExecutor{class: "other", stepIDs: make(chan string, 8), exitCode: 1}

	p := &plan.Plan{Version: 1, Nodes: []plan.Node{{
		ID: "deploy", Kind: "exec", Cmd: []string{"true"},
		Executor:  &plan.ExecutorSpec{Kind: plan.ExecutorContainer, Image: "img:1"},
		OnFailure: []plan.Node{{ID: "collect", Kind: "exec", Cmd: []string{"true"}}},
	}}}

	dir := t.TempDir()
	status, err := engine.Run(context.Background(), p, engine.Options{
		Dir: dir, RunID: "r1", Sink: sink.Nop(),
		Executor: def,
		Executors: map[string]executor.Executor{
			"container:img:1": other,
		},
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if status != api.RunFailed {
		t.Fatalf("status = %s, want failed", status)
	}
	if got := def.sandbox.Load(); got != 0 {
		t.Errorf("the default executor made %d sandbox(es); the only node names another", got)
	}
	if got := other.sandbox.Load(); got != 2 {
		t.Errorf("the named executor made %d sandbox(es), want 2 (the step and its handler)", got)
	}
	_ = filepath.Join(dir, "events.jsonl")
}

func TestRunRefusesAPlanNamingAnExecutorItWasNotGiven(t *testing.T) {
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{{
		ID: "build", Kind: "exec", Cmd: []string{"true"},
		Executor: &plan.ExecutorSpec{Kind: plan.ExecutorContainer, Image: "img:1"},
	}}}
	_, err := engine.Run(context.Background(), p, engine.Options{
		Dir: t.TempDir(), RunID: "r1", Sink: sink.Nop(),
		Executor: &countingExecutor{class: "default", stepIDs: make(chan string, 1)},
	})
	if err == nil {
		t.Fatal("Run accepted a plan naming an executor it has no instance of")
	}
}

// TestRunRefusesALocalPlanWithNoDefaultExecutorConfigured is checkExecutors'
// other branch: a node naming no executor at all (ExecutorKey ==
// ExecutorLocal) still needs Options.Executor to actually run on, and a
// caller of engine.Run directly, with no senro.Run in front of it to always
// supply localexec, can get this wrong exactly as easily as naming an
// executor with no map entry.
func TestRunRefusesALocalPlanWithNoDefaultExecutorConfigured(t *testing.T) {
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{{ID: "build", Kind: "exec", Cmd: []string{"true"}}}}
	_, err := engine.Run(context.Background(), p, engine.Options{
		Dir: t.TempDir(), RunID: "r1", Sink: sink.Nop(),
	})
	if err == nil {
		t.Fatal("Run accepted a plan with no Options.Executor configured")
	}
}
