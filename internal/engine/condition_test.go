package engine_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/internal/engine"
	"github.com/xavidop/senro/internal/executor"
	"github.com/xavidop/senro/internal/executor/localexec"
	"github.com/xavidop/senro/internal/plan"
	"github.com/xavidop/senro/internal/sink"
)

// runToEventsWithParams runs p with the given run parameters and returns the
// ledger, failing the test if Run itself returns an engine error. It is this
// file's equivalent of group_test.go's runToEvents, with a Params hook added
// for senro.When's evaluation.
func runToEventsWithParams(t *testing.T, p *plan.Plan, params map[string]string) []api.Event {
	t.Helper()
	_, events := runWithStatus(t, p, params)
	return events
}

// runWithStatus is runToEventsWithParams plus the run's own rolled-up status,
// for the cascade tests that must check the run's final status and not only
// each step's own state (a pruned node must never poison RollUp).
func runWithStatus(t *testing.T, p *plan.Plan, params map[string]string) (api.RunStatus, []api.Event) {
	t.Helper()
	dir := t.TempDir()
	status, err := engine.Run(context.Background(), p, engine.Options{
		Dir:         dir,
		Executor:    localexec.New(dir, nil),
		Sink:        sink.Nop(),
		MaxParallel: 4,
		RunID:       "01CONDTEST",
		Params:      params,
	})
	if err != nil {
		t.Fatalf("Run returned an engine error: %v", err)
	}
	return status, readLedger(t, dir)
}

// stepFinished finds id's step.finished event and decodes its body, failing
// the test if there is none: every test that calls this is asserting a
// terminal state, and a missing event is itself a failure worth stopping on.
func stepFinished(t *testing.T, events []api.Event, id string) (api.State, api.StepFinishedBody) {
	t.Helper()
	for _, e := range events {
		if e.Type == api.StepFinished && e.Step == id {
			var b api.StepFinishedBody
			if err := e.Decode(&b); err != nil {
				t.Fatalf("decode step.finished for %q: %v", id, err)
			}
			return b.State, b
		}
	}
	t.Fatalf("no step.finished event for %q", id)
	return "", api.StepFinishedBody{}
}

// localExecutor is a throwaway local executor for tests that only need Run
// to accept a plan, such as TestRunRefusesAnUnparseableCondition, which never
// reaches the point of actually running anything.
func localExecutor(t *testing.T) executor.Executor {
	t.Helper()
	return localexec.New(t.TempDir(), nil)
}

// TestAStepWhoseConditionIsFalseIsSkippedRatherThanRun is the primary case.
func TestAStepWhoseConditionIsFalseIsSkippedRatherThanRun(t *testing.T) {
	marker := filepath.Join(t.TempDir(), "ran")
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{{
		ID: "deploy", Kind: "exec", Cmd: []string{"touch", marker},
		When: []string{"branch:main"},
	}}}
	events := runToEventsWithParams(t, p, map[string]string{"branch": "pr-12"})

	if _, err := os.Stat(marker); err == nil {
		t.Fatal("the skipped step ran")
	}
	st, body := stepFinished(t, events, "deploy")
	if st != api.StateSkippedCondition {
		t.Fatalf("state = %s, want skipped_condition", st)
	}
	if !strings.Contains(body.Reason, "branch:main") {
		t.Errorf("reason = %q, want the condition named", body.Reason)
	}
	for _, e := range events {
		if e.Type == api.StepStarted && e.Step == "deploy" {
			t.Error("a step that never ran emitted step.started")
		}
	}
}

// TestASkippedConditionCascadesAsItselfAndKeepsTheRunGreen is decision 7 in
// this plan's header, and the reason it is a decision rather than an accident:
// the existing cascade turns any unsatisfied need into
// skipped_upstream_failed, which rolls up to a PARTIAL run. A main-only deploy
// workflow would have reported every pull request run as partially failed.
func TestASkippedConditionCascadesAsItselfAndKeepsTheRunGreen(t *testing.T) {
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{
		{ID: "deploy", Kind: "exec", Cmd: []string{"true"}, When: []string{"branch:main"}},
		{ID: "smoke", Kind: "exec", Cmd: []string{"true"}, Needs: []string{"deploy"}},
		{ID: "always-runs", Kind: "exec", Cmd: []string{"true"}},
	}}
	status, events := runWithStatus(t, p, map[string]string{"branch": "pr-12"})

	if st, _ := stepFinished(t, events, "smoke"); st != api.StateSkippedCondition {
		t.Errorf("the dependent settled as %s, want skipped_condition", st)
	}
	if st, _ := stepFinished(t, events, "always-runs"); st != api.StateSucceeded {
		t.Errorf("an unrelated step settled as %s", st)
	}
	if status != api.RunSucceeded {
		t.Fatalf("run status = %s, want succeeded; a pruned node is not a failure", status)
	}
}

func TestARunWhoseConditionsAreAllTrueRunsEverything(t *testing.T) {
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{
		{ID: "deploy", Kind: "exec", Cmd: []string{"true"},
			When: []string{"branch:main", "env:DEPLOY_ENV=prod"}},
	}}
	t.Setenv("DEPLOY_ENV", "prod")
	status, events := runWithStatus(t, p, map[string]string{"branch": "main"})
	if st, _ := stepFinished(t, events, "deploy"); st != api.StateSucceeded {
		t.Fatalf("state = %s", st)
	}
	if status != api.RunSucceeded {
		t.Fatalf("status = %s", status)
	}
}

// TestContinueOnErrorDoesNotResurrectASkippedDependent keeps the two concepts
// apart. ContinueOnError is about FAILURE ("lets dependents run even if this
// step fails"), and a pruned node did not fail: running a dependent against
// output that was never produced would be worse than skipping it.
func TestContinueOnErrorDoesNotResurrectASkippedDependent(t *testing.T) {
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{
		{ID: "deploy", Kind: "exec", Cmd: []string{"true"},
			When: []string{"branch:main"}, ContinueOnError: true},
		{ID: "smoke", Kind: "exec", Cmd: []string{"true"}, Needs: []string{"deploy"}},
	}}
	_, events := runWithStatus(t, p, map[string]string{"branch": "pr-12"})
	if st, _ := stepFinished(t, events, "smoke"); st != api.StateSkippedCondition {
		t.Fatalf("state = %s, want skipped_condition", st)
	}
}

func TestRunRefusesAnUnparseableCondition(t *testing.T) {
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{
		{ID: "x", Kind: "exec", Cmd: []string{"true"}, When: []string{"moon:full"}},
	}}
	_, err := engine.Run(context.Background(), p, engine.Options{
		Dir: t.TempDir(), RunID: "r", Sink: sink.Nop(), Executor: localExecutor(t),
	})
	if err == nil {
		t.Fatal("Run accepted a condition nothing can evaluate")
	}
}

// TestAWhenOnAnExpansionChildIsEvaluatedLikeAnyOtherNode checks the "a
// condition-skipped node with a When on an expansion child" case: a child
// materialized by Expand is an ordinary node from the scheduler's point of
// view, so its own When gates it exactly the same way a plain step's does,
// with no special-casing anywhere in readySet or pruned.
func TestAWhenOnAnExpansionChildIsEvaluatedLikeAnyOtherNode(t *testing.T) {
	p := &plan.Plan{
		Version: 1,
		Groups:  []plan.GroupSpec{{Name: "lint"}},
		Nodes: []plan.Node{
			{ID: "lint[unit=a]", Kind: "exec", Cmd: []string{"true"}, Group: "lint", When: []string{"branch:main"}},
			{ID: "lint[unit=b]", Kind: "exec", Cmd: []string{"true"}, Group: "lint"},
		},
	}
	status, events := runWithStatus(t, p, map[string]string{"branch": "pr-12"})
	if st, _ := stepFinished(t, events, "lint[unit=a]"); st != api.StateSkippedCondition {
		t.Errorf("lint[unit=a] = %s, want skipped_condition", st)
	}
	if st, _ := stepFinished(t, events, "lint[unit=b]"); st != api.StateSucceeded {
		t.Errorf("lint[unit=b] = %s, want succeeded, its own condition was never declared", st)
	}
	if status != api.RunSucceeded {
		t.Fatalf("run status = %s, want succeeded", status)
	}
}

// TestConditionEvaluationDoesNotMoveThePlanDigest is the "evaluated at ready
// time, not plan time" property stated plainly: the condition's PRESENCE is
// part of the plan (and so is its own field in plan.Digest), but its OUTCOME
// must never be: two runs of the identical plan, one where the condition
// holds and one where it does not, still describe the identical timetable.
func TestConditionEvaluationDoesNotMoveThePlanDigest(t *testing.T) {
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{
		{ID: "deploy", Kind: "exec", Cmd: []string{"true"}, When: []string{"branch:main"}},
	}}
	before := p.Digest()
	if _, err := engine.Run(context.Background(), p, engine.Options{
		Dir: t.TempDir(), Executor: localExecutor(t), Sink: sink.Nop(),
		MaxParallel: 4, RunID: "r1", Params: map[string]string{"branch": "pr-12"},
	}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	after := p.Digest()
	if before != after {
		t.Fatalf("Run mutated the plan's own digest: %s -> %s", before, after)
	}

	trueRun := &plan.Plan{Version: 1, Nodes: []plan.Node{
		{ID: "deploy", Kind: "exec", Cmd: []string{"true"}, When: []string{"branch:main"}},
	}}
	if trueRun.Digest() != before {
		t.Fatal("two plans differing only in what a condition will evaluate to have different digests")
	}
}
