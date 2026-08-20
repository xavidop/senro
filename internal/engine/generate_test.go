package engine_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/internal/engine"
	"github.com/xavidop/senro/internal/executor/localexec"
	"github.com/xavidop/senro/internal/plan"
	"github.com/xavidop/senro/internal/sink"
)

// The whole point of a generator, end to end: a step that has run decides
// what else the run does, and the nodes it names become ordinary steps that
// actually execute. Everything else in this file is a property of that.
func TestAGeneratedNodeRunsAsAnOrdinaryStep(t *testing.T) {
	dir := t.TempDir()
	marker := filepath.Join(dir, "generated-ran")

	p := &plan.Plan{Version: 1, Nodes: []plan.Node{
		{ID: "discover", Kind: "exec", Cmd: []string{"true"}, Generate: &plan.GenerateSpec{}},
	}}
	csink := sink.Recording()

	status, err := engine.Run(context.Background(), p, engine.Options{
		Dir: dir, Executor: localexec.New(dir, nil), Sink: csink, MaxParallel: 4, RunID: "01GEN",
		Generators: generatorsFrom(map[string]engine.GenerateFunc{
			"discover": func(context.Context, *plan.Node, string) (*plan.Fragment, error) {
				return &plan.Fragment{Version: plan.FragmentVersion, Nodes: []plan.Node{
					{ID: "apply", Kind: "exec", Cmd: []string{"touch", marker}},
				}}, nil
			},
		}),
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if status != api.RunSucceeded {
		t.Errorf("status = %s, want succeeded", status)
	}
	if _, statErr := os.Stat(marker); statErr != nil {
		t.Fatalf("the generated step never ran: %v", statErr)
	}
}

// The generated node's id is hierarchical and derived from the generator, so
// a fragment can be written once without knowing where it will land, and two
// generators producing an "apply" cannot collide.
func TestAGeneratedNodeIsNamedUnderItsGenerator(t *testing.T) {
	dir := t.TempDir()
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{
		{ID: "discover", Kind: "exec", Cmd: []string{"true"}, Generate: &plan.GenerateSpec{}},
	}}
	csink := sink.Recording()

	if _, err := engine.Run(context.Background(), p, engine.Options{
		Dir: dir, Executor: localexec.New(dir, nil), Sink: csink, MaxParallel: 4, RunID: "01GENID",
		Generators: generatorsFrom(map[string]engine.GenerateFunc{
			"discover": func(context.Context, *plan.Node, string) (*plan.Fragment, error) {
				return &plan.Fragment{Version: plan.FragmentVersion, Nodes: []plan.Node{
					{ID: "apply", Kind: "exec", Cmd: []string{"true"}},
				}}, nil
			},
		}),
	}); err != nil {
		t.Fatalf("Run: %v", err)
	}

	var finished bool
	for _, e := range csink.Events() {
		if e.Type == api.StepFinished && e.Step == "discover/apply" {
			finished = true
		}
	}
	if !finished {
		t.Error("no step.finished for \"discover/apply\"; a generated node is named under its generator")
	}
}

// The event is what a reader that was not there learns the graph grew from.
// There is no plan.json to fall back on: a generated node cannot be in a file
// written before the run.
func TestASpliceEmitsPlanGeneratedNamingItsChildren(t *testing.T) {
	dir := t.TempDir()
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{
		{ID: "discover", Kind: "exec", Cmd: []string{"true"}, Generate: &plan.GenerateSpec{}},
	}}
	csink := sink.Recording()

	if _, err := engine.Run(context.Background(), p, engine.Options{
		Dir: dir, Executor: localexec.New(dir, nil), Sink: csink, MaxParallel: 4, RunID: "01GENEV",
		Generators: generatorsFrom(map[string]engine.GenerateFunc{
			"discover": func(context.Context, *plan.Node, string) (*plan.Fragment, error) {
				return &plan.Fragment{Version: plan.FragmentVersion, Nodes: []plan.Node{
					{ID: "apply", Kind: "exec", Cmd: []string{"true"}},
				}}, nil
			},
		}),
	}); err != nil {
		t.Fatalf("Run: %v", err)
	}

	var body api.PlanGeneratedBody
	var found bool
	for _, e := range csink.Events() {
		if e.Type == api.PlanGenerated {
			if err := e.Decode(&body); err != nil {
				t.Fatalf("decode plan.generated: %v", err)
			}
			found = true
		}
	}
	if !found {
		t.Fatal("no plan.generated event; a graph that grew and did not say so is unreadable after the fact")
	}
	if body.Generator != "discover" {
		t.Errorf("Generator = %q, want %q", body.Generator, "discover")
	}
	if len(body.Children) != 1 || body.Children[0] != "discover/apply" {
		t.Errorf("Children = %v, want [discover/apply]", body.Children)
	}
}

// A dependent of the generator must wait for the BOUNDARY, not merely for the
// generator: the generator finishing is the moment the generated work starts.
// Without this a deploy would run against a fleet still being prepared.
func TestAGeneratorsDependentWaitsForTheFragmentsBoundary(t *testing.T) {
	dir := t.TempDir()
	slow := filepath.Join(dir, "slow-done")

	p := &plan.Plan{Version: 1, Nodes: []plan.Node{
		{ID: "discover", Kind: "exec", Cmd: []string{"true"}, Generate: &plan.GenerateSpec{}},
		{ID: "after", Kind: "exec", Cmd: []string{"sh", "-c", "test -f " + slow}, Needs: []string{"discover"}},
	}}
	csink := sink.Recording()

	status, err := engine.Run(context.Background(), p, engine.Options{
		Dir: dir, Executor: localexec.New(dir, nil), Sink: csink, MaxParallel: 4, RunID: "01GENB",
		Generators: generatorsFrom(map[string]engine.GenerateFunc{
			"discover": func(context.Context, *plan.Node, string) (*plan.Fragment, error) {
				return &plan.Fragment{Version: plan.FragmentVersion,
					Nodes: []plan.Node{
						{ID: "slow", Kind: "exec", Cmd: []string{"sh", "-c", "sleep 0.3; touch " + slow}},
					},
					Boundary: []string{"slow"},
				}, nil
			},
		}),
	})
	if err != nil {
		t.Fatalf("Run: %v; \"after\" ran before the boundary it was supposed to wait for", err)
	}
	if status != api.RunSucceeded {
		t.Errorf("status = %s, want succeeded", status)
	}
}

// A fragment that cannot be spliced fails the generator, and adds nothing: a
// partially applied fragment is a graph no re-run could reproduce.
func TestAGeneratorProducingAnUnspliceableFragmentFails(t *testing.T) {
	dir := t.TempDir()
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{
		{ID: "discover", Kind: "exec", Cmd: []string{"true"}, Generate: &plan.GenerateSpec{}},
	}}
	csink := sink.Recording()

	status, _ := engine.Run(context.Background(), p, engine.Options{
		Dir: dir, Executor: localexec.New(dir, nil), Sink: csink, MaxParallel: 4, RunID: "01GENBAD",
		Generators: generatorsFrom(map[string]engine.GenerateFunc{
			"discover": func(context.Context, *plan.Node, string) (*plan.Fragment, error) {
				// Needs something the fragment does not contain.
				return &plan.Fragment{Version: plan.FragmentVersion, Nodes: []plan.Node{
					{ID: "apply", Kind: "exec", Cmd: []string{"true"}, Needs: []string{"nope"}},
				}}, nil
			},
		}),
	})
	if status == api.RunSucceeded {
		t.Fatal("a generator whose fragment cannot be spliced must fail the run, not be ignored")
	}
	for _, e := range csink.Events() {
		if e.Type == api.PlanGenerated {
			t.Error("a refused fragment must emit no plan.generated: nothing was added")
		}
	}
}

// An empty fragment is legal and means "nothing to do here": the generator's
// dependents run rather than being skipped or left waiting.
func TestAnEmptyFragmentLetsTheGeneratorsDependentsRun(t *testing.T) {
	dir := t.TempDir()
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{
		{ID: "discover", Kind: "exec", Cmd: []string{"true"}, Generate: &plan.GenerateSpec{}},
		{ID: "after", Kind: "exec", Cmd: []string{"true"}, Needs: []string{"discover"}},
	}}
	csink := sink.Recording()

	status, err := engine.Run(context.Background(), p, engine.Options{
		Dir: dir, Executor: localexec.New(dir, nil), Sink: csink, MaxParallel: 4, RunID: "01GENEMPTY",
		Generators: generatorsFrom(map[string]engine.GenerateFunc{
			"discover": func(context.Context, *plan.Node, string) (*plan.Fragment, error) {
				return &plan.Fragment{Version: plan.FragmentVersion}, nil
			},
		}),
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if status != api.RunSucceeded {
		t.Errorf("status = %s, want succeeded: an empty fragment is not a failure", status)
	}
}

// A generated node may itself be a generator, which is the fork bomb design
// §2.8.2 is blunt about: without a depth limit, a generator that generates a
// generator recurses until the machine gives out, holding whatever credential
// the pipeline was given.
func TestAGeneratorNestedPastMaxDepthFails(t *testing.T) {
	dir := t.TempDir()
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{
		{ID: "discover", Kind: "exec", Cmd: []string{"true"}, Generate: &plan.GenerateSpec{}},
	}}
	csink := sink.Recording()

	status, _ := engine.Run(context.Background(), p, engine.Options{
		Dir: dir, Executor: localexec.New(dir, nil), Sink: csink, MaxParallel: 4, RunID: "01GENDEPTH",
		MaxDepth: 1,
		Generators: generatorsFrom(map[string]engine.GenerateFunc{
			"discover": func(context.Context, *plan.Node, string) (*plan.Fragment, error) {
				return &plan.Fragment{Version: plan.FragmentVersion, Nodes: []plan.Node{
					{ID: "inner", Kind: "exec", Cmd: []string{"true"}, Generate: &plan.GenerateSpec{}},
				}}, nil
			},
			"discover/inner": func(context.Context, *plan.Node, string) (*plan.Fragment, error) {
				return &plan.Fragment{Version: plan.FragmentVersion, Nodes: []plan.Node{
					{ID: "leaf", Kind: "exec", Cmd: []string{"true"}},
				}}, nil
			},
		}),
	})
	if status == api.RunSucceeded {
		t.Fatal("a generator nested past MaxDepth must fail the run rather than recurse")
	}
	for _, e := range csink.Events() {
		if e.Type == api.StepCreated && e.Step == "discover/inner/leaf" {
			t.Error("the too-deep fragment was spliced anyway")
		}
	}
}

// Nesting inside the limit is legal and is the point of having a limit rather
// than a ban: one generator discovering clusters and another discovering what
// each needs is a reasonable two levels.
func TestANestedGeneratorWithinMaxDepthRuns(t *testing.T) {
	dir := t.TempDir()
	marker := filepath.Join(dir, "leaf-ran")
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{
		{ID: "discover", Kind: "exec", Cmd: []string{"true"}, Generate: &plan.GenerateSpec{}},
	}}

	status, err := engine.Run(context.Background(), p, engine.Options{
		Dir: dir, Executor: localexec.New(dir, nil), Sink: sink.Recording(), MaxParallel: 4, RunID: "01GENNEST",
		Generators: generatorsFrom(map[string]engine.GenerateFunc{
			"discover": func(context.Context, *plan.Node, string) (*plan.Fragment, error) {
				return &plan.Fragment{Version: plan.FragmentVersion, Nodes: []plan.Node{
					{ID: "inner", Kind: "exec", Cmd: []string{"true"}, Generate: &plan.GenerateSpec{}},
				}}, nil
			},
			"discover/inner": func(context.Context, *plan.Node, string) (*plan.Fragment, error) {
				return &plan.Fragment{Version: plan.FragmentVersion, Nodes: []plan.Node{
					{ID: "leaf", Kind: "exec", Cmd: []string{"touch", marker}},
				}}, nil
			},
		}),
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if status != api.RunSucceeded {
		t.Errorf("status = %s, want succeeded", status)
	}
	if _, statErr := os.Stat(marker); statErr != nil {
		t.Fatalf("the twice-generated step never ran: %v", statErr)
	}
}

// The budget bounds the whole run, not one fragment: a hundred generators
// producing fifty nodes each is the same runaway as one producing five
// thousand, and only a run-wide count sees it.
func TestASpliceExceedingTheRunsNodeBudgetFails(t *testing.T) {
	dir := t.TempDir()
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{
		{ID: "discover", Kind: "exec", Cmd: []string{"true"}, Generate: &plan.GenerateSpec{}},
	}}
	csink := sink.Recording()

	status, _ := engine.Run(context.Background(), p, engine.Options{
		Dir: dir, Executor: localexec.New(dir, nil), Sink: csink, MaxParallel: 4, RunID: "01GENBUDGET",
		MaxNodes: 3, // the plan already holds one, so a four-node fragment cannot fit
		Generators: generatorsFrom(map[string]engine.GenerateFunc{
			"discover": func(context.Context, *plan.Node, string) (*plan.Fragment, error) {
				f := &plan.Fragment{Version: plan.FragmentVersion}
				for _, id := range []string{"a", "b", "c", "d"} {
					f.Nodes = append(f.Nodes, plan.Node{ID: id, Kind: "exec", Cmd: []string{"true"}})
				}
				return f, nil
			},
		}),
	})
	if status == api.RunSucceeded {
		t.Fatal("a splice past the run's node budget must fail the run")
	}
	// Asserted on the LEDGER rather than on Run's error, because a failed step
	// is a failed status with no error here: the reason reaches an operator
	// through the event stream, so that is where it has to be right.
	var reason string
	for _, e := range csink.Events() {
		if e.Type == api.StepFinished && e.Step == "discover" {
			var b api.StepFinishedBody
			if err := e.Decode(&b); err != nil {
				t.Fatalf("decode step.finished: %v", err)
			}
			reason = b.Error
		}
	}
	// The chain that spent the budget is the actionable part, not the number.
	if !strings.Contains(reason, "discover") || !strings.Contains(reason, "MaxNodes") {
		t.Errorf("step.finished error = %q; it must name the generator chain and the limit it hit", reason)
	}
}

// Re-running a generator must not collide with the subgraph it produced the
// first time. Those nodes are still in the graph, because a splice is
// additive and nothing removes them, and rerun_from has just unsettled them
// along with the generator itself. Splicing the same fragment again is
// therefore a replay of what is already there, not a duplicate.
//
// Without this, the second attempt is refused as "the run already has
// discover/apply" and rerun_from on any generator fails the step it was
// asked to re-run.
func TestRerunFromAGeneratorReplaysItsFragmentWithoutColliding(t *testing.T) {
	dir := t.TempDir()
	gate := filepath.Join(dir, "open")

	csink := newControlSink()
	applyDone := make(chan api.Event, 8)
	csink.onEmit = func(e api.Event) {
		if e.Type == api.StepFinished && e.Step == "discover/apply" {
			applyDone <- e
		}
	}

	p := &plan.Plan{Version: 1, Nodes: []plan.Node{
		{ID: "discover", Kind: "exec", Cmd: []string{"true"}, Generate: &plan.GenerateSpec{}},
		// Keeps the run live long enough for a control request to arrive:
		// control is only served between scheduling passes.
		{ID: "gate", Kind: "exec", Cmd: []string{"sh", "-c",
			"while [ ! -f " + gate + " ]; do sleep 0.02; done"}},
	}}

	out := runAsync(context.Background(), p, engine.Options{
		Dir: dir, Executor: localexec.New(dir, nil), Sink: csink, MaxParallel: 4, RunID: "01GENRERUN",
		Generators: generatorsFrom(map[string]engine.GenerateFunc{
			"discover": func(context.Context, *plan.Node, string) (*plan.Fragment, error) {
				return &plan.Fragment{Version: plan.FragmentVersion, Nodes: []plan.Node{
					{ID: "apply", Kind: "exec", Cmd: []string{"true"}},
				}}, nil
			},
		}),
	})
	waitForEvent(t, applyDone, func(e api.Event) bool { return e.Attempt == 1 })

	if resp := sendSettled(t, csink, sink.ControlRequest{
		ID: "f1", Op: api.OpRunRerunFrom, ClientID: "tester", Args: map[string]string{"step": "discover"},
	}); !resp.OK {
		t.Fatalf("run.rerun_from on a generator = %+v, want OK", resp)
	}
	// Coming back round is what proves the re-splice was accepted: the
	// generated node is in the generator's dependents closure precisely
	// because a fragment node needs the generator that produced it.
	waitForEvent(t, applyDone, func(e api.Event) bool { return e.Attempt == 2 })

	if err := os.WriteFile(gate, nil, 0o600); err != nil {
		t.Fatalf("opening the gate: %v", err)
	}
	res := <-out
	if res.err != nil {
		t.Fatalf("Run: %v", res.err)
	}
	if res.status != api.RunSucceeded {
		t.Errorf("status = %s, want succeeded", res.status)
	}
}

// generatorsFrom adapts a fixed set of generators to the lookup the engine
// takes. Tests know their whole set up front; senro does not (see
// Options.Generators).
func generatorsFrom(m map[string]engine.GenerateFunc) func(string) engine.GenerateFunc {
	return func(id string) engine.GenerateFunc { return m[id] }
}
