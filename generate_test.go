package senro_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/xavidop/senro"
	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/artifact"
	"github.com/xavidop/senro/exec"
	"github.com/xavidop/senro/internal/plan"
	"github.com/xavidop/senro/internal/sink"
)

// node returns the built node with this id, or fails the test. Generator
// tests assert on what reached the PLAN, because the plan is the only thing
// the engine reads: a declaration that stops at the builder is a declaration
// the run never sees.
func node(t *testing.T, p *senro.Plan, id string) plan.Node {
	t.Helper()
	for i := range p.Nodes {
		if p.Nodes[i].ID == id {
			return p.Nodes[i]
		}
	}
	t.Fatalf("no node %q in the built plan", id)
	return plan.Node{}
}

// A generator is an ordinary step that also says where its plan fragment
// comes from. GenerateFromJSON names a file the step writes, which is the
// form a shell script or a Python tool can honour, so the path has to
// survive into the plan rather than living only in the builder.
func TestAStepGeneratingFromJSONCarriesThePathIntoItsNode(t *testing.T) {
	pipe := senro.New("release")
	l := pipe.Workflow("main")
	l.Step("discover", exec.Command("./list-clusters")).
		Generates(senro.GenerateFromJSON("fragment.json"))

	p, err := pipe.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	n := node(t, p, "discover")
	if n.Generate == nil {
		t.Fatal("node.Generate is nil; the step declared GenerateFromJSON and the engine reads that from the plan")
	}
	if n.Generate.Path != "fragment.json" {
		t.Errorf("Generate.Path = %q, want %q", n.Generate.Path, "fragment.json")
	}
}

// A step that declares no generator must carry no generator. Without this
// every ordinary node would grow a generate field, which moves the digest of
// every plan ever built.
func TestAnOrdinaryStepCarriesNoGenerator(t *testing.T) {
	pipe := senro.New("release")
	l := pipe.Workflow("main")
	l.Step("build", exec.Command("true"))

	p, err := pipe.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if n := node(t, p, "build"); n.Generate != nil {
		t.Errorf("Generate = %+v, want nil for a step that declared none", n.Generate)
	}
}

// An empty path is refused at Build rather than at run time: "the file the
// step writes its fragment to" with no name is a typo, and the run would
// otherwise reach the splice before saying so.
func TestGenerateFromJSONWithNoPathIsRefusedAtBuild(t *testing.T) {
	pipe := senro.New("release")
	l := pipe.Workflow("main")
	l.Step("discover", exec.Command("./list-clusters")).Generates(senro.GenerateFromJSON(""))

	_, err := pipe.Build()
	if err == nil {
		t.Fatal("Build must reject a generator whose fragment path is empty")
	}
	if !strings.Contains(err.Error(), "discover") {
		t.Errorf("error %q must name the step it is about", err)
	}
}

// A fragment built in Go and a fragment written as JSON by a shell script
// must be the same thing by the time anything reads it. Serializing the Go
// form and parsing it back through the very code the JSON form uses is what
// makes that true by construction rather than by review: one schema, one
// validation path, and one set of bytes to put in the CAS whichever form
// produced it.
func TestAGoFragmentSerializesToTheSameWireFormAJSONGeneratorWrites(t *testing.T) {
	f := senro.NewFragment()
	pre := f.Step("preflight-cm4", exec.Command("./preflight", "cm4"))
	f.Step("apply-cm4", exec.Command("./apply", "cm4")).Needs("preflight-cm4")
	f.Boundary(f.Step("verify-cm4", exec.Command("./verify", "cm4")))
	_ = pre

	b, err := json.Marshal(f)
	if err != nil {
		t.Fatalf("marshal fragment: %v", err)
	}

	got, err := plan.ParseFragment(b)
	if err != nil {
		t.Fatalf("ParseFragment on the bytes the Go form produced: %v", err)
	}
	if len(got.Nodes) != 3 {
		t.Fatalf("Nodes = %d, want 3", len(got.Nodes))
	}
	if got.Nodes[1].ID != "apply-cm4" {
		t.Errorf("Nodes[1].ID = %q, want %q: a fragment keeps its declared order", got.Nodes[1].ID, "apply-cm4")
	}
	if len(got.Nodes[1].Needs) != 1 || got.Nodes[1].Needs[0] != "preflight-cm4" {
		t.Errorf("Nodes[1].Needs = %v, want [preflight-cm4]", got.Nodes[1].Needs)
	}
	if len(got.Boundary) != 1 || got.Boundary[0] != "verify-cm4" {
		t.Errorf("Boundary = %v, want [verify-cm4]: it is what the generator's dependents wait on", got.Boundary)
	}
}

// The Go form declares the same thing as the JSON form and carries no path:
// its fragment comes from a closure on the coordinator, which no plan can
// hold. The engine tells the two apart by exactly this, so a Go generator
// whose node carried a path would be read as a file that will never exist.
func TestAStepGeneratingInGoCarriesAGeneratorWithNoPath(t *testing.T) {
	pipe := senro.New("release")
	l := pipe.Workflow("main")
	l.Step("discover", exec.Command("./list-clusters")).
		Generates(senro.Generate(func(senro.GenCtx) (*senro.Fragment, error) {
			return senro.NewFragment(), nil
		}))

	p, err := pipe.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	n := node(t, p, "discover")
	if n.Generate == nil {
		t.Fatal("node.Generate is nil; the step declared Generate")
	}
	if n.Generate.Path != "" {
		t.Errorf("Generate.Path = %q, want empty: a Go generator has no file to read", n.Generate.Path)
	}
}

// A nil function is refused at Build. It would otherwise be a step that
// declares it generates, produces nothing, and leaves every dependent
// waiting on a boundary that never arrives.
func TestGenerateWithNoFunctionIsRefusedAtBuild(t *testing.T) {
	pipe := senro.New("release")
	l := pipe.Workflow("main")
	l.Step("discover", exec.Command("./list-clusters")).Generates(senro.Generate(nil))

	_, err := pipe.Build()
	if err == nil {
		t.Fatal("Build must reject a Go generator with no function")
	}
	if !strings.Contains(err.Error(), "discover") {
		t.Errorf("error %q must name the step it is about", err)
	}
}

// Declaring both forms on one step is a contradiction, not a merge: the
// engine would have to pick one, and whichever it picked would silently
// ignore something the author wrote down.
func TestAStepDeclaringGeneratesTwiceIsRefusedAtBuild(t *testing.T) {
	pipe := senro.New("release")
	l := pipe.Workflow("main")
	l.Step("discover", exec.Command("./list-clusters")).
		Generates(senro.GenerateFromJSON("a.json")).
		Generates(senro.GenerateFromJSON("b.json"))

	_, err := pipe.Build()
	if err == nil {
		t.Fatal("Build must reject a step that declares Generates twice")
	}
	if !strings.Contains(err.Error(), "discover") {
		t.Errorf("error %q must name the step it is about", err)
	}
}

// The Go form, end to end through the public API: a closure cannot live in a
// plan, so senro has to carry it to the run itself. Without that wiring
// senro.Generate builds a plan that describes a generator and then fails at
// run time for want of the function it just declared.
func TestAGoGeneratorsFragmentReachesTheRun(t *testing.T) {
	dir := t.TempDir()
	marker := filepath.Join(dir, "generated-ran")

	pipe := senro.New("release")
	l := pipe.Workflow("main")
	l.Step("discover", exec.Command("true")).
		Generates(senro.Generate(func(ctx senro.GenCtx) (*senro.Fragment, error) {
			if ctx.Step() != "discover" {
				return nil, fmt.Errorf("GenCtx.Step() = %q, want %q", ctx.Step(), "discover")
			}
			f := senro.NewFragment()
			f.Step("apply", exec.Command("touch", marker))
			return f, nil
		}))

	if err := senro.Run(t.Context(), pipe, senro.WithDir(filepath.Join(dir, "run"))); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if _, err := os.Stat(marker); err != nil {
		t.Fatalf("the step the generator produced never ran: %v", err)
	}
}

// The claim §2.8.1 rests on: a generator may be nondeterministic because
// senro records the fragment it produced and replays that recording. A cached
// generator therefore does not run at all, and the run it serves still has
// every node the generator once produced.
//
// The counter is the whole point. If the second run calls the generator
// again, the recording is decoration and a generator that queried an API
// would silently reshape a re-run.
func TestACachedGeneratorRestoresItsFragmentWithoutRunningAgain(t *testing.T) {
	root := t.TempDir()
	cacheDir := filepath.Join(root, "cache")

	var mu sync.Mutex
	calls := 0

	build := func() *senro.Pipeline {
		ws := senro.Workspace("src", senro.Scope(senro.ScopeRun))
		pipe := senro.New("ci")
		l := pipe.Workflow("main")
		l.Step("seed", exec.Command("sh", "-c", "echo hi > in.txt")).
			WorkDir("/src").Mount(ws.At("/src", senro.RW))
		l.Step("discover", exec.Command("true")).Needs("seed").
			WorkDir("/src").Mount(ws.At("/src", senro.RO)).
			Pure().Inputs(artifact.Glob("**/*.txt")).
			Generates(senro.Generate(func(senro.GenCtx) (*senro.Fragment, error) {
				mu.Lock()
				calls++
				mu.Unlock()
				f := senro.NewFragment()
				f.Step("apply", exec.Command("true"))
				return f, nil
			}))
		return pipe
	}

	run := func(name string) []api.Event {
		t.Helper()
		rec := sink.Recording()
		if err := senro.Run(context.Background(), build(),
			senro.WithDir(filepath.Join(root, name)),
			senro.WithCacheDir(cacheDir),
			senro.WithSink(rec)); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		return rec.Events()
	}

	first := run("run1")
	second := run("run2")

	var firstHadHit, secondHadHit bool
	for _, e := range first {
		if e.Type == api.CacheHit && e.Step == "discover" {
			firstHadHit = true
		}
	}
	for _, e := range second {
		if e.Type == api.CacheHit && e.Step == "discover" {
			secondHadHit = true
		}
	}
	if firstHadHit {
		t.Fatal("the first run hit the cache; this test needs it to be a cold miss")
	}
	if !secondHadHit {
		t.Fatal("the second run did not hit the cache for the generator, so it proves nothing about replay")
	}

	mu.Lock()
	n := calls
	mu.Unlock()
	if n != 1 {
		t.Errorf("the generator ran %d times, want 1: a cached generator restores its recorded fragment "+
			"rather than being asked again", n)
	}

	var restored bool
	for _, e := range second {
		if e.Type == api.StepFinished && e.Step == "discover/apply" {
			restored = true
		}
	}
	if !restored {
		t.Error("the second run has no discover/apply: a cached generator must restore the graph it produced, " +
			"not silently drop it")
	}
}

// A recorded fragment can go missing: the cache is garbage collected, and a
// blob no entry still needs is exactly what a GC removes. When it does, the
// entry must stop being a usable hit and the generator must run again.
//
// The failure this guards is silent and severe: serving the hit anyway would
// mark the generator cached and carry on against a graph with none of the
// work it existed to produce.
func TestAGeneratorWhoseRecordedFragmentIsGoneRunsAgain(t *testing.T) {
	root := t.TempDir()
	cacheDir := filepath.Join(root, "cache")

	var mu sync.Mutex
	calls := 0

	build := func() *senro.Pipeline {
		ws := senro.Workspace("src", senro.Scope(senro.ScopeRun))
		pipe := senro.New("ci")
		l := pipe.Workflow("main")
		l.Step("seed", exec.Command("sh", "-c", "echo hi > in.txt")).
			WorkDir("/src").Mount(ws.At("/src", senro.RW))
		l.Step("discover", exec.Command("true")).Needs("seed").
			WorkDir("/src").Mount(ws.At("/src", senro.RO)).
			Pure().Inputs(artifact.Glob("**/*.txt")).
			Generates(senro.Generate(func(senro.GenCtx) (*senro.Fragment, error) {
				mu.Lock()
				calls++
				mu.Unlock()
				f := senro.NewFragment()
				f.Step("apply", exec.Command("true"))
				return f, nil
			}))
		return pipe
	}

	run := func(name string) []api.Event {
		t.Helper()
		rec := sink.Recording()
		if err := senro.Run(context.Background(), build(),
			senro.WithDir(filepath.Join(root, name)),
			senro.WithCacheDir(cacheDir),
			senro.WithSink(rec)); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		return rec.Events()
	}

	first := run("run1")

	// The digest the run recorded, taken from the ledger rather than
	// recomputed: what a reader can see is what the entry points at.
	var digest string
	for _, e := range first {
		if e.Type == api.PlanGenerated {
			var b api.PlanGeneratedBody
			if err := e.Decode(&b); err != nil {
				t.Fatalf("decode plan.generated: %v", err)
			}
			digest = b.Digest
		}
	}
	if digest == "" {
		t.Fatal("the first run recorded no fragment digest, so there is nothing to lose")
	}

	hex := strings.TrimPrefix(digest, "sha256:")
	blob := filepath.Join(cacheDir, "cas", "sha256", hex[0:2], hex[2:4], hex)
	if err := os.Remove(blob); err != nil {
		t.Fatalf("removing the recorded fragment at %s: %v", blob, err)
	}

	second := run("run2")

	mu.Lock()
	n := calls
	mu.Unlock()
	if n != 2 {
		t.Errorf("the generator ran %d times, want 2: with its recording gone the entry is not servable", n)
	}
	var restored, hit bool
	for _, e := range second {
		if e.Type == api.StepFinished && e.Step == "discover/apply" {
			restored = true
		}
		if e.Type == api.CacheHit && e.Step == "discover" {
			hit = true
		}
	}
	if hit {
		t.Error("the entry was served as a hit despite its fragment being gone")
	}
	if !restored {
		t.Error("the run lost discover/apply: degrading to a miss must re-run the generator, not drop its work")
	}
}

// A fragment is built in a loop, where a step usually needs the sibling it
// just created. Without an accessor every id is written twice, once to
// declare it and once to depend on it, and the second copy is where the typo
// goes: a mistyped need is refused at splice time, mid-run, rather than by
// the compiler.
func TestAStepBuildersIDIsReadableForDeclaringNeeds(t *testing.T) {
	f := senro.NewFragment()
	pre := f.Step("preflight-cm4", exec.Command("true"))
	if pre.ID() != "preflight-cm4" {
		t.Fatalf("ID() = %q, want %q", pre.ID(), "preflight-cm4")
	}
	f.Step("apply-cm4", exec.Command("true")).Needs(pre.ID())

	b, err := json.Marshal(f)
	if err != nil {
		t.Fatalf("marshal fragment: %v", err)
	}
	got, err := plan.ParseFragment(b)
	if err != nil {
		t.Fatalf("ParseFragment: %v", err)
	}
	if len(got.Nodes[1].Needs) != 1 || got.Nodes[1].Needs[0] != "preflight-cm4" {
		t.Errorf("Needs = %v, want [preflight-cm4]", got.Nodes[1].Needs)
	}
}

// The JSON form end to end: a step writes a fragment file and senro reads it.
// This is the form that matters for a pipeline whose graph is decided by a
// tool that is not written in Go, and it shares no code with the Go form past
// the point the bytes are parsed.
func TestAJSONGeneratorsFragmentIsReadFromTheStepsWorkspace(t *testing.T) {
	root := t.TempDir()
	marker := filepath.Join(root, "generated-ran")

	fragment := `{"version":1,"nodes":[` +
		`{"id":"apply","kind":"exec","cmd":["touch","` + marker + `"]}` +
		`],"boundary":["apply"]}`

	ws := senro.Workspace("src", senro.Scope(senro.ScopeRun))
	pipe := senro.New("ci")
	l := pipe.Workflow("main")
	l.Step("discover", exec.Command("sh", "-c", "printf '%s' '"+fragment+"' > fragment.json")).
		WorkDir("/src").Mount(ws.At("/src", senro.RW)).
		Generates(senro.GenerateFromJSON("fragment.json"))

	rec := sink.Recording()
	if err := senro.Run(context.Background(), pipe,
		senro.WithDir(filepath.Join(root, "run")),
		senro.WithCacheDir(filepath.Join(root, "cache")),
		senro.WithSink(rec)); err != nil {
		t.Fatalf("Run: %v", err)
	}

	if _, err := os.Stat(marker); err != nil {
		t.Fatalf("the step named in the fragment file never ran: %v", err)
	}
	var created bool
	for _, e := range rec.Events() {
		if e.Type == api.StepCreated && e.Step == "discover/apply" {
			created = true
		}
	}
	if !created {
		t.Error("no step.created for discover/apply: a fragment read from a file is named under its generator too")
	}
}

// The limits exist to stop a runaway generator, but a pipeline that
// legitimately wants a bigger graph has to be able to say so. Without an
// option the bound is unreachable from the public API and a real fan-out of
// six thousand nodes is simply impossible.
func TestWithMaxNodesRaisesTheRunsNodeBudget(t *testing.T) {
	root := t.TempDir()
	build := func() *senro.Pipeline {
		pipe := senro.New("ci")
		l := pipe.Workflow("main")
		l.Step("discover", exec.Command("true")).
			Generates(senro.Generate(func(senro.GenCtx) (*senro.Fragment, error) {
				f := senro.NewFragment()
				for _, id := range []string{"a", "b", "c", "d"} {
					f.Step(id, exec.Command("true"))
				}
				return f, nil
			}))
		return pipe
	}

	// One plan node plus four generated is five, so a budget of three refuses.
	err := senro.Run(context.Background(), build(),
		senro.WithDir(filepath.Join(root, "tight")), senro.WithMaxNodes(3))
	if err == nil {
		t.Fatal("a budget smaller than the graph must fail the run")
	}

	// The same pipeline with room to breathe.
	if err := senro.Run(context.Background(), build(),
		senro.WithDir(filepath.Join(root, "roomy")), senro.WithMaxNodes(50)); err != nil {
		t.Fatalf("Run with a raised budget: %v", err)
	}
}

// Nesting, through the public API, with the inner generator written in Go.
//
// That inner closure belongs to a node that does not exist until the outer
// generator has run, so it cannot be registered up front the way a pipeline
// step's is. Senro registers it when the fragment that declares it is
// produced, which is what makes a Go generator inside a Go generator work at
// all, and MaxDepth is what keeps it from being unbounded.
func TestWithMaxDepthBoundsNestedGoGenerators(t *testing.T) {
	root := t.TempDir()
	build := func(marker string) *senro.Pipeline {
		pipe := senro.New("ci")
		l := pipe.Workflow("main")
		l.Step("discover", exec.Command("true")).
			Generates(senro.Generate(func(senro.GenCtx) (*senro.Fragment, error) {
				f := senro.NewFragment()
				f.Step("inner", exec.Command("true")).
					Generates(senro.Generate(func(senro.GenCtx) (*senro.Fragment, error) {
						g := senro.NewFragment()
						g.Step("leaf", exec.Command("touch", marker))
						return g, nil
					}))
				return f, nil
			}))
		return pipe
	}

	deep := filepath.Join(root, "leaf-ran")
	if err := senro.Run(context.Background(), build(deep),
		senro.WithDir(filepath.Join(root, "ok")), senro.WithMaxDepth(2)); err != nil {
		t.Fatalf("two levels of generation must fit under MaxDepth(2): %v", err)
	}
	if _, err := os.Stat(deep); err != nil {
		t.Fatalf("the twice-generated step never ran: %v", err)
	}

	if err := senro.Run(context.Background(), build(filepath.Join(root, "never")),
		senro.WithDir(filepath.Join(root, "shallow")), senro.WithMaxDepth(1)); err == nil {
		t.Fatal("a second level of generation must be refused under MaxDepth(1)")
	}
}

// --regenerate is the other half of the replay story: sometimes you WANT the
// generator asked again, because the world has changed and the recorded
// graph describes a fleet that no longer exists. It is a separate verb
// precisely because silently re-deriving a graph during what looked like a
// retry is a confusing failure.
func TestRegenerateAsksTheGeneratorAgainInsteadOfReplaying(t *testing.T) {
	root := t.TempDir()
	cacheDir := filepath.Join(root, "cache")

	var mu sync.Mutex
	calls := 0

	build := func() *senro.Pipeline {
		ws := senro.Workspace("src", senro.Scope(senro.ScopeRun))
		pipe := senro.New("ci")
		l := pipe.Workflow("main")
		l.Step("seed", exec.Command("sh", "-c", "echo hi > in.txt")).
			WorkDir("/src").Mount(ws.At("/src", senro.RW))
		l.Step("discover", exec.Command("true")).Needs("seed").
			WorkDir("/src").Mount(ws.At("/src", senro.RO)).
			Pure().Inputs(artifact.Glob("**/*.txt")).
			Generates(senro.Generate(func(senro.GenCtx) (*senro.Fragment, error) {
				mu.Lock()
				calls++
				mu.Unlock()
				f := senro.NewFragment()
				f.Step("apply", exec.Command("true"))
				return f, nil
			}))
		return pipe
	}

	if err := senro.Run(context.Background(), build(),
		senro.WithDir(filepath.Join(root, "run1")), senro.WithCacheDir(cacheDir)); err != nil {
		t.Fatalf("run1: %v", err)
	}
	// Without it, the second run replays and the generator is not asked.
	if err := senro.Run(context.Background(), build(),
		senro.WithDir(filepath.Join(root, "run2")), senro.WithCacheDir(cacheDir)); err != nil {
		t.Fatalf("run2: %v", err)
	}
	mu.Lock()
	replayed := calls
	mu.Unlock()
	if replayed != 1 {
		t.Fatalf("the generator ran %d times without --regenerate, want 1", replayed)
	}

	if err := senro.Run(context.Background(), build(),
		senro.WithDir(filepath.Join(root, "run3")), senro.WithCacheDir(cacheDir),
		senro.WithRegenerate()); err != nil {
		t.Fatalf("run3: %v", err)
	}
	mu.Lock()
	regenerated := calls
	mu.Unlock()
	if regenerated != 2 {
		t.Errorf("the generator ran %d times in total, want 2: --regenerate must ask it again", regenerated)
	}
}

// Restricting a run to some steps is what makes re-running one piece of a
// recorded run possible without executing the whole thing again.
func TestWithOnlyStepsRestrictsWhatExecutes(t *testing.T) {
	root := t.TempDir()
	ranA := filepath.Join(root, "a-ran")
	ranB := filepath.Join(root, "b-ran")

	pipe := senro.New("ci")
	l := pipe.Workflow("main")
	l.Step("a", exec.Command("touch", ranA))
	l.Step("b", exec.Command("touch", ranB))

	if err := senro.Run(context.Background(), pipe,
		senro.WithDir(filepath.Join(root, "run")), senro.WithOnlySteps("b")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if _, err := os.Stat(ranB); err != nil {
		t.Errorf("the selected step did not run: %v", err)
	}
	if _, err := os.Stat(ranA); err == nil {
		t.Error("a step outside the selection ran anyway")
	}
}
