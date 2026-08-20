package senro

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/xavidop/senro/internal/engine"
	"github.com/xavidop/senro/internal/funcs"
	"github.com/xavidop/senro/internal/plan"
)

// Generator declares where a step's plan fragment comes from.
//
// A generator is a step whose OUTPUT is a piece of graph: the nodes it
// describes are spliced into the run that is already executing, and they
// become ordinary steps with their own cache entries, retries, logs and
// states. It is the general case that Expand is the cheap special case of.
// Reach for When first and Expand second; a generator is for the list that
// only exists once something has run, which is the one thing neither can do.
//
// Build with GenerateFromJSON. The zero Generator declares nothing.
type Generator struct {
	path string
	// fn is the Go form's closure. It never reaches the plan: a plan must be
	// serializable, and this is a function value. senro.Run carries it to the
	// engine out of band, keyed by the step that declared it.
	fn GenFunc
	// err defers a bad declaration to Build, the way StepBuilder's own errs
	// slice does: GenerateFromJSON returns a value rather than an error so
	// it reads inside a Generates call, and a mistake there must still be
	// reported by name rather than silently producing a generator that
	// generates nothing.
	err error
}

// GenerateFromJSON declares that the step writes its plan fragment to path,
// as JSON, relative to the step's own output root.
//
// This form matters as much as the Go one: "write a plan fragment to this
// path" is a contract a shell script, a Python tool or a Terraform wrapper
// can honour, and the fragment schema is public. A pipeline whose graph is
// decided by a tool that is not written in Go is exactly the case a
// generator exists for.
func GenerateFromJSON(path string) Generator {
	if path == "" {
		return Generator{err: errors.New(
			"GenerateFromJSON needs the path of the file the step writes its fragment to")}
	}
	return Generator{path: path}
}

// GenCtx is what a Go generator is called with: enough to read what the step
// it belongs to produced, and nothing else.
//
// Deliberately narrow. A generator decides SHAPE, and a generator handed the
// engine could change a run it is only supposed to describe.
type GenCtx interface {
	// Step is the id of the generator step, which is also the prefix every
	// id in the returned fragment receives.
	Step() string
	// Dir is the step's own output root, where the files it produced are.
	Dir() string
	// OutputJSON decodes a JSON file the step wrote, relative to Dir, into
	// v. The common case, and the one the design's own example uses.
	OutputJSON(name string, v any) error
}

// GenFunc builds a fragment from what a step produced.
type GenFunc func(GenCtx) (*Fragment, error)

// Generate declares that a Go function on the coordinator turns this step's
// output into a plan fragment.
//
// The function may be as nondeterministic as it likes: it can call an API,
// read a clock, iterate a map. senro records the fragment it produced and
// REPLAYS that recording rather than calling it again (design §2.8.1), so
// reproducibility comes from the record, not from a promise about the code.
// That is the constraint Temporal-style workflow engines put on user code and
// this one does not.
func Generate(fn GenFunc) Generator {
	if fn == nil {
		return Generator{err: errors.New(
			"Generate needs a function that builds the fragment; it received nil")}
	}
	return Generator{fn: fn}
}

// Generates declares that this step produces a plan fragment, spliced into
// the running graph when the step succeeds.
//
// The step is otherwise ordinary: it runs, it can fail, it can be cached. A
// step that fails generates nothing, because a fragment is something a
// successful step produced.
func (s *StepBuilder) Generates(g Generator) *StepBuilder {
	if g.err != nil {
		s.errs = append(s.errs, fmt.Errorf("senro: step %q: %w", s.id, g.err))
		return s
	}
	if s.generate != nil {
		s.errs = append(s.errs, fmt.Errorf(
			"senro: step %q declares Generates twice; a step produces at most one fragment", s.id))
		return s
	}
	s.generate = &g
	return s
}

// Fragment is a piece of graph a generator builds: the steps to splice into
// the running graph, and the boundary the generator's dependents wait on.
//
// Step ids are RELATIVE to the generator. The engine prefixes them with the
// generator's own id, so a fragment does not need to know where it will land
// and the ids it produces are hierarchical and stable.
type Fragment struct {
	steps    []*StepBuilder
	boundary []string
}

// NewFragment starts an empty fragment. An empty fragment is legal and
// common: it means "nothing to do here", and the generator's dependents run
// immediately rather than being skipped.
func NewFragment() *Fragment { return &Fragment{} }

// Step adds one step to the fragment, with the same shape a workflow's Step
// has so a fragment is written in the vocabulary the rest of a pipeline
// already uses. Needs names other steps IN THIS FRAGMENT, by their relative
// id.
func (f *Fragment) Step(id string, a Action) *StepBuilder {
	sb := &StepBuilder{id: id, action: a}
	f.steps = append(f.steps, sb)
	return sb
}

// Boundary declares which of this fragment's steps the generator's existing
// dependents must wait on.
//
// Without it a downstream step would run as soon as the generator finished,
// which is the moment the generated work STARTS rather than the moment it is
// done. Declaring nothing is legal and means exactly that: the generator
// produced work nobody downstream consumes.
func (f *Fragment) Boundary(steps ...*StepBuilder) *Fragment {
	for _, s := range steps {
		f.boundary = append(f.boundary, s.id)
	}
	return f
}

// MarshalJSON writes the fragment in the public wire schema, the same one a
// generator in any other language writes.
//
// A Go fragment reaching the engine as bytes, and being parsed back exactly
// as a JSON one is, is deliberate: the two forms then cannot drift, they
// share one validation path, and the blob recorded in the CAS is the same
// whichever produced it. The cost is one round trip per generator, which is
// nothing next to running the step that produced it.
func (f *Fragment) MarshalJSON() ([]byte, error) {
	pf := plan.Fragment{
		Version:  plan.FragmentVersion,
		Boundary: append([]string(nil), f.boundary...),
	}
	for _, sb := range f.steps {
		n, err := toNode(sb, nil)
		if err != nil {
			return nil, err
		}
		pf.Nodes = append(pf.Nodes, n)
	}
	return json.Marshal(pf)
}

// genCtx is the GenCtx a Go generator is called with.
type genCtx struct {
	step string
	dir  string
}

func (c genCtx) Step() string { return c.step }
func (c genCtx) Dir() string  { return c.dir }

func (c genCtx) OutputJSON(name string, v any) error {
	if c.dir == "" {
		return fmt.Errorf(
			"senro: step %q reads %q, but it mounts no workspace, so senro has nowhere to "+
				"read what it produced; mount one with Mount", c.step, name)
	}
	b, err := os.ReadFile(filepath.Join(c.dir, name))
	if err != nil {
		return fmt.Errorf("senro: step %q: reading %s: %w", c.step, name, err)
	}
	if err := json.Unmarshal(b, v); err != nil {
		return fmt.Errorf("senro: step %q: decoding %s: %w", c.step, name, err)
	}
	return nil
}

// generatorRegistry resolves a step id to its Go generator.
//
// Mutable and locked, because the set grows DURING the run: a generated node
// can itself declare a Go generator, and that node does not exist when the
// run starts. adaptGenerator registers those the moment the fragment naming
// them is produced, which is strictly before the engine can schedule them.
type generatorRegistry struct {
	mu sync.Mutex
	fn map[string]engine.GenerateFunc
}

func (r *generatorRegistry) add(id string, fn engine.GenerateFunc) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.fn == nil {
		r.fn = make(map[string]engine.GenerateFunc)
	}
	r.fn[id] = fn
}

func (r *generatorRegistry) lookup(id string) engine.GenerateFunc {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.fn[id]
}

// generators collects the Go generators this pipeline declared, keyed by step
// id, for senro.Run to hand to the engine.
//
// Only the Go form appears: a JSON generator's fragment is a file the engine
// reads unaided, so it needs nothing carried. A plan run from disk therefore
// supports JSON generators and not Go ones, which is not a gap but the same
// fact stated twice: a closure was never in the plan to begin with.
func (p *Pipeline) generators() *generatorRegistry {
	r := &generatorRegistry{}
	for _, w := range p.workflows {
		for _, sb := range w.steps {
			if sb.generate == nil || sb.generate.fn == nil {
				continue
			}
			r.add(sb.id, adaptGenerator(sb.generate.fn, sb.id, r))
		}
	}
	return r
}

// adaptGenerator turns a caller's GenFunc into the engine's GenerateFunc.
//
// The fragment goes out as WIRE BYTES and comes back through
// plan.ParseFragment, the very code a JSON generator's file takes. That round
// trip is the point: the two forms cannot drift into accepting different
// things, and the bytes a run records for a Go generator are the same bytes it
// would have recorded for the equivalent file.
//
// Before the round trip, any step in the fragment that declares its OWN Go
// generator is registered under the id it will be spliced under. The closure
// cannot survive being serialized, so it is kept here instead, and the node
// carries only the empty GenerateSpec that means "Go form, look it up".
func adaptGenerator(fn GenFunc, generatorID string, reg *generatorRegistry) engine.GenerateFunc {
	return func(_ context.Context, n *plan.Node, root string) (*plan.Fragment, error) {
		f, err := fn(genCtx{step: n.ID, dir: root})
		if err != nil {
			return nil, err
		}
		if f == nil {
			return nil, fmt.Errorf("senro: step %q: its generator returned no fragment", n.ID)
		}
		for _, sb := range f.steps {
			if sb.generate == nil || sb.generate.fn == nil {
				continue
			}
			childID := n.ID + "/" + sb.id
			reg.add(childID, adaptGenerator(sb.generate.fn, childID, reg))
		}
		b, err := json.Marshal(f)
		if err != nil {
			return nil, fmt.Errorf("senro: step %q: serializing its fragment: %w", n.ID, err)
		}
		return plan.ParseFragment(b)
	}
}

// RunSubgraph runs f as a nested graph, from inside a registered function,
// and returns when it has finished.
//
// This is the imperative escape hatch. Some control flow genuinely is not a
// DAG: "roll out to clusters one at a time until quorum, then stop" cannot be
// drawn as one, because whether the next node runs depends on what the
// previous ones did. A function can express that directly, calling this once
// per iteration.
//
//	senro.RegisterFunc("deploy/rolling", func(ctx senro.Ctx, p RollParams) error {
//	    for _, c := range p.Clusters {
//	        if err := senro.RunSubgraph(ctx, deployFragment(c)); err != nil {
//	            return err
//	        }
//	        if quorumReached(ctx) {
//	            return nil
//	        }
//	    }
//	    return errNoQuorum
//	})
//
// Prefer a GENERATOR (Generates) for anything that is a graph. A generator's
// nodes are ordinary steps: individually cached, individually retried,
// individually visible. A subgraph is work this step is doing, so the cache
// and re-run granularity is the WHOLE subgraph and re-running means re-running
// the step. That is the price of expressing a loop with a stopping condition.
//
// A free function rather than a method on Ctx because not every Ctx can offer
// it: a func step running on a remote host is a staged binary on the far side
// of a transport, and the engine a subgraph needs is back on the coordinator.
// Called from there, this returns an error saying so by name.
func RunSubgraph(ctx Ctx, f *Fragment) error {
	if f == nil {
		return errors.New("senro: RunSubgraph was given no fragment")
	}
	runner, ok := ctx.(funcs.SubgraphRunner)
	if !ok {
		return errors.New(
			"senro: this step cannot run a subgraph; it is not running on the coordinator")
	}
	b, err := json.Marshal(f)
	if err != nil {
		return fmt.Errorf("senro: RunSubgraph: serializing the fragment: %w", err)
	}
	return runner.RunSubgraph(ctx, b)
}
