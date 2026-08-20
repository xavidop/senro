package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/internal/cas"
	"github.com/xavidop/senro/internal/plan"
)

// GenerateFunc turns a finished generator step into a plan fragment.
//
// It is how the Go form of a generator reaches the engine: a closure cannot
// live in a plan, so senro.Run carries it here keyed by step id (see
// senro.Generate). The JSON form needs no entry, because its fragment is a
// file the step wrote and the engine can read it unaided.
type GenerateFunc func(ctx context.Context, n *plan.Node, root string) (*plan.Fragment, error)

const (
	// DefaultMaxDepth bounds generator nesting when Options.MaxDepth is zero.
	// Three, because two levels is a real shape (discover the clusters, then
	// discover what each needs) and beyond that a pipeline is describing a
	// program rather than a graph.
	DefaultMaxDepth = 3

	// DefaultMaxNodes bounds a run's whole node count when Options.MaxNodes
	// is zero. Distinct from plan.DefaultMaxNodes, which bounds ONE expansion
	// at plan time; this bounds everything a run can hold.
	DefaultMaxNodes = 5000
)

// splice runs n's generator and adds what it produced to the graph.
//
// Called from runStep BEFORE the step settles, which is what makes the
// boundary attachment safe: the generator's dependents cannot have started,
// because they need a generator whose state has not been written yet.
//
// Every failure returns an error and adds NOTHING. A generator that produced
// a graph senro cannot splice has not done its job, and its step fails,
// rather than the run carrying on against a fragment that was half applied.
func (rc *runCore) splice(ctx context.Context, n *plan.Node, opts Options) (string, error) {
	f, err := rc.fragmentFor(ctx, n, opts)
	if err != nil {
		return "", err
	}
	// Recorded from the PARSED fragment, not from whatever bytes produced it:
	// a Go closure and a hand-written file that describe the same graph then
	// record the same blob, and an author's indentation cannot change a
	// digest.
	digest, err := rc.recordFragment(ctx, f, opts)
	if err != nil {
		return "", fmt.Errorf("engine: step %q: recording its fragment: %w", n.ID, err)
	}

	rc.oc.mu.Lock()
	nodes, boundary, err := plan.SpliceFragment(f, n.ID, rc.graphExcludingOwnLocked(n.ID))
	if err != nil {
		rc.oc.mu.Unlock()
		return "", err
	}
	if err := rc.withinLimitsLocked(n.ID, rc.newCountLocked(nodes), opts); err != nil {
		rc.oc.mu.Unlock()
		return "", err
	}
	rc.addLocked(n.ID, nodes, boundary)
	rc.oc.mu.Unlock()
	rc.publish(n.ID, nodes, digest)
	return digest, nil
}

// addLocked puts a validated fragment into the running graph.
//
// The boundary is attached BEFORE the nodes are added, so the graph a reader
// sees from plan.generated onwards is already the whole truth rather than a
// set of nodes whose dependents have not been rewired yet.
//
// Caller holds rc.oc.mu.
func (rc *runCore) addLocked(generatorID string, nodes []plan.Node, boundary []string) {
	rc.attachBoundaryLocked(generatorID, boundary)
	depth := rc.genDepth[generatorID] + 1
	for i := range nodes {
		node := nodes[i]
		if _, replay := rc.byID[node.ID]; replay {
			// This generator produced this node before and rerun_from has
			// just unsettled it: replace the value rather than adding a
			// second node answering to the same name.
			rc.byID[node.ID] = &node
			for j := range *rc.live {
				if (*rc.live)[j].ID == node.ID {
					(*rc.live)[j] = &node
					break
				}
			}
		} else {
			rc.byID[node.ID] = &node
			*rc.live = append(*rc.live, &node)
		}
		if rc.genDepth == nil {
			rc.genDepth = make(map[string]int)
			rc.genParent = make(map[string]string)
		}
		rc.genDepth[node.ID] = depth
		rc.genParent[node.ID] = generatorID
	}
}

// publish announces a splice: what was added, and the recording that will let
// a later run reproduce it.
//
// Shared by the live and the cache-restored paths on purpose. A reader must
// not be able to tell from the stream whether a generator ran or was served
// from cache, exactly as replayLog makes a cached step's logs
// indistinguishable from a step that really ran.
func (rc *runCore) publish(generatorID string, nodes []plan.Node, digest string) {
	children := make([]string, 0, len(nodes))
	edges := 0
	for i := range nodes {
		children = append(children, nodes[i].ID)
		edges += len(nodes[i].Needs)
	}
	rc.emit(api.Event{
		Type: api.PlanGenerated, Step: generatorID, Group: generatorID,
		Payload: mustMarshal(api.PlanGeneratedBody{
			Generator: generatorID, Children: children,
			Nodes: len(nodes), Edges: edges, Digest: digest,
		}),
	})
	// One per generated node, for the reason the static plan emits them at
	// the start: a reader folding the stream learns a node's kind and needs
	// from step.created, and a generated node has no earlier mention.
	for i := range nodes {
		rc.emit(api.Event{
			Type: api.StepCreated, Step: nodes[i].ID, Group: generatorID,
			Payload: mustMarshal(api.StepCreatedBody{
				Kind: nodes[i].Kind, Group: generatorID, Needs: nodes[i].Needs,
			}),
		})
	}
}

// recordFragment stores a fragment's canonical bytes and returns their
// digest, or "" when this run has no object store to put them in.
//
// A run without a store still splices: recording is what makes a LATER run
// reproduce this one, and a run that cannot cache has no later run to serve.
func (rc *runCore) recordFragment(ctx context.Context, f *plan.Fragment, opts Options) (string, error) {
	if opts.Storage == nil || opts.Storage.Objects == nil {
		return "", nil
	}
	b, err := json.Marshal(f)
	if err != nil {
		return "", err
	}
	d, err := cas.PutBytes(ctx, opts.Storage.Objects, b)
	if err != nil {
		return "", err
	}
	return string(d), nil
}

// spliceRecorded replays the fragment a previous run recorded, for a
// generator served from the cache.
//
// This is §2.8.1 made real: the generator is not called, so it may be as
// nondeterministic as it likes, and the graph the run gets is the one that
// was actually produced rather than one derived again from a changed world.
func (rc *runCore) spliceRecorded(ctx context.Context, n *plan.Node, digest string, opts Options) error {
	b, err := cas.GetBytes(ctx, opts.Storage.Objects, cas.Digest(digest))
	if err != nil {
		return fmt.Errorf("engine: step %q: reading its recorded fragment: %w", n.ID, err)
	}
	f, err := plan.ParseFragment(b)
	if err != nil {
		return fmt.Errorf("engine: step %q: its recorded fragment: %w", n.ID, err)
	}
	rc.oc.mu.Lock()
	nodes, boundary, err := plan.SpliceFragment(f, n.ID, rc.graphExcludingOwnLocked(n.ID))
	if err != nil {
		rc.oc.mu.Unlock()
		return err
	}
	if err := rc.withinLimitsLocked(n.ID, rc.newCountLocked(nodes), opts); err != nil {
		rc.oc.mu.Unlock()
		return err
	}
	rc.addLocked(n.ID, nodes, boundary)
	rc.oc.mu.Unlock()
	rc.publish(n.ID, nodes, digest)
	return nil
}

// attachBoundaryLocked makes every existing dependent of generatorID wait on
// the fragment's boundary as well.
//
// Without it a dependent would run the moment the generator finished, which
// is when the generated work STARTS rather than when it is done.
//
// The dependent is COPIED rather than edited in place: live holds pointers
// into the caller's plan, and a run must not leave its Needs rewritten in a
// Plan value the caller may run again. Swapping the pointer is safe precisely
// here, because a dependent of an unsettled generator cannot be running, so
// nothing else holds the old one.
//
// Caller holds rc.oc.mu.
func (rc *runCore) attachBoundaryLocked(generatorID string, boundary []string) {
	if len(boundary) == 0 {
		return
	}
	for i, node := range *rc.live {
		if !needs(node, generatorID) {
			continue
		}
		clone := *node
		clone.Needs = append([]string(nil), node.Needs...)
		for _, b := range boundary {
			if !needs(&clone, b) {
				clone.Needs = append(clone.Needs, b)
			}
		}
		(*rc.live)[i] = &clone
		rc.byID[clone.ID] = &clone
	}
}

func needs(n *plan.Node, id string) bool {
	for _, need := range n.Needs {
		if need == id {
			return true
		}
	}
	return false
}

// fragmentFor produces n's fragment, from whichever form it declared.
//
// Both forms end at the same *plan.Fragment and the same SpliceFragment, so
// a fragment written by a shell script and one built in Go cannot diverge in
// what the engine will accept.
func (rc *runCore) fragmentFor(ctx context.Context, n *plan.Node, opts Options) (*plan.Fragment, error) {
	if n.Generate.Path == "" {
		var fn GenerateFunc
		if opts.Generators != nil {
			fn = opts.Generators(n.ID)
		}
		if fn == nil {
			return nil, fmt.Errorf(
				"engine: step %q declares a Go generator, but no function was supplied for it; "+
					"senro.Generate carries it from the pipeline to the run", n.ID)
		}
		// The root a Go generator reads its step's output from, the same
		// directory the JSON form resolves its path against. Empty when the
		// step mounts no workspace, which senro's GenCtx reports by name
		// rather than reading from a directory that means nothing.
		var root string
		if rc.ws != nil {
			root = rc.ws.inputRoot(n)
		}
		f, err := fn(ctx, n, root)
		if err != nil {
			return nil, fmt.Errorf("engine: step %q: generator: %w", n.ID, err)
		}
		return f, nil
	}

	// The declared path resolves against the same root a step's Outputs do,
	// which is the directory senro can actually read what a step produced
	// from. A step with no workspace cannot produce files senro reads, and
	// Validate already refuses Outputs there for the same reason.
	if rc.ws == nil {
		return nil, fmt.Errorf(
			"engine: step %q reads its fragment from %q, but the step mounts no workspace, "+
				"so senro has nowhere to read what it produced", n.ID, n.Generate.Path)
	}
	path := filepath.Join(rc.ws.inputRoot(n), n.Generate.Path)
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("engine: step %q: reading its fragment: %w", n.ID, err)
	}
	f, err := plan.ParseFragment(b)
	if err != nil {
		return nil, fmt.Errorf("engine: step %q: %w", n.ID, err)
	}
	return f, nil
}

// withinLimitsLocked refuses a splice that would nest too deep or spend more
// of the run's node budget than is left.
//
// Both errors name the generator CHAIN rather than just the step: with
// generators producing generators, "which one did this" is the actionable
// part and the last link alone does not answer it.
//
// Caller holds rc.oc.mu.
func (rc *runCore) withinLimitsLocked(generatorID string, adding int, opts Options) error {
	maxDepth := opts.MaxDepth
	if maxDepth <= 0 {
		maxDepth = DefaultMaxDepth
	}
	if depth := rc.genDepth[generatorID] + 1; depth > maxDepth {
		return fmt.Errorf(
			"engine: generator chain %s would nest %d deep, past MaxDepth(%d); "+
				"a generator that generates generators is bounded deliberately",
			rc.generatorChainLocked(generatorID), depth, maxDepth)
	}
	maxNodes := opts.MaxNodes
	if maxNodes <= 0 {
		maxNodes = DefaultMaxNodes
	}
	if total := len(*rc.live) + adding; total > maxNodes {
		return fmt.Errorf(
			"engine: generator chain %s would take this run to %d nodes, past MaxNodes(%d)",
			rc.generatorChainLocked(generatorID), total, maxNodes)
	}
	return nil
}

// generatorChainLocked renders the generators that led to id, outermost
// first. Caller holds rc.oc.mu.
func (rc *runCore) generatorChainLocked(id string) string {
	var chain []string
	for cur := id; cur != ""; cur = rc.genParent[cur] {
		chain = append([]string{cur}, chain...)
	}
	return strings.Join(chain, " -> ")
}

// graphExcludingOwnLocked is the graph a fragment is checked for collisions
// against: everything except what THIS generator produced before.
//
// Its own previous children are not collisions. rerun_from re-runs a
// generator with its whole dependents closure unsettled, which includes the
// nodes it generated, so producing them again is a replay of work that is
// already there. Every other id in the run is still a collision, because two
// nodes answering to one name would make every event, log and cache entry
// keyed by it ambiguous.
//
// Caller holds rc.oc.mu.
func (rc *runCore) graphExcludingOwnLocked(generatorID string) map[string]*plan.Node {
	out := make(map[string]*plan.Node, len(rc.byID))
	for id, node := range rc.byID {
		if rc.genParent[id] == generatorID {
			continue
		}
		out[id] = node
	}
	return out
}

// newCountLocked is how many of these nodes the run does not already hold,
// which is what the node budget should be charged for: a replay adds nothing.
//
// Caller holds rc.oc.mu.
func (rc *runCore) newCountLocked(nodes []plan.Node) int {
	n := 0
	for i := range nodes {
		if _, exists := rc.byID[nodes[i].ID]; !exists {
			n++
		}
	}
	return n
}
