package engine

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/internal/plan"
	"github.com/xavidop/senro/internal/sink"
)

// runSubgraph executes a fragment as a NESTED run, on behalf of the func step
// that asked for it (design §2.9, senro.RunSubgraph).
//
// This is the imperative escape hatch, and it is deliberately less capable
// than a generator. A generator describes work and hands it to the scheduler,
// which is why its nodes are ordinary steps with their own cache entries and
// can be retried one at a time. A subgraph is work the FUNCTION is doing: it
// belongs to that step, it lives and dies with it, and re-running means
// re-running the step. That is the trade for being able to express "one at a
// time until quorum, then stop", which is not a DAG at all.
//
// The nested run inherits the executors, storage and secrets of the run that
// contains it, and gets its own event range parented to the step.
func (rc *runCore) runSubgraph(ctx context.Context, parent *plan.Node, opts Options, b []byte) error {
	f, err := plan.ParseFragment(b)
	if err != nil {
		return fmt.Errorf("engine: step %q: subgraph: %w", parent.ID, err)
	}
	p, err := plan.SubgraphPlan(f, parent.ID)
	if err != nil {
		return fmt.Errorf("engine: step %q: %w", parent.ID, err)
	}
	if len(p.Nodes) == 0 {
		// Nothing to do is not an error, exactly as an empty fragment is not
		// one for a generator.
		return nil
	}

	seq := rc.subgraphSeq.Add(1)
	nested := opts
	// Its own run id, built from the step's: an event range a reader can
	// attribute without guessing, and one that cannot collide with the run
	// that contains it.
	nested.RunID = fmt.Sprintf("%s/%s#%d", opts.RunID, parent.ID, seq)
	nested.Dir = filepath.Join(opts.Dir, "subgraph", fmt.Sprintf("%s-%d", dirSafe(parent.ID), seq))
	nested.Sink = &subgraphSink{inner: opts.Sink, group: parent.ID}
	// A nested run serves no control requests: subgraphSink.Control returns
	// nil. run.cancel on a subgraph would mean cancelling half of what a
	// function is in the middle of doing, and the honest granularity is the
	// step: cancel that, the function's context ends, and this ends with it.

	status, err := Run(ctx, p, nested)
	if err != nil {
		return fmt.Errorf("engine: step %q: subgraph: %w", parent.ID, err)
	}
	if status != api.RunSucceeded {
		return fmt.Errorf("engine: step %q: subgraph finished %s", parent.ID, status)
	}
	return nil
}

// dirSafe turns a step id into one path segment. Ids are hierarchical and
// contain "/", which would otherwise make a subgraph's directory a tree
// shaped like the pipeline rather than one directory per invocation.
func dirSafe(id string) string {
	return strings.ReplaceAll(strings.ReplaceAll(id, "/", "_"), string(filepath.Separator), "_")
}

// subgraphSink puts a nested run's events into the parent's stream, under the
// step that ran it.
//
// run.started and run.finished are dropped: the ledger describes ONE run, and
// a second pair of lifecycle events would make every reader that folds the
// stream believe a new run began. The step's own step.started and
// step.finished already bracket the subgraph, which is the honest framing,
// because the subgraph is that step's work.
type subgraphSink struct {
	inner sink.Sink
	group string
}

func (s *subgraphSink) Emit(e api.Event) {
	switch e.Type {
	case api.RunStarted, api.RunFinished:
		return
	}
	if e.Group == "" {
		e.Group = s.group
	}
	s.inner.Emit(e)
}

// Control returns nil: nothing may steer a nested run. See runSubgraph.
func (s *subgraphSink) Control() <-chan sink.ControlRequest { return nil }
