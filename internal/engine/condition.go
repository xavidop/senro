package engine

import (
	"fmt"
	"os"

	"github.com/xavidop/senro/internal/cond"
	"github.com/xavidop/senro/internal/plan"
)

// conditionScope is the run's evaluation scope, built once in Run: the
// parameters the caller passed and the coordinator's own environment.
func conditionScope(opts Options) cond.Scope {
	return cond.Scope{Params: opts.Params, Env: os.Getenv}
}

// checkConditions refuses, before any step runs, a plan carrying a condition
// nothing can parse.
//
// Fail fast, and fail CLOSED. The alternative to refusing is treating an
// unknown condition as true, which would run a deploy that was gated on the
// main branch because a newer engine wrote a condition this one does not
// understand. Refusing names the step and the condition once, at second zero.
func checkConditions(p *plan.Plan) error {
	for i := range p.Nodes {
		for _, s := range p.Nodes[i].When {
			if _, err := cond.Parse(s); err != nil {
				return fmt.Errorf("engine: step %q: %w", p.Nodes[i].ID, err)
			}
		}
	}
	return nil
}

// pruned reports whether n's conditions gate it out of this run, and why.
//
// Evaluated when the node becomes READY rather than at run start, which costs
// nothing (the scope is immutable for the run) and keeps one property worth
// having: a node is only ever pruned after its dependencies have settled, so
// the reason a node did not run reads in the same order a person reads the
// graph.
func (rc *runCore) pruned(n *plan.Node) (bool, string) {
	if len(n.When) == 0 {
		return false, ""
	}
	run, because, err := cond.EvalAll(n.When, rc.scope)
	if err != nil {
		// checkConditions already refused this at run start, so reaching here
		// means a *plan.Plan assembled by hand. Failing closed is the same
		// answer for the same reason.
		return true, err.Error()
	}
	return !run, because
}
