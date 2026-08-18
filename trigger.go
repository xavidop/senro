package senro

import (
	"github.com/xavidop/senro/trigger"
)

// WithTrigger makes the pipeline decide for itself whether ev is its
// business: the event this process was handed, and the triggers this
// pipeline declares.
//
//	ev, err := trigger.LoadEvent(*eventPath)
//	if err != nil {
//		return err
//	}
//	err = senro.Run(ctx, pipeline, senro.WithTrigger(ev,
//		trigger.OnPush(trigger.Branches("main")),
//		trigger.OnPullRequest(trigger.Actions("opened", "synchronize")),
//	))
//	if errors.Is(err, trigger.ErrNoMatch) {
//		os.Exit(78)
//	}
//
// Three outcomes. A match runs the pipeline, and Run's error means what it
// always meant. No match is trigger.ErrNoMatch, wrapped: Run starts no run,
// creates no directory, emits no event, and returns before touching disk.
// Anything else wrong with the wiring is an ordinary error, so "not my
// business" and "somebody wired this wrong" never look alike.
//
// Run never exits. Exit 78 (EX_CONFIG) is the convention for a no-match, and
// mapping the sentinel to it is main's decision: a library that calls
// os.Exit has taken it for every host that embeds it.
//
// A match carries the trigger's Params, laid over the event's own, into the
// run's senro.Params; the event's branch becomes the "branch" param
// senro.Branch reads, so a trigger-driven run needs nothing extra for a
// branch condition. WithParams still wins over both. The mode and the
// affected-set base are recorded in run.json (see RunManifest) and on the
// trigger.Match; they are deliberately NOT injected as parameters, since
// senro computes no affected set and should not claim parameter names on
// that work's behalf.
//
// A Run with no WithTrigger gates nothing and costs nothing; so does
// WithTrigger with a nil event, which is what makes the local loop work:
// `./pipeline` with no --trigger-event runs everything, and a dispatcher
// that forgets the flag over-runs visibly rather than silently never
// running.
func WithTrigger(ev *trigger.Event, ts ...trigger.Trigger) Option {
	return func(c *runConfig) {
		c.triggerEvent = ev
		c.triggers = append(c.triggers, ts...)
	}
}

// runParams folds a match into the run's parameters. Three layers, narrowest
// last: the event's branch, then the trigger's own Params, then whatever
// WithParams said.
//
// "branch" and nothing else is derived: senro.Branch already reads it. Any
// other name a trigger could claim ("mode", "tag", "ref") would be senro
// inventing an interface for the affected-set computation on its behalf.
func runParams(m *trigger.Match, explicit Params) Params {
	if m == nil {
		return explicit
	}
	out := make(Params, len(m.Params)+len(explicit)+1)
	if m.Event.Branch != "" {
		out["branch"] = m.Event.Branch
	}
	for k, v := range m.Params {
		out[k] = v
	}
	for k, v := range explicit {
		out[k] = v
	}
	return out
}
