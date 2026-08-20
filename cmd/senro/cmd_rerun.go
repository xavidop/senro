package main

import (
	"context"
	"fmt"
	"io"
	"sort"

	"github.com/xavidop/senro"
	"github.com/xavidop/senro/internal/plan"
)

// rerunUsage documents the two verbs that are deliberately separate.
//
// The shapes below are an interface: flags and exit codes are documented
// here, in site/src/pages/docs/cli/rerun.md and in
// skills/senro/references/cli.md, and changing either is a breaking change.
const rerunUsage = `Usage:
  senro rerun [--run RUN] [--step STEP] [--regenerate] [--cache-dir DIR]
      [--dir DIR] [--local-class CLASS]

      Re-execute the plan a previous run recorded, from <run>/plan.json rather
      than by rebuilding the pipeline package: rebuilding would re-resolve a
      pipeline that may have been edited since, which is a different plan and
      therefore a different run.

      Steps whose inputs have not changed are served from the action cache, so
      an unchanged re-run is fast and a generator REPLAYS the subgraph it
      recorded rather than being asked again. That is what makes a re-run
      reproduce a run instead of re-discovering one.

  Flags:
    --run RUN            the run to re-execute (default: the most recent one)
    --step STEP          re-execute this step, WHAT IT NEEDS, and everything
                         below it; branches unrelated to it are skipped. Its
                         dependencies are included because a step cannot run
                         without its inputs, and the action cache is what makes
                         them cheap: unchanged ones are served rather than
                         re-executed. All of it is read from the recorded plan,
                         so "below it" means what it meant in the original run.
    --regenerate         ask generators for a FRESH subgraph instead of
                         replaying the recorded one. Named separately from
                         --step on purpose: silently re-deriving a graph during
                         what looked like a retry is a confusing failure, so a
                         run that may do different work says so out loud.
    --dir DIR            where to write this run (default: a new run directory)
    --cache-dir DIR      the storage root (default: $SENRO_CACHE_DIR)
    --local-class CLASS  mirror senro.WithLocalClass for this pipeline

  Exit codes: 0 if the re-run succeeded, 1 if it failed, 2 for a usage error.

  A Go generator cannot be re-invoked from a recorded plan: its closure lives
  in the pipeline package, not in plan.json. --regenerate therefore works for
  a generator declared with GenerateFromJSON, and reports the Go form by name
  rather than silently replaying it.
`

func cmdRerun(args []string, stdout, stderr io.Writer) int {
	var run, step, cacheDir, dir, localClass string
	var regenerate bool
	for i := 0; i < len(args); i++ {
		switch a := args[i]; {
		case a == "--regenerate":
			regenerate = true
		case a == "--run" && i+1 < len(args):
			run, i = args[i+1], i+1
		case a == "--step" && i+1 < len(args):
			step, i = args[i+1], i+1
		case a == "--dir" && i+1 < len(args):
			dir, i = args[i+1], i+1
		case a == "--cache-dir" && i+1 < len(args):
			cacheDir, i = args[i+1], i+1
		case a == "--local-class" && i+1 < len(args):
			localClass, i = args[i+1], i+1
		case a == "-h" || a == "--help":
			_, _ = fmt.Fprint(stdout, rerunUsage)
			return exitSuccess
		default:
			_, _ = fmt.Fprintf(stderr, "senro rerun: unexpected argument %q\n\n%s", a, rerunUsage)
			return exitUsage
		}
	}

	runDir, err := resolveRunDir(run)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "senro rerun: %v\n", err)
		return exitUsage
	}
	p, err := readRunPlan(runDir)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "senro rerun: %v\n", err)
		return exitUsage
	}

	opts := []senro.Option{}
	if dir != "" {
		opts = append(opts, senro.WithDir(dir))
	}
	if cacheDir != "" {
		opts = append(opts, senro.WithCacheDir(cacheDir))
	}
	if localClass != "" {
		opts = append(opts, senro.WithLocalClass(localClass))
	}
	if regenerate {
		opts = append(opts, senro.WithRegenerate())
	}
	if step != "" {
		sel, err := selectFrom(p, step)
		if err != nil {
			_, _ = fmt.Fprintf(stderr, "senro rerun: %v\n", err)
			return exitUsage
		}
		opts = append(opts, senro.WithOnlySteps(sel...))
	}

	if err := senro.RunPlan(context.Background(), p, opts...); err != nil {
		_, _ = fmt.Fprintf(stderr, "senro rerun: %v\n", err)
		return exitRunFailed
	}
	return exitSuccess
}

// selectFrom is step, everything it transitively needs, and everything that
// transitively depends on it, taken from the RECORDED plan.
//
// The ancestors are in the set because a step cannot run without its inputs.
// Selecting only the step and its dependents settles its dependencies as
// skipped, and a dependent of a skipped step is itself skipped, so the one
// step the operator asked for is the one thing that would not run. Including
// them costs little: the action cache serves the unchanged ones.
//
// From the recorded plan and not a rebuilt one, for the reason readRunPlan
// exists: "below it" has to mean what it meant in the run being repeated, not
// what it would mean in a pipeline someone has edited since.
func selectFrom(p *plan.Plan, step string) ([]string, error) {
	var found bool
	for i := range p.Nodes {
		if p.Nodes[i].ID == step {
			found = true
			break
		}
	}
	if !found {
		return nil, fmt.Errorf("the recorded plan has no step %q", step)
	}

	in := map[string]bool{step: true}
	// Repeated passes rather than one, in both directions: a node's
	// dependents can appear before it in the plan, so a single pass can miss
	// a chain.
	for grew := true; grew; {
		grew = false
		for i := range p.Nodes {
			n := &p.Nodes[i]
			if in[n.ID] {
				// Everything it needs comes with it.
				for _, need := range n.Needs {
					if !in[need] {
						in[need] = true
						grew = true
					}
				}
				continue
			}
			for _, need := range n.Needs {
				if in[need] {
					in[n.ID] = true
					grew = true
					break
				}
			}
		}
	}

	out := make([]string, 0, len(in))
	for id := range in {
		out = append(out, id)
	}
	sort.Strings(out)
	return out, nil
}
