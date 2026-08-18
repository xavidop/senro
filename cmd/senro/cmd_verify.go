package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"

	"github.com/xavidop/senro/internal/cache"
	"github.com/xavidop/senro/internal/plan"
	"github.com/xavidop/senro/internal/storage"
	"github.com/xavidop/senro/internal/verify"
)

// verifyUsage documents the one check this command performs today. A check
// SELECTOR (--recheck-pure) rather than a bare `senro verify`, so adding a
// second check never changes what the first one's invocation means.
//
// The shapes below are an interface: flags and exit codes are documented
// here, in site/src/pages/docs/cli/cache.md and in
// skills/senro/references/cli.md, and changing either is a breaking change.
const verifyUsage = `Usage:
  senro verify --recheck-pure [--run RUN] [--rerun] [--step STEP] [--limit N]
      [--json] [--no-classify] [--keep] [--fail-on-mismatch]
      [--cache-dir DIR] [--local-class CLASS]

      Re-execute a run's cached Pure() steps and compare what they produce
      against what the action cache recorded, so a step that claims purity and
      then reaches the network is caught by its own digests.

      Every re-run happens in a throwaway tree restored from the workspace
      content the step's OWN cache key records, never in the run's workspaces
      and never in your checkout, and nothing here ever writes a cache entry.

      NOTHING IS EXECUTED WITHOUT --rerun. A step marked Pure() is supposed to
      be safe to re-run; the premise of this command is that the claim may be
      false, so it does not help itself to the claim's safety corollary
      either. Without --rerun it reports what it WOULD run and stops.

  Flags:
    --run RUN            the run to verify (default: the most recent one here)
    --rerun              actually execute. Without it, nothing runs.
    --step STEP          verify one step rather than every cached Pure() step
    --limit N            check at most N steps, in plan order (0 = no limit)
    --no-classify        skip the second re-run that tells a non-deterministic
                         step apart from one that depends on something outside
                         its key. Halves the work and merges the two verdicts.
    --keep               keep the re-run trees instead of removing them
    --json               emit the report as JSON instead of text
    --fail-on-mismatch   exit 1 when any step failed to reproduce its entry
    --cache-dir DIR      the storage root (default: $SENRO_CACHE_DIR)
    --local-class CLASS  mirror senro.WithLocalClass for this pipeline

  Exit codes: 0 whether or not it finds anything, like senro ws diff, because
  a finding is an answer and not a failed run. 1 only with --fail-on-mismatch,
  or if the pass itself broke. 2 for a usage error.
`

// cmdVerify implements `senro verify`.
func cmdVerify(args []string, stdout, stderr io.Writer) int {
	var (
		run, step, cacheDir, localClass string
		recheckPure, rerun, asJSON      bool
		noClassify, keep, failOnFinding bool
		limit                           int
	)
	for i := 0; i < len(args); i++ {
		switch a := args[i]; {
		case a == "--recheck-pure":
			recheckPure = true
		case a == "--rerun":
			rerun = true
		case a == "--no-classify":
			noClassify = true
		case a == "--keep":
			keep = true
		case a == "--json":
			asJSON = true
		case a == "--fail-on-mismatch":
			failOnFinding = true
		case a == "--run" && i+1 < len(args):
			run, i = args[i+1], i+1
		case a == "--step" && i+1 < len(args):
			step, i = args[i+1], i+1
		case a == "--cache-dir" && i+1 < len(args):
			cacheDir, i = args[i+1], i+1
		case a == "--local-class" && i+1 < len(args):
			localClass, i = args[i+1], i+1
		case a == "--limit" && i+1 < len(args):
			n, err := strconv.Atoi(args[i+1])
			if err != nil || n < 0 {
				_, _ = fmt.Fprintf(stderr, "senro verify: --limit wants a non-negative count, got %q\n",
					args[i+1])
				return exitUsage
			}
			limit, i = n, i+1
		default:
			_, _ = fmt.Fprintf(stderr, "senro verify: unknown argument %q\n\n%s", a, verifyUsage)
			return exitUsage
		}
	}

	// The check has to be named: a default would mean something different
	// the day a second check exists.
	if !recheckPure {
		_, _ = fmt.Fprintf(stderr,
			"senro verify: name a check; this build has one, --recheck-pure\n\n%s", verifyUsage)
		return exitUsage
	}

	dir, err := resolveRunDir(run)
	if err != nil {
		_, _ = fmt.Fprintln(stderr, "senro verify:", err)
		return exitUsage
	}

	p, err := readRunPlan(dir)
	if err != nil {
		_, _ = fmt.Fprintln(stderr, "senro verify:", err)
		return exitUsage
	}
	recs, err := cache.ReadRecords(filepath.Join(dir, "cache"))
	if err != nil {
		_, _ = fmt.Fprintln(stderr, "senro verify:", err)
		return exitUsage
	}

	root, err := resolveCacheDir(cacheDir)
	if err != nil {
		_, _ = fmt.Fprintln(stderr, "senro verify:", err)
		return exitUsage
	}
	store, err := storage.Open(root)
	if err != nil {
		_, _ = fmt.Fprintln(stderr, "senro verify:", err)
		return exitUsage
	}
	defer func() { _ = store.Close() }()

	// A fresh temp directory rather than somewhere under the run: a run
	// directory is the run's own record, and a verification pass happened
	// later. Removed on the way out unless --keep.
	workRoot, err := os.MkdirTemp("", "senro-verify-")
	if err != nil {
		_, _ = fmt.Fprintln(stderr, "senro verify:", err)
		return exitRunFailed
	}
	if keep {
		_, _ = fmt.Fprintf(stderr, "senro verify: re-run trees are being kept under %s\n", workRoot)
	} else {
		defer func() { _ = os.RemoveAll(workRoot) }()
	}

	var steps []string
	if step != "" {
		steps = []string{step}
	}
	rep, err := verify.Check(context.Background(), verify.Options{
		Plan: p, Records: recs, Storage: store,
		WorkRoot: workRoot, LocalClass: localClass,
		Steps: steps, Limit: limit,
		Execute: rerun, Classify: !noClassify,
	})
	if err != nil {
		_, _ = fmt.Fprintln(stderr, "senro verify:", err)
		return exitRunFailed
	}
	if step != "" && len(rep.Steps) == 0 {
		_, _ = fmt.Fprintf(stderr,
			"senro verify: no cache record for step %q in %s (only Pure() steps have one, and only once "+
				"they are actually attempted)\n", step, dir)
		return exitUsage
	}
	// The re-run trees are about to be removed, so the report must not name
	// paths that will not be there when it is read.
	if !keep {
		for i := range rep.Steps {
			rep.Steps[i].WorkDir = ""
		}
	}
	verify.SortSteps(&rep)

	if asJSON {
		if err := verify.FormatJSON(stdout, rep); err != nil {
			_, _ = fmt.Fprintln(stderr, "senro verify:", err)
			return exitRunFailed
		}
	} else if err := verify.Format(stdout, rep, relToCwd(dir)); err != nil {
		_, _ = fmt.Fprintln(stderr, "senro verify:", err)
		return exitRunFailed
	}

	// Exit 0 whether or not there are findings, as `senro ws diff` does:
	// exit 1 means "the run failed" throughout this CLI, and a verification
	// that found something answered rather than failed. --fail-on-mismatch
	// is the opt-in that turns the answer into a gate.
	//
	// A step that could not be checked does not change the exit code
	// either, unlike `ws diff`'s exit 2 for a partial answer: those skips
	// are the ordinary shape of a pass over a real pipeline, and a code
	// that was 2 almost every time would tell a script nothing.
	if failOnFinding && rep.Findings() > 0 {
		return exitRunFailed
	}
	return exitSuccess
}

// readRunPlan loads the plan the run actually executed, from
// <run>/plan.json rather than by rebuilding the pipeline package:
// rebuilding would re-resolve a pipeline that may have been edited since,
// which is a different plan and therefore a different step. It also means
// this command needs no Go toolchain and no pipeline source.
func readRunPlan(dir string) (*plan.Plan, error) {
	b, err := os.ReadFile(filepath.Join(dir, "plan.json"))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf(
				"%s has no plan.json, so there is no record of what its steps were meant to run; "+
					"a run directory written by a senro older than plan.json looks like this", dir)
		}
		return nil, err
	}
	p, err := plan.Unmarshal(b)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", filepath.Join(dir, "plan.json"), err)
	}
	return p, nil
}
