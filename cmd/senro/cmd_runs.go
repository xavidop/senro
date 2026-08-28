package main

import (
	"context"
	"fmt"
	"io"
	"path/filepath"
	"strconv"
	"text/tabwriter"
	"time"

	"github.com/xavidop/senro/internal/source"
)

// defaultRunsLimit caps `senro runs` with no -n: enough to answer "what ran
// today" without flooding a terminal, the same reasoning ws ls's largeWorkspaceBytes
// applies to a different flood.
const defaultRunsLimit = 20

// runsUsage documents `senro runs`, kept beside the other per-command usage
// consts (wsUsage, logsUsage) rather than folded only into the top-level
// usage string.
const runsUsage = `Usage:
  senro runs [-n LIMIT]
      List runs recorded under ./runs, newest first: run ID, pipeline,
      status, when it started, and how long it took (or "running" for one
      still in progress). Answers "what ran" without already knowing a run
      ID, so its own ID can be handed to senro attach --run, senro ws ls, or
      senro cache explain. -n caps how many are printed (default 20).
`

// cmdRuns implements `senro runs`.
//
// Reuses runCandidates, the same newest-first listing resolveRunDir's
// no-argument default already builds, so this command and "the run senro
// attach picks with no --run" can never disagree about what "newest" means.
// Each run's status and timing come from source.OpenFile+State, the same
// events.jsonl fold every other view of a run uses (see api.RunState).
func cmdRuns(args []string, stdout, stderr io.Writer) int {
	limit := defaultRunsLimit
	for i := 0; i < len(args); i++ {
		switch a := args[i]; {
		case (a == "-n" || a == "--limit") && i+1 < len(args):
			n, err := strconv.Atoi(args[i+1])
			if err != nil || n < 1 {
				_, _ = fmt.Fprintf(stderr, "senro runs: -n wants a positive integer, got %q\n\n%s", args[i+1], runsUsage)
				return exitUsage
			}
			limit = n
			i++
		default:
			_, _ = fmt.Fprintf(stderr, "senro runs: unexpected argument %q\n\n%s", a, runsUsage)
			return exitUsage
		}
	}

	found, err := runCandidates()
	if err != nil {
		_, _ = fmt.Fprintln(stderr, "senro runs:", err)
		return exitUsage
	}
	if len(found) == 0 {
		_, _ = fmt.Fprintln(stdout, "no runs under ./runs")
		return exitSuccess
	}
	if len(found) > limit {
		found = found[:limit]
	}

	tw := tabwriter.NewWriter(stdout, 0, 4, 2, ' ', 0)
	_, _ = fmt.Fprintln(tw, "RUN ID\tPIPELINE\tSTATUS\tSTARTED\tDURATION")
	ctx := context.Background()
	for _, c := range found {
		dir := filepath.Join("runs", c.name)
		_, _ = fmt.Fprintln(tw, runsLine(ctx, c.name, dir))
	}
	if err := tw.Flush(); err != nil {
		return exitRunFailed
	}
	return exitSuccess
}

// runsLine renders one run's row. A run whose events.jsonl cannot be opened
// or folded still gets a line, with the failure in the status column: a
// half-written or corrupt run directory must not make the rest of the
// listing disappear behind an error for the whole command.
func runsLine(ctx context.Context, id, dir string) string {
	src, err := source.OpenFile(dir, false)
	if err != nil {
		return fmt.Sprintf("%s\t\t(%v)\t\t", id, err)
	}
	defer func() { _ = src.Close() }()

	st, err := src.State(ctx)
	if err != nil {
		return fmt.Sprintf("%s\t\t(%v)\t\t", id, err)
	}

	status := string(st.Run.Status)
	if !st.Run.Done {
		status = "running"
	}
	dur := "-"
	if !st.Run.Started.IsZero() {
		end := st.Run.Finished
		if end.IsZero() {
			end = time.Now()
		}
		dur = end.Sub(st.Run.Started).Round(time.Second).String()
	}
	started := "-"
	if !st.Run.Started.IsZero() {
		started = st.Run.Started.Local().Format("2006-01-02 15:04:05")
	}
	return fmt.Sprintf("%s\t%s\t%s\t%s\t%s", id, st.Run.Pipeline, status, started, dur)
}
