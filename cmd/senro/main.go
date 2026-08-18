// Command senro is the CLI companion to the senro pipeline engine: it builds
// and runs an embedded pipeline (senro run), attaches to one already running
// or finished (senro attach), opens a session on a live step (senro shell),
// reads what a run left behind (senro cache, senro ws, senro logs, senro
// verify), and serves a browser view of a live run (senro ui).
package main

import (
	"fmt"
	"io"
	"os"
)

const usage = `senro is the CLI companion to the senro pipeline engine.

Usage:
  senro run <pkg> [--ui=auto|tui|plain|none] [--trigger-event PATH]
      [-- pipeline-args...]
      Build the pipeline package and run it, attaching and rendering.
      --trigger-event is forwarded to the pipeline binary, which decides for
      itself whether the event is its business. PATH may be "-" for stdin.

  senro attach [--pid <pid> | --run <id> | --addr <host:port>] [--follow]
      [--tls] [--ui=auto|tui|plain|none]
      Attach to a live run (bare, or by pid), or a finished one (--run).
      --follow tails a run from disk only, no socket needed. --addr dials a
      TCP attach server directly, taking its token from $SENRO_ATTACH_TOKEN;
      --tls says that endpoint speaks TLS.

  senro ui [--pid <pid> | --run <id> | --addr <host:port>] [--tls] [--port N]
      Serve a browser view of a LIVE run on loopback, and print a one-time
      link. The page is a Go client compiled to WebAssembly that folds the
      run's events with the same api.RunState.Apply the terminal UI uses, so
      the two cannot disagree about what a stream means. The run's bearer
      token stays in this process and never reaches the page. It offers all
      ten control operations the TUI does, and deliberately not senro shell.
      A run that has already finished has no attach server; read one with
      senro attach --follow.

  senro cache gc [--max-size 50G] [--keep-failed 168h] [--dry-run] [--cache-dir DIR]
      Reclaim disk. Least recently used entries go first; the workspaces of a
      failed run are kept for --keep-failed so the snapshot you are debugging
      is still there.

  senro cache explain [--run RUN] [STEP]
      Explain why a Pure() step hit or missed the action cache: every key
      component that changed, both sides, and what stayed the same.

  senro verify --recheck-pure [--run RUN] [--rerun] [--step STEP] [--limit N]
      Re-execute a run's cached Pure() steps and compare what they produce
      against what the action cache recorded, so a step that claims purity
      and then reaches the network is caught by its own digests. Every re-run
      happens in a throwaway tree restored from the workspace content the
      step's own cache key records, and no cache entry is ever written.
      NOTHING RUNS WITHOUT --rerun: the premise is that a Pure() claim may be
      false, so its safety corollary is not assumed either.

  senro ws ls [RUN] [NAME]
      List a run's workspaces with their digests and sizes. With a workspace
      name, list its files from the stored index, without downloading the
      body.

  senro ws pull [--force] RUN NAME [DEST]
      Write a workspace's stored body out to DEST (default ./NAME), so the
      files a failed step left behind can be read. DEST is replaced, not
      merged into, and an existing non-empty DEST is refused without --force.

  senro ws diff [--json] RUN-A RUN-B [NAME]
      Compare two runs' workspaces from their stored indexes alone, no body
      downloaded: what was added, removed, rewritten, chmod'd, or replaced by
      a different kind of thing. Exits 0 whether or not they differ.

  senro shell [--pid <pid> | --run <id> | --addr <host:port>] [--tls] [--tty]
      --step ID [-- cmd...]
      Open an interactive session on a LIVE run's step: the step's own
      workspaces, read-only, at the paths the step saw them, on the step's
      own executor. Pair it with a breakpoint to stop the run before a step
      and look at what it was about to run against. No secrets are delivered
      into a session. Without --tty there is no terminal: no prompt, no line
      editing, no job control. --tty runs the session on a real terminal;
      local and container host one, ssh does not. A finished run has no
      engine to host a session; use senro ws pull.

  senro logs fetch [--force] RUN [DEST]
      Fetch a run archived in the shared cache, a bucket or an OCI registry,
      back into a local run directory (default ./runs/RUN), so a run whose
      machine no longer exists can be read with senro attach. Configured
      from the same SENRO_REMOTE_CACHE environment the run that archived it
      used. DEST is replaced, not merged into, and an existing non-empty
      DEST is refused without --force.

  senro func check [--dir DIR] [packages...]
      Report cgo in a Func step's dependency graph, with the import chain
      that pulled it in. A Func step on an ssh host, in a container or in a
      pod of another platform is cross-compiled with CGO_ENABLED=0, and cgo is
      what breaks that. Steps on the coordinator, or on a target of its own
      platform, are unaffected.

Exit codes: 0 success, 1 run failed (or, for func check, offenders found, and
for verify --fail-on-mismatch, a step that did not reproduce its cached
result), 2 usage error, 78 no trigger matched the event (nothing to run),
130 cancelled. senro ws diff and senro verify both exit 0 whether or not they
find anything: a finding is an answer, not a failed run.
`

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

// run is main's entire logic, minus the os.Exit boundary. Every other
// function here is a plain function of its arguments (plus an injected
// isTTY), which is what lets the tests exercise every exit code and failure
// message without spawning a subprocess.
func run(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		// A stderr write failure has no further diagnostic channel and does
		// not change the exit code, so it is deliberately discarded.
		_, _ = fmt.Fprint(stderr, usage)
		return exitUsage
	}

	switch args[0] {
	case "run":
		return cmdRun(args[1:], stdout, stderr, stdoutIsTTY())
	case "attach":
		return cmdAttach(args[1:], stdout, stderr, stdoutIsTTY())
	case "ui":
		return cmdUI(args[1:], stdout, stderr)
	case "cache":
		return cmdCache(args[1:], stdout, stderr)
	case "verify":
		return cmdVerify(args[1:], stdout, stderr)
	case "ws":
		return cmdWS(args[1:], stdout, stderr)
	case "shell":
		return cmdShellMain(args[1:], stdout, stderr)
	case "logs":
		return cmdLogs(args[1:], stdout, stderr)
	case "func":
		return cmdFunc(args[1:], stdout, stderr)
	case "-h", "--help", "help":
		_, _ = fmt.Fprint(stdout, usage)
		return exitSuccess
	default:
		_, _ = fmt.Fprintf(stderr, "senro: unknown command %q\n\n%s", args[0], usage)
		return exitUsage
	}
}
