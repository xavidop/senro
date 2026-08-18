package main

import (
	"context"
	"flag"
	"fmt"
	"io"

	"github.com/xavidop/senro/internal/attachsrv"
	"github.com/xavidop/senro/internal/source"
)

// cmdAttach implements
// `senro attach [--pid|--run|--follow|--addr] [--tls] [--ui=...]`.
// isTTY is injected rather than read from os.Stdout directly, so every
// branch here (including the tui-on-non-tty refusal) is exercisable
// without a real terminal.
func cmdAttach(args []string, stdout, stderr io.Writer, isTTY bool) int {
	fs := flag.NewFlagSet("senro attach", flag.ContinueOnError)
	fs.SetOutput(stderr)
	pid := fs.Int("pid", 0, "attach to the live run with this pid")
	runID := fs.String("run", "", "attach to (live) or tail (--follow) the run with this ID")
	follow := fs.Bool("follow", false, "tail a finished-or-running run from disk only, no socket needed (requires --run)")
	// --addr is for a run this machine has no registry entry for, which in
	// practice means one reached through a port-forward. There is
	// deliberately no --token beside it; see endpoint.go.
	addr := fs.String("addr", "", "attach to a TCP attach server at this host:port, with its token from $"+source.TokenEnv)
	useTLS := fs.Bool("tls", false, "the --addr endpoint speaks TLS (verified against the system roots)")
	ui := fs.String("ui", string(uiAuto), "auto|tui|plain|none")
	if err := fs.Parse(args); err != nil {
		return exitUsage
	}
	// Every Fprint*(stderr, ...) below is a best-effort diagnostic: a write
	// failure there has no further channel to report through and does not
	// change the exit code, so it is deliberately discarded.
	if fs.NArg() > 0 {
		_, _ = fmt.Fprintf(stderr, "senro attach: unexpected arguments: %v\n", fs.Args())
		return exitUsage
	}
	if *follow && *runID == "" {
		_, _ = fmt.Fprintln(stderr, "senro attach: --follow requires --run <id>: it tails a "+
			"finished-or-running run straight from the run's files on disk, with no live socket needed")
		return exitUsage
	}
	if *pid != 0 && *runID != "" {
		_, _ = fmt.Fprintln(stderr, "senro attach: --pid and --run are mutually exclusive")
		return exitUsage
	}
	// --addr names an endpoint outright; the others say "go and find one".
	// Accepting both would mean silently ignoring one.
	if *addr != "" && (*pid != 0 || *runID != "" || *follow) {
		_, _ = fmt.Fprintln(stderr, "senro attach: --addr names an endpoint directly, "+
			"so it cannot be combined with --pid, --run or --follow, which discover one")
		return exitUsage
	}
	if *useTLS && *addr == "" {
		_, _ = fmt.Fprintln(stderr, "senro attach: --tls describes the --addr endpoint and means nothing without it: "+
			"a discovered run already records whether it speaks TLS")
		return exitUsage
	}

	parsed, err := parseUIMode(*ui)
	if err != nil {
		_, _ = fmt.Fprintln(stderr, err)
		return exitUsage
	}
	mode, err := resolveUIMode(parsed, isTTY)
	if err != nil {
		_, _ = fmt.Fprintln(stderr, err)
		return exitUsage
	}

	ctx, stop, interrupted := attachSignalContext(context.Background())
	defer stop()

	src, err := resolveAttachSource(ctx, attachTarget{
		pid: *pid, runID: *runID, follow: *follow, addr: *addr, useTLS: *useTLS,
	}, stderr)
	if err != nil {
		_, _ = fmt.Fprintln(stderr, err)
		return exitUsage
	}
	// Every Source.Close in this codebase returns nil; the discard is
	// explicit to say so, and to survive one that someday does not.
	defer func() { _ = src.Close() }()

	status, watchErr := watch(ctx, src, mode, stdout)
	if interrupted.Load() {
		bestEffortCancel(src)
		return exitCodeForInterrupted(status)
	}
	if watchErr != nil {
		_, _ = fmt.Fprintln(stderr, "senro attach:", watchErr)
		return exitRunFailed
	}
	return exitCodeForRunStatus(status)
}

// attachTarget is the parsed flags that between them say WHICH run to
// attach to: one struct rather than five parameters, because they are one
// decision and their mutual exclusions are checked as a unit.
type attachTarget struct {
	pid    int
	runID  string
	follow bool
	addr   string
	useTLS bool
}

// resolveAttachSource picks live vs offline from the parsed flags and, for
// a live source, negotiates the protocol version first: src.State inside
// negotiateVersion is always the client's first contact with the engine.
func resolveAttachSource(ctx context.Context, target attachTarget, stderr io.Writer) (source.Source, error) {
	pid, runID, follow := target.pid, target.runID, target.follow

	// An explicit address skips discovery and gets no disk fallback: the
	// run is on another machine, so there is no runs/<id> here to fall back
	// to, and inventing one would tail an unrelated directory that happened
	// to share a run id.
	if target.addr != "" {
		ep, err := endpointForAddr(target.addr, target.useTLS, tokenFromEnv())
		if err != nil {
			return nil, err
		}
		ls, err := source.DialEndpoint(ctx, ep)
		if err != nil {
			return nil, fmt.Errorf("senro attach: --addr %s: %w", target.addr, err)
		}
		if err := negotiateVersion(ctx, ls, stderr); err != nil {
			_ = ls.Close()
			return nil, err
		}
		return ls, nil
	}

	if follow {
		// Always from disk, no socket, whether or not the run is live.
		dir := runDir(runID)
		fs, err := source.OpenFile(dir, true)
		if err != nil {
			return nil, fmt.Errorf("senro attach: --follow %s: %w (expected a recorded run under %s)", runID, err, dir)
		}
		return fs, nil
	}

	if runID != "" {
		// Prefer a live entry with a matching RunID, which also gets the
		// disk-fallback-on-exit behaviour. A Discover error is not fatal:
		// fall through to the offline lookup and its own message.
		if entries, dErr := attachsrv.Discover(); dErr == nil {
			for _, e := range entries {
				if e.RunID == runID {
					return connectAndNegotiate(ctx, e, stderr)
				}
			}
		}
		dir := runDir(runID)
		fs, err := source.OpenFile(dir, false)
		if err != nil {
			return nil, fmt.Errorf(
				"senro attach: no run named %q found: it is not currently live, "+
					"and %s has no recorded run: %w", runID, dir, err)
		}
		return fs, nil
	}

	e, err := selectEntry(pid)
	if err != nil {
		return nil, err
	}
	return connectAndNegotiate(ctx, e, stderr)
}

// connectAndNegotiate is connectLive plus protocol negotiation, the one
// place a live Source is constructed here, so neither step can be skipped
// by a future call site.
func connectAndNegotiate(ctx context.Context, e attachsrv.Entry, stderr io.Writer) (source.Source, error) {
	src, err := connectLive(ctx, e)
	if err != nil {
		return nil, err
	}
	if err := negotiateVersion(ctx, src, stderr); err != nil {
		_ = src.Close()
		return nil, err
	}
	return src, nil
}
