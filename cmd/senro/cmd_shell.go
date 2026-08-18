package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/xavidop/senro/internal/attachsrv"
	"github.com/xavidop/senro/internal/source"
)

// shellBanner is printed once before a pipe session starts: without a pty
// there is no prompt, no line editing, no history and no job control, which
// is workable but surprising to be dropped into unwarned. On stderr, so
// `senro shell --step build -- cat build.log > out` captures the file and
// not this.
const shellBanner = "senro shell: this session runs against pipes: no prompt, no line editing, " +
	"no job control. Type a command and press enter; ^D ends the session. Pass --tty for a real terminal.\n"

// ttyBanner is the terminal session's own. Shorter: what matters about a
// raw-mode session is how to get out, since the local terminal interprets
// nothing and the usual escape hatches belong to the far side.
const ttyBanner = "senro shell: terminal session. ^D ends it; ^C goes to the remote command.\n"

// cmdShell implements `senro shell [--pid|--run] --step ID [-- cmd...]`.
// Deliberately the smallest command here: correctness lives in the engine
// and the wire in internal/source, so this resolves which live run to talk
// to, hands over the three streams, and maps the result to an exit code.
//
// It requires a LIVE run and does not fall back to disk the way `senro
// attach` can: a session stands inside the running engine's workspace
// directories and needs that engine to create a sandbox. The error names
// `senro ws pull`, which writes the same files out.
func cmdShell(args []string, stdout, stderr io.Writer, stdin io.Reader) int {
	fs := flag.NewFlagSet("senro shell", flag.ContinueOnError)
	fs.SetOutput(stderr)
	pid := fs.Int("pid", 0, "open a shell on the live run with this pid")
	runID := fs.String("run", "", "open a shell on the live run with this ID")
	// A session over TCP is an interactive prompt inside a step's workspace,
	// reached across a network. It is behind the same bearer token as every
	// other endpoint and there is deliberately no --token beside this; see
	// endpoint.go.
	addr := fs.String("addr", "", "open a shell on the TCP attach server at this host:port, with its token from $"+source.TokenEnv)
	useTLS := fs.Bool("tls", false, "the --addr endpoint speaks TLS (verified against the system roots)")
	step := fs.String("step", "", "the step whose workspaces the session stands in (required)")
	tty := fs.Bool("tty", false, "run the session on a real terminal: job control, line editing, "+
		"^C as a signal, and a window size. Refused by an executor that cannot host one")
	if err := fs.Parse(args); err != nil {
		return exitUsage
	}
	// Every Fprint*(stderr, ...) below is a best-effort diagnostic: a write
	// failure there has no further channel to report through and does not
	// change the exit code, so it is deliberately discarded.
	if *step == "" {
		_, _ = fmt.Fprintln(stderr, "senro shell: --step is required: a session stands in one step's "+
			"workspaces, so there is no meaningful default. `senro attach` lists a run's steps.")
		return exitUsage
	}
	if *pid != 0 && *runID != "" {
		_, _ = fmt.Fprintln(stderr, "senro shell: --pid and --run are mutually exclusive")
		return exitUsage
	}
	if *addr != "" && (*pid != 0 || *runID != "") {
		_, _ = fmt.Fprintln(stderr, "senro shell: --addr names an endpoint directly, "+
			"so it cannot be combined with --pid or --run, which discover one")
		return exitUsage
	}
	if *useTLS && *addr == "" {
		_, _ = fmt.Fprintln(stderr, "senro shell: --tls describes the --addr endpoint and means nothing without it: "+
			"a discovered run already records whether it speaks TLS")
		return exitUsage
	}

	ctx, stop, _ := attachSignalContext(context.Background())
	defer stop()

	src, err := resolveShellSource(ctx, *pid, *runID, *addr, *useTLS)
	if err != nil {
		_, _ = fmt.Fprintln(stderr, err)
		return exitUsage
	}
	defer func() { _ = src.Close() }()

	sh, ok := src.(source.Sheller)
	if !ok {
		_, _ = fmt.Fprintln(stderr, "senro shell: this run cannot host a session: it is being read "+
			"from disk rather than from a live engine. Use `senro ws pull` to write its workspaces out instead.")
		return exitUsage
	}

	if *tty {
		_, _ = fmt.Fprint(stderr, ttyBanner)
	} else {
		_, _ = fmt.Fprint(stderr, shellBanner)
	}

	shellReq := source.ShellRequest{
		Step: *step, Cmd: fs.Args(), Stdin: stdin, Stdout: stdout, Stderr: stderr,
	}
	if *tty {
		// Raw mode and the size watcher belong to the CLIENT: the server
		// never learns what kind of terminal is on this end, only how big
		// it is. See shell_tty.go for why raw mode is not optional.
		stopWinch := make(chan struct{})
		defer close(stopWinch)
		restore, err := enterRawMode(stdin)
		if err != nil {
			// Not fatal: a stdin that is not a terminal can still host a
			// session, it simply has no size to give and no resize to
			// forward.
			_, _ = fmt.Fprintln(stderr, "senro shell: this stdin is not a terminal, "+
				"so the session gets no window size and no line discipline of its own")
		} else {
			defer restore()
		}
		shellReq.TTY = true
		if fd, ok := stdinFD(stdin); ok {
			if ws, isTerm := terminalSize(fd); isTerm {
				shellReq.Initial = ws
			}
			shellReq.Resize = watchWinch(stopWinch, fd)
		}
	}

	res, err := sh.Shell(ctx, shellReq)
	if err != nil {
		if errors.Is(err, source.ErrReadOnly) {
			_, _ = fmt.Fprintln(stderr, "senro shell: this run's attach socket is read-only, "+
				"so it does not hand out a command prompt")
			return exitRunFailed
		}
		_, _ = fmt.Fprintln(stderr, "senro shell:", err)
		return exitRunFailed
	}
	if !res.OK {
		// An engine refusal, printed verbatim rather than translated: the
		// reasons live in internal/engine/control.go, and a friendlier
		// second translation here would be a second place to drift.
		_, _ = fmt.Fprintf(stderr, "senro shell: refused: %s\n", res.Error)
		return exitRunFailed
	}
	if res.Error != "" {
		// The session ended for a reason other than its command exiting:
		// the connection broke, or the run finished underneath it. Said out
		// loud, or an operator concludes their shell exited normally.
		_, _ = fmt.Fprintf(stderr, "senro shell: session %s ended: %s\n", res.Session, res.Error)
		return exitRunFailed
	}
	return shellExitCode(res.ExitCode)
}

// shellExitCode maps the session command's own status onto this process's,
// so `senro shell --step build -- test -f out/binary` is usable in a script
// exactly like the command it ran. A different contract from
// exitCodeForRunStatus, which reports a RUN's outcome. Codes pass through
// unchanged, except a negative one (killed by a signal, which os/exec
// reports as -1), which becomes exitRunFailed rather than whatever the OS
// would make of it.
func shellExitCode(code int) int {
	if code < 0 {
		return exitRunFailed
	}
	return code
}

// resolveShellSource finds the live run to open a session on. Unlike
// resolveAttachSource it has no offline path at all; see cmdShell's own doc.
func resolveShellSource(ctx context.Context, pid int, runID, addr string, useTLS bool) (source.Source, error) {
	// An explicit address skips discovery: the run is on another machine,
	// and nothing in this machine's registry describes it.
	if addr != "" {
		ep, err := endpointForAddr(addr, useTLS, tokenFromEnv())
		if err != nil {
			return nil, err
		}
		ls, err := source.DialEndpoint(ctx, ep)
		if err != nil {
			return nil, fmt.Errorf("senro shell: --addr %s: %w", addr, err)
		}
		return ls, nil
	}
	if runID != "" {
		entries, err := attachsrv.Discover()
		if err != nil {
			return nil, fmt.Errorf("senro shell: discovering live runs: %w", err)
		}
		for _, e := range entries {
			if e.RunID == runID {
				return connectLive(ctx, e)
			}
		}
		return nil, fmt.Errorf(
			"senro shell: no LIVE run named %q: a session needs the running engine that owns the "+
				"run's workspaces. If the run has finished, `senro ws pull %s <workspace>` writes its "+
				"files out instead", runID, runID)
	}
	e, err := selectEntry(pid)
	if err != nil {
		return nil, err
	}
	return connectLive(ctx, e)
}

// cmdShellMain is main's entry point, closing over the process's own stdin.
// Split from cmdShell so tests can pass a reader of their own.
func cmdShellMain(args []string, stdout, stderr io.Writer) int {
	return cmdShell(args, stdout, stderr, os.Stdin)
}

// stdinFD reports the file descriptor behind stdin, when there is one: a
// test hands this command a bytes.Reader or a pipe, so os.Stdin is only one
// of the things that reaches here.
func stdinFD(stdin io.Reader) (uintptr, bool) {
	f, ok := stdin.(interface{ Fd() uintptr })
	if !ok {
		return 0, false
	}
	return f.Fd(), true
}

// enterRawMode puts stdin into raw mode when it is a terminal. A stdin that
// is not one gets an error the caller treats as a diagnostic: a session
// driven from a pipe simply has no line discipline to suspend.
func enterRawMode(stdin io.Reader) (func(), error) {
	fd, ok := stdinFD(stdin)
	if !ok {
		return nil, errNotATerminal
	}
	if _, isTerm := terminalSize(fd); !isTerm {
		return nil, errNotATerminal
	}
	return makeRaw(fd)
}

var errNotATerminal = errors.New("stdin is not a terminal")
