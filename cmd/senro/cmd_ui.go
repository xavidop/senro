package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"

	"github.com/xavidop/senro/internal/attachsrv"
	"github.com/xavidop/senro/internal/source"
	"github.com/xavidop/senro/internal/webui"
)

// cmdUI implements `senro ui [--pid|--run|--addr] [--tls] [--port]`.
//
// It serves a page on loopback and forwards the read-only attach routes
// plus POST /api/control to the run, holding the run's bearer token in this
// process. The browser client is Go compiled to WebAssembly, folding the
// run's events with api.RunState.Apply, the same fold the terminal UI uses;
// see internal/webui. The page can steer the run (pause, cancel, retry,
// breakpoints) but there is deliberately no shell route, enforced by the
// set of routes the server forwards rather than by what the page asks for.
//
// The command blocks until interrupted, deliberately: the page's session,
// this process's copy of the credential, and the forwarded connection all
// end with it.
func cmdUI(args []string, stdout, stderr io.Writer) int {
	ctx, stop, _ := attachSignalContext(context.Background())
	defer stop()
	return runUI(ctx, args, stdout, stderr)
}

// runUI is cmdUI with the cancellation supplied rather than wired to real
// signals, so the command can be tested as the long-running server it is;
// raising a real SIGINT against the test binary would take every other test
// with it.
func runUI(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("senro ui", flag.ContinueOnError)
	fs.SetOutput(stderr)
	pid := fs.Int("pid", 0, "serve the live run with this pid")
	runID := fs.String("run", "", "serve the live run with this ID")
	// --addr is for a run this machine has no registry entry for, which in
	// practice means one reached through a port-forward. There is
	// deliberately no --token beside it; see endpoint.go.
	addr := fs.String("addr", "", "serve a TCP attach server at this host:port, with its token from $"+source.TokenEnv)
	useTLS := fs.Bool("tls", false, "the --addr endpoint speaks TLS (verified against the system roots)")
	port := fs.Int("port", 0, "loopback port to serve the UI on (0 picks a free one)")
	if err := fs.Parse(args); err != nil {
		return exitUsage
	}
	// Every Fprint*(stderr, ...) below is a best-effort diagnostic: a write
	// failure there has no further channel to report through and does not
	// change the exit code, so it is deliberately discarded.
	if fs.NArg() > 0 {
		_, _ = fmt.Fprintf(stderr, "senro ui: unexpected arguments: %v\n", fs.Args())
		return exitUsage
	}
	if *pid != 0 && *runID != "" {
		_, _ = fmt.Fprintln(stderr, "senro ui: --pid and --run are mutually exclusive")
		return exitUsage
	}
	if *addr != "" && (*pid != 0 || *runID != "") {
		_, _ = fmt.Fprintln(stderr, "senro ui: --addr names an endpoint directly, "+
			"so it cannot be combined with --pid or --run, which discover one")
		return exitUsage
	}
	if *useTLS && *addr == "" {
		_, _ = fmt.Fprintln(stderr, "senro ui: --tls describes the --addr endpoint and means nothing without it: "+
			"a discovered run already records whether it speaks TLS")
		return exitUsage
	}
	if *port < 0 || *port > 65535 {
		_, _ = fmt.Fprintf(stderr, "senro ui: --port %d is not a port number\n", *port)
		return exitUsage
	}

	up, err := upstreamFor(*pid, *runID, *addr, *useTLS)
	if err != nil {
		_, _ = fmt.Fprintln(stderr, err)
		return exitUsage
	}

	srv, err := webui.Listen(ctx, webui.Options{
		Bind:     fmt.Sprintf("127.0.0.1:%d", *port),
		Upstream: up,
	})
	if err != nil {
		if errors.Is(err, webui.ErrBundleMissing) {
			_, _ = fmt.Fprintf(stderr, "senro ui: %v\n", err)
			return exitUsage
		}
		_, _ = fmt.Fprintln(stderr, "senro ui:", err)
		return exitUsage
	}
	defer func() { _ = srv.Close() }()

	// The URL carries a one-time nonce the browser trades for a session
	// cookie (see internal/webui/session.go). It goes to stdout on its own
	// line so a terminal can make it clickable and a script can read it;
	// everything else goes to stderr.
	_, _ = fmt.Fprintln(stdout, srv.URL())
	_, _ = fmt.Fprintln(stderr, "senro ui: serving a view of this run, with controls. "+
		"The link above works once. Press Ctrl-C to stop.")

	errCh := make(chan error, 1)
	go func() { errCh <- srv.Wait() }()
	select {
	case <-ctx.Done():
		return exitSuccess
	case err := <-errCh:
		if err != nil {
			_, _ = fmt.Fprintln(stderr, "senro ui:", err)
			return exitRunFailed
		}
		return exitSuccess
	}
}

// upstreamFor resolves which live run the UI will serve, reusing the same
// discovery and credential assembly `senro attach` uses: endpoint.go is the
// only place in this CLI a token is ever put together. There is no
// --follow: a finished run has no attach server, and serving a recorded one
// would mean a second server over internal/source.FileSource; `senro attach
// --run <id> --follow` is what reads a finished run.
func upstreamFor(pid int, runID, addr string, useTLS bool) (webui.Upstream, error) {
	if addr != "" {
		ep, err := endpointForAddr(addr, useTLS, tokenFromEnv())
		if err != nil {
			return webui.Upstream{}, err
		}
		return upstreamFromEndpoint(ep), nil
	}

	if runID != "" {
		entries, err := attachsrv.Discover()
		if err != nil {
			return webui.Upstream{}, fmt.Errorf("senro ui: discovering live runs: %w", err)
		}
		for _, e := range entries {
			if e.RunID == runID {
				ep, err := endpointForEntry(e, tokenFromEnv())
				if err != nil {
					return webui.Upstream{}, err
				}
				return upstreamFromEndpoint(ep), nil
			}
		}
		return webui.Upstream{}, fmt.Errorf(
			"senro ui: no live run named %q. The browser UI serves a RUNNING engine through its attach "+
				"server, and a run that has already finished has none. Read a finished run with "+
				"`senro attach --run %s --follow`", runID, runID)
	}

	e, err := selectEntry(pid)
	if err != nil {
		return webui.Upstream{}, err
	}
	ep, err := endpointForEntry(e, tokenFromEnv())
	if err != nil {
		return webui.Upstream{}, err
	}
	return upstreamFromEndpoint(ep), nil
}

// upstreamFromEndpoint converts what this CLI dials into what the UI server
// forwards to. Two structs, because internal/webui deliberately does not
// import internal/source: its import tree is what a WebAssembly client
// cannot afford.
func upstreamFromEndpoint(ep source.Endpoint) webui.Upstream {
	network := ep.Network
	if network == "" {
		network = attachsrv.NetworkUnix
	}
	return webui.Upstream{
		Network: network,
		Address: ep.Address,
		Token:   ep.Token,
		TLS:     ep.TLS,
	}
}
