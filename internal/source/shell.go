package source

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/xavidop/senro/internal/shellwire"
)

// ShellRequest asks a live engine for an interactive session on one step's
// workspaces.
//
// Stdin, Stdout and Stderr are the caller's own streams. This package never
// touches the caller's terminal modes: a Source is a transport, and the
// client is what knows whether it has a terminal at all.
type ShellRequest struct {
	// Step names the step whose workspaces the session stands in.
	Step string
	// Cmd is the argv to run. Empty lets the engine choose its default shell,
	// which is the ordinary case.
	Cmd []string

	Stdin  io.Reader
	Stdout io.Writer
	Stderr io.Writer

	// TTY asks for a session on a real terminal rather than on pipes. The
	// engine refuses, rather than downgrading, on an executor that cannot
	// host one. A pty is one device, so all output arrives on Stdout and
	// Stderr is never written.
	TTY bool
	// Initial is the caller's terminal size at connect. It travels with the
	// request because the terminal is created with it: a pty given no size
	// reports "0 0", and a full-screen program reading that draws nothing.
	// Zero means unknown; the terminal is then created without a size.
	Initial WinSize
	// Resize carries every later size. The caller closes it, or leaves it
	// nil for a session whose window never changes.
	Resize <-chan WinSize
}

// WinSize is a terminal's dimensions, in character cells.
type WinSize struct {
	Cols uint16
	Rows uint16
}

// ShellResult is how a session ended, as the engine reported it.
//
// OK false means the engine refused the request and no session ever ran;
// Error carries the same short reason a control refusal uses (unknown_step,
// run_not_active). OK true with a non-empty Error means a session ran and
// ended for a reason other than its command exiting, such as the client
// disconnecting or the run finishing underneath it. ExitCode is meaningful
// only when Error is empty.
type ShellResult struct {
	OK       bool
	Session  string
	ExitCode int
	Error    string
}

// Sheller is the optional Source capability for opening a session.
//
// Not part of Source itself, unlike Control: there is no meaningful "shell
// against a finished run on disk" for FileSource to refuse, because the
// workspaces a session stands in are the running engine's directories. A
// client asks whether its Source can do this at all.
//
// FallbackSource forwards to whatever it wraps: a session works while the
// live side is alive and reports ErrReadOnly once fallen back to disk.
type Sheller interface {
	Shell(ctx context.Context, req ShellRequest) (ShellResult, error)
}

var (
	_ Sheller = (*LiveSource)(nil)
	_ Sheller = (*FallbackSource)(nil)
)

// shellHandshakeBudget bounds the upgrade exchange, and nothing after it:
// a session lasts as long as an operator stands in it and must have no
// deadline. Same split as internal/dockerd's attach handshake.
const shellHandshakeBudget = 30 * time.Second

// Shell opens an interactive session over POST /api/shell and pumps it until
// the engine reports it has ended.
//
// It cannot go through this Source's http.Client: a session is a hijacked
// connection, and net/http's client cannot hand the socket back. So this
// dials the endpoint directly, writes the upgrade request by hand and reads
// the response with http.ReadResponse; the credential is applied via
// LiveSource.authorize because this request never meets the Transport.
//
// Everything is read off the bufio.Reader that parsed the response, never
// off the connection: http.ReadResponse reads ahead, and reading the
// connection directly would silently skip whatever it buffered.
//
// A refusal by the engine is NOT an error return: it is a completed round
// trip whose ShellResult says why. An error return means the session could
// not be established or the connection broke.
func (ls *LiveSource) Shell(ctx context.Context, req ShellRequest) (ShellResult, error) {
	if err := ls.checkOpen(); err != nil {
		return ShellResult{}, err
	}

	conn, br, err := ls.dialShell(ctx, req)
	if err != nil {
		return ShellResult{}, err
	}
	defer func() { _ = conn.Close() }()

	// Cancellation closes the connection, the only thing that can interrupt
	// the frame read below (blocked on a socket, not a select). Stopped
	// when the session ends so a later cancellation of a long-lived caller
	// context cannot close an unrelated connection.
	sessionOver := make(chan struct{})
	defer close(sessionOver)
	go func() {
		select {
		case <-ctx.Done():
			_ = conn.Close()
		case <-sessionOver:
		}
	}()

	out := shellwire.NewWriter(conn)

	// On its own goroutine: reading a terminal blocks indefinitely and the
	// frame loop must keep draining meanwhile. It can outlive this call
	// parked on that read; a Read on an arbitrary io.Reader cannot be
	// interrupted from here.
	go pumpShellStdin(req.Stdin, out)
	// Sizes travel on the same connection as the input that caused them, so
	// they stay ordered with it: see shellwire.StreamResize.
	if req.Resize != nil {
		go pumpShellResize(sessionOver, req.Resize, out)
	}

	frames := shellwire.NewReader(br)
	for {
		stream, payload, err := frames.ReadFrame()
		if err != nil {
			if ctx.Err() != nil {
				return ShellResult{}, fmt.Errorf("source: shell: %w", ctx.Err())
			}
			return ShellResult{}, fmt.Errorf("source: shell: %w", err)
		}
		switch stream {
		case shellwire.StreamStdout:
			if _, err := req.Stdout.Write(payload); err != nil {
				return ShellResult{}, fmt.Errorf("source: shell: writing stdout: %w", err)
			}
		case shellwire.StreamStderr:
			if _, err := req.Stderr.Write(payload); err != nil {
				return ShellResult{}, fmt.Errorf("source: shell: writing stderr: %w", err)
			}
		case shellwire.StreamExit:
			var e shellwire.Exit
			if err := json.Unmarshal(payload, &e); err != nil {
				return ShellResult{}, fmt.Errorf("source: shell: decoding the session's result: %w", err)
			}
			return ShellResult{OK: e.OK, Session: e.Session, ExitCode: e.ExitCode, Error: e.Error}, nil
		default:
			// A server sending input frames is speaking the protocol
			// backwards; ending the session surfaces the defect.
			return ShellResult{}, fmt.Errorf("source: shell: %w: %d from a server",
				shellwire.ErrUnknownStream, stream)
		}
	}
}

// pumpShellStdin copies the caller's input onto the session, then sends the
// end-of-input frame: sent for ANY end of stdin, not only io.EOF, since
// both mean no more input is coming. What tells the engine a client
// vanished is the connection breaking, which this cannot fake. This is what
// makes ^D end a shell.
func pumpShellStdin(stdin io.Reader, out *shellwire.Writer) {
	buf := make([]byte, 32*1024)
	for {
		n, err := stdin.Read(buf)
		if n > 0 {
			if werr := out.WriteFrame(shellwire.StreamStdin, buf[:n]); werr != nil {
				return
			}
		}
		if err != nil {
			// Best effort: the connection may already be gone; the session
			// is ending either way.
			_ = out.WriteFrame(shellwire.StreamStdinEOF, nil)
			return
		}
	}
}

// dialShell performs the upgrade handshake and returns the raw connection
// plus the buffered reader that must be read from afterwards.
func (ls *LiveSource) dialShell(ctx context.Context, req ShellRequest) (net.Conn, *bufio.Reader, error) {
	q := url.Values{}
	q.Set("step", req.Step)
	for _, a := range req.Cmd {
		q.Add("cmd", a)
	}
	if req.TTY {
		q.Set("tty", "1")
		// In the query because the terminal is created from it (see
		// ShellRequest.Initial); omitted when unknown, not sent as zero.
		if req.Initial.Cols > 0 && req.Initial.Rows > 0 {
			q.Set("cols", strconv.FormatUint(uint64(req.Initial.Cols), 10))
			q.Set("rows", strconv.FormatUint(uint64(req.Initial.Rows), 10))
		}
	}

	var d net.Dialer
	conn, err := d.DialContext(ctx, ls.network, ls.addr)
	if err != nil {
		return nil, nil, fmt.Errorf("source: shell: dial %s: %w", ls.addr, err)
	}
	if ls.tlsConfig != nil {
		// tls.Client, not tls.Dial: the connection is already open, and the
		// handshake must finish before the upgrade request is written.
		tlsConn := tls.Client(conn, ls.tlsConfig)
		if err := tlsConn.HandshakeContext(ctx); err != nil {
			_ = conn.Close()
			return nil, nil, fmt.Errorf("source: shell: TLS handshake with %s: %w", ls.addr, err)
		}
		conn = tlsConn
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost,
		ls.url("/api/shell?"+q.Encode()), nil)
	if err != nil {
		_ = conn.Close()
		return nil, nil, fmt.Errorf("source: shell: %w", err)
	}
	httpReq.Header.Set("Connection", "Upgrade")
	httpReq.Header.Set("Upgrade", shellwire.Protocol)
	ls.authorize(httpReq)

	// Bounded for the handshake alone, then cleared: see
	// shellHandshakeBudget.
	deadline := time.Now().Add(shellHandshakeBudget)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	_ = conn.SetDeadline(deadline)

	if err := httpReq.Write(conn); err != nil {
		_ = conn.Close()
		return nil, nil, fmt.Errorf("source: shell: POST /api/shell: %w", err)
	}
	br := bufio.NewReader(conn)
	resp, err := http.ReadResponse(br, httpReq)
	if err != nil {
		_ = conn.Close()
		return nil, nil, fmt.Errorf("source: shell: POST /api/shell: %w", err)
	}
	if resp.StatusCode != http.StatusSwitchingProtocols {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		_ = conn.Close()
		if resp.StatusCode == http.StatusForbidden {
			// ErrReadOnly so a caller can errors.Is it, as Control does for
			// the same server-side refusal.
			return nil, nil, fmt.Errorf("source: shell: %w", ErrReadOnly)
		}
		return nil, nil, fmt.Errorf("source: shell: status %d: %s", resp.StatusCode, bytes.TrimSpace(body))
	}
	_ = conn.SetDeadline(time.Time{})
	return conn, br, nil
}

// Shell forwards to the wrapped live source: a session works while the
// engine is alive and reports ErrReadOnly once fallen back to disk, where
// there is no engine to create a sandbox.
func (fs *FallbackSource) Shell(ctx context.Context, req ShellRequest) (ShellResult, error) {
	if sh, ok := fs.live.(Sheller); ok {
		return sh.Shell(ctx, req)
	}
	return ShellResult{}, fmt.Errorf("source: shell: %w", ErrReadOnly)
}

// pumpShellResize forwards the caller's window sizes for the session's
// life. A write failure just ends the loop: the frame loop will see the
// failing connection too.
func pumpShellResize(done <-chan struct{}, resize <-chan WinSize, out *shellwire.Writer) {
	for {
		select {
		case <-done:
			return
		case ws, ok := <-resize:
			if !ok {
				return
			}
			if err := out.WriteResize(shellwire.WinSize{Cols: ws.Cols, Rows: ws.Rows}); err != nil {
				return
			}
		}
	}
}
