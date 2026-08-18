package dockerd

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"sync"
	"time"
)

// attachHandshakeBudget bounds the one HTTP exchange that turns a fresh
// connection into an attach stream, and nothing after it: the session has
// no deadline because the thing on the other end is a person.
const attachHandshakeBudget = 30 * time.Second

// AttachedStream is one live, bidirectional connection to a running
// container's standard streams: the daemon's /attach endpoint after it has
// stopped speaking HTTP.
//
// It hand-dials the socket, writes one HTTP request, reads the response
// with http.ReadResponse and keeps the connection, because net/http offers
// no supported way to take the socket back out of the Transport for a
// bidirectional session. This is the same protocol upgrade the docker CLI
// performs for `docker attach`.
//
// Reads come off the bufio.Reader that parsed the response headers and
// NEVER off the raw connection: http.ReadResponse reads ahead, so the first
// frames may already sit in the reader's buffer, and a direct read would
// silently skip exactly that many bytes.
type AttachedStream struct {
	conn net.Conn
	br   *bufio.Reader

	// closeOnce makes Close idempotent. Session teardown uses Close to
	// unblock a Demux and races the ordinary end of the stream by design,
	// so both paths reaching it at once is the expected case.
	closeOnce sync.Once
}

// ContainerAttach opens a bidirectional stream to a container's standard
// streams and returns once the daemon has switched protocols.
//
// Order matters: /attach replays nothing, so attaching after
// ContainerStart loses whatever the container produced in between (a
// shell's first prompt; a short command's entire answer). ContainerLogs
// tolerates starting first only because follow=1 replays from the first
// byte. Create, attach, start, in that order.
//
// stdin is requested unconditionally: a container without OpenStdin simply
// has nothing on the other end, so asking costs nothing.
func (c *Client) ContainerAttach(ctx context.Context, id string) (*AttachedStream, error) {
	q := url.Values{"stream": {"1"}, "stdin": {"1"}, "stdout": {"1"}, "stderr": {"1"}}
	path := "/containers/" + ref(id) + "/attach?" + q.Encode()

	var d net.Dialer
	conn, err := d.DialContext(ctx, "unix", c.socket)
	if err != nil {
		return nil, fmt.Errorf("dockerd: dialling %s for attach: %w", c.socket, err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://docker/"+APIVersion+path, nil)
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	// Without the upgrade the daemon answers 200 and streams anyway, but
	// asking makes it treat the connection as hijacked on its own side
	// rather than as a response body it may later try to frame.
	req.Header.Set("Connection", "Upgrade")
	req.Header.Set("Upgrade", "tcp")

	// The handshake alone is bounded; the bound is then REMOVED. Two
	// different lifetimes, and conflating them is how a shell dies after
	// ten seconds. A deadline rather than a goroutine watching ctx: closing
	// from a second goroutine as the handshake succeeds is a race with no
	// winner worth having. The cost: this window honours ctx's DEADLINE but
	// not a bare cancellation (worst case, attachHandshakeBudget); the dial
	// above honours cancellation in full.
	deadline := time.Now().Add(attachHandshakeBudget)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	_ = conn.SetDeadline(deadline)

	if err := req.Write(conn); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("dockerd: POST %s: %w", path, err)
	}
	br := bufio.NewReader(conn)
	resp, err := http.ReadResponse(br, req)
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("dockerd: POST %s: %w", path, err)
	}
	// 101 is the upgrade being honoured; 200 is a daemon that streamed
	// without upgrading. Both leave the connection carrying the same frames,
	// and both are what the docker CLI itself accepts here.
	if resp.StatusCode != http.StatusSwitchingProtocols && resp.StatusCode != http.StatusOK {
		err := statusError(http.MethodPost, path, resp)
		_ = resp.Body.Close()
		_ = conn.Close()
		return nil, err
	}
	// The bound is lifted: a session that expires while somebody reads a
	// stack trace is a bug, not a safety feature.
	_ = conn.SetDeadline(time.Time{})
	return &AttachedStream{conn: conn, br: br}, nil
}

// Write sends bytes to the container's standard input. Nothing is framed on
// the way in: only the OUTPUT direction carries the 8-byte headers demux
// reads. That asymmetry is the daemon's, not this client's.
func (a *AttachedStream) Write(p []byte) (int, error) { return a.conn.Write(p) }

// CloseWrite closes the stdin half alone, leaving output flowing: a
// session's ^D. A container created with StdinOnce sees EOF, so an ordinary
// shell exits by itself. A connection that cannot half-close is closed
// outright rather than left open, so a future transport cannot silently
// turn "done typing" into "still there"; every current connection is a unix
// socket, which can.
func (a *AttachedStream) CloseWrite() error {
	if cw, ok := a.conn.(interface{ CloseWrite() error }); ok {
		return cw.CloseWrite()
	}
	return a.Close()
}

// Read returns the stream's bytes exactly as the daemon sent them, with no
// demultiplexing: correct for a Tty container, WRONG otherwise. Without a
// TTY the daemon frames the stream, so reading raw hands the caller frame
// headers as output; with one, Demux would read the first eight bytes of
// output as a header. Tty means Read, no Tty means Demux, and the wrong
// choice produces garbage rather than an error. See ContainerSpec.Tty.
func (a *AttachedStream) Read(p []byte) (int, error) { return a.br.Read(p) }

// Demux copies the container's output into stdout and stderr through the
// same frame reader ContainerLogs uses. It returns nil at a clean end and
// an error for a truncated frame.
func (a *AttachedStream) Demux(stdout, stderr io.Writer) error {
	return demux(a.br, stdout, stderr)
}

// Close ends the stream in both directions: what a caller uses to unblock a
// Demux waiting on a container producing nothing, the usual state of an
// abandoned session. Idempotent, because the ordinary end of a session and
// a teardown that races it both arrive here.
func (a *AttachedStream) Close() error {
	var err error
	a.closeOnce.Do(func() { err = a.conn.Close() })
	return err
}
