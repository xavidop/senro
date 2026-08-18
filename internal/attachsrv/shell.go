package attachsrv

import (
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/xavidop/senro/internal/shellwire"
	"github.com/xavidop/senro/internal/sink"
)

// POST /api/shell?step=<id>[&cmd=...] is the one endpoint on this server
// that stops speaking HTTP.
//
// Its own connection, not a control frame: control requests are never
// handled concurrently, which is what makes run.cancel idempotent without
// locking (internal/engine/control.go), and a session held in that
// position would stop the run for as long as somebody stood in it. So a
// session gets its own connection, its own channel (Hub.Shells) and its
// own frame format (internal/shellwire), touching no part of the control
// path.
//
// This is a remote code execution surface. It has no boundary of its own:
// it inherits whatever guards the listener it arrived over.
//
//   - Over a unix socket (the default): 0600 in a 0700 directory, and
//     every connection has already passed CheckPeer, which fails closed.
//   - Over TCP: the per-run bearer token, checked in constant time like
//     every other request (tokenAuth wraps the whole mux), plus TLS unless
//     the bind is loopback. There is no peer credential and none is
//     pretended.
//   - Either way, Options.ReadOnly refuses a session before anything is
//     hijacked: a read-only attach that handed out a command prompt would
//     make the option meaningless.
//
// Deliberately NOT refused over TCP: the control channel on the same
// listener already runs a step's own command (step.retry, run.rerun_from),
// so a token good enough for those is good enough for this, and refusing
// only this would be theatre. The obligation that follows is to say so:
// over TCP, this token is a remote shell for whoever holds it, which
// /docs/attach/security/ states plainly.
//
// Refusals split across two mechanisms on purpose. Anything this server
// can decide itself (a malformed upgrade, a read-only server, a finished
// run) is an ordinary HTTP status BEFORE the hijack, so a client need not
// speak the session protocol to read it. Anything only the ENGINE knows
// (an unknown step, an executor that cannot host a session) necessarily
// arrives after the upgrade, as a shellwire exit frame, the same frame a
// session that ran and exited uses: one client code path, read frames
// until the exit frame.

// shellSubmitTimeout bounds how long a hijacked session waits for the
// engine to accept its request off the unbuffered shell channel (see
// Hub.Shells): the wait for a reader to actually be there. The engine's
// reader is normally parked on it; this bounds the window between a run
// finishing and Hub.Done becoming true, where a client would otherwise
// hold an upgraded connection forever.
const shellSubmitTimeout = 10 * time.Second

// handleShell upgrades one connection into an interactive session and
// pumps it until the engine says the session is over. The order matters:
// refuse what can be refused cheaply, THEN hijack (irreversible, and it
// turns every later error into a protocol-level one), then hand the engine
// plain streams and get out of the way.
func (s *Server) handleShell(w http.ResponseWriter, r *http.Request) {
	if s.readOnly {
		http.Error(w, "attachsrv: "+ErrReadOnly.Error()+
			": a read-only attach does not hand out a command prompt", http.StatusForbidden)
		return
	}
	if !wantsShellUpgrade(r) {
		http.Error(w, "attachsrv: this endpoint speaks "+shellwire.Protocol+
			" and needs Connection: Upgrade with Upgrade: "+shellwire.Protocol, http.StatusBadRequest)
		return
	}

	// r.URL.Query() has already percent-decoded this, and a query parameter
	// carries no path structure for a router to split on, so a nested step
	// id arrives whole (unlike GET /api/logs/{step}, which needs
	// stepid.Encode).
	step := r.URL.Query().Get("step")
	cmd := r.URL.Query()["cmd"]

	// The same precheck handleControl makes: once the hub knows the run is
	// over, nothing will read the shell channel again, and a request now
	// would wait out shellSubmitTimeout to learn what is already known. A
	// status rather than a frame, because nothing has been hijacked yet.
	if s.hub.Done() {
		http.Error(w, "attachsrv: "+sink.ReasonRunFinished, http.StatusServiceUnavailable)
		return
	}

	hj, ok := w.(http.Hijacker)
	if !ok {
		http.Error(w, "attachsrv: this connection cannot carry a session", http.StatusInternalServerError)
		return
	}

	// Tracked before the hijack: once the connection leaves net/http's
	// hands, Server.Close cannot reach it (net/http does not know about
	// hijacked connections), so it must be this package's responsibility
	// from before that instant. False means Close has already committed.
	if !s.track() {
		http.Error(w, "attachsrv: server is shutting down", http.StatusServiceUnavailable)
		return
	}
	defer s.untrack()

	conn, buf, err := hj.Hijack()
	if err != nil {
		http.Error(w, "attachsrv: "+err.Error(), http.StatusInternalServerError)
		return
	}
	// From here nothing may write an HTTP response: this is a raw connection
	// and the client is about to be told so.
	registered := s.addShellConn(conn)
	defer func() {
		s.removeShellConn(conn)
		_ = conn.Close()
	}()
	if !registered {
		// Close raced the hijack and already swept the connection set, so
		// nothing would ever tear this connection down.
		return
	}

	if _, err := io.WriteString(conn, "HTTP/1.1 101 Switching Protocols\r\n"+
		"Upgrade: "+shellwire.Protocol+"\r\n"+
		"Connection: Upgrade\r\n\r\n"); err != nil {
		return
	}

	// buf.Reader, never conn: net/http may already have read ahead into
	// that buffered reader while parsing the request, so reading the
	// connection directly would silently skip whatever it buffered.
	frames := shellwire.NewReader(buf.Reader)
	out := shellwire.NewWriter(conn)

	// The session KIND, asked for by the client (see sink.ShellRequest.TTY):
	// a terminal is one device with one output stream, and an executor that
	// cannot host one refuses rather than quietly handing back pipes.
	tty := r.URL.Query().Get("tty") == "1"
	in := shellwire.NewInput(frames)

	var resize chan sink.WinSize
	if tty {
		// Buffered and non-blocking: fed from the frame-reading goroutine,
		// which must not stall between one byte of the operator's typing
		// and the next. A dropped resize is superseded by the next one.
		resize = make(chan sink.WinSize, 1)
		in.OnResize(func(ws shellwire.WinSize) {
			select {
			case resize <- sink.WinSize{Cols: ws.Cols, Rows: ws.Rows}:
			default:
			}
		})
	}

	reply := make(chan sink.ShellResponse, 1)
	req := sink.ShellRequest{
		ID:       shellRequestID(r),
		ClientID: clientIDFromContext(r.Context()),
		Step:     step,
		Cmd:      cmd,
		TTY:      tty,
		Initial:  initialWinSize(r),
		Resize:   resize,
		Stdin:    in,
		Stdout:   out.Stream(shellwire.StreamStdout),
		Stderr:   out.Stream(shellwire.StreamStderr),
		Reply:    reply,
	}

	select {
	case s.hub.shells <- req:
	case <-time.After(shellSubmitTimeout):
		writeShellExit(out, shellwire.Exit{Error: sink.ReasonRunFinished})
		return
	case <-s.done:
		writeShellExit(out, shellwire.Exit{Error: "server_shutting_down"})
		return
	}

	// From here the engine owns the streams and this handler owns only the
	// wait. It must NOT also read the connection: shellwire.Input is the
	// only reader, and a second would steal frames from the session.
	//
	// The ordinary end is the engine answering, whatever happened. Even
	// Server.Close's sweep reaches the engine that way: force-closing this
	// connection fails its next read, which it treats as a disconnect.
	//
	// s.done is still selected on, bounded, and is not redundant: Close
	// waits for this handler, so an engine that never answered would hold
	// Close open forever, and a server that could not shut down is worse
	// than a missing exit frame on an already-closed connection.
	select {
	case resp := <-reply:
		writeShellExit(out, shellwire.Exit{
			OK: resp.OK, Session: resp.Session, Error: resp.Error, ExitCode: resp.ExitCode,
		})
	case <-s.done:
		select {
		case resp := <-reply:
			writeShellExit(out, shellwire.Exit{
				OK: resp.OK, Session: resp.Session, Error: resp.Error, ExitCode: resp.ExitCode,
			})
		case <-time.After(streamWriteTimeout):
		}
	}
}

// writeShellExit sends the terminal frame, best effort: a client that has
// already gone has nowhere to receive it, and the session is over either
// way.
func writeShellExit(w *shellwire.Writer, e shellwire.Exit) {
	_ = w.WriteExit(e)
}

// shellRequestID names one request for correlation: the connection's id
// plus a counter is enough, since it is only compared within one run's own
// event stream, like the client id it is built from.
func shellRequestID(r *http.Request) string {
	return "shell-" + clientIDFromContext(r.Context()) + "-" + strconv.FormatUint(shellSeq.Add(1), 10)
}

// wantsShellUpgrade reports whether the client asked for exactly the
// protocol this build speaks. Exactly, not approximately: a client naming
// a different version is refused with a message saying what this server
// speaks, rather than dropped into a session whose framing it may not
// share. Connection is matched loosely (a comma-separated list proxies add
// to); the Upgrade token strictly.
func wantsShellUpgrade(r *http.Request) bool {
	if !headerContainsToken(r.Header.Values("Connection"), "upgrade") {
		return false
	}
	return headerContainsToken(r.Header.Values("Upgrade"), shellwire.Protocol)
}

// addShellConn registers a hijacked connection so Close can tear it down,
// and reports whether that succeeded.
//
// net/http will not do it: http.Server.Close "does not attempt to close
// (and does not even know about) any hijacked connections". Without this a
// shutting-down server would leave every open session attached to a hub
// that no longer exists, having torn down everything except the one thing
// holding a command prompt open.
//
// False means Close has already swept, so the caller must not proceed:
// nothing else would ever close its connection.
func (s *Server) addShellConn(c net.Conn) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closing {
		return false
	}
	if s.shellConns == nil {
		s.shellConns = make(map[net.Conn]struct{})
	}
	s.shellConns[c] = struct{}{}
	return true
}

func (s *Server) removeShellConn(c net.Conn) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.shellConns, c)
}

// closeShellConns force-closes every open session connection, called from
// Close under the same commitment that stops new ones registering.
//
// Force-closing rather than draining, for Close's own reason about
// /api/stream: a session is held open until its client leaves, so draining
// one is waiting for a person. Closing is also what ENDS the session: the
// engine's next read fails, it reads that as a disconnect, kills the
// command and answers on its reply channel, completing the handler's wait
// through the path it already had.
func (s *Server) closeShellConns() {
	s.mu.Lock()
	conns := make([]net.Conn, 0, len(s.shellConns))
	for c := range s.shellConns {
		conns = append(conns, c)
	}
	s.shellConns = nil
	s.mu.Unlock()

	for _, c := range conns {
		_ = c.Close()
	}
}

// headerContainsToken reports whether any of values, each possibly a
// comma-separated list, contains token, case-insensitively: both header
// fields this checks are defined that way.
func headerContainsToken(values []string, token string) bool {
	for _, v := range values {
		for _, part := range strings.Split(v, ",") {
			if strings.EqualFold(strings.TrimSpace(part), token) {
				return true
			}
		}
	}
	return false
}

// shellSeq numbers session requests within one server, so two sessions
// from one connection stay distinguishable. A counter, like every other
// identifier this package assigns: only compared for equality inside one
// run, and assertable in a test.
var shellSeq atomic.Uint64

// initialWinSize reads the size the client's terminal had when it
// connected, from the query string.
//
// In the URL rather than a first frame, because the terminal has to be
// CREATED with it: a pty whose creator sets no size reports "0 0" and a
// full-screen program reading that draws nothing, and a frame would arrive
// after the create.
//
// An absent or unparseable size is zero, meaning "the client did not know",
// rather than inventing 80x24 and being confidently wrong.
func initialWinSize(r *http.Request) sink.WinSize {
	q := r.URL.Query()
	cols, cerr := strconv.ParseUint(q.Get("cols"), 10, 16)
	rows, rerr := strconv.ParseUint(q.Get("rows"), 10, 16)
	if cerr != nil || rerr != nil {
		return sink.WinSize{}
	}
	return sink.WinSize{Cols: uint16(cols), Rows: uint16(rows)}
}
