package attachsrv

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/internal/eventlog"
	"github.com/xavidop/senro/internal/sink"
)

// streamWriteTimeout bounds a single write to a /api/stream client. A
// Write blocked on a wedged connection cannot be interrupted by the
// handler's select loop; only a deadline on the connection can, and
// without one a client that stopped reading would pin the handler's
// goroutine (and its hub subscription) forever. See
// TestWedgedStreamClientIsClosedByItsWriteDeadlineAlone.
const streamWriteTimeout = 3 * time.Second

// controlTimeout bounds how long POST /api/control waits for the queued
// request to be accepted and answered: a defensive backstop for a wedged
// or absent consumer (the engine's scheduler consumes Hub.Control() for
// the life of a run, and Hub.Done() answers post-run requests without the
// channel), degrading that case to a clear 504 instead of a goroutine
// blocked forever.
//
// internal/webui depends on the VALUE: its proxy waits longer than this on
// purpose, so raising this past webui.controlHeaderBudget would cut the
// request off before this server could produce the 504. Raise them
// together.
const controlTimeout = 30 * time.Second

// maxControlBodyBytes bounds POST /api/control's request body. Every
// control frame is small by design (api.Frame: only per-step log chunks
// carry bulk), and without a bound the read-only 403 path echoes req.ID
// back into its own body, turning an unbounded request into a same-size
// reflection on the one endpoint meant to be inert. 64KiB is generous
// slack.
const maxControlBodyBytes = 64 * 1024

// ErrReadOnly is the reason named in a control response's Frame.Error field
// when the server was configured with Options.ReadOnly and a client submits
// a control request anyway.
var ErrReadOnly = errors.New("attachsrv: server is read-only")

// NetworkUnix and NetworkTCP are the two values Options.Network takes. The
// empty string means NetworkUnix, so an Options built before this field
// existed still binds the transport it always did.
const (
	NetworkUnix = "unix"
	NetworkTCP  = "tcp"
)

// Options configures Listen.
type Options struct {
	// Bind is the address to listen on: a unix socket path when Network is
	// NetworkUnix (the default), or a host:port when it is NetworkTCP.
	Bind string
	// Network selects the transport, and with it which access boundary
	// applies; the difference is the whole security story of this package:
	//
	//   - NetworkUnix (the default) is guarded by the filesystem and the
	//     kernel: mode 0600 in a 0700 directory, and CheckPeer on every
	//     accepted connection, failing closed. It takes no Token; one is
	//     refused rather than ignored.
	//   - NetworkTCP is guarded by Token alone, plus TLSConfig when the
	//     bind is not loopback. There is no peer for the kernel to vouch
	//     for and no file mode to enforce; see token.go.
	Network string
	// Token is the per-run bearer credential every request over a TCP
	// listener must present as "Authorization: Bearer <token>". Required for
	// NetworkTCP, at least minTokenLength characters; refused outright for
	// NetworkUnix.
	Token string
	// TLSConfig, when set, wraps a TCP listener in TLS. Required when the
	// TCP bind is not loopback: see listenTCP for why that requirement has
	// no escape hatch. Refused for NetworkUnix, which is already local by
	// construction and has nothing to encrypt against.
	TLSConfig *tls.Config
	// Dir is the run directory: GET /api/plan reads Dir/plan.json and
	// GET /api/logs/{step} reads Dir/logs/....
	Dir string
	// Hub is the run's event hub. GET /api/state, GET /api/stream and
	// POST /api/control are all backed by it.
	Hub *Hub
	// ReadOnly makes POST /api/control refuse every request with
	// ErrReadOnly instead of forwarding it to Hub.Control().
	ReadOnly bool
}

// Server is the attach server: one http.Server, over one listener,
// exposing one Hub and one run directory to attached clients. The same mux
// is meant to grow to carry the embedded browser UI later; nothing about
// these endpoints forecloses that.
type Server struct {
	hub      *Hub
	dir      string
	readOnly bool

	ln      net.Listener
	httpSrv *http.Server
	addr    string
	network string

	// peerRejected counts connections peerCheckedListener closed for
	// failing the peer-credential check, before net/http ever saw them.
	// Atomic-only access: written from the Serve goroutine, read from any.
	peerRejected uint64

	// authRejected is peerRejected's TCP counterpart: requests tokenAuth
	// refused. Two counters because they count refusals at two layers on
	// two transports, and collapsing them would make "connection nobody
	// could vouch for, or request nobody could authenticate" unanswerable.
	authRejected uint64

	// done is closed exactly once, by Close: the third, independent way a
	// /api/stream handler's select loop can end.
	done      chan struct{}
	closeOnce sync.Once

	// mu guards closing: a positive-delta wg.Add must be ordered against
	// Close's wg.Wait, and a handler goroutine starting has no other
	// ordering against a concurrent Close. See track.
	mu      sync.Mutex
	closing bool

	// shellConns is every hijacked interactive session connection currently
	// open, guarded by mu alongside closing so a connection registered
	// after Close's sweep is not orphaned. net/http does not track hijacked
	// connections at all, so this is the only thing that can. See shell.go's
	// addShellConn.
	shellConns map[net.Conn]struct{}

	// wg tracks the Serve goroutine and every long-lived handler goroutine
	// (added only through track), so Close does not return while one is
	// still running.
	wg sync.WaitGroup

	// streamWG tracks only the /api/stream handler goroutines, so Close can
	// wait on stream handlers ALONE, bounded, before force-closing
	// anything. wg cannot serve that purpose: it also carries the Serve
	// goroutine, which exits only when the listener closes, so waiting on
	// wg first would deadlock. See Close for what the pre-drain buys.
	streamWG sync.WaitGroup
}

// track registers one long-lived handler goroutine (handleStream's) with
// the shutdown wait groups, and reports whether that succeeded: false once
// Close has committed, in which case the caller must not proceed and must
// not call untrack.
//
// sync.WaitGroup forbids a positive-delta Add racing a Wait, and a handler
// goroutine has no other ordering against a concurrent Close. mu
// serializes the two: either track adds before Close can set closing, or
// Close sets closing first and track never Adds. The unconditional
// s.wg.Add(1) version is exactly what the -race suite caught; see
// TestCloseDoesNotRaceConcurrentStreamHandlers.
func (s *Server) track() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closing {
		return false
	}
	s.wg.Add(1)
	s.streamWG.Add(1)
	return true
}

// untrack releases what a successful track registered.
func (s *Server) untrack() {
	s.wg.Done()
	s.streamWG.Done()
}

// Listen binds opts.Bind and starts serving in the background; the
// returned Server is ready before Listen returns. If ctx is cancelled the
// server closes itself, exactly as if Close had been called; Close remains
// available too.
//
// Which guard applies depends entirely on opts.Network:
//
//   - NetworkUnix (the default): a unix socket, mode 0600, every accepted
//     connection checked with CheckPeer before net/http ever sees it (see
//     peerCheckedListener).
//   - NetworkTCP: a host:port, every REQUEST checked against opts.Token in
//     constant time before it reaches the mux, and TLS required unless the
//     bind is loopback (see listenTCP and token.go).
func Listen(ctx context.Context, opts Options) (*Server, error) {
	return listen(ctx, opts, nil)
}

// listen is Listen's real implementation, with the peer-credential check
// as an explicit parameter: nil means "use the real CheckPeer", which is
// what every production call gets.
//
// A nil SENTINEL rather than Listen passing CheckPeer by name: it keeps
// the correct call as simple as Go syntax allows and keeps nil meaning "no
// override". The proof the binding is correct is
// TestListenWiresTheRealPeerCheck, which calls the real Listen and
// inspects the check the Server retained. The indirection exists so a
// white-box test can substitute a deterministic check and prove the
// ACCEPT-PATH WIRING (rejected connection closed before a handler, no
// Accept error, rejection counted) without fabricating a connection from a
// different uid, which no unprivileged test can do; the credential check
// itself is proven in peercred_test.go and peercred_internal_test.go.
func listen(ctx context.Context, opts Options, peerCheck func(net.Conn) error) (*Server, error) {
	if peerCheck == nil {
		peerCheck = CheckPeer
	}
	if opts.Bind == "" {
		return nil, errors.New("attachsrv: Options.Bind is required")
	}
	if opts.Hub == nil {
		return nil, errors.New("attachsrv: Options.Hub is required")
	}
	if opts.Dir == "" {
		return nil, errors.New("attachsrv: Options.Dir is required")
	}

	network := opts.Network
	if network == "" {
		network = NetworkUnix
	}

	s := &Server{
		hub:      opts.Hub,
		dir:      opts.Dir,
		readOnly: opts.ReadOnly,
		network:  network,
		done:     make(chan struct{}),
	}

	var auth *tokenAuth
	var err error
	switch network {
	case NetworkUnix:
		s.ln, err = s.listenUnix(opts, peerCheck)
	case NetworkTCP:
		s.ln, auth, err = s.listenTCP(opts)
	default:
		return nil, fmt.Errorf("attachsrv: Options.Network %q is not a transport this build binds: use %q or %q",
			network, NetworkUnix, NetworkTCP)
	}
	if err != nil {
		return nil, err
	}
	s.addr = s.ln.Addr().String()

	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/state", s.handleState)
	mux.HandleFunc("GET /api/plan", s.handlePlan)
	mux.HandleFunc("GET /api/logs/{step}", s.handleLogs)
	mux.HandleFunc("GET /api/stream", s.handleStream)
	mux.HandleFunc("POST /api/control", s.handleControl)
	mux.HandleFunc("POST /api/shell", s.handleShell)

	// The whole mux behind the credential check, never a route at a time
	// (see tokenAuth). auth is nil for a unix bind: that transport's
	// boundary is the peer check the listener already applied.
	var handler http.Handler = mux
	if auth != nil {
		auth.next = mux
		handler = auth
	}

	s.httpSrv = &http.Server{
		Handler: handler,
		// ConnContext is what makes identifiedConn's id reachable from a
		// handler at all: net/http hands a handler the request context, not
		// the net.Conn, so without this hook handleControl could not reach
		// the id Accept assigned. c is exactly what this package's listener
		// returned; see connID for the one indirection TLS adds.
		ConnContext: func(ctx context.Context, c net.Conn) context.Context {
			if id, ok := connID(c); ok {
				return context.WithValue(ctx, clientIDContextKey{}, id)
			}
			return ctx
		},
	}
	if network == NetworkTCP {
		// Bounds how long a connection that has said nothing can hold a
		// goroutine (the cheapest DoS against a routable listener) and,
		// via net/http, the TLS handshake. Not applied to the unix
		// listener: nothing can open one without already running code as
		// this user. Request HEADERS only, so /api/stream's deliberately
		// long-lived body and a session's lifetime are untouched.
		s.httpSrv.ReadHeaderTimeout = requestHeaderTimeout
	}

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		// http.ErrServerClosed is Serve's signal that Close was deliberate;
		// anything else is a listener failure with no caller left to
		// report it to.
		_ = s.httpSrv.Serve(s.ln)
	}()

	if ctx != nil {
		go func() {
			select {
			case <-ctx.Done():
				_ = s.Close()
			case <-s.done:
			}
		}()
	}

	return s, nil
}

// requestHeaderTimeout bounds how long a TCP connection may take to finish
// sending its request headers, and with them its credential. Generous by
// design: it is not a latency target, only a bound on a connection that has
// said nothing at all.
const requestHeaderTimeout = 30 * time.Second

// listenUnix binds a unix socket: mode 0600, a stale-socket retry, and
// CheckPeer on every accepted connection. The two refusals at the top
// refuse rather than ignore: a caller who set a Token or a TLSConfig on a
// unix bind believes one of them is protecting something, and neither is.
// The peer check is.
func (s *Server) listenUnix(opts Options, peerCheck func(net.Conn) error) (net.Listener, error) {
	// len(...) > 0 rather than != "": no token may ever be an operand of Go's
	// own equality anywhere in this package's production code, and
	// TestTheTokenComparisonGoesThroughConstantTimeCompare enforces that
	// mechanically rather than by review. A length is not the secret.
	if len(opts.Token) > 0 {
		return nil, errors.New("attachsrv: a unix listener takes no Options.Token: " +
			"its boundary is the peer-credential check (CheckPeer) plus the socket's own 0600 mode in a 0700 " +
			"directory, not a bearer credential. Set Options.Network to tcp if a token is what you want")
	}
	if opts.TLSConfig != nil {
		return nil, errors.New("attachsrv: a unix listener takes no Options.TLSConfig: " +
			"a unix socket never leaves the machine, so there is no transport between here and the peer to encrypt")
	}

	ln, err := net.Listen("unix", opts.Bind)
	if err != nil && errors.Is(err, syscall.EADDRINUSE) && removeStaleSocket(opts.Bind) {
		ln, err = net.Listen("unix", opts.Bind)
	}
	if err != nil {
		return nil, fmt.Errorf("attachsrv: listen: %w", err)
	}
	// Mode 0600: defence in depth, not the boundary itself (a 0600 socket
	// still admits root; see CheckPeer). The actual guard is
	// peerCheckedListener below.
	if err := os.Chmod(opts.Bind, 0o600); err != nil {
		_ = ln.Close()
		return nil, fmt.Errorf("attachsrv: chmod socket: %w", err)
	}
	return &peerCheckedListener{
		Listener: ln,
		check:    peerCheck,
		rejected: &s.peerRejected,
	}, nil
}

// listenTCP binds a host:port, and enforces the two conditions under which
// that is defensible.
//
// The token is not optional: everything a unix socket got for free is gone
// here. No file mode, no 0700 directory, no peer credential (SO_PEERCRED
// answers a question about a process on this machine, so CheckPeer would
// be meaningless on TCP and is not wired in at all). The token is the
// boundary, on its own, so a listener without one does not start.
//
// TLS is not optional off loopback, and there is no flag: the token is a
// bearer credential that can cancel the run, skip steps and open
// POST /api/shell, and on plaintext it travels in a header, in the clear,
// on every request. An opt-in flag would go into a CI config once and be
// copied by people who never read its name; a refusal cannot be
// copy-pasted past, and the alternative (a loopback bind behind an SSH
// tunnel or port-forward) already supplies the transport security,
// authenticated, with no certificate to manage.
//
// Loopback WITHOUT TLS is allowed deliberately: loopback traffic never
// reaches a network, and capturing it needs privileges that already
// include reading this process's memory, where the token is anyway. What
// loopback plus a token does NOT reproduce is the unix socket's guarantee
// against another unprivileged local user, who can connect to a loopback
// port; against them the token alone stands, and attach's docs say so.
//
// The operator brings the certificate: a self-signed one generated here
// would encrypt without authenticating the server, handing the token to an
// active interceptor while looking like protection. Doing it honestly
// means pinning plus out-of-band delivery, a second
// credential-distribution mechanism this build does not invent.
func (s *Server) listenTCP(opts Options) (net.Listener, *tokenAuth, error) {
	if len(opts.Token) < minTokenLength {
		return nil, nil, fmt.Errorf("%w: got %d", ErrTokenRequired, len(opts.Token))
	}

	loopback, err := bindIsLoopback(opts.Bind)
	if err != nil {
		return nil, nil, err
	}
	if !loopback && opts.TLSConfig == nil {
		return nil, nil, fmt.Errorf(
			"%w: %q is reachable from off this machine, and the bearer token that guards it "+
				"(which can cancel the run, skip steps, and open an interactive session inside a step's "+
				"workspace) would travel in cleartext in every request header. There is no flag that "+
				"turns this off",
			ErrTLSRequired, opts.Bind)
	}

	raw, err := net.Listen("tcp", opts.Bind)
	if err != nil {
		return nil, nil, fmt.Errorf("attachsrv: listen: %w", err)
	}
	auth := &tokenAuth{
		guard:    newTokenGuard(opts.Token),
		credit:   newFailureCredit(),
		rejected: &s.authRejected,
	}
	return &identifiedListener{Listener: raw, tlsConfig: opts.TLSConfig}, auth, nil
}

// staleSocketProbeTimeout bounds removeStaleSocket's own connect attempt:
// unix socket connects succeed or fail essentially synchronously, so this
// is slack against a scheduling delay, not a realistic wait.
const staleSocketProbeTimeout = 250 * time.Millisecond

// removeStaleSocket unlinks path if it names a unix socket file with
// nothing actually listening any more, and reports whether it did. A
// hard-killed engine (SIGKILL, os.Exit, an unrecovered panic) skips
// UnixListener's own unlink, so the file persists with nothing behind it;
// listen calls this only after EADDRINUSE and retries the bind once.
//
// Safety: only a socket nothing answers on is removed. A path with a live
// listener behind it is left alone (the probe connects, is dropped, and
// the original bind failure surfaces). See
// TestListenDoesNotStealASocketFromALiveListener.
func removeStaleSocket(path string) bool {
	conn, err := net.DialTimeout("unix", path, staleSocketProbeTimeout)
	if err == nil {
		_ = conn.Close()
		return false // something is genuinely listening; not stale
	}
	// Any dial failure (refused, already removed, anything else) means no
	// live listener is using this path. A failed Remove is not special:
	// the original EADDRINUSE still surfaces from the retried Listen.
	return os.Remove(path) == nil
}

// PeerRejected reports how many connections peerCheckedListener has closed
// for failing the peer-credential check: without it, a rejected attach
// attempt is invisible from this side.
func (s *Server) PeerRejected() uint64 { return atomic.LoadUint64(&s.peerRejected) }

// AuthRejected reports how many requests tokenAuth has refused:
// PeerRejected's counterpart for the TCP transport, always 0 for a unix
// listener, which has no credential to present.
func (s *Server) AuthRejected() uint64 { return atomic.LoadUint64(&s.authRejected) }

// Network reports which transport this server bound, so a caller (attach's
// registry Entry, most of all) can describe which guard is standing.
func (s *Server) Network() string { return s.network }

// peerCheckedListener runs check against every accepted connection before
// it is handed to net/http: CheckPeer (peercred.go) is only the decision,
// this is what makes every connection go through it.
//
// A connection that fails check is closed and Accept loops to the next:
// returning an error would stop net/http's entire accept loop, turning one
// unauthorized attempt into a denial of service against the run's own
// operator. Rejections are counted (PeerRejected), not logged per
// occurrence, which an unauthenticated peer could exhaust for free by
// reconnecting.
type peerCheckedListener struct {
	net.Listener
	check    func(net.Conn) error
	rejected *uint64

	// nextClientID assigns each ACCEPTED connection a stable identity for
	// its lifetime; see identifiedConn.
	nextClientID uint64
}

func (l *peerCheckedListener) Accept() (net.Conn, error) {
	for {
		conn, err := l.Listener.Accept()
		if err != nil {
			// A real accept failure (most commonly the listener closing via
			// Server.Close): nothing to check, hand it up.
			return nil, err
		}
		if err := l.check(conn); err != nil {
			atomic.AddUint64(l.rejected, 1)
			_ = conn.Close()
			continue
		}
		id := atomic.AddUint64(&l.nextClientID, 1)
		return &identifiedConn{Conn: conn, id: fmt.Sprintf("c%d", id)}, nil
	}
}

// identifiedConn tags one accepted connection with a stable client
// identity for its whole lifetime: every request served over it carries
// the same id, which httpSrv.ConnContext stashes in each request's context
// and handleControl uses to attribute control.applied to "who did this"
// (sink.ControlRequest.ClientID). Not a second authentication mechanism:
// CheckPeer already decided acceptance, this only names it.
//
// Identity is per-CONNECTION, not per client process: two connections from
// one client get two ids. A stronger notion would need a session token,
// out of scope for this build; this still makes two DIFFERENT attached
// clients distinguishable, which is what control.applied needs.
//
// A monotonic counter, not a random token: the id is only compared for
// equality within one engine's own event stream and never authenticates
// anything, so unpredictability protects nothing and a counter is trivial
// to assert on.
type identifiedConn struct {
	net.Conn
	id string
}

// identifiedListener is peerCheckedListener's TCP counterpart: the same
// per-connection identity, no credential check, because none exists at
// accept time on TCP; the credential arrives in a request header and is
// checked there (see tokenAuth).
//
// Two types rather than one with a nil check: "check is nil" and "this
// transport has no check" would be the same value, and a mutation that
// nilled the unix listener's check would look like correct code. Here a
// unix bind holding an identifiedListener is a type error's worth of
// obvious; TestListenWiresTheRealPeerCheck asserts the concrete type.
type identifiedListener struct {
	net.Listener

	// tlsConfig, when non-nil, wraps every accepted connection in TLS. The
	// wrapping is deliberately OUTSIDE the identifiedConn: net/http detects
	// TLS by type-asserting the accepted value to *tls.Conn (handshake,
	// timeout, ALPN and r.TLS all hang off that), so a *tls.Conn hidden
	// inside a wrapper would never be handshaken. The id is recovered
	// through tls.Conn.NetConn; see connID.
	tlsConfig *tls.Config

	// nextClientID assigns each accepted connection a stable identity for
	// its lifetime; see identifiedConn.
	nextClientID uint64
}

func (l *identifiedListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	id := atomic.AddUint64(&l.nextClientID, 1)
	ic := &identifiedConn{Conn: conn, id: fmt.Sprintf("c%d", id)}
	if l.tlsConfig != nil {
		return tls.Server(ic, l.tlsConfig), nil
	}
	return ic, nil
}

// connID recovers the id this package's listeners assigned to c, unwrapping
// one layer of TLS to find it. Returns false for a connection neither
// listener produced, which cannot happen in production and does happen in a
// test that builds its own.
func connID(c net.Conn) (string, bool) {
	switch v := c.(type) {
	case *identifiedConn:
		return v.id, true
	case *tls.Conn:
		if inner := v.NetConn(); inner != nil {
			return connID(inner)
		}
	}
	return "", false
}

// Addr is the address the server is listening on: the unix socket path for a
// unix bind, or the resolved host:port (with the real port, even when the
// bind asked for :0) for a TCP one.
func (s *Server) Addr() string { return s.addr }

// Close shuts the server down: stops accepting, force-closes existing
// connections (a graceful drain would wait out /api/stream responses that
// are deliberately held open), and waits for every tracked /api/stream
// handler goroutine, so a caller that Closes and then tears down the Hub
// never races a stream write still in flight.
//
// One case is NOT force-closed blind: when the Hub is ALREADY closed
// (exactly what attach.Attach.Close does, closing the hub before the
// server), every tracked stream handler is about to observe its hub
// channel closed, and Close gives them a bounded chance
// (streamWriteTimeout) to write their terminal marker first. Closing
// s.done ahead of that would race each handler's select between an
// informative closed-hub case and a silent s.done case, and select breaks
// ties uniformly at random. See
// TestStreamMarkerSurvivesACloseRaceAgainstAnAlreadyClosedHub. When the
// Hub is not closed, the pre-drain is skipped and Close proceeds as
// always: immediate signal, immediate force-close.
//
// Only /api/stream's goroutines are tracked; the short-lived handlers end
// promptly on their own, because force-closing their connection cancels
// r.Context(), which every blocking wait in handleControl selects on.
//
// Close is idempotent and safe to call more than once.
func (s *Server) Close() error {
	s.closeOnce.Do(func() {
		// Committed under the same lock track() checks, strictly before any
		// wg.Wait below, so no Add can race a Wait (see track). Set before
		// the pre-drain so no NEW stream can join streamWG while the drain
		// is underway.
		s.mu.Lock()
		s.closing = true
		s.mu.Unlock()

		if s.hub.Closed() {
			drained := make(chan struct{})
			go func() {
				s.streamWG.Wait()
				close(drained)
			}()
			select {
			case <-drained:
			case <-time.After(streamWriteTimeout):
			}
		}

		close(s.done)
	})
	// Hijacked session connections, which httpSrv.Close below will not touch:
	// see closeShellConns. Closing them is also what ends the sessions behind
	// them, since the engine reads a failed read as its client disconnecting.
	s.closeShellConns()
	err := s.httpSrv.Close()
	s.wg.Wait()
	return err
}

// --- GET /api/state ---

func (s *Server) handleState(w http.ResponseWriter, r *http.Request) {
	st := s.hub.State()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(st)
}

// --- GET /api/plan ---

func (s *Server) handlePlan(w http.ResponseWriter, r *http.Request) {
	b, err := os.ReadFile(filepath.Join(s.dir, "plan.json"))
	if err != nil {
		if os.IsNotExist(err) {
			http.Error(w, "attachsrv: plan.json not found", http.StatusNotFound)
			return
		}
		http.Error(w, "attachsrv: "+err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(b)
}

// --- GET /api/logs/{step}?attempt=&stream=&from= ---

// handleLogs is registered on the "{step}" wildcard pattern. Step IDs
// contain "/", percent-encoded into a single segment via stepid.Encode.
// Go's ServeMux (1.22+) matches wildcards against the escaped path, so
// r.PathValue("step") comes back with the "/" preserved and %2F decoded
// only within that captured segment: no manual raw-path handling, and no
// SECOND decode either. r.PathValue is already fully unescaped, so running
// it through stepid.Decode again would error on a real "%" and silently
// corrupt a name containing the literal text "%2F". See
// TestLogsHandlesAStepIDContainingASlash and
// TestLogsHandlesAStepIDContainingAPercent.
func (s *Server) handleLogs(w http.ResponseWriter, r *http.Request) {
	step := r.PathValue("step")

	q := r.URL.Query()

	attempt := 1
	if v := q.Get("attempt"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			http.Error(w, "attachsrv: bad attempt", http.StatusBadRequest)
			return
		}
		attempt = n
	}

	// stream is joined as LogSet.Path's final path element, so anything
	// but an exact match against the two real streams is a file-read
	// primitive: an unchecked stream=../../../../etc/passwd resolves
	// outside the run directory (see
	// TestLogsRejectsPathTraversalViaStream). ReadOnly does not apply: it
	// gates POST /api/control, and a file read was never a control op.
	stream := q.Get("stream")
	if stream == "" {
		stream = api.StreamStdout
	}
	switch stream {
	case api.StreamStdout, api.StreamStderr:
	default:
		http.Error(w, "attachsrv: unknown stream", http.StatusBadRequest)
		return
	}

	var from int64
	if v := q.Get("from"); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			http.Error(w, "attachsrv: bad from", http.StatusBadRequest)
			return
		}
		from = n
	}

	path := eventlog.NewLogSet(s.dir).Path(step, attempt, stream)

	// The allowlist above closes the stream parameter; this closes the
	// class: whatever the segments were, the resolved path must land
	// inside the run's log tree before it is opened. Not redundant: it
	// catches step itself. stepid.Encode does not escape ".", so a step
	// id decoding to ".." reaches LogSet.Path as a literal ".." element
	// and Clean resolves it one directory above logs/. filepath.Abs calls
	// Clean, so the check must come after that resolution, or it would
	// compare an unresolved path against a resolved one.
	root, err := filepath.Abs(filepath.Join(s.dir, "logs"))
	if err != nil {
		http.Error(w, "attachsrv: "+err.Error(), http.StatusInternalServerError)
		return
	}
	resolved, err := filepath.Abs(path)
	if err != nil {
		http.Error(w, "attachsrv: "+err.Error(), http.StatusInternalServerError)
		return
	}
	if resolved != root && !strings.HasPrefix(resolved, root+string(filepath.Separator)) {
		http.Error(w, "attachsrv: step id escapes the log tree", http.StatusBadRequest)
		return
	}

	f, err := os.Open(resolved)
	if err != nil {
		http.NotFound(w, r)
		return
	}
	// Read-only; a close error cannot lose data this handler wrote.
	defer func() { _ = f.Close() }()

	// Matches source.FileSource.Logs: only seek for a strictly positive
	// offset, so from=0 just reads from the start.
	if from > 0 {
		if _, err := f.Seek(from, io.SeekStart); err != nil {
			http.Error(w, "attachsrv: "+err.Error(), http.StatusInternalServerError)
			return
		}
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	_, _ = io.Copy(w, f)
}

// --- GET /api/stream?from= ---

// overflowBody is the 410 Gone body when the requested fromSeq has been
// evicted from the hub's ring: api.OverflowBody, the published wire shape,
// not a private hand-maintained copy (see api/streamend.go).
var overflowBody = api.OverflowBody{
	Error: api.OverflowError,
	Hint:  "GET /api/state for a fresh snapshot, then GET /api/stream?from=<state.seq+1>",
}

// streamEndRunEnded and streamEndOverflowed name precisely why a stream
// ended; the two cases are otherwise byte-identical on the wire, a bare
// closed channel either way:
//
//   - streamEndRunEnded: the run finished or the hub closed. Nothing more
//     will ever stream for this run; stop.
//   - streamEndOverflowed: this subscriber fell behind the hub's ring
//     while the hub kept running for everyone else. Resubscribe from
//     last_seq+1, or re-snapshot via /api/state.
//
// A THIRD reason, api.StreamEndWriteStalled, is never sent by this server:
// after a failed write there is no connection left to reach (case 3 in
// handleStream), so sending it would be dead code. Clients must still
// tolerate the string; see internal/source's reasonWriteStalled.
const (
	streamEndRunEnded   = api.StreamEndRunEnded
	streamEndOverflowed = api.StreamEndOverflowed
)

// streamEndMarker is api.StreamEndMarker: the terminal NDJSON line
// handleStream writes when it gives up for a reason the hub's own channel
// closing does not itself explain; never on a plain disconnect or a server
// shutdown. Deliberately NOT api.Event-shaped (StreamEnd is a field no
// api.Event carries, so a client recognises it without guessing from a
// Type value), and a type alias of the published wire type rather than a
// hand copy that could drift.
//
// LastSeq is the seq of the last event this connection actually delivered;
// 0 also means "never delivered anything", so do not derive a resume point
// as LastSeq+1 unconditionally: use Hint's pairing, correct in every case.
//
// Overflowed is kept for wire compatibility with clients that predate
// Reason (true iff Reason == streamEndOverflowed); prefer Reason, which
// can express values this bool cannot. It is a heuristic: true only when
// LastSeq > 0 AND a fresh Hub.Seq() exceeds it. The LastSeq>0 guard keeps
// a subscriber that received exactly what it asked for (nothing) from
// being reported as overflowed (see
// TestStreamOverflowedIsFalseWhenNothingWasEverDelivered); when it says
// true it is sound, because an overflow-disconnect only closes a NON-empty
// channel, so at least one delivery preceded it.
//
// Reason is a plain string so an older client still decodes a future
// value, the stance api.Event.Type takes; always set. Hint always names
// the resume pairing every Source uses (/api/state, then
// /api/stream?from=<state.seq+1>), and /api/state's Run.Done is how a
// client tells "the run ended" apart from "reconnect".
type streamEndMarker = api.StreamEndMarker

// handleStream streams NDJSON (one api.Event per line, flushed after each)
// starting at fromSeq. Four things can end the response, and only one of
// them writes a terminal line first:
//
//  1. The client disconnects (r.Context().Done()): nothing to write to.
//  2. The server is shutting down (s.done): same, from this connection's
//     point of view.
//  3. A write blocks past streamWriteTimeout and errors: a fact about
//     THIS connection failing to drain, not about the engine, and silent,
//     because the failed write has already made the connection unusable
//     for a second one. What protects a merely-slow client is downstream:
//     FallbackSource's relay (internal/source/fallback.go) treats a
//     markerless close like a delivered "write_stalled": reconnect,
//     bounded, never fall back on it alone.
//  4. The hub's own channel closes: either Hub.Close or Emit's overflow
//     guard disconnecting THIS subscriber. The two look identical as a
//     bare closed channel, and without a marker line a `for e := range
//     ch` client would just stop with a truncated fold; this is the one
//     case where a terminal streamEndMarker reliably reaches the client.
//
// In every case the resume algorithm is the same: GET /api/state, then, if
// the run is not Done, GET /api/stream?from=<state.seq+1>. Only case 4
// lets a client tell from THIS response that it needs to; 1, 2 and 3 are
// indistinguishable, which is exactly why FallbackSource treats any
// markerless close the same way.
//
// Lifecycle events are never dropped (see the Hub doc): if fromSeq asks
// for history the ring already evicted, Subscribe reports
// ErrLifecycleOverflow up front and this handler responds 410 Gone before
// any NDJSON body, rather than silently starting at a later seq.
func (s *Server) handleStream(w http.ResponseWriter, r *http.Request) {
	fromSeq, err := parseUint64Param(r, "from")
	if err != nil {
		http.Error(w, "attachsrv: bad from", http.StatusBadRequest)
		return
	}

	ch, cancel, err := s.hub.Subscribe(fromSeq)
	if err != nil {
		switch {
		case errors.Is(err, ErrLifecycleOverflow):
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusGone)
			_ = json.NewEncoder(w).Encode(overflowBody)
		case errors.Is(err, ErrClosed):
			http.Error(w, "attachsrv: the run's hub is closed", http.StatusServiceUnavailable)
		default:
			http.Error(w, "attachsrv: "+err.Error(), http.StatusInternalServerError)
		}
		return
	}
	defer cancel()

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "attachsrv: streaming not supported", http.StatusInternalServerError)
		return
	}
	rc := http.NewResponseController(w)

	// Tracked from here on: everything above returns almost immediately,
	// while from here the handler can run for the connection's lifetime.
	// track (not a bare wg.Add) keeps this from racing a concurrent Close;
	// false means Close has committed, so refuse rather than start a
	// stream Close would have to wait out.
	if !s.track() {
		http.Error(w, "attachsrv: server is shutting down", http.StatusServiceUnavailable)
		return
	}
	defer s.untrack()

	w.Header().Set("Content-Type", "application/x-ndjson")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	enc := json.NewEncoder(w) // Encode writes a trailing '\n' after each value: exactly NDJSON.
	var lastSeq uint64

	// handleEvent processes one receive from ch (a regular event, or the
	// hub channel closing: case 4) and reports whether the stream is over.
	// Shared by the loop's select and the disconnect-path recheck.
	handleEvent := func(e api.Event, ok bool) (streamOver bool) {
		if !ok {
			// Case 4: write the terminal marker so a `for e := range ch`
			// client gets a definite signal instead of a silently truncated
			// fold. Best-effort: a failed marker write has nothing further
			// to try. The write is tracked, so a later Server.Close can be
			// held up to streamWriteTimeout by a wedged client's deadline;
			// the force-close of active connections usually unblocks it
			// sooner.
			//
			// lastSeq > 0 excludes a connection that never delivered
			// anything from being reported as overflowed (see
			// TestStreamOverflowedIsFalseWhenNothingWasEverDelivered).
			// Hub.Seq(), not State().Seq: no reason to clone a whole
			// RunState for one field.
			overflowed := lastSeq > 0 && s.hub.Seq() > lastSeq
			reason := streamEndRunEnded
			if overflowed {
				reason = streamEndOverflowed
			}
			_ = rc.SetWriteDeadline(time.Now().Add(streamWriteTimeout))
			_ = enc.Encode(streamEndMarker{
				StreamEnd:  true,
				LastSeq:    lastSeq,
				Overflowed: overflowed,
				Reason:     string(reason),
				Hint:       "GET /api/state for a fresh snapshot; if the run is not yet done, GET /api/stream?from=<state.seq+1>",
			})
			flusher.Flush()
			return true
		}
		// Refreshed before every write, not set once: a slow-but-alive
		// client is judged on whether it is stalling NOW, not on a burst
		// earlier in the stream. See streamWriteTimeout.
		_ = rc.SetWriteDeadline(time.Now().Add(streamWriteTimeout))
		if err := enc.Encode(e); err != nil {
			// Case 3: this connection did not drain in time. A second
			// write (naming write_stalled) would never arrive: net/http
			// tears the connection down on any write error essentially
			// synchronously. The real protection for a slow client is
			// FallbackSource's relay treating this markerless close like a
			// delivered write_stalled: reconnect, bounded.
			return true
		}
		lastSeq = e.Seq
		flusher.Flush()
		return false
	}

	for {
		select {
		case e, ok := <-ch:
			if handleEvent(e, ok) {
				return
			}
		case <-r.Context().Done():
			// The hub channel may have closed at the same instant as this
			// disconnect: select breaks ties uniformly at random, and once
			// attach.Attach.Close closes the hub before the server, both
			// cases can be ready together; losing the tie would silently
			// discard the run_ended marker. The recheck CLOSES that window
			// rather than narrowing it: a channel close is permanent, so
			// if ch closed before or during this select, this non-blocking
			// read observes it, whichever case the select woke on; it
			// cannot block or false-positive. See
			// TestCloseDeliversRunEndedToARealAttachedClient and
			// TestStreamMarkerSurvivesACloseRaceAgainstAnAlreadyClosedHub.
			if e, ok, ready := tryRecvEvent(ch); ready {
				handleEvent(e, ok)
			}
			return
		case <-s.done:
			// Same race, same fix, as the r.Context().Done() case above:
			// s.done can become ready alongside an already-closed ch once
			// Attach.Close() closes the hub before the server.
			if e, ok, ready := tryRecvEvent(ch); ready {
				handleEvent(e, ok)
			}
			return
		}
	}
}

// tryRecvEvent performs one non-blocking receive on ch, with an explicit
// ready bool: ok alone cannot distinguish "the channel was empty" from "it
// delivered a zero-value close", which handleStream's disconnect cases
// must tell apart before returning silently.
func tryRecvEvent(ch <-chan api.Event) (e api.Event, ok bool, ready bool) {
	select {
	case e, ok = <-ch:
		return e, ok, true
	default:
		return api.Event{}, false, false
	}
}

func parseUint64Param(r *http.Request, name string) (uint64, error) {
	v := r.URL.Query().Get(name)
	if v == "" {
		return 0, nil
	}
	return strconv.ParseUint(v, 10, 64)
}

// --- POST /api/control ---

// clientIDContextKey is the unexported key ConnContext stashes a
// connection's id under. An unexported struct type, not a string, so no
// other package's context value can collide with it.
type clientIDContextKey struct{}

// clientIDFromContext reads back the id ConnContext attached, or "" for a
// connection this listener did not itself accept (never in production).
func clientIDFromContext(ctx context.Context) string {
	id, _ := ctx.Value(clientIDContextKey{}).(string)
	return id
}

// maxControlArgsBytes bounds a control request's Payload, tighter than the
// whole-body 64KiB: no op this build recognises needs more than a few
// hundred bytes. Checked before json.Unmarshal, so an oversized payload is
// refused without paying to decode it, and (with controlArgAllowlist) it
// bounds what a single control request can contribute to the permanent
// ledger via control.applied.
const maxControlArgsBytes = 1024

// controlArgAllowlist names, per op, the only Args keys handleControl will
// forward into a sink.ControlRequest and, from there, into a permanent,
// broadcast control.applied event. Anything else is refused before it is
// decoded into Args, and so before it can reach the ledger: the event log
// is a permanent, shared artifact, and the payload is client-supplied JSON.
//
// An allow-list, not a denylist or a sanitiser: a denylist fails on "the
// one secret-shaped key nobody thought to name", and a sanitiser still
// lets arbitrary bytes into the ledger. run.cancel's entry is the EMPTY
// set: the op takes no arguments, and a request that sends any is refused,
// not silently stripped.
//
// internal/engine/control.go does not trust this map alone: it
// reconstructs Args from already-validated data rather than forwarding
// req.Args verbatim. Both layers exist on purpose.
var controlArgAllowlist = map[string]map[string]bool{
	api.OpRunCancel:       {},
	api.OpStepRetry:       {"step": true},
	api.OpStepSkip:        {"step": true},
	api.OpBreakpointSet:   {"step": true},
	api.OpBreakpointClear: {"step": true},
	api.OpRunRerunFrom:    {"step": true},
	api.OpWSSnapshot:      {"step": true},

	// Both empty, like run.cancel's: a pause is run-wide, so there is no
	// step for it to name.
	api.OpRunPause:  {},
	api.OpRunResume: {},

	// The analysis operations take an id, never a step: a client that could
	// name the step could retry any step in the plan by calling it a
	// proposal. See handleAnalysisAccept, which rewrites Args rather than
	// merging them.
	api.OpAnalysisAccept: {"id": true},
	api.OpAnalysisReject: {"id": true},
}

// handleControl decodes one api.Frame request, forwards it to the hub's
// control channel, waits for the correlated reply, and encodes it back:
// control is request/response by nature. The engine's scheduler is the
// consumer on the other end (internal/engine/control.go); this handler
// decodes, refuses a malformed or over-argumented request, attributes it
// to its connection, answers immediately when the hub already knows
// nothing will read it, and otherwise forwards.
func (s *Server) handleControl(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maxControlBodyBytes)

	var req api.Frame
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "attachsrv: bad request frame: "+err.Error(), http.StatusBadRequest)
		return
	}

	if s.readOnly {
		writeControlFrame(w, http.StatusForbidden, api.Frame{
			V: api.Version, Kind: api.KindRes, ID: req.ID,
			OK: falsePtr(), Error: ErrReadOnly.Error(),
		})
		return
	}

	// Args carries a step id for step.retry and is empty for an op that
	// needs none. A malformed or oversized payload is a client error
	// (400), not a control-plane refusal (200 with ok:false).
	var args map[string]string
	if len(req.Payload) > 0 {
		if len(req.Payload) > maxControlArgsBytes {
			http.Error(w, "attachsrv: control payload too large", http.StatusBadRequest)
			return
		}
		if err := json.Unmarshal(req.Payload, &args); err != nil {
			http.Error(w, "attachsrv: bad control payload: "+err.Error(), http.StatusBadRequest)
			return
		}
		// The key name is never echoed back in the refusal: that would
		// make this response a same-size reflection of client-chosen
		// bytes, the amplification maxControlBodyBytes exists to prevent.
		if allowed, known := controlArgAllowlist[req.Type]; known {
			for k := range args {
				if !allowed[k] {
					http.Error(w, "attachsrv: this operation does not accept that argument", http.StatusBadRequest)
					return
				}
			}
		}
	}

	// The hub already knows nothing is left to act on a request once the
	// run is terminal or the hub closed (see Hub.Done). Answering here,
	// before touching the channel, keeps a late request from queuing for a
	// reader that no longer exists; the residual race (the run finishing
	// between this check and the send) is closed by the engine's own
	// durable post-schedule refusal goroutine (internal/engine/control.go).
	if s.hub.Done() {
		writeControlFrame(w, http.StatusOK, api.Frame{
			V: api.Version, Kind: api.KindRes, ID: req.ID,
			OK: falsePtr(), Error: sink.ReasonRunFinished,
		})
		return
	}

	reply := make(chan sink.ControlResponse, 1)
	creq := sink.ControlRequest{
		ID: req.ID, Op: req.Type, Args: args, Reply: reply,
		ClientID: clientIDFromContext(r.Context()),
	}

	// Hub.Control() is typed <-chan so Hub satisfies sink.Sink, which the
	// ENGINE reads from; the writer is this handler, reaching the hub's
	// own channel directly since the two files share a package.
	select {
	case s.hub.control <- creq:
	case <-r.Context().Done():
		return
	case <-time.After(controlTimeout):
		http.Error(w, "attachsrv: control request queue did not accept the request in time", http.StatusServiceUnavailable)
		return
	}

	select {
	case resp := <-reply:
		ok := resp.OK
		writeControlFrame(w, http.StatusOK, api.Frame{
			V: api.Version, Kind: api.KindRes, ID: resp.ID,
			OK: &ok, Error: resp.Error,
		})
	case <-r.Context().Done():
	case <-time.After(controlTimeout):
		http.Error(w, "attachsrv: control response timed out", http.StatusGatewayTimeout)
	}
}

func writeControlFrame(w http.ResponseWriter, status int, f api.Frame) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(f)
}

func falsePtr() *bool { b := false; return &b }
