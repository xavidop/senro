// Package webui serves senro's browser UI: a page, a Go client compiled to
// WebAssembly, and a view onto one live run that an operator can also
// steer.
//
// The browser never talks to the attach server. This package binds its own
// loopback listener and forwards four read-only routes plus one control
// route, adding the run's bearer token on the way through; the page never
// sees that token (session.go has the reasoning, proxy.go the allowlist).
// The forwarded routes are byte-for-byte the attach protocol, and the
// client folds every event with api.RunState.Apply through internal/tail:
// no second protocol, no second fold.
//
// The client is WebAssembly rather than JavaScript because
// api.RunState.Apply is 320 lines of rules a JS reimplementation would get
// subtly wrong on the first try; api is standard-library only so a WASM
// client can import it. The cost is ~3.6MB of WASM (1.0MB gzipped), mostly
// Go runtime and encoding/json. Linking net/http would have tripled that
// just to reach fetch, so the client calls fetch directly through
// syscall/js and gets its HTTP semantics from internal/tail, tested on the
// host against a real attach server.
package webui

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

// Options configures Listen.
type Options struct {
	// Bind is the loopback address to serve on. Empty means 127.0.0.1:0,
	// which takes an ephemeral port; Addr reports what was actually bound.
	//
	// Loopback only, with no flag to change it. See Listen.
	Bind string
	// Upstream is the attach server this UI reads from.
	Upstream Upstream
}

// Server is the browser UI's own HTTP server.
type Server struct {
	upstream     Upstream
	upstreamBase string
	client       *http.Client
	// controlClient forwards POST /api/control, and is separate from client
	// only because it waits longer for response headers. See
	// controlHeaderBudget.
	controlClient *http.Client

	ln      net.Listener
	httpSrv *http.Server
	addr    string

	// allowedHosts is every Host header value this server answers to,
	// computed once at bind time. See checkHost.
	allowedHosts map[string]bool

	session credential
	handoff handoff

	assets *bundle

	mu     sync.Mutex
	closed bool
	// serveErr carries the Serve goroutine's own failure, so Wait can
	// report a listener that died rather than returning as though the
	// caller had asked for shutdown.
	serveErr error
	done     chan struct{}
}

// ErrBundleMissing reports that this build has no compiled WebAssembly
// client to serve: the client is built by `make wasm` into an embedded
// directory (see bundle), and a tree that never ran it can do everything
// except this.
var ErrBundleMissing = errors.New("webui: this build carries no compiled WebAssembly client")

// Listen binds the UI server and verifies it can reach the run.
//
// Loopback only, with no option to change it: a routable bind would put a
// live build's view, guarded only by a session cookie sent over plaintext
// HTTP, on the network. The attach server holds the same rule
// (attachsrv.ErrTLSRequired). Reach a run on another machine by forwarding
// the port (ssh -L), where credential and traffic are both protected.
//
// The upstream is contacted once, here, so an operator learns of an
// unreachable engine or a wrong token from their terminal, before a
// browser tab is opened on it.
func Listen(ctx context.Context, opts Options) (*Server, error) {
	bundle, err := loadBundle()
	if err != nil {
		return nil, err
	}
	return listen(ctx, opts, bundle)
}

// listen is Listen with the asset bundle supplied rather than loaded: the
// seam this package's tests bind through, so the suite is meaningful in a
// checkout that has not run `make wasm`. The bundle's own existence is
// tested as its own question.
func listen(ctx context.Context, opts Options, bundle *bundle) (*Server, error) {
	bind := opts.Bind
	if bind == "" {
		bind = "127.0.0.1:0"
	}
	loopback, err := bindIsLoopback(bind)
	if err != nil {
		return nil, err
	}
	if !loopback {
		return nil, fmt.Errorf(
			"webui: %q is not a loopback address, and the browser UI binds nothing else: "+
				"it would put a live run's view, and the session cookie that opens it, on the network in plaintext. "+
				"Reach a run on another machine by forwarding its port (ssh -L) and pointing a local `senro ui` at that", bind)
	}

	session, err := newCredential()
	if err != nil {
		return nil, fmt.Errorf("webui: generating a session credential: %w", err)
	}
	nonce, err := newCredential()
	if err != nil {
		return nil, fmt.Errorf("webui: generating a handoff nonce: %w", err)
	}

	s := &Server{
		upstream:      opts.Upstream,
		upstreamBase:  upstreamBase(opts.Upstream),
		client:        newUpstreamClient(opts.Upstream),
		controlClient: newUpstreamClientWithBudget(opts.Upstream, controlHeaderBudget),
		session:       session,
		assets:        bundle,
		done:          make(chan struct{}),
	}
	s.handoff.cred = nonce

	if err := s.checkUpstream(ctx); err != nil {
		return nil, err
	}

	ln, err := net.Listen("tcp", bind)
	if err != nil {
		return nil, fmt.Errorf("webui: binding %s: %w", bind, err)
	}
	s.ln = ln
	s.addr = ln.Addr().String()
	s.allowedHosts = allowedHostsFor(s.addr)

	s.httpSrv = &http.Server{
		Handler: s.routes(),
		// A browser that opens a connection and says nothing must not hold
		// a slot forever. WriteTimeout stays unset on purpose: GET
		// /api/stream stays open for the life of the run.
		ReadHeaderTimeout: 10 * time.Second,
	}
	go func() {
		err := s.httpSrv.Serve(ln)
		s.mu.Lock()
		if !s.closed && !errors.Is(err, http.ErrServerClosed) {
			s.serveErr = err
		}
		s.mu.Unlock()
		close(s.done)
	}()
	return s, nil
}

// Addr is the host:port actually bound.
func (s *Server) Addr() string { return s.addr }

// URL is the one-time address an operator opens, carrying the handoff
// nonce that becomes a session cookie. It is valid exactly once; see
// handoff's own doc for what that buys and what it does not.
func (s *Server) URL() string {
	return "http://" + s.addr + handoffPrefix + s.handoff.cred.value
}

// Wait blocks until the server stops, and reports why if it was not asked
// to.
func (s *Server) Wait() error {
	<-s.done
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.serveErr
}

// Close shuts the server down. Idempotent.
func (s *Server) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	s.mu.Unlock()

	err := s.httpSrv.Close()
	s.client.CloseIdleConnections()
	s.controlClient.CloseIdleConnections()
	<-s.done
	return err
}

func (s *Server) routes() http.Handler {
	mux := http.NewServeMux()

	// The handoff is the only route reachable without a session, because it
	// is the route that creates one.
	mux.HandleFunc("GET "+handoffPrefix+"{nonce}", s.guard(s.handleHandoff))

	mux.HandleFunc("GET /{$}", s.guard(s.requireSession(s.handleIndex)))
	mux.HandleFunc("GET "+assetPrefix+"{name}", s.guard(s.requireSession(s.handleAsset)))

	// Every forwarded read route, registered one at a time, GET only: a
	// prefix registration would route every method and sibling path to the
	// read handler, making POST /api/shell reachable without anybody
	// deciding it should be.
	for _, p := range readableRoutes {
		mux.HandleFunc("GET "+p, s.guard(s.requireSession(s.handleAPI)))
	}
	mux.HandleFunc("GET "+logRoutePrefix+"{step}", s.guard(s.requireSession(s.handleAPI)))

	// The one route that acts: POST alone, behind the same guards and
	// session requirement, plus an origin check the others do not carry
	// (see control.go). POST /api/shell has no registration and no handler
	// anywhere in this package: the difference between a page that can
	// steer a run and a page that can run commands on the machine.
	mux.HandleFunc("POST /api/control", s.guard(s.requireSession(s.handleControl)))

	// A catch-all so an unknown path gets the same guarded, header-carrying
	// refusal as a failed check: net/http's own 404 would answer without
	// the CSP and Referrer-Policy stated on every other response. It also
	// catches every unserved method and unforwarded attach endpoint, POST
	// /api/shell among them.
	mux.HandleFunc("/", s.guard(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, refusalBody, http.StatusNotFound)
	}))

	return mux
}

// guard applies the checks that hold for every route, including any added
// later: a check registered per route is a check somebody forgets on the
// next one (attachsrv's tokenAuth makes the same argument).
func (s *Server) guard(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		setSecurityHeaders(w)
		if !s.checkHost(r) {
			// Fixed refusal: a caller under a hostname this server does not
			// answer to learns nothing about what is here.
			http.Error(w, refusalBody, http.StatusNotFound)
			return
		}
		if !checkFetchSite(r) {
			http.Error(w, refusalBody, http.StatusNotFound)
			return
		}
		next(w, r)
	}
}

// requireSession refuses anything without this server's session cookie.
func (s *Server) requireSession(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !s.session.accepts(presentedSession(r)) {
			http.Error(w, "senro ui: this page needs the one-time link `senro ui` printed. "+
				"Open that link again, or restart `senro ui` for a new one.\n", http.StatusForbidden)
			return
		}
		next(w, r)
	}
}

// refusalBody is what any failed boundary check gets, byte for byte. It
// names nothing: a caller arriving by DNS rebinding or from the open
// internet should not learn they found a senro engine (attachsrv's
// unauthorizedBody makes the same argument).
const refusalBody = "not found\n"

// setSecurityHeaders writes the headers that hold for every response.
func setSecurityHeaders(w http.ResponseWriter) {
	h := w.Header()
	// default-src 'none' denies every fetch not named below.
	// wasm-unsafe-eval permits WebAssembly.instantiate, not JS eval. No
	// 'unsafe-inline' anywhere: the page's bootstrap is a file, not a
	// <script> block.
	h.Set("Content-Security-Policy",
		"default-src 'none'; "+
			"script-src 'self' 'wasm-unsafe-eval'; "+
			"style-src 'self'; "+
			"connect-src 'self'; "+
			"img-src 'self' data:; "+
			"base-uri 'none'; "+
			"form-action 'none'; "+
			"frame-ancestors 'none'")
	// No Referer on anything this page requests: keeps the handoff nonce
	// out of a header.
	h.Set("Referrer-Policy", "no-referrer")
	h.Set("X-Content-Type-Options", "nosniff")
	h.Set("X-Frame-Options", "DENY")
}

// checkHost is the defence against DNS rebinding. Same-origin policy is
// enforced on the ORIGIN, a name and a port: an attacker who repoints
// their domain at 127.0.0.1 after the browser loaded their page shares an
// origin with this server, and every cross-origin protection stops
// applying. The Host header still carries the attacker's name, so
// answering only to the loopback names this server was bound under closes
// that.
func (s *Server) checkHost(r *http.Request) bool {
	return s.allowedHosts[strings.ToLower(r.Host)]
}

// allowedHostsFor builds the Host header allowlist for a bound address: the
// literal address, plus the loopback names that resolve to it and that a
// person or a printed URL might legitimately use.
func allowedHostsFor(addr string) map[string]bool {
	hosts := map[string]bool{strings.ToLower(addr): true}
	_, port, err := net.SplitHostPort(addr)
	if err != nil {
		return hosts
	}
	for _, h := range []string{"localhost", "127.0.0.1", "[::1]"} {
		hosts[h+":"+port] = true
	}
	return hosts
}

// checkFetchSite refuses a request the browser says came from somewhere
// else. Sec-Fetch-Site cannot be forged by a page: "none" is a top-level
// navigation (the handoff link), "same-origin" is this page's own fetches,
// "cross-site"/"same-site" is another page reaching in. Absent means a
// non-browser client (curl): allowed through, because the SameSite=Strict
// session cookie is still required and another page cannot cause it to be
// sent. A second lock, not the only one.
func checkFetchSite(r *http.Request) bool {
	switch r.Header.Get("Sec-Fetch-Site") {
	case "", "none", "same-origin":
		return true
	default:
		return false
	}
}

// handleHandoff spends the one-time nonce and issues the session cookie.
// The redirect drops the nonce from the URL, so the settled page and any
// screenshot of the address bar carry no credential.
func (s *Server) handleHandoff(w http.ResponseWriter, r *http.Request) {
	if !s.handoff.claim(r.PathValue("nonce")) {
		// A spent nonce and a wrong one get the same fixed answer: whether
		// a link has been used is not confirmed to somebody without it.
		http.Error(w, refusalBody, http.StatusNotFound)
		return
	}
	setSession(w, s.session.value)
	w.Header().Set("Cache-Control", "no-store")
	http.Redirect(w, r, "/", http.StatusSeeOther)
}

func (s *Server) handleIndex(w http.ResponseWriter, r *http.Request) {
	s.assets.serve(w, r, "index.html")
}

func (s *Server) handleAsset(w http.ResponseWriter, r *http.Request) {
	s.assets.serve(w, r, r.PathValue("name"))
}

func (s *Server) handleAPI(w http.ResponseWriter, r *http.Request) {
	if !forwardable(r.URL.Path) {
		// Unreachable through the mux as registered, checked anyway: this
		// is the one function that can put a request in front of the
		// engine, and a mis-registered future route must not be how that
		// happens.
		http.Error(w, refusalBody, http.StatusNotFound)
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	s.forward(w, r)
}

// bindIsLoopback reports whether a host:port bind names an address nothing
// off this machine can reach. Mirrors attachsrv's check, edge cases
// included: the empty host is the wildcard bind (every interface), and a
// NAME counts as loopback only if every address it resolves to is, so a
// hosts file cannot talk this into treating a routable address as local.
func bindIsLoopback(bind string) (bool, error) {
	host, _, err := net.SplitHostPort(bind)
	if err != nil {
		return false, fmt.Errorf("webui: %q is not a host:port address: %w", bind, err)
	}
	if host == "" {
		return false, nil
	}
	if ip := net.ParseIP(host); ip != nil {
		return ip.IsLoopback(), nil
	}
	addrs, err := net.LookupHost(host)
	if err != nil {
		return false, fmt.Errorf("webui: resolving bind host %q: %w", host, err)
	}
	if len(addrs) == 0 {
		return false, nil
	}
	for _, a := range addrs {
		ip := net.ParseIP(a)
		if ip == nil || !ip.IsLoopback() {
			return false, nil
		}
	}
	return true, nil
}
