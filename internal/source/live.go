package source

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"

	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/internal/attachsrv"
	"github.com/xavidop/senro/internal/ndjson"
	"github.com/xavidop/senro/internal/stepid"
)

// ErrOverflow reports that Subscribe's fromSeq is older than what the
// server's hub still retains (attachsrv.ErrLifecycleOverflow on the wire:
// GET /api/stream answers 410 Gone). Remedy: fetch a fresh State() and
// Subscribe from state.Seq+1.
var ErrOverflow = errors.New("source: fromSeq is older than the server's retained lifecycle ring")

// StreamEnd.Reason values, taken from the published api.StreamEndReason
// constants. See StreamEnd.Reason for what each means.
const (
	reasonRunEnded     = string(api.StreamEndRunEnded)
	reasonOverflowed   = string(api.StreamEndOverflowed)
	reasonWriteStalled = string(api.StreamEndWriteStalled)
)

// StreamEnd is what a LiveSource decodes from the server's terminal NDJSON
// line: the one place a caller can tell apart an overflow disconnect, a
// write-stalled connection, and a clean shutdown, which are otherwise
// byte-identical on the wire.
type StreamEnd struct {
	// LastSeq is the seq of the last event this Subscribe call delivered, 0
	// if none were. 0 is ambiguous on its own (see attachsrv's
	// streamEndMarker.LastSeq); do not turn it into a resume point by
	// adding 1.
	LastSeq uint64
	// Overflowed reports whether the server disconnected this subscriber
	// for falling behind (resubscribe from LastSeq+1, or re-snapshot via
	// State on a 410). Kept for callers written before Reason existed;
	// Reason is authoritative when present, since only it can say
	// "write_stalled".
	Overflowed bool
	// Reason mirrors api.StreamEndReason's values as a plain string so a
	// server naming a reason this client has never seen still decodes (the
	// stance api.Event.Type takes). Recognised values:
	//
	//   "run_ended"      the run finished or the hub closed; nothing more
	//                    is coming.
	//   "overflowed"     this subscriber fell behind the retained ring;
	//                    resubscribe from LastSeq+1, or re-snapshot on 410.
	//   "write_stalled"  the server gave up writing to this connection; the
	//                    engine is presumably fine, reconnect from
	//                    LastSeq+1. NOT run_ended: treating it that way
	//                    silently demotes a merely-slow client to disk.
	//                    attachsrv itself never sends it (a markerless
	//                    close reaches this client instead); recognised for
	//                    other live Source implementations.
	//
	// Empty on a server built before this field existed; Overflowed alone
	// then gives the pre-existing behavior.
	Reason string
	// Hint is the server's own resume advice, verbatim.
	Hint string
}

// subscribeStreamer is a live Source that can report why its Subscribe
// channel closed. Only *LiveSource satisfies it; FallbackSource asserts for
// it to distinguish "the engine is gone" (run_ended, or no marker after a
// failed reconnect) from the two cases that must not demote a session:
// overflowed and write_stalled, where the engine is presumably fine.
type subscribeStreamer interface {
	SubscribeStream(ctx context.Context, fromSeq uint64) (<-chan api.Event, <-chan StreamEnd, error)
}

// LiveSource is a Source backed by a running engine's attach server: plain
// HTTP requests over the listener attachsrv.Listen bound (unix socket or
// host:port), plus one streaming NDJSON endpoint (GET /api/stream). Built
// on net/http and encoding/json only; the root module carries no
// third-party dependencies. See internal/attachsrv/server.go for the wire
// shapes.
type LiveSource struct {
	client *http.Client
	// network and addr are what this dialled. Kept because Shell cannot go
	// through client: a hijacked connection cannot be handed back by
	// net/http, so Shell dials the same endpoint itself. See shell.go.
	network string
	addr    string
	// tlsConfig is non-nil for an https endpoint; Shell needs it because it
	// dials by hand.
	tlsConfig *tls.Config
	// base is the scheme and authority every request URL is built on:
	// "http://unix" for a unix socket (a placeholder host net/http requires
	// and the Transport ignores), or the real scheme://host:port for TCP.
	base string
	// token is the bearer credential a TCP endpoint requires, empty for a
	// unix socket. Applied only in authorize so every request gets it.
	token string

	mu      sync.Mutex
	closed  bool
	nextID  int
	cancels map[int]context.CancelFunc
}

// Endpoint names one attach server and everything needed to reach it,
// built from a discovered attachsrv.Entry or from flags and the environment
// when there is no registry (a port-forward to another machine, most of
// all).
type Endpoint struct {
	// Network is "tcp", or "unix" (the default, and what the empty string
	// means).
	Network string
	// Address is the unix socket path, or the host:port.
	Address string
	// Token is the per-run bearer credential. Required for tcp, refused for
	// unix, whose boundary is the server's peer-credential check on the
	// connection itself.
	Token string
	// TLS, when non-nil, makes this an https endpoint verified against that
	// configuration. Nil means plaintext, defensible only for loopback or a
	// unix socket; the server enforces that at bind time.
	//
	// Never set InsecureSkipVerify here: an unverifying client hands the
	// bearer token to whoever answered the port.
	TLS *tls.Config
}

// Dial connects to the attach server listening on the unix socket at addr:
// DialEndpoint for the default transport, which is what almost every caller
// wants.
func Dial(ctx context.Context, addr string) (*LiveSource, error) {
	return DialEndpoint(ctx, Endpoint{Network: attachsrv.NetworkUnix, Address: addr})
}

var _ Source = (*LiveSource)(nil)

// ResponseHeaderBudget is how long a request to the attach server may wait
// for response headers before it is treated as unanswerable. Generous on
// purpose: it only has to survive a heavily loaded machine, and being too
// short causes spurious failures where too long merely delays one. A var
// rather than a const only so tests can shorten it; nothing outside a test
// writes to it.
var ResponseHeaderBudget = 30 * time.Second

// DialEndpoint connects to the attach server ep names. It dials once up
// front, so a caller learns immediately that nothing is listening, then
// builds an http.Client whose Transport reaches the same endpoint.
//
// A tcp endpoint with no Token is refused here rather than left to collect
// a 401: the server's 401 is deliberately identical for a wrong or missing
// credential, so this is the last point at which "nobody gave me a token"
// can be said as itself.
func DialEndpoint(ctx context.Context, ep Endpoint) (*LiveSource, error) {
	network := ep.Network
	if network == "" {
		network = attachsrv.NetworkUnix
	}
	switch network {
	case attachsrv.NetworkUnix:
		if ep.Token != "" {
			return nil, errors.New("source: a unix endpoint takes no token: " +
				"that transport is guarded by the server's peer-credential check, not by a credential this client presents")
		}
		if ep.TLS != nil {
			return nil, errors.New("source: a unix endpoint takes no TLS configuration: it never leaves the machine")
		}
	case attachsrv.NetworkTCP:
		if ep.Token == "" {
			return nil, fmt.Errorf("source: dialling %s over tcp needs this run's bearer token, and none was given: "+
				"on the machine running the pipeline `senro attach` reads it from the run's registry entry, "+
				"and for a run reached over a port-forward it comes from $%s", ep.Address, TokenEnv)
		}
	default:
		return nil, fmt.Errorf("source: %q is not a transport this build dials", network)
	}

	scheme, base := "http", "unix"
	if network == attachsrv.NetworkTCP {
		base = ep.Address
		if ep.TLS != nil {
			scheme = "https"
		}
	}

	tr := &http.Transport{
		// Bound the wait for response headers only. http.Client.Timeout
		// would cover the body too, breaking /api/stream, whose body stays
		// open for the life of the run; every senro endpoint flushes its
		// header before it ever blocks.
		//
		// Needed, not decoration: a connection in the accept backlog when
		// the listener closes is never answered, and on a unix socket
		// nothing resets it, so without this a client that dialled a run as
		// it exited waits forever. The caller's context is no backstop;
		// `senro attach` hands in one with no deadline.
		ResponseHeaderTimeout: ResponseHeaderBudget,
		TLSClientConfig:       ep.TLS,
	}
	if network == attachsrv.NetworkUnix {
		// The URL's host is a placeholder net/http insists on; the socket
		// path is what is actually dialled.
		tr.DialContext = func(ctx context.Context, _, _ string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, attachsrv.NetworkUnix, ep.Address)
		}
	}

	var d net.Dialer
	conn, err := d.DialContext(ctx, network, ep.Address)
	if err != nil {
		return nil, fmt.Errorf("source: dial %s: %w", ep.Address, err)
	}
	_ = conn.Close()

	return &LiveSource{
		client:    &http.Client{Transport: tr},
		network:   network,
		addr:      ep.Address,
		tlsConfig: ep.TLS,
		base:      scheme + "://" + base,
		token:     ep.Token,
		cancels:   make(map[int]context.CancelFunc),
	}, nil
}

// TokenEnv is the environment variable a client reads this run's bearer
// token from when there is no registry entry (a port-forward, in
// particular). An env var rather than a flag: a flag value is visible to
// `ps` and /proc for every user on the machine.
const TokenEnv = "SENRO_ATTACH_TOKEN"

func (ls *LiveSource) url(path string) string { return ls.base + path }

// authorize attaches this endpoint's credential to one request, and is the
// only place in this package that does. Every request goes through it,
// including the two built outside ls.client (Control's own request and
// Shell's hand-written upgrade). A no-op for a unix endpoint, which has no
// token.
func (ls *LiveSource) authorize(req *http.Request) {
	if ls.token != "" {
		req.Header.Set("Authorization", "Bearer "+ls.token)
	}
}

// do builds and issues one request. Callers that get a non-nil response own
// its Body and must close it.
func (ls *LiveSource) do(ctx context.Context, method, u string, body io.Reader) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, method, u, body)
	if err != nil {
		return nil, err
	}
	ls.authorize(req)
	return ls.client.Do(req)
}

// statusError turns a non-2xx response into an error carrying its status
// and a bounded prefix of its body, then drains and closes the body. Every
// caller of statusError is done with resp at that point.
func statusError(resp *http.Response) error {
	// Close errors on response bodies carry nothing actionable once the
	// read has happened; they are discarded here and package-wide.
	defer func() { _ = resp.Body.Close() }()
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	return fmt.Errorf("status %d: %s", resp.StatusCode, bytes.TrimSpace(b))
}

// State issues GET /api/state.
func (ls *LiveSource) State(ctx context.Context) (*api.RunState, error) {
	if err := ls.checkOpen(); err != nil {
		return nil, err
	}
	resp, err := ls.do(ctx, http.MethodGet, ls.url("/api/state"), nil)
	if err != nil {
		return nil, fmt.Errorf("source: GET /api/state: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("source: GET /api/state: %w", statusError(resp))
	}
	defer func() { _ = resp.Body.Close() }()
	var st api.RunState
	if err := json.NewDecoder(resp.Body).Decode(&st); err != nil {
		return nil, fmt.Errorf("source: decode state: %w", err)
	}
	return &st, nil
}

// Subscribe implements Source: SubscribeStream with the terminal-marker
// channel discarded. FallbackSource uses SubscribeStream directly when it
// needs to tell overflow apart from a clean end.
func (ls *LiveSource) Subscribe(ctx context.Context, fromSeq uint64) (<-chan api.Event, error) {
	// end is discarded, not drained: it is buffered (capacity 1), so
	// stream's single possible send never blocks.
	events, _, err := ls.SubscribeStream(ctx, fromSeq)
	if err != nil {
		return nil, err
	}
	return events, nil
}

// SubscribeStream is Subscribe's full form: it also reports, on the
// StreamEnd channel, what the server's terminal marker said, if one
// arrived. That channel receives at most once and is then closed; it is
// closed with nothing sent if the events channel ended for any other reason
// (ctx cancelled, the LiveSource closed, or a break with no marker).
func (ls *LiveSource) SubscribeStream(ctx context.Context, fromSeq uint64) (<-chan api.Event, <-chan StreamEnd, error) {
	if err := ls.checkOpen(); err != nil {
		return nil, nil, err
	}

	sctx, cancel := context.WithCancel(ctx)
	id, ok := ls.registerCancel(cancel)
	if !ok {
		cancel()
		return nil, nil, fmt.Errorf("source: %w", ErrClosed)
	}

	u := ls.url("/api/stream?from=" + strconv.FormatUint(fromSeq, 10))
	resp, err := ls.do(sctx, http.MethodGet, u, nil)
	if err != nil {
		ls.unregisterCancel(id)
		cancel()
		return nil, nil, fmt.Errorf("source: GET /api/stream: %w", err)
	}
	if resp.StatusCode == http.StatusGone {
		_ = resp.Body.Close()
		ls.unregisterCancel(id)
		cancel()
		return nil, nil, fmt.Errorf("source: %w", ErrOverflow)
	}
	if resp.StatusCode != http.StatusOK {
		err := statusError(resp)
		ls.unregisterCancel(id)
		cancel()
		return nil, nil, fmt.Errorf("source: GET /api/stream: %w", err)
	}

	events := make(chan api.Event)
	end := make(chan StreamEnd, 1)
	go ls.stream(sctx, resp.Body, events, end, id, cancel)
	return events, end, nil
}

// stream decodes NDJSON off body (one api.Event per line, or the one
// terminal marker line) until the connection ends, ctx is cancelled, or the
// LiveSource is closed. It owns body and closes it on every return path.
func (ls *LiveSource) stream(ctx context.Context, body io.ReadCloser, events chan<- api.Event, end chan<- StreamEnd, id int, cancel context.CancelFunc) {
	defer cancel()
	defer ls.unregisterCancel(id)
	defer close(events)
	defer close(end)
	defer func() { _ = body.Close() }()

	// The line-by-line decode, including recognising the terminal marker by
	// its "stream_end" field before decoding the line as an event, lives in
	// internal/ndjson: the WASM client reads the identical bytes and must
	// agree on every rule.
	marker, ok := ndjson.Read(body, func(e api.Event) bool {
		select {
		case events <- e:
			return true
		case <-ctx.Done():
			return false
		}
	})
	if ok {
		end <- StreamEnd{
			LastSeq:    marker.LastSeq,
			Overflowed: marker.Overflowed,
			Reason:     marker.Reason,
			Hint:       marker.Hint,
		}
	}
}

// Logs opens one step attempt's log stream, starting at byte offset from,
// via GET /api/logs/{step}. The caller owns the returned ReadCloser.
func (ls *LiveSource) Logs(ctx context.Context, step string, attempt int, stream string, from int64) (io.ReadCloser, error) {
	if err := ls.checkOpen(); err != nil {
		return nil, err
	}

	q := url.Values{}
	q.Set("attempt", strconv.Itoa(attempt))
	q.Set("stream", stream)
	if from > 0 {
		q.Set("from", strconv.FormatInt(from, 10))
	}
	u := ls.url("/api/logs/" + stepid.Encode(step) + "?" + q.Encode())

	resp, err := ls.do(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, fmt.Errorf("source: GET /api/logs: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("source: GET /api/logs: %w", statusError(resp))
	}
	return resp.Body, nil
}

// Control issues POST /api/control with req as the body and decodes the
// correlated response frame. A 403 (Options.ReadOnly on the server) is
// reported as ErrReadOnly, matching FileSource: the same error whether
// there is no engine or one that refuses.
func (ls *LiveSource) Control(ctx context.Context, req api.Frame) (api.Frame, error) {
	if err := ls.checkOpen(); err != nil {
		return api.Frame{}, err
	}
	b, err := json.Marshal(req)
	if err != nil {
		return api.Frame{}, fmt.Errorf("source: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, ls.url("/api/control"), bytes.NewReader(b))
	if err != nil {
		return api.Frame{}, fmt.Errorf("source: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	ls.authorize(httpReq)

	resp, err := ls.client.Do(httpReq)
	if err != nil {
		return api.Frame{}, fmt.Errorf("source: POST /api/control: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	var res api.Frame
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return api.Frame{}, fmt.Errorf("source: decode control response: %w", err)
	}
	if resp.StatusCode == http.StatusForbidden {
		return res, fmt.Errorf("source: control %q: %w", req.Type, ErrReadOnly)
	}
	if resp.StatusCode != http.StatusOK {
		return res, fmt.Errorf("source: control %q: status %d: %s", req.Type, resp.StatusCode, res.Error)
	}
	return res, nil
}

// Close releases the LiveSource: every in-flight subscribe is cancelled
// (even one whose caller passed context.Background(), matching
// FileSource's no-leak guarantee) and idle connections are released.
// Idempotent.
func (ls *LiveSource) Close() error {
	ls.mu.Lock()
	if ls.closed {
		ls.mu.Unlock()
		return nil
	}
	ls.closed = true
	cancels := make([]context.CancelFunc, 0, len(ls.cancels))
	for _, c := range ls.cancels {
		cancels = append(cancels, c)
	}
	ls.cancels = nil
	ls.mu.Unlock()

	for _, c := range cancels {
		c()
	}
	ls.client.CloseIdleConnections()
	return nil
}

func (ls *LiveSource) checkOpen() error {
	ls.mu.Lock()
	defer ls.mu.Unlock()
	if ls.closed {
		return fmt.Errorf("source: %w", ErrClosed)
	}
	return nil
}

// registerCancel adds cancel under a fresh id, unless Close has already
// committed to shutting down: whichever of the two takes ls.mu first is
// ordered before the other, so no cancel func is added after Close has
// collected the set it will call. Mirrors attachsrv.Server.track.
func (ls *LiveSource) registerCancel(cancel context.CancelFunc) (int, bool) {
	ls.mu.Lock()
	defer ls.mu.Unlock()
	if ls.closed {
		return 0, false
	}
	id := ls.nextID
	ls.nextID++
	ls.cancels[id] = cancel
	return id, true
}

func (ls *LiveSource) unregisterCancel(id int) {
	ls.mu.Lock()
	if ls.cancels != nil {
		delete(ls.cancels, id)
	}
	ls.mu.Unlock()
}
