package webui

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"time"
)

// Upstream names the attach server this UI reads from: exactly what
// internal/source.Endpoint names, restated here so this package does not
// depend on that one just to carry four fields.
type Upstream struct {
	// Network is "unix" or "tcp".
	Network string
	// Address is the unix socket path or the host:port.
	Address string
	// Token is the run's bearer credential, required for tcp and absent for
	// unix. It stays in this process. Nothing writes it to a response, a
	// log, or a page.
	Token string
	// TLS, when non-nil, makes the upstream https, verified against that
	// configuration. Never set InsecureSkipVerify: a client that does not
	// verify hands the bearer token to whoever answered the port.
	TLS *tls.Config
}

// readableRoutes is every path this server will forward a GET to: an
// allowlist, not a prefix match.
//
// The attach server also carries POST /api/control and POST /api/shell.
// Control IS forwarded, by its own route and handler under a fail-closed
// origin check (control.go); it is not in this list because it must not
// reach the GET handler. POST /api/shell is forwarded by nothing, a
// standing decision, not an unimplemented feature: a page that can steer a
// run is not a page that can run arbitrary commands on the operator's
// machine. The boundary is enforced by routing, not by a check somebody
// could forget to apply.
var readableRoutes = []string{
	"/api/state",
	"/api/stream",
	"/api/plan",
}

// logRoutePrefix is the one forwarded route with a variable path segment:
// GET /api/logs/{step}. Matched as a prefix rather than listed, since the
// step id is part of the path.
const logRoutePrefix = "/api/logs/"

// forwardable reports whether path is a route this server forwards.
func forwardable(path string) bool {
	for _, p := range readableRoutes {
		if path == p {
			return true
		}
	}
	// A bare "/api/logs/" with no step is not a log request; the attach
	// server would not route it either.
	return len(path) > len(logRoutePrefix) && path[:len(logRoutePrefix)] == logRoutePrefix
}

// newUpstreamClient builds the http.Client this server forwards with.
// DisableCompression, so the bytes handed to the browser are the bytes the
// attach server produced, with no decompressor between the engine and a
// page waiting on individual stream lines. No Client.Timeout: it would
// cover the body, and GET /api/stream stays open for the life of the run;
// the bound is on response headers, which every endpoint sends promptly.
func newUpstreamClient(up Upstream) *http.Client {
	return newUpstreamClientWithBudget(up, upstreamHeaderBudget)
}

// newUpstreamClientWithBudget is newUpstreamClient with the response-header
// bound supplied, because the read routes and the control route want
// different ones. See controlHeaderBudget for why they must differ.
func newUpstreamClientWithBudget(up Upstream, headerBudget time.Duration) *http.Client {
	tr := &http.Transport{
		DisableCompression:    true,
		ResponseHeaderTimeout: headerBudget,
		TLSClientConfig:       up.TLS,
	}
	if up.Network == "unix" {
		tr.DialContext = func(ctx context.Context, _, _ string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, "unix", up.Address)
		}
	}
	return &http.Client{Transport: tr}
}

// upstreamHeaderBudget bounds the wait for an attach server's response
// headers. Generous, matching internal/source.ResponseHeaderBudget: it
// only has to survive a loaded machine, and being wrong the other way is a
// request that hangs with no way out.
var upstreamHeaderBudget = 30 * time.Second

// upstreamBase is the scheme and authority forwarded URLs are built on.
// "http://unix" for a unix socket, where the host is a placeholder net/http
// requires and the Transport's own dialer ignores.
func upstreamBase(up Upstream) string {
	if up.Network == "unix" {
		return "http://unix"
	}
	if up.TLS != nil {
		return "https://" + up.Address
	}
	return "http://" + up.Address
}

// forward proxies one allowlisted GET to the attach server, adding the
// run's bearer token, and streams the answer straight back.
//
// Hand-written rather than httputil.ReverseProxy: what matters is what
// does NOT cross. No request header the browser sent is forwarded (no
// Cookie, Origin, Referer or page-chosen Authorization reaches the
// engine), and no response header but the content type comes back. A
// ReverseProxy forwards faithfully and is told what to strip: the wrong
// default for a three-item allowlist.
func (s *Server) forward(w http.ResponseWriter, r *http.Request) {
	u := s.upstreamBase + r.URL.Path
	if q := r.URL.RawQuery; q != "" {
		u += "?" + q
	}

	req, err := http.NewRequestWithContext(r.Context(), http.MethodGet, u, nil)
	if err != nil {
		http.Error(w, "senro ui: bad upstream request", http.StatusInternalServerError)
		return
	}
	// The one header that crosses, and the one place this process's copy of
	// the run's credential is ever written to a wire.
	if s.upstream.Token != "" {
		req.Header.Set("Authorization", "Bearer "+s.upstream.Token)
	}

	resp, err := s.client.Do(req)
	if err != nil {
		// The engine's address is deliberately not in this message: this
		// text is rendered in a browser.
		http.Error(w, "senro ui: the attach server did not answer", http.StatusBadGateway)
		return
	}
	defer func() { _ = resp.Body.Close() }()

	if ct := resp.Header.Get("Content-Type"); ct != "" {
		w.Header().Set("Content-Type", ct)
	}
	w.WriteHeader(resp.StatusCode)

	// Flushed as it arrives, not buffered: GET /api/stream emits a line
	// whenever the run does, and buffering would turn a live view into
	// bursts, or nothing at all for a quiet run.
	_, _ = io.Copy(flushWriter{w: w, f: flusherOf(w)}, resp.Body)
}

// flushWriter flushes after every write, which for a chunked NDJSON body
// means every batch of lines the engine produced reaches the browser as
// soon as this process has them.
type flushWriter struct {
	w io.Writer
	f http.Flusher
}

func (fw flushWriter) Write(p []byte) (int, error) {
	n, err := fw.w.Write(p)
	if fw.f != nil {
		fw.f.Flush()
	}
	return n, err
}

func flusherOf(w http.ResponseWriter) http.Flusher {
	f, _ := w.(http.Flusher)
	return f
}

// checkUpstream makes one request to the attach server before the UI claims
// to be serving anything, so an operator learns "there is no engine there,
// or your token is wrong" from their terminal rather than from a blank page
// in a browser they just opened.
func (s *Server) checkUpstream(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.upstreamBase+"/api/state", nil)
	if err != nil {
		return err
	}
	if s.upstream.Token != "" {
		req.Header.Set("Authorization", "Bearer "+s.upstream.Token)
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return fmt.Errorf("webui: reaching the attach server at %s: %w", s.upstream.Address, err)
	}
	defer func() { _ = resp.Body.Close() }()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
	if resp.StatusCode == http.StatusUnauthorized {
		return fmt.Errorf("webui: the attach server at %s refused this run's bearer token", s.upstream.Address)
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("webui: the attach server at %s answered %d to GET /api/state", s.upstream.Address, resp.StatusCode)
	}
	return nil
}
