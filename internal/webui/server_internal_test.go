package webui

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/xavidop/senro/api"
)

// These tests are about the access boundary, the routing and the
// forwarding, and none of them is about the several megabytes of compiled
// WebAssembly a tree that has not run `make wasm` does not have. They bind
// through listen with a synthetic bundle so the suite is meaningful in a
// fresh checkout; whether the real bundle is complete is its own question,
// asked once, at the bottom of this file.

// fakeBundle is an asset set with the right names and trivial contents.
func fakeBundle(t *testing.T) *bundle {
	t.Helper()
	b := &bundle{files: map[string]*asset{}}
	for name, body := range map[string]string{
		"index.html":  "<!doctype html><title>senro</title>",
		"app.css":     "body{}",
		"boot.js":     "// boot",
		execAsset:     "// wasm_exec",
		clientAsset:   gzipOf(t, "\x00asm fake"),
		"plain.wasm":  "\x00asm plain",
		"unused.junk": "junk",
	} {
		b.files[name] = &asset{
			body:        []byte(body),
			contentType: contentTypeFor(name),
			gzipped:     strings.HasSuffix(name, ".gz"),
			etag:        `"` + name + `"`,
		}
	}
	return b
}

func gzipOf(t *testing.T, s string) string {
	t.Helper()
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	if _, err := io.WriteString(zw, s); err != nil {
		t.Fatalf("gzip write: %v", err)
	}
	if err := zw.Close(); err != nil {
		t.Fatalf("gzip close: %v", err)
	}
	return buf.String()
}

// upstreamStub stands in for an attach server and records what reached it.
type upstreamStub struct {
	srv *httptest.Server

	mu    sync.Mutex
	paths []string
	auth  []string
	// headers records every request header the UI server forwarded, so a
	// test can assert that the browser's own cookies and origin did not.
	headers []http.Header
	// controlBodies records the bytes of every POST /api/control that
	// arrived, so a test can assert what the UI server actually sent rather
	// than only that it sent something.
	controlBodies []string
}

func newUpstreamStub(t *testing.T) *upstreamStub {
	t.Helper()
	u := &upstreamStub{}
	u.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body []byte
		if r.Method == http.MethodPost {
			body, _ = io.ReadAll(io.LimitReader(r.Body, 1<<20))
		}

		u.mu.Lock()
		u.paths = append(u.paths, r.URL.RequestURI())
		u.auth = append(u.auth, r.Header.Get("Authorization"))
		u.headers = append(u.headers, r.Header.Clone())
		if r.URL.Path == "/api/control" {
			u.controlBodies = append(u.controlBodies, string(body))
		}
		u.mu.Unlock()

		switch r.URL.Path {
		case "/api/state":
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"seq":3,"run":{"id":"r1"},"steps":{},"order":[]}`)
		case "/api/stream":
			w.Header().Set("Content-Type", "application/x-ndjson")
			_, _ = io.WriteString(w, `{"stream_end":true,"reason":"run_ended"}`+"\n")
		case "/api/control":
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"v":1,"kind":"res","id":"c1","ok":true}`)
		case "/api/shell":
			// Answers, so a test that reaches it fails loudly on content
			// rather than on a status that could be mistaken for a refusal.
			_, _ = io.WriteString(w, "SHELL REACHED")
		default:
			_, _ = io.WriteString(w, "ok")
		}
	}))
	t.Cleanup(u.srv.Close)
	return u
}

func (u *upstreamStub) seen() ([]string, []string) {
	u.mu.Lock()
	defer u.mu.Unlock()
	return append([]string(nil), u.paths...), append([]string(nil), u.auth...)
}

// controlBody is the last control frame that reached the stub, or "" if
// none did.
func (u *upstreamStub) controlBody() string {
	u.mu.Lock()
	defer u.mu.Unlock()
	if len(u.controlBodies) == 0 {
		return ""
	}
	return u.controlBodies[len(u.controlBodies)-1]
}

const stubToken = "a-token-the-page-must-never-see-0123456789"

// newTestServer starts a UI server in front of a stub attach server.
func newTestServer(t *testing.T) (*Server, *upstreamStub) {
	t.Helper()
	up := newUpstreamStub(t)
	s, err := listen(context.Background(), Options{
		Bind: "127.0.0.1:0",
		Upstream: Upstream{
			Network: "tcp",
			Address: strings.TrimPrefix(up.srv.URL, "http://"),
			Token:   stubToken,
		},
	}, fakeBundle(t))
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s, up
}

// client is a plain http.Client that does not follow redirects, so a test
// can inspect the handoff's own response, and that carries a cookie jar so
// the session survives between requests.
func newClient(t *testing.T) *http.Client {
	t.Helper()
	jar := &simpleJar{}
	return &http.Client{
		Jar: jar,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
}

// simpleJar is a cookie jar with no policy at all: it stores what it is
// given and offers it back for every request. A test asserting that the
// SERVER refuses an unauthenticated request must not be able to pass
// because a jar declined to send a cookie.
type simpleJar struct {
	mu sync.Mutex
	cs []*http.Cookie
}

func (j *simpleJar) SetCookies(_ *url.URL, cookies []*http.Cookie) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.cs = append(j.cs, cookies...)
}

func (j *simpleJar) Cookies(*url.URL) []*http.Cookie {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.cs
}

// get issues one GET at a path on the UI server.
func get(t *testing.T, c *http.Client, s *Server, path string, headers map[string]string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, "http://"+s.Addr()+path, nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := c.Do(req)
	if err != nil {
		t.Fatalf("GET %s: %v", path, err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	return resp
}

// handshake walks the one-time link and returns a client holding the
// session, which is what every ordinary request needs.
func handshake(t *testing.T, s *Server) *http.Client {
	t.Helper()
	c := newClient(t)
	u := s.URL()
	path := u[strings.Index(u, handoffPrefix):]
	resp := get(t, c, s, path, nil)
	if resp.StatusCode != http.StatusSeeOther {
		t.Fatalf("handoff status = %d, want 303", resp.StatusCode)
	}
	return c
}

// The headline property of this whole design: the run's bearer token
// reaches the attach server and reaches nothing else. Not the page, not a
// URL, not a cookie, not a response body.
func TestTheRunsTokenNeverReachesTheBrowser(t *testing.T) {
	s, up := newTestServer(t)
	c := handshake(t, s)

	for _, path := range []string{"/", "/_ui/app.css", "/_ui/boot.js", "/api/state", "/api/stream?from=1"} {
		resp := get(t, c, s, path, nil)
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		if strings.Contains(string(body), stubToken) {
			t.Errorf("GET %s: the response body carries the run's bearer token", path)
		}
		for k, vs := range resp.Header {
			for _, v := range vs {
				if strings.Contains(v, stubToken) {
					t.Errorf("GET %s: response header %s carries the run's bearer token", path, k)
				}
			}
		}
	}
	if strings.Contains(s.URL(), stubToken) {
		t.Error("the one-time link carries the run's bearer token")
	}

	// And it does reach the engine, on every forwarded request, or the UI
	// would simply not work and this test would be vacuous.
	_, auth := up.seen()
	if len(auth) == 0 {
		t.Fatal("nothing was forwarded")
	}
	for i, a := range auth {
		if a != "Bearer "+stubToken {
			t.Errorf("forwarded request %d presented %q, want the run's bearer token", i, a)
		}
	}
}

// The shell is the one attach endpoint this server forwards by nothing at
// all, and that is a standing decision rather than an unimplemented
// feature: steering a run is a reasonable thing for an operator holding the
// one-time link to do, and running arbitrary commands on their machine is
// not the same proposition. Neither method, and no path that spells it
// sideways, reaches it.
func TestTheShellIsNotReachable(t *testing.T) {
	s, up := newTestServer(t)
	c := handshake(t, s)

	for _, path := range []string{"/api/shell", "/api/../api/shell"} {
		resp := get(t, c, s, path, nil)
		if resp.StatusCode == http.StatusOK {
			body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
			t.Errorf("GET %s: status 200 with %q: the shell is reachable", path, body)
		}
	}
	req, err := http.NewRequest(http.MethodPost, "http://"+s.Addr()+"/api/shell", strings.NewReader(`{}`))
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	req.Header.Set("Origin", "http://"+s.Addr())
	resp, err := c.Do(req)
	if err != nil {
		t.Fatalf("POST /api/shell: %v", err)
	}
	status := resp.StatusCode
	_ = resp.Body.Close()
	if status == http.StatusOK {
		t.Errorf("POST /api/shell: status 200: the shell is reachable")
	}

	paths, _ := up.seen()
	for _, p := range paths {
		if strings.Contains(p, "shell") {
			t.Errorf("a request for %q reached the attach server", p)
		}
	}
}

// control issues one POST /api/control at the UI server. headers are set
// last so a test can override the origin this sends by default.
func control(t *testing.T, c *http.Client, s *Server, body string, headers map[string]string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, "http://"+s.Addr()+"/api/control", strings.NewReader(body))
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Origin", "http://"+s.Addr())
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := c.Do(req)
	if err != nil {
		t.Fatalf("POST /api/control: %v", err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	return resp
}

// The operator's own page can steer the run: a control frame from this
// server's own origin, carrying the session, reaches the attach server with
// the run's bearer token attached.
func TestTheOperatorsPageCanSteerTheRun(t *testing.T) {
	s, up := newTestServer(t)
	c := handshake(t, s)

	resp := control(t, c, s, `{"v":1,"kind":"req","id":"c1","type":"run.cancel"}`, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	paths, auth := up.seen()
	var idx = -1
	for i, p := range paths {
		if p == "/api/control" {
			idx = i
		}
	}
	if idx < 0 {
		t.Fatalf("no control request reached the attach server; saw %v", paths)
	}
	if got := auth[idx]; got != "Bearer "+stubToken {
		t.Errorf("upstream Authorization = %q, want the run's bearer token", got)
	}
}

// The check that the read routes deliberately do not have. A POST is
// refused unless its Origin is exactly this server's own, and a request
// that declines to say where it came from is refused rather than trusted:
// on loopback, SameSite=Strict does not isolate this server from another
// port on the same host, because a site does not include the port.
func TestAControlRequestFromAnotherOriginIsRefused(t *testing.T) {
	s, up := newTestServer(t)
	c := handshake(t, s)

	// A real, valid port that is definitely not the one this server bound:
	// taken by binding it and letting it go, so the number is plausible
	// rather than arithmetic on the server's own port that could land
	// outside the valid range.
	other, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	otherAddr := other.Addr().String()
	if err := other.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	for _, origin := range []string{
		"",                                     // no Origin at all: fail closed
		"http://" + otherAddr,                  // same SITE, different port
		"http://evil.example",                  //
		"https://" + s.Addr(),                  // right authority, wrong scheme
		"null",                                 // an opaque origin, which is what a sandboxed frame sends
		"http://" + s.Addr() + ".evil.example", // prefix of the real origin
	} {
		t.Run("origin="+origin, func(t *testing.T) {
			resp := control(t, c, s, `{"v":1,"kind":"req","id":"c1","type":"run.cancel"}`, map[string]string{"Origin": origin})
			if resp.StatusCode == http.StatusOK {
				t.Errorf("Origin %q: status 200: a foreign page can steer this run", origin)
			}
		})
	}

	paths, _ := up.seen()
	for _, p := range paths {
		if strings.Contains(p, "control") {
			t.Errorf("a control request from a foreign origin reached the attach server")
		}
	}
}

// An op this page does not forward is refused here rather than upstream,
// so the ruling about what a browser may ask for is made in one place.
func TestAnUnforwardedOpIsRefused(t *testing.T) {
	s, up := newTestServer(t)
	c := handshake(t, s)

	for _, op := range []string{"run.exec", "", "shell.open"} {
		resp := control(t, c, s, `{"v":1,"kind":"req","id":"c1","type":"`+op+`"}`, nil)
		if resp.StatusCode != http.StatusForbidden {
			t.Errorf("op %q: status = %d, want 403", op, resp.StatusCode)
		}
	}
	paths, _ := up.seen()
	for _, p := range paths {
		if strings.Contains(p, "control") {
			t.Errorf("an unforwarded op reached the attach server")
		}
	}
}

// Control needs the session like everything else. Without it the origin
// check is never even reached.
func TestControlNeedsTheSession(t *testing.T) {
	s, up := newTestServer(t)
	c := newClient(t) // no handshake

	resp := control(t, c, s, `{"v":1,"kind":"req","id":"c1","type":"run.cancel"}`, nil)
	if resp.StatusCode == http.StatusOK {
		t.Errorf("status 200 without a session: control is unauthenticated")
	}
	paths, _ := up.seen()
	for _, p := range paths {
		if strings.Contains(p, "control") {
			t.Errorf("an unauthenticated control request reached the attach server")
		}
	}
}

// What the page sends is not what crosses. A frame carrying response-only
// fields and unknown keys is re-encoded from the fields this server
// recognises, so nothing a page invented rides along into the engine's
// decoder.
func TestAControlFrameIsRebuiltRatherThanPassedThrough(t *testing.T) {
	s, up := newTestServer(t)
	c := handshake(t, s)

	resp := control(t, c, s,
		`{"v":1,"kind":"res","id":"c1","type":"run.cancel","ok":true,"error":"x","surprise":"y"}`, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	body := up.controlBody()
	if body == "" {
		t.Fatal("no control body reached the attach server")
	}
	for _, unwanted := range []string{"surprise", `"ok"`, `"error"`, `"res"`} {
		if strings.Contains(body, unwanted) {
			t.Errorf("forwarded body %q still carries %s", body, unwanted)
		}
	}
	if !strings.Contains(body, `"kind":"req"`) {
		t.Errorf("forwarded body %q is not a request frame", body)
	}
}

// A body large enough to be worth using as a pipe is refused on size,
// before it is decoded.
func TestAnOversizedControlRequestIsRefused(t *testing.T) {
	s, up := newTestServer(t)
	c := handshake(t, s)

	huge := `{"v":1,"kind":"req","id":"` + strings.Repeat("x", maxControlRequestBytes) + `","type":"run.cancel"}`
	resp := control(t, c, s, huge, nil)
	if resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Errorf("status = %d, want 413", resp.StatusCode)
	}
	paths, _ := up.seen()
	for _, p := range paths {
		if strings.Contains(p, "control") {
			t.Errorf("an oversized control request reached the attach server")
		}
	}
}

// Every control op api declares gets a deliberate ruling here: forwarded to
// the browser, or written down as withheld. The failure this prevents is a
// future op becoming browser-reachable, or silently unreachable, because
// nobody weighed it against this page's threat model when they declared it.
func TestEveryDeclaredOpHasABrowserRuling(t *testing.T) {
	// Withheld ops, with the reason. Empty today: every op this build
	// declares acts on the run the operator is already watching.
	withheld := map[string]string{}

	for _, op := range api.DeclaredOps() {
		if controllableOps[op] {
			continue
		}
		if _, ok := withheld[op]; ok {
			continue
		}
		t.Errorf("api declares control op %q and internal/webui has no ruling on it: "+
			"either add it to controllableOps, or add it to this test's withheld map with the reason", op)
	}
	for op := range controllableOps {
		if !slices.Contains(api.DeclaredOps(), op) {
			t.Errorf("controllableOps forwards %q, which api no longer declares", op)
		}
	}
}

// Without the session, nothing. This is what stops another process on the
// machine, or a page that got past the origin checks, from reading a live
// build.
func TestEveryRouteNeedsTheSession(t *testing.T) {
	s, up := newTestServer(t)
	c := newClient(t) // no handshake

	// Listen makes one GET /api/state of its own, to report an unreachable
	// engine before a browser is ever pointed at this. That request is the
	// baseline; anything past it came from the browser.
	before, _ := up.seen()

	for _, path := range []string{"/", "/_ui/app.css", "/_ui/senro-ui.wasm", "/api/state", "/api/stream?from=1", "/api/logs/build?attempt=1&stream=stdout"} {
		resp := get(t, c, s, path, nil)
		if resp.StatusCode != http.StatusForbidden {
			t.Errorf("GET %s without a session: status = %d, want 403", path, resp.StatusCode)
		}
	}
	if after, _ := up.seen(); len(after) != len(before) {
		t.Errorf("unauthenticated requests reached the attach server: %v", after[len(before):])
	}
}

// The one-time link is one time. Two tabs opening the same URL is what
// happens when somebody clicks a terminal's link twice, and the second must
// not also get a session.
func TestTheHandoffLinkWorksExactlyOnce(t *testing.T) {
	s, _ := newTestServer(t)
	u := s.URL()
	path := u[strings.Index(u, handoffPrefix):]

	first := get(t, newClient(t), s, path, nil)
	if first.StatusCode != http.StatusSeeOther {
		t.Fatalf("first use: status = %d, want 303", first.StatusCode)
	}
	if len(first.Cookies()) == 0 {
		t.Fatal("first use set no cookie")
	}

	second := get(t, newClient(t), s, path, nil)
	if second.StatusCode != http.StatusNotFound {
		t.Errorf("second use: status = %d, want 404", second.StatusCode)
	}
	if len(second.Cookies()) != 0 {
		t.Error("a spent link still handed out a session")
	}
}

// The redirect lands on a URL with no credential in it, so the address bar
// somebody screenshots, and the page's own document.location, carry
// nothing.
func TestTheRedirectStripsTheNonceFromTheURL(t *testing.T) {
	s, _ := newTestServer(t)
	u := s.URL()
	path := u[strings.Index(u, handoffPrefix):]
	resp := get(t, newClient(t), s, path, nil)

	loc := resp.Header.Get("Location")
	if loc != "/" {
		t.Fatalf("Location = %q, want /", loc)
	}
	if strings.Contains(loc, s.handoff.cred.value) {
		t.Error("the redirect target still carries the nonce")
	}
}

// The session cookie must be unreadable from the page and unsendable from
// any other page. Those two attributes are the entire reason a cookie is a
// safer place for this than anything the client could hold itself.
func TestTheSessionCookieIsHttpOnlyAndStrict(t *testing.T) {
	s, _ := newTestServer(t)
	u := s.URL()
	resp := get(t, newClient(t), s, u[strings.Index(u, handoffPrefix):], nil)

	cookies := resp.Cookies()
	if len(cookies) != 1 {
		t.Fatalf("got %d cookies, want 1", len(cookies))
	}
	c := cookies[0]
	if !c.HttpOnly {
		t.Error("the session cookie is readable by the page's own scripts")
	}
	if c.SameSite != http.SameSiteStrictMode {
		t.Errorf("SameSite = %v, want Strict: another page could cause it to be sent", c.SameSite)
	}
	if c.Path != "/" {
		t.Errorf("Path = %q, want /", c.Path)
	}
	if !c.Expires.IsZero() || c.MaxAge != 0 {
		t.Error("the session cookie persists to disk; it is meaningless after this process exits")
	}
}

// DNS rebinding is the attack that makes a loopback server reachable from a
// page on the open internet, and the Host header is the one thing it cannot
// change. A request arriving under any other name is refused.
func TestARequestUnderAnotherHostnameIsRefused(t *testing.T) {
	s, up := newTestServer(t)
	c := handshake(t, s)

	before, _ := up.seen()

	// The Host header has to be set through Request.Host: net/http derives
	// it from the URL otherwise, so a header map entry would be ignored and
	// this test would pass without testing anything.
	_, port, _ := strings.Cut(s.Addr(), ":")
	for _, host := range []string{"evil.example:" + port, "attacker.test", "127.0.0.1.nip.io:" + port} {
		req, err := http.NewRequest(http.MethodGet, "http://"+s.Addr()+"/api/state", nil)
		if err != nil {
			t.Fatalf("NewRequest: %v", err)
		}
		req.Host = host
		resp, err := c.Do(req)
		if err != nil {
			t.Fatalf("GET: %v", err)
		}
		status := resp.StatusCode
		_ = resp.Body.Close()
		if status != http.StatusNotFound {
			t.Errorf("Host %q: status = %d, want 404", host, status)
		}
	}

	// The loopback names this server WAS bound under still work, or the
	// check above would be refusing the operator too.
	for _, host := range []string{"localhost:" + port, "127.0.0.1:" + port} {
		req, err := http.NewRequest(http.MethodGet, "http://"+s.Addr()+"/api/state", nil)
		if err != nil {
			t.Fatalf("NewRequest: %v", err)
		}
		req.Host = host
		resp, err := c.Do(req)
		if err != nil {
			t.Fatalf("GET: %v", err)
		}
		status := resp.StatusCode
		_ = resp.Body.Close()
		if status != http.StatusOK {
			t.Errorf("Host %q: status = %d, want 200", host, status)
		}
	}

	after, _ := up.seen()
	if len(after) != len(before)+2 {
		t.Errorf("%d requests reached the attach server, want the 2 legitimate ones: %v",
			len(after)-len(before), after[len(before):])
	}
}

// A browser tells us when a request came from another site, and a page on
// another site has no business reading this run even if it somehow held a
// session.
func TestACrossSiteRequestIsRefused(t *testing.T) {
	s, _ := newTestServer(t)
	c := handshake(t, s)

	for _, site := range []string{"cross-site", "same-site"} {
		resp := get(t, c, s, "/api/state", map[string]string{"Sec-Fetch-Site": site})
		if resp.StatusCode != http.StatusNotFound {
			t.Errorf("Sec-Fetch-Site: %s: status = %d, want 404", site, resp.StatusCode)
		}
	}
	for _, site := range []string{"same-origin", "none", ""} {
		resp := get(t, c, s, "/api/state", map[string]string{"Sec-Fetch-Site": site})
		if resp.StatusCode != http.StatusOK {
			t.Errorf("Sec-Fetch-Site: %q: status = %d, want 200", site, resp.StatusCode)
		}
	}
}

// A browser's own cookies, origin and any Authorization it invented must
// not be forwarded to the engine. The proxy speaks for itself.
func TestTheBrowsersHeadersDoNotReachTheEngine(t *testing.T) {
	s, up := newTestServer(t)
	c := handshake(t, s)

	get(t, c, s, "/api/state", map[string]string{
		"Authorization": "Bearer forged",
		"Origin":        "https://evil.example",
		"Referer":       "https://evil.example/page",
		"X-Smuggled":    "value",
	})

	up.mu.Lock()
	defer up.mu.Unlock()
	for i, h := range up.headers {
		if h.Get("Cookie") != "" {
			t.Errorf("forwarded request %d carried the browser's Cookie header", i)
		}
		if h.Get("Origin") != "" || h.Get("Referer") != "" {
			t.Errorf("forwarded request %d carried the browser's Origin/Referer", i)
		}
		if h.Get("X-Smuggled") != "" {
			t.Errorf("forwarded request %d carried a header the page invented", i)
		}
		if a := h.Get("Authorization"); a != "" && a != "Bearer "+stubToken {
			t.Errorf("forwarded request %d presented %q, not this process's own token", i, a)
		}
	}
}

// The query string is what carries the resume point, and a proxy that
// dropped it would silently replay the whole run on every subscription.
func TestTheResumePointSurvivesTheProxy(t *testing.T) {
	s, up := newTestServer(t)
	c := handshake(t, s)
	get(t, c, s, "/api/stream?from=4096", nil)

	paths, _ := up.seen()
	var found bool
	for _, p := range paths {
		if p == "/api/stream?from=4096" {
			found = true
		}
	}
	if !found {
		t.Fatalf("forwarded paths = %v, want /api/stream?from=4096 among them", paths)
	}
}

// Every response carries a policy that denies the page everything it was
// not explicitly given, including the ability to load anything from
// anywhere else.
func TestEveryResponseCarriesItsPolicy(t *testing.T) {
	s, _ := newTestServer(t)
	c := handshake(t, s)

	for _, path := range []string{"/", "/_ui/app.css", "/api/state", "/nope"} {
		resp := get(t, c, s, path, nil)
		csp := resp.Header.Get("Content-Security-Policy")
		for _, want := range []string{"default-src 'none'", "wasm-unsafe-eval", "connect-src 'self'", "frame-ancestors 'none'"} {
			if !strings.Contains(csp, want) {
				t.Errorf("GET %s: policy %q is missing %q", path, csp, want)
			}
		}
		if strings.Contains(csp, "unsafe-inline") {
			t.Errorf("GET %s: policy allows inline scripts", path)
		}
		if got := resp.Header.Get("Referrer-Policy"); got != "no-referrer" {
			t.Errorf("GET %s: Referrer-Policy = %q, want no-referrer", path, got)
		}
		if got := resp.Header.Get("X-Content-Type-Options"); got != "nosniff" {
			t.Errorf("GET %s: X-Content-Type-Options = %q, want nosniff", path, got)
		}
	}
}

// The WebAssembly module has to be served as application/wasm or
// instantiateStreaming refuses it outright, and it has to arrive
// compressed or it is four megabytes on the wire.
func TestTheClientIsServedAsWasmAndCompressed(t *testing.T) {
	s, _ := newTestServer(t)
	c := handshake(t, s)

	resp := get(t, c, s, "/_ui/senro-ui.wasm", map[string]string{"Accept-Encoding": "gzip"})
	if got := resp.Header.Get("Content-Type"); got != "application/wasm" {
		t.Errorf("Content-Type = %q, want application/wasm", got)
	}
	if got := resp.Header.Get("Content-Encoding"); got != "gzip" {
		t.Errorf("Content-Encoding = %q, want gzip", got)
	}
	if got := resp.Header.Get("ETag"); got == "" {
		t.Error("no ETag: a reload would re-download the whole client")
	}
}

// A reload must cost a conditional request and nothing else. This is the
// only answer to "a browser downloads it every time" that does not involve
// making the binary smaller.
func TestAReloadRevalidatesRatherThanRedownloading(t *testing.T) {
	s, _ := newTestServer(t)
	c := handshake(t, s)

	first := get(t, c, s, "/_ui/senro-ui.wasm", nil)
	etag := first.Header.Get("ETag")
	if etag == "" {
		t.Fatal("no ETag on the first fetch")
	}
	second := get(t, c, s, "/_ui/senro-ui.wasm", map[string]string{"If-None-Match": etag})
	if second.StatusCode != http.StatusNotModified {
		t.Fatalf("conditional fetch: status = %d, want 304", second.StatusCode)
	}
	body, _ := io.ReadAll(second.Body)
	if len(body) != 0 {
		t.Errorf("a 304 carried %d bytes of body", len(body))
	}
}

// A client that does not accept gzip still gets the module, decompressed.
func TestAClientWithoutGzipStillGetsTheModule(t *testing.T) {
	s, _ := newTestServer(t)
	c := handshake(t, s)

	req, err := http.NewRequest(http.MethodGet, "http://"+s.Addr()+"/_ui/senro-ui.wasm", nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	req.Header.Set("Accept-Encoding", "identity")
	resp, err := c.Do(req)
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if got := resp.Header.Get("Content-Encoding"); got != "" {
		t.Errorf("Content-Encoding = %q, want none", got)
	}
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "asm fake") {
		t.Errorf("body = %q, want the decompressed module", body)
	}
}

// The browser UI binds loopback and nothing else, with no flag to change
// it. A routable bind would put a live run's view, and the session cookie
// that opens it, on the network in plaintext.
func TestANonLoopbackBindIsRefused(t *testing.T) {
	up := newUpstreamStub(t)
	for _, bind := range []string{"0.0.0.0:0", ":0", "192.0.2.1:0"} {
		_, err := listen(context.Background(), Options{
			Bind:     bind,
			Upstream: Upstream{Network: "tcp", Address: strings.TrimPrefix(up.srv.URL, "http://"), Token: stubToken},
		}, fakeBundle(t))
		if err == nil {
			t.Errorf("bind %q was accepted", bind)
			continue
		}
		if !strings.Contains(err.Error(), "loopback") {
			t.Errorf("bind %q: error = %v, want it to name the loopback rule", bind, err)
		}
	}
}

// An operator learns that the engine is unreachable from their terminal,
// not from a blank page in a browser they just opened.
func TestAnUnreachableEngineIsReportedBeforeAnythingIsServed(t *testing.T) {
	_, err := listen(context.Background(), Options{
		Bind: "127.0.0.1:0",
		// A port nothing is listening on. Port 1 on loopback is refused
		// promptly rather than timing out.
		Upstream: Upstream{Network: "tcp", Address: "127.0.0.1:1", Token: stubToken},
	}, fakeBundle(t))
	if err == nil {
		t.Fatal("listen succeeded against an engine that is not there")
	}
	if !strings.Contains(err.Error(), "attach server") {
		t.Errorf("error = %v, want it to say the attach server could not be reached", err)
	}
}

// A wrong token is a different failure from an absent engine, and an
// operator needs to be told which one they have.
func TestARefusedTokenIsReportedAsSuch(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = io.WriteString(w, "unauthorized\n")
	}))
	t.Cleanup(srv.Close)

	_, err := listen(context.Background(), Options{
		Bind:     "127.0.0.1:0",
		Upstream: Upstream{Network: "tcp", Address: strings.TrimPrefix(srv.URL, "http://"), Token: stubToken},
	}, fakeBundle(t))
	if err == nil {
		t.Fatal("listen succeeded against a server that refused the token")
	}
	if !strings.Contains(err.Error(), "token") {
		t.Errorf("error = %v, want it to name the token", err)
	}
}

// An asset name is matched against the embedded set, never joined onto a
// path, so there is no traversal to defend against. This asserts the
// property rather than the implementation.
func TestAnAssetNameCannotEscapeTheBundle(t *testing.T) {
	s, _ := newTestServer(t)
	c := handshake(t, s)

	for _, path := range []string{"/_ui/../server.go", "/_ui/%2e%2e%2fserver.go", "/_ui/nothing-here"} {
		resp := get(t, c, s, path, nil)
		if resp.StatusCode == http.StatusOK {
			t.Errorf("GET %s: status 200", path)
		}
	}
}

// The forwarded set is an allowlist, and a route not on it must not reach
// the engine even when it sits under /api.
func TestOnlyTheAllowlistedRoutesForward(t *testing.T) {
	for _, tc := range []struct {
		path string
		want bool
	}{
		{"/api/state", true},
		{"/api/stream", true},
		{"/api/plan", true},
		{"/api/logs/build", true},
		{"/api/logs/deploy%2Fapply", true},
		{"/api/control", false},
		{"/api/shell", false},
		{"/api/logs/", false},
		{"/api/logs", false},
		{"/api/state/extra", false},
		{"/", false},
	} {
		if got := forwardable(tc.path); got != tc.want {
			t.Errorf("forwardable(%q) = %v, want %v", tc.path, got, tc.want)
		}
	}
}

// The credential comparison must not leak the secret's length, which is
// what comparing digests rather than the values themselves guarantees.
func TestCredentialsCompareByDigest(t *testing.T) {
	c, err := newCredential()
	if err != nil {
		t.Fatalf("newCredential: %v", err)
	}
	if !c.accepts(c.value) {
		t.Fatal("a credential does not accept itself")
	}
	for _, wrong := range []string{"", "x", c.value + "x", c.value[:len(c.value)-1], strings.Repeat("y", 4096)} {
		if c.accepts(wrong) {
			t.Errorf("a credential accepted %q", wrong[:min(len(wrong), 16)])
		}
	}
	// Two credentials generated in a row are not the same one.
	other, err := newCredential()
	if err != nil {
		t.Fatalf("newCredential: %v", err)
	}
	if other.value == c.value {
		t.Fatal("two generated credentials are identical")
	}
}

// Whether this build actually carries a client is its own question, asked
// once. Both answers are legitimate: a fresh checkout has not run
// `make wasm` and must still compile and test clean, while a built tree
// must have every asset the page loads.
func TestTheBundleIsEitherCompleteOrSaysWhatIsMissing(t *testing.T) {
	b, err := loadBundle()
	if err != nil {
		if !errors.Is(err, ErrBundleMissing) {
			t.Fatalf("loadBundle: %v", err)
		}
		if !strings.Contains(err.Error(), "make wasm") {
			t.Errorf("error = %v, want it to say how to build the client", err)
		}
		t.Skip("this tree has not built the WebAssembly client; the rest of this test needs one")
	}
	for _, name := range []string{"index.html", "app.css", "boot.js", execAsset, clientAsset} {
		if _, ok := b.files[name]; !ok {
			t.Errorf("the bundle is missing %s", name)
		}
	}
	// The page's own markup names the URLs it loads, and a bundle that does
	// not carry them is a page that fails in a console.
	index := string(b.files["index.html"].body)
	for _, ref := range []string{"/_ui/app.css", "/_ui/wasm_exec.js", "/_ui/boot.js"} {
		if !strings.Contains(index, ref) {
			t.Errorf("index.html does not reference %s", ref)
		}
	}
	boot := string(b.files["boot.js"].body)
	if !strings.Contains(boot, "/_ui/senro-ui.wasm") {
		t.Error("boot.js does not load the client")
	}
}
