package source_test

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/internal/attachsrv"
	"github.com/xavidop/senro/internal/sink"
	"github.com/xavidop/senro/internal/source"
)

// The client half of the TCP transport. The narrow thing no server-side
// test would catch: EVERY request this Source makes has to carry the
// credential, including the two that do not go through its http.Client
// (Control builds its own request; Shell dials by hand). A Source that
// authenticated four endpoints out of six would look fine until somebody
// pressed r.

// recordingServer stands in for an attach server over real TCP: it records
// the Authorization header of every request and answers plausibly.
type recordingServer struct {
	mu   sync.Mutex
	seen map[string]string // path -> Authorization header, "" if absent
	srv  *httptest.Server
}

func newRecordingServer(t *testing.T, useTLS bool) *recordingServer {
	t.Helper()
	rs := &recordingServer{seen: map[string]string{}}
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rs.mu.Lock()
		rs.seen[r.URL.Path] = r.Header.Get("Authorization")
		rs.mu.Unlock()

		switch {
		case r.URL.Path == "/api/state":
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"v":1,"seq":3,"run":{"done":false},"proto_major":1,"proto_minor":0}`)
		case r.URL.Path == "/api/stream":
			w.Header().Set("Content-Type", "application/x-ndjson")
			w.WriteHeader(http.StatusOK)
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
			_, _ = io.WriteString(w, `{"v":1,"seq":4,"type":"run.started"}`+"\n")
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
		case strings.HasPrefix(r.URL.Path, "/api/logs/"):
			_, _ = io.WriteString(w, "log bytes\n")
		case r.URL.Path == "/api/control":
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"v":1,"kind":"res","id":"c1","ok":true}`)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	})
	if useTLS {
		rs.srv = httptest.NewTLSServer(h)
	} else {
		rs.srv = httptest.NewServer(h)
	}
	t.Cleanup(rs.srv.Close)
	return rs
}

func (rs *recordingServer) authFor(path string) (string, bool) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	v, ok := rs.seen[path]
	return v, ok
}

func (rs *recordingServer) hostPort() string {
	return strings.TrimPrefix(strings.TrimPrefix(rs.srv.URL, "https://"), "http://")
}

func (rs *recordingServer) rootPool() *x509.CertPool {
	pool := x509.NewCertPool()
	pool.AddCert(rs.srv.Certificate())
	return pool
}

const clientTestToken = "MpQ7bqTn6Rr1yfKuHhVe0sZgWc4LxJd2AaBbCcDdEeF"

func TestEveryRequestOverTCPCarriesTheBearerToken(t *testing.T) {
	rs := newRecordingServer(t, false)

	ls, err := source.DialEndpoint(context.Background(), source.Endpoint{
		Network: "tcp",
		Address: rs.hostPort(),
		Token:   clientTestToken,
	})
	if err != nil {
		t.Fatalf("DialEndpoint: %v", err)
	}
	defer func() { _ = ls.Close() }()

	ctx := context.Background()
	if _, err := ls.State(ctx); err != nil {
		t.Fatalf("State: %v", err)
	}
	rc, err := ls.Logs(ctx, "build", 1, api.StreamStdout, 0)
	if err != nil {
		t.Fatalf("Logs: %v", err)
	}
	_, _ = io.Copy(io.Discard, rc)
	_ = rc.Close()
	if _, err := ls.Control(ctx, api.Frame{V: 1, Kind: api.KindReq, ID: "c1", Type: api.OpRunCancel}); err != nil {
		t.Fatalf("Control: %v", err)
	}
	events, err := ls.Subscribe(ctx, 1)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	for range events {
	}

	want := "Bearer " + clientTestToken
	for _, path := range []string{"/api/state", "/api/logs/build", "/api/control", "/api/stream"} {
		got, seen := rs.authFor(path)
		if !seen {
			t.Errorf("%s was never requested, so this test proves nothing about it", path)
			continue
		}
		if got != want {
			t.Errorf("%s sent Authorization %q, want %q: an unauthenticated request would be refused by a real server",
				path, got, want)
		}
	}
}

// A unix Source must not start sending an Authorization header just because
// the field exists: nothing on that transport reads it, and a credential
// travelling where nothing checks it is a credential in one more place than
// it needs to be.
func TestAUnixSourceSendsNoAuthorizationHeader(t *testing.T) {
	sockPath := shortSocketPath(t)
	seen := make(chan string, 8)
	ln, err := net.Listen("unix", sockPath)
	if err != nil {
		t.Fatalf("listen unix: %v", err)
	}
	srv := &http.Server{
		ReadHeaderTimeout: 10 * time.Second,
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			seen <- r.Header.Get("Authorization")
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"v":1,"seq":1,"run":{"done":false}}`)
		}),
	}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() { _ = srv.Close() })

	ls, err := source.Dial(context.Background(), sockPath)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = ls.Close() }()
	if _, err := ls.State(context.Background()); err != nil {
		t.Fatalf("State: %v", err)
	}
	if got := <-seen; got != "" {
		t.Fatalf("a unix Source sent Authorization %q, want none", got)
	}
}

// TLS is not a separate code path a caller has to remember to take: the
// same Endpoint, with a tls.Config, dials https and still authenticates.
func TestATLSEndpointDialsHTTPSAndStillAuthenticates(t *testing.T) {
	rs := newRecordingServer(t, true)

	ls, err := source.DialEndpoint(context.Background(), source.Endpoint{
		Network: "tcp",
		Address: rs.hostPort(),
		Token:   clientTestToken,
		TLS:     &tls.Config{RootCAs: rs.rootPool(), MinVersion: tls.VersionTLS12},
	})
	if err != nil {
		t.Fatalf("DialEndpoint: %v", err)
	}
	defer func() { _ = ls.Close() }()

	if _, err := ls.State(context.Background()); err != nil {
		t.Fatalf("State over TLS: %v", err)
	}
	if got, _ := rs.authFor("/api/state"); got != "Bearer "+clientTestToken {
		t.Fatalf("Authorization over TLS = %q, want the bearer token", got)
	}
}

// A certificate the client has no reason to trust must fail, not be
// accepted: an Endpoint that skipped verification would hand the token to
// whoever answered the port.
func TestATLSEndpointRefusesAnUntrustedCertificate(t *testing.T) {
	rs := newRecordingServer(t, true)

	ls, err := source.DialEndpoint(context.Background(), source.Endpoint{
		Network: "tcp",
		Address: rs.hostPort(),
		Token:   clientTestToken,
		TLS:     &tls.Config{MinVersion: tls.VersionTLS12}, // system roots only
	})
	if err != nil {
		return // refused at dial time is also a correct answer
	}
	defer func() { _ = ls.Close() }()
	if _, err := ls.State(context.Background()); err == nil {
		t.Fatal("State against a server with an untrusted certificate succeeded, want a verification failure")
	}
	if got, seen := rs.authFor("/api/state"); seen {
		t.Fatalf("the token reached a server whose certificate did not verify (Authorization %q)", got)
	}
}

// A TCP endpoint with no token is a client that will be refused by every
// real server, so it is refused here, where the message can say why, rather
// than at the first 401.
func TestATCPEndpointWithoutATokenIsRefused(t *testing.T) {
	rs := newRecordingServer(t, false)
	_, err := source.DialEndpoint(context.Background(), source.Endpoint{
		Network: "tcp",
		Address: rs.hostPort(),
	})
	if err == nil {
		t.Fatal("DialEndpoint with no token succeeded, want a refusal naming the missing credential")
	}
	if !strings.Contains(err.Error(), "token") {
		t.Fatalf("err = %v, want one naming the token", err)
	}
}

// The real client against the real server, over TCP, all the way through a
// session: the tests above use a hand-written server, attachsrv's own TCP
// test a hand-written client, and only this arrangement notices the two
// drifting apart.
func TestAShellRoundTripsOverTCPThroughARealServer(t *testing.T) {
	const tok = clientTestToken
	dir := t.TempDir()
	hub := attachsrv.NewHub(64)
	srv, err := attachsrv.Listen(context.Background(), attachsrv.Options{
		Bind: "127.0.0.1:0", Network: attachsrv.NetworkTCP, Token: tok, Dir: dir, Hub: hub,
	})
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	t.Cleanup(func() { _ = srv.Close() })

	stop := make(chan struct{})
	t.Cleanup(func() { close(stop) })
	go func() {
		for {
			select {
			case req := <-hub.Shells():
				go func(req sink.ShellRequest) {
					b, _ := io.ReadAll(req.Stdin)
					_, _ = req.Stdout.Write([]byte("saw: " + string(b)))
					req.Reply <- sink.ShellResponse{ID: req.ID, OK: true, Session: "s9", ExitCode: 7}
				}(req)
			case <-stop:
				return
			}
		}
	}()

	ls, err := source.DialEndpoint(context.Background(), source.Endpoint{
		Network: attachsrv.NetworkTCP, Address: srv.Addr(), Token: tok,
	})
	if err != nil {
		t.Fatalf("DialEndpoint: %v", err)
	}
	t.Cleanup(func() { _ = ls.Close() })

	var out bytes.Buffer
	res, err := ls.Shell(context.Background(), source.ShellRequest{
		Step:   "build",
		Stdin:  strings.NewReader("typed over tcp\n"),
		Stdout: &out,
		Stderr: io.Discard,
	})
	if err != nil {
		t.Fatalf("Shell over TCP: %v", err)
	}
	if !res.OK || res.ExitCode != 7 {
		t.Errorf("ShellResult = %+v, want OK with exit 7", res)
	}
	if got := out.String(); !strings.Contains(got, "typed over tcp") {
		t.Errorf("stdout = %q, want the typed line echoed back over TCP", got)
	}

	// And the same client with the wrong credential gets nothing: the
	// refusal lands before the upgrade, so there is never a prompt to reach.
	bad, err := source.DialEndpoint(context.Background(), source.Endpoint{
		Network: attachsrv.NetworkTCP, Address: srv.Addr(), Token: strings.Repeat("w", len(tok)),
	})
	if err != nil {
		t.Fatalf("DialEndpoint with a wrong token: %v", err)
	}
	t.Cleanup(func() { _ = bad.Close() })
	if _, err := bad.Shell(context.Background(), source.ShellRequest{
		Step: "build", Stdin: strings.NewReader(""), Stdout: io.Discard, Stderr: io.Discard,
	}); err == nil {
		t.Fatal("Shell with a wrong token succeeded: an unauthenticated caller must never get a prompt")
	}
}
