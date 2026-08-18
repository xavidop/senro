package attachsrv_test

import (
	"bufio"
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"errors"
	"io"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/internal/attachsrv"
	"github.com/xavidop/senro/internal/shellwire"
	"github.com/xavidop/senro/internal/sink"
)

// A TCP listener has no peer-credential check to fall back on, so the
// bearer token IS the boundary. Every test here dials a real TCP (or TLS)
// listener rather than calling a handler directly, because what is under
// test is exactly what a handler-level test would skip: whether the guard
// really sits in front of the mux, on every endpoint, including the one
// that hands somebody a command prompt.

// testToken is a stand-in for the per-run token attach.Listen generates. It
// is long enough to satisfy the server's own minimum and is deliberately
// NOT random: a test that has to guess its own fixture proves nothing.
const testToken = "MpQ7bqTn6Rr1yfKuHhVe0sZgWc4LxJd2AaBbCcDdEeF"

// tcpTestServer is the TCP counterpart of server_test.go's testServer. It is
// its own type rather than a flag on that one because almost nothing is
// shared: there is no socket path, the client speaks http(s) to a host:port,
// and every request has to carry a credential.
type tcpTestServer struct {
	srv   *attachsrv.Server
	hub   *attachsrv.Hub
	dir   string
	base  string // "http://127.0.0.1:PORT" or "https://..."
	token string
	tls   *tls.Config // client side; nil for plaintext
}

type tcpServerOpts struct {
	// TLS makes the server speak TLS on a freshly generated, test-only
	// certificate, and gives the client a root pool that trusts it.
	TLS bool
	// Bind overrides the default loopback bind. Used by the tests that prove
	// the non-loopback policy.
	Bind string
	// Token overrides testToken, including with "" for the tests that prove
	// a TCP listener refuses to start without one.
	Token    string
	NoToken  bool
	ReadOnly bool
}

func newTCPTestServer(t *testing.T, opts tcpServerOpts) *tcpTestServer {
	t.Helper()
	srv, ts, err := tryTCPTestServer(t, opts)
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	_ = srv
	return ts
}

// tryTCPTestServer is newTCPTestServer's non-fatal form, for the tests whose
// whole assertion is that Listen REFUSES.
func tryTCPTestServer(t *testing.T, opts tcpServerOpts) (*attachsrv.Server, *tcpTestServer, error) {
	t.Helper()

	dir := t.TempDir()
	hub := attachsrv.NewHub(64)

	bind := opts.Bind
	if bind == "" {
		bind = "127.0.0.1:0"
	}
	token := opts.Token
	if token == "" && !opts.NoToken {
		token = testToken
	}

	o := attachsrv.Options{
		Bind:     bind,
		Network:  attachsrv.NetworkTCP,
		Token:    token,
		Dir:      dir,
		Hub:      hub,
		ReadOnly: opts.ReadOnly,
	}
	var clientTLS *tls.Config
	if opts.TLS {
		serverCfg, pool := selfSignedPair(t)
		o.TLSConfig = serverCfg
		clientTLS = &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}
	}

	srv, err := attachsrv.Listen(context.Background(), o)
	if err != nil {
		return nil, nil, err
	}
	t.Cleanup(func() { _ = srv.Close() })

	scheme := "http"
	if opts.TLS {
		scheme = "https"
	}
	return srv, &tcpTestServer{
		srv:   srv,
		hub:   hub,
		dir:   dir,
		base:  scheme + "://" + srv.Addr(),
		token: token,
		tls:   clientTLS,
	}, nil
}

// client builds a fresh http.Client for this server. Fresh per call, and
// never shared: a pooled connection would let one test's authenticated
// request ride on a connection another test already opened, which is exactly
// the confusion these tests exist to rule out.
func (ts *tcpTestServer) client(t *testing.T) *http.Client {
	t.Helper()
	tr := &http.Transport{TLSClientConfig: ts.tls}
	t.Cleanup(tr.CloseIdleConnections)
	return &http.Client{Transport: tr} // no blanket Timeout: streaming tests hold the body open
}

// get issues one GET, optionally presenting a bearer credential. token == ""
// means "present no Authorization header at all", which is a different case
// from presenting a wrong one and is tested as such.
func (ts *tcpTestServer) get(t *testing.T, path, token string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, ts.base+path, nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := ts.client(t).Do(req)
	if err != nil {
		t.Fatalf("GET %s: %v", path, err)
	}
	return resp
}

// --- The three tests that matter most ---

// A request with no credential at all reaches no handler. Every endpoint,
// not just the interesting one: a guard that covers /api/control and forgets
// /api/logs is a file-read primitive for anyone who can route to the port.
func TestTCPRefusesEveryEndpointWithNoToken(t *testing.T) {
	ts := newTCPTestServer(t, tcpServerOpts{})
	writePlanFileAt(t, ts.dir)

	for _, path := range []string{"/api/state", "/api/plan", "/api/logs/build", "/api/stream"} {
		resp := ts.get(t, path, "")
		// The status is checked before the body is touched, and the body is
		// only read once the status says this was a refusal. GET /api/stream
		// holds its response open for the life of the run by design, so a
		// build where the guard had been removed would hang here rather than
		// fail, and a test that hangs under the mutation it exists to catch
		// is not doing its job.
		if resp.StatusCode != http.StatusUnauthorized {
			_ = resp.Body.Close()
			t.Errorf("GET %s with no token: status = %d, want 401: an unauthenticated request must not reach a handler", path, resp.StatusCode)
			continue
		}
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		_ = resp.Body.Close()
		if bytes.Contains(body, []byte("plan")) || bytes.Contains(body, []byte("seq")) {
			t.Errorf("GET %s with no token returned %q: the refusal leaked handler output", path, body)
		}
	}

	// POST /api/control is the one that can act on the run, so it gets its
	// own assertion rather than riding on the loop above.
	req, err := http.NewRequest(http.MethodPost, ts.base+"/api/control",
		strings.NewReader(`{"v":1,"kind":"req","id":"1","type":"run.cancel"}`))
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	resp, err := ts.client(t).Do(req)
	if err != nil {
		t.Fatalf("POST /api/control: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("POST /api/control with no token: status = %d, want 401", resp.StatusCode)
	}
	if got := ts.srv.AuthRejected(); got == 0 {
		t.Error("AuthRejected() = 0 after several refused requests: a refusal nobody can count is a refusal nobody can notice")
	}
}

// A wrong credential is refused exactly like a missing one, and a right one
// is not. Without the positive half this test would pass against a server
// that refused everything.
func TestTCPRefusesAWrongTokenAndAcceptsTheRightOne(t *testing.T) {
	ts := newTCPTestServer(t, tcpServerOpts{})

	for _, wrong := range []string{
		"",                                  // no header at all
		"x",                                 // far too short
		strings.Repeat("z", len(testToken)), // right length, every byte wrong
		testToken[:len(testToken)-1] + "X",  // differs only in the LAST byte
		"X" + testToken[1:],                 // differs only in the FIRST byte
		testToken + "extra",                 // a correct prefix, then more
		strings.ToUpper(testToken),          // case-folded
		strings.Repeat(testToken, 200),      // absurdly long
	} {
		resp := ts.get(t, "/api/state", wrong)
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusUnauthorized {
			t.Errorf("GET /api/state with token %q: status = %d, want 401", truncate(wrong), resp.StatusCode)
		}
	}

	resp := ts.get(t, "/api/state", ts.token)
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /api/state with the right token: status = %d, want 200: the guard must let the operator through", resp.StatusCode)
	}
	var st api.RunState
	if err := json.NewDecoder(resp.Body).Decode(&st); err != nil {
		t.Fatalf("decode state: %v", err)
	}
}

// "Wrong token" and "no token" must be indistinguishable on the wire, and
// neither may say anything about whether this port has a run behind it at
// all. Byte-identical: status, the headers a client can see, and the body.
func TestAWrongTokenIsRefusedIdenticallyToAMissingOne(t *testing.T) {
	ts := newTCPTestServer(t, tcpServerOpts{})

	missing := ts.get(t, "/api/state", "")
	missingBody, _ := io.ReadAll(missing.Body)
	_ = missing.Body.Close()

	wrong := ts.get(t, "/api/state", strings.Repeat("q", len(testToken)))
	wrongBody, _ := io.ReadAll(wrong.Body)
	_ = wrong.Body.Close()

	if missing.StatusCode != wrong.StatusCode {
		t.Errorf("status: missing = %d, wrong = %d: the two refusals must be indistinguishable",
			missing.StatusCode, wrong.StatusCode)
	}
	if !bytes.Equal(missingBody, wrongBody) {
		t.Errorf("body: missing = %q, wrong = %q: the two refusals must be byte-identical", missingBody, wrongBody)
	}
	for _, h := range []string{"WWW-Authenticate", "Content-Type", "Content-Length"} {
		if missing.Header.Get(h) != wrong.Header.Get(h) {
			t.Errorf("header %s: missing = %q, wrong = %q: the two refusals must be indistinguishable",
				h, missing.Header.Get(h), wrong.Header.Get(h))
		}
	}
	// And neither may name the run, the pipeline, or anything else that
	// tells an unauthenticated caller they found a senro engine rather than
	// some other 401.
	for _, leak := range []string{"run", "pipeline", "senro", "token"} {
		if bytes.Contains(bytes.ToLower(missingBody), []byte(leak)) {
			t.Errorf("the refusal body %q mentions %q: it must not distinguish 'no such run' from 'wrong token'", missingBody, leak)
		}
	}
}

func truncate(s string) string {
	if len(s) <= 24 {
		return s
	}
	return s[:24] + "..."
}

// --- The bind policy ---

// A TCP listener with no token is a control channel for anyone who can route
// to the port. Refusing at bind time is the only place that cannot be
// forgotten later.
func TestATCPBindWithoutATokenIsRefused(t *testing.T) {
	_, _, err := tryTCPTestServer(t, tcpServerOpts{NoToken: true})
	if err == nil {
		t.Fatal("Listen with Network tcp and no Token succeeded, want a refusal")
	}
	if !errors.Is(err, attachsrv.ErrTokenRequired) {
		t.Fatalf("Listen err = %v, want ErrTokenRequired", err)
	}
}

// A token short enough to be guessable is worse than none, because it looks
// like protection.
func TestATCPBindWithATooShortTokenIsRefused(t *testing.T) {
	_, _, err := tryTCPTestServer(t, tcpServerOpts{Token: "short"})
	if err == nil {
		t.Fatal("Listen with a 5-character token succeeded, want a refusal")
	}
	if !errors.Is(err, attachsrv.ErrTokenRequired) {
		t.Fatalf("Listen err = %v, want ErrTokenRequired", err)
	}
}

// Loopback plus a token is allowed without TLS: the traffic never leaves the
// machine, and an adversary who can capture loopback can already read this
// process's memory.
func TestALoopbackTCPBindWithoutTLSIsAllowed(t *testing.T) {
	ts := newTCPTestServer(t, tcpServerOpts{Bind: "127.0.0.1:0"})
	resp := ts.get(t, "/api/state", ts.token)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200 over a plaintext loopback bind", resp.StatusCode)
	}
}

// Anything that is not loopback carries the token across a network, and the
// token is a command prompt. There is no opt-out flag for this on purpose:
// a flag is something a person copy-pastes past.
func TestANonLoopbackTCPBindWithoutTLSIsRefused(t *testing.T) {
	for _, bind := range []string{"0.0.0.0:0", ":0", "[::]:0"} {
		_, _, err := tryTCPTestServer(t, tcpServerOpts{Bind: bind})
		if err == nil {
			t.Errorf("Listen on %q without TLS succeeded, want a refusal", bind)
			continue
		}
		if !errors.Is(err, attachsrv.ErrTLSRequired) {
			t.Errorf("Listen on %q: err = %v, want ErrTLSRequired", bind, err)
		}
	}
}

// The refusal above is about TLS, not about the address: the same bind with
// a certificate works.
func TestANonLoopbackTCPBindWithTLSIsAllowed(t *testing.T) {
	ts := newTCPTestServer(t, tcpServerOpts{Bind: "127.0.0.1:0", TLS: true})
	resp := ts.get(t, "/api/state", ts.token)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200 over TLS", resp.StatusCode)
	}
	if !strings.HasPrefix(ts.base, "https://") {
		t.Fatalf("base = %q, want an https endpoint", ts.base)
	}
}

// The token is a TCP-transport mechanism. Accepting one on a unix bind would
// invite the belief that it is what protects that socket, when the
// peer-credential check is, so it is refused rather than quietly ignored.
func TestAUnixBindWithATokenIsRefused(t *testing.T) {
	dir := t.TempDir()
	_, err := attachsrv.Listen(context.Background(), attachsrv.Options{
		Bind:  shortSocketPath(t),
		Token: testToken,
		Dir:   dir,
		Hub:   attachsrv.NewHub(64),
	})
	if err == nil {
		t.Fatal("Listen with a unix Bind and a Token succeeded, want a refusal")
	}
	if !strings.Contains(err.Error(), "unix") {
		t.Fatalf("err = %v, want one naming the unix transport", err)
	}
}

// The unix path must not have grown a credential requirement: it is still
// the default, and its boundary is still the peer check.
func TestAUnixListenerStillNeedsNoToken(t *testing.T) {
	ts := newTestServer(t, testServerOpts{})
	resp, err := ts.client.Get(ts.url("/api/state"))
	if err != nil {
		t.Fatalf("GET /api/state over unix: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d over a unix socket with no Authorization header, want 200", resp.StatusCode)
	}
}

// --- Every operation, over TCP ---

func TestEveryOperationWorksOverTCPWithTheToken(t *testing.T) {
	ts := newTCPTestServer(t, tcpServerOpts{})
	writePlanFileAt(t, ts.dir)
	writeLogFile(t, ts.dir, "build", 1, api.StreamStdout, "hello from a log\n")

	client := ts.client(t)
	do := func(path string) *http.Response {
		t.Helper()
		req, err := http.NewRequest(http.MethodGet, ts.base+path, nil)
		if err != nil {
			t.Fatalf("NewRequest: %v", err)
		}
		req.Header.Set("Authorization", "Bearer "+ts.token)
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("GET %s: %v", path, err)
		}
		return resp
	}

	for _, path := range []string{"/api/state", "/api/plan", "/api/logs/build?stream=stdout"} {
		resp := do(path)
		body, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Errorf("GET %s: status = %d, want 200 (%s)", path, resp.StatusCode, body)
		}
	}

	// The stream, which is the endpoint a token guard is easiest to
	// accidentally break: it holds its response open.
	streamResp := do("/api/stream?from=1")
	defer func() { _ = streamResp.Body.Close() }()
	if streamResp.StatusCode != http.StatusOK {
		t.Fatalf("GET /api/stream: status = %d, want 200", streamResp.StatusCode)
	}
	ts.hub.Emit(api.Event{V: 1, Seq: 1, Type: api.RunStarted})
	line, err := bufio.NewReader(streamResp.Body).ReadBytes('\n')
	if err != nil {
		t.Fatalf("read one streamed event: %v", err)
	}
	var e api.Event
	if err := json.Unmarshal(line, &e); err != nil {
		t.Fatalf("decode streamed event %q: %v", line, err)
	}
	if e.Type != api.RunStarted {
		t.Errorf("streamed event type = %q, want %q", e.Type, api.RunStarted)
	}

	// Control, answered by a stand-in for the engine's own scheduler.
	go func() {
		req := <-ts.hub.Control()
		req.Reply <- sink.ControlResponse{ID: req.ID, OK: true}
	}()
	ctrlReq, err := http.NewRequest(http.MethodPost, ts.base+"/api/control",
		strings.NewReader(`{"v":1,"kind":"req","id":"c1","type":"run.cancel"}`))
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	ctrlReq.Header.Set("Authorization", "Bearer "+ts.token)
	ctrlResp, err := client.Do(ctrlReq)
	if err != nil {
		t.Fatalf("POST /api/control: %v", err)
	}
	defer func() { _ = ctrlResp.Body.Close() }()
	if ctrlResp.StatusCode != http.StatusOK {
		t.Fatalf("POST /api/control: status = %d, want 200", ctrlResp.StatusCode)
	}
	var frame api.Frame
	if err := json.NewDecoder(ctrlResp.Body).Decode(&frame); err != nil {
		t.Fatalf("decode control response: %v", err)
	}
	if frame.OK == nil || !*frame.OK {
		t.Errorf("control response OK = %v, want true", frame.OK)
	}
}

// senro shell over TCP hands somebody an interactive prompt inside a step's
// workspace, across a network. It works, deliberately and documentedly, and
// it is behind the same token as everything else. This test proves both
// halves: the credential is required, and with it the session runs.
func TestShellOverTCPNeedsTheTokenAndThenWorks(t *testing.T) {
	ts := newTCPTestServer(t, tcpServerOpts{})
	go func() {
		for req := range ts.hub.Shells() {
			go func(req sink.ShellRequest) {
				_, _ = io.Copy(req.Stdout, req.Stdin)
				req.Reply <- sink.ShellResponse{ID: req.ID, OK: true, Session: "s1"}
			}(req)
		}
	}()

	// No token: refused before the upgrade, with no 101 anywhere.
	conn, _, resp := dialShellTCP(t, ts, "step=build", "")
	_ = conn.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("POST /api/shell with no token: status = %d, want 401: an unauthenticated caller must never get a prompt", resp.StatusCode)
	}

	// With the token: a real session.
	conn2, br, resp2 := dialShellTCP(t, ts, "step=build", ts.token)
	defer func() { _ = conn2.Close() }()
	if resp2.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("POST /api/shell with the token: status = %d, want 101", resp2.StatusCode)
	}
	w := shellwire.NewWriter(conn2)
	if err := w.WriteFrame(shellwire.StreamStdin, []byte("echo over tcp\n")); err != nil {
		t.Fatalf("write stdin frame: %v", err)
	}
	if err := w.WriteFrame(shellwire.StreamStdinEOF, nil); err != nil {
		t.Fatalf("write eof frame: %v", err)
	}
	frames := shellwire.NewReader(br)
	var out bytes.Buffer
	for {
		stream, payload, err := frames.ReadFrame()
		if err != nil {
			t.Fatalf("read frame: %v", err)
		}
		if stream == shellwire.StreamExit {
			break
		}
		out.Write(payload)
	}
	if got := out.String(); !strings.Contains(got, "echo over tcp") {
		t.Errorf("session echoed %q, want it to carry the typed line back over TCP", got)
	}
}

// dialShellTCP is shell_test.go's dialShell over TCP, with a credential.
func dialShellTCP(t *testing.T, ts *tcpTestServer, query, token string) (net.Conn, *bufio.Reader, *http.Response) {
	t.Helper()
	host := strings.TrimPrefix(strings.TrimPrefix(ts.base, "https://"), "http://")
	var conn net.Conn
	var err error
	if ts.tls != nil {
		conn, err = tls.Dial("tcp", host, ts.tls)
	} else {
		conn, err = net.Dial("tcp", host)
	}
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	req, err := http.NewRequest(http.MethodPost, ts.base+"/api/shell?"+query, nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	req.Header.Set("Connection", "Upgrade")
	req.Header.Set("Upgrade", shellwire.Protocol)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	if err := req.Write(conn); err != nil {
		t.Fatalf("write request: %v", err)
	}
	br := bufio.NewReader(conn)
	resp, err := http.ReadResponse(br, req)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	return conn, br, resp
}

// --- Bounded authentication attempts ---

// A flood of wrong credentials must cost the attacker something and must not
// cost the operator their run. Past the burst, refusals stop being evaluated
// at all; a correct credential still goes straight through.
func TestFailedAuthenticationIsBoundedAndDoesNotLockOutTheOperator(t *testing.T) {
	ts := newTCPTestServer(t, tcpServerOpts{})

	var sawThrottled bool
	for i := 0; i < attachsrv.AuthFailureBurst+5; i++ {
		resp := ts.get(t, "/api/state", "wrong-but-plausible-looking-credential-value")
		_ = resp.Body.Close()
		if resp.StatusCode == http.StatusTooManyRequests {
			sawThrottled = true
		}
	}
	if !sawThrottled {
		t.Fatalf("%d consecutive failed authentications were never throttled: an unauthenticated peer must not get unbounded attempts for free",
			attachsrv.AuthFailureBurst+5)
	}

	resp := ts.get(t, "/api/state", ts.token)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d for the RIGHT token after a flood of wrong ones, want 200: throttling failures must never lock out the operator",
			resp.StatusCode)
	}
}

// --- helpers ---

// writePlanFileAt writes a minimal plan.json so /api/plan has something to
// serve. Kept separate from server_test.go's writePlanFile, which takes a
// *plan.Plan this file has no need to construct.
func writePlanFileAt(t *testing.T, dir string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, "plan.json"), []byte(`{"version":1,"nodes":[]}`), 0o644); err != nil {
		t.Fatalf("write plan.json: %v", err)
	}
}

// selfSignedPair mints a throwaway certificate for 127.0.0.1 and returns the
// server's tls.Config plus a root pool that trusts it. Test-only: production
// requires an operator-supplied certificate precisely so that nothing has to
// be told to trust a key it just met.
func selfSignedPair(t *testing.T) (*tls.Config, *x509.CertPool) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "senro attach test"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")},
		DNSNames:              []string{"localhost"},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("CreateCertificate: %v", err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("MarshalECPrivateKey: %v", err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})

	pair, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatalf("X509KeyPair: %v", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(certPEM) {
		t.Fatal("AppendCertsFromPEM: the generated certificate did not parse")
	}
	return &tls.Config{Certificates: []tls.Certificate{pair}, MinVersion: tls.VersionTLS12}, pool
}
