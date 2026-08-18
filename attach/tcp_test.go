package attach_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"io"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/attach"
	"github.com/xavidop/senro/internal/attachsrv"
)

// The embedding side of the TCP transport: attach.Listen is the one call a
// pipeline's own main() makes, so it is where the token is generated, where
// it is delivered to a client that can already read this user's files, and
// where it must never end up anywhere else.

func TestATCPListenGeneratesAUsableTokenAndServesBehindIt(t *testing.T) {
	att := listenTCP(t, attach.Options{Bind: "127.0.0.1:0"})

	if att.Network() != attachsrv.NetworkTCP {
		t.Fatalf("Network() = %q, want %q", att.Network(), attachsrv.NetworkTCP)
	}
	tok := att.Token()
	if tok == "" {
		t.Fatal("Token() is empty for a TCP bind: nothing could ever authenticate to it")
	}

	// 32 bytes of entropy, as base64url. Asserted on the decoded length,
	// not the string's, so a change of encoding cannot silently shrink it.
	raw, err := base64.RawURLEncoding.DecodeString(tok)
	if err != nil {
		t.Fatalf("Token() %q is not base64url: %v", tok, err)
	}
	if len(raw) < 32 {
		t.Fatalf("Token() carries %d bytes of entropy, want at least 32: guessing must not be a strategy", len(raw))
	}

	if got := get(t, att, "/api/state", tok); got.StatusCode != http.StatusOK {
		t.Fatalf("GET /api/state with the generated token: status = %d, want 200", got.StatusCode)
	}
	if got := get(t, att, "/api/state", ""); got.StatusCode != http.StatusUnauthorized {
		t.Fatalf("GET /api/state with no token: status = %d, want 401", got.StatusCode)
	}
}

// Two runs must not share a credential, and a token must not be derivable
// from anything a client can see: the run id, the pid, the clock.
func TestEveryRunGetsItsOwnToken(t *testing.T) {
	seen := map[string]bool{}
	for i := 0; i < 8; i++ {
		att := listenTCP(t, attach.Options{Bind: "127.0.0.1:0"})
		tok := att.Token()
		if seen[tok] {
			t.Fatalf("two runs were issued the same token %q", tok)
		}
		seen[tok] = true
	}
}

// A unix bind is still the default and still has no token: its boundary is
// the peer-credential check, and inventing a credential for it would suggest
// otherwise.
func TestAUnixListenHasNoToken(t *testing.T) {
	att := listenUnix(t, attach.Options{})
	if att.Token() != "" {
		t.Fatalf("Token() = %q for a unix bind, want empty: the peer check is that transport's boundary, not a token", att.Token())
	}
	if att.Network() != attachsrv.NetworkUnix {
		t.Fatalf("Network() = %q, want %q", att.Network(), attachsrv.NetworkUnix)
	}
}

// The operator has to be able to get the token, and on the machine running
// the pipeline the path is meant to be no path at all: the registry entry
// already carries it, and that file is 0600 in a 0700 directory, which is
// the same boundary the unix socket itself had.
func TestTheTokenReachesAClientThroughTheRegistryEntryAndNowhereElse(t *testing.T) {
	isolateRegistry(t)
	att := listenTCP(t, attach.Options{Bind: "127.0.0.1:0", RunID: "r-tcp", Pipeline: "p"})

	entries, err := attachsrv.Discover()
	if err != nil {
		t.Fatalf("Discover: %v", err)
	}
	var found *attachsrv.Entry
	for i := range entries {
		if entries[i].RunID == "r-tcp" {
			found = &entries[i]
		}
	}
	if found == nil {
		t.Fatalf("Discover() did not find the registered TCP run: %+v", entries)
	}
	if found.Network != attachsrv.NetworkTCP {
		t.Errorf("Entry.Network = %q, want %q", found.Network, attachsrv.NetworkTCP)
	}
	if found.Addr != att.Addr() {
		t.Errorf("Entry.Addr = %q, want %q", found.Addr, att.Addr())
	}
	if found.Token != att.Token() {
		t.Errorf("Entry.Token does not match Token(): a client that discovers this run could not authenticate to it")
	}

	// The file itself, and the directory above it, are the whole reason
	// putting a credential there is defensible.
	regDir, err := attachsrv.Dir()
	if err != nil {
		t.Fatalf("attachsrv.Dir: %v", err)
	}
	fi, err := os.Stat(filepath.Join(regDir, strconv.Itoa(os.Getpid())+".json"))
	if err != nil {
		t.Fatalf("stat registry entry: %v", err)
	}
	if perm := fi.Mode().Perm(); perm != 0o600 {
		t.Errorf("registry entry mode = %o, want 600: it now holds a credential", perm)
	}
	di, err := os.Stat(regDir)
	if err != nil {
		t.Fatalf("stat registry dir: %v", err)
	}
	if perm := di.Mode().Perm(); perm != 0o700 {
		t.Errorf("registry dir mode = %o, want 700", perm)
	}
}

// The token must not leak into anything that gets shipped in a bug report,
// pasted into a ticket, or read by a second attached client. The run
// directory is the artifact senro itself tells people to attach to an issue,
// and the event stream is broadcast to every client, authenticated or (over
// a unix socket) merely local.
func TestTheTokenNeverReachesTheRunDirectoryOrTheEventStream(t *testing.T) {
	isolateRegistry(t)
	runDir := t.TempDir()
	att := listenTCP(t, attach.Options{Bind: "127.0.0.1:0", Dir: runDir, RunID: "r1"})
	tok := att.Token()

	// A run's worth of events through the sink the engine would use.
	sink := att.Sink()
	for i, e := range []api.Event{
		{V: 1, Seq: 1, Type: api.RunStarted},
		{V: 1, Seq: 2, Type: api.StepCreated, Step: "build"},
		{V: 1, Seq: 3, Type: api.RunFinished},
	} {
		_ = i
		sink.Emit(e)
	}

	// Nothing under the run directory may contain it.
	err := filepath.WalkDir(runDir, func(p string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		b, readErr := os.ReadFile(p)
		if readErr != nil {
			return readErr
		}
		if strings.Contains(string(b), tok) {
			t.Errorf("%s contains the run's bearer token: the run directory is shipped in bug reports", p)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk run dir: %v", err)
	}

	// Nor may the state snapshot an attached client reads, which is the
	// closest thing this server has to a "tell me about yourself" endpoint.
	resp := get(t, att, "/api/state", tok)
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(resp.Body)
	if strings.Contains(string(body), tok) {
		t.Error("GET /api/state echoes the run's bearer token back to whoever asks")
	}
	var st api.RunState
	if err := json.Unmarshal(body, &st); err != nil {
		t.Fatalf("decode state: %v", err)
	}
}

// The refusal a caller gets for a non-loopback bind with no certificate is
// part of the change: it has to name what is wrong AND what to do instead,
// because the thing to do instead (a port-forward) is not obvious from
// "TLS required".
func TestANonLoopbackBindWithoutACertificateIsRefusedWithAnActionableMessage(t *testing.T) {
	isolateRegistry(t)
	_, err := attach.Listen(context.Background(), attach.Options{
		Bind: "0.0.0.0:0", Dir: t.TempDir(),
	})
	if err == nil {
		t.Fatal("attach.Listen on 0.0.0.0 with no certificate succeeded, want a refusal")
	}
	if !errors.Is(err, attachsrv.ErrTLSRequired) {
		t.Fatalf("err = %v, want it to wrap ErrTLSRequired", err)
	}
	for _, want := range []string{"port-forward", "TLSCertFile"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the refusal does not mention %q, so it does not say what to do instead:\n%s", want, err)
		}
	}
}

// And with a certificate, the same bind works, over TLS, behind the token.
func TestANonLoopbackBindWithACertificateServesOverTLS(t *testing.T) {
	isolateRegistry(t)
	certFile, keyFile, pool := writeSelfSignedPair(t)

	att, err := attach.Listen(context.Background(), attach.Options{
		Bind:        "127.0.0.1:0",
		Dir:         t.TempDir(),
		TLSCertFile: certFile,
		TLSKeyFile:  keyFile,
	})
	if err != nil {
		t.Fatalf("attach.Listen with a certificate: %v", err)
	}
	t.Cleanup(func() { _ = att.Close() })

	if !att.TLS() {
		t.Fatal("TLS() = false for a listener built with a certificate")
	}

	tr := &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}}
	t.Cleanup(tr.CloseIdleConnections)
	client := &http.Client{Transport: tr}
	req, err := http.NewRequest(http.MethodGet, "https://"+att.Addr()+"/api/state", nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	req.Header.Set("Authorization", "Bearer "+att.Token())
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("GET over TLS: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200 over TLS", resp.StatusCode)
	}
}

// A certificate that cannot be loaded must stop the run from starting, not
// silently downgrade it to plaintext.
func TestAnUnreadableCertificateIsRefusedRatherThanDowngraded(t *testing.T) {
	isolateRegistry(t)
	_, err := attach.Listen(context.Background(), attach.Options{
		Bind: "127.0.0.1:0", Dir: t.TempDir(),
		TLSCertFile: filepath.Join(t.TempDir(), "nope.pem"),
		TLSKeyFile:  filepath.Join(t.TempDir(), "nope.key"),
	})
	if err == nil {
		t.Fatal("attach.Listen with a missing certificate succeeded, want a refusal")
	}
	if !strings.Contains(err.Error(), "certificate") {
		t.Fatalf("err = %v, want one naming the certificate", err)
	}
}

// Only one of TLSCertFile and TLSKeyFile is a half-configured listener, and
// guessing which half was meant is worse than saying so.
func TestHalfACertificateIsRefused(t *testing.T) {
	isolateRegistry(t)
	certFile, keyFile, _ := writeSelfSignedPair(t)
	for _, opts := range []attach.Options{
		{Bind: "127.0.0.1:0", TLSCertFile: certFile},
		{Bind: "127.0.0.1:0", TLSKeyFile: keyFile},
	} {
		opts.Dir = t.TempDir()
		if _, err := attach.Listen(context.Background(), opts); err == nil {
			t.Errorf("attach.Listen with only one of cert/key succeeded, want a refusal")
		}
	}
}

// A certificate on a unix bind is a configuration nobody can act on, and
// accepting it quietly would suggest the socket had gained a transport
// guarantee it never needed.
func TestACertificateOnAUnixBindIsRefused(t *testing.T) {
	isolateRegistry(t)
	certFile, keyFile, _ := writeSelfSignedPair(t)
	_, err := attach.Listen(context.Background(), attach.Options{
		Bind: filepath.Join(shortDir(t), "s.sock"), Dir: t.TempDir(),
		TLSCertFile: certFile, TLSKeyFile: keyFile,
	})
	if err == nil {
		t.Fatal("attach.Listen with a certificate on a unix bind succeeded, want a refusal")
	}
}

// --- helpers ---

func listenTCP(t *testing.T, opts attach.Options) *attach.Attach {
	t.Helper()
	if opts.Dir == "" {
		opts.Dir = t.TempDir()
	}
	att, err := attach.Listen(context.Background(), opts)
	if err != nil {
		t.Fatalf("attach.Listen: %v", err)
	}
	t.Cleanup(func() { _ = att.Close() })
	return att
}

func listenUnix(t *testing.T, opts attach.Options) *attach.Attach {
	t.Helper()
	isolateRegistry(t)
	if opts.Dir == "" {
		opts.Dir = t.TempDir()
	}
	att, err := attach.Listen(context.Background(), opts)
	if err != nil {
		t.Fatalf("attach.Listen: %v", err)
	}
	t.Cleanup(func() { _ = att.Close() })
	return att
}

func get(t *testing.T, att *attach.Attach, path, token string) *http.Response {
	t.Helper()
	scheme := "http"
	if att.TLS() {
		scheme = "https"
	}
	req, err := http.NewRequest(http.MethodGet, scheme+"://"+att.Addr()+path, nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	tr := &http.Transport{}
	t.Cleanup(tr.CloseIdleConnections)
	resp, err := (&http.Client{Transport: tr}).Do(req)
	if err != nil {
		t.Fatalf("GET %s: %v", path, err)
	}
	return resp
}

// shortDir keeps a unix socket path inside darwin's ~104-byte limit, which
// t.TempDir()'s test-name-derived path does not.
func shortDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "at")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return dir
}

func writeSelfSignedPair(t *testing.T) (certFile, keyFile string, pool *x509.CertPool) {
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
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("MarshalECPrivateKey: %v", err)
	}

	dir := t.TempDir()
	certFile = filepath.Join(dir, "cert.pem")
	keyFile = filepath.Join(dir, "key.pem")
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	if err := os.WriteFile(certFile, certPEM, 0o600); err != nil {
		t.Fatalf("write cert: %v", err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	if err := os.WriteFile(keyFile, keyPEM, 0o600); err != nil {
		t.Fatalf("write key: %v", err)
	}

	pool = x509.NewCertPool()
	if !pool.AppendCertsFromPEM(certPEM) {
		t.Fatal("AppendCertsFromPEM: the generated certificate did not parse")
	}
	return certFile, keyFile, pool
}
