package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/internal/attachsrv"
	"github.com/xavidop/senro/internal/source"
	"github.com/xavidop/senro/internal/webui"
)

// --- Flag handling -------------------------------------------------------

// A flag combination that cannot mean one thing must not be resolved by
// silently ignoring half of it, and which half is not something a person
// should have to guess.
func TestUIRefusesFlagCombinationsThatCannotMeanOneThing(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
		want string
	}{
		{"pid and run", []string{"--pid", "1", "--run", "r1"}, "mutually exclusive"},
		{"addr and pid", []string{"--addr", "127.0.0.1:1", "--pid", "1"}, "cannot be combined"},
		{"addr and run", []string{"--addr", "127.0.0.1:1", "--run", "r1"}, "cannot be combined"},
		{"tls without addr", []string{"--tls"}, "means nothing without it"},
		{"stray arguments", []string{"extra"}, "unexpected arguments"},
		{"impossible port", []string{"--port", "70000"}, "not a port number"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			if code := runUI(context.Background(), tc.args, &stdout, &stderr); code != exitUsage {
				t.Fatalf("exit code = %d, want %d", code, exitUsage)
			}
			if !strings.Contains(stderr.String(), tc.want) {
				t.Errorf("stderr = %q, want it to contain %q", stderr.String(), tc.want)
			}
			if stdout.Len() != 0 {
				t.Errorf("stdout = %q, want empty: nothing was served", stdout.String())
			}
		})
	}
}

// A credential never comes from a flag, on any command. A flag value is in
// this process's argv, where `ps` shows it to every other user on the
// machine, and in the shell history of whoever typed it.
func TestUIDefinesNoTokenFlag(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := runUI(context.Background(), []string{"--token", "hunter2"}, &stdout, &stderr)
	if code != exitUsage {
		t.Fatalf("exit code = %d, want a usage refusal", code)
	}
	if !strings.Contains(stderr.String(), "flag provided but not defined") {
		t.Errorf("stderr = %q, want the flag to be undefined", stderr.String())
	}
}

// --addr reaches a run this machine has no registry entry for, so nothing
// local knows its token. The message has to say where one comes from.
func TestUIWithAnAddressAndNoTokenSaysWhereOneComesFrom(t *testing.T) {
	t.Setenv(source.TokenEnv, "")
	var stdout, stderr bytes.Buffer
	if code := runUI(context.Background(), []string{"--addr", "127.0.0.1:9"}, &stdout, &stderr); code != exitUsage {
		t.Fatalf("exit code = %d, want %d", code, exitUsage)
	}
	if !strings.Contains(stderr.String(), source.TokenEnv) {
		t.Errorf("stderr = %q, want it to name $%s", stderr.String(), source.TokenEnv)
	}
}

// The browser UI serves a running engine. A finished run has no attach
// server, and saying so plainly, with the command that DOES read one, is
// better than a connection failure the operator has to interpret.
func TestUIOnAFinishedRunSaysWhatToUseInstead(t *testing.T) {
	isolateRegistryForUI(t)
	var stdout, stderr bytes.Buffer
	if code := runUI(context.Background(), []string{"--run", "long-gone"}, &stdout, &stderr); code != exitUsage {
		t.Fatalf("exit code = %d, want %d", code, exitUsage)
	}
	if got := stderr.String(); !strings.Contains(got, "--follow") {
		t.Errorf("stderr = %q, want it to point at `senro attach --follow`", got)
	}
}

// --- Serving -------------------------------------------------------------

// The command has to serve the thing it prints a link to, and the link has
// to work: a real attach server, a real `senro ui`, and a real HTTP client
// walking the one-time link.
func TestUIServesTheRunBehindTheLinkItPrints(t *testing.T) {
	requireBundle(t)

	hub := attachsrv.NewHub(64)
	t.Cleanup(func() { _ = hub.Close() })
	const token = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFG"
	srv, err := attachsrv.Listen(context.Background(), attachsrv.Options{
		Bind:    "127.0.0.1:0",
		Network: attachsrv.NetworkTCP,
		Token:   token,
		Dir:     t.TempDir(),
		Hub:     hub,
	})
	if err != nil {
		t.Fatalf("attachsrv.Listen: %v", err)
	}
	t.Cleanup(func() { _ = srv.Close() })

	hub.Emit(api.Event{V: api.Version, Seq: 1, Type: api.RunStarted, Run: "browser-run",
		Payload: []byte(`{"pipeline":"demo"}`)})
	hub.Emit(api.Event{V: api.Version, Seq: 2, Type: api.StepCreated, Run: "browser-run", Step: "build",
		Payload: []byte(`{"kind":"shell"}`)})

	t.Setenv(source.TokenEnv, token)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var stdout, stderr syncBuffer
	done := make(chan int, 1)
	go func() { done <- runUI(ctx, []string{"--addr", srv.Addr()}, &stdout, &stderr) }()

	link := waitForLine(t, &stdout)
	if !strings.HasPrefix(link, "http://127.0.0.1:") {
		t.Fatalf("printed link = %q, want a loopback URL", link)
	}
	// The link a person is handed must not carry the run's own credential.
	if strings.Contains(link, token) {
		t.Fatal("the printed link carries the run's bearer token")
	}

	c := &http.Client{
		Jar:           &allCookies{},
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
	u, err := url.Parse(link)
	if err != nil {
		t.Fatalf("parsing the printed link: %v", err)
	}
	base := "http://" + u.Host

	resp, err := c.Get(link)
	if err != nil {
		t.Fatalf("GET the one-time link: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusSeeOther {
		t.Fatalf("the one-time link answered %d, want 303", resp.StatusCode)
	}

	page := mustGet(t, c, base+"/")
	if !strings.Contains(page, "/_ui/boot.js") {
		t.Errorf("the served page does not load the client: %q", firstBytes(page))
	}

	// The run, through the proxy, with the browser never holding a token.
	state := mustGet(t, c, base+"/api/state")
	if !strings.Contains(state, "browser-run") {
		t.Errorf("GET /api/state = %q, want the run", firstBytes(state))
	}
	if strings.Contains(state, token) {
		t.Error("the state response carries the run's bearer token")
	}

	// The client binary, which is the whole reason this is a browser UI
	// rather than a page that reimplements the fold in JavaScript.
	wasmResp, err := c.Get(base + "/_ui/senro-ui.wasm")
	if err != nil {
		t.Fatalf("GET the client: %v", err)
	}
	ct := wasmResp.Header.Get("Content-Type")
	n, _ := io.Copy(io.Discard, wasmResp.Body)
	_ = wasmResp.Body.Close()
	if ct != "application/wasm" {
		t.Errorf("client Content-Type = %q, want application/wasm", ct)
	}
	if n < 100_000 {
		t.Errorf("the client is %d bytes on the wire, which is not a compiled Go WebAssembly binary", n)
	}

	// The control route exists, and refuses a request that did not come from
	// the page: this POST carries neither the session cookie nor an Origin,
	// so it must not reach the attach server. A page that opened the one-time
	// link does control the run; see internal/webui/control.go.
	ctlResp, err := c.Post(base+"/api/control", "application/json", strings.NewReader(`{}`))
	if err != nil {
		t.Fatalf("POST /api/control: %v", err)
	}
	ctlStatus := ctlResp.StatusCode
	_ = ctlResp.Body.Close()
	if ctlStatus == http.StatusOK {
		t.Error("POST /api/control succeeded with no session and no Origin: the guard is not refusing")
	}

	cancel()
	select {
	case code := <-done:
		if code != exitSuccess {
			t.Errorf("exit code = %d, want %d after a clean shutdown", code, exitSuccess)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("senro ui did not stop after its context was cancelled")
	}
}

// --- Helpers -------------------------------------------------------------

// requireBundle skips a test needing the compiled WebAssembly client in a
// tree that has not built one: `make all` builds it first, but a bare
// `go test ./...` in a fresh checkout legitimately has none.
func requireBundle(t *testing.T) {
	t.Helper()
	s, err := webui.Listen(context.Background(), webui.Options{
		Bind:     "127.0.0.1:0",
		Upstream: webui.Upstream{Network: "tcp", Address: "127.0.0.1:1"},
	})
	if s != nil {
		_ = s.Close()
	}
	if errors.Is(err, webui.ErrBundleMissing) {
		t.Skip("this tree has not built the WebAssembly client; run `make wasm`")
	}
}

// isolateRegistryForUI points the attach registry at a directory this test
// owns, so a discovery lookup cannot find a run somebody happens to have
// going on the machine the suite is running on.
func isolateRegistryForUI(t *testing.T) {
	t.Helper()
	dir := t.TempDir()
	t.Setenv("HOME", dir)
	t.Setenv("XDG_RUNTIME_DIR", dir)
	t.Setenv("XDG_CACHE_HOME", dir)
}

// syncBuffer is an io.Writer a test goroutine can read while the command
// under test is still writing to it.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.String()
}

// waitForLine blocks until the buffer holds a complete line and returns it.
func waitForLine(t *testing.T, b *syncBuffer) string {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if line, _, ok := strings.Cut(b.String(), "\n"); ok {
			return line
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("no line was printed within the deadline (buffer: %q)", b.String())
	return ""
}

// allCookies is a jar with no policy: it stores what it is given and offers
// it back. A test asserting the SERVER refuses something must not be able
// to pass because a jar declined to send a cookie.
type allCookies struct {
	mu sync.Mutex
	cs []*http.Cookie
}

func (j *allCookies) SetCookies(_ *url.URL, cookies []*http.Cookie) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.cs = append(j.cs, cookies...)
}

func (j *allCookies) Cookies(*url.URL) []*http.Cookie {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.cs
}

func mustGet(t *testing.T, c *http.Client, u string) string {
	t.Helper()
	resp, err := c.Get(u)
	if err != nil {
		t.Fatalf("GET %s: %v", u, err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET %s: status %d: %s", u, resp.StatusCode, body)
	}
	return string(body)
}

func firstBytes(s string) string {
	if len(s) > 200 {
		return s[:200]
	}
	return s
}
