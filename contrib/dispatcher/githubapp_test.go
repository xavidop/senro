package main

import (
	"bytes"
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/xavidop/senro/notify"
)

const (
	pushBody = `{"ref":"refs/heads/main","after":"abc123","repository":{"full_name":"acme/web"},"installation":{"id":7}}`
	prBody   = `{"action":"opened","pull_request":{"head":{"sha":"def456"}},"repository":{"full_name":"acme/web"},"installation":{"id":7}}`
)

// testKey is generated once: 2048-bit generation is slow enough to notice
// when every test pays for it.
var testKey = sync.OnceValue(func() *rsa.PrivateKey {
	k, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		panic(err)
	}
	return k
})

// fakeGitHub stands in for the GitHub API and records what it was asked.
type fakeGitHub struct {
	mu          sync.Mutex
	calls       []string // "METHOD path"
	bodies      map[string][]string
	auth        map[string]string
	tokenStatus int    // 0 means 201
	checkStatus string // what GET check-runs/{id} answers
}

func (f *fakeGitHub) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	key := r.Method + " " + r.URL.Path
	f.mu.Lock()
	f.calls = append(f.calls, key)
	f.bodies[key] = append(f.bodies[key], string(body))
	f.auth[key] = r.Header.Get("Authorization")
	f.mu.Unlock()

	switch {
	case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/access_tokens"):
		if f.tokenStatus != 0 {
			w.WriteHeader(f.tokenStatus)
			_, _ = io.WriteString(w, `{"message":"boom"}`)
			return
		}
		w.WriteHeader(http.StatusCreated)
		_, _ = io.WriteString(w, `{"token":"ghs_x"}`)
	case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/check-runs"):
		w.WriteHeader(http.StatusCreated)
		_, _ = io.WriteString(w, `{"id":42}`)
	case r.Method == http.MethodGet:
		_, _ = io.WriteString(w, `{"status":"`+f.checkStatus+`"}`)
	default:
		_, _ = io.WriteString(w, `{}`)
	}
}

func (f *fakeGitHub) saw(key string) []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.bodies[key]...)
}

func (f *fakeGitHub) count(key string) int { return len(f.saw(key)) }

func newFakeGitHub(t *testing.T) (*fakeGitHub, *httptest.Server) {
	t.Helper()
	f := &fakeGitHub{bodies: map[string][]string{}, auth: map[string]string{}, checkStatus: "completed"}
	srv := httptest.NewServer(f)
	t.Cleanup(srv.Close)
	return f, srv
}

func testApp(api string) *githubApp {
	return &githubApp{
		appID:      "123",
		key:        testKey(),
		api:        api,
		checkName:  "senro",
		detailsURL: "https://ci.example/runs/{run}",
		client:     &http.Client{},
	}
}

// appDispatcher is newDispatcher with the App mode turned on against a fake.
func appDispatcher(t *testing.T, script string) (*dispatcher, *fakeGitHub) {
	t.Helper()
	f, srv := newFakeGitHub(t)
	d := newDispatcher(t, script)
	d.app = testApp(srv.URL)
	return d, f
}

func deliverEvent(t *testing.T, d *dispatcher, event, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	req.Header.Set("X-GitHub-Event", event)
	req.Header.Set("X-Hub-Signature-256", sign("shh", body))
	w := httptest.NewRecorder()
	d.serve(w, req)
	return w
}

func TestJWTIsSignedWithTheAppKey(t *testing.T) {
	a := testApp("http://unused")
	now := time.Unix(1_700_000_000, 0)
	tok, err := a.jwt(now)
	if err != nil {
		t.Fatalf("jwt: %v", err)
	}
	parts := strings.Split(tok, ".")
	if len(parts) != 3 {
		t.Fatalf("got %d segments, want 3", len(parts))
	}
	header, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		t.Fatalf("header: %v", err)
	}
	if string(header) != `{"alg":"RS256","typ":"JWT"}` {
		t.Errorf("header = %s", header)
	}
	raw, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		t.Fatalf("claims: %v", err)
	}
	var claims struct {
		Iat int64  `json:"iat"`
		Exp int64  `json:"exp"`
		Iss string `json:"iss"`
	}
	if err := json.Unmarshal(raw, &claims); err != nil {
		t.Fatalf("claims: %v", err)
	}
	if claims.Iss != "123" {
		t.Errorf("iss = %q, want 123", claims.Iss)
	}
	if claims.Iat != now.Add(-60*time.Second).Unix() {
		t.Errorf("iat = %d, want now-60s", claims.Iat)
	}
	if claims.Exp != now.Add(9*time.Minute).Unix() {
		t.Errorf("exp = %d, want now+9m", claims.Exp)
	}
	sig, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		t.Fatalf("signature: %v", err)
	}
	sum := sha256.Sum256([]byte(parts[0] + "." + parts[1]))
	if err := rsa.VerifyPKCS1v15(&testKey().PublicKey, crypto.SHA256, sum[:], sig); err != nil {
		t.Errorf("the signature does not verify: %v", err)
	}
}

func TestLoadGitHubAppReadsPKCS1AndPKCS8(t *testing.T) {
	dir := t.TempDir()
	pkcs8, err := x509.MarshalPKCS8PrivateKey(testKey())
	if err != nil {
		t.Fatal(err)
	}
	for name, block := range map[string]*pem.Block{
		"pkcs1": {Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(testKey())},
		"pkcs8": {Type: "PRIVATE KEY", Bytes: pkcs8},
	} {
		path := filepath.Join(dir, name+".pem")
		if err := os.WriteFile(path, pem.EncodeToMemory(block), 0o600); err != nil {
			t.Fatal(err)
		}
		a, err := loadGitHubApp("123", path, "https://api.github.com/", "senro", "")
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if !a.key.Equal(testKey()) {
			t.Errorf("%s: a different key was loaded", name)
		}
		if a.api != "https://api.github.com" {
			t.Errorf("%s: api = %q, want the trailing slash trimmed", name, a.api)
		}
	}
	bad := filepath.Join(dir, "bad.pem")
	if err := os.WriteFile(bad, []byte("not a key"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadGitHubApp("123", bad, "https://api.github.com", "senro", ""); err == nil {
		t.Error("a file holding no PEM key loaded")
	}
}

func TestInstallationTokenIsExchanged(t *testing.T) {
	f, srv := newFakeGitHub(t)
	a := testApp(srv.URL)
	tok, err := a.installationToken(context.Background(), 7)
	if err != nil {
		t.Fatalf("installationToken: %v", err)
	}
	if tok != "ghs_x" {
		t.Errorf("token = %q, want ghs_x", tok)
	}
	const key = "POST /app/installations/7/access_tokens"
	if f.count(key) != 1 {
		t.Fatalf("calls = %v, want one %s", f.calls, key)
	}
	auth, ok := strings.CutPrefix(f.auth[key], "Bearer ")
	if !ok || strings.Count(auth, ".") != 2 {
		t.Errorf("Authorization = %q, want Bearer <jwt>", f.auth[key])
	}
}

func TestGitHubDeliveryExtractsHeadAndInstallation(t *testing.T) {
	for _, tc := range []struct {
		event, body, sha string
	}{
		{"push", pushBody, "abc123"},
		{"pull_request", prBody, "def456"},
	} {
		d, ok := githubDelivery(tc.event, []byte(tc.body))
		if !ok {
			t.Fatalf("%s: not recognized", tc.event)
		}
		if d.installation != 7 || d.repo != "acme/web" || d.sha != tc.sha {
			t.Errorf("%s: got %+v", tc.event, d)
		}
	}
	noInstall := `{"after":"abc123","repository":{"full_name":"acme/web"}}`
	if _, ok := githubDelivery("push", []byte(noInstall)); ok {
		t.Error("a payload without installation was recognized")
	}
	if _, ok := githubDelivery("ping", []byte(pushBody)); ok {
		t.Error("a ping was recognized")
	}
}

func TestDeliveryCreatesAQueuedCheckBeforeTheRun(t *testing.T) {
	envFile := filepath.Join(t.TempDir(), "env")
	d, f := appDispatcher(t, "env > "+envFile)

	if got := deliverEvent(t, d, "push", pushBody).Code; got != http.StatusAccepted {
		t.Fatalf("delivery got %d, want 202", got)
	}
	// serve creates the check before it answers, so it is there before the
	// pipeline goroutine could have run.
	creates := f.saw("POST /repos/acme/web/check-runs")
	if len(creates) != 1 {
		t.Fatalf("check-runs POSTs = %d, want 1 by the time serve returned", len(creates))
	}
	for _, want := range []string{`"status":"queued"`, `"head_sha":"abc123"`, `"name":"senro"`} {
		if !strings.Contains(creates[0], want) {
			t.Errorf("the create body lacks %s: %s", want, creates[0])
		}
	}
	waitFree(t, d)

	env, err := os.ReadFile(envFile)
	if err != nil {
		t.Fatalf("the pipeline wrote no env: %v", err)
	}
	for _, want := range []string{
		notify.EnvGitHubToken + "=ghs_x",
		notify.EnvGitHubAPI + "=" + d.app.api,
		notify.EnvGitHubRepo + "=acme/web",
		notify.EnvGitHubSHA + "=abc123",
		notify.EnvGitHubCheckRun + "=42",
		notify.EnvGitHubCheckName + "=senro",
		notify.EnvGitHubDetailsURL + "=https://ci.example/runs/{run}",
	} {
		if !strings.Contains(string(env), want+"\n") {
			t.Errorf("the pipeline env lacks %s", want)
		}
	}
}

func TestExit78CompletesNeutral(t *testing.T) {
	d, f := appDispatcher(t, "exit 78")
	if got := deliverEvent(t, d, "push", pushBody).Code; got != http.StatusAccepted {
		t.Fatalf("delivery got %d", got)
	}
	waitFree(t, d)
	patches := f.saw("PATCH /repos/acme/web/check-runs/42")
	if len(patches) != 1 {
		t.Fatalf("PATCHes = %d, want 1", len(patches))
	}
	for _, want := range []string{`"conclusion":"neutral"`, `"title":"No trigger matched"`, `"status":"completed"`} {
		if !strings.Contains(patches[0], want) {
			t.Errorf("the PATCH lacks %s: %s", want, patches[0])
		}
	}
}

func TestCrashedPipelineIsCompletedAsFailure(t *testing.T) {
	d, f := appDispatcher(t, "exit 1")
	f.checkStatus = "in_progress"
	if got := deliverEvent(t, d, "push", pushBody).Code; got != http.StatusAccepted {
		t.Fatalf("delivery got %d", got)
	}
	waitFree(t, d)
	if f.count("GET /repos/acme/web/check-runs/42") != 1 {
		t.Errorf("the check was not fetched: %v", f.calls)
	}
	patches := f.saw("PATCH /repos/acme/web/check-runs/42")
	if len(patches) != 1 {
		t.Fatalf("PATCHes = %d, want 1", len(patches))
	}
	for _, want := range []string{`"conclusion":"failure"`, `"title":"Pipeline exited 1"`} {
		if !strings.Contains(patches[0], want) {
			t.Errorf("the PATCH lacks %s: %s", want, patches[0])
		}
	}

	// When the pipeline's notifier already completed it, it is left alone.
	d2, f2 := appDispatcher(t, "exit 1")
	f2.checkStatus = "completed"
	if got := deliverEvent(t, d2, "push", pushBody).Code; got != http.StatusAccepted {
		t.Fatalf("delivery got %d", got)
	}
	waitFree(t, d2)
	if n := f2.count("PATCH /repos/acme/web/check-runs/42"); n != 0 {
		t.Errorf("a completed check was PATCHed %d times", n)
	}
}

func TestBusyGroupCompletesNeutral(t *testing.T) {
	d, f := appDispatcher(t, "exit 0")
	u, err := d.locker.TryAcquire(context.Background(), d.group, "someone-else")
	if err != nil {
		t.Fatalf("holding the group: %v", err)
	}
	defer func() { _ = u.Release(context.Background()) }()

	if got := deliverEvent(t, d, "push", pushBody).Code; got != http.StatusConflict {
		t.Fatalf("delivery got %d, want 409", got)
	}
	patches := f.saw("PATCH /repos/acme/web/check-runs/42")
	if len(patches) != 1 {
		t.Fatalf("PATCHes = %d, want 1", len(patches))
	}
	for _, want := range []string{`"conclusion":"neutral"`, `"title":"Not run"`, "another delivery holds group test-group"} {
		if !strings.Contains(patches[0], want) {
			t.Errorf("the PATCH lacks %s: %s", want, patches[0])
		}
	}
}

func TestTokenFailureStillRuns(t *testing.T) {
	dir := t.TempDir()
	ran := filepath.Join(dir, "ran")
	envFile := filepath.Join(dir, "env")
	d, f := appDispatcher(t, "touch "+ran+"; env > "+envFile)
	f.tokenStatus = http.StatusInternalServerError

	var buf lockedBuffer
	log.SetOutput(&buf)
	t.Cleanup(func() { log.SetOutput(os.Stderr) })

	if got := deliverEvent(t, d, "push", pushBody).Code; got != http.StatusAccepted {
		t.Fatalf("delivery got %d, want 202", got)
	}
	waitFree(t, d)

	if _, err := os.Stat(ran); err != nil {
		t.Errorf("the pipeline did not run: %v", err)
	}
	if n := f.count("POST /repos/acme/web/check-runs"); n != 0 {
		t.Errorf("a check was created %d times without a token", n)
	}
	env, _ := os.ReadFile(envFile)
	if strings.Contains(string(env), notify.EnvGitHubToken) {
		t.Error("the pipeline was handed a token variable without a token")
	}
	logged := buf.String()
	if !strings.Contains(logged, "installation token") {
		t.Errorf("the failure was not logged: %q", logged)
	}
	pkcs1 := base64.StdEncoding.EncodeToString(x509.MarshalPKCS1PrivateKey(testKey()))
	for _, bad := range []string{"Bearer", pkcs1[:40], "PRIVATE KEY"} {
		if strings.Contains(logged, bad) {
			t.Errorf("the log contains %q: %q", bad, logged)
		}
	}
}

// lockedBuffer is a log sink the run goroutine and the test can share.
type lockedBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *lockedBuffer) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *lockedBuffer) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

// Deliveries that are not GitHub App deliveries run exactly as before.
func TestNonAppDeliveryTouchesNoAPI(t *testing.T) {
	d, f := appDispatcher(t, "exit 0")
	const body = `{"zen":"hi","hook_id":1}`
	if got := deliverEvent(t, d, "ping", body).Code; got != http.StatusAccepted {
		t.Fatalf("delivery got %d", got)
	}
	waitFree(t, d)
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.calls) != 0 {
		t.Errorf("a ping called GitHub: %v", f.calls)
	}
}

func TestAppFlagsMustComeTogether(t *testing.T) {
	for _, tc := range []struct {
		id, key string
		bad     bool
	}{
		{"", "", false},
		{"123", "/k.pem", false},
		{"123", "", true},
		{"", "/k.pem", true},
	} {
		err := appFlagsError(tc.id, tc.key)
		if (err != nil) != tc.bad {
			t.Errorf("appFlagsError(%q, %q) = %v, want error %v", tc.id, tc.key, err, tc.bad)
		}
	}
}

// A pipeline that exits 0 without reporting through the check closes it as a
// success: the run worked, it just never said so.
func TestExit0WithTheCheckOpenCompletesSuccess(t *testing.T) {
	d, f := appDispatcher(t, "exit 0")
	f.checkStatus = "in_progress"
	if got := deliverEvent(t, d, "push", pushBody).Code; got != http.StatusAccepted {
		t.Fatalf("delivery got %d", got)
	}
	waitFree(t, d)
	patches := f.saw("PATCH /repos/acme/web/check-runs/42")
	if len(patches) != 1 {
		t.Fatalf("PATCHes = %d, want 1", len(patches))
	}
	for _, want := range []string{`"conclusion":"success"`, `"title":"Pipeline exited 0"`, "did not report"} {
		if !strings.Contains(patches[0], want) {
			t.Errorf("the PATCH lacks %s: %s", want, patches[0])
		}
	}
}

// A run killed by -timeout (or displaced by -cancel-in-progress) is
// cancelled, not failed.
func TestAKilledRunCompletesCancelled(t *testing.T) {
	d, f := appDispatcher(t, "exec sleep 5")
	d.timeout = 200 * time.Millisecond
	f.checkStatus = "in_progress"
	if got := deliverEvent(t, d, "push", pushBody).Code; got != http.StatusAccepted {
		t.Fatalf("delivery got %d", got)
	}
	waitHeld(t, d)
	waitFree(t, d)
	patches := f.saw("PATCH /repos/acme/web/check-runs/42")
	if len(patches) != 1 {
		t.Fatalf("PATCHes = %d, want 1", len(patches))
	}
	for _, want := range []string{`"conclusion":"cancelled"`, `"title":"Pipeline was terminated"`, "The dispatcher stopped"} {
		if !strings.Contains(patches[0], want) {
			t.Errorf("the PATCH lacks %s: %s", want, patches[0])
		}
	}
}

// A pipeline killed by a signal the dispatcher did not send (an OOM killer,
// an operator's kill) failed: nobody cancelled it. The fake pipeline sends
// SIGTERM to itself, so the run ends on a signal with its context alive.
func TestASignalTheDispatcherDidNotSendCompletesFailure(t *testing.T) {
	d, f := appDispatcher(t, "kill -TERM $$")
	f.checkStatus = "in_progress"
	if got := deliverEvent(t, d, "push", pushBody).Code; got != http.StatusAccepted {
		t.Fatalf("delivery got %d", got)
	}
	waitFree(t, d)
	patches := f.saw("PATCH /repos/acme/web/check-runs/42")
	if len(patches) != 1 {
		t.Fatalf("PATCHes = %d, want 1", len(patches))
	}
	for _, want := range []string{`"conclusion":"failure"`, `"title":"Pipeline was terminated"`, "killed by a signal"} {
		if !strings.Contains(patches[0], want) {
			t.Errorf("the PATCH lacks %s: %s", want, patches[0])
		}
	}
}

// When the delivery cannot be staged the pipeline never starts, and the
// check must not stay queued.
func TestStagingFailureCompletesTheCheck(t *testing.T) {
	d, f := appDispatcher(t, "exit 0")
	t.Setenv("TMPDIR", filepath.Join(t.TempDir(), "missing"))
	if got := deliverEvent(t, d, "push", pushBody).Code; got != http.StatusAccepted {
		t.Fatalf("delivery got %d", got)
	}
	waitFree(t, d)
	patches := f.saw("PATCH /repos/acme/web/check-runs/42")
	if len(patches) != 1 {
		t.Fatalf("PATCHes = %d, want 1", len(patches))
	}
	for _, want := range []string{`"conclusion":"failure"`, `"title":"Pipeline did not start"`} {
		if !strings.Contains(patches[0], want) {
			t.Errorf("the PATCH lacks %s: %s", want, patches[0])
		}
	}
}

// A run past the token's useful life closes its check with a fresh token
// rather than an expired one.
func TestALongRunClosesWithAFreshToken(t *testing.T) {
	f, srv := newFakeGitHub(t)
	f.checkStatus = "in_progress"
	c := &appCheck{
		app:     testApp(srv.URL),
		token:   "ghs_old",
		d:       delivery{installation: 7, repo: "acme/web", sha: "abc123"},
		id:      42,
		started: time.Now().Add(-51 * time.Minute),
	}
	c.finish(nil, false)
	if n := f.count("POST /app/installations/7/access_tokens"); n != 1 {
		t.Errorf("token mints = %d, want 1", n)
	}
	f.mu.Lock()
	got := f.auth["PATCH /repos/acme/web/check-runs/42"]
	f.mu.Unlock()
	if got != "Bearer ghs_x" {
		t.Errorf("the PATCH was sent with %q, want the fresh token", got)
	}

	// A short run keeps the token it has.
	f2, srv2 := newFakeGitHub(t)
	f2.checkStatus = "in_progress"
	c2 := &appCheck{app: testApp(srv2.URL), token: "ghs_old", d: c.d, id: 42, started: time.Now()}
	c2.finish(nil, false)
	if n := f2.count("POST /app/installations/7/access_tokens"); n != 0 {
		t.Errorf("a short run minted %d tokens", n)
	}
}
