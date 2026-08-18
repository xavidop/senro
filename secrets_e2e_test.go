package senro_test

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/xavidop/mamori"
	"github.com/xavidop/mamori/mamoritest"
	"github.com/xavidop/mamori/secret"

	"github.com/xavidop/senro"
	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/artifact"
	"github.com/xavidop/senro/attach"
	"github.com/xavidop/senro/exec"
	"github.com/xavidop/senro/internal/eventlog"
	"github.com/xavidop/senro/internal/redact"
)

// TestSecretsEndToEnd exercises the full secrets pipeline at once: a Pure
// step reads its credential from the file SecretEnv delivered, prints it
// (the leak senro's own redaction has to defend against), writes a derived
// artifact, and is cached; then the same pipeline runs again, hitting the
// cache.
//
// It proves the containment claim over every byte the run left behind, in
// combinations no single narrower test covers:
//
//   - Delivery with redaction: the printed value is [REDACTED] on disk.
//   - Delivery with the cache key: the second run hits, so the per-attempt
//     file path did NOT reach the key.
//   - The cache with redaction: the replayed log is redacted too.
//   - The workspace snapshot path: the secret directory is not under the
//     run directory, so no snapshot or declared output can reach it.
//   - Redaction with attach: an attached client receives the value from
//     neither the lifecycle stream nor the log-pull endpoint, the same
//     on-disk claim proven over the wire.
//   - Delivery with redaction for a HANDLER: "boom" always fails and its
//     OnFailure handler prints the same secret. A handler is a distinct
//     code path for secret delivery and redaction, exercised rather than
//     assumed to mirror the step case (checks 1b and 3b/3c below).
func TestSecretsEndToEnd(t *testing.T) {
	// Keeps attach.Listen's socket registry out of the operator's real
	// $HOME/$XDG_RUNTIME_DIR, like every other attach test here.
	isolateAttachRegistry(t)

	const value = "s3cr3t-registry-token-aaaa"

	type Config struct {
		RegistryToken secret.String `source:"fake://ci/ghcr#token"`
		Registry      string        `source:"fake://ci/ghcr#host"`
	}
	pr := mamoritest.NewProvider("fake")
	pr.Set("ci/ghcr#token", value)
	pr.Set("ci/ghcr#host", "ghcr.io/acme")

	ctx := context.Background()
	cfg, err := mamori.Load[Config](ctx, mamori.WithProvider(pr))
	if err != nil {
		t.Fatalf("mamori.Load: %v", err)
	}
	if cfg.Registry != "ghcr.io/acme" {
		t.Fatalf("the fixture did not load: Registry = %q", cfg.Registry)
	}

	cacheDir := t.TempDir()
	work := t.TempDir()
	if err := os.WriteFile(filepath.Join(work, "Dockerfile"), []byte("FROM scratch\n"), 0o600); err != nil {
		t.Fatalf("fixture: %v", err)
	}
	// The step mounts no workspace, so its declared Inputs resolve against
	// the coordinator's working directory (wsManager.inputRoot's fallback),
	// not WorkDir; Chdir makes the two agree.
	t.Chdir(work)

	build := func() *senro.Pipeline {
		p := senro.New("monorepo")
		wf := p.Workflow("publish")
		wf.Step("login", exec.Command("sh", "-c",
			`echo "registry is $REGISTRY"
			 echo "credential file is $REGISTRY_TOKEN"
			 cat "$REGISTRY_TOKEN"
			 echo
			 cat "$SENRO_SECRET_REGISTRYTOKEN" > receipt.txt`)).
			WorkDir(work).
			Env("REGISTRY", cfg.Registry).
			SecretEnv("REGISTRY_TOKEN", "RegistryToken").
			CacheEnv("REGISTRY").
			Pure().
			Inputs(artifact.File("Dockerfile"))
		// "boom" always fails, and its OnFailure handler prints the same
		// secret; see this test's doc.
		wf.Step("boom", exec.Command("sh", "-c", "exit 3")).
			OnFailure(senro.Handler("notify", exec.Command("sh", "-c",
				`echo "posting to $(cat "$WEBHOOK")"`)).
				SecretEnv("WEBHOOK", "RegistryToken"))
		return p
	}

	// att is live for the whole first Run: Options.Dir pins the attach
	// server to the same directory WithDir gives Run, so both sides serve
	// the one run this test inspects.
	firstDir := t.TempDir()
	att, err := attach.Listen(ctx, attach.Options{Bind: attach.AutoUnixSocket, Dir: firstDir})
	if err != nil {
		t.Fatalf("attach.Listen: %v", err)
	}
	defer func() { _ = att.Close() }()

	// "boom" always fails by design, so this run's error is expected: a
	// *senro.RunError naming it, never a plain wrapped engine error.
	assertOnlyBoomFailed(t, senro.Run(ctx, build(), senro.WithAttach(att),
		senro.WithDir(firstDir), senro.WithCacheDir(cacheDir), senro.WithSecrets(cfg)))

	// 1. The step really ran and really printed its credential.
	logPath := eventlog.NewLogSet(firstDir).Path("login", 1, api.StreamStdout)
	body, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("reading the log: %v", err)
	}
	if !bytes.Contains(body, []byte("registry is ghcr.io/acme")) {
		t.Fatalf("the step's own output is missing: %q", body)
	}
	if !bytes.Contains(body, []byte(redact.Placeholder)) {
		t.Fatalf("nothing was redacted, so either the value was never printed or the "+
			"redactor never ran: %q", body)
	}
	if bytes.Contains(body, []byte(value)) {
		t.Error("the log file holds the credential")
	}

	// 1b. The handler really ran and printed its credential too: "boom"
	// fails every run, so its OnFailure handler always runs.
	handlerLogPath := eventlog.NewLogSet(firstDir).Path("boom/on_failure/notify", 1, api.StreamStdout)
	handlerBody, err := os.ReadFile(handlerLogPath)
	if err != nil {
		t.Fatalf("reading the handler's log: %v", err)
	}
	if !bytes.Contains(handlerBody, []byte("posting to")) {
		t.Fatalf("the handler's own output is missing: %q", handlerBody)
	}
	if !bytes.Contains(handlerBody, []byte(redact.Placeholder)) {
		t.Fatalf("nothing was redacted in the handler's log, so either the value was never "+
			"printed or the redactor never ran: %q", handlerBody)
	}
	if bytes.Contains(handlerBody, []byte(value)) {
		t.Error("the handler's log file holds the credential")
	}

	// 2. The step wrote the value into a file it controls, which senro
	//    deliberately does NOT protect: proving it is there makes the sweep
	//    below a real statement rather than an accident of the pipeline
	//    never producing the value.
	receipt, err := os.ReadFile(filepath.Join(work, "receipt.txt"))
	if err != nil {
		t.Fatalf("reading the step's own output file: %v", err)
	}
	if !bytes.Contains(receipt, []byte(value)) {
		t.Fatalf("the step did not write the value where it was told to; the rest of " +
			"this test is not measuring what it claims")
	}

	// 3. Nothing senro itself wrote holds the value.
	if found := scanTreeFor(t, firstDir, value); found != "" {
		t.Errorf("the value appears under the run directory, in %s", found)
	}
	if found := scanTreeFor(t, cacheDir, value); found != "" {
		t.Errorf("the value appears under the cache root, in %s", found)
	}

	// 3b. What an attached client received over the live protocol holds no
	//     value either: the lifecycle stream, and the log-pull endpoint GET
	//     /api/logs/{step}, a different code path than check 1's file read.
	firstEvents := readLedgerAt(t, firstDir)
	streamed := attachedStream(t, att.Addr(), len(firstEvents))
	if bytes.Contains(streamed, []byte(value)) {
		t.Error("the attach lifecycle stream carries the credential")
	}
	pulled := attachedLogPull(t, att.Addr(), "login")
	if !bytes.Contains(pulled, []byte(redact.Placeholder)) {
		t.Fatalf("the attach log-pull endpoint returned no placeholder, so this check is "+
			"not measuring what it claims: %q", pulled)
	}
	if bytes.Contains(pulled, []byte(value)) {
		t.Error("the attach server served the credential over GET /api/logs")
	}

	// 3c. The same GET /api/logs endpoint, for the HANDLER's composite
	// log-step id, "GET /api/logs/boom%2Fon_failure%2Fnotify" over the
	// wire, not a file read.
	pulledHandler := attachedLogPull(t, att.Addr(), "boom/on_failure/notify")
	if !bytes.Contains(pulledHandler, []byte(redact.Placeholder)) {
		t.Fatalf("the attach log-pull endpoint returned no placeholder for the handler, so "+
			"this check is not measuring what it claims: %q", pulledHandler)
	}
	if bytes.Contains(pulledHandler, []byte(value)) {
		t.Error("the attach server served the handler's credential over GET /api/logs")
	}

	// 4. The second run hits, which is only possible if the per-attempt
	//    secret file's PATH stayed out of the cache key.
	secondDir := t.TempDir()
	assertOnlyBoomFailed(t, senro.Run(ctx, build(),
		senro.WithDir(secondDir), senro.WithCacheDir(cacheDir), senro.WithSecrets(cfg)))
	events := readLedgerAt(t, secondDir)
	if !hasEventType(events, api.CacheHit) {
		t.Error("the second run missed; a per-attempt path reached the cache key")
	}

	// 5. The replayed log is redacted too.
	replayed, err := os.ReadFile(eventlog.NewLogSet(secondDir).Path("login", 1, api.StreamStdout))
	if err != nil {
		t.Fatalf("reading the replayed log: %v", err)
	}
	if !bytes.Contains(replayed, []byte("registry is ghcr.io/acme")) {
		t.Fatalf("the replayed log has no content: %q", replayed)
	}
	if bytes.Contains(replayed, []byte(value)) {
		t.Error("the replayed log holds the credential")
	}

	// 6. And the run's own record names the secret without its value.
	raw, err := os.ReadFile(filepath.Join(firstDir, "events.jsonl"))
	if err != nil {
		t.Fatalf("reading the ledger: %v", err)
	}
	if !bytes.Contains(raw, []byte(`"secret.resolved"`)) {
		t.Error("no secret.resolved event")
	}
	if !bytes.Contains(raw, []byte(`"secret.redacted"`)) {
		t.Error("no secret.redacted event, so the UI has no way to show redaction is live")
	}
	if !strings.Contains(string(raw), `"RegistryToken"`) {
		t.Error("the ledger does not name the secret")
	}
}

// assertOnlyBoomFailed checks senro.Run's error after both runs: "boom"
// fails by design, so the error must be a *senro.RunError naming it, never
// nil and never a plain wrapped engine error.
func assertOnlyBoomFailed(t *testing.T, err error) {
	t.Helper()
	var runErr *senro.RunError
	if !errors.As(err, &runErr) {
		t.Fatalf("Run: %v, want a *senro.RunError naming \"boom\"", err)
	}
	if runErr.Status != api.RunFailed {
		t.Errorf("RunError.Status = %q, want %q", runErr.Status, api.RunFailed)
	}
	var namesBoom bool
	for _, s := range runErr.Steps {
		if s.ID == "boom" {
			namesBoom = true
		}
	}
	if !namesBoom {
		t.Errorf("RunError.Steps = %v, want \"boom\" among them", runErr.Steps)
	}
}

// readLedgerAt reads the ledger a completed run at dir left behind.
// t.Fatal on an empty ledger: "does the ledger contain X" over a silently
// empty slice would pass for the wrong reason.
func readLedgerAt(t *testing.T, dir string) []api.Event {
	t.Helper()
	events, err := eventlog.Read(filepath.Join(dir, "events.jsonl"))
	if err != nil {
		t.Fatalf("readLedgerAt(%s): %v", dir, err)
	}
	if len(events) == 0 {
		t.Fatalf("readLedgerAt(%s): the ledger is empty", dir)
	}
	return events
}

// hasEventType is this package's copy of internal/engine's hasEvent helper.
func hasEventType(events []api.Event, ty api.Type) bool {
	for _, e := range events {
		if e.Type == ty {
			return true
		}
	}
	return false
}

// attachHTTPClient dials addr, an attach server's unix socket, the same way
// internal/attachsrv's own tests and a real "senro attach" client do:
// http.Transport.DialContext ignores the network/address net/http hands it
// and always dials the one unix socket this test's attach server is
// actually listening on.
func attachHTTPClient(t *testing.T, addr string) *http.Client {
	t.Helper()
	tr := &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, "unix", addr)
		},
	}
	t.Cleanup(tr.CloseIdleConnections)
	return &http.Client{Transport: tr}
}

// attachedStream reads up to wantLines NDJSON lifecycle events from the
// attach server at addr, from sequence 1. Bounded by wantLines rather than
// EOF: the run has finished, the hub is still open and will emit nothing
// more, so an unbounded read would block until the test timed out.
// wantLines should be the run's own ledger length, so this reads exactly
// the replay the hub's ring can provide.
func attachedStream(t *testing.T, addr string, wantLines int) []byte {
	t.Helper()
	resp, err := attachHTTPClient(t, addr).Get("http://unix/api/stream?from=1")
	if err != nil {
		t.Fatalf("GET /api/stream: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /api/stream status = %d, want 200", resp.StatusCode)
	}

	var buf bytes.Buffer
	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 0, 64*1024), 1<<20)
	var lines int
	for lines < wantLines && scanner.Scan() {
		buf.Write(scanner.Bytes())
		buf.WriteByte('\n')
		lines++
	}
	if lines == 0 {
		t.Fatal("the attach lifecycle stream delivered nothing; this check proves nothing")
	}
	return buf.Bytes()
}

// attachedLogPull fetches a step's stdout through the attach server's
// log-pull endpoint, GET /api/logs/{step}: same bytes as check 1's file
// read, a different code path serving them.
//
// url.PathEscape, not step verbatim: a handler's composite log-step id
// ("boom/on_failure/notify") must travel as ONE path segment with its "/"
// percent-encoded, or net/http's router would split it into three.
func attachedLogPull(t *testing.T, addr, step string) []byte {
	t.Helper()
	resp, err := attachHTTPClient(t, addr).Get(
		"http://unix/api/logs/" + url.PathEscape(step) + "?stream=" + api.StreamStdout + "&attempt=1")
	if err != nil {
		t.Fatalf("GET /api/logs/%s: %v", step, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /api/logs/%s status = %d, want 200", step, resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("reading /api/logs/%s: %v", step, err)
	}
	return body
}
