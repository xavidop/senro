package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/internal/attachsrv"
	"github.com/xavidop/senro/internal/source"
)

// endpointForEntry is the one place a discovered run turns into something
// dialable, so it is the one place a TCP run's credential can go missing.

func TestEndpointForAUnixEntryCarriesNoCredential(t *testing.T) {
	ep, err := endpointForEntry(attachsrv.Entry{Socket: "/tmp/x.sock"}, "")
	if err != nil {
		t.Fatalf("endpointForEntry: %v", err)
	}
	if ep.Network != attachsrv.NetworkUnix {
		t.Errorf("Network = %q, want unix", ep.Network)
	}
	if ep.Address != "/tmp/x.sock" {
		t.Errorf("Address = %q, want the socket path", ep.Address)
	}
	if ep.Token != "" || ep.TLS != nil {
		t.Errorf("a unix endpoint carried a token or a TLS config: %+v", ep)
	}
}

func TestEndpointForATCPEntryCarriesTheRegisteredToken(t *testing.T) {
	ep, err := endpointForEntry(attachsrv.Entry{
		Network: attachsrv.NetworkTCP,
		Addr:    "127.0.0.1:9999",
		Token:   "MpQ7bqTn6Rr1yfKuHhVe0sZgWc4LxJd2AaBbCcDdEeF",
	}, "")
	if err != nil {
		t.Fatalf("endpointForEntry: %v", err)
	}
	if ep.Network != attachsrv.NetworkTCP || ep.Address != "127.0.0.1:9999" {
		t.Errorf("endpoint = %+v, want the registered tcp address", ep)
	}
	if ep.Token != "MpQ7bqTn6Rr1yfKuHhVe0sZgWc4LxJd2AaBbCcDdEeF" {
		t.Errorf("Token = %q, want the one from the registry entry: a discovered run must need no extra step", ep.Token)
	}
	if ep.TLS != nil {
		t.Error("a plaintext entry produced a TLS config")
	}
}

// The environment wins over the registry. A registry entry is written by
// whoever ran the pipeline; the environment is set by the person at the
// terminal, and if they went to the trouble of setting it they mean it.
func TestTheEnvironmentTokenOverridesTheRegisteredOne(t *testing.T) {
	ep, err := endpointForEntry(attachsrv.Entry{
		Network: attachsrv.NetworkTCP, Addr: "127.0.0.1:1", Token: "from-the-registry-entry-not-the-env-var",
	}, "from-the-environment-and-long-enough-to-pass")
	if err != nil {
		t.Fatalf("endpointForEntry: %v", err)
	}
	if ep.Token != "from-the-environment-and-long-enough-to-pass" {
		t.Errorf("Token = %q, want the environment's", ep.Token)
	}
}

func TestATLSEntryProducesAVerifyingTLSConfig(t *testing.T) {
	ep, err := endpointForEntry(attachsrv.Entry{
		Network: attachsrv.NetworkTCP, Addr: "example.invalid:8443", TLS: true,
		Token: "MpQ7bqTn6Rr1yfKuHhVe0sZgWc4LxJd2AaBbCcDdEeF",
	}, "")
	if err != nil {
		t.Fatalf("endpointForEntry: %v", err)
	}
	if ep.TLS == nil {
		t.Fatal("a TLS entry produced no TLS config, so the client would dial plaintext and be refused")
	}
	if ep.TLS.InsecureSkipVerify {
		t.Fatal("InsecureSkipVerify is set: a client that does not verify its server hands the token to whoever answers the port")
	}
}

// --addr is the path for a run with no registry entry: another machine,
// through a port-forward. Its token may come only from the environment,
// never a flag, which `ps` shows to every other user.
func TestAnExplicitAddressNeedsATokenFromTheEnvironment(t *testing.T) {
	_, err := endpointForAddr("127.0.0.1:9999", false, "")
	if err == nil {
		t.Fatal("endpointForAddr with no token succeeded, want a refusal")
	}
	if !strings.Contains(err.Error(), source.TokenEnv) {
		t.Fatalf("err = %v, want it to name $%s, which is where the token has to come from", err, source.TokenEnv)
	}

	ep, err := endpointForAddr("127.0.0.1:9999", true, "MpQ7bqTn6Rr1yfKuHhVe0sZgWc4LxJd2AaBbCcDdEeF")
	if err != nil {
		t.Fatalf("endpointForAddr: %v", err)
	}
	if ep.Network != attachsrv.NetworkTCP || ep.Address != "127.0.0.1:9999" {
		t.Errorf("endpoint = %+v, want the given tcp address", ep)
	}
	if ep.TLS == nil {
		t.Error("--tls produced no TLS config")
	}
}

// `senro attach --token abc` would put the credential in this process's
// argv, where `ps` shows it to every other user. There is no such flag,
// and this is the mechanical check rather than a comment somebody
// deletes.
func TestNoCommandDefinesATokenFlag(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatalf("Glob: %v", err)
	}
	for _, path := range files {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		b, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		for _, forbidden := range []string{`.String("token"`, `.String("bearer"`, `.String("secret"`, `.String("auth"`} {
			if strings.Contains(string(b), forbidden) {
				t.Errorf("%s defines a flag matching %s: a credential on a command line is readable "+
					"by every other user on the machine through ps, for as long as the command runs. "+
					"It comes from $%s instead", path, forbidden, source.TokenEnv)
			}
		}
	}
}

// --- the two routes, end to end through the real command ---

const cliTestToken = "MpQ7bqTn6Rr1yfKuHhVe0sZgWc4LxJd2AaBbCcDdEeF"

// startTCPRun brings up a real TCP attach server with a finished run in it,
// and returns its address plus its hub, so a test can close the hub once the
// command under test has actually subscribed.
func startTCPRun(t *testing.T) (addr string, hub *attachsrv.Hub) {
	t.Helper()
	dir := mustShortDir(t)
	hub = attachsrv.NewHub(64)
	srv, err := attachsrv.Listen(context.Background(), attachsrv.Options{
		Bind: "127.0.0.1:0", Network: attachsrv.NetworkTCP, Token: cliTestToken, Dir: dir, Hub: hub,
	})
	if err != nil {
		t.Fatalf("attachsrv.Listen: %v", err)
	}
	t.Cleanup(func() { _ = srv.Close() })

	hub.Emit(api.Event{V: api.Version, Seq: 1, Type: api.RunStarted, Run: "r1"})
	hub.Emit(api.Event{V: api.Version, Seq: 2, Type: api.RunFinished, Run: "r1",
		Payload: mustJSONAttach(api.RunFinishedBody{Status: api.RunSucceeded})})
	return srv.Addr(), hub
}

// A TCP run discovered through the registry needs nothing from the operator:
// the entry already carries the credential. This is the path almost everyone
// is on and the one that has to be frictionless.
func TestCmdAttachFindsATCPRunAndItsTokenThroughTheRegistry(t *testing.T) {
	isolateRegistry(t)
	addr, hub := startTCPRun(t)

	unregister, err := attachsrv.Register(attachsrv.Entry{
		Network: attachsrv.NetworkTCP, Addr: addr, Token: cliTestToken, RunID: "r1", Pipeline: "ci",
	})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	defer unregister()

	var stdout, stderr strings.Builder
	done := make(chan int, 1)
	go func() { done <- cmdAttach([]string{"--ui=none"}, &stdout, &stderr, false) }()

	waitForSubscriber(t, hub)
	_ = hub.Close()

	select {
	case code := <-done:
		if code != exitSuccess {
			t.Fatalf("exit code = %d, want %d; stderr=%s", code, exitSuccess, stderr.String())
		}
	case <-time.After(30 * time.Second):
		t.Fatal("cmdAttach did not return")
	}
}

// The other route: an address given outright, with the credential from the
// environment, which is what a port-forward to another machine looks like
// from here.
func TestCmdAttachDialsAnExplicitAddressWithTheEnvironmentToken(t *testing.T) {
	isolateRegistry(t)
	addr, hub := startTCPRun(t)
	t.Setenv(source.TokenEnv, cliTestToken)

	var stdout, stderr strings.Builder
	done := make(chan int, 1)
	go func() { done <- cmdAttach([]string{"--addr", addr, "--ui=none"}, &stdout, &stderr, false) }()

	waitForSubscriber(t, hub)
	_ = hub.Close()

	select {
	case code := <-done:
		if code != exitSuccess {
			t.Fatalf("exit code = %d, want %d; stderr=%s", code, exitSuccess, stderr.String())
		}
	case <-time.After(30 * time.Second):
		t.Fatal("cmdAttach did not return")
	}
}

// And without the credential it stops before dialling, with a message that
// says where the token comes from. The alternative is a 401 the server
// deliberately makes indistinguishable from every other refusal.
func TestCmdAttachWithAnExplicitAddressAndNoTokenSaysWhereToGetOne(t *testing.T) {
	isolateRegistry(t)
	t.Setenv(source.TokenEnv, "")

	var stdout, stderr strings.Builder
	code := cmdAttach([]string{"--addr", "127.0.0.1:9", "--ui=none"}, &stdout, &stderr, false)
	if code != exitUsage {
		t.Fatalf("exit code = %d, want %d (usage)", code, exitUsage)
	}
	if !strings.Contains(stderr.String(), source.TokenEnv) {
		t.Fatalf("stderr = %q, want it to name $%s", stderr.String(), source.TokenEnv)
	}
}

func TestCmdAttachRefusesAddrCombinedWithDiscoveryFlags(t *testing.T) {
	isolateRegistry(t)
	for _, args := range [][]string{
		{"--addr", "127.0.0.1:9", "--pid", "1"},
		{"--addr", "127.0.0.1:9", "--run", "r1"},
		{"--addr", "127.0.0.1:9", "--follow", "--run", "r1"},
		{"--tls"},
	} {
		var stdout, stderr strings.Builder
		if code := cmdAttach(args, &stdout, &stderr, false); code != exitUsage {
			t.Errorf("cmdAttach(%v) exit = %d, want %d (usage)", args, code, exitUsage)
		}
	}
}

// The multi-run listing is the one place this CLI prints an Entry to a
// human, and a human who sees an ambiguous-selection error is quite likely
// to paste it into a ticket. It must not carry the credential.
func TestTheMultiRunListingNeverPrintsATokenOrAnAddress(t *testing.T) {
	const secret = "MpQ7bqTn6Rr1yfKuHhVe0sZgWc4LxJd2AaBbCcDdEeF"
	got := formatEntries([]attachsrv.Entry{
		{PID: 1, RunID: "r1", Pipeline: "ci", CWD: "/w"},
		{PID: 2, RunID: "r2", Pipeline: "ci", CWD: "/w",
			Network: attachsrv.NetworkTCP, Addr: "127.0.0.1:8443", Token: secret},
	})
	if strings.Contains(got, secret) {
		t.Fatalf("the run listing printed the bearer token:\n%s", got)
	}
	// The listing is still useful: it must name the runs it is asking a
	// person to choose between.
	for _, want := range []string{"r1", "r2", "pid 1", "pid 2"} {
		if !strings.Contains(got, want) {
			t.Errorf("the listing does not mention %q:\n%s", want, got)
		}
	}
}
