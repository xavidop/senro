package containerexec_test

// The registry-credential tests. Most of them script a fake daemon rather
// than use a real one: a pull that a registry REFUSES cannot be asked for on
// demand from a live daemon (the test registry this repository starts is
// published on the coordinator's loopback, which a daemon inside a virtual
// machine cannot reach), and the properties under test are what senro sends
// and how it reads the answer. The one thing a fake cannot show, that a
// credential is in no field of a real container's configuration, is proved
// against a real daemon at the bottom of this file.

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/xavidop/senro/internal/cas"
	"github.com/xavidop/senro/internal/dockerd"
	"github.com/xavidop/senro/internal/dockerd/dockertest"
	"github.com/xavidop/senro/internal/executor"
	"github.com/xavidop/senro/internal/executor/containerexec"
	"github.com/xavidop/senro/internal/plan"
	"github.com/xavidop/senro/internal/workspace"
)

// fakeDaemon starts an HTTP server on a unix socket and returns a client
// dialled to it. dockerd has one of these for its own tests and cannot
// export it: a test file inside package dockerd may not import dockertest,
// which imports dockerd.
func fakeDaemon(t *testing.T, handler http.HandlerFunc) *dockerd.Client {
	t.Helper()
	// Not t.TempDir(): it embeds the test's name, and a unix socket path is
	// capped at 104 bytes on darwin.
	dir, err := os.MkdirTemp("", "cexec")
	if err != nil {
		t.Fatalf("creating a short-path temp dir: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })

	socket := dir + "/d.sock"
	ln, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatalf("listening on a fake daemon socket: %v", err)
	}
	srv := &httptest.Server{Listener: ln, Config: &http.Server{Handler: handler}}
	srv.Start()
	t.Cleanup(srv.Close)

	t.Setenv("DOCKER_HOST", "unix://"+socket)
	c, err := dockerd.Open()
	if err != nil {
		t.Fatalf("Open against the fake daemon: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// refusingDaemon answers "I do not have that image" to every inspect and
// refuses every pull with status and message, which is the exact sequence
// Executor.resolve walks.
func refusingDaemon(t *testing.T, status int, message string) *dockerd.Client {
	t.Helper()
	return fakeDaemon(t, func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/json") {
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"message":"No such image"}`))
			return
		}
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(map[string]string{"message": message})
	})
}

func newExecutorWith(t *testing.T, c *dockerd.Client, image string, opts ...containerexec.Option) *containerexec.Executor {
	t.Helper()
	store, err := cas.Open(filepath.Join(t.TempDir(), "cas"))
	if err != nil {
		t.Fatal(err)
	}
	opts = append([]containerexec.Option{
		containerexec.WithClient(c), containerexec.WithRunID(testRunID(t)),
	}, opts...)
	ex, err := containerexec.New(
		plan.ExecutorSpec{Kind: plan.ExecutorContainer, Image: image},
		workspace.NewSnapshotter(store), opts...)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return ex
}

// The message a registry's refusal produces has to send somebody to the
// right place: which registry, which account, and that the credential
// senro presented was rejected rather than missing.
func TestARefusedCredentialNamesTheRegistryAndTheAccount(t *testing.T) {
	const password = "ghp-not-a-real-token"
	c := refusingDaemon(t, http.StatusInternalServerError, "error from registry: denied\ndenied")
	ex := newExecutorWith(t, c, "ghcr.io/acme/builder:v3",
		containerexec.WithRegistryAuth("acme-ci", []byte(password)))

	_, err := ex.Class(t.Context())
	if err == nil {
		t.Fatal("Class succeeded against a registry that refused the pull")
	}
	msg := err.Error()
	for _, want := range []string{"ghcr.io", "acme-ci", "ghcr.io/acme/builder:v3", "refused the credential"} {
		if !strings.Contains(msg, want) {
			t.Errorf("the refusal does not mention %q: %v", want, err)
		}
	}
	if strings.Contains(msg, password) {
		t.Fatalf("the refusal quotes the credential itself: %v", err)
	}
	if !errors.Is(err, executor.ErrInfra) {
		t.Errorf("a failure to resolve an image is infrastructure, and this one is not: %v", err)
	}
}

// A private image with no credential declared is the case senro could not
// serve at all until now, so the refusal has to say how to declare one
// rather than repeating the daemon's "may require 'docker login'".
func TestAPrivateImageWithNoCredentialSaysHowToDeclareOne(t *testing.T) {
	c := refusingDaemon(t, http.StatusNotFound,
		"pull access denied for acme/builder, repository does not exist or may require 'docker login'")
	ex := newExecutorWith(t, c, "ghcr.io/acme/builder:v3")

	_, err := ex.Class(t.Context())
	if err == nil {
		t.Fatal("Class succeeded against a registry that refused the pull")
	}
	for _, want := range []string{"ghcr.io", "container.RegistryAuth", "senro.WithSecrets"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the refusal does not mention %q: %v", want, err)
		}
	}
}

// The other half of the requirement: an image that is genuinely not there
// must not be reported as a credential problem, or every mistyped tag sends
// somebody to rotate a token.
func TestAnAbsentImageIsNotReportedAsARefusedCredential(t *testing.T) {
	c := refusingDaemon(t, http.StatusNotFound,
		`failed to resolve reference "ghcr.io/acme/builder:v9": ghcr.io/acme/builder:v9: not found`)
	ex := newExecutorWith(t, c, "ghcr.io/acme/builder:v9",
		containerexec.WithRegistryAuth("acme-ci", []byte("ghp-not-a-real-token")))

	_, err := ex.Class(t.Context())
	if err == nil {
		t.Fatal("Class succeeded against a registry that had no such image")
	}
	if errors.Is(err, dockerd.ErrRegistryAuth) {
		t.Errorf("an absent image was classified as a refused credential: %v", err)
	}
	if strings.Contains(err.Error(), "refused the credential") {
		t.Errorf("the message blames the credential for an absent image: %v", err)
	}
}

// The containment assertion for everything the executor says to the daemon
// while resolving an image: the credential belongs in one header of one
// request, and in no URL, no body and no other request.
func TestTheCredentialReachesOneHeaderOfOneRequest(t *testing.T) {
	const password = "ghp-not-a-real-token"
	type seen struct {
		method, uri, header, body string
	}
	var requests []seen
	c := fakeDaemon(t, func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		requests = append(requests, seen{
			method: r.Method, uri: r.URL.RequestURI(),
			header: r.Header.Get("X-Registry-Auth"), body: string(body),
		})
		if !strings.HasSuffix(r.URL.Path, "/json") {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"status":"Pull complete"}` + "\n"))
			return
		}
		// The first inspect misses, so a pull happens; the second finds what
		// the pull fetched.
		if len(requests) == 1 {
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"message":"No such image"}`))
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"Id":          "sha256:aaaa",
			"RepoDigests": []string{"ghcr.io/acme/builder@sha256:bbbb"},
			"Os":          "linux", "Architecture": "arm64",
		})
	})
	ex := newExecutorWith(t, c, "ghcr.io/acme/builder:v3",
		containerexec.WithRegistryAuth("acme-ci", []byte(password)))

	if _, err := ex.Class(t.Context()); err != nil {
		t.Fatalf("Class: %v", err)
	}
	if len(requests) != 3 {
		t.Fatalf("resolve made %d requests, want inspect, pull, inspect: %+v", len(requests), requests)
	}
	var carried int
	for i, req := range requests {
		if strings.Contains(req.uri, password) || strings.Contains(req.body, password) {
			t.Errorf("request %d put the credential in its URI or body: %s %s %s",
				i, req.method, req.uri, req.body)
		}
		if req.header == "" {
			continue
		}
		carried++
		if !strings.Contains(req.uri, "/images/create") {
			t.Errorf("request %d carries a credential and is not the pull: %s %s", i, req.method, req.uri)
		}
	}
	if carried != 1 {
		t.Errorf("%d requests carried a credential, want exactly the pull", carried)
	}
}

// A credential is not part of the cache equivalence class, deliberately: the
// class already carries the resolved image DIGEST, which is the content
// address of the exact bytes the credential fetched. Without this, rotating
// a token would invalidate every cache entry on that image, and two machines
// holding two equally valid credentials would never share one.
func TestTheCredentialIsNotInTheCacheClass(t *testing.T) {
	image := dockertest.Image
	withAuth := newExecutor(t, plan.ExecutorSpec{Kind: plan.ExecutorContainer, Image: image})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	plain, err := withAuth.Class(ctx)
	if err != nil {
		t.Fatalf("Class: %v", err)
	}

	c := dockertest.Require(t)
	dockertest.Pull(t, c)
	ex := newExecutorWith(t, c, image,
		containerexec.WithRegistryAuth("acme-ci", []byte("ghp-not-a-real-token")))
	authed, err := ex.Class(ctx)
	if err != nil {
		t.Fatalf("Class with a credential: %v", err)
	}
	if authed != plain {
		t.Errorf("a declared credential moved the cache class: %q vs %q", authed, plain)
	}
}

// Greps the raw inspect document, byte for byte, for the pull credential, the
// way TestASecretsValueNeverAppearsInDockerInspect does for a step's own
// secret. A real daemon, because the point is what Docker itself considers
// part of a container's configuration.
func TestThePullCredentialNeverAppearsInDockerInspect(t *testing.T) {
	const password = "pull-credential-must-not-leak"
	c := dockertest.Require(t)
	dockertest.Pull(t, c)
	ex := newExecutorWith(t, c, dockertest.Image,
		containerexec.WithRegistryAuth("acme-ci", []byte(password)))

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	sb, err := ex.Sandbox(ctx, executor.SandboxSpec{StepID: "inspect", Attempt: 1})
	if err != nil {
		t.Fatalf("Sandbox: %v", err)
	}
	defer func() { _ = sb.Close(context.Background(), false) }()

	code, err := sb.Run(ctx, executor.Cmd{Args: []string{"true"}}, io.Discard, io.Discard)
	if err != nil || code != 0 {
		t.Fatalf("Run: exit %d, err %v", code, err)
	}
	id := sb.(interface{ ContainerID() string }).ContainerID()
	if id == "" {
		t.Fatal("ContainerID is empty after Run; Run should have created a container")
	}
	raw, err := c.ContainerInspectRaw(ctx, id)
	if err != nil {
		t.Fatalf("ContainerInspectRaw: %v", err)
	}
	if strings.Contains(string(raw), password) {
		t.Fatalf("the pull credential appears in docker inspect output: %s", raw)
	}
	if strings.Contains(string(raw), "acme-ci") {
		t.Fatalf("the registry account appears in docker inspect output: %s", raw)
	}
}
