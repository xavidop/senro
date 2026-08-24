package dockerd

// These tests exercise request construction and error handling against a
// fake daemon: a real http.Server on a real unix socket, scripted per case.
// They run with no Docker installed, so the request-building and
// error-classification logic is exercised even where a daemon-gated test
// would silently skip.

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"
)

// shortSocketDir returns a fresh directory suitable for a unix socket path.
// Not t.TempDir(): that embeds the long test name, and a unix socket
// address is capped at 104 bytes on darwin (108 on linux), so a short fixed
// prefix keeps every socket path under the limit.
func shortSocketDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "dockerd")
	if err != nil {
		t.Fatalf("creating a short-path temp dir: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return dir
}

// fakeDaemon starts an HTTP server on a unix socket and returns a Client
// dialled to it through the same Open/SocketPath path a real caller uses,
// so these tests also cover DOCKER_HOST handling rather than constructing a
// Client by hand.
func fakeDaemon(t *testing.T, handler http.HandlerFunc) *Client {
	t.Helper()
	dir := shortSocketDir(t)
	socket := dir + "/d.sock"
	ln, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatalf("listening on a fake daemon socket: %v", err)
	}
	srv := &httptest.Server{Listener: ln, Config: &http.Server{Handler: handler}}
	srv.Start()
	t.Cleanup(srv.Close)

	t.Setenv("DOCKER_HOST", "unix://"+socket)
	c, err := Open()
	if err != nil {
		t.Fatalf("Open against the fake daemon: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func ctx(t *testing.T) context.Context {
	t.Helper()
	c, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	return c
}

// TestDoPrefixesEveryRequestWithTheAPIVersion pins the path shape the
// daemon actually routes on: "/v1.44/<endpoint>", not "/<endpoint>" and not
// "v1.44/<endpoint>" with no leading slash. Get this wrong and every
// request 404s against a real daemon while looking, from the caller's side,
// like the resource just does not exist.
func TestDoPrefixesEveryRequestWithTheAPIVersion(t *testing.T) {
	var gotPath, gotMethod string
	c := fakeDaemon(t, func(w http.ResponseWriter, r *http.Request) {
		gotPath, gotMethod = r.URL.Path, r.Method
		w.WriteHeader(http.StatusOK)
	})
	if err := c.Ping(ctx(t)); err != nil {
		t.Fatalf("Ping: %v", err)
	}
	if gotMethod != http.MethodGet {
		t.Errorf("method = %q, want GET", gotMethod)
	}
	if gotPath != "/"+APIVersion+"/_ping" {
		t.Errorf("path = %q, want /%s/_ping", gotPath, APIVersion)
	}
}

// TestDoSetsJSONContentTypeOnlyWhenThereIsABody: a GET with no body (Ping,
// ContainerStart, ...) must not claim a JSON content type it does not send,
// and a POST that does carry a JSON body (ContainerCreate) must send both
// the header and a body the daemon can decode.
func TestDoSetsJSONContentTypeOnlyWhenThereIsABody(t *testing.T) {
	var sawContentType string
	var sawBody ContainerSpec
	c := fakeDaemon(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/" + APIVersion + "/_ping":
			if ct := r.Header.Get("Content-Type"); ct != "" {
				t.Errorf("Ping set Content-Type %q on a bodyless request", ct)
			}
			w.WriteHeader(http.StatusOK)
		case "/" + APIVersion + "/containers/create":
			sawContentType = r.Header.Get("Content-Type")
			if err := json.NewDecoder(r.Body).Decode(&sawBody); err != nil {
				t.Errorf("decoding the request body the client sent: %v", err)
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]string{"Id": "abc123"})
		default:
			t.Errorf("unexpected request path %q", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	})
	if err := c.Ping(ctx(t)); err != nil {
		t.Fatalf("Ping: %v", err)
	}
	id, err := c.ContainerCreate(ctx(t), ContainerSpec{Image: "busybox:1.36", Cmd: []string{"echo", "hi"}})
	if err != nil {
		t.Fatalf("ContainerCreate: %v", err)
	}
	if id != "abc123" {
		t.Errorf("ContainerCreate id = %q, want abc123", id)
	}
	if sawContentType != "application/json" {
		t.Errorf("ContainerCreate Content-Type = %q, want application/json", sawContentType)
	}
	if sawBody.Image != "busybox:1.36" || len(sawBody.Cmd) != 2 {
		t.Errorf("the daemon decoded a different spec than was sent: %+v", sawBody)
	}
}

// TestImageInspectTreatsA404AsAbsentNotError pins the contract ImageInspect
// promises callers: an image the daemon does not have is (ImageInfo{},
// false, nil), not an error a caller has to string-match to recognise.
func TestImageInspectTreatsA404AsAbsentNotError(t *testing.T) {
	c := fakeDaemon(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"message":"No such image: nosuch:1"}`))
	})
	info, ok, err := c.ImageInspect(ctx(t), "nosuch:1")
	if err != nil {
		t.Fatalf("ImageInspect: %v", err)
	}
	if ok {
		t.Fatal("ok = true for a 404")
	}
	if info.ID != "" || info.RepoDigests != nil || info.OS != "" || info.Arch != "" || info.User != "" || info.Env != nil {
		t.Errorf("ImageInspect returned a non-zero ImageInfo for a 404: %+v", info)
	}
}

// TestImageInspectPropagatesANonNotFoundStatus makes sure the 404 carve-out
// in ImageInspect does not swallow every error: a 500 from the daemon must
// still surface, with its message, so a caller does not silently treat "the
// daemon is broken" the same as "the image is not pulled yet".
func TestImageInspectPropagatesANonNotFoundStatus(t *testing.T) {
	c := fakeDaemon(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"message":"driver failed programming external connectivity"}`))
	})
	_, ok, err := c.ImageInspect(ctx(t), "whatever:1")
	if err == nil {
		t.Fatal("ImageInspect returned nil error for a 500")
	}
	if ok {
		t.Fatal("ok = true for a 500")
	}
	if !strings.Contains(err.Error(), "driver failed programming external connectivity") {
		t.Errorf("error dropped the daemon's message: %v", err)
	}
}

// TestImagePullDrainsProgressAndReturnsTheMidStreamError pins the behaviour
// the doc comment on ImagePull calls out by name: the daemon answers 200 and
// reports a pull failure as a later {"error":...} object in the stream, so a
// client that only checks the initial status code would report success for
// a pull that never completed.
func TestImagePullDrainsProgressAndReturnsTheMidStreamError(t *testing.T) {
	c := fakeDaemon(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		fl, _ := w.(http.Flusher)
		for _, line := range []string{
			`{"status":"Pulling from library/nosuch"}`,
			`{"status":"Pulling fs layer"}`,
			`{"error":"manifest for nosuch:1 not found"}`,
		} {
			_, _ = w.Write([]byte(line + "\n"))
			if fl != nil {
				fl.Flush()
			}
		}
	})
	err := c.ImagePull(ctx(t), "nosuch:1", nil)
	if err == nil {
		t.Fatal("ImagePull returned nil for a stream that ended in an error object")
	}
	if !strings.Contains(err.Error(), "manifest for nosuch:1 not found") {
		t.Errorf("error dropped the daemon's message: %v", err)
	}
}

// TestImagePullSendsTheCredentialInOneHeaderAndNowhereElse is the
// containment assertion for a pull credential: it must reach the daemon in
// the X-Registry-Auth header and appear in no other part of the request.
//
// The URL matters most: it is what every error in client.go quotes back and
// what a daemon writes to its own log, so a credential in the query string
// would leak into places senro's redactor never sees.
func TestImagePullSendsTheCredentialInOneHeaderAndNowhereElse(t *testing.T) {
	const password = "ghp-not-a-real-token"
	var gotHeader, gotURI string
	var gotBody []byte
	c := fakeDaemon(t, func(w http.ResponseWriter, r *http.Request) {
		gotHeader = r.Header.Get("X-Registry-Auth")
		gotURI = r.URL.RequestURI()
		gotBody, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"Pull complete"}` + "\n"))
	})
	auth := &RegistryAuth{Username: "acme-ci", Password: password}
	if err := c.ImagePull(ctx(t), "ghcr.io/acme/builder:v3", auth); err != nil {
		t.Fatalf("ImagePull: %v", err)
	}

	if gotHeader == "" {
		t.Fatal("no X-Registry-Auth header was sent, so the daemon would pull anonymously")
	}
	raw, err := base64.URLEncoding.DecodeString(gotHeader)
	if err != nil {
		t.Fatalf("the header is not the padded base64url the Engine API documents: %v", err)
	}
	var doc struct {
		Username      string `json:"username"`
		Password      string `json:"password"`
		ServerAddress string `json:"serveraddress"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("the header does not decode to an auth document: %v", err)
	}
	if doc.Username != "acme-ci" || doc.Password != password {
		t.Errorf("auth document = %+v, want the credential it was given", doc)
	}
	if doc.ServerAddress != "ghcr.io" {
		t.Errorf("serveraddress = %q, want the registry the reference names", doc.ServerAddress)
	}
	if strings.Contains(gotURI, password) || strings.Contains(gotURI, "acme-ci") {
		t.Errorf("the credential reached the request URI: %q", gotURI)
	}
	if strings.Contains(string(gotBody), password) {
		t.Errorf("the credential reached the request body: %q", gotBody)
	}
}

// TestImagePullWithNoCredentialSendsNoRegistryAuthHeader keeps the ordinary
// pull byte-identical to what it was before credentials existed: an empty
// header is not the same as an absent one, and a daemon that sees one may
// stop consulting its own login.
func TestImagePullWithNoCredentialSendsNoRegistryAuthHeader(t *testing.T) {
	var present bool
	c := fakeDaemon(t, func(w http.ResponseWriter, r *http.Request) {
		_, present = r.Header["X-Registry-Auth"]
		w.WriteHeader(http.StatusOK)
	})
	if err := c.ImagePull(ctx(t), "busybox:1.36", nil); err != nil {
		t.Fatalf("ImagePull: %v", err)
	}
	if present {
		t.Error("a pull with no credential sent an X-Registry-Auth header")
	}
}

// TestAPullRefusalIsToldApartFromEveryOtherPullFailure is the requirement
// that "the credential was refused" and "no such image" reach different
// people: Docker Hub answers 404 for a private repository and 404 for a tag
// that is genuinely absent, so only the message separates them.
//
// Every message here was measured against a real daemon and a real registry
// (ghcr.io, Docker Hub, GitLab, Quay), not invented; see the report on
// container registry credentials.
func TestAPullRefusalIsToldApartFromEveryOtherPullFailure(t *testing.T) {
	cases := []struct {
		name    string
		status  int
		message string
		want    bool
	}{
		{
			name:    "ghcr private repository",
			status:  http.StatusInternalServerError,
			message: "error from registry: denied\ndenied",
			want:    true,
		},
		{
			name:   "docker hub private repository",
			status: http.StatusNotFound,
			message: "pull access denied for senroprivateorg/nope, repository does not exist " +
				"or may require 'docker login'",
			want: true,
		},
		{
			name:    "gitlab registry",
			status:  http.StatusForbidden,
			message: "error from registry: access forbidden",
			want:    true,
		},
		{
			name:   "quay",
			status: http.StatusInternalServerError,
			message: `unknown: failed to resolve reference "quay.io/x/nope:v1": unexpected status ` +
				"from HEAD request to https://quay.io/v2/x/nope/manifests/v1: 401 Unauthorized",
			want: true,
		},
		{
			name:   "a tag that does not exist",
			status: http.StatusNotFound,
			message: `failed to resolve reference "docker.io/library/busybox:no-such-tag": ` +
				"docker.io/library/busybox:no-such-tag: not found",
			want: false,
		},
		{
			name:   "a registry nothing is listening on",
			status: http.StatusInternalServerError,
			message: `failed to resolve reference "127.0.0.1:1/nope:v1": failed to do request: ` +
				`Head "https://127.0.0.1:1/v2/nope/manifests/v1": dial tcp 127.0.0.1:1: ` +
				"connect: connection refused",
			want: false,
		},
		{
			// The phrase that must NOT read as a refused credential: a layer
			// senro could not unpack is a local storage failure.
			name:    "a layer that could not be unpacked",
			status:  http.StatusInternalServerError,
			message: "failed to register layer: error creating overlay mount: permission denied",
			want:    false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := fakeDaemon(t, func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tc.status)
				_ = json.NewEncoder(w).Encode(map[string]string{"message": tc.message})
			})
			err := c.ImagePull(ctx(t), "example.com/x/y:v1", nil)
			if err == nil {
				t.Fatal("ImagePull returned nil for a failed pull")
			}
			if got := errors.Is(err, ErrRegistryAuth); got != tc.want {
				t.Errorf("errors.Is(err, ErrRegistryAuth) = %v, want %v; err = %v", got, tc.want, err)
			}
			// A refusal names the reference as written, not as the URL escapes
			// it: whoever reads it has to recognise what they declared.
			if tc.want && !strings.Contains(err.Error(), "example.com/x/y:v1") {
				t.Errorf("the refusal does not name the image: %v", err)
			}
			if !strings.Contains(err.Error(), tc.message) {
				t.Errorf("the daemon's own message was dropped: %v", err)
			}
		})
	}
}

// TestAPullRefusalIsClassifiedFromTheProgressStreamToo: the daemon reports
// some pull failures as a 200 followed by an {"error":...} object, so a
// classification that only looked at status codes would miss half of them.
func TestAPullRefusalIsClassifiedFromTheProgressStreamToo(t *testing.T) {
	c := fakeDaemon(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"error":"unauthorized: authentication required"}` + "\n"))
	})
	err := c.ImagePull(ctx(t), "ghcr.io/acme/builder:v3", nil)
	if !errors.Is(err, ErrRegistryAuth) {
		t.Errorf("a mid-stream refusal was not classified as one: %v", err)
	}
}

func TestRegistryOfNamesTheHostAReferenceIsPulledFrom(t *testing.T) {
	cases := []struct{ in, want string }{
		{"busybox:1.36", DefaultRegistry},
		{"library/busybox:1.36", DefaultRegistry},
		{"acme/builder:v3", DefaultRegistry},
		{"ghcr.io/acme/builder:v3", "ghcr.io"},
		{"registry.internal:5000/acme/builder:v3", "registry.internal:5000"},
		{"localhost:5000/acme/builder", "localhost:5000"},
		{"localhost/acme/builder", "localhost"},
		{"127.0.0.1:5000/acme/builder@sha256:" + strings.Repeat("a", 64), "127.0.0.1:5000"},
	}
	for _, tc := range cases {
		if got := RegistryOf(tc.in); got != tc.want {
			t.Errorf("RegistryOf(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

// TestImagePullSucceedsWhenTheStreamEndsWithNoError is the companion case:
// a progress stream with no {"error":...} object anywhere in it must not be
// mistaken for a failure merely because it is long.
func TestImagePullSucceedsWhenTheStreamEndsWithNoError(t *testing.T) {
	c := fakeDaemon(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		for _, line := range []string{
			`{"status":"Pulling from library/busybox"}`,
			`{"status":"Pull complete"}`,
			`{"status":"Digest: sha256:abc"}`,
		} {
			_, _ = w.Write([]byte(line + "\n"))
		}
	})
	if err := c.ImagePull(ctx(t), "busybox:1.36", nil); err != nil {
		t.Fatalf("ImagePull: %v", err)
	}
}

// TestContainerWaitReturnsTheDaemonsWaitError pins that a non-empty
// Error.Message in the wait response is surfaced as a Go error even though
// the HTTP status was 200: /wait's own failure signalling is inside its
// JSON body, the same shape ImagePull's is.
func TestContainerWaitReturnsTheDaemonsWaitError(t *testing.T) {
	c := fakeDaemon(t, func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"StatusCode": 0,
			"Error":      map[string]string{"Message": "OCI runtime create failed"},
		})
	})
	code, err := c.ContainerWait(ctx(t), "deadbeef")
	if err == nil {
		t.Fatal("ContainerWait returned nil for a response carrying an Error message")
	}
	if !strings.Contains(err.Error(), "OCI runtime create failed") {
		t.Errorf("error dropped the daemon's message: %v", err)
	}
	if code != 0 {
		t.Errorf("code = %d, want the StatusCode the daemon reported even alongside an error", code)
	}
}

// TestContainerKillTreatsAnAlreadyStoppedContainerAsSuccess pins the one
// case ContainerKill's doc comment promises is not an error: the daemon
// answers 409 with a message naming the container as already stopped.
func TestContainerKillTreatsAnAlreadyStoppedContainerAsSuccess(t *testing.T) {
	c := fakeDaemon(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusConflict)
		_, _ = w.Write([]byte(`{"message":"Cannot kill container: container abc is not running"}`))
	})
	if err := c.ContainerKill(ctx(t), "abc"); err != nil {
		t.Fatalf("ContainerKill on an already-stopped container: %v", err)
	}
}

// TestContainerKillPropagatesAnUnrelatedConflict makes sure the 409
// carve-out is specific to "is not running" and does not swallow every 409.
func TestContainerKillPropagatesAnUnrelatedConflict(t *testing.T) {
	c := fakeDaemon(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusConflict)
		_, _ = w.Write([]byte(`{"message":"some other conflict entirely"}`))
	})
	if err := c.ContainerKill(ctx(t), "abc"); err == nil {
		t.Fatal("ContainerKill swallowed an unrelated 409")
	}
}

// TestContainerRemoveTreatsA404AsSuccess mirrors ImageInspect's contract for
// the same reason: a caller tearing down a container it already lost the
// race on should not have to distinguish "already gone" from "gone".
func TestContainerRemoveTreatsA404AsSuccess(t *testing.T) {
	c := fakeDaemon(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"message":"No such container: abc"}`))
	})
	if err := c.ContainerRemove(ctx(t), "abc"); err != nil {
		t.Fatalf("ContainerRemove on an absent container: %v", err)
	}
}

// TestContainerLogsDemuxesAFakeDaemonsResponse wires ContainerLogs straight
// into a scripted multiplexed body, proving the seam between client.go and
// stream.go without needing a real container: the frames a real daemon
// would send are exactly what the fake one sends here.
func TestContainerLogsDemuxesAFakeDaemonsResponse(t *testing.T) {
	c := fakeDaemon(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(frame(streamStdout, "hello "))
		_, _ = w.Write(frame(streamStderr, "uh oh\n"))
		_, _ = w.Write(frame(streamStdout, "world\n"))
	})
	var out, errOut strings.Builder
	if err := c.ContainerLogs(ctx(t), "abc", &out, &errOut); err != nil {
		t.Fatalf("ContainerLogs: %v", err)
	}
	if out.String() != "hello world\n" {
		t.Errorf("stdout = %q", out.String())
	}
	if errOut.String() != "uh oh\n" {
		t.Errorf("stderr = %q", errOut.String())
	}
}

// TestPingWrapsADialFailure covers the path with no fake daemon at all: a
// socket file that exists (so Open succeeds) with nothing listening on it,
// which is what a stale socket left by a crashed daemon looks like.
//
// net.UnixListener.Close unlinks its socket file by default, which would
// make this test accidentally reproduce TestOpenRejectsAMissingSocket
// instead of the case it names. SetUnlinkOnClose(false) keeps the file in
// place after Close so the socket genuinely exists but answers nobody.
func TestPingWrapsADialFailure(t *testing.T) {
	dir := shortSocketDir(t)
	socket := dir + "/d.sock"
	ln, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatalf("listening: %v", err)
	}
	ln.(*net.UnixListener).SetUnlinkOnClose(false)
	_ = ln.Close()

	t.Setenv("DOCKER_HOST", "unix://"+socket)
	c, err := Open()
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })

	if err := c.Ping(ctx(t)); err == nil {
		t.Fatal("Ping succeeded against a socket with nothing listening")
	}
}

// TestOpenRejectsAMissingSocket is the other half of Open's contract:
// SocketPath resolving is not the same as a daemon being reachable, but a
// socket that does not even exist on disk is caught immediately rather than
// deferred to the first request.
func TestOpenRejectsAMissingSocket(t *testing.T) {
	t.Setenv("DOCKER_HOST", "unix:///nonexistent/senro-test.sock")
	if _, err := Open(); err == nil {
		t.Fatal("Open accepted a socket path that does not exist")
	}
}

// --- error classification, tested directly with no socket at all ---

func TestIsNotFoundMatchesOnlyA404APIError(t *testing.T) {
	if !isNotFound(&apiError{statusCode: http.StatusNotFound, status: "404 Not Found"}) {
		t.Error("isNotFound(404) = false")
	}
	if isNotFound(&apiError{statusCode: http.StatusConflict, status: "409 Conflict"}) {
		t.Error("isNotFound(409) = true")
	}
}

// TestIsNotFoundIgnoresAnUnrelatedErrorContainingTheDigits404 is the
// regression test for the string-matching approach this package does not
// use: isNotFound must not fire just because "404" appears somewhere in an
// error's text (a container id, an image digest fragment, ...).
func TestIsNotFoundIgnoresAnUnrelatedErrorContainingTheDigits404(t *testing.T) {
	err := errors.New("dial unix /var/run/docker.sock: connect: container 404abc123 not found locally")
	if isNotFound(err) {
		t.Error("isNotFound matched a plain error that merely mentions 404")
	}
}

func TestIsNotRunningMatchesTheMessageNotTheStatus(t *testing.T) {
	if !isNotRunning(&apiError{statusCode: http.StatusConflict, message: "container abc is not running"}) {
		t.Error("isNotRunning = false for a message containing \"is not running\"")
	}
	if isNotRunning(&apiError{statusCode: http.StatusConflict, message: "some other conflict"}) {
		t.Error("isNotRunning = true for an unrelated 409")
	}
	if isNotRunning(errors.New("is not running")) {
		t.Error("isNotRunning matched a plain error, not an *apiError")
	}
}

func TestSplitRefHandlesTagsDigestsAndRegistryPorts(t *testing.T) {
	// This exercises the exported SplitRef (client.go), which is what
	// ImagePull calls (see also client_test.go's TestSplitRef), so every
	// case here is checked against the function that actually runs.
	cases := []struct {
		in       string
		wantName string
		wantTag  string
	}{
		{"busybox", "busybox", "latest"},
		{"busybox:1.36", "busybox", "1.36"},
		{"library/busybox:1.36", "library/busybox", "1.36"},
		{"myregistry.example.com:5000/repo", "myregistry.example.com:5000/repo", "latest"},
		{"myregistry.example.com:5000/repo:tag", "myregistry.example.com:5000/repo", "tag"},
		{"busybox@sha256:" + strings.Repeat("a", 64), "busybox", "sha256:" + strings.Repeat("a", 64)},
		{
			"myregistry.example.com:5000/repo@sha256:" + strings.Repeat("b", 64),
			"myregistry.example.com:5000/repo",
			"sha256:" + strings.Repeat("b", 64),
		},
	}
	for _, tc := range cases {
		name, tag := SplitRef(tc.in)
		if name != tc.wantName || tag != tc.wantTag {
			t.Errorf("SplitRef(%q) = (%q, %q), want (%q, %q)", tc.in, name, tag, tc.wantName, tc.wantTag)
		}
	}
}

// ClassifyStartFailure decides whether a refused /start was the COMMAND's
// fault or the daemon's, and the difference is what senro reports: a step
// verdict for the first, an infrastructure failure retry.OnInfra() can act
// on for the second. Getting the boundary wrong in either direction is
// expensive — a mistyped command retried until its budget is gone, or a
// node out of memory recorded as the pipeline's mistake — so the boundary
// is pinned here rather than left to whatever the daemon last said.
func TestClassifyStartFailureSeparatesTheCommandFromTheDaemon(t *testing.T) {
	// The runc sentences, as the daemon relays them. Both have been stable
	// for years; a message that stops matching costs the distinction and
	// nothing else, since the caller then reports infrastructure exactly as
	// it did before this existed.
	cases := []struct {
		name    string
		message string
		want    StartFailure
	}{
		{
			name: "not on PATH",
			message: `failed to create task for container: failed to create shim task: OCI ` +
				`runtime create failed: runc create failed: unable to start container process: ` +
				`exec: "senro-no-such": executable file not found in $PATH`,
			want: StartFailureNotFound,
		},
		{
			name: "absolute path that is not there",
			message: `failed to create task for container: OCI runtime create failed: runc create ` +
				`failed: unable to start container process: error during container init: exec: ` +
				`"/no/such/binary": stat /no/such/binary: no such file or directory`,
			want: StartFailureNotFound,
		},
		{
			name: "there and not executable",
			message: `failed to create task for container: OCI runtime create failed: runc create ` +
				`failed: unable to start container process: exec: "/app/run.sh": permission denied`,
			want: StartFailureNotExecutable,
		},
		{
			name:    "the node is out of memory",
			message: "failed to create task for container: cannot allocate memory",
			want:    StartFailureNone,
		},
		{
			name:    "the daemon is shutting down",
			message: "cannot start a container that is being removed",
			want:    StartFailureNone,
		},
		{
			name:    "a device the host does not have",
			message: "error gathering device information while adding custom device: no such device",
			want:    StartFailureNone,
		},
		{
			// The false positive the process-framing check exists to stop: a
			// bind whose source went away says exactly what a mistyped
			// command says, and it is infrastructure.
			name: "a bind mount whose source vanished",
			message: `error while creating mount source path '/var/run/senro/ws-1': ` +
				`mkdir /var/run/senro/ws-1: no such file or directory`,
			want: StartFailureNone,
		},
		{
			name: "a working directory the image does not have",
			message: `failed to create task for container: OCI runtime create failed: runc create ` +
				`failed: unable to start container process: error during container init: ` +
				`chdir to cwd ("/nope") set in config.json failed: no such file or directory`,
			want: StartFailureNotFound,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := &apiError{
				method: "POST", path: "/containers/abc/start",
				statusCode: 400, status: "400 Bad Request", message: tc.message,
			}
			if got := ClassifyStartFailure(err); got != tc.want {
				t.Errorf("ClassifyStartFailure = %d, want %d\nmessage: %s", got, tc.want, tc.message)
			}
		})
	}
}

// An error that is not the daemon's answer at all — a dial failure, a
// cancelled context — is never a command-level verdict: reporting 127 for a
// daemon that never answered would tell a pipeline its command was wrong
// when nothing ever tried to run it.
func TestClassifyStartFailureIgnoresErrorsThatAreNotTheDaemons(t *testing.T) {
	for _, err := range []error{
		nil,
		errors.New("dial unix /var/run/docker.sock: connect: connection refused"),
		context.Canceled,
	} {
		if got := ClassifyStartFailure(err); got != StartFailureNone {
			t.Errorf("ClassifyStartFailure(%v) = %d, want %d", err, got, StartFailureNone)
		}
	}
}
