package dockerd

// This file tests SocketPath's discovery machinery in isolation from the
// real machine: every test drives resolve, firstSocket and noSocketError
// with candidate lists built from temp directories, never the real system
// paths, so the logic is proven regardless of what is installed wherever
// the tests run.

import (
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// realSocket creates an actual unix socket at path, kept alive via
// t.Cleanup (closing a *net.UnixListener unlinks its file). path must come
// from shortSocketDir, not t.TempDir, to stay under the unix socket address
// length cap.
func realSocket(t *testing.T, path string) {
	t.Helper()
	ln, err := net.Listen("unix", path)
	if err != nil {
		t.Fatalf("creating a real socket at %s: %v", path, err)
	}
	t.Cleanup(func() { _ = ln.Close() })
}

func TestIsSocketAcceptsARealSocketAndRejectsEverythingElse(t *testing.T) {
	dir := shortSocketDir(t)

	sock := dir + "/d.sock"
	realSocket(t, sock)
	if !isSocket(sock) {
		t.Error("isSocket(real socket) = false")
	}

	plain := filepath.Join(dir, "plain")
	if err := os.WriteFile(plain, []byte("not a socket"), 0o644); err != nil {
		t.Fatal(err)
	}
	if isSocket(plain) {
		t.Error("isSocket(regular file) = true")
	}

	subdir := filepath.Join(dir, "subdir")
	if err := os.Mkdir(subdir, 0o755); err != nil {
		t.Fatal(err)
	}
	if isSocket(subdir) {
		t.Error("isSocket(directory) = true")
	}

	if isSocket(filepath.Join(dir, "nonexistent")) {
		t.Error("isSocket(nonexistent path) = true")
	}
}

// TestFirstSocketSkipsMissingAndWrongTypedCandidates: a firstSocket that
// only checked existence, not file type, would return the decoy plain file
// here instead of skipping to the real socket after it.
func TestFirstSocketSkipsMissingAndWrongTypedCandidates(t *testing.T) {
	dir := shortSocketDir(t)
	decoy := filepath.Join(dir, "decoy")
	if err := os.WriteFile(decoy, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	real := dir + "/real.sock"
	realSocket(t, real)

	cs := []runtimeCandidate{
		{path: filepath.Join(dir, "missing"), runtime: "nothing"},
		{path: decoy, runtime: "a decoy file"},
		{path: real, runtime: "the real one"},
	}
	got, ok := firstSocket(cs)
	if !ok {
		t.Fatal("firstSocket found nothing among candidates that include a real socket")
	}
	if got != real {
		t.Fatalf("firstSocket = %q, want %q (a decoy file must not win)", got, real)
	}
}

func TestFirstSocketReportsFalseWhenNoCandidateIsASocket(t *testing.T) {
	dir := shortSocketDir(t)
	cs := []runtimeCandidate{
		{path: filepath.Join(dir, "a"), runtime: "a"},
		{path: filepath.Join(dir, "b"), runtime: "b"},
	}
	if _, ok := firstSocket(cs); ok {
		t.Fatal("firstSocket found something among candidates none of which exist")
	}
}

func TestCandidatesTriesTheDockerDefaultFirstThenHomeBasedRuntimes(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	cs := candidates()
	if len(cs) == 0 {
		t.Fatal("candidates() returned none")
	}
	if cs[0].path != "/var/run/docker.sock" {
		t.Fatalf("candidates()[0] = %q, want /var/run/docker.sock first", cs[0].path)
	}
	want := []string{
		home + "/.docker/run/docker.sock",
		home + "/.orbstack/run/docker.sock",
		home + "/.colima/default/docker.sock",
		home + "/.rd/docker.sock",
	}
	joined := make([]string, len(cs))
	for i, c := range cs {
		joined[i] = c.path
	}
	all := strings.Join(joined, "\n")
	for _, w := range want {
		if !strings.Contains(all, w) {
			t.Errorf("candidates() does not include %q; got:\n%s", w, all)
		}
	}
}

// TestCandidatesSkipsHomeBasedRuntimesWhenHomeIsUnknown pins that a HOME
// os.UserHomeDir cannot resolve does not produce candidate paths built from
// an empty string ("" + "/.docker/run/docker.sock" == "/.docker/run/docker.sock",
// a real, if unlikely, root-relative path that must never be probed).
func TestCandidatesSkipsHomeBasedRuntimesWhenHomeIsUnknown(t *testing.T) {
	t.Setenv("HOME", "")
	cs := candidates()
	for _, c := range cs {
		if strings.HasPrefix(c.path, "/.") {
			t.Errorf("candidates() built a path from an empty HOME: %q", c.path)
		}
	}
}

func TestCandidatesUsesXDGRuntimeDirForRootlessPodmanWhenSet(t *testing.T) {
	t.Setenv("XDG_RUNTIME_DIR", "/tmp/senro-xdg-test")
	t.Setenv("HOME", t.TempDir())
	cs := candidates()
	want := "/tmp/senro-xdg-test/podman/podman.sock"
	for _, c := range cs {
		if c.path == want {
			return
		}
	}
	t.Errorf("candidates() did not honour XDG_RUNTIME_DIR for the rootless Podman path; want %q among %v", want, cs)
}

func TestCandidatesFallsBackToRunUserUIDWhenXDGRuntimeDirIsUnset(t *testing.T) {
	t.Setenv("XDG_RUNTIME_DIR", "")
	t.Setenv("HOME", t.TempDir())
	cs := candidates()
	want := "/run/user/" + strconv.Itoa(os.Getuid()) + "/podman/podman.sock"
	for _, c := range cs {
		if c.path == want {
			return
		}
	}
	t.Errorf("candidates() did not fall back to /run/user/<uid> for the rootless Podman path; want %q among %v", want, cs)
}

func TestResolveDockerHostWinsOverAnyDiscoveredCandidate(t *testing.T) {
	dir := shortSocketDir(t)
	wanted := dir + "/wanted.sock"
	realSocket(t, wanted)
	decoy := dir + "/decoy.sock"
	realSocket(t, decoy)

	got, err := resolve("unix://"+wanted, []runtimeCandidate{{path: decoy, runtime: "discovered"}}, dir+"/containerd.sock")
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if got != wanted {
		t.Fatalf("resolve = %q, want the DOCKER_HOST path %q even though a candidate is also a real socket", got, wanted)
	}
}

func TestResolveRefusesATCPDockerHostEvenWhenACandidateExists(t *testing.T) {
	dir := shortSocketDir(t)
	decoy := dir + "/decoy.sock"
	realSocket(t, decoy)

	_, err := resolve("tcp://build-07.internal:2375",
		[]runtimeCandidate{{path: decoy, runtime: "discovered"}}, dir+"/containerd.sock")
	if err == nil {
		t.Fatal("resolve accepted a tcp:// DOCKER_HOST even though a discoverable candidate exists")
	}
	if !strings.Contains(err.Error(), "bind-mount") {
		t.Fatalf("the refusal does not say why a remote daemon cannot work: %v", err)
	}
}

func TestResolveFindsANonFirstCandidate(t *testing.T) {
	dir := shortSocketDir(t)
	real := dir + "/podman.sock"
	realSocket(t, real)
	cs := []runtimeCandidate{
		{path: filepath.Join(dir, "missing1"), runtime: "docker"},
		{path: filepath.Join(dir, "missing2"), runtime: "colima"},
		{path: real, runtime: "podman"},
	}
	got, err := resolve("", cs, dir+"/containerd.sock")
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if got != real {
		t.Fatalf("resolve = %q, want %q", got, real)
	}
}

func TestResolveWithNoCandidateFoundNamesEveryPathTried(t *testing.T) {
	dir := shortSocketDir(t)
	cs := []runtimeCandidate{
		{path: filepath.Join(dir, "docker.sock"), runtime: "Docker"},
		{path: filepath.Join(dir, "podman.sock"), runtime: "Podman"},
	}
	_, err := resolve("", cs, filepath.Join(dir, "containerd.sock"))
	if err == nil {
		t.Fatal("resolve found a daemon among candidates none of which exist")
	}
	for _, c := range cs {
		if !strings.Contains(err.Error(), c.path) {
			t.Errorf("error does not mention tried path %q: %v", c.path, err)
		}
	}
	if !strings.Contains(err.Error(), "DOCKER_HOST") {
		t.Errorf("error does not say how to point senro at a daemon explicitly: %v", err)
	}
	if strings.Contains(err.Error(), "containerd") {
		t.Errorf("error mentions containerd when no containerd socket exists: %v", err)
	}
}

// TestResolveWithOnlyAContainerdSocketExplainsWhy is the case a Podman-only
// or Docker-only user never hits but a containerd-only one does: every
// Engine-API candidate is absent, yet a daemon of SOME kind is plainly
// running on this machine. Leaving that unexplained would be a confusing
// failure to hand back.
func TestResolveWithOnlyAContainerdSocketExplainsWhy(t *testing.T) {
	dir := shortSocketDir(t)
	cs := []runtimeCandidate{
		{path: filepath.Join(dir, "docker.sock"), runtime: "Docker"},
	}
	containerd := dir + "/containerd.sock"
	realSocket(t, containerd)

	_, err := resolve("", cs, containerd)
	if err == nil {
		t.Fatal("resolve found a daemon when only a containerd socket exists")
	}
	if !strings.Contains(err.Error(), "containerd") {
		t.Fatalf("error does not explain the containerd socket it found: %v", err)
	}
	if !strings.Contains(err.Error(), "gRPC") {
		t.Fatalf("error does not say containerd speaks a different protocol: %v", err)
	}
}

// TestOpenRejectsAPathThatExistsButIsNotASocket: a stale regular file at
// the DOCKER_HOST path must be refused clearly, not dialled into a
// confusing low-level connection error.
func TestOpenRejectsAPathThatExistsButIsNotASocket(t *testing.T) {
	dir := shortSocketDir(t)
	notASocket := dir + "/docker.sock"
	if err := os.WriteFile(notASocket, []byte("not a socket"), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Setenv("DOCKER_HOST", "unix://"+notASocket)
	if _, err := Open(); err == nil {
		t.Fatal("Open accepted a path that exists but is not a unix socket")
	}
}
