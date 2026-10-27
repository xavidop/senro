package containerexec

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/xavidop/senro/internal/dockerd"
	senroexec "github.com/xavidop/senro/internal/executor"
)

// HostAddress tells a port the step never declared apart from a declared
// one asked for too early: the first is the caller's mistake, the second a
// moment's wait.
func TestHostAddressNamesWhyAPortHasNoAddress(t *testing.T) {
	sb := &sandbox{spec: senroexec.SandboxSpec{StepID: "web", Ports: []int{8080}}}
	if _, err := sb.HostAddress(context.Background(), 9999); err == nil ||
		!strings.Contains(err.Error(), "only the ports it declares") {
		t.Errorf("HostAddress(9999) = %v, want the undeclared-port refusal", err)
	}
	if _, err := sb.HostAddress(context.Background(), 8080); err == nil ||
		!strings.Contains(err.Error(), "no container yet") {
		t.Errorf("HostAddress(8080) before start = %v, want the no-container message", err)
	}
}

// fakeDaemonClient is a dockerd client talking to handler over a unix
// socket, the transport Open insists on. The socket lives under a short
// temporary directory: macOS caps a socket path at 104 bytes, which a
// t.TempDir path can exceed.
func fakeDaemonClient(t *testing.T, handler http.HandlerFunc) *dockerd.Client {
	t.Helper()
	dir, err := os.MkdirTemp("", "sx")
	if err != nil {
		t.Fatal(err)
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

// A daemon that refuses the stop request still gets a kill before Stop
// reports the failure: the error says the stop went wrong, and the
// container must not be left running because it did.
func TestStopKillsWhenTheDaemonRefusesTheStop(t *testing.T) {
	var mu sync.Mutex
	var paths []string
	cli := fakeDaemonClient(t, func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		paths = append(paths, r.URL.Path)
		mu.Unlock()
		if strings.HasSuffix(r.URL.Path, "/stop") {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(`{"message":"boom"}`))
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	svc := &containerService{cli: cli, id: "abc", exited: make(chan struct{}), done: make(chan struct{})}

	if err := svc.Stop(context.Background(), 0); err == nil || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("Stop = %v, want the daemon's refusal", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(paths) != 2 || !strings.HasSuffix(paths[0], "/containers/abc/stop") ||
		!strings.HasSuffix(paths[1], "/containers/abc/kill") {
		t.Errorf("requests = %v, want a stop then a kill", paths)
	}
}

// A Stop whose ctx is already done sends no stop request at all, only the
// kill: the caller has no time left for a grace.
func TestStopOnACancelledContextOnlyKills(t *testing.T) {
	var mu sync.Mutex
	var paths []string
	// Stop waits for the container to be seen exiting, and no ContainerWait
	// runs here, so the fake marks the exit when the kill lands.
	exited := make(chan struct{})
	var once sync.Once
	cli := fakeDaemonClient(t, func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		paths = append(paths, r.URL.Path)
		mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
		if strings.HasSuffix(r.URL.Path, "/kill") {
			once.Do(func() { close(exited) })
		}
	})
	svc := &containerService{cli: cli, id: "abc", exited: exited, done: make(chan struct{})}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := svc.Stop(ctx, 0); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(paths) != 1 || !strings.HasSuffix(paths[0], "/containers/abc/kill") {
		t.Errorf("requests = %v, want only a kill", paths)
	}
}
