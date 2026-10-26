//go:build unix

package senro

import (
	"context"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/exec"
	"github.com/xavidop/senro/service"
)

func upFreePort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	return ln.Addr().(*net.TCPAddr).Port
}

// SENRO_UP=1 selects only the services and what they need, holds them until
// the run is cancelled, and a consumer outside that set is skipped.
func TestUpEnvSelectsServicesAndHoldsThem(t *testing.T) {
	t.Setenv(upEnv, "1")
	t.Chdir(t.TempDir())
	self, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	port := upFreePort(t)

	pipe := New("up")
	w := pipe.Workflow("main")
	w.Service("db", exec.Command(self, "-test.run=^TestHelperServe$"),
		service.Ports(port), service.ReadyTimeout(10*time.Second)).
		Env(helperServePort, strconv.Itoa(port)).Env("GORACE", "atexit_sleep_ms=0")
	w.Step("consumer", exec.Command("true")).Needs("db")
	p, err := pipe.Build()
	if err != nil {
		t.Fatal(err)
	}

	var mu sync.Mutex
	var events []api.Event
	ready := make(chan struct{})
	var once sync.Once
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sk := SinkFunc(func(e api.Event) {
		mu.Lock()
		events = append(events, e)
		mu.Unlock()
		if e.Type == api.ServiceReady {
			once.Do(func() { close(ready) })
		}
	})
	done := make(chan error, 1)
	go func() { done <- RunPlan(ctx, p, WithSink(sk), WithDir(t.TempDir())) }()

	select {
	case <-ready:
	case err := <-done:
		t.Fatalf("the run ended (%v) before the service was ready", err)
	case <-time.After(20 * time.Second):
		t.Fatal("service never became ready")
	}
	select {
	case err := <-done:
		t.Fatalf("the run ended (%v) with a held service and no cancellation", err)
	case <-time.After(300 * time.Millisecond):
	}
	cancel()
	select {
	case <-done:
	case <-time.After(20 * time.Second):
		t.Fatal("the run did not end after cancel")
	}

	mu.Lock()
	defer mu.Unlock()
	finished := map[string]api.StepFinishedBody{}
	for _, e := range events {
		if e.Type == api.StepFinished {
			var b api.StepFinishedBody
			if err := e.Decode(&b); err != nil {
				t.Fatal(err)
			}
			finished[e.Step] = b
		}
	}
	if got := finished["db"].State; got != api.StateSucceeded {
		t.Errorf("db = %s, want succeeded", got)
	}
	c := finished["consumer"]
	if c.State != api.StateSkippedCondition || !strings.Contains(c.Reason, "not selected") {
		t.Errorf("consumer = %s (%q), want skipped as not selected", c.State, c.Reason)
	}
}

// A pipeline without services has nothing for `senro up` to hold; refused
// before any run directory exists.
func TestUpEnvRefusesAPlanWithoutServices(t *testing.T) {
	t.Setenv(upEnv, "1")
	dir := t.TempDir()
	pipe := New("none")
	pipe.Workflow("main").Step("a", exec.Command("true"))
	p, err := pipe.Build()
	if err != nil {
		t.Fatal(err)
	}
	err = RunPlan(context.Background(), p, WithDir(dir))
	if err == nil || !strings.Contains(err.Error(), "declares no services") {
		t.Fatalf("err = %v", err)
	}
	if ents, _ := os.ReadDir(dir); len(ents) != 0 {
		t.Errorf("run directory has %d entries, want none", len(ents))
	}
}
