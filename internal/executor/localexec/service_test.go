//go:build unix

package localexec_test

import (
	"context"
	"io"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/xavidop/senro/internal/executor"
)

// helperServeEnv tells TestHelperServe to act as a service. Its value is
// "<mode>:<port>": mode "graceful" exits 0 on SIGTERM, "stubborn" ignores
// SIGTERM so only a kill ends it.
const helperServeEnv = "SENRO_TEST_HELPER_SERVE"

// TestHelperServe is not a test: it is the server the service tests start,
// the test binary re-executed with -test.run=^TestHelperServe$. Without the
// env var it returns at once, so an ordinary `go test` passes straight over it.
func TestHelperServe(t *testing.T) {
	v := os.Getenv(helperServeEnv)
	if v == "" {
		return
	}
	mode, port, _ := strings.Cut(v, ":")
	// Before Listen, so a caller that has dialled the port knows the
	// disposition is already in place.
	term := make(chan os.Signal, 1)
	if mode == "stubborn" {
		signal.Ignore(syscall.SIGTERM)
	} else {
		signal.Notify(term, syscall.SIGTERM)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:"+port)
	if err != nil {
		os.Exit(3)
	}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			_ = c.Close()
		}
	}()
	if mode == "stubborn" {
		for {
			time.Sleep(time.Hour)
		}
	}
	<-term
	os.Exit(0)
}

// freePort asks the kernel for a port nothing holds. The window between this
// close and the helper's Listen is a race in principle and negligible in
// practice on a test machine.
func freePort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("finding a free port: %v", err)
	}
	defer func() { _ = ln.Close() }()
	return ln.Addr().(*net.TCPAddr).Port
}

func serviceSandbox(t *testing.T, ports ...int) executor.Sandbox {
	t.Helper()
	ex := newLocal(t, t.TempDir())
	sb, err := ex.Sandbox(context.Background(),
		executor.SandboxSpec{StepID: "db", Attempt: 1, Ports: ports})
	if err != nil {
		t.Fatalf("Sandbox: %v", err)
	}
	t.Cleanup(func() { _ = sb.Close(context.Background(), false) })
	return sb
}

// startHelper starts the test binary as a service in mode on port and waits
// until it accepts a connection at the sandbox's reported address.
func startHelper(t *testing.T, mode string, port int) executor.RunningService {
	t.Helper()
	sb := serviceSandbox(t, port)
	runner, ok := sb.(executor.ServiceRunner)
	if !ok {
		t.Fatal("the local sandbox does not implement executor.ServiceRunner")
	}
	self, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	svc, err := runner.StartService(context.Background(), executor.Cmd{
		Args: []string{self, "-test.run=^TestHelperServe$"},
		Env: []string{
			helperServeEnv + "=" + mode + ":" + strconv.Itoa(port),
			// Under -race the helper otherwise sleeps a second on exit to
			// let late race reports land, which reads as a Stop that waited.
			"GORACE=atexit_sleep_ms=0",
		},
	}, io.Discard, io.Discard)
	if err != nil {
		t.Fatalf("StartService: %v", err)
	}
	stopped := &stopOnce{RunningService: svc}
	t.Cleanup(func() { _ = stopped.Stop(context.Background(), 0) })

	addr, err := sb.(executor.Addressable).HostAddress(context.Background(), port)
	if err != nil {
		t.Fatalf("HostAddress: %v", err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		c, err := net.DialTimeout("tcp", addr, time.Second)
		if err == nil {
			_ = c.Close()
			return stopped
		}
		if time.Now().After(deadline) {
			t.Fatalf("the service never accepted a connection at %s: %v", addr, err)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// stopOnce lets a cleanup stop a helper the test may already have stopped,
// without calling Stop twice, which RunningService does not allow.
type stopOnce struct {
	executor.RunningService
	once sync.Once
	err  error
}

func (s *stopOnce) Stop(ctx context.Context, grace time.Duration) error {
	s.once.Do(func() { s.err = s.RunningService.Stop(ctx, grace) })
	return s.err
}

// waitWithin calls Wait and fails the test if it does not return in d.
func waitWithin(t *testing.T, svc executor.RunningService, d time.Duration) (int, error) {
	t.Helper()
	type result struct {
		exit int
		err  error
	}
	got := make(chan result, 1)
	go func() {
		exit, err := svc.Wait()
		got <- result{exit, err}
	}()
	select {
	case r := <-got:
		return r.exit, r.err
	case <-time.After(d):
		t.Fatalf("Wait did not return within %s", d)
		return 0, nil
	}
}

// A service that honours SIGTERM ends on it, well inside the grace, and the
// exit it chose is what Wait reports.
func TestStartServiceStopsGracefullyOnSIGTERM(t *testing.T) {
	svc := startHelper(t, "graceful", freePort(t))

	start := time.Now()
	if err := svc.Stop(context.Background(), 5*time.Second); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if took := time.Since(start); took > time.Second {
		t.Errorf("Stop took %s for a process that exits on SIGTERM; it waited out the grace", took)
	}
	exit, err := waitWithin(t, svc, time.Second)
	if err != nil || exit != 0 {
		t.Errorf("Wait = (%d, %v), want (0, nil): the service exited cleanly on SIGTERM", exit, err)
	}
}

// A service that ignores SIGTERM is killed once the grace runs out, and its
// death reads the way a shell reports SIGKILL: 128+9.
func TestStopKillsAfterGrace(t *testing.T) {
	svc := startHelper(t, "stubborn", freePort(t))

	if err := svc.Stop(context.Background(), 200*time.Millisecond); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	exit, err := waitWithin(t, svc, 2*time.Second)
	if err != nil || exit != 137 {
		t.Errorf("Wait = (%d, %v), want (137, nil): a SIGKILL death is 128+9", exit, err)
	}
}

// A process's address on this executor is the port it bound itself, on the
// loopback: the declared port is the host port.
func TestHostAddressIsLoopbackAndTheDeclaredPort(t *testing.T) {
	sb := serviceSandbox(t, 5432)
	a, ok := sb.(executor.Addressable)
	if !ok {
		t.Fatal("the local sandbox does not implement executor.Addressable")
	}
	got, err := a.HostAddress(context.Background(), 5432)
	if err != nil || got != "127.0.0.1:5432" {
		t.Errorf("HostAddress(5432) = (%q, %v), want (\"127.0.0.1:5432\", nil)", got, err)
	}
}

// A service's own exit is the workload's verdict, and stopping one that has
// already gone is not an error: the outcome Stop wants is already true.
func TestAServiceThatExitsReportsItsCodeAndStopIsThenANoOp(t *testing.T) {
	runner := serviceSandbox(t).(executor.ServiceRunner)
	svc, err := runner.StartService(context.Background(),
		executor.Cmd{Args: []string{"sh", "-c", "exit 3"}}, io.Discard, io.Discard)
	if err != nil {
		t.Fatalf("StartService: %v", err)
	}
	exit, err := waitWithin(t, svc, 5*time.Second)
	if err != nil || exit != 3 {
		t.Errorf("Wait = (%d, %v), want (3, nil)", exit, err)
	}
	if err := svc.Stop(context.Background(), time.Second); err != nil {
		t.Errorf("Stop after exit = %v, want nil", err)
	}
}

// A program that is not there is the pipeline's mistake, reported as the
// shell's 127 exactly as Run reports it, not as an infrastructure failure.
func TestAServiceWhoseProgramIsMissingExits127(t *testing.T) {
	runner := serviceSandbox(t).(executor.ServiceRunner)
	svc, err := runner.StartService(context.Background(),
		executor.Cmd{Args: []string{"senro-no-such-program"}}, io.Discard, io.Discard)
	if err != nil {
		t.Fatalf("StartService = %v, want a service that exited 127", err)
	}
	exit, err := waitWithin(t, svc, time.Second)
	if err != nil || exit != 127 {
		t.Errorf("Wait = (%d, %v), want (127, nil)", exit, err)
	}
}

// A service that forks the real server is stopped as a whole tree. Were only
// the wrapper signalled, the backgrounded child would live on holding the
// output pipe, and Wait would sit out the full waitDelay with the server
// still running.
func TestStopEndsTheChildrenAServiceForked(t *testing.T) {
	runner := serviceSandbox(t).(executor.ServiceRunner)
	out := &syncBuffer{}
	svc, err := runner.StartService(context.Background(),
		executor.Cmd{Args: []string{"sh", "-c", "sleep 60 & echo up; wait"}}, out, io.Discard)
	if err != nil {
		t.Fatalf("StartService: %v", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for !strings.Contains(out.String(), "up") {
		if time.Now().After(deadline) {
			t.Fatal("the wrapper never started its child")
		}
		time.Sleep(20 * time.Millisecond)
	}
	start := time.Now()
	if err := svc.Stop(context.Background(), 2*time.Second); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	// Under the grace and well under waitDelay: both processes took SIGTERM.
	if took := time.Since(start); took > time.Second {
		t.Errorf("Stop took %s; the forked child outlived the signal and held the pipe", took)
	}
	if _, err := waitWithin(t, svc, time.Second); err != nil {
		t.Errorf("Wait: %v", err)
	}
}

// syncBuffer is a strings.Builder shared by a process's output copy and a
// test polling what it has written so far.
type syncBuffer struct {
	mu sync.Mutex
	b  strings.Builder
}

func (w *syncBuffer) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.b.Write(p)
}

func (w *syncBuffer) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.b.String()
}
