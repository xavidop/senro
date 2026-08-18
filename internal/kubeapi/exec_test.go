package kubeapi_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/xavidop/senro/internal/kubeapi"
	"github.com/xavidop/senro/internal/kubeapi/kindtest"
)

// TestMain owns the kind cluster's lifetime for this package, exactly as the
// k8s executor's own TestMain does: Require creates it lazily on the first
// test that needs one, and this takes it away again.
func TestMain(m *testing.M) {
	code := m.Run()
	kindtest.Cleanup()
	os.Exit(code)
}

// execPod creates a pod that stays up long enough to be exec'd into, and
// returns its name. It carries an init container that idles as well, so a
// test can prove exec reaches a container the pod's phase does not yet call
// running.
func execPod(t *testing.T, c *kindtest.Cluster, name string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	pod := kubeapi.Pod{
		Metadata: kubeapi.ObjectMeta{Name: name, Namespace: kindtest.Namespace},
		Spec: kubeapi.PodSpec{
			RestartPolicy:                "Never",
			AutomountServiceAccountToken: new(bool),
			InitContainers: []kubeapi.Container{{
				Name: "gate", Image: kindtest.Image, ImagePullPolicy: "IfNotPresent",
				Command: []string{"sh", "-c", "while [ ! -f /tmp/go ]; do sleep 1; done"},
			}},
			Containers: []kubeapi.Container{{
				Name: "main", Image: kindtest.Image, ImagePullPolicy: "IfNotPresent",
				Command: []string{"sh", "-c", "sleep 600"},
			}},
		},
	}
	if _, err := c.Client.CreatePod(ctx, kindtest.Namespace, pod); err != nil {
		t.Fatalf("creating the exec target pod: %v", err)
	}
	t.Cleanup(func() {
		bg, bgCancel := context.WithTimeout(context.Background(), time.Minute)
		defer bgCancel()
		_ = c.Client.DeletePod(bg, kindtest.Namespace, name, 0)
	})
	return name
}

// waitInitRunning blocks until the pod's init container is running.
func waitInitRunning(t *testing.T, c *kindtest.Cluster, name string) {
	t.Helper()
	deadline := time.Now().Add(4 * time.Minute)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		pod, err := c.Client.GetPod(ctx, kindtest.Namespace, name)
		cancel()
		if err == nil {
			for _, st := range pod.Status.InitContainerStatuses {
				if st.Name == "gate" && st.State.Running != nil {
					return
				}
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("the init container of pod %s never started (last error %v)", name, err)
		}
		time.Sleep(250 * time.Millisecond)
	}
}

// waitMainRunning blocks until the pod's ordinary container is running, which
// on this pod means the init container has been released.
func waitMainRunning(t *testing.T, c *kindtest.Cluster, name string) {
	t.Helper()
	deadline := time.Now().Add(4 * time.Minute)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		pod, err := c.Client.GetPod(ctx, kindtest.Namespace, name)
		cancel()
		if err == nil {
			for _, st := range pod.Status.ContainerStatuses {
				if st.Name == "main" && st.State.Running != nil {
					return
				}
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("the container of pod %s never started (last error %v)", name, err)
		}
		time.Sleep(250 * time.Millisecond)
	}
}

// TestExecReachesAContainerAndReportsItsExitCode is the whole subresource in
// one test: a command runs inside a container that is already up, its two
// streams come back apart, and its exit status is a number rather than an
// error.
//
// The split matters for the same reason it matters in every executor: a
// command that failed is a verdict, and a stream that broke is not.
func TestExecReachesAContainerAndReportsItsExitCode(t *testing.T) {
	c := kindtest.Require(t)
	name := execPod(t, c, "kubeapi-exec-basic")
	waitInitRunning(t, c, name)
	if err := release(t, c, name, "gate"); err != nil {
		t.Fatalf("releasing the init container: %v", err)
	}
	waitMainRunning(t, c, name)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	var out, errOut bytes.Buffer
	exit, err := c.Client.Exec(ctx, kubeapi.ExecSpec{
		Namespace: kindtest.Namespace, Pod: name, Container: "main",
		Command: []string{"sh", "-c", "echo to-stdout; echo to-stderr >&2; exit 7"},
		Stdout:  &out, Stderr: &errOut,
	})
	if err != nil {
		t.Fatalf("Exec: %v", err)
	}
	if exit != 7 {
		t.Errorf("exit = %d, want 7", exit)
	}
	if got := strings.TrimSpace(out.String()); got != "to-stdout" {
		t.Errorf("stdout = %q, want %q", got, "to-stdout")
	}
	if got := strings.TrimSpace(errOut.String()); got != "to-stderr" {
		t.Errorf("stderr = %q, want %q: the exec endpoint keeps the two apart even though the "+
			"pod log endpoint merges them", got, "to-stderr")
	}
}

// TestExecCarriesStdinAllTheWayToEOF is the half a workspace transfer stands
// on. The bytes must arrive whole, and the command must see the END of them:
// a reader that never gets EOF hangs forever, which is why this asks for a
// byte count from a command that cannot answer until its input is closed.
//
// A megabyte rather than a line, so the payload crosses several websocket
// frames and several reads on the far side.
func TestExecCarriesStdinAllTheWayToEOF(t *testing.T) {
	c := kindtest.Require(t)
	name := execPod(t, c, "kubeapi-exec-stdin")
	waitInitRunning(t, c, name)
	if err := release(t, c, name, "gate"); err != nil {
		t.Fatalf("releasing the init container: %v", err)
	}
	waitMainRunning(t, c, name)

	payload := bytes.Repeat([]byte("senro"), 200000) // 1,000,000 bytes
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	var out, errOut bytes.Buffer
	exit, err := c.Client.Exec(ctx, kubeapi.ExecSpec{
		Namespace: kindtest.Namespace, Pod: name, Container: "main",
		Command: []string{"sh", "-c", "wc -c"},
		Stdin:   bytes.NewReader(payload), Stdout: &out, Stderr: &errOut,
	})
	if err != nil {
		t.Fatalf("Exec: %v (stderr %q)", err, errOut.String())
	}
	if exit != 0 {
		t.Fatalf("exit = %d, stderr %q", exit, errOut.String())
	}
	if got := strings.TrimSpace(out.String()); got != "1000000" {
		t.Errorf("the command counted %q bytes, want 1000000: stdin did not arrive whole", got)
	}
}

// TestExecReachesARunningInitContainer is the ordering property the whole
// copy-in design rests on. A pod's ordinary containers do not start until its
// init containers have finished, so an init container that idles is the only
// place a workspace's bytes can land BEFORE the step's process runs. The pod
// is Pending while that is true, and the exec subresource still reaches it.
func TestExecReachesARunningInitContainer(t *testing.T) {
	c := kindtest.Require(t)
	name := execPod(t, c, "kubeapi-exec-init")
	waitInitRunning(t, c, name)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	pod, err := c.Client.GetPod(ctx, kindtest.Namespace, name)
	if err != nil {
		t.Fatalf("GetPod: %v", err)
	}
	if pod.Status.Phase != "Pending" {
		t.Errorf("pod phase is %q while its init container runs, want Pending: this test is "+
			"meant to prove exec works on a pod that is not yet Running", pod.Status.Phase)
	}
	var out, errOut bytes.Buffer
	exit, err := c.Client.Exec(ctx, kubeapi.ExecSpec{
		Namespace: kindtest.Namespace, Pod: name, Container: "gate",
		Command: []string{"sh", "-c", "echo in-the-init-container"},
		Stdout:  &out, Stderr: &errOut,
	})
	if err != nil {
		t.Fatalf("Exec into a running init container: %v (stderr %q)", err, errOut.String())
	}
	if exit != 0 {
		t.Fatalf("exit = %d, stderr %q", exit, errOut.String())
	}
	if !strings.Contains(out.String(), "in-the-init-container") {
		t.Errorf("stdout = %q", out.String())
	}
}

// TestExecOnAContainerThatIsNotThereFails: an exec that never ran a command
// must not report exit 0. A transfer built on this would otherwise believe a
// workspace had been written when nothing had.
func TestExecOnAContainerThatIsNotThereFails(t *testing.T) {
	c := kindtest.Require(t)
	name := execPod(t, c, "kubeapi-exec-missing")
	waitInitRunning(t, c, name)
	if err := release(t, c, name, "gate"); err != nil {
		t.Fatalf("releasing the init container: %v", err)
	}
	waitMainRunning(t, c, name)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	var out bytes.Buffer
	exit, err := c.Client.Exec(ctx, kubeapi.ExecSpec{
		Namespace: kindtest.Namespace, Pod: name, Container: "no-such-container",
		Command: []string{"true"}, Stdout: &out, Stderr: &out,
	})
	if err == nil {
		t.Fatalf("Exec into a container that does not exist returned exit %d and no error", exit)
	}
}

// TestACancelledExecFailsRatherThanReturningWhatItHad is the loud-failure
// promise. A transfer interrupted halfway must be an error: a truncated
// workspace that came back as a success would be digested and cached, and
// every run afterwards would be built on it.
func TestACancelledExecFailsRatherThanReturningWhatItHad(t *testing.T) {
	c := kindtest.Require(t)
	name := execPod(t, c, "kubeapi-exec-cancel")
	waitInitRunning(t, c, name)
	if err := release(t, c, name, "gate"); err != nil {
		t.Fatalf("releasing the init container: %v", err)
	}
	waitMainRunning(t, c, name)

	ctx, cancel := context.WithCancel(context.Background())
	// Exec's own reader goroutine writes here while this test reads, so the
	// writer carries its own lock. A plain bytes.Buffer is a data race the
	// detector catches, and a mutex held around the Exec CALL instead would
	// deadlock: Exec does not return until the command is cancelled, and the
	// cancel does not happen until this loop sees the command start.
	out := &syncBuffer{}
	done := make(chan error, 1)
	go func() {
		_, err := c.Client.Exec(ctx, kubeapi.ExecSpec{
			Namespace: kindtest.Namespace, Pod: name, Container: "main",
			Command: []string{"sh", "-c", "echo started; sleep 600"},
			Stdout:  out, Stderr: out,
		})
		done <- err
	}()
	deadline := time.Now().Add(2 * time.Minute)
	for !strings.Contains(out.String(), "started") {
		if time.Now().After(deadline) {
			cancel()
			<-done
			t.Fatal("the exec'd command never produced output")
		}
		time.Sleep(100 * time.Millisecond)
	}
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("a cancelled exec reported success, so a half-finished transfer would " +
				"look like a whole one")
		}
		if !errors.Is(err, context.Canceled) {
			t.Logf("cancelled exec failed with %v", err)
		}
	case <-time.After(time.Minute):
		t.Fatal("Exec did not return after its context was cancelled")
	}
}

// TestAStdinSourceThatFailsEndsTheCommandRatherThanHangingIt: the far side
// of a transfer is `tar -x`, which reads until its input ends, so a reader
// that fails halfway must still close stdin or the step fails as a TIMEOUT
// instead of naming the real fault. The command keeps a shell alive around
// the reader on purpose: a single exec-ed process happens to end the
// session on its own on a kind cluster, while a shell that outlives the
// reader hangs for the whole context, which is the shape a truncated tar
// has and therefore the shape worth pinning.
func TestAStdinSourceThatFailsEndsTheCommandRatherThanHangingIt(t *testing.T) {
	c := kindtest.Require(t)
	name := execPod(t, c, "kubeapi-exec-badstdin")
	waitInitRunning(t, c, name)
	if err := release(t, c, name, "gate"); err != nil {
		t.Fatalf("releasing the init container: %v", err)
	}
	waitMainRunning(t, c, name)

	// A generous deadline that a hang would still hit, so a regression shows
	// up as this test failing rather than as the whole package timing out.
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	var out bytes.Buffer
	start := time.Now()
	_, err := c.Client.Exec(ctx, kubeapi.ExecSpec{
		Namespace: kindtest.Namespace, Pod: name, Container: "main",
		Command: []string{"sh", "-c", "cat > /tmp/received; echo done"},
		Stdin:   &failingReader{after: 1 << 16}, Stdout: &out, Stderr: &out,
	})
	if err == nil {
		t.Fatal("an exec whose stdin failed halfway reported success")
	}
	if !strings.Contains(err.Error(), "senro test: the source went away") {
		t.Errorf("the error does not name the reader that failed: %v", err)
	}
	if ctx.Err() != nil {
		t.Errorf("Exec waited for its context (%s) instead of ending when its input failed",
			time.Since(start))
	}
}

// failingReader produces bytes and then breaks, which is what an unreadable
// file inside a workspace does to the tar being written from it.
type failingReader struct {
	after int
	sent  int
}

func (f *failingReader) Read(p []byte) (int, error) {
	if f.sent >= f.after {
		return 0, errors.New("senro test: the source went away")
	}
	n := min(len(p), f.after-f.sent)
	for i := range n {
		p[i] = 'x'
	}
	f.sent += n
	return n, nil
}

// TestExecOnATerminalAllocatesOneAndAppliesItsSize is the fifth channel: a
// request that asks for a tty gets a pseudo-terminal from the runtime and a
// channel the client writes window sizes to. `senro shell --tty` on a
// cluster stands on both, and a client that could not send a size would
// leave every full-screen program drawing at "0 0".
//
// Two things this test has to get right, both of which a terminal session
// gets right in production and a naive test does not:
//
//   - Stdin stays OPEN. A terminal has no EOF, so closing the input closes
//     the pty and the shell on the far side dies with it; k8sexec's
//     RunTerminal sends the VEOF byte instead, and only when the operator's
//     own input has ended.
//   - The size is POLLED rather than read once, because it arrives as a
//     message on the same connection and a single read can lose the race
//     with a terminal that has only just been created. One second of
//     granularity, never a fractional sleep, which is not POSIX.
func TestExecOnATerminalAllocatesOneAndAppliesItsSize(t *testing.T) {
	c := kindtest.Require(t)
	name := execPod(t, c, "kubeapi-exec-tty")
	waitInitRunning(t, c, name)
	if err := release(t, c, name, "gate"); err != nil {
		t.Fatalf("releasing the init container: %v", err)
	}
	waitMainRunning(t, c, name)

	sizes := make(chan kubeapi.TermSize, 1)
	sizes <- kubeapi.TermSize{Width: 120, Height: 40}
	defer close(sizes)
	stdin, stdinW := io.Pipe()
	defer func() { _ = stdinW.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	var out syncBuffer
	exit, err := c.Client.Exec(ctx, kubeapi.ExecSpec{
		Namespace: kindtest.Namespace, Pod: name, Container: "main",
		Command: []string{"sh", "-c",
			`test -t 0 && echo IS_TTY; i=0; ` +
				`while [ $i -lt 30 ]; do s=$(stty size); ` +
				`case "$s" in "40 120") echo "GOT $s"; exit 0;; esac; ` +
				`i=$((i+1)); sleep 1; done; echo "LAST $(stty size)"`},
		Stdin: stdin, Stdout: &out, TTY: true, Resize: sizes,
	})
	if err != nil {
		t.Fatalf("Exec: %v", err)
	}
	if exit != 0 {
		t.Errorf("exit = %d, want 0: %q", exit, out.String())
	}
	if !strings.Contains(out.String(), "IS_TTY") {
		t.Errorf("the command did not get a terminal: %q", out.String())
	}
	if !strings.Contains(out.String(), "GOT 40 120") {
		t.Errorf("the command read %q, want the 40x120 window this client sent", out.String())
	}
}

// release lets the idling init container of an exec target finish.
func release(t *testing.T, c *kindtest.Cluster, pod, container string) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	var out bytes.Buffer
	exit, err := c.Client.Exec(ctx, kubeapi.ExecSpec{
		Namespace: kindtest.Namespace, Pod: pod, Container: container,
		Command: []string{"touch", "/tmp/go"}, Stdout: &out, Stderr: &out,
	})
	if err != nil {
		return err
	}
	if exit != 0 {
		return errors.New("touch exited " + out.String())
	}
	return nil
}

// syncBuffer is a bytes.Buffer a test can read while a goroutine under test
// writes into it.
type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}
