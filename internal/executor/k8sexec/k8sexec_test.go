package k8sexec_test

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	senroexec "github.com/xavidop/senro/internal/executor"
	"github.com/xavidop/senro/internal/executor/k8sexec"
	"github.com/xavidop/senro/internal/kubeapi"
	"github.com/xavidop/senro/internal/kubeapi/kindtest"
	"github.com/xavidop/senro/internal/plan"
	"github.com/xavidop/senro/internal/storage"
)

// TestMain owns the cluster's lifetime. Require creates it lazily on the
// first test that needs one, and this is what takes it away again: the
// cluster is per binary, so it cannot be a t.Cleanup on any single test.
//
// Cleanup runs on every exit path, including one where every test skipped,
// because deleting a cluster that is not there succeeds.
func TestMain(m *testing.M) {
	code := m.Run()
	kindtest.Cleanup()
	os.Exit(code)
}

func spec() plan.ExecutorSpec {
	return plan.ExecutorSpec{
		Kind: plan.ExecutorK8s, Image: kindtest.Image, Namespace: kindtest.Namespace,
	}
}

// newExec builds an executor bound to the guarded cluster. Every test goes
// through here, so no test can construct one against anything else.
func newExec(t *testing.T, opts ...k8sexec.Option) *k8sexec.Executor {
	t.Helper()
	c := kindtest.Require(t)
	store, err := storage.Open(filepath.Join(t.TempDir(), "store"))
	if err != nil {
		t.Fatalf("storage.Open: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	opts = append([]k8sexec.Option{
		k8sexec.WithClient(c.Client),
		k8sexec.WithRunID("kindtest"),
		// The image is already on the node after the first test pulls it, and
		// a cluster with no capacity is a failure to report rather than to
		// wait five minutes for.
		k8sexec.WithStartTimeout(3 * time.Minute),
	}, opts...)
	ex, err := k8sexec.New(spec(), store.Snapshotter, opts...)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return ex
}

func sandbox(t *testing.T, ex *k8sexec.Executor, s senroexec.SandboxSpec) senroexec.Sandbox {
	t.Helper()
	if s.StepID == "" {
		s.StepID = t.Name()
	}
	if s.Attempt == 0 {
		s.Attempt = 1
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	sb, err := ex.Sandbox(ctx, s)
	if err != nil {
		t.Fatalf("Sandbox: %v", err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), time.Minute)
		defer closeCancel()
		if err := sb.Close(closeCtx, false); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	return sb
}

func run(t *testing.T, sb senroexec.Sandbox, args ...string) (int, string, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	var out, errOut bytes.Buffer
	exit, err := sb.Run(ctx, senroexec.Cmd{Args: args}, &out, &errOut)
	return exit, out.String() + errOut.String(), err
}

// TestAStepRunsInAPodAndItsOutputComesBack is the whole tranche in one test:
// a command runs in a pod on a real cluster, its bytes reach the caller's
// writer, and it reports success.
func TestAStepRunsInAPodAndItsOutputComesBack(t *testing.T) {
	sb := sandbox(t, newExec(t), senroexec.SandboxSpec{})
	exit, out, err := run(t, sb, "sh", "-c", "echo hello from a pod")
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if exit != 0 {
		t.Errorf("exit = %d, want 0", exit)
	}
	if !strings.Contains(out, "hello from a pod") {
		t.Errorf("output = %q, want the step's own line", out)
	}
}

// TestAFailingCommandIsAnExitCodeAndNotAnError is the load-bearing half of
// the split: a command that exits 7 is the workload's verdict, and returning
// an error for it would make retry.OnInfra() retry every failing test suite.
func TestAFailingCommandIsAnExitCodeAndNotAnError(t *testing.T) {
	sb := sandbox(t, newExec(t), senroexec.SandboxSpec{})
	exit, _, err := run(t, sb, "sh", "-c", "exit 7")
	if err != nil {
		t.Fatalf("a command that exited 7 was reported as an infrastructure failure: %v", err)
	}
	if exit != 7 {
		t.Errorf("exit = %d, want 7", exit)
	}
}

// TestOutputFromBothStreamsSurvives records a real divergence from the
// container executor rather than asserting parity with it: Kubernetes merges
// a container's stdout and stderr into one log, so senro cannot tell them
// apart here. Both sets of bytes must still arrive, and this is what proves
// nothing is dropped on the way.
func TestOutputFromBothStreamsSurvives(t *testing.T) {
	sb := sandbox(t, newExec(t), senroexec.SandboxSpec{})
	exit, out, err := run(t, sb, "sh", "-c", "echo to-stdout; echo to-stderr >&2")
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if exit != 0 {
		t.Fatalf("exit = %d", exit)
	}
	for _, want := range []string{"to-stdout", "to-stderr"} {
		if !strings.Contains(out, want) {
			t.Errorf("output %q is missing %q", out, want)
		}
	}
}

// Outbound trace context, proved where it counts: in the environment of the
// process that actually runs. The pod-spec half is pinned separately in
// TestTheTraceparentIsAnOrdinaryFieldOfThePod; this is the other half, what
// the process was launched with after three translations.
func TestATracedStepsContextReachesTheProcessInThePod(t *testing.T) {
	const header = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"

	sb := sandbox(t, newExec(t), senroexec.SandboxSpec{})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	var out, errOut bytes.Buffer
	exit, err := sb.Run(ctx, senroexec.Cmd{
		Args: []string{"sh", "-c", `printf '%s' "$TRACEPARENT"`},
		Env:  []string{"TRACEPARENT=" + header},
	}, &out, &errOut)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if exit != 0 {
		t.Fatalf("exit = %d, stderr %q", exit, errOut.String())
	}
	if got := strings.TrimSpace(out.String()); got != header {
		t.Errorf("the process in the pod ran with TRACEPARENT=%q, want %q: a tool inside it "+
			"would start a trace of its own instead of joining the run's", got, header)
	}
}

// TestAnImageThatCannotBePulledIsInfrastructure is the other load-bearing
// half, against a live cluster rather than a synthetic status: a pod whose
// image does not exist never runs the command, so there is no verdict to
// report and retry must see executor.ErrInfra.
func TestAnImageThatCannotBePulledIsInfrastructure(t *testing.T) {
	c := kindtest.Require(t)
	bad := spec()
	// A syntactically valid digest reference for an image nobody has. The
	// registry answers, and answers that this manifest is not there.
	bad.Image = "busybox@sha256:" + strings.Repeat("0", 64)
	ex, err := k8sexec.New(bad, nil,
		k8sexec.WithClient(c.Client), k8sexec.WithRunID("kindtest"),
		k8sexec.WithStartTimeout(2*time.Minute))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	sb := sandbox(t, ex, senroexec.SandboxSpec{})
	exit, _, runErr := run(t, sb, "true")
	if runErr == nil {
		t.Fatalf("a pod whose image cannot be pulled returned exit %d and no error", exit)
	}
	if !senroexec.IsInfra(runErr) {
		t.Fatalf("error does not carry executor.ErrInfra, so retry.OnInfra() will not see it: %v", runErr)
	}
}

// The secrets promise, checked against the object the cluster actually
// holds: the value must be readable by the step as a file and appear
// nowhere in the pod (not env, not a command, not an annotation). A pod
// spec is readable by a far wider audience than the Secret, and lands in
// support bundles, describe output and admission webhook logs.
func TestASecretIsAFileAndNeverAFieldOfThePod(t *testing.T) {
	c := kindtest.Require(t)
	const value = "s3cr3t-canary-value"
	sb := sandbox(t, newExec(t), senroexec.SandboxSpec{
		Secrets: []senroexec.SecretRef{{Name: "Registry.Token"}},
	})

	path, err := sb.PutSecret(context.Background(), "Registry.Token", []byte(value))
	if err != nil {
		t.Fatalf("PutSecret: %v", err)
	}
	if !strings.HasPrefix(path, k8sexec.SecretMountPath+"/") {
		t.Fatalf("PutSecret returned %q, which is not inside the projected volume", path)
	}

	exit, out, err := run(t, sb, "sh", "-c", "cat "+path)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if exit != 0 {
		t.Fatalf("the step could not read its secret file: exit %d, %s", exit, out)
	}
	if !strings.Contains(out, value) {
		t.Errorf("the step read %q, want the delivered value", out)
	}

	// The pod as the apiserver holds it, in full.
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	raw, err := c.Kubectl(ctx, "get", "pod", podNameOf(t, sb), "-o", "json")
	if err != nil {
		t.Fatalf("reading the pod back: %v", err)
	}
	if strings.Contains(raw, value) {
		t.Error("the secret's VALUE appears somewhere in the pod object, which anyone with " +
			"`get pods` in this namespace can read")
	}
	if !strings.Contains(raw, "senro-secrets") {
		t.Error("the pod does not project the secret volume at all")
	}
}

// TestClosingASandboxRemovesThePodAndTheSecret is teardown. A leaked Secret
// is a credential sitting in etcd for as long as nobody notices, and a leaked
// pod holds a node.
func TestClosingASandboxRemovesThePodAndTheSecret(t *testing.T) {
	c := kindtest.Require(t)
	ex := newExec(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	sb, err := ex.Sandbox(ctx, senroexec.SandboxSpec{
		StepID: t.Name(), Attempt: 1,
		Secrets: []senroexec.SecretRef{{Name: "Token"}},
	})
	if err != nil {
		t.Fatalf("Sandbox: %v", err)
	}
	if _, err := sb.PutSecret(ctx, "Token", []byte("value")); err != nil {
		t.Fatalf("PutSecret: %v", err)
	}
	var out bytes.Buffer
	if _, err := sb.Run(ctx, senroexec.Cmd{Args: []string{"true"}}, &out, &out); err != nil {
		t.Fatalf("Run: %v", err)
	}
	pod, secret := podNameOf(t, sb), secretNameOf(t, sb)
	if secret == "" {
		t.Fatal("no Secret object was created for a step that declared one")
	}

	if err := sb.Close(ctx, false); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := c.Client.GetPod(ctx, kindtest.Namespace, pod); !kubeapi.IsNotFound(err) {
		t.Errorf("the pod still exists after Close: %v", err)
	}
	// Deleted explicitly by Close rather than left to the ownerReference, so
	// this must be gone immediately rather than eventually.
	if _, err := c.Kubectl(ctx, "get", "secret", secret); err == nil {
		t.Error("the Secret still exists after Close")
	}
}

// TestAWorkspaceIsAWritableDirectoryInThePod is the workspace half of the
// tranche: a mount with no content becomes an emptyDir at the declared path,
// present and writable, which is exactly as much as this executor claims.
func TestAWorkspaceIsAWritableDirectoryInThePod(t *testing.T) {
	sb := sandbox(t, newExec(t), senroexec.SandboxSpec{
		Mounts: []senroexec.Mount{{Name: "build", Path: t.TempDir(), At: "/work"}},
	})
	exit, out, err := run(t, sb, "sh", "-c",
		"touch /work/artifact && ls /work && df /work | tail -1")
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if exit != 0 {
		t.Fatalf("exit = %d: %s", exit, out)
	}
	if !strings.Contains(out, "artifact") {
		t.Errorf("the workspace was not writable: %q", out)
	}
}

// TestAReadOnlyWorkspaceIsEnforced: unlike the local executor, this one CAN
// keep senro.RO, because a volumeMount carries it. The container executor
// makes the same promise through a read-only bind.
func TestAReadOnlyWorkspaceIsEnforced(t *testing.T) {
	sb := sandbox(t, newExec(t), senroexec.SandboxSpec{
		Mounts: []senroexec.Mount{{Name: "inputs", Path: t.TempDir(), At: "/inputs", RO: true}},
	})
	exit, out, err := run(t, sb, "sh", "-c", "touch /inputs/nope")
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if exit == 0 {
		t.Fatalf("a write through a read-only mount succeeded: %q", out)
	}
}

// TestTheObservedPlatformComesFromTheNodeThatRanIt is the second,
// independent observation containerexec's own doc notes a k8s executor
// genuinely has: the declaration comes from the cluster's nodes as a set, and
// the fact comes from the one node that took this pod.
func TestTheObservedPlatformComesFromTheNodeThatRanIt(t *testing.T) {
	ex := newExec(t)
	sb := sandbox(t, ex, senroexec.SandboxSpec{})
	if _, _, err := run(t, sb, "true"); err != nil {
		t.Fatalf("Run: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	observed, err := sb.ObservedPlatform(ctx)
	if err != nil {
		t.Fatalf("ObservedPlatform: %v", err)
	}
	declared, err := ex.DeclaredPlatform(ctx)
	if err != nil {
		t.Fatalf("DeclaredPlatform: %v", err)
	}
	if observed.OS == "" || observed.Arch == "" {
		t.Fatalf("observed platform is incomplete: %+v", observed)
	}
	if observed != declared {
		t.Errorf("observed %s but declared %s on a single-node cluster", observed, declared)
	}
}

// TestClassCarriesTheImageDigestAndNotTheCluster. A cache class built from
// which cluster the step ran in would mean a fleet never shares an entry,
// which is the same defect containerexec's doc records about a host uid.
func TestClassCarriesTheImageDigestAndNotTheCluster(t *testing.T) {
	c := kindtest.Require(t)
	ex := newExec(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	class, err := ex.Class(ctx)
	if err != nil {
		t.Fatalf("Class: %v", err)
	}
	if !strings.Contains(class, kindtest.Image) {
		t.Errorf("class %q does not carry the pinned image", class)
	}
	if strings.Contains(class, c.Conn.Server) || strings.Contains(class, kindtest.Namespace) {
		t.Errorf("class %q names the cluster or namespace, so no two clusters could share a cache entry", class)
	}
}

// TestCancellationIsInfrastructureAndTakesThePodWithIt. A cancelled run must
// not leave a pod running in somebody's cluster, and the error must be
// ErrInfra so runAttempt classifies it as cancelled rather than as a failure
// the retry predicate happened not to match. That is exactly what
// containerexec's Run promises for a killed container.
func TestCancellationIsInfrastructureAndTakesThePodWithIt(t *testing.T) {
	c := kindtest.Require(t)
	sb := sandbox(t, newExec(t), senroexec.SandboxSpec{})

	ctx, cancel := context.WithCancel(context.Background())
	// Run's log goroutine writes here while this test reads, so the writer
	// carries its own lock. A plain bytes.Buffer would be a data race, and a
	// mutex held around the Run CALL instead would deadlock: Run does not
	// return until the step is cancelled and the cancel does not happen until
	// this loop sees the step start.
	out := &syncBuffer{}
	done := make(chan error, 1)
	go func() {
		_, err := sb.Run(ctx, senroexec.Cmd{
			Args: []string{"sh", "-c", "echo started; sleep 600"},
		}, out, out)
		done <- err
	}()

	// Wait for the step to actually be running before cancelling, so this
	// tests cancellation of a live pod rather than of a pod create.
	deadline := time.Now().Add(3 * time.Minute)
	for !strings.Contains(out.String(), "started") {
		if time.Now().After(deadline) {
			cancel()
			<-done
			t.Fatal("the step never started")
		}
		time.Sleep(250 * time.Millisecond)
	}
	cancel()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("a cancelled run returned no error")
		}
		if !senroexec.IsInfra(err) {
			t.Errorf("cancellation does not carry ErrInfra, so runAttempt would classify it as a "+
				"plain failure rather than as cancelled: %v", err)
		}
	case <-time.After(2 * time.Minute):
		t.Fatal("Run did not return after its context was cancelled")
	}

	// The pod is gone: a cancelled run leaving a pod behind is exactly the
	// leak the label exists to find, and it should not need finding.
	poll, pollCancel := context.WithTimeout(context.Background(), time.Minute)
	defer pollCancel()
	name := podNameOf(t, sb)
	for {
		pod, err := c.Client.GetPod(poll, kindtest.Namespace, name)
		if kubeapi.IsNotFound(err) {
			return
		}
		select {
		case <-poll.Done():
			t.Fatalf("the pod survived a cancelled run: %s is still %s (get error %v)",
				name, pod.Status.Phase, err)
		case <-time.After(250 * time.Millisecond):
		}
	}
}

// syncBuffer is a bytes.Buffer a test can read while Run's log goroutine
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

// podNameOf and secretNameOf reach the test-facing accessors the sandbox
// exposes, through the interface value the executor hands back.
func podNameOf(t *testing.T, sb senroexec.Sandbox) string {
	t.Helper()
	n, ok := sb.(interface{ PodName() string })
	if !ok {
		t.Fatal("the sandbox does not expose its pod name")
	}
	return n.PodName()
}

func secretNameOf(t *testing.T, sb senroexec.Sandbox) string {
	t.Helper()
	n, ok := sb.(interface{ SecretName() string })
	if !ok {
		t.Fatal("the sandbox does not expose its secret name")
	}
	return n.SecretName()
}
