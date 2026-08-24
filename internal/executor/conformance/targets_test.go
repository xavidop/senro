package conformance_test

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/xavidop/senro/internal/dockerd/dockertest"
	senroexec "github.com/xavidop/senro/internal/executor"
	"github.com/xavidop/senro/internal/executor/containerexec"
	"github.com/xavidop/senro/internal/executor/k8sexec"
	"github.com/xavidop/senro/internal/executor/localexec"
	"github.com/xavidop/senro/internal/executor/sshexec"
	"github.com/xavidop/senro/internal/executor/sshexec/sshdtest"
	"github.com/xavidop/senro/internal/kubeapi/kindtest"
	"github.com/xavidop/senro/internal/plan"
	"github.com/xavidop/senro/internal/storage"
	"github.com/xavidop/senro/internal/workspace"
)

// target is one executor under test, with everything a case needs to build a
// sandbox on it and to seed a workspace it can mount.
type target struct {
	name string
	// new builds the executor with a store of its own. It skips the test
	// itself when the substrate is absent, exactly as each executor's own
	// package does.
	new func(t *testing.T) (senroexec.Executor, *workspace.Snapshotter)
	// newOn builds the executor over a snapshotter the CALLER owns. The
	// engine-level cases need it: senro.Run hands every executor the run's
	// own storage, and an executor snapshotting into a store of its own
	// would write workspace objects the action cache then cannot find.
	newOn func(t *testing.T, snap *workspace.Snapshotter) senroexec.Executor
	// shell is the POSIX shell available on this target. Every executor in
	// this build reaches a /bin/sh; the local one is the coordinator's.
	shell string
	// remoteMounts is true when the target realizes its mounts on a machine
	// that does NOT share the coordinator's filesystem, so a workspace has
	// to be sent out and read back rather than bound in place. It is
	// plan.Node.RemoteMounts' answer, and only ssh and k8s give it:
	// containerexec binds the coordinator's own directories, so a container
	// step and the coordinator are looking at one tree.
	remoteMounts bool
	// mergedStreams is true where the substrate keeps ONE log per step, so
	// stdout and stderr cannot be told apart again. Kubernetes is the one;
	// see k8sexec's doc.
	mergedStreams bool
	// ptySizedAfterStart is true where the substrate offers no way to size a
	// pseudo-terminal before the command runs, so the first size arrives as
	// a frame the command may already have read past. Kubernetes is the one:
	// its exec subresource takes no initial size (see k8sexec's terminal.go).
	// A property of the platform, stated here so the terminal case asserts
	// what each target can actually promise instead of skipping.
	ptySizedAfterStart bool
}

// targets is the matrix. Named once so a new executor is added here and
// every case in this package runs against it.
func targets() []target {
	return []target{
		{name: "local", shell: "/bin/sh", new: newLocal, newOn: newLocalOn},
		{name: "container", shell: "/bin/sh", new: newContainer, newOn: newContainerOn},
		{name: "ssh", shell: "/bin/sh", new: newSSH, newOn: newSSHOn, remoteMounts: true},
		{
			name: "k8s", shell: "/bin/sh", new: newK8s, newOn: newK8sOn,
			remoteMounts: true, mergedStreams: true, ptySizedAfterStart: true,
		},
	}
}

func newStore(t *testing.T) *storage.Storage {
	t.Helper()
	st, err := storage.Open(filepath.Join(t.TempDir(), "store"))
	if err != nil {
		t.Fatalf("storage.Open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return st
}

func newLocal(t *testing.T) (senroexec.Executor, *workspace.Snapshotter) {
	t.Helper()
	st := newStore(t)
	return newLocalOn(t, st.Snapshotter), st.Snapshotter
}

func newLocalOn(t *testing.T, snap *workspace.Snapshotter) senroexec.Executor {
	t.Helper()
	return localexec.New(t.TempDir(), snap)
}

func newContainer(t *testing.T) (senroexec.Executor, *workspace.Snapshotter) {
	t.Helper()
	st := newStore(t)
	return newContainerOn(t, st.Snapshotter), st.Snapshotter
}

func newContainerOn(t *testing.T, snap *workspace.Snapshotter) senroexec.Executor {
	t.Helper()
	c := dockertest.Require(t)
	dockertest.Pull(t, c)
	ex, err := containerexec.New(
		plan.ExecutorSpec{Kind: plan.ExecutorContainer, Image: dockertest.Image},
		snap,
		containerexec.WithClient(c),
		containerexec.WithRunID("conf-"+strings.ReplaceAll(t.Name(), "/", "-")),
	)
	if err != nil {
		t.Fatalf("containerexec.New: %v", err)
	}
	return ex
}

func newSSH(t *testing.T) (senroexec.Executor, *workspace.Snapshotter) {
	t.Helper()
	st := newStore(t)
	return newSSHOn(t, st.Snapshotter), st.Snapshotter
}

func newSSHOn(t *testing.T, snap *workspace.Snapshotter) senroexec.Executor {
	t.Helper()
	srv := sshdtest.Require(t)
	ex, err := sshexec.New(
		plan.ExecutorSpec{Kind: plan.ExecutorSSH, Host: srv.Alias},
		snap,
		sshexec.WithConfig(srv.ConfigPath), sshexec.WithRunID("conf"),
	)
	if err != nil {
		t.Fatalf("sshexec.New: %v", err)
	}
	t.Cleanup(func() { _ = ex.Close() })
	return ex
}

func newK8s(t *testing.T) (senroexec.Executor, *workspace.Snapshotter) {
	t.Helper()
	st := newStore(t)
	return newK8sOn(t, st.Snapshotter), st.Snapshotter
}

func newK8sOn(t *testing.T, snap *workspace.Snapshotter) senroexec.Executor {
	t.Helper()
	c := kindtest.Require(t)
	ex, err := k8sexec.New(
		plan.ExecutorSpec{
			Kind: plan.ExecutorK8s, Image: kindtest.Image, Namespace: kindtest.Namespace,
		},
		snap,
		k8sexec.WithClient(c.Client), k8sexec.WithRunID("conf"),
		k8sexec.WithStartTimeout(3*time.Minute),
	)
	if err != nil {
		t.Fatalf("k8sexec.New: %v", err)
	}
	return ex
}

// sandboxOn builds one sandbox and registers its teardown.
func sandboxOn(
	t *testing.T, ex senroexec.Executor, spec senroexec.SandboxSpec,
) senroexec.Sandbox {
	t.Helper()
	if spec.StepID == "" {
		spec.StepID = "conf"
	}
	if spec.Attempt == 0 {
		spec.Attempt = 1
	}
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	sb, err := ex.Sandbox(ctx, spec)
	if err != nil {
		t.Fatalf("Sandbox: %v", err)
	}
	t.Cleanup(func() {
		c, cc := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cc()
		_ = sb.Close(c, false)
	})
	return sb
}
