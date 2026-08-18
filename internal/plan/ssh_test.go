package plan_test

import (
	"strings"
	"testing"

	"github.com/xavidop/senro/internal/plan"
)

// sshNode is the smallest plan a validation test can be about: one exec step
// on one ssh host.
func sshNode(spec plan.ExecutorSpec, mounts ...plan.MountSpec) *plan.Plan {
	return &plan.Plan{Nodes: []plan.Node{{
		ID: "deploy", Kind: "exec", Cmd: []string{"true"},
		Executor: &spec, Mounts: mounts,
	}}}
}

func TestAnSSHStepWithAHostValidates(t *testing.T) {
	p := sshNode(plan.ExecutorSpec{Kind: plan.ExecutorSSH, Host: "build-07.internal"})
	if err := p.Validate(); err != nil {
		t.Fatalf("Validate refused a well formed ssh step: %v", err)
	}
}

// The refusal that used to say "the SSH executor is not implemented" is now
// about a genuinely unknown family, and the ssh family reaches an executor.
func TestAnUnknownExecutorFamilyIsStillRefusedByName(t *testing.T) {
	p := sshNode(plan.ExecutorSpec{Kind: "carrier-pigeon"})
	err := p.Validate()
	if err == nil {
		t.Fatal("Validate accepted a step on an executor family this build does not have")
	}
	if !strings.Contains(err.Error(), "carrier-pigeon") {
		t.Errorf("the refusal does not name the family: %v", err)
	}
	if strings.Contains(err.Error(), "SSH executor is not implemented") {
		t.Errorf("the refusal still claims the ssh executor is unimplemented: %v", err)
	}
}

func TestAnSSHStepWithNoHostIsRefused(t *testing.T) {
	p := sshNode(plan.ExecutorSpec{Kind: plan.ExecutorSSH})
	err := p.Validate()
	if err == nil {
		t.Fatal("Validate accepted an ssh step with no destination")
	}
	if !strings.Contains(err.Error(), "ssh.Host") {
		t.Errorf("the refusal does not say how to declare one: %v", err)
	}
}

func TestAnSSHStepWithHalfAPlatformIsRefused(t *testing.T) {
	p := sshNode(plan.ExecutorSpec{Kind: plan.ExecutorSSH, Host: "h", OS: "linux"})
	if err := p.Validate(); err == nil {
		t.Fatal("Validate accepted an ssh step declaring an os and no arch")
	}
}

// A scratch cache crosses to the host and comes back exactly as a workspace
// does, and the run saves what came back, so there is nothing to refuse.
func TestAnSSHStepMountingAScratchCacheIsAccepted(t *testing.T) {
	p := sshNode(
		plan.ExecutorSpec{Kind: plan.ExecutorSSH, Host: "h"},
		plan.MountSpec{Scratch: "gomod", At: "/cache"},
	)
	p.Scratch = []plan.ScratchSpec{{Name: "gomod", Key: "gomod-v1"}}
	if err := p.Validate(); err != nil {
		t.Fatalf("Validate refused an ssh step mounting a scratch cache: %v", err)
	}
}

// A mount path that climbs out of the attempt directory is refused for a
// scratch cache exactly as for a workspace: senro is not root on the host.
func TestAnSSHScratchMountClimbingOutOfTheAttemptDirectoryIsRefused(t *testing.T) {
	p := sshNode(
		plan.ExecutorSpec{Kind: plan.ExecutorSSH, Host: "h"},
		plan.MountSpec{Scratch: "gomod", At: "../../cache"},
	)
	p.Scratch = []plan.ScratchSpec{{Name: "gomod", Key: "gomod-v1"}}
	err := p.Validate()
	if err == nil {
		t.Fatal("Validate accepted an ssh scratch mount climbing out of the attempt directory")
	}
	if !strings.Contains(err.Error(), "gomod") {
		t.Errorf("the refusal does not name the cache: %v", err)
	}
}

// Unlike the k8s executor, an ssh step CAN mount a workspace and CAN be
// snapshotted afterwards: there is a real filesystem on the other end and tar
// over the connection carries it in both directions.
func TestAnSSHStepMountingAWorkspaceWithoutNoSnapshotIsAccepted(t *testing.T) {
	p := &plan.Plan{
		Workspaces: []plan.WorkspaceSpec{{Name: "src", Scope: "run"}},
		Nodes: []plan.Node{{
			ID: "build", Kind: "exec", Cmd: []string{"make"},
			Executor: &plan.ExecutorSpec{Kind: plan.ExecutorSSH, Host: "h"},
			Mounts:   []plan.MountSpec{{Workspace: "src", At: "/src"}},
			WorkDir:  "/src",
		}},
	}
	if err := p.Validate(); err != nil {
		t.Fatalf("Validate refused an ssh step that mounts a workspace: %v", err)
	}
}

// A mount path is realized UNDER the step's own per-attempt directory on the
// remote host, because senro is not root there and cannot create /src. A path
// climbing out of that directory with ".." would land somewhere on the host
// nobody named, so it is refused where it is written rather than where it
// would be created.
func TestAnSSHMountPathThatClimbsOutIsRefused(t *testing.T) {
	p := &plan.Plan{
		Workspaces: []plan.WorkspaceSpec{{Name: "src", Scope: "run"}},
		Nodes: []plan.Node{{
			ID: "build", Kind: "exec", Cmd: []string{"make"},
			Executor: &plan.ExecutorSpec{Kind: plan.ExecutorSSH, Host: "h"},
			Mounts:   []plan.MountSpec{{Workspace: "src", At: "../../etc"}},
		}},
	}
	err := p.Validate()
	if err == nil {
		t.Fatal("Validate accepted an ssh mount path that leaves the attempt directory")
	}
	if !strings.Contains(err.Error(), "..") {
		t.Errorf("the refusal does not quote the path: %v", err)
	}
}

// Two hosts are two executors: one connection and one set of resolved host
// facts each. Two classes on one host are also two, because the executor reads
// the class off its own spec.
func TestTheExecutorKeySeparatesHostsAndClasses(t *testing.T) {
	a := plan.ExecutorSpec{Kind: plan.ExecutorSSH, Host: "build-07"}
	b := plan.ExecutorSpec{Kind: plan.ExecutorSSH, Host: "build-08"}
	c := plan.ExecutorSpec{Kind: plan.ExecutorSSH, Host: "build-07", Class: "toolchain-v3"}
	d := plan.ExecutorSpec{Kind: plan.ExecutorSSH, Host: "build-07", Root: "/var/lib/senro/ws"}
	keys := map[string]bool{a.Key(): true, b.Key(): true, c.Key(): true, d.Key(): true}
	if len(keys) != 4 {
		t.Errorf("four distinct ssh targets collapsed into %d executor keys: %v", len(keys), keys)
	}
}

// The new fields must not move any key that existed before them.
func TestTheExecutorKeyOfANonSSHSpecIsUnmoved(t *testing.T) {
	for _, tc := range []struct {
		spec plan.ExecutorSpec
		want string
	}{
		{plan.ExecutorSpec{Kind: plan.ExecutorLocal}, "local"},
		{plan.ExecutorSpec{Kind: plan.ExecutorContainer, Image: "node:22"}, "container:node:22"},
		{
			plan.ExecutorSpec{Kind: plan.ExecutorK8s, Image: "r@sha256:a", Namespace: "ci"},
			"k8s:r@sha256:a@ci",
		},
	} {
		if got := tc.spec.Key(); got != tc.want {
			t.Errorf("Key() = %q, want %q", got, tc.want)
		}
	}
}
