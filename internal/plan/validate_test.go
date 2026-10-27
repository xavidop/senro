package plan_test

import (
	"strings"
	"testing"

	"github.com/xavidop/senro/internal/plan"
)

// checkGenPlan is one exec step that regenerates into a read-write mount of
// "gen" and declares CheckGenerated, which Validate accepts.
func checkGenPlan(mutate func(*plan.Node)) *plan.Plan {
	n := plan.Node{
		ID: "codegen", Kind: "exec", Cmd: []string{"go", "generate", "./..."},
		Mounts:         []plan.MountSpec{{Workspace: "gen", At: "/src", Mode: "rw"}},
		CheckGenerated: true,
	}
	if mutate != nil {
		mutate(&n)
	}
	return &plan.Plan{
		Version:    1,
		Nodes:      []plan.Node{n},
		Workspaces: []plan.WorkspaceSpec{{Name: "gen", Scope: "run"}},
		Scratch:    []plan.ScratchSpec{{Name: "gomod", Key: "gomod-v1"}},
	}
}

func wantCheckGeneratedErr(t *testing.T, p *plan.Plan, parts ...string) {
	t.Helper()
	err := p.Validate()
	if err == nil {
		t.Fatalf("Validate accepted the plan, want an error containing %q", parts)
	}
	for _, s := range parts {
		if !strings.Contains(err.Error(), s) {
			t.Fatalf("err = %v, want it to contain %q", err, s)
		}
	}
}

func TestValidateAcceptsCheckGeneratedOnAStepWithAReadWriteMount(t *testing.T) {
	if err := checkGenPlan(nil).Validate(); err != nil {
		t.Fatalf("Validate refused CheckGenerated on a step with a read-write mount: %v", err)
	}
	// An empty Mode is read-write.
	if err := checkGenPlan(func(n *plan.Node) { n.Mounts[0].Mode = "" }).Validate(); err != nil {
		t.Fatalf("Validate refused CheckGenerated on a mount whose empty mode means rw: %v", err)
	}
}

func TestValidateRefusesCheckGeneratedOnAService(t *testing.T) {
	wantCheckGeneratedErr(t, checkGenPlan(func(n *plan.Node) {
		n.Service = &plan.ServiceSpec{
			Ports: []int{5432}, Probe: plan.ProbeSpec{Kind: plan.ServiceProbeTCP, Port: 5432},
			ReadyTimeoutMS: 60000, StopGraceMS: 10000,
		}
	}), `step "codegen"`, "CheckGenerated", "service")
}

func TestValidateRefusesCheckGeneratedWithNoSnapshot(t *testing.T) {
	wantCheckGeneratedErr(t, checkGenPlan(func(n *plan.Node) { n.NoSnapshot = true }),
		`step "codegen"`, "CheckGenerated", "NoSnapshot")
}

// A step-scoped workspace is never snapshotted (nobody reads it after the
// step), so a step whose only read-write mount is one gives the check
// nothing to compare.
func TestValidateRefusesCheckGeneratedWhoseOnlyReadWriteMountIsStepScoped(t *testing.T) {
	p := checkGenPlan(nil)
	p.Workspaces[0].Scope = "step"
	wantCheckGeneratedErr(t, p, `step "codegen"`, "CheckGenerated", "step-scoped")

	// A run-scoped rw mount beside it is enough.
	p = checkGenPlan(func(n *plan.Node) {
		n.Mounts = append(n.Mounts, plan.MountSpec{Workspace: "repo", At: "/repo"})
	})
	p.Workspaces[0].Scope = "step"
	p.Workspaces = append(p.Workspaces, plan.WorkspaceSpec{Name: "repo", Scope: "run"})
	if err := p.Validate(); err != nil {
		t.Fatalf("Validate refused CheckGenerated with a run-scoped rw mount beside a step-scoped one: %v", err)
	}
}

// A claim-backed workspace lives in the cluster and has no coordinator-side
// copy to snapshot, so it gives the check nothing to compare either.
func TestValidateRefusesCheckGeneratedWhoseOnlyReadWriteMountIsClaimBacked(t *testing.T) {
	wantCheckGeneratedErr(t, checkGenPlan(func(n *plan.Node) {
		n.Executor = &plan.ExecutorSpec{
			Kind:      plan.ExecutorK8s,
			Image:     "ghcr.io/acme/runner@sha256:" + strings.Repeat("a", 64),
			Namespace: "ci",
			Claims:    map[string]string{"gen": "gen-pvc"},
		}
	}), `step "codegen"`, "CheckGenerated", "claim")
}

// A retry would compare against the tree the drifted attempt left.
func TestValidateRefusesCheckGeneratedWithRetry(t *testing.T) {
	wantCheckGeneratedErr(t, checkGenPlan(func(n *plan.Node) {
		n.Retry = &plan.RetrySpec{MaxAttempts: 3}
	}), `step "codegen"`, "CheckGenerated", "Retry", "drifted tree", "own step")
}

func TestValidateRefusesCheckGeneratedWithOnlyReadOnlyMounts(t *testing.T) {
	wantCheckGeneratedErr(t, checkGenPlan(func(n *plan.Node) { n.Mounts[0].Mode = "ro" }),
		`step "codegen"`, "CheckGenerated", "read-write")
	// A scratch cache is writable but is not a workspace: nothing snapshots
	// it, so it gives the check nothing to compare.
	wantCheckGeneratedErr(t, checkGenPlan(func(n *plan.Node) {
		n.Mounts = []plan.MountSpec{{Scratch: "gomod", At: "/go/pkg/mod"}}
	}), `step "codegen"`, "CheckGenerated", "read-write")
}
