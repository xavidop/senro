package k8s_test

import (
	"strings"
	"testing"

	"github.com/xavidop/senro"
	"github.com/xavidop/senro/exec"
	"github.com/xavidop/senro/executor/k8s"
)

const pinned = "ghcr.io/acme/runner@sha256:" +
	"9f2c1e8b7a4d3c6f0e5b2a9d8c7f6e5d4c3b2a1908f7e6d5c4b3a29180716253"

func TestPodBuildsASpecSenroCanRecord(t *testing.T) {
	spec := k8s.Pod(pinned, k8s.Namespace("ci")).ExecutorSpec()
	if spec.Kind != "k8s" || spec.Image != pinned || spec.Namespace != "ci" {
		t.Fatalf("spec = %+v", spec)
	}
}

// TestTheNamespaceChangesTheExecutorKey: two namespaces are two places to
// create pods, so they must be two executor instances rather than one shared
// between them.
func TestTheNamespaceChangesTheExecutorKey(t *testing.T) {
	ci := k8s.Pod(pinned, k8s.Namespace("ci")).ExecutorSpec().Key()
	staging := k8s.Pod(pinned, k8s.Namespace("ci-staging")).ExecutorSpec().Key()
	if ci == staging {
		t.Fatalf("two namespaces share the executor key %q", ci)
	}
}

func TestADeclaredUserAndPlatformChangeTheExecutorKey(t *testing.T) {
	base := k8s.Pod(pinned, k8s.Namespace("ci")).ExecutorSpec().Key()
	asRoot := k8s.Pod(pinned, k8s.Namespace("ci"), k8s.User("0:0")).ExecutorSpec().Key()
	arm := k8s.Pod(pinned, k8s.Namespace("ci"), k8s.Platform("linux", "arm64")).ExecutorSpec().Key()
	for name, got := range map[string]string{"user": asRoot, "platform": arm} {
		if got == base {
			t.Errorf("a declared %s did not change the executor key", name)
		}
	}
}

// TestATargetSatisfiesSenroExecutorTarget is the compile-time assertion that
// keeps senro.On(k8s.Pod(...)) working: the interface lives in the root
// package and this package must never import it, so nothing else would catch
// a signature drift.
func TestATargetSatisfiesSenroExecutorTarget(t *testing.T) {
	var _ senro.ExecutorTarget = k8s.Pod(pinned, k8s.Namespace("ci"))
}

// TestBuildAcceptsAWorkflowOnKubernetes proves the whole declaration path,
// from senro.On through Build, with no cluster anywhere: this package
// contains no Kubernetes code at all, which is what lets `go test` digest a
// Kubernetes pipeline on a machine that has never installed kubectl.
func TestBuildAcceptsAWorkflowOnKubernetes(t *testing.T) {
	p := senro.New("ci")
	w := p.Workflow("deploy", senro.On(k8s.Pod(pinned, k8s.Namespace("ci"))))
	w.Step("apply", exec.Command("helm", "upgrade", "--install", "web", "./chart"))

	pl, err := p.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	n, ok := pl.Node("apply")
	if !ok {
		t.Fatal("no node")
	}
	if n.Executor == nil || n.Executor.Kind != "k8s" || n.Executor.Namespace != "ci" {
		t.Fatalf("the target did not reach the plan: %+v", n.Executor)
	}
}

// TestBuildAcceptsAK8sWorkspaceWithNoSnapshot is the shape the documentation
// tells a reader to write for a k8s step whose workspace is large enough that
// carrying it across the apiserver is not worth it. It is here so a change to
// the builder API breaks this test rather than silently making the docs wrong.
func TestBuildAcceptsAK8sWorkspaceWithNoSnapshot(t *testing.T) {
	p := senro.New("ci")
	work := senro.Workspace("work", senro.Scope(senro.ScopeRun))
	deploy := p.Workflow("deploy", senro.On(k8s.Pod(pinned, k8s.Namespace("ci"))))
	deploy.Step("render", exec.Command("sh", "-c", "helm template ./chart > /work/out.yaml")).
		Mount(work.At("/work", senro.RW)).
		NoSnapshot()

	if _, err := p.Build(); err != nil {
		t.Fatalf("Build refused the workspace shape the docs recommend: %v", err)
	}
}

// A workspace is carried into the pod before the step's container starts
// and read back out after it exits, so mounting one and snapshotting it is
// an ordinary step; requiring NoSnapshot() would be an opt-out of something
// that works.
func TestBuildAcceptsAK8sWorkspaceThatIsSnapshotted(t *testing.T) {
	p := senro.New("ci")
	work := senro.Workspace("work", senro.Scope(senro.ScopeRun))
	deploy := p.Workflow("deploy", senro.On(k8s.Pod(pinned, k8s.Namespace("ci"))))
	deploy.Step("render", exec.Command("true")).Mount(work.At("/work", senro.RW))

	if _, err := p.Build(); err != nil {
		t.Fatalf("Build refused a k8s step that mounts a workspace it can now carry: %v", err)
	}
}

// A pod fills a scratch cache from the coordinator's copy and hands it back
// through the same tar a workspace crosses on, so the run saves what the pod
// left rather than what it was sent. Checked where a pipeline author meets
// it, not only in plan.Validate.
func TestBuildAcceptsAK8sScratchCache(t *testing.T) {
	p := senro.New("ci")
	mods := senro.ScratchCache("gomod", senro.Key("gomod-v1"))
	deploy := p.Workflow("deploy", senro.On(k8s.Pod(pinned, k8s.Namespace("ci"))))
	deploy.Step("build", exec.Command("go", "build", "./...")).
		Mount(mods.At("/go/pkg/mod"))

	if _, err := p.Build(); err != nil {
		t.Fatalf("Build refused a k8s step mounting a scratch cache it can now carry: %v", err)
	}
}

// The refusal that survives: a step on the coordinator's own filesystem
// writes the cache directory LIVE, and a pod tarring that same directory
// would send a half-written tree and then save it under a key nothing can
// ever rewrite.
func TestBuildRefusesAScratchCacheSharedWithALocalStep(t *testing.T) {
	p := senro.New("ci")
	mods := senro.ScratchCache("gomod", senro.Key("gomod-v1"))
	deploy := p.Workflow("deploy", senro.On(k8s.Pod(pinned, k8s.Namespace("ci"))))
	deploy.Step("build", exec.Command("go", "build", "./...")).
		Mount(mods.At("/go/pkg/mod"))
	local := p.Workflow("lint")
	local.Step("vet", exec.Command("go", "vet", "./...")).
		Mount(mods.At("/go/pkg/mod"))

	_, err := p.Build()
	if err == nil {
		t.Fatal("Build accepted one scratch cache mounted by a pod and by a coordinator step, " +
			"so a half-written tree could be saved under an immutable key")
	}
	if !strings.Contains(err.Error(), "gomod") {
		t.Fatalf("the refusal does not name the cache: %v", err)
	}
}

// TestBuildRefusesAnUnpinnedImage is the rule that keeps a moving tag out of
// a cache key, checked where a pipeline author meets it.
func TestBuildRefusesAnUnpinnedImage(t *testing.T) {
	p := senro.New("ci")
	w := p.Workflow("deploy", senro.On(k8s.Pod("ghcr.io/acme/runner:v1", k8s.Namespace("ci"))))
	w.Step("apply", exec.Command("true"))
	_, err := p.Build()
	if err == nil {
		t.Fatal("Build accepted an unpinned image on the k8s executor")
	}
	if !strings.Contains(err.Error(), "digest") {
		t.Fatalf("the refusal does not say the image must be pinned: %v", err)
	}
}

func TestBuildRefusesAMissingNamespace(t *testing.T) {
	p := senro.New("ci")
	w := p.Workflow("deploy", senro.On(k8s.Pod(pinned)))
	w.Step("apply", exec.Command("true"))
	_, err := p.Build()
	if err == nil {
		t.Fatal("Build accepted a k8s workflow with no namespace")
	}
	if !strings.Contains(err.Error(), "namespace") {
		t.Fatalf("the refusal does not name the namespace: %v", err)
	}
}
