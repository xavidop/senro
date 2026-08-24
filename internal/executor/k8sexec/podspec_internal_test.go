package k8sexec

import (
	"slices"
	"strings"
	"testing"

	senroexec "github.com/xavidop/senro/internal/executor"
	"github.com/xavidop/senro/internal/kubeapi"
	"github.com/xavidop/senro/internal/plan"
)

// podSpecFor builds a pod spec for one command, with no cluster involved:
// podSpec is pure, and every property this file asserts is a property of the
// object rather than of anything the apiserver does with it.
func podSpecFor(env []string) []string {
	s := &sandbox{
		ex:   &Executor{spec: plan.ExecutorSpec{Image: "img@sha256:abc", Namespace: "senro"}, runID: "01RUN"},
		spec: senroexec.SandboxSpec{StepID: "build", Attempt: 1},
		pod:  "senro-build-1",
	}
	pod := s.podSpec(senroexec.Cmd{Args: []string{"true"}, Env: env}, "/w", false)
	out := make([]string, 0, len(pod.Spec.Containers[0].Env))
	for _, e := range pod.Spec.Containers[0].Env {
		out = append(out, e.Name+"="+e.Value)
	}
	return out
}

// The deliberate half of carrying trace context: a pod's environment is a
// plain readable field, which is exactly why SECRETS are never there
// (TestASecretIsAFileAndNeverAFieldOfThePod holds that line). A traceparent
// names no principal and grants no access, and its entire point is to be
// read by the step, so it goes in the pod spec, in the open, on purpose.
func TestTheTraceparentIsAnOrdinaryFieldOfThePod(t *testing.T) {
	const header = "TRACEPARENT=00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"

	env := podSpecFor([]string{"CGO_ENABLED=0", header})

	found := 0
	for _, kv := range env {
		if kv == header {
			found++
		}
	}
	if found != 1 {
		t.Errorf("the pod carries %d copies of %q, want exactly 1; env = %v", found, header, env)
	}
	if len(env) != 2 {
		t.Errorf("the pod's env = %v, want exactly what the command was given", env)
	}
}

// TestThePodCarriesNoTraceContextTheCommandWasNotGiven keeps this executor
// from inventing one of its own. The span belongs to the attempt, and the
// engine is the only thing that knows which attempt this is: an executor that
// added a traceparent here would be minting a span nothing in the ledger ever
// mentions.
func TestThePodCarriesNoTraceContextTheCommandWasNotGiven(t *testing.T) {
	env := podSpecFor([]string{"CGO_ENABLED=0"})

	for _, kv := range env {
		if kv != "CGO_ENABLED=0" {
			t.Errorf("the pod carries %q, which the command was never given", kv)
		}
	}
}

// podFor builds a pod spec for a step with the given mounts, no cluster
// involved.
func podFor(mounts []senroexec.Mount) kubeapi.Pod {
	s := &sandbox{
		ex:     &Executor{spec: plan.ExecutorSpec{Image: "img@sha256:abc", Namespace: "senro"}, runID: "01RUN"},
		spec:   senroexec.SandboxSpec{StepID: "build", Attempt: 1},
		pod:    "senro-build-1",
		mounts: mounts,
	}
	return s.podSpec(senroexec.Cmd{Args: []string{"true"}}, "/w", false)
}

func containerNames(pod kubeapi.Pod) []string {
	out := []string{}
	for _, c := range pod.Spec.InitContainers {
		out = append(out, "init:"+c.Name)
	}
	for _, c := range pod.Spec.Containers {
		out = append(out, c.Name)
	}
	return out
}

// A claim-backed workspace is mounted from the claim, and nothing carries
// it. The absence of the staging and reader containers IS the feature: a
// pod that mounted the claim and then tarred the tree in on top of itself
// would spend exactly the traffic the claim avoids, while racing another
// run's pod for the same files.
func TestAClaimBackedWorkspaceIsMountedAndNeverCarried(t *testing.T) {
	pod := podFor([]senroexec.Mount{
		{Name: "cache", At: "/w", Claim: "senro-build-cache"},
	})

	if n := len(pod.Spec.Volumes); n != 1 {
		t.Fatalf("volumes = %d, want 1", n)
	}
	v := pod.Spec.Volumes[0]
	if v.ClaimRef == nil {
		t.Fatal("the workspace volume is not a PersistentVolumeClaim")
	}
	if v.ClaimRef.ClaimName != "senro-build-cache" {
		t.Errorf("claimName = %q, want %q", v.ClaimRef.ClaimName, "senro-build-cache")
	}
	if v.EmptyDir != nil {
		t.Error("a claim-backed workspace also got an emptyDir")
	}

	// One container, the step's own. No stage, no reader.
	if got, want := containerNames(pod), []string{StepContainer}; !slices.Equal(got, want) {
		t.Errorf("containers = %v, want %v: a claim-backed workspace must not be staged or read back", got, want)
	}

	// And it is actually mounted where the step asked for it.
	var mounted bool
	for _, m := range pod.Spec.Containers[0].VolumeMounts {
		if m.Name == v.Name && m.MountPath == "/w" {
			mounted = true
		}
	}
	if !mounted {
		t.Errorf("the claim is not mounted at /w: %+v", pod.Spec.Containers[0].VolumeMounts)
	}
}

// A workspace with no claim keeps the pod it has always had. This is the
// regression that matters: the claim path must not change the default one.
func TestAnOrdinaryWorkspaceStillGetsAnEmptyDirAndBothCarriers(t *testing.T) {
	pod := podFor([]senroexec.Mount{{Name: "src", At: "/w"}})

	if n := len(pod.Spec.Volumes); n != 1 {
		t.Fatalf("volumes = %d, want 1", n)
	}
	if pod.Spec.Volumes[0].EmptyDir == nil {
		t.Error("an ordinary workspace is not an emptyDir")
	}
	if pod.Spec.Volumes[0].ClaimRef != nil {
		t.Error("an ordinary workspace got a claim")
	}
	want := []string{"init:" + StageContainer, StepContainer, IOContainer}
	if got := containerNames(pod); !slices.Equal(got, want) {
		t.Errorf("containers = %v, want %v", got, want)
	}
}

// A step that mounts one of each carries only the one that needs carrying.
// The positional volume names must still line up with the stage paths, which
// is the part a `continue` in the middle of an indexed loop is easiest to get
// wrong.
func TestAMixedStepCarriesOnlyTheUnclaimedWorkspace(t *testing.T) {
	pod := podFor([]senroexec.Mount{
		{Name: "cache", At: "/cache", Claim: "senro-cache"},
		{Name: "src", At: "/w"},
	})

	if n := len(pod.Spec.Volumes); n != 2 {
		t.Fatalf("volumes = %d, want 2", n)
	}
	if pod.Spec.Volumes[0].ClaimRef == nil || pod.Spec.Volumes[0].ClaimRef.ClaimName != "senro-cache" {
		t.Errorf("volume 0 is not the claim: %+v", pod.Spec.Volumes[0])
	}
	if pod.Spec.Volumes[1].EmptyDir == nil {
		t.Errorf("volume 1 is not an emptyDir: %+v", pod.Spec.Volumes[1])
	}

	// Both carriers exist, because one workspace still needs carrying.
	want := []string{"init:" + StageContainer, StepContainer, IOContainer}
	if got := containerNames(pod); !slices.Equal(got, want) {
		t.Fatalf("containers = %v, want %v", got, want)
	}

	// And they touch ONLY the unclaimed workspace, at the stage path that
	// matches its index. Getting this wrong would tar the source tree into
	// the cache's volume.
	stage := pod.Spec.InitContainers[0].VolumeMounts
	if len(stage) != 1 {
		t.Fatalf("the staging container mounts %d volumes, want 1", len(stage))
	}
	if stage[0].Name != pod.Spec.Volumes[1].Name {
		t.Errorf("the staging container mounts %q, want the unclaimed workspace %q",
			stage[0].Name, pod.Spec.Volumes[1].Name)
	}
	if stage[0].MountPath != stagePath(1) {
		t.Errorf("stage path = %q, want %q: the volume name and the stage path have drifted apart",
			stage[0].MountPath, stagePath(1))
	}
}

// Every mount claimed means nothing to carry, so the pod is the one a step
// with no workspace at all gets.
func TestAPodWhoseEveryWorkspaceIsClaimedHasNoCarriers(t *testing.T) {
	pod := podFor([]senroexec.Mount{
		{Name: "cache", At: "/cache", Claim: "senro-cache"},
		{Name: "deps", At: "/deps", Claim: "senro-deps"},
	})
	if got, want := containerNames(pod), []string{StepContainer}; !slices.Equal(got, want) {
		t.Errorf("containers = %v, want %v", got, want)
	}
	if n := len(pod.Spec.InitContainers); n != 0 {
		t.Errorf("init containers = %d, want 0", n)
	}
}

// A read-only claim mount says so on the claim as well as on the mount. The
// volume-level flag is what stops a pod writing through it at all, and the
// mount-level one is what the container sees.
func TestAReadOnlyClaimIsReadOnlyOnBoth(t *testing.T) {
	pod := podFor([]senroexec.Mount{{Name: "cache", At: "/w", Claim: "senro-cache", RO: true}})
	if !pod.Spec.Volumes[0].ClaimRef.ReadOnly {
		t.Error("the claim volume is not read-only")
	}
	if !pod.Spec.Containers[0].VolumeMounts[0].ReadOnly {
		t.Error("the container's mount of the claim is not read-only")
	}
}

// podForSpec builds a pod spec for a given executor spec, so the delegation
// tests can vary the target rather than the mounts.
func podForSpec(spec plan.ExecutorSpec, secretObj string) kubeapi.Pod {
	s := &sandbox{
		ex:        &Executor{spec: spec, runID: "01RUN"},
		spec:      senroexec.SandboxSpec{StepID: "build", Attempt: 1},
		pod:       "senro-build-1",
		secretObj: secretObj,
	}
	return s.podSpec(senroexec.Cmd{Args: []string{"true"}}, "/w", false)
}

// The default: no ServiceAccount, no token, and a step's secrets projected
// from a Secret senro created. A step's command has no business holding a
// credential to the cluster it happens to be running in.
func TestByDefaultThePodGetsNoClusterCredential(t *testing.T) {
	pod := podForSpec(plan.ExecutorSpec{
		Image: "img@sha256:abc", Namespace: "senro",
	}, "senro-secret-1")

	if pod.Spec.AutomountServiceAccountToken == nil || *pod.Spec.AutomountServiceAccountToken {
		t.Error("a token is mounted by default")
	}
	if pod.Spec.ServiceAccountName != "" {
		t.Errorf("ServiceAccountName = %q, want empty", pod.Spec.ServiceAccountName)
	}
	var found bool
	for _, v := range pod.Spec.Volumes {
		if v.Secret != nil {
			found = true
		}
	}
	if !found {
		t.Error("the pushed Secret is not projected")
	}
}

// Delegating turns the token ON, which is the cost DelegateSecrets' doc
// states: that token is what IRSA exchanges for an AWS identity, so
// delegation cannot work without it.
func TestDelegatingMountsTheTokenAndProjectsNoSecret(t *testing.T) {
	pod := podForSpec(plan.ExecutorSpec{
		Image: "img@sha256:abc", Namespace: "senro",
		ServiceAccount: "senro-ci", DelegateSecrets: true,
	}, "senro-secret-1")

	if pod.Spec.AutomountServiceAccountToken == nil || !*pod.Spec.AutomountServiceAccountToken {
		t.Error("delegation did not mount the ServiceAccount token, so IRSA cannot work")
	}
	if pod.Spec.ServiceAccountName != "senro-ci" {
		t.Errorf("ServiceAccountName = %q, want senro-ci", pod.Spec.ServiceAccountName)
	}
	// And no pushed Secret, because senro resolved nothing to push.
	for _, v := range pod.Spec.Volumes {
		if v.Secret != nil {
			t.Error("a delegating pod still projects a pushed Secret")
		}
	}
}

// Delegation without a ServiceAccount is refused at construction, because the
// namespace's default account is one every other pod there also has.
func TestDelegationWithoutAServiceAccountIsRefused(t *testing.T) {
	err := CheckSpec(plan.ExecutorSpec{
		Image: "img@sha256:abc", Namespace: "senro", DelegateSecrets: true,
	})
	if err == nil {
		t.Fatal("delegation with no ServiceAccount was accepted")
	}
	for _, want := range []string{"ServiceAccount", "default"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("refusal does not mention %q: %v", want, err)
		}
	}
}

// A ServiceAccount on its own does NOT mount a token. Naming an account is
// how a pod is identified for other purposes too, and turning the credential
// on as a side effect of that would hand one to every step whose target
// happened to name an account.
func TestAServiceAccountAloneDoesNotMountAToken(t *testing.T) {
	pod := podForSpec(plan.ExecutorSpec{
		Image: "img@sha256:abc", Namespace: "senro", ServiceAccount: "senro-ci",
	}, "")
	if pod.Spec.ServiceAccountName != "senro-ci" {
		t.Errorf("ServiceAccountName = %q, want senro-ci", pod.Spec.ServiceAccountName)
	}
	if pod.Spec.AutomountServiceAccountToken == nil || *pod.Spec.AutomountServiceAccountToken {
		t.Error("naming a ServiceAccount mounted a token on its own")
	}
}

// Resources land on the step's own container and nowhere else: senro's
// stage and reader containers are its own plumbing, not part of what the
// pipeline asked to run, and a limit sized for the step's workload would
// starve them.
func TestResourcesLandOnTheStepContainerOnly(t *testing.T) {
	pod := podForSpec(plan.ExecutorSpec{
		Image: "img@sha256:abc", Namespace: "senro",
		Resources: &plan.ResourceSpec{
			Requests: map[string]string{"cpu": "500m"},
			Limits:   map[string]string{"cpu": "1", "memory": "256Mi"},
		},
	}, "")
	got := pod.Spec.Containers[0].Resources
	if got == nil || got.Requests["cpu"] != "500m" || got.Limits["cpu"] != "1" || got.Limits["memory"] != "256Mi" {
		t.Errorf("step container Resources = %+v", got)
	}
}

// With no Resources declared, the container carries no requests or limits
// at all, so an unmodified pipeline's pod digests and schedules exactly as
// it always has.
func TestByDefaultTheContainerHasNoResources(t *testing.T) {
	pod := podForSpec(plan.ExecutorSpec{Image: "img@sha256:abc", Namespace: "senro"}, "")
	if pod.Spec.Containers[0].Resources != nil {
		t.Errorf("Resources = %+v, want nil", pod.Spec.Containers[0].Resources)
	}
}

// NodeSelector, Tolerations and ImagePullSecrets are pod-wide, so they land
// on the PodSpec rather than on any one container.
func TestNodeSelectorTolerationsAndImagePullSecretsLandOnThePod(t *testing.T) {
	pod := podForSpec(plan.ExecutorSpec{
		Image: "img@sha256:abc", Namespace: "senro",
		NodeSelector: map[string]string{"disktype": "ssd"},
		Tolerations: []plan.TolerationSpec{
			{Key: "dedicated", Operator: "Equal", Value: "ci", Effect: "NoSchedule"},
		},
		ImagePullSecrets: []string{"regcred"},
	}, "")

	if pod.Spec.NodeSelector["disktype"] != "ssd" {
		t.Errorf("NodeSelector = %+v", pod.Spec.NodeSelector)
	}
	want := []kubeapi.Toleration{{Key: "dedicated", Operator: "Equal", Value: "ci", Effect: "NoSchedule"}}
	if !slices.Equal(pod.Spec.Tolerations, want) {
		t.Errorf("Tolerations = %+v, want %+v", pod.Spec.Tolerations, want)
	}
	wantSecrets := []kubeapi.LocalObjectRefName{{Name: "regcred"}}
	if !slices.Equal(pod.Spec.ImagePullSecrets, wantSecrets) {
		t.Errorf("ImagePullSecrets = %+v, want %+v", pod.Spec.ImagePullSecrets, wantSecrets)
	}
}
