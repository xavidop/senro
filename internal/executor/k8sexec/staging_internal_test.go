package k8sexec

import (
	"testing"

	senroexec "github.com/xavidop/senro/internal/executor"
	"github.com/xavidop/senro/internal/kubeapi"
	"github.com/xavidop/senro/internal/plan"
)

// podForBinary builds a pod spec for a command senro will exec into, with and
// without a staged binary to deliver. No cluster involved: podSpec is pure,
// and the volume is a property of the object.
func podForBinary(bin bool) kubeapi.Pod {
	s := &sandbox{
		ex:   &Executor{spec: plan.ExecutorSpec{Image: "img@sha256:abc", Namespace: "senro"}, runID: "01RUN"},
		spec: senroexec.SandboxSpec{StepID: "deploy", Attempt: 1},
		pod:  "senro-deploy-1",
	}
	return s.podSpec(senroexec.Cmd{Args: holdCommand()}, "/w", bin)
}

// The binary lands on a volume rather than in the image's own filesystem, so
// the transfer does not depend on the image's root being writable by the uid
// k8s.User named. An emptyDir is created world-writable and goes with the pod.
func TestAStagedBinaryGetsAnEmptyDirOfItsOwn(t *testing.T) {
	pod := podForBinary(true)

	var vol *kubeapi.Volume
	for i, v := range pod.Spec.Volumes {
		if v.Name == binVolume {
			vol = &pod.Spec.Volumes[i]
		}
	}
	if vol == nil {
		t.Fatalf("no %q volume for a step whose command is a staged binary: %+v", binVolume, pod.Spec.Volumes)
	}
	if vol.EmptyDir == nil {
		t.Errorf("the binary volume is not an emptyDir: %+v", vol)
	}

	var mounted bool
	for _, m := range pod.Spec.Containers[0].VolumeMounts {
		if m.Name == binVolume {
			mounted = true
			if m.MountPath != BinDir {
				t.Errorf("the binary volume is mounted at %q, want %q", m.MountPath, BinDir)
			}
			if m.ReadOnly {
				t.Error("the binary volume is read-only, so nothing could be written into it")
			}
		}
	}
	if !mounted {
		t.Errorf("the step's container does not mount the binary volume: %+v",
			pod.Spec.Containers[0].VolumeMounts)
	}
}

// Every other step gets no such volume. senro's own executable has no
// business in the pod spec of a step that runs a command from the image, and
// an unconditional volume would put it in every one.
func TestAStepThatRunsNoStagedBinaryGetsNoBinaryVolume(t *testing.T) {
	pod := podForBinary(false)

	for _, v := range pod.Spec.Volumes {
		if v.Name == binVolume {
			t.Errorf("a step with no staged binary still carries the %q volume", binVolume)
		}
	}
	for _, m := range pod.Spec.Containers[0].VolumeMounts {
		if m.MountPath == BinDir {
			t.Errorf("a step with no staged binary still mounts %q", BinDir)
		}
	}
}
