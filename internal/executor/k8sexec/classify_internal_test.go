package k8sexec

import (
	"strings"
	"testing"

	senroexec "github.com/xavidop/senro/internal/executor"
	"github.com/xavidop/senro/internal/kubeapi"
)

// The split this file pins is the whole reason the k8s executor can be
// retried at all. retry.OnInfra() keys off executor.ErrInfra, so a pod that
// could not be scheduled must carry it and a command that exited 1 must not.
// Getting the first wrong makes every retry policy useless; getting the
// second wrong makes `go test` retry until it passes.

func waiting(reason string) kubeapi.Pod {
	return kubeapi.Pod{
		Status: kubeapi.PodStatus{
			Phase: kubeapi.PodPending,
			ContainerStatuses: []kubeapi.ContainerStatus{{
				Name:  StepContainer,
				State: kubeapi.ContainerState{Waiting: &kubeapi.ContainerStateWaiting{Reason: reason}},
			}},
		},
	}
}

func terminated(code int, reason string) kubeapi.Pod {
	phase := kubeapi.PodSucceeded
	if code != 0 {
		phase = kubeapi.PodFailed
	}
	return kubeapi.Pod{
		Status: kubeapi.PodStatus{
			Phase: phase,
			ContainerStatuses: []kubeapi.ContainerStatus{{
				Name: StepContainer,
				State: kubeapi.ContainerState{
					Terminated: &kubeapi.ContainerStateTerminated{ExitCode: code, Reason: reason},
				},
			}},
		},
	}
}

// TestATerminatedContainerIsTheWorkloadsVerdict is the "not infra" half. A
// container that RAN reports its exit code and no error, whatever killed it:
// the process is what the pipeline asked to run, so its exit status is the
// pipeline's answer and not the substrate's.
func TestATerminatedContainerIsTheWorkloadsVerdict(t *testing.T) {
	cases := []struct {
		name   string
		pod    kubeapi.Pod
		want   int
		reason string
	}{
		{"success", terminated(0, "Completed"), 0, "Completed"},
		{"ordinary failure", terminated(1, "Error"), 1, "Error"},
		{"a test suite's own code", terminated(7, "Error"), 7, "Error"},
		// 137 is SIGKILL. The node killing a container for memory is not a
		// senro infrastructure failure in the sense retry cares about:
		// retrying it runs the same command with the same memory appetite.
		{"out of memory", terminated(137, "OOMKilled"), 137, "OOMKilled"},
		// The runtime could not launch the process at all, which Kubernetes
		// reports as a terminated container rather than a waiting one. A
		// command that does not exist in the image is a pipeline defect, and
		// retrying it forever would never fix it.
		{"the command does not exist", terminated(128, "StartError"), 128, "StartError"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			st, err := classify(tc.pod, StepContainer)
			if err != nil {
				t.Fatalf("classify returned an infrastructure error for %s: %v", tc.reason, err)
			}
			if !st.Exited || st.ExitCode != tc.want {
				t.Fatalf("state = %+v, want exit %d", st, tc.want)
			}
			if !st.Started {
				t.Error("a terminated container must count as started, or its log is never opened")
			}
		})
	}
}

// TestASubstrateFailureIsInfrastructure is the other half. None of these is a
// verdict on the workload: the command never ran.
func TestASubstrateFailureIsInfrastructure(t *testing.T) {
	evicted := kubeapi.Pod{Status: kubeapi.PodStatus{
		Phase: kubeapi.PodFailed, Reason: "Evicted",
		Message: "The node was low on resource: ephemeral-storage",
	}}
	unknown := kubeapi.Pod{Status: kubeapi.PodStatus{
		Phase: kubeapi.PodUnknown, Reason: "NodeLost",
	}}
	cases := []struct {
		name string
		pod  kubeapi.Pod
		says string
	}{
		{"the image does not exist", waiting("ImagePullBackOff"), "ImagePullBackOff"},
		{"the image reference is malformed", waiting("InvalidImageName"), "InvalidImageName"},
		{"the image is absent and pulling is off", waiting("ErrImageNeverPull"), "ErrImageNeverPull"},
		{"a mounted secret is missing", waiting("CreateContainerConfigError"), "CreateContainerConfigError"},
		{"the runtime refused the container", waiting("CreateContainerError"), "CreateContainerError"},
		{"the node evicted the pod", evicted, "Evicted"},
		{"the node stopped reporting", unknown, "NodeLost"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := classify(tc.pod, StepContainer)
			if err == nil {
				t.Fatal("classify accepted a substrate failure as an ordinary outcome")
			}
			if !senroexec.IsInfra(err) {
				t.Fatalf("error does not carry executor.ErrInfra, so retry.OnInfra() will not see it: %v", err)
			}
			if !strings.Contains(err.Error(), tc.says) {
				t.Errorf("error %q does not name %q, so nobody can tell what went wrong", err, tc.says)
			}
		})
	}
}

// TestAPodStillSettlingIsNeither keeps the classifier from deciding early.
// Every one of these is a pod on its way to running, and treating any of them
// as a failure would make the executor flaky rather than fast.
func TestAPodStillSettlingIsNeither(t *testing.T) {
	unscheduled := kubeapi.Pod{Status: kubeapi.PodStatus{
		Phase: kubeapi.PodPending,
		Conditions: []kubeapi.PodCondition{{
			Type: "PodScheduled", Status: "False", Reason: "Unschedulable",
			Message: "0/1 nodes are available",
		}},
	}}
	cases := []struct {
		name string
		pod  kubeapi.Pod
	}{
		{"no container status yet", kubeapi.Pod{Status: kubeapi.PodStatus{Phase: kubeapi.PodPending}}},
		{"being created", waiting("ContainerCreating")},
		// ErrImagePull is what the kubelet reports on the FIRST failed pull,
		// before it decides to back off. A registry hiccup produces it and
		// then recovers, so it is deliberately not terminal: the terminal
		// form is ImagePullBackOff, which is above.
		{"a pull that has not backed off yet", waiting("ErrImagePull")},
		// Unschedulable is not terminal either: a cluster autoscaler adding
		// a node is the ordinary resolution, and the bounded start timeout is
		// what stops this waiting forever.
		{"not yet scheduled", unscheduled},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			st, err := classify(tc.pod, StepContainer)
			if err != nil {
				t.Fatalf("classify failed a pod that is merely still settling: %v", err)
			}
			if st.Started || st.Exited {
				t.Fatalf("state = %+v, want neither started nor exited", st)
			}
		})
	}
}

// TestARunningContainerIsStartedButNotExited is what tells Run it may open
// the log stream.
func TestARunningContainerIsStartedButNotExited(t *testing.T) {
	pod := kubeapi.Pod{Status: kubeapi.PodStatus{
		Phase: kubeapi.PodRunning,
		ContainerStatuses: []kubeapi.ContainerStatus{{
			Name:  StepContainer,
			State: kubeapi.ContainerState{Running: &kubeapi.ContainerStateRunning{StartedAt: "now"}},
		}},
	}}
	st, err := classify(pod, StepContainer)
	if err != nil {
		t.Fatalf("classify: %v", err)
	}
	if !st.Started || st.Exited {
		t.Fatalf("state = %+v, want started and not exited", st)
	}
}

// TestClassifyIgnoresOtherContainers guards the lookup itself: a pod whose
// step container is still waiting must not be read through some other
// container's terminated status.
func TestClassifyIgnoresOtherContainers(t *testing.T) {
	pod := waiting("ContainerCreating")
	pod.Status.ContainerStatuses = append(pod.Status.ContainerStatuses, kubeapi.ContainerStatus{
		Name: "somebody-elses-sidecar",
		State: kubeapi.ContainerState{
			Terminated: &kubeapi.ContainerStateTerminated{ExitCode: 0, Reason: "Completed"},
		},
	})
	st, err := classify(pod, StepContainer)
	if err != nil {
		t.Fatalf("classify: %v", err)
	}
	if st.Exited {
		t.Fatal("classify read another container's exit code as the step's")
	}
}
