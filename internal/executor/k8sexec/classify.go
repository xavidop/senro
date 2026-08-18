package k8sexec

import (
	"fmt"
	"strings"

	senroexec "github.com/xavidop/senro/internal/executor"
	"github.com/xavidop/senro/internal/kubeapi"
)

// podState is one observation of a pod, reduced to the two facts Run needs.
type podState struct {
	// Started means the container's log can be opened: it is running now, or
	// it ran and has finished. Both cases are true for a container that
	// exited before the first poll came round.
	Started bool
	// Exited means the process finished and ExitCode is its status.
	Exited   bool
	ExitCode int
}

// terminalWaitReasons are the kubelet's waiting reasons that never resolve on
// their own. Each means the container will not start, so continuing to poll
// only postpones the same failure until the step's timeout.
//
// ErrImagePull is deliberately absent. The kubelet reports it on the first
// failed pull and then backs off; a registry that was briefly unreachable
// produces exactly that and then recovers. ImagePullBackOff is the form that
// says the kubelet has already given up once, and that is what is terminal
// here. See TestAPodStillSettlingIsNeither.
var terminalWaitReasons = map[string]string{
	"ImagePullBackOff":  "the image could not be pulled",
	"InvalidImageName":  "the image reference is not a valid name",
	"ErrImageNeverPull": "the image is not present on the node and imagePullPolicy forbids pulling it",
	"ImageInspectError": "the image could not be inspected on the node",
	"CreateContainerConfigError": "the container's configuration could not be built, which usually " +
		"means a mounted secret or config map does not exist",
	"CreateContainerError": "the container runtime refused to create the container",
	"RegistryUnavailable":  "the image registry could not be reached",
}

// classify decides what one pod observation means, on the line
// executor.Sandbox.Run states: the returned error is infrastructure and
// carries executor.ErrInfra; an exit code is the workload's verdict and
// carries no error at all.
//
// A TERMINATED container is deliberately unconditional: it ran, so its exit
// code is the answer, whatever the reason field says. OOMKilled (137) and
// StartError (128) land here on purpose: retrying either runs the identical
// command and fails identically, and containerexec reports a Docker OOM
// kill as exit 137 too.
//
// A WAITING container is the opposite: nothing ran, so there is no verdict,
// and every reason in terminalWaitReasons is a fact about the cluster
// rather than about the pipeline's command.
func classify(pod kubeapi.Pod, container string) (podState, error) {
	cs, found := containerStatus(pod, container)

	if found {
		switch {
		case cs.State.Terminated != nil:
			return podState{Started: true, Exited: true, ExitCode: cs.State.Terminated.ExitCode}, nil
		case cs.State.Running != nil:
			return podState{Started: true}, nil
		case cs.State.Waiting != nil:
			if why, terminal := terminalWaitReasons[cs.State.Waiting.Reason]; terminal {
				return podState{}, fmt.Errorf(
					"k8sexec: %w: container %q will not start (%s): %s%s",
					senroexec.ErrInfra, container, cs.State.Waiting.Reason, why,
					detail(cs.State.Waiting.Message))
			}
		}
	}

	// No container verdict. The pod itself may still have one, and a pod that
	// has reached a terminal phase without its container ever terminating did
	// not run the step: the node took it away.
	switch pod.Status.Phase {
	case kubeapi.PodFailed:
		return podState{}, fmt.Errorf(
			"k8sexec: %w: the pod failed without container %q reporting an exit status (%s)%s",
			senroexec.ErrInfra, container, orUnknown(pod.Status.Reason), detail(pod.Status.Message))
	case kubeapi.PodUnknown:
		return podState{}, fmt.Errorf(
			"k8sexec: %w: the pod's state is Unknown, which means the node stopped reporting (%s)%s",
			senroexec.ErrInfra, orUnknown(pod.Status.Reason), detail(pod.Status.Message))
	case kubeapi.PodSucceeded:
		if !found {
			return podState{}, fmt.Errorf(
				"k8sexec: %w: the pod succeeded but reported no status for container %q",
				senroexec.ErrInfra, container)
		}
	}

	// Pending, or Running with the container still coming up. Not a decision.
	return podState{}, nil
}

// containerStatus finds one container's status BY NAME, in either list:
// this executor's pod has several containers, so statuses[0] would be a
// live bug (see TestClassifyIgnoresOtherContainers). Init containers report
// in a list of their own, and a name is unique across the two, which is
// what lets the staging container's wait share this classifier.
func containerStatus(pod kubeapi.Pod, name string) (kubeapi.ContainerStatus, bool) {
	for _, cs := range pod.Status.ContainerStatuses {
		if cs.Name == name {
			return cs, true
		}
	}
	for _, cs := range pod.Status.InitContainerStatuses {
		if cs.Name == name {
			return cs, true
		}
	}
	return kubeapi.ContainerStatus{}, false
}

// pendingReason is the best one-line account of why a pod has not started,
// for the message a bounded start wait produces when it gives up. It reads
// the scheduler's own condition first, because "0/12 nodes are available: 12
// Insufficient cpu" is the answer, and "Pending" is not.
func pendingReason(pod kubeapi.Pod) string {
	if cs, ok := containerStatus(pod, StepContainer); ok && cs.State.Waiting != nil {
		w := cs.State.Waiting
		if w.Reason != "" {
			return w.Reason + detail(w.Message)
		}
	}
	for _, c := range pod.Status.Conditions {
		if c.Type == "PodScheduled" && c.Status == "False" {
			return orUnknown(c.Reason) + detail(c.Message)
		}
	}
	if pod.Status.Reason != "" {
		return pod.Status.Reason + detail(pod.Status.Message)
	}
	return "phase " + orUnknown(pod.Status.Phase)
}

func detail(msg string) string {
	msg = strings.TrimSpace(msg)
	if msg == "" {
		return ""
	}
	return ": " + msg
}

func orUnknown(s string) string {
	if strings.TrimSpace(s) == "" {
		return "no reason reported"
	}
	return s
}
