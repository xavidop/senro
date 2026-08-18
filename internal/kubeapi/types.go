package kubeapi

// The subset of the Kubernetes core/v1 API this executor uses.
//
// Hand-written rather than generated, and deliberately partial: a field that
// is not here is a field the executor does not set and does not read, so the
// type is also the documentation of the surface. Every field carries
// omitempty so a create request contains exactly what was asked for, which
// matters because the apiserver defaults everything else and a zero value
// sent explicitly is not the same as a field omitted (an explicit
// "restartPolicy": "" is rejected; an absent one is defaulted).

// ObjectMeta is the metadata every object carries.
type ObjectMeta struct {
	Name      string            `json:"name,omitempty"`
	Namespace string            `json:"namespace,omitempty"`
	UID       string            `json:"uid,omitempty"`
	Labels    map[string]string `json:"labels,omitempty"`
	// ResourceVersion is the apiserver's optimistic-concurrency token. Sent
	// back on an update, it makes that update conditional on nothing else
	// having changed the object since it was read, which is what turns "take
	// over an expired lease" into an operation exactly one of two racing
	// coordinators can win. See lease.go.
	ResourceVersion string `json:"resourceVersion,omitempty"`
	// Annotations hold the values a label cannot. A label value must match a
	// narrow character class and fit in 63 bytes, and a senro step id
	// routinely fails both ("verify/lint[unit=apps/web]"), so the id is
	// recorded here verbatim while the label carries a sanitized form to
	// select on.
	Annotations     map[string]string `json:"annotations,omitempty"`
	OwnerReferences []OwnerReference  `json:"ownerReferences,omitempty"`
}

// OwnerReference makes the apiserver's garbage collector delete this object
// when the owner goes away. It is how a secret cannot outlive the pod it was
// created for even if the coordinator is killed before it can clean up.
type OwnerReference struct {
	APIVersion string `json:"apiVersion"`
	Kind       string `json:"kind"`
	Name       string `json:"name"`
	UID        string `json:"uid"`
	// Controller and BlockOwnerDeletion are omitted on purpose: this is a
	// lifetime link, not a controller relationship, and blocking the pod's
	// deletion on the secret's would be exactly backwards.
}

// Pod is one step's execution, as an API object.
type Pod struct {
	APIVersion string     `json:"apiVersion,omitempty"`
	Kind       string     `json:"kind,omitempty"`
	Metadata   ObjectMeta `json:"metadata,omitempty"`
	Spec       PodSpec    `json:"spec,omitempty"`
	Status     PodStatus  `json:"status,omitempty"`
}

// PodSpec is what to run.
type PodSpec struct {
	Containers []Container `json:"containers,omitempty"`
	// InitContainers run to completion, in order, BEFORE any ordinary
	// container starts. That ordering is the only barrier Kubernetes offers a
	// client, and it is what lets a workspace's bytes land in a volume before
	// the step's own process can look at it. See k8sexec's doc.
	InitContainers []Container `json:"initContainers,omitempty"`
	Volumes        []Volume    `json:"volumes,omitempty"`
	RestartPolicy  string      `json:"restartPolicy,omitempty"`
	NodeName       string      `json:"nodeName,omitempty"`
	// AutomountServiceAccountToken is set false by the executor. A step's
	// command has no business holding a credential to the cluster it happens
	// to be running in, and the default is to mount one.
	AutomountServiceAccountToken *bool                `json:"automountServiceAccountToken,omitempty"`
	SecurityContext              *PodSecurityContext  `json:"securityContext,omitempty"`
	NodeSelector                 map[string]string    `json:"nodeSelector,omitempty"`
	ServiceAccountName           string               `json:"serviceAccountName,omitempty"`
	TerminationGracePeriod       *int64               `json:"terminationGracePeriodSeconds,omitempty"`
	ImagePullSecrets             []LocalObjectRefName `json:"imagePullSecrets,omitempty"`
}

// LocalObjectRefName names another object in the same namespace.
type LocalObjectRefName struct {
	Name string `json:"name"`
}

// PodSecurityContext carries the uid and gid a step runs as.
type PodSecurityContext struct {
	RunAsUser  *int64 `json:"runAsUser,omitempty"`
	RunAsGroup *int64 `json:"runAsGroup,omitempty"`
}

// Container is the step's process.
type Container struct {
	Name            string        `json:"name"`
	Image           string        `json:"image"`
	Command         []string      `json:"command,omitempty"`
	Env             []EnvVar      `json:"env,omitempty"`
	WorkingDir      string        `json:"workingDir,omitempty"`
	VolumeMounts    []VolumeMount `json:"volumeMounts,omitempty"`
	ImagePullPolicy string        `json:"imagePullPolicy,omitempty"`
}

// EnvVar is one name/value pair. Only the literal form is here: there is no
// ValueFrom, and specifically no SecretKeyRef, because this executor never
// puts a secret in a pod's environment. See k8sexec's doc.
type EnvVar struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

// VolumeMount attaches a Volume inside a container.
type VolumeMount struct {
	Name      string `json:"name"`
	MountPath string `json:"mountPath"`
	ReadOnly  bool   `json:"readOnly,omitempty"`
}

// Volume is a pod-scoped volume. Exactly three sources are supported: an
// emptyDir for a workspace senro carries in and out, a secret projection for
// credentials, and a PersistentVolumeClaim for a workspace that stays in the
// cluster between runs.
type Volume struct {
	Name     string                    `json:"name"`
	EmptyDir *EmptyDirVolume           `json:"emptyDir,omitempty"`
	Secret   *SecretVolumeSource       `json:"secret,omitempty"`
	ClaimRef *PersistentVolumeClaimRef `json:"persistentVolumeClaim,omitempty"`
}

// EmptyDirVolume is a directory created when the pod is assigned to a node
// and deleted with the pod. Medium "" is the node's disk; "Memory" is a
// tmpfs that counts against the container's memory limit.
type EmptyDirVolume struct {
	Medium string `json:"medium,omitempty"`
}

// PersistentVolumeClaimRef mounts a claim that already exists.
//
// A reference and nothing more: this package has no call that creates,
// resizes or deletes a PersistentVolumeClaim, and that absence is the design.
// A claim has a storage class, a size and an access mode, all of which are
// cluster-admin decisions with money attached, and all of which senro would
// have to guess. The operator points senro at a claim they made; senro mounts
// it. See executor/k8s.Claim.
type PersistentVolumeClaimRef struct {
	ClaimName string `json:"claimName"`
	ReadOnly  bool   `json:"readOnly,omitempty"`
}

// SecretVolumeSource projects a Secret as files.
type SecretVolumeSource struct {
	SecretName string `json:"secretName"`
	// DefaultMode is octal in Go source and decimal on the wire; the
	// apiserver wants the number. 0400 is 256.
	DefaultMode *int32 `json:"defaultMode,omitempty"`
}

// PodStatus is what the kubelet and scheduler report back.
type PodStatus struct {
	Phase             string            `json:"phase,omitempty"`
	Reason            string            `json:"reason,omitempty"`
	Message           string            `json:"message,omitempty"`
	Conditions        []PodCondition    `json:"conditions,omitempty"`
	ContainerStatuses []ContainerStatus `json:"containerStatuses,omitempty"`
	// InitContainerStatuses is a list of its own rather than part of the one
	// above, which is why anything waiting for an init container to become
	// exec-able has to look here for it.
	InitContainerStatuses []ContainerStatus `json:"initContainerStatuses,omitempty"`
}

// Pod phases, as the apiserver spells them.
const (
	PodPending   = "Pending"
	PodRunning   = "Running"
	PodSucceeded = "Succeeded"
	PodFailed    = "Failed"
	PodUnknown   = "Unknown"
)

// PodCondition is one scheduling or readiness fact.
type PodCondition struct {
	Type    string `json:"type,omitempty"`
	Status  string `json:"status,omitempty"`
	Reason  string `json:"reason,omitempty"`
	Message string `json:"message,omitempty"`
}

// ContainerStatus is one container's lifecycle, which is where both the exit
// code and every reason a container never ran are reported.
type ContainerStatus struct {
	Name         string         `json:"name,omitempty"`
	State        ContainerState `json:"state,omitempty"`
	Ready        bool           `json:"ready,omitempty"`
	RestartCount int            `json:"restartCount,omitempty"`
	Image        string         `json:"image,omitempty"`
	ImageID      string         `json:"imageID,omitempty"`
}

// ContainerState is a union: exactly one of the three is set.
type ContainerState struct {
	Waiting    *ContainerStateWaiting    `json:"waiting,omitempty"`
	Running    *ContainerStateRunning    `json:"running,omitempty"`
	Terminated *ContainerStateTerminated `json:"terminated,omitempty"`
}

// ContainerStateWaiting is a container that has not started. Reason is the
// single most important string in this whole package: it is what separates
// "the image does not exist" from "the command exited 1".
type ContainerStateWaiting struct {
	Reason  string `json:"reason,omitempty"`
	Message string `json:"message,omitempty"`
}

// ContainerStateRunning is a container whose process exists.
type ContainerStateRunning struct {
	StartedAt string `json:"startedAt,omitempty"`
}

// ContainerStateTerminated is a container whose process has exited, and
// ExitCode is that process's verdict.
type ContainerStateTerminated struct {
	ExitCode   int    `json:"exitCode"`
	Signal     int    `json:"signal,omitempty"`
	Reason     string `json:"reason,omitempty"`
	Message    string `json:"message,omitempty"`
	StartedAt  string `json:"startedAt,omitempty"`
	FinishedAt string `json:"finishedAt,omitempty"`
}

// Secret is a namespaced blob of credential material.
type Secret struct {
	APIVersion string            `json:"apiVersion,omitempty"`
	Kind       string            `json:"kind,omitempty"`
	Metadata   ObjectMeta        `json:"metadata,omitempty"`
	Type       string            `json:"type,omitempty"`
	Data       map[string][]byte `json:"data,omitempty"`
	Immutable  *bool             `json:"immutable,omitempty"`
}

// Node is a cluster member, read for its architecture.
type Node struct {
	Metadata ObjectMeta `json:"metadata,omitempty"`
	Spec     NodeSpec   `json:"spec,omitempty"`
	Status   NodeStatus `json:"status,omitempty"`
}

// NodeSpec carries the taints that say whether a node takes ordinary work.
type NodeSpec struct {
	Unschedulable bool        `json:"unschedulable,omitempty"`
	Taints        []NodeTaint `json:"taints,omitempty"`
}

// NodeTaint repels pods that do not tolerate it.
type NodeTaint struct {
	Key    string `json:"key,omitempty"`
	Value  string `json:"value,omitempty"`
	Effect string `json:"effect,omitempty"`
}

// NodeStatus carries the node's own report of what it is.
type NodeStatus struct {
	NodeInfo NodeSystemInfo `json:"nodeInfo,omitempty"`
}

// NodeSystemInfo is the node's OS and architecture, in exactly the spelling
// Go uses ("linux", "arm64"), which is why no translation table is needed
// between this and executor.Platform.
type NodeSystemInfo struct {
	OperatingSystem string `json:"operatingSystem,omitempty"`
	Architecture    string `json:"architecture,omitempty"`
	KubeletVersion  string `json:"kubeletVersion,omitempty"`
}

type nodeList struct {
	Items []Node `json:"items"`
}

// versionInfo is what GET /version answers.
type versionInfo struct {
	GitVersion string `json:"gitVersion"`
	Major      string `json:"major"`
	Minor      string `json:"minor"`
	Platform   string `json:"platform"`
}
