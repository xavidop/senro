// Package k8s targets a workflow at a pod in a Kubernetes cluster.
//
//	runner := k8s.Pod("ghcr.io/acme/runner@sha256:9f2c...", k8s.Namespace("ci"))
//	deploy := p.Workflow("deploy", senro.On(runner))
//
// The image must be pinned to a digest and the namespace must be stated:
// the executor talks to an apiserver rather than a registry, so it cannot
// resolve a tag into the digest a cache key needs, and defaulting the
// namespace would create a pipeline's pods in a real namespace because
// nobody said otherwise. Both are refused when the pipeline is built.
//
// The cluster comes from SENRO_K8S_* environment variables read by the
// coordinator at run time (see internal/kubeapi), never from here and never
// from $KUBECONFIG or ~/.kube/config: a plan must stay portable and carry
// no credential, and a development kubeconfig commonly holds dozens of
// contexts, most of them production. With nothing configured, the run fails
// at start naming the variables it needs.
//
// This package deliberately contains no Kubernetes code: it is the
// declaration, and internal/executor/k8sexec is the execution. Building a
// pipeline costs nothing, needs no cluster, and works on a machine that has
// never installed kubectl.
package k8s

import "github.com/xavidop/senro/internal/plan"

// Target is what senro.On accepts. It satisfies senro.ExecutorTarget.
type Target struct{ spec plan.ExecutorSpec }

// ExecutorSpec reports where the workflow's steps run.
func (t Target) ExecutorSpec() plan.ExecutorSpec { return t.spec }

// Option configures a k8s target.
type Option func(*plan.ExecutorSpec)

// Namespace is where the step's pod is created. Required.
//
// It is part of the plan because it is a property of the pipeline rather than
// of the machine running it, and it is part of the executor's instance key,
// so two namespaces are two executors. It is deliberately NOT part of the
// cache equivalence class: the same image running the same command produces
// the same bytes in "ci" as in "ci-staging".
func Namespace(ns string) Option {
	return func(s *plan.ExecutorSpec) { s.Namespace = ns }
}

// User runs the step's container as a numeric uid, or "uid:gid".
//
// Numeric only: a Kubernetes securityContext takes a uid, and senro cannot
// resolve a name against the image's /etc/passwd because this executor
// never talks to a registry, so k8s.User("node") is refused where
// container.User("node") works.
//
// Empty means the image's own USER, the ordinary case here: there are no
// bind mounts of coordinator directories for a root process to leave
// unremovable files in.
//
// A declared user is part of the step's cache equivalence class, since a
// step that runs as root is not the same step.
func User(u string) Option {
	return func(s *plan.ExecutorSpec) { s.User = u }
}

// Platform declares the execution platform, as Go spells it: ("linux",
// "arm64").
//
// Usually unnecessary. Left undeclared, the executor reads the platform
// from the cluster's own schedulable nodes, more accurate for a
// multi-architecture image than the manifest. Declare it when the nodes do
// not all agree: the one case with no single answer to read, refused until
// the pipeline names one.
func Platform(os, arch string) Option {
	return func(s *plan.ExecutorSpec) { s.OS, s.Arch = os, arch }
}

// Claim backs one workspace with a PersistentVolumeClaim that already exists
// in the target namespace, instead of carrying that workspace in and out of
// every pod.
//
//	k8s.Pod(img, k8s.Namespace("ci"), k8s.Claim("build-cache", "senro-build-cache"))
//
// Without it, a workspace crosses the apiserver twice per attempt (see
// k8sexec's transfer.go); a claim removes both crossings, so a large
// workspace stops being apiserver traffic. Worth most on a
// senro.ScopePersistent workspace, which is large by intent.
//
// senro never creates, binds, resizes or deletes the claim: storage class,
// size and access mode are cluster decisions with money attached. Point it
// at a claim you made; a missing one is reported when the pod fails to
// schedule, naming it.
//
// A step that mounts a claim-backed workspace cannot be senro.Pure(), and
// Build refuses the combination naming the workspace: the action cache
// digests a pure step's inputs by walking the workspace, and it cannot walk
// a tree that lives only in the cluster. senro will not write a cache key
// describing bytes it did not measure.
//
// Access mode is the cluster's business, but it decides parallelism: a
// ReadWriteOnce claim ties every pod mounting it to one node, so a fan-out
// over that workspace serialises on scheduling. ReadWriteMany means a
// networked filesystem.
//
// Exclusion still holds: a ScopePersistent workspace is leased to one run
// at a time, and with a claim the lease is a coordination.k8s.io Lease in
// the same namespace, so two coordinators on two machines exclude each
// other, which a file lock on one machine could not.
func Claim(workspace, claim string) Option {
	return func(s *plan.ExecutorSpec) {
		if s.Claims == nil {
			s.Claims = map[string]string{}
		}
		s.Claims[workspace] = claim
	}
}

// ServiceAccount runs the step's pod under a named Kubernetes ServiceAccount.
//
// Unset means the namespace's "default" ServiceAccount and, because senro
// sets automountServiceAccountToken false, no cluster credential in the pod
// at all.
//
// The reason to set one is DelegateSecrets: on EKS a ServiceAccount
// annotated for IRSA, or bound through Pod Identity, gives the pod an AWS
// identity of its own. Naming it here does not on its own mount a token;
// see DelegateSecrets.
func ServiceAccount(name string) Option {
	return func(s *plan.ExecutorSpec) { s.ServiceAccount = name }
}

// DelegateSecrets stops senro fetching a step's secrets and lets the pod
// fetch its own, using the identity its ServiceAccount carries.
//
// By default senro resolves every declared secret on the coordinator and
// pushes the values into the cluster as an ephemeral Secret, owned by the
// pod and projected read-only at 0400. That stays the default because it
// works on every cluster and trusts the pod with nothing.
//
// With this, senro creates no Secret and no VALUE crosses the boundary:
// each declared secret's SOURCE URI is placed in the step's environment as
// SENRO_SECRET_<NAME>_SOURCE, the pod runs under the named ServiceAccount
// with its token mounted, and whatever the step runs resolves the URI.
//
// The costs, stated plainly:
//
//   - The pod gets a ServiceAccount token, which senro otherwise refuses
//     it, and a step's command can read it.
//   - senro can no longer tell you the secret arrived: a delegation fails
//     inside your command, with your tool's error, not the step.
//   - The redactor never sees the value, so it cannot scrub it from the
//     step's log.
//
// Refused without a ServiceAccount: delegating to the namespace's default
// account is delegating to whatever anyone else in that namespace also has.
func DelegateSecrets() Option {
	return func(s *plan.ExecutorSpec) { s.DelegateSecrets = true }
}

// Pod targets the workflow at a pod running ref, which must be pinned to a
// digest.
func Pod(ref string, opts ...Option) Target {
	spec := plan.ExecutorSpec{Kind: plan.ExecutorK8s, Image: ref}
	for _, o := range opts {
		o(&spec)
	}
	return Target{spec: spec}
}
