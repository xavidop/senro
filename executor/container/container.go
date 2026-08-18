// Package container targets a workflow at a container on the coordinator's
// own Docker daemon.
//
//	node := container.Image("node:22-bookworm-slim")
//	setup := p.Workflow("setup", senro.On(node))
//
// The image reference is recorded in the plan exactly as written here and
// resolves to a digest once per run; the digest, not the tag, enters the
// cache key and step.started's executor_class. Resolving at build time
// would make a plan's identity depend on one machine's daemon cache.
//
// This package deliberately contains no Docker code: it is the declaration,
// and internal/executor/containerexec is the execution. Building a pipeline
// costs nothing, needs no daemon, and works on a machine that has never
// installed Docker.
package container

import "github.com/xavidop/senro/internal/plan"

// Target is what senro.On accepts. It satisfies senro.ExecutorTarget.
type Target struct{ spec plan.ExecutorSpec }

// ExecutorSpec reports where the workflow's steps run.
func (t Target) ExecutorSpec() plan.ExecutorSpec { return t.spec }

// Option configures a container target.
type Option func(*plan.ExecutorSpec)

// User runs the step as a specific user, in Docker's own "uid:gid" or "name"
// spelling.
//
// The default is the coordinator's own uid and gid, because every mount is
// a bind mount inside the run directory: a step running as root leaves
// root-owned files in runs/<id>/ws that nobody can delete without sudo.
// Declare User("0:0") for a step that genuinely needs root, and expect
// exactly that consequence.
//
// A declared user is part of the step's cache equivalence class; the
// default is not, because a class built from the coordinator's own identity
// would mean a fleet never shares a cache entry.
func User(u string) Option {
	return func(s *plan.ExecutorSpec) { s.User = u }
}

// RegistryAuth authenticates the pull of this image, so a workflow can run
// on an image in a private registry with no `docker login` on the machine:
//
//	builder := container.Image("ghcr.io/acme/builder:v3",
//		container.RegistryAuth("acme-ci", "GHCRToken"))
//
// account is the registry account's name and is NOT a credential ("AWS" for
// Elastic Container Registry, "oauth2accesstoken" for Artifact Registry, a
// login for ghcr.io); it is recorded in the plan as written. field names a
// FIELD of the struct handed to senro.WithSecrets, exactly as
// StepBuilder.SecretEnv names one, and mamori has already resolved it before
// the run starts. A password typed here is a field name senro cannot
// resolve, and the run is refused at second zero naming it, rather than
// written into plan.json.
//
// The value reaches ONE place: the X-Registry-Auth header of the pull. Never
// argv, never an environment value, never the plan, a cache key, an event or
// a log, and it is registered with the run's redactor like every other
// resolved secret.
//
// It is deliberately NOT part of the step's cache equivalence class: that
// class already carries the resolved image DIGEST, so two credentials that
// fetch the same bytes are the same step, and folding the credential in
// would mean a rotated token invalidated every entry on that image. It IS
// part of the executor's instance key, so two targets naming one image under
// two credentials stay two executors and two pulls.
//
// senro runs no credential helper, reads no ~/.docker/config.json and
// contacts no metadata service: for a registry whose credential is issued by
// another service, resolve it into the configuration struct first.
func RegistryAuth(account, field string) Option {
	return func(s *plan.ExecutorSpec) {
		s.RegistryAuth = &plan.RegistryAuthSpec{Username: account, Secret: field}
	}
}

// Image targets the workflow at a container built from ref.
func Image(ref string, opts ...Option) Target {
	spec := plan.ExecutorSpec{Kind: plan.ExecutorContainer, Image: ref}
	for _, o := range opts {
		o(&spec)
	}
	return Target{spec: spec}
}
