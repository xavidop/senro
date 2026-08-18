// Package ssh targets a workflow at a command running on a remote host over
// SSH.
//
//	build := ssh.Host("deploy@build-07.internal",
//		ssh.CacheClass("ubuntu-24.04/amd64/toolchain-v3"))
//	release := p.Workflow("release", senro.On(build))
//
// The connection comes from your own SSH configuration, and from nowhere
// else: senro shells out to the `ssh` binary on the coordinator's PATH, so
// ~/.ssh/config, known_hosts, the agent, ProxyJump and certificates apply
// unchanged, and the destination written here is exactly the destination
// you would type. No key, password or host key enters a senro pipeline: a
// plan must stay portable and carry no credential.
//
// senro adds exactly one configuration option of its own, `-o BatchMode=yes`,
// overriding none of yours: a coordinator with no terminal fails instead of
// blocking on a passphrase prompt, and an UNKNOWN host key is refused rather
// than asked about, because senro delivers secrets to these hosts. The only
// other flag is `-T` (pipes rather than a terminal). senro never passes
// StrictHostKeyChecking in either direction: accept-on-first-use is your
// ssh_config's decision, not senro's.
//
// senro also opens one connection per host and multiplexes every command of
// the run over it, unless your own configuration already names a ControlPath
// for the destination, in which case it adds nothing and yours is in force.
// See NoMultiplexing.
//
// This package deliberately contains no SSH code: it is the declaration, and
// internal/executor/sshexec is the execution. Building a pipeline costs
// nothing, needs no host, and works on a machine with no network.
package ssh

import "github.com/xavidop/senro/internal/plan"

// Target is what senro.On accepts. It satisfies senro.ExecutorTarget.
type Target struct{ spec plan.ExecutorSpec }

// ExecutorSpec reports where the workflow's steps run.
func (t Target) ExecutorSpec() plan.ExecutorSpec { return t.spec }

// Option configures an ssh target.
type Option func(*plan.ExecutorSpec)

// CacheClass declares the cache equivalence class of the hosts this target
// names: a statement that any host you give the same class to computes the
// same bytes from the same inputs.
//
// Left undeclared, the executor reports "ssh/<os>/<arch>", read from the
// host with uname. That is deliberately NOT the hostname: a class built
// from host identity would mean a fleet of forty identical build machines
// never shared a single cache entry, a failure no error message would ever
// report.
//
// The value is yours to keep honest: senro cannot tell that build-07 has a
// different Go toolchain from build-08, so declare what actually changes a
// build's output.
//
//	ssh.CacheClass("ubuntu-24.04/amd64/go1.26/node22")
//
// The same lever localexec.WithClass gives local execution; a class
// declared in both places means both share entries.
func CacheClass(class string) Option {
	return func(s *plan.ExecutorSpec) { s.Class = class }
}

// Platform declares the execution platform, as Go spells it: ("linux",
// "amd64").
//
// Usually unnecessary. Left undeclared, the executor asks the host with
// `uname -s -m` on its first connection of the run and translates the answer.
// Declare it when the host reports something senro does not translate, which
// it will name in its refusal rather than guess at.
func Platform(os, arch string) Option {
	return func(s *plan.ExecutorSpec) { s.OS, s.Arch = os, arch }
}

// WorkspaceRoot is the directory on the REMOTE host under which senro creates
// one directory per step attempt: its workspaces, and the exit status of its
// command.
//
// Empty means "$HOME/.senro/work", which is chosen because it needs no
// privilege and no prior setup on the host. Point it somewhere with room on a
// fleet whose home directories are small or on a shared filesystem, for
// example ssh.WorkspaceRoot("/var/lib/senro/ws").
//
// Secrets never live here. They go to the host's own runtime directory, which
// is tmpfs where the host has one; see the SSH executor documentation.
func WorkspaceRoot(dir string) Option {
	return func(s *plan.ExecutorSpec) { s.Root = dir }
}

// NoMultiplexing gives every command its own connection, as senro did before
// it multiplexed.
//
// senro's default is one OpenSSH control master per host, opened once and
// reused, so a step pays a handshake instead of six. The cost is a shared
// failure: one master that breaks takes the commands riding it with it, and a
// host whose sshd caps sessions per connection caps senro's parallelism on it
// too. Declare this for a fleet where that trade is wrong.
//
//	ssh.Host("deploy@build-07.internal", ssh.NoMultiplexing())
//
// It changes no cache key: multiplexing decides how a step's bytes cross the
// wire, never what the step computes.
func NoMultiplexing() Option {
	return func(s *plan.ExecutorSpec) { s.NoMultiplex = true }
}

// Host targets the workflow at dest, which is written exactly as `ssh` itself
// takes it: a hostname, a "user@host", or an alias from your own ssh_config.
func Host(dest string, opts ...Option) Target {
	spec := plan.ExecutorSpec{Kind: plan.ExecutorSSH, Host: dest}
	for _, o := range opts {
		o(&spec)
	}
	return Target{spec: spec}
}
