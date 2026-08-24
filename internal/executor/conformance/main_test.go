// Package conformance_test runs ONE suite against EVERY executor in this
// build, so a semantic that differs between them is a failure here rather
// than a discovery made in production.
//
// internal/executor promises portability: the same Cmd, the same
// SandboxSpec, the same answers, whether the step runs as a child process
// on this machine, in a container on the local daemon, in a pod on a
// cluster, or over ssh on somebody else's host. Each executor's own package
// proves it works. Nothing before this proved they AGREE.
//
// Two levels, because a divergence can live at either:
//
//   - the Sandbox interface directly (command_test.go, mount_test.go,
//     secret_test.go, capability_test.go, user_test.go): argv and
//     environment fidelity, exit codes, stream separation, workspace
//     realization and snapshots, secret delivery, the optional capabilities.
//   - whole plans through internal/engine (engine_test.go,
//     funcstep_test.go): the features that are the ENGINE's but reach a
//     target through Sandbox, so each one can diverge per executor —
//     workspaces handed between steps, retries, handlers, timeouts, the
//     action cache, scratch caches, and a func step staged and re-entered
//     off the coordinator.
//
// Running it:
//
//	SENRO_REQUIRE_DOCKER=1 SENRO_REQUIRE_KIND=1 go test ./internal/executor/conformance/
//
// Without those, an absent Docker daemon or kind skips the executors that
// need them, exactly as every other suite in this repository does. Add
// SENRO_KIND_KEEP=1 while iterating, so the cluster survives between runs.
//
// A case here that fails on some executors and passes on others is the
// point of the package: read the failure as "these two disagree", and the
// message names which promise they disagree about.
package conformance_test

import (
	"os"
	"testing"

	"github.com/xavidop/senro/internal/executor/sshexec/sshdtest"
	"github.com/xavidop/senro/internal/kubeapi/kindtest"
)

// TestMain owns the two shared fixtures' lifetimes. Both are per BINARY: the
// sshd container serves every ssh case and the kind cluster every k8s one,
// so neither can be a t.Cleanup on any single test. Both teardowns are
// idempotent, so a run where every case skipped still exits cleanly.
func TestMain(m *testing.M) {
	code := sshdtest.RunMain(m)
	kindtest.Cleanup()
	os.Exit(code)
}
