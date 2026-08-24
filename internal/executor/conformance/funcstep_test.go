package conformance_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/internal/binprov"
	senroexec "github.com/xavidop/senro/internal/executor"
	"github.com/xavidop/senro/internal/plan"
)

// binCache is ONE cross-build cache for the whole test binary. Per test it
// would compile the fixture once per test, which is minutes; the on-disk
// cache is keyed by the fixture's own digest and the target platform, so one
// directory shared here compiles once per platform however many cases run.
var (
	binCacheOnce sync.Once
	binCachePath string
)

func binCache(t *testing.T) string {
	t.Helper()
	binCacheOnce.Do(func() {
		dir, err := os.MkdirTemp("", "senro-conffunc-bin")
		if err != nil {
			t.Fatalf("creating the cross-build cache: %v", err)
		}
		binCachePath = dir
	})
	return binCachePath
}

func fixturePkg(t *testing.T) string {
	t.Helper()
	dir, err := filepath.Abs(filepath.Join("testdata", "fixtures", "conffunc"))
	if err != nil {
		t.Fatalf("resolving the fixture package: %v", err)
	}
	return dir
}

// withFuncBinaries points a run at the fixture, cross-compiled for whatever
// the target turns out to be.
func withFuncBinaries(t *testing.T) runOpt {
	t.Helper()
	return func(c *runConfig) {
		c.binaries = binprov.New(binprov.Options{
			Dir: binCache(t), Pkg: fixturePkg(t),
			// Pinned so the strategy does not depend on whose machine this
			// is: binprov ships the coordinator's own executable when the
			// platform matches and cross-compiles otherwise. Identity is
			// also wrong here, since the coordinator IS the test binary and
			// it cannot re-enter as a step child.
			SelfPlatform: senroexec.Platform{OS: "plan9", Arch: "386"},
		})
	}
}

func funcNode(id, name string, params any) plan.Node {
	raw, err := json.Marshal(params)
	if err != nil {
		panic(err)
	}
	return plan.Node{ID: id, Kind: "func", Func: &plan.FuncSpec{Name: name, Params: raw}}
}

// TestAFuncStepRunsOffTheCoordinator is the whole re-entry story, on every
// executor that can host it: the pipeline binary is cross-compiled for the
// target, staged there content-addressed, re-entered as a step child, and
// the function's own output comes back as the step's log.
//
// senro's own suite proves this for ssh (internal/engine's remote func
// tests) and for containers; nothing until now ran it on Kubernetes, where
// the same story goes through a pod, an init container and the apiserver's
// exec subresource instead.
func TestAFuncStepRunsOffTheCoordinator(t *testing.T) {
	for _, tg := range targets() {
		t.Run(tg.name, func(t *testing.T) {
			t.Parallel()
			if tg.name == "local" {
				t.Skip("a func step runs in the coordinator's own process on the local executor; " +
					"there is nothing to stage and nothing to re-enter")
			}
			p := &plan.Plan{
				Version: 1,
				Nodes:   []plan.Node{funcNode("whoami", "conffunc/whoami", map[string]string{})},
			}
			res := runPlanOn(t, tg, p, withFuncBinaries(t))
			if res.err != nil {
				t.Fatalf("engine.Run: %v", res.err)
			}
			if res.status != api.RunSucceeded {
				for _, e := range res.events {
					t.Logf("%s step=%q %s", e.Type, e.Step, string(e.Payload))
				}
				t.Fatalf("run status = %q", res.status)
			}

			out := stepLogText(t, res.dir, "whoami", 1, "stdout")
			if !strings.Contains(out, "whoami linux/") {
				t.Errorf("the function did not report running on linux; its stdout was %q", out)
			}
			if !strings.Contains(out, "step=whoami") {
				t.Errorf("the re-entered child did not receive the step's identity: %q", out)
			}
			// The two streams stay apart wherever the executor keeps them
			// apart; on Kubernetes they are merged by construction, so the
			// line is looked for in either.
			errOut := stepLogText(t, res.dir, "whoami", 1, "stderr")
			if !strings.Contains(errOut+out, "whoami on stderr") {
				t.Errorf("the function's stderr did not come back: stdout=%q stderr=%q", out, errOut)
			}

			// binary.staged is how a client knows a transfer happened at all.
			var staged bool
			for _, e := range res.events {
				if e.Type == api.BinaryStaged {
					staged = true
				}
			}
			if !staged {
				t.Error("no binary.staged event: nothing recorded that senro moved a binary")
			}
		})
	}
}

// TestAFuncStepOffTheCoordinatorReadsAndWritesAMountedWorkspace. A workspace
// crosses to the target as a mount, and the re-entered child has to be told
// where the target realized it (executor.MountLocator): the coordinator's
// own path means nothing over there.
func TestAFuncStepOffTheCoordinatorReadsAndWritesAMountedWorkspace(t *testing.T) {
	for _, tg := range targets() {
		t.Run(tg.name, func(t *testing.T) {
			t.Parallel()
			if tg.name == "local" {
				t.Skip("nothing is staged or re-entered on the local executor")
			}
			p := &plan.Plan{
				Version:    1,
				Workspaces: []plan.WorkspaceSpec{{Name: "src", Scope: "run"}},
				Nodes: []plan.Node{
					{
						ID: "seed", Kind: "exec", WorkDir: "/ws",
						Cmd:    []string{tg.shell, "-c", `printf 'from-the-coordinator\n' > in.txt`},
						Mounts: []plan.MountSpec{{Workspace: "src", At: "/ws"}},
					},
					func() plan.Node {
						n := funcNode("use", "conffunc/workspace",
							map[string]string{"want": "from-the-coordinator\n"})
						n.Needs = []string{"seed"}
						// No WorkDir: plan.Validate refuses one on a func
						// step, because a func step on the coordinator runs
						// in the coordinator's own process. The function
						// reaches its files through ctx.Workspace instead.
						n.Mounts = []plan.MountSpec{{Workspace: "src", At: "/ws"}}
						return n
					}(),
					{
						ID: "check", Kind: "exec", WorkDir: "/ws", Needs: []string{"use"},
						Cmd:    []string{tg.shell, "-c", `cat out.txt`},
						Mounts: []plan.MountSpec{{Workspace: "src", At: "/ws"}},
					},
				},
			}
			res := runPlanOn(t, tg, p, withFuncBinaries(t))
			if res.err != nil {
				t.Fatalf("engine.Run: %v", res.err)
			}
			if res.status != api.RunSucceeded {
				for _, e := range res.events {
					t.Logf("%s step=%q %s", e.Type, e.Step, string(e.Payload))
				}
				t.Fatalf("run status = %q", res.status)
			}
			if got := stepLogText(t, res.dir, "check", 1, "stdout"); !strings.Contains(got, "written-by-func") {
				t.Errorf("what the re-entered function wrote into the workspace did not come "+
					"back; the next step read %q", got)
			}
		})
	}
}

// TestAFailingFuncStepOffTheCoordinatorFailsTheStepAndNotTheSubstrate. A
// function's error is a verdict that travels back in the protocol; reported
// as infrastructure it would be retried by retry.OnInfra().
func TestAFailingFuncStepOffTheCoordinatorFailsTheStepAndNotTheSubstrate(t *testing.T) {
	for _, tg := range targets() {
		t.Run(tg.name, func(t *testing.T) {
			t.Parallel()
			if tg.name == "local" {
				t.Skip("nothing is staged or re-entered on the local executor")
			}
			p := &plan.Plan{
				Version: 1,
				Nodes: []plan.Node{func() plan.Node {
					n := funcNode("boom", "conffunc/boom", map[string]string{})
					n.Retry = &plan.RetrySpec{MaxAttempts: 2, Predicate: "infra", BackoffBaseMS: 1}
					return n
				}()},
			}
			res := runPlanOn(t, tg, p, withFuncBinaries(t))
			if res.err != nil {
				t.Fatalf("engine.Run: %v", res.err)
			}
			if res.status == api.RunSucceeded {
				t.Fatal("a function that returned an error produced a successful run")
			}
			st, _, found := stateOf(t, res.events, "boom")
			if !found {
				t.Fatal("no step.finished for the failing func step")
			}
			if st != api.StateFailed {
				t.Errorf("a function that returned an error settled as %q, want %q", st, api.StateFailed)
			}
			if n := attemptsOf(res.events, "boom"); n > 1 {
				t.Errorf("a function's own error was retried %d times under retry.OnInfra(): it "+
					"was classified as an infrastructure failure", n)
			}
		})
	}
}
