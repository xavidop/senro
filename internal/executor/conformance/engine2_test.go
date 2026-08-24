package conformance_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/xavidop/mamori"
	"github.com/xavidop/mamori/mamoritest"
	"github.com/xavidop/mamori/secret"

	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/internal/plan"
	"github.com/xavidop/senro/internal/secrets"
)

// TestASenroignoreKeepsPathsOutOfEveryExecutorsSnapshot.
//
// A workspace's own .senroignore is part of what "this workspace" MEANS, and
// the EXECUTOR takes the snapshot: two executors that read it differently
// would give one workspace two content addresses, and the cache would stop
// hitting the moment a pipeline moved a workflow from one to the other. The
// file travels with the workspace rather than with the plan, which is what
// makes this a cross-executor question at all — the copy the digest is
// computed from is the one that came back off the target.
//
// The second half of the case is the divergence nobody would guess and
// everybody eventually meets: an excluded path is out of the SNAPSHOT
// everywhere, but whether the next step can still SEE it depends on the
// executor, and the line is plan.Node.RemoteMounts rather than "is it
// local". Where the target shares the coordinator's filesystem the
// workspace is one directory both steps mount and an exclusion removes
// nothing from it — that includes CONTAINERS, which bind those very
// directories. Where it does not (ssh, k8s), the next step gets a copy
// restored from the snapshot and the file is simply not there. Both are
// asserted, so neither can drift and the difference stays written down.
func TestASenroignoreKeepsPathsOutOfEveryExecutorsSnapshot(t *testing.T) {
	for _, tg := range targets() {
		t.Run(tg.name, func(t *testing.T) {
			t.Parallel()
			p := &plan.Plan{
				Version:    1,
				Workspaces: []plan.WorkspaceSpec{{Name: "src", Scope: "run"}},
				Nodes: []plan.Node{
					{
						ID: "seed", Kind: "exec", WorkDir: "/ws",
						Cmd: []string{tg.shell, "-c",
							`printf 'build/\n*.tmp\n' > .senroignore && ` +
								`mkdir -p build && printf 'x\n' > build/out.bin && ` +
								`printf 'x\n' > scratch.tmp && printf 'kept\n' > keep.txt`},
						Mounts: []plan.MountSpec{{Workspace: "src", At: "/ws"}},
					},
					{
						ID: "read", Kind: "exec", WorkDir: "/ws", Needs: []string{"seed"},
						Cmd: []string{tg.shell, "-c",
							`for f in keep.txt build/out.bin scratch.tmp; do ` +
								`if [ -e "$f" ]; then printf '%s=present\n' "$f"; ` +
								`else printf '%s=absent\n' "$f"; fi; done`},
						Mounts: []plan.MountSpec{{Workspace: "src", At: "/ws"}},
					},
				},
			}
			res := runPlanOn(t, tg, p)
			if res.err != nil {
				t.Fatalf("engine.Run: %v", res.err)
			}
			if res.status != api.RunSucceeded {
				t.Fatalf("run status = %q", res.status)
			}

			// The snapshot is the portable promise: it is what every cache
			// key downstream is computed from. Two files survive the
			// exclusions — .senroignore itself and keep.txt.
			var snap api.WSSnapshotBody
			var seen bool
			for _, e := range res.events {
				if e.Type != api.WSSnapshot || e.Step != "seed" {
					continue
				}
				if err := e.Decode(&snap); err != nil {
					t.Fatalf("decode ws.snapshot: %v", err)
				}
				seen = true
			}
			if !seen {
				t.Fatal("no ws.snapshot for the seeding step")
			}
			if snap.Files != 2 {
				t.Errorf("the snapshot carries %d files, want 2 (.senroignore and keep.txt): "+
					"build/out.bin and scratch.tmp are excluded by the workspace's own "+
					".senroignore, and anything else means this executor read it differently",
					snap.Files)
			}

			// And what the next step can see, which is where the executors
			// legitimately differ.
			got := stepLogText(t, res.dir, "read", 1, "stdout")
			if !strings.Contains(got, "keep.txt=present") {
				t.Errorf("the next step could not see keep.txt at all:\n%s", got)
			}
			wantExcluded := "absent"
			why := "the next step mounts a copy restored from the snapshot, which does not carry them"
			if !tg.remoteMounts {
				wantExcluded = "present"
				why = "this target shares the coordinator's filesystem, so both steps mount ONE " +
					"directory and an exclusion removes nothing from it"
			}
			for _, f := range []string{"build/out.bin", "scratch.tmp"} {
				if !strings.Contains(got, f+"="+wantExcluded) {
					t.Errorf("want %s=%s in the next step's view (%s), got:\n%s",
						f, wantExcluded, why, got)
				}
			}
		})
	}
}

// TestCancellingARunEndsItBoundedAndHonestly. Ctrl-C on a CI job is the
// commonest way a run ends badly, and the promise is the same on every
// executor: the engine returns, the step's own record says cancelled rather
// than succeeded, and the ledger is sealed rather than truncated.
func TestCancellingARunEndsItBoundedAndHonestly(t *testing.T) {
	for _, tg := range targets() {
		t.Run(tg.name, func(t *testing.T) {
			t.Parallel()
			p := &plan.Plan{
				Version: 1,
				Nodes: []plan.Node{{
					ID: "long", Kind: "exec",
					Cmd: []string{tg.shell, "-c", "echo started; sleep 600"},
				}},
			}

			ctx, cancel := context.WithCancel(context.Background())
			// Cancelled once the step is genuinely running on the target,
			// which on a cold cluster is a pod pull away.
			go func() {
				time.Sleep(45 * time.Second)
				cancel()
			}()

			done := make(chan runResult, 1)
			go func() { done <- runPlanOn(t, tg, p, withCtx(ctx)) }()

			var res runResult
			select {
			case res = <-done:
			case <-time.After(6 * time.Minute):
				t.Fatal("the run did not return within 6 minutes of its context being cancelled")
			}
			cancel()

			if res.status == api.RunSucceeded {
				t.Errorf("a cancelled run reported %q", res.status)
			}
			st, _, found := stateOf(t, res.events, "long")
			if !found {
				t.Fatal("no step.finished for the cancelled step: the ledger was truncated")
			}
			if st == api.StateSucceeded || st == api.StateRecovered {
				t.Errorf("a step killed by cancellation settled as %q", st)
			}
			// run.finished is what seals the stream; a client replaying this
			// ledger has to be able to tell a cancelled run from a truncated
			// one.
			var sealed bool
			for _, e := range res.events {
				if e.Type == api.RunFinished {
					sealed = true
				}
			}
			if !sealed {
				t.Error("no run.finished: a cancelled run's ledger is indistinguishable from a " +
					"coordinator that was killed")
			}
		})
	}
}

// TestASecretNeverReachesAStepsLogOnAnyExecutor. Redaction is the engine's,
// but what it has to redact arrives through the executor, and the executors
// do not deliver a step's output the same way: two streams on three of them,
// one merged log on Kubernetes. A value that survived on any of them is the
// credential in a file people archive and attach to bug reports.
func TestASecretNeverReachesAStepsLogOnAnyExecutor(t *testing.T) {
	const value = "conformance-secret-value-8f31c2"
	for _, tg := range targets() {
		t.Run(tg.name, func(t *testing.T) {
			t.Parallel()
			// Built the way a real run's are: mamori resolves them before
			// senro.Run is called, and internal/secrets takes the values out
			// of the struct it returned. senro itself resolves nothing.
			type config struct {
				Token secret.String `source:"fake://ci/token"`
			}
			pr := mamoritest.NewProvider("fake")
			pr.Set("ci/token", value)
			cfg, err := mamori.Load[config](context.Background(), mamori.WithProvider(pr))
			if err != nil {
				t.Fatalf("mamori.Load: %v", err)
			}
			set, err := secrets.FromConfig(cfg)
			if err != nil {
				t.Fatalf("secrets.FromConfig: %v", err)
			}

			p := &plan.Plan{
				Version: 1,
				Nodes: []plan.Node{{
					ID: "leak", Kind: "exec",
					// A step doing the careless thing on purpose: printing
					// the credential it was handed, on both streams.
					Cmd: []string{tg.shell, "-c",
						`cat "$SENRO_SECRET_TOKEN"; cat "$SENRO_SECRET_TOKEN" >&2; echo done`},
					Secrets: []plan.SecretSpec{{Name: "Token"}},
				}},
			}
			res := runPlanOn(t, tg, p, withSecrets(set))
			if res.err != nil {
				t.Fatalf("engine.Run: %v", res.err)
			}
			if res.status != api.RunSucceeded {
				t.Fatalf("run status = %q", res.status)
			}

			// Every file the run wrote, not only the step's log: the ledger
			// carries a tail of the step's output too.
			var found []string
			walkErr := filepath.WalkDir(res.dir, func(path string, d os.DirEntry, err error) error {
				if err != nil || d.IsDir() {
					return nil
				}
				b, readErr := os.ReadFile(path)
				if readErr != nil {
					return nil
				}
				if strings.Contains(string(b), value) {
					rel, _ := filepath.Rel(res.dir, path)
					found = append(found, rel)
				}
				return nil
			})
			if walkErr != nil {
				t.Fatalf("walking the run directory: %v", walkErr)
			}
			if len(found) > 0 {
				t.Errorf("the secret's VALUE is in %v under the run directory", found)
			}

			// And the redaction really happened, rather than the step
			// having failed to print anything at all.
			var redacted int
			for _, e := range res.events {
				if e.Type != api.SecretRedacted {
					continue
				}
				var b api.SecretRedactedBody
				if err := e.Decode(&b); err != nil {
					t.Fatalf("decode secret.redacted: %v", err)
				}
				redacted += b.Count
			}
			if redacted == 0 {
				t.Errorf("no secret.redacted event: the step printed its credential twice and "+
					"nothing recorded that senro removed it (stdout: %q)",
					stepLogText(t, res.dir, "leak", 1, "stdout"))
			}
		})
	}
}
