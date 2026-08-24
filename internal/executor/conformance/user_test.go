package conformance_test

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/xavidop/senro/internal/dockerd/dockertest"
	senroexec "github.com/xavidop/senro/internal/executor"
	"github.com/xavidop/senro/internal/executor/containerexec"
	"github.com/xavidop/senro/internal/executor/k8sexec"
	"github.com/xavidop/senro/internal/kubeapi/kindtest"
	"github.com/xavidop/senro/internal/plan"
	"github.com/xavidop/senro/internal/workspace"
)

// nonRootUID is a uid no image's own content is owned by, so a mount or a
// secret the step can read is one it could read BECAUSE senro arranged it.
const nonRootUID = "1000:1000"

// asUser builds the two executors that take a declared user. The local and
// ssh executors do not: a step runs as the account senro itself runs as
// there.
func asUser(t *testing.T, name string, snap *workspace.Snapshotter) (senroexec.Executor, bool) {
	t.Helper()
	switch name {
	case "container":
		c := dockertest.Require(t)
		dockertest.Pull(t, c)
		ex, err := containerexec.New(
			plan.ExecutorSpec{
				Kind: plan.ExecutorContainer, Image: dockertest.Image, User: nonRootUID,
			},
			snap, containerexec.WithClient(c), containerexec.WithRunID("confuser"))
		if err != nil {
			t.Fatalf("containerexec.New: %v", err)
		}
		return ex, true
	case "k8s":
		c := kindtest.Require(t)
		ex, err := k8sexec.New(
			plan.ExecutorSpec{
				Kind: plan.ExecutorK8s, Image: kindtest.Image,
				Namespace: kindtest.Namespace, User: nonRootUID,
			},
			snap, k8sexec.WithClient(c.Client), k8sexec.WithRunID("confuser"),
			k8sexec.WithStartTimeout(3*time.Minute))
		if err != nil {
			t.Fatalf("k8sexec.New: %v", err)
		}
		return ex, true
	}
	return nil, false
}

// TestAStepRunningAsADeclaredUserCanWriteItsWorkspace. A declared user is
// the ordinary case for a hardened image, and a workspace the step cannot
// write is a pipeline that cannot build anything.
func TestAStepRunningAsADeclaredUserCanWriteItsWorkspace(t *testing.T) {
	for _, tg := range targets() {
		t.Run(tg.name, func(t *testing.T) {
			t.Parallel()
			st := newStore(t)
			ex, ok := asUser(t, tg.name, st.Snapshotter)
			if !ok {
				t.Skipf("%s takes no declared user: a step runs as senro's own account there", tg.name)
			}

			m := seed(t, st.Snapshotter, "src", "/ws", func(dir string) {
				write(t, filepath.Join(dir, "input.txt"), "seeded\n", 0o644)
			})

			sb := sandboxOn(t, ex, senroexec.SandboxSpec{
				StepID: "asuser", WorkDir: "/ws", Mounts: []senroexec.Mount{m},
			})
			exit, stdout, stderr, err := runOn(t, sb, senroexec.Cmd{
				Args: []string{tg.shell, "-c",
					`id -u; cat input.txt; printf 'produced\n' > made.txt && echo WROTE`},
				Dir: senroexec.CmdDir("/ws", []senroexec.Mount{m}),
			})
			if err != nil {
				t.Fatalf("Run: %v (stderr: %s)", err, stderr)
			}
			if !strings.Contains(stdout, "1000") {
				t.Errorf("the step did not run as the declared uid; it reported %q", stdout)
			}
			if !strings.Contains(stdout, "seeded") {
				t.Errorf("the step could not READ its workspace as uid 1000: exit=%d stdout=%q stderr=%q",
					exit, stdout, stderr)
			}
			if !strings.Contains(stdout, "WROTE") {
				t.Errorf("the step could not WRITE its workspace as uid 1000: exit=%d stdout=%q stderr=%q",
					exit, stdout, stderr)
			}
		})
	}
}

// TestAStepRunningAsADeclaredUserCanReadItsSecret. The value is delivered as
// a file with no bit for group or other, which is the whole point; it must
// still be a file the step's own account can open.
func TestAStepRunningAsADeclaredUserCanReadItsSecret(t *testing.T) {
	for _, tg := range targets() {
		t.Run(tg.name, func(t *testing.T) {
			t.Parallel()
			st := newStore(t)
			ex, ok := asUser(t, tg.name, st.Snapshotter)
			if !ok {
				t.Skipf("%s takes no declared user", tg.name)
			}

			sb := sandboxOn(t, ex, senroexec.SandboxSpec{
				StepID: "asusersecret", Secrets: []senroexec.SecretRef{{Name: "token"}},
			})
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			p, err := sb.PutSecret(ctx, "token", []byte("value-1234"))
			if err != nil {
				t.Fatalf("PutSecret: %v", err)
			}

			exit, stdout, stderr, err := runOn(t, sb, senroexec.Cmd{
				Args: []string{tg.shell, "-c",
					`printf 'uid=%s gid=%s\n' "$(id -u)" "$(id -g)"; ls -ln "$1"; cat "$1"`,
					"senro-step", p},
			})
			if err != nil {
				t.Fatalf("Run: %v (stderr: %s)", err, stderr)
			}
			if exit != 0 || !strings.Contains(stdout, "value-1234") {
				t.Errorf("a step running as uid 1000 could not read the secret senro delivered to "+
					"it. The step saw:\n%s\nstderr: %s\n(exit %d)", stdout, stderr, exit)
			}
		})
	}
}
