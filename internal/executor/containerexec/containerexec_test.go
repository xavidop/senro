package containerexec_test

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/xavidop/senro/internal/cas"
	"github.com/xavidop/senro/internal/dockerd/dockertest"
	"github.com/xavidop/senro/internal/executor"
	"github.com/xavidop/senro/internal/executor/containerexec"
	"github.com/xavidop/senro/internal/executor/secretdir"
	"github.com/xavidop/senro/internal/plan"
	"github.com/xavidop/senro/internal/workspace"
)

// testRunID gives each test its own value for the "senro.run" label every
// container carries: TestNoContainerSurvivesAClosedSandbox counts containers
// by this label, so a shared value would let another test's container move
// the count. t.Name() is unique per test and stable per run.
func testRunID(t *testing.T) string {
	t.Helper()
	return "test-" + strings.ReplaceAll(t.Name(), "/", "-")
}

func newExecutor(t *testing.T, spec plan.ExecutorSpec) *containerexec.Executor {
	t.Helper()
	c := dockertest.Require(t)
	dockertest.Pull(t, c)
	store, err := cas.Open(filepath.Join(t.TempDir(), "cas"))
	if err != nil {
		t.Fatal(err)
	}
	ex, err := containerexec.New(spec, workspace.NewSnapshotter(store),
		containerexec.WithClient(c), containerexec.WithRunID(testRunID(t)))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return ex
}

// The digest, not the tag, is what lands in the cache key, so an image tag
// moving under you must invalidate the cache.
func TestTheClassCarriesTheResolvedImageDigestAndNotTheTag(t *testing.T) {
	ex := newExecutor(t, plan.ExecutorSpec{Kind: plan.ExecutorContainer, Image: dockertest.Image})
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	class, err := ex.Class(ctx)
	if err != nil {
		t.Fatalf("Class: %v", err)
	}
	if !strings.Contains(class, "sha256:") {
		t.Errorf("class = %q, want a resolved image digest in it", class)
	}
	if strings.Contains(class, "1.36") {
		t.Errorf("class = %q, and it carries the TAG; a tag that moves would not invalidate", class)
	}
	again, err := ex.Class(ctx)
	if err != nil || again != class {
		t.Errorf("Class is not stable within a run: %q then %q (%v)", class, again, err)
	}
}

func TestTheDeclaredPlatformComesFromTheImage(t *testing.T) {
	ex := newExecutor(t, plan.ExecutorSpec{Kind: plan.ExecutorContainer, Image: dockertest.Image})
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	p, err := ex.DeclaredPlatform(ctx)
	if err != nil {
		t.Fatalf("DeclaredPlatform: %v", err)
	}
	if p.OS != "linux" {
		t.Errorf("platform = %s, want a linux image even on a darwin coordinator", p)
	}
	if p.Arch == "" {
		t.Error("no architecture")
	}
}

// A cache key's env component must be built from what the step ACTUALLY
// receives: CacheEnv("PATH") on a container step must see the image's PATH,
// which never appears in the plan at all.
func TestEffectiveEnvMergesTheImagesOwnEnvironmentUnderTheDeclaredOne(t *testing.T) {
	c := dockertest.Require(t)
	dockertest.Pull(t, c)
	ex := newExecutor(t, plan.ExecutorSpec{Kind: plan.ExecutorContainer, Image: dockertest.Image})
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	got, err := ex.EffectiveEnv(ctx, []string{"CI=1", "PATH=/only/mine"})
	if err != nil {
		t.Fatalf("EffectiveEnv: %v", err)
	}
	var sawCI, sawPath int
	for _, kv := range got {
		switch {
		case kv == "CI=1":
			sawCI++
		case strings.HasPrefix(kv, "PATH="):
			sawPath++
			if kv != "PATH=/only/mine" {
				t.Errorf("PATH = %q, want the declared value to win over the image's", kv)
			}
		}
	}
	if sawCI != 1 || sawPath != 1 {
		t.Errorf("CI appeared %d time(s) and PATH %d time(s), want one each", sawCI, sawPath)
	}

	// Everything above is only the OVERRIDE half: the test image declares
	// exactly one variable (PATH) and the declaration replaces it, so an
	// EffectiveEnv that discarded the image's environment entirely would
	// still pass it. Assert the image's contribution against its own
	// manifest, with a declared env that mentions none of the image's names:
	// an image env silently dropped means CacheEnv("PATH") on a container
	// step watches nothing at all and never invalidates.
	info, ok, err := c.ImageInspect(ctx, dockertest.Image)
	if err != nil || !ok {
		t.Fatalf("ImageInspect(%s): ok=%v err=%v", dockertest.Image, ok, err)
	}
	if len(info.Env) == 0 {
		t.Fatalf("%s declares no environment of its own, so this assertion proves nothing; "+
			"the test image has to carry at least one ENV", dockertest.Image)
	}
	untouched, err := ex.EffectiveEnv(ctx, []string{"CI=1"})
	if err != nil {
		t.Fatalf("EffectiveEnv: %v", err)
	}
	for _, want := range info.Env {
		if !slices.Contains(untouched, want) {
			t.Errorf("the image declares %q and EffectiveEnv did not report it (%q); the image's own "+
				"environment is being discarded", want, untouched)
		}
	}
}

// mergeEnv computes the merge in Go for the cache key, and the DAEMON
// computes it again for the process; nothing makes the two agree, and a name
// EffectiveEnv reports with one value while the process gets another is a
// cache key describing a step that never ran. So this runs `env` in a real
// container and checks every pair EffectiveEnv promised. The reverse
// direction is deliberately not asserted: the daemon and the shell add
// HOSTNAME, HOME, PWD and SHLVL of their own, and HOSTNAME is the container
// id, so a cache key built from those would never hit twice.
func TestEffectiveEnvIsWhatTheStepActuallyReceives(t *testing.T) {
	ex := newExecutor(t, plan.ExecutorSpec{Kind: plan.ExecutorContainer, Image: dockertest.Image})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	// A name the image also declares (PATH), a name it does not (CI), and a
	// value with an "=" in it, so a naive split on every "=" is caught too.
	declared := []string{"CI=1", "SENRO_EQ=a=b", "PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"}
	want, err := ex.EffectiveEnv(ctx, declared)
	if err != nil {
		t.Fatalf("EffectiveEnv: %v", err)
	}

	sb, err := ex.Sandbox(ctx, executor.SandboxSpec{StepID: "env", Attempt: 1})
	if err != nil {
		t.Fatalf("Sandbox: %v", err)
	}
	defer func() { _ = sb.Close(context.Background(), false) }()

	var out, errOut strings.Builder
	code, err := sb.Run(ctx, executor.Cmd{Args: []string{"env"}, Env: declared}, &out, &errOut)
	if err != nil || code != 0 {
		t.Fatalf("Run: exit %d, err %v, stderr %q", code, err, errOut.String())
	}
	actual := map[string]string{}
	for _, line := range strings.Split(out.String(), "\n") {
		if n, v, ok := strings.Cut(line, "="); ok {
			actual[n] = v
		}
	}
	if len(actual) == 0 {
		t.Fatalf("no environment came back from the container: %q", out.String())
	}
	for _, kv := range want {
		n, v, _ := strings.Cut(kv, "=")
		got, ok := actual[n]
		if !ok {
			t.Errorf("EffectiveEnv reports %q but the step's process has no %s at all; the cache key "+
				"describes an environment the step never ran with", kv, n)
			continue
		}
		if got != v {
			t.Errorf("%s: EffectiveEnv says %q, the step's process sees %q; the cache key and the "+
				"process disagree", n, v, got)
		}
	}
}

func TestAStepRunsInTheContainerAndSeesItsBindMountedWorkspace(t *testing.T) {
	ex := newExecutor(t, plan.ExecutorSpec{Kind: plan.ExecutorContainer, Image: dockertest.Image})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	ws := t.TempDir()
	if err := os.WriteFile(filepath.Join(ws, "in.txt"), []byte("payload\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	sb, err := ex.Sandbox(ctx, executor.SandboxSpec{
		StepID: "build", Attempt: 1, WorkDir: "/repo",
		Mounts: []executor.Mount{{Name: "src", Path: ws, At: "/repo"}},
	})
	if err != nil {
		t.Fatalf("Sandbox: %v", err)
	}
	defer func() { _ = sb.Close(context.Background(), false) }()

	var out, errOut strings.Builder
	code, err := sb.Run(ctx, executor.Cmd{
		Args: []string{"sh", "-c", "cat in.txt && echo made > out.txt"},
	}, &out, &errOut)
	if err != nil {
		t.Fatalf("Run: %v (stderr %q)", err, errOut.String())
	}
	if code != 0 {
		t.Fatalf("exit %d, stderr %q", code, errOut.String())
	}
	if out.String() != "payload\n" {
		t.Errorf("stdout = %q, want the workspace's file", out.String())
	}
	// The write landed on the HOST, which is what makes a bind mount worth
	// more than a volume: the next step, and a person debugging, both see it.
	if _, err := os.Stat(filepath.Join(ws, "out.txt")); err != nil {
		t.Errorf("the step's write did not reach the host workspace: %v", err)
	}
}

// The difference between the two executors this build ships, asserted:
// senro.RO's doc, senro.Mount's doc and the README all say the container
// executor enforces read-only, and this is what makes that sentence true.
func TestAReadOnlyMountIsEnforced(t *testing.T) {
	ex := newExecutor(t, plan.ExecutorSpec{Kind: plan.ExecutorContainer, Image: dockertest.Image})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	ws := t.TempDir()
	sb, err := ex.Sandbox(ctx, executor.SandboxSpec{
		StepID: "readonly", Attempt: 1,
		Mounts: []executor.Mount{{Name: "src", Path: ws, At: "/repo", RO: true}},
	})
	if err != nil {
		t.Fatalf("Sandbox: %v", err)
	}
	defer func() { _ = sb.Close(context.Background(), false) }()

	var out, errOut strings.Builder
	code, err := sb.Run(ctx, executor.Cmd{Args: []string{"sh", "-c", "touch /repo/written"}}, &out, &errOut)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if code == 0 {
		t.Fatal("a write through a read-only mount succeeded")
	}
	if _, err := os.Stat(filepath.Join(ws, "written")); err == nil {
		t.Fatal("the file reached the host through a read-only mount")
	}
}

// TestASecretIsAFileInTheContainerAndNowhereElse asserts the container
// secret contract's promises all at once: the step reads a file, the file is
// at the path the environment says, the VALUE is in no container
// configuration field, and nothing survives Close.
func TestASecretIsAFileInTheContainerAndNowhereElse(t *testing.T) {
	ex := newExecutor(t, plan.ExecutorSpec{Kind: plan.ExecutorContainer, Image: dockertest.Image})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	const value = "s3cret-value-not-in-inspect"
	sb, err := ex.Sandbox(ctx, executor.SandboxSpec{
		StepID: "deliver", Attempt: 1,
		Secrets: []executor.SecretRef{{Name: "NPMToken"}},
	})
	if err != nil {
		t.Fatalf("Sandbox: %v", err)
	}
	path, err := sb.PutSecret(ctx, "NPMToken", []byte(value))
	if err != nil {
		t.Fatalf("PutSecret: %v", err)
	}
	if !strings.HasPrefix(path, containerexec.SecretMountPath+"/") {
		t.Fatalf("PutSecret returned %q, which is not a path inside the container", path)
	}

	var out, errOut strings.Builder
	code, err := sb.Run(ctx, executor.Cmd{
		Args: []string{"sh", "-c", "cat \"$SENRO_SECRET_NPMTOKEN\""},
		Env:  []string{"SENRO_SECRET_NPMTOKEN=" + path},
	}, &out, &errOut)
	if err != nil || code != 0 {
		t.Fatalf("Run: exit %d, err %v, stderr %q", code, err, errOut.String())
	}
	if out.String() != value {
		t.Errorf("the step read %q, want the delivered value", out.String())
	}

	// The host file must keep secretdir's guarantee (0600 inside 0700), the
	// same one localexec's TestPutSecretWritesOutsideTheRunDirectory pins.
	hostFile := filepath.Join(
		sb.(interface{ HostSecretDir() string }).HostSecretDir(), secretdir.FileName("NPMToken"))
	fi, err := os.Stat(hostFile)
	if err != nil {
		t.Fatalf("stat host secret file: %v", err)
	}
	if fi.Mode().Perm() != 0o600 {
		t.Errorf("host secret file mode = %v, want 0600", fi.Mode().Perm())
	}

	// NOT filepath.Dir(...): HostSecretDir already IS the directory.
	// Wrapping it in Dir would check its PARENT (/tmp or /dev/shm), which
	// exists after Close regardless, so that assertion could never fail.
	hostDir := sb.(interface{ HostSecretDir() string }).HostSecretDir()
	if hostDir == "" {
		t.Fatal("HostSecretDir is empty; PutSecret should have created it")
	}
	if err := sb.Close(ctx, true); err != nil { // keep=true, and it still goes
		t.Fatalf("Close: %v", err)
	}
	if _, err := os.Stat(hostDir); err == nil {
		t.Error("the host secret directory survived Close(keep=true)")
	}
}

// The other leg of localexec's TestCloseWithNoSecretsCostsNothing: a step
// declaring no secrets must get no bind at SecretMountPath and leave no
// directory for Close to find. This pins that Sandbox does not call Ensure
// or Put on its own when SandboxSpec.Secrets is empty.
func TestASandboxWithNoSecretsCreatesNoSecretDirectory(t *testing.T) {
	ex := newExecutor(t, plan.ExecutorSpec{Kind: plan.ExecutorContainer, Image: dockertest.Image})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	sb, err := ex.Sandbox(ctx, executor.SandboxSpec{StepID: "nosecrets", Attempt: 1})
	if err != nil {
		t.Fatalf("Sandbox: %v", err)
	}
	defer func() { _ = sb.Close(context.Background(), false) }()

	if hostDir := sb.(interface{ HostSecretDir() string }).HostSecretDir(); hostDir != "" {
		t.Fatalf("Sandbox created a secret directory (%q) for a step declaring no secrets", hostDir)
	}
	if _, err := sb.Run(ctx, executor.Cmd{Args: []string{"sh", "-c", "exit 0"}}, io.Discard, io.Discard); err != nil {
		t.Fatal(err)
	}
	if err := sb.Close(ctx, false); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

// Greps the raw inspect document, byte for byte, for the secret's value:
// inspect must show only the bind's SOURCE path, and this would catch a
// future change that puts the value in Env, Cmd, or any other field Docker
// considers part of a container's configuration.
func TestASecretsValueNeverAppearsInDockerInspect(t *testing.T) {
	c := dockertest.Require(t)
	dockertest.Pull(t, c)
	store, err := cas.Open(filepath.Join(t.TempDir(), "cas"))
	if err != nil {
		t.Fatal(err)
	}
	ex, err := containerexec.New(
		plan.ExecutorSpec{Kind: plan.ExecutorContainer, Image: dockertest.Image},
		workspace.NewSnapshotter(store),
		containerexec.WithClient(c), containerexec.WithRunID("inspect-probe"))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	const value = "s3cret-must-not-leak-into-inspect"
	sb, err := ex.Sandbox(ctx, executor.SandboxSpec{
		StepID: "inspect", Attempt: 1,
		Secrets: []executor.SecretRef{{Name: "Token"}},
	})
	if err != nil {
		t.Fatalf("Sandbox: %v", err)
	}
	defer func() { _ = sb.Close(context.Background(), false) }()

	path, err := sb.PutSecret(ctx, "Token", []byte(value))
	if err != nil {
		t.Fatalf("PutSecret: %v", err)
	}

	code, err := sb.Run(ctx, executor.Cmd{
		Args: []string{"sh", "-c", "cat \"$SENRO_SECRET_TOKEN\" > /dev/null"},
		Env:  []string{"SENRO_SECRET_TOKEN=" + path},
	}, io.Discard, io.Discard)
	if err != nil || code != 0 {
		t.Fatalf("Run: exit %d, err %v", code, err)
	}

	id := sb.(interface{ ContainerID() string }).ContainerID()
	if id == "" {
		t.Fatal("ContainerID is empty after Run; Run should have created a container")
	}
	raw, err := c.ContainerInspectRaw(ctx, id)
	if err != nil {
		t.Fatalf("ContainerInspectRaw: %v", err)
	}
	if strings.Contains(string(raw), value) {
		t.Fatalf("the secret's plaintext VALUE appears in docker inspect output: %s", raw)
	}
}

func TestANonZeroExitIsTheWorkloadsVerdictAndNotAnInfraFailure(t *testing.T) {
	ex := newExecutor(t, plan.ExecutorSpec{Kind: plan.ExecutorContainer, Image: dockertest.Image})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	sb, err := ex.Sandbox(ctx, executor.SandboxSpec{StepID: "fail", Attempt: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sb.Close(context.Background(), false) }()

	code, err := sb.Run(ctx, executor.Cmd{Args: []string{"sh", "-c", "exit 7"}}, io.Discard, io.Discard)
	if err != nil {
		t.Fatalf("a non-zero exit reported an error: %v", err)
	}
	if code != 7 {
		t.Errorf("exit = %d, want 7", code)
	}
	if executor.IsInfra(err) {
		t.Error("a failing workload was classified as infrastructure; retry.OnInfra would retry it forever")
	}
}

func TestACancelledRunKillsTheContainerAndReportsInfra(t *testing.T) {
	ex := newExecutor(t, plan.ExecutorSpec{Kind: plan.ExecutorContainer, Image: dockertest.Image})
	sb, err := ex.Sandbox(context.Background(), executor.SandboxSpec{StepID: "slow", Attempt: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sb.Close(context.Background(), false) }()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	start := time.Now()
	_, err = sb.Run(ctx, executor.Cmd{Args: []string{"sh", "-c", "sleep 120"}}, io.Discard, io.Discard)
	if err == nil {
		t.Fatal("a cancelled step returned no error, so the engine would record it as a clean exit")
	}
	if !executor.IsInfra(err) {
		t.Errorf("err = %v, want an ErrInfra so runAttempt classifies it as cancelled", err)
	}
	if d := time.Since(start); d > 30*time.Second {
		t.Errorf("Run took %s after cancellation; the container was not killed", d)
	}
}

func TestARelativeMountPathIsRefused(t *testing.T) {
	ex := newExecutor(t, plan.ExecutorSpec{Kind: plan.ExecutorContainer, Image: dockertest.Image})
	_, err := ex.Sandbox(context.Background(), executor.SandboxSpec{
		StepID: "rel", Attempt: 1,
		Mounts: []executor.Mount{{Name: "src", Path: t.TempDir(), At: "repo"}},
	})
	if err == nil {
		t.Fatal("Sandbox accepted a relative mount path; the daemon would reject it far less clearly")
	}
	if !strings.Contains(err.Error(), "absolute") {
		t.Fatalf("the refusal does not say what is wrong: %v", err)
	}
}

// The working-directory half of the rule TestARelativeMountPathIsRefused
// pins for mounts: both are plain strings the pipeline author types,
// unvalidated at plan time, and the daemon's own 400 names neither the step
// nor the senro call that produced it. Refusing rather than resolving is
// deliberate: a container has no anchor to resolve against (see
// checkWorkDir).
func TestARelativeWorkingDirectoryIsRefused(t *testing.T) {
	ex := newExecutor(t, plan.ExecutorSpec{Kind: plan.ExecutorContainer, Image: dockertest.Image})
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	// The spec's own WorkDir, refused before a sandbox exists at all.
	_, err := ex.Sandbox(ctx, executor.SandboxSpec{StepID: "relwd", Attempt: 1, WorkDir: "build"})
	if err == nil {
		t.Fatal("Sandbox accepted a relative WorkDir")
	}
	if !executor.IsInfra(err) {
		t.Errorf("err = %v, want ErrInfra: a working directory is configuration, not a workload verdict", err)
	}
	for _, want := range []string{"relwd", "build", "absolute", "WorkDir"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the refusal does not mention %q, so it cannot be acted on: %v", want, err)
		}
	}

	// Cmd.Dir, which the engine sends separately and which is the only one
	// that exists for a step whose WorkDir a mount already realized.
	sb, err := ex.Sandbox(ctx, executor.SandboxSpec{StepID: "relcmddir", Attempt: 1})
	if err != nil {
		t.Fatalf("Sandbox: %v", err)
	}
	defer func() { _ = sb.Close(context.Background(), false) }()

	_, err = sb.Run(ctx, executor.Cmd{Args: []string{"pwd"}, Dir: "sub"}, io.Discard, io.Discard)
	if err == nil {
		t.Fatal("Run accepted a relative Cmd.Dir")
	}
	if !executor.IsInfra(err) {
		t.Errorf("err = %v, want ErrInfra", err)
	}
	for _, want := range []string{"relcmddir", "sub", "absolute"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the refusal does not mention %q: %v", want, err)
		}
	}
	// Refused before the daemon was asked, so there is no container to leak
	// and no create request to have paid for.
	if id := sb.(interface{ ContainerID() string }).ContainerID(); id != "" {
		t.Errorf("a container (%s) was created for a command that could never run", id)
	}
}

// Counts senro-labelled containers before and after, so a leak fails here
// rather than filling a developer's disk over a week. The label filter makes
// the count meaningful on a machine running other containers, and "before"
// and "after" must count the SAME label or the comparison proves nothing.
func TestNoContainerSurvivesAClosedSandbox(t *testing.T) {
	c := dockertest.Require(t)
	dockertest.Pull(t, c)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	runID := testRunID(t)
	before, err := c.ContainerList(ctx, map[string]string{"senro.run": runID})
	if err != nil {
		t.Fatal(err)
	}
	ex := newExecutor(t, plan.ExecutorSpec{Kind: plan.ExecutorContainer, Image: dockertest.Image})
	sb, err := ex.Sandbox(ctx, executor.SandboxSpec{StepID: "leak", Attempt: 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sb.Run(ctx, executor.Cmd{Args: []string{"sh", "-c", "exit 0"}}, io.Discard, io.Discard); err != nil {
		t.Fatal(err)
	}
	// The canary: this test can only prove absence if it first proved
	// presence, so it asserts the counter moved before asserting it came back.
	during, err := c.ContainerList(ctx, map[string]string{"senro.run": runID})
	if err != nil {
		t.Fatal(err)
	}
	if len(during) <= len(before) {
		t.Fatal("no labelled container existed even while the step was running; this test proves nothing")
	}
	if err := sb.Close(ctx, false); err != nil {
		t.Fatal(err)
	}
	after, err := c.ContainerList(ctx, map[string]string{"senro.run": runID})
	if err != nil {
		t.Fatal(err)
	}
	if len(after) != len(before) {
		// Name the survivors and dump what the daemon still thinks of them:
		// this fires rarely, so the message must carry enough to diagnose an
		// occurrence from the log alone.
		survivors := make(map[string]bool, len(after))
		for _, id := range after {
			survivors[id] = true
		}
		for _, id := range before {
			delete(survivors, id)
		}
		t.Errorf("%d container(s) survived Close (before=%d after=%d, label senro.run=%s)",
			len(after)-len(before), len(before), len(after), runID)
		for id := range survivors {
			raw, err := c.ContainerInspectRaw(ctx, id)
			if err != nil {
				t.Errorf("  survivor %s: inspect failed: %v (it may have been removed between the list and this call, which would make the removal merely slow rather than lost)", id, err)
				continue
			}
			t.Errorf("  survivor %s: %s", id, raw)
		}
	}
}

// No dockertest.Require: this proves the OPPOSITE case, a daemon that is
// not there, and must run the same whether or not this machine has one. A
// pipeline that cannot run should say so from New, before it has written
// half a run directory.
func TestNewFailsCleanlyWithNoDaemon(t *testing.T) {
	t.Setenv("DOCKER_HOST", "unix:///nonexistent/senro-test-no-daemon.sock")
	store, err := cas.Open(filepath.Join(t.TempDir(), "cas"))
	if err != nil {
		t.Fatal(err)
	}
	_, err = containerexec.New(
		plan.ExecutorSpec{Kind: plan.ExecutorContainer, Image: dockertest.Image},
		workspace.NewSnapshotter(store))
	if err == nil {
		t.Fatal("New succeeded with no daemon socket present")
	}
}

// A bad image name is a property of the pipeline's configuration, not of
// what the step's command did, so it must classify as infrastructure like an
// unreachable daemon: a transient registry blip looks identical and
// retry.OnInfra must be able to retry it.
func TestAnImageThatDoesNotExistFailsAsInfrastructure(t *testing.T) {
	c := dockertest.Require(t)
	store, err := cas.Open(filepath.Join(t.TempDir(), "cas"))
	if err != nil {
		t.Fatal(err)
	}
	ex, err := containerexec.New(
		plan.ExecutorSpec{Kind: plan.ExecutorContainer, Image: "senro-test/does-not-exist-2026:v0"},
		workspace.NewSnapshotter(store), containerexec.WithClient(c))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	_, err = ex.Class(ctx)
	if err == nil {
		t.Fatal("Class succeeded for an image no registry has")
	}
	if !executor.IsInfra(err) {
		t.Errorf("err = %v, want ErrInfra: a bad image reference is infrastructure, not a workload failure", err)
	}
}

// A command the image does not have is the PIPELINE's mistake, not the
// daemon's, and is reported as the shell's own 127.
//
// The daemon refuses /start for it, so there is no process and no exit code
// to read; senro supplies the code the rest of the world already uses. It
// has to: the ssh and k8s executors run their command through a shell and
// answer 127 for exactly this, so reporting infrastructure here would make
// the same pipeline mean two different things, and retry.OnInfra() would
// re-run a typo until its budget ran out on two executors of four. See
// dockerd.ClassifyStartFailure, and the cross-executor case in
// internal/executor/conformance.
func TestACommandTheImageDoesNotHaveIsTheWorkloadsVerdict(t *testing.T) {
	ex := newExecutor(t, plan.ExecutorSpec{Kind: plan.ExecutorContainer, Image: dockertest.Image})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	sb, err := ex.Sandbox(ctx, executor.SandboxSpec{StepID: "badexec", Attempt: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sb.Close(context.Background(), false) }()

	var errb strings.Builder
	exit, err := sb.Run(ctx,
		executor.Cmd{Args: []string{"/no/such/binary-senro-test"}}, io.Discard, &errb)
	if err != nil {
		t.Fatalf("Run: %v, want no error: a program the image lacks is the workload's verdict", err)
	}
	if exit != 127 {
		t.Errorf("exit = %d, want 127, the code every shell reports for a command it cannot find",
			exit)
	}
	// The container never ran, so it produced no output of its own: the
	// daemon's sentence is the only account of what was wrong with the name,
	// and losing it would leave a step that failed 127 with an empty log.
	if !strings.Contains(errb.String(), "/no/such/binary-senro-test") {
		t.Errorf("stderr = %q, want the daemon's account of the name it could not run",
			errb.String())
	}
}
