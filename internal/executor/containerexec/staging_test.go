package containerexec_test

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/xavidop/senro/internal/binprov"
	"github.com/xavidop/senro/internal/dockerd/dockertest"
	senroexec "github.com/xavidop/senro/internal/executor"
	"github.com/xavidop/senro/internal/executor/containerexec"
	"github.com/xavidop/senro/internal/plan"
)

// A staged binary on this executor is a READ-ONLY BIND of the coordinator's
// own file, so the whole of this file is about a path inside the container
// naming a file that never moved.

// scriptBinary writes a shell script and calls it a binary: staging only
// opens, stats and binds the file, so a script is a truthful stand-in with
// no cross-compile. internal/engine's end-to-end tests use a real one.
func scriptBinary(t *testing.T, body string) (path, digest string, size int64) {
	t.Helper()
	path = filepath.Join(t.TempDir(), "senro-fake")
	if err := os.WriteFile(path, []byte(body), 0o755); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	d, err := binprov.Digest(path)
	if err != nil {
		t.Fatalf("binprov.Digest: %v", err)
	}
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	return path, d, fi.Size()
}

func TestAStagedBinaryIsNamedByItsDigestInsideTheContainer(t *testing.T) {
	ex := newExecutor(t, plan.ExecutorSpec{Kind: plan.ExecutorContainer, Image: dockertest.Image})
	path, digest, size := scriptBinary(t, "#!/bin/sh\nexit 0\n")

	staged, err := ex.StageBinary(context.Background(), senroexec.StagedBinary{
		Digest: digest, Path: path, Size: size,
	})
	if err != nil {
		t.Fatalf("StageBinary: %v", err)
	}
	want := containerexec.BinDir + "/senro-sha256-" + strings.TrimPrefix(digest, "sha256:")
	if staged.Path != want {
		t.Errorf("staged at %q, want %q", staged.Path, want)
	}
}

// Nothing is transferred, ever: the daemon shares this filesystem by
// requirement, so the file the coordinator holds is the file the container
// runs, and reporting a transfer would describe a copy senro never makes.
func TestStagingTransfersNothingBecauseTheDaemonSharesThisFilesystem(t *testing.T) {
	ex := newExecutor(t, plan.ExecutorSpec{Kind: plan.ExecutorContainer, Image: dockertest.Image})
	path, digest, size := scriptBinary(t, "#!/bin/sh\nexit 0\n")

	for i, name := range []string{"first", "second"} {
		staged, err := ex.StageBinary(context.Background(), senroexec.StagedBinary{
			Digest: digest, Path: path, Size: size,
		})
		if err != nil {
			t.Fatalf("StageBinary (%s): %v", name, err)
		}
		if !staged.Reused {
			t.Errorf("the %s staging reports a transfer (call %d); a bind mount copies nothing",
				name, i+1)
		}
	}
}

// The staged path is executable in a container this executor creates, which is
// the only claim that matters: a path senro reports and cannot run is worse
// than a refusal.
func TestAStagedBinaryRunsInsideTheContainerAtTheStagedPath(t *testing.T) {
	ex := newExecutor(t, plan.ExecutorSpec{Kind: plan.ExecutorContainer, Image: dockertest.Image})
	path, digest, size := scriptBinary(t, "#!/bin/sh\necho staged-binary-ran\n")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	staged, err := ex.StageBinary(ctx, senroexec.StagedBinary{Digest: digest, Path: path, Size: size})
	if err != nil {
		t.Fatalf("StageBinary: %v", err)
	}
	sb, err := ex.Sandbox(ctx, senroexec.SandboxSpec{StepID: "s", Attempt: 1})
	if err != nil {
		t.Fatalf("Sandbox: %v", err)
	}
	defer func() { _ = sb.Close(context.Background(), false) }()

	var out, errb bytes.Buffer
	exit, err := sb.Run(ctx, senroexec.Cmd{Args: []string{staged.Path}}, &out, &errb)
	if err != nil {
		t.Fatalf("Run: %v (stderr=%s)", err, errb.String())
	}
	if exit != 0 {
		t.Fatalf("the staged binary exited %d: %s%s", exit, out.String(), errb.String())
	}
	if !strings.Contains(out.String(), "staged-binary-ran") {
		t.Errorf("stdout = %q, want the staged binary's own output", out.String())
	}
}

// A container that is not running the staged binary does not get it. The bind
// is the one thing that puts senro's own executable inside somebody else's
// image, so it appears for the command that IS that binary and for nothing
// else.
func TestAnOrdinaryStepDoesNotGetTheStagedBinaryBoundIntoIt(t *testing.T) {
	ex := newExecutor(t, plan.ExecutorSpec{Kind: plan.ExecutorContainer, Image: dockertest.Image})
	path, digest, size := scriptBinary(t, "#!/bin/sh\nexit 0\n")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	if _, err := ex.StageBinary(ctx, senroexec.StagedBinary{
		Digest: digest, Path: path, Size: size,
	}); err != nil {
		t.Fatalf("StageBinary: %v", err)
	}
	sb, err := ex.Sandbox(ctx, senroexec.SandboxSpec{StepID: "s", Attempt: 1})
	if err != nil {
		t.Fatalf("Sandbox: %v", err)
	}
	defer func() { _ = sb.Close(context.Background(), false) }()

	var out, errb bytes.Buffer
	exit, err := sb.Run(ctx, senroexec.Cmd{
		Args: []string{"sh", "-c", "ls " + containerexec.BinDir + " 2>&1 || true"},
	}, &out, &errb)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if exit != 0 {
		t.Fatalf("the probe exited %d: %s%s", exit, out.String(), errb.String())
	}
	if strings.Contains(out.String()+errb.String(), "senro-sha256-") {
		t.Errorf("an ordinary step can see the staged binary: %q", out.String()+errb.String())
	}
}

func TestStagingRefusesABinaryWithNoDigest(t *testing.T) {
	ex := newExecutor(t, plan.ExecutorSpec{Kind: plan.ExecutorContainer, Image: dockertest.Image})
	_, err := ex.StageBinary(context.Background(), senroexec.StagedBinary{Path: "/nope"})
	if err == nil || !strings.Contains(err.Error(), "digest") {
		t.Errorf("StageBinary with no digest returned %v, want a refusal naming the digest", err)
	}
}

// A file the coordinator cannot open is caught HERE rather than by the daemon,
// which answers a missing bind source by creating a directory at it: the step
// would then start and fail with "permission denied" on a path that is not
// even a file.
func TestStagingReportsAMissingCoordinatorSideFileAsInfrastructure(t *testing.T) {
	ex := newExecutor(t, plan.ExecutorSpec{Kind: plan.ExecutorContainer, Image: dockertest.Image})
	_, err := ex.StageBinary(context.Background(), senroexec.StagedBinary{
		Digest: "sha256:" + strings.Repeat("a", 64),
		Path:   filepath.Join(t.TempDir(), "does-not-exist"),
		Size:   1,
	})
	if err == nil {
		t.Fatal("StageBinary of a missing file succeeded")
	}
	if !senroexec.IsInfra(err) {
		t.Errorf("StageBinary of a missing file returned %v, want an infrastructure failure", err)
	}
}

// A remote func step reaches a file through ctx.Workspace(name), and that has
// to be the path INSIDE the container rather than the coordinator's own
// directory on the other side of the bind.
func TestASandboxReportsWhereEachMountLandedInsideTheContainer(t *testing.T) {
	ex := newExecutor(t, plan.ExecutorSpec{Kind: plan.ExecutorContainer, Image: dockertest.Image})
	src := t.TempDir()
	sb, err := ex.Sandbox(context.Background(), senroexec.SandboxSpec{
		StepID: "s", Attempt: 1,
		Mounts: []senroexec.Mount{{Name: "src", Path: src, At: "/src"}},
	})
	if err != nil {
		t.Fatalf("Sandbox: %v", err)
	}
	defer func() { _ = sb.Close(context.Background(), false) }()

	loc, ok := sb.(senroexec.MountLocator)
	if !ok {
		t.Fatal("a container sandbox cannot say where it put a mount")
	}
	got, ok := loc.MountPath("src")
	if !ok || got != "/src" {
		t.Errorf("MountPath(src) = %q, %v; want the path inside the container", got, ok)
	}
	if _, ok := loc.MountPath("nope"); ok {
		t.Error("MountPath answered for a mount the step never declared")
	}
}
