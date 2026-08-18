package k8sexec_test

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/xavidop/senro/internal/binprov"
	senroexec "github.com/xavidop/senro/internal/executor"
	"github.com/xavidop/senro/internal/executor/k8sexec"
)

// What a func step in a pod stands on, against a real cluster: the
// coordinator's own binary reaches a path inside the pod, runs there, is
// handed its input on stdin and answers on two streams that stay apart. The
// engine's half (the step child protocol) is one seam above this and is
// tested where it lives; everything below it is here.

// scriptBinary writes a shell script and calls it a binary: staging opens,
// stats, tars and executes the file, so a script is a truthful stand-in for
// all four with no cross-compile in the way. internal/engine's tests use a
// real one.
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

func TestAStagedBinaryIsNamedByItsDigestInsideThePod(t *testing.T) {
	ex := newExec(t)
	path, digest, size := scriptBinary(t, "#!/bin/sh\nexit 0\n")

	staged, err := ex.StageBinary(context.Background(), senroexec.StagedBinary{
		Digest: digest, Path: path, Size: size,
	})
	if err != nil {
		t.Fatalf("StageBinary: %v", err)
	}
	want := k8sexec.BinDir + "/senro-sha256-" + strings.TrimPrefix(digest, "sha256:")
	if staged.Path != want {
		t.Errorf("staged at %q, want %q", staged.Path, want)
	}
}

// Reused is false on every call, and that is the honest answer rather than a
// missing optimisation: a pod's filesystem does not outlive the pod and senro
// owns no cluster object to keep a copy in, so the binary crosses the
// apiserver once per attempt. An executor reporting true here would tell a
// reader of binary.staged that a transfer was avoided that was not.
func TestStagingReportsATransferBecauseEveryPodIsFresh(t *testing.T) {
	ex := newExec(t)
	path, digest, size := scriptBinary(t, "#!/bin/sh\nexit 0\n")

	for _, name := range []string{"first", "second"} {
		staged, err := ex.StageBinary(context.Background(), senroexec.StagedBinary{
			Digest: digest, Path: path, Size: size,
		})
		if err != nil {
			t.Fatalf("StageBinary (%s): %v", name, err)
		}
		if staged.Reused {
			t.Errorf("the %s staging reports that nothing moved, and every pod is a fresh "+
				"filesystem: the binary is sent again for each one", name)
		}
	}
}

func TestStagingRefusesABinaryWithNoDigest(t *testing.T) {
	ex := newExec(t)
	_, err := ex.StageBinary(context.Background(), senroexec.StagedBinary{Path: "/nope"})
	if err == nil || !strings.Contains(err.Error(), "digest") {
		t.Errorf("StageBinary with no digest returned %v, want a refusal naming the digest", err)
	}
}

// A file the coordinator cannot open is caught HERE rather than in the pod,
// where it would arrive as a tar of nothing and then as a command that does
// not exist, after a pod had been created and scheduled for it.
func TestStagingReportsAMissingCoordinatorSideFileAsInfrastructure(t *testing.T) {
	ex := newExec(t)
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

// The whole transport in one test: the binary is sent into the pod, is
// executable at the path senro reported, receives its input on stdin, and
// answers on two streams that STAY APART with an exit code of its own.
//
// The separation is the load-bearing half. A pod's log merges stdout and
// stderr (TestOutputFromBothStreamsSurvives records that for Run), and a step
// child writes length-prefixed frames on stdout and unframed diagnostics on
// stderr, so a merged pair would shred the protocol. Running the child
// through an exec rather than as the container's command is what buys this.
func TestAStagedBinaryRunsInThePodWithItsStreamsApart(t *testing.T) {
	ex := newExec(t)
	path, digest, size := scriptBinary(t, "#!/bin/sh\nread line\n"+
		"echo \"child-read:$line\"\necho child-diagnostic >&2\nexit 7\n")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	staged, err := ex.StageBinary(ctx, senroexec.StagedBinary{
		Digest: digest, Path: path, Size: size,
	})
	if err != nil {
		t.Fatalf("StageBinary: %v", err)
	}
	sb := sandbox(t, ex, senroexec.SandboxSpec{})

	var out, errOut bytes.Buffer
	exit, err := interactive(t, sb).RunInteractive(ctx,
		senroexec.Cmd{Args: []string{staged.Path}},
		strings.NewReader("state-on-stdin\n"), &out, &errOut)
	if err != nil {
		t.Fatalf("RunInteractive: %v (stdout=%q stderr=%q)", err, out.String(), errOut.String())
	}
	if exit != 7 {
		t.Errorf("exit = %d, want 7: the staged binary's own verdict is what comes back", exit)
	}
	if !strings.Contains(out.String(), "child-read:state-on-stdin") {
		t.Errorf("stdout = %q, want the line the binary read from its stdin: a step child reads "+
			"its whole state from there", out.String())
	}
	if !strings.Contains(errOut.String(), "child-diagnostic") {
		t.Errorf("stderr = %q, want the binary's own diagnostic", errOut.String())
	}
	if strings.Contains(out.String(), "child-diagnostic") {
		t.Errorf("stderr arrived on stdout (%q), which would shred a step child's frames", out.String())
	}
}

// A pod that is not running the staged binary does not get it. The transfer
// puts senro's own executable inside an image somebody else built, so it
// happens for the command that IS that binary and for nothing else.
func TestAnOrdinaryStepDoesNotGetTheStagedBinaryInItsPod(t *testing.T) {
	ex := newExec(t)
	path, digest, size := scriptBinary(t, "#!/bin/sh\nexit 0\n")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	if _, err := ex.StageBinary(ctx, senroexec.StagedBinary{
		Digest: digest, Path: path, Size: size,
	}); err != nil {
		t.Fatalf("StageBinary: %v", err)
	}
	sb := sandbox(t, ex, senroexec.SandboxSpec{})

	exit, out, err := run(t, sb, "sh", "-c", "ls "+k8sexec.BinDir+" 2>&1 || true")
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if exit != 0 {
		t.Fatalf("the probe exited %d: %s", exit, out)
	}
	if strings.Contains(out, "senro-sha256-") {
		t.Errorf("an ordinary step's pod can see the staged binary: %q", out)
	}
}

// A re-entered func step reaches a file through ctx.Workspace(name), and that
// has to be the path INSIDE the pod: Mount.Path is a directory on the
// coordinator, which nothing in the cluster can open.
func TestASandboxReportsWhereEachMountLandedInsideThePod(t *testing.T) {
	sb := sandbox(t, newExec(t), senroexec.SandboxSpec{
		Mounts: []senroexec.Mount{{Name: "src", Path: t.TempDir(), At: "/src"}},
	})

	loc, ok := sb.(senroexec.MountLocator)
	if !ok {
		t.Fatal("a pod sandbox cannot say where it put a mount, so a func step there could not " +
			"be told where its workspaces are")
	}
	got, ok := loc.MountPath("src")
	if !ok || got != "/src" {
		t.Errorf("MountPath(src) = %q, %v; want the path inside the pod", got, ok)
	}
	if _, ok := loc.MountPath("nope"); ok {
		t.Error("MountPath answered for a mount the step never declared")
	}
}
