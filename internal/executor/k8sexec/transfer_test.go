package k8sexec_test

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	senroexec "github.com/xavidop/senro/internal/executor"
	"github.com/xavidop/senro/internal/storage"
	"github.com/xavidop/senro/internal/workspace"
)

// Every test below is one half of the claim that a k8s step can receive a
// workspace from an earlier step and hand one to a later one, against a
// real cluster.

func write(t *testing.T, path, body string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
}

func read(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path) // #nosec G304 -- a path this test just wrote
	if err != nil {
		t.Fatalf("reading %s: %v", path, err)
	}
	return string(b)
}

// TestAWorkspaceRoundTripsThroughThePod is the whole tranche in one test, and
// it is deliberately the same test sshexec.TestAWorkspaceRoundTripsThroughTheHost
// is: a real file goes into the pod, the step reads it, changes the directory,
// and the change comes back with a digest that describes what is there.
func TestAWorkspaceRoundTripsThroughThePod(t *testing.T) {
	ex := newExec(t)
	ws := t.TempDir()
	write(t, filepath.Join(ws, "sent.txt"), "from the coordinator\n")
	write(t, filepath.Join(ws, "nested", "deep.txt"), "nested\n")
	write(t, filepath.Join(ws, "run.sh"), "#!/bin/sh\necho hi\n")
	if err := os.Chmod(filepath.Join(ws, "run.sh"), 0o755); err != nil {
		t.Fatalf("chmod: %v", err)
	}

	mount := senroexec.Mount{Name: "src", Path: ws, At: "/src"}
	sb := sandbox(t, ex, senroexec.SandboxSpec{
		Mounts: []senroexec.Mount{mount}, WorkDir: "/src",
	})

	exit, out, err := run(t, sb, "sh", "-c",
		"cat /src/sent.txt /src/nested/deep.txt; test -x /src/run.sh || exit 3; "+
			"echo 'from the pod' > /src/produced.txt; rm /src/sent.txt")
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if exit != 0 {
		t.Fatalf("exit = %d: %s", exit, out)
	}
	if !strings.Contains(out, "from the coordinator") || !strings.Contains(out, "nested") {
		t.Errorf("the pod did not see the files it was sent: %q", out)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	snap, err := sb.Snapshot(ctx, "src")
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if snap.Digest == "" {
		t.Error("Snapshot reported no digest")
	}

	// The coordinator's directory is now exactly what the pod held: the file
	// the step wrote is here, the file it deleted is gone, and the executable
	// bit survived both directions.
	if got := read(t, filepath.Join(ws, "produced.txt")); strings.TrimSpace(got) != "from the pod" {
		t.Errorf("produced.txt = %q, want what the step in the pod wrote", got)
	}
	if _, err := os.Stat(filepath.Join(ws, "sent.txt")); !os.IsNotExist(err) {
		t.Error("a file the step deleted is still in the coordinator's copy, so the recorded " +
			"digest does not describe this directory")
	}
	fi, err := os.Stat(filepath.Join(ws, "run.sh"))
	if err != nil {
		t.Fatalf("run.sh did not come back: %v", err)
	}
	if fi.Mode().Perm()&0o111 == 0 {
		t.Errorf("run.sh came back as %v, want the executable bit preserved", fi.Mode().Perm())
	}
}

// TestAnEmptyWorkspaceCrossesAndComesBack is the shape the first step of every
// pipeline mounts, and the one that exercises a tar stream with no entries at
// all: 1024 zero bytes, which some tar implementations call a corrupt archive
// rather than an empty one.
func TestAnEmptyWorkspaceCrossesAndComesBack(t *testing.T) {
	ex := newExec(t)
	ws := t.TempDir()

	sb := sandbox(t, ex, senroexec.SandboxSpec{
		Mounts: []senroexec.Mount{{Name: "src", Path: ws, At: "/src"}},
	})
	exit, out, err := run(t, sb, "sh", "-c", "echo made-here > /src/out.txt")
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if exit != 0 {
		t.Fatalf("exit = %d: %s", exit, out)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	if _, err := sb.Snapshot(ctx, "src"); err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if got := read(t, filepath.Join(ws, "out.txt")); strings.TrimSpace(got) != "made-here" {
		t.Errorf("out.txt = %q, want what the step wrote into an empty workspace", got)
	}
}

// TestARoundTripReproducesTheDigestOfTheSameContent is the property that makes
// the digest worth having. It must be the digest the SAME content has locally,
// or a k8s step's output would never match a local step's and the cache would
// never hit across executors.
//
// It is also what pins the digest to what came BACK rather than to what was
// sent: the snapshot is taken from the copy read out of the pod, so the
// coordinator's directory afterwards is exactly what the digest describes.
func TestARoundTripReproducesTheDigestOfTheSameContent(t *testing.T) {
	ex := newExec(t)
	ws := t.TempDir()
	write(t, filepath.Join(ws, "a.txt"), "alpha\n")
	write(t, filepath.Join(ws, "d", "b.txt"), "beta\n")

	store, err := storage.Open(filepath.Join(t.TempDir(), "store2"))
	if err != nil {
		t.Fatalf("storage.Open: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	local, err := store.Snapshotter.Snapshot(context.Background(), ws,
		workspace.NewExcluder(workspace.DefaultExcludesFor(false)...))
	if err != nil {
		t.Fatalf("local snapshot: %v", err)
	}

	sb := sandbox(t, ex, senroexec.SandboxSpec{
		Mounts: []senroexec.Mount{{Name: "src", Path: ws, At: "/src"}},
	})
	if _, _, err := run(t, sb, "true"); err != nil {
		t.Fatalf("Run: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	remote, err := sb.Snapshot(ctx, "src")
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if remote.Digest != string(local.Digest) {
		t.Errorf("a workspace that crossed into a pod unchanged digests as %s, and the same "+
			"content locally digests as %s", remote.Digest, local.Digest)
	}
}

// A read-only mount is read back so the engine's breach check has something
// to compare, and NOT swapped, so senro does not carry a breach home. This
// executor really does enforce read-only through the volumeMount, so the
// step's write fails inside the pod; the snapshot still has to happen, or a
// breach would be invisible the day the enforcement loosened.
func TestAReadOnlyMountIsSnapshottedButNotSwappedBack(t *testing.T) {
	ex := newExec(t)
	ws := t.TempDir()
	write(t, filepath.Join(ws, "input.txt"), "original\n")

	sb := sandbox(t, ex, senroexec.SandboxSpec{
		Mounts: []senroexec.Mount{{Name: "src", Path: ws, At: "/src", RO: true}},
	})
	if _, _, err := run(t, sb, "sh", "-c",
		"cat /src/input.txt; echo tampered > /src/input.txt || true"); err != nil {
		t.Fatalf("Run: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	snap, err := sb.Snapshot(ctx, "src")
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if snap.Digest == "" {
		t.Fatal("a read-only mount produced no digest, so the engine's breach check has nothing " +
			"to compare")
	}
	if got := read(t, filepath.Join(ws, "input.txt")); strings.TrimSpace(got) != "original" {
		t.Errorf("the coordinator's read-only copy was replaced from the pod: %q", got)
	}
}

// TestTwoWorkspacesEachLandInTheirOwnDirectory. One pod, two mounts, and
// nothing of one in the other. The volumes are named positionally inside the
// pod spec and staged under a private root, so a bug that crossed the wires
// would show up here and nowhere else.
func TestTwoWorkspacesEachLandInTheirOwnDirectory(t *testing.T) {
	ex := newExec(t)
	first, second := t.TempDir(), t.TempDir()
	write(t, filepath.Join(first, "one.txt"), "first\n")
	write(t, filepath.Join(second, "two.txt"), "second\n")

	sb := sandbox(t, ex, senroexec.SandboxSpec{
		Mounts: []senroexec.Mount{
			{Name: "a", Path: first, At: "/a"},
			{Name: "b", Path: second, At: "/b"},
		},
	})
	exit, out, err := run(t, sb, "sh", "-c",
		"cat /a/one.txt /b/two.txt; test ! -e /a/two.txt && test ! -e /b/one.txt; "+
			"echo done-a > /a/made.txt; echo done-b > /b/made.txt")
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if exit != 0 {
		t.Fatalf("exit = %d: %s", exit, out)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	for _, name := range []string{"a", "b"} {
		if _, err := sb.Snapshot(ctx, name); err != nil {
			t.Fatalf("Snapshot(%s): %v", name, err)
		}
	}
	if got := read(t, filepath.Join(first, "made.txt")); strings.TrimSpace(got) != "done-a" {
		t.Errorf("workspace a came back with %q", got)
	}
	if got := read(t, filepath.Join(second, "made.txt")); strings.TrimSpace(got) != "done-b" {
		t.Errorf("workspace b came back with %q", got)
	}
	if _, err := os.Stat(filepath.Join(first, "two.txt")); !os.IsNotExist(err) {
		t.Error("a file from workspace b came back inside workspace a")
	}
}

// A mount that already carries a digest is the ordinary case: a workspace
// an earlier step filled.
func TestAWorkspaceWithContentIsAcceptedAndDelivered(t *testing.T) {
	ex := newExec(t)
	ws := t.TempDir()
	write(t, filepath.Join(ws, "from-an-earlier-step.txt"), "carried\n")

	sb := sandbox(t, ex, senroexec.SandboxSpec{
		Mounts: []senroexec.Mount{{
			Name: "repo", Path: ws, At: "/repo",
			Digest: "sha256:" + strings.Repeat("b", 64),
		}},
	})
	exit, out, err := run(t, sb, "cat", "/repo/from-an-earlier-step.txt")
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if exit != 0 {
		t.Fatalf("exit = %d: %s", exit, out)
	}
	if !strings.Contains(out, "carried") {
		t.Errorf("the step read %q, want the content the earlier step left", out)
	}
}

// TestAWorkspaceSenroCannotReadFailsTheStepAndNamesIt is the other end of
// kubeapi's TestAStdinSourceThatFailsEndsTheCommandRatherThanHangingIt, seen
// from the executor: when the coordinator cannot read what it is meant to
// send, the step fails promptly with an error naming the workspace, rather
// than the tar waiting inside the pod holding the step open until its own
// timeout.
func TestAWorkspaceSenroCannotReadFailsTheStepAndNamesIt(t *testing.T) {
	ex := newExec(t)
	// A path that passes Sandbox's checks and is not there when the transfer
	// reads it, which is what a workspace directory removed under senro looks
	// like from here.
	missing := filepath.Join(t.TempDir(), "not-a-directory")

	sb := sandbox(t, ex, senroexec.SandboxSpec{
		Mounts: []senroexec.Mount{{Name: "src", Path: missing, At: "/src"}},
	})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	var out bytes.Buffer
	start := time.Now()
	_, err := sb.Run(ctx, senroexec.Cmd{Args: []string{"true"}}, &out, &out)
	if err == nil {
		t.Fatal("a step whose workspace could not be read reported success")
	}
	if !senroexec.IsInfra(err) {
		t.Errorf("the failure does not carry ErrInfra: %v", err)
	}
	if !strings.Contains(err.Error(), "src") {
		t.Errorf("the failure does not name the workspace: %v", err)
	}
	if ctx.Err() != nil {
		t.Errorf("the step waited for its context (%s) instead of failing when its workspace "+
			"could not be read", time.Since(start))
	}
}

// A SCRATCH cache crosses into the pod and comes back through ReadMount,
// which is what lets the engine save the bytes the pod left rather than the
// copy it sent in. Deliberately the same test
// sshexec.TestAScratchCacheCrossesToTheHostAndComesBack is.
//
// Three claims in one round trip: what was restored on the coordinator
// reaches the pod, what the step added comes back, and the coordinator's own
// directory is left exactly as it was, because a sibling step may be tarring
// that directory out at the same moment.
func TestAScratchCacheCrossesIntoThePodAndComesBack(t *testing.T) {
	ex := newExec(t)
	cache := t.TempDir()
	write(t, filepath.Join(cache, "restored.txt"), "from an earlier run\n")
	// node_modules and .git are the mandatory excludes for a workspace, and
	// must NOT apply to a scratch cache: one is usually node_modules.
	write(t, filepath.Join(cache, "node_modules", "left-pad", "index.js"), "module.exports = 1\n")

	sb := sandbox(t, ex, senroexec.SandboxSpec{
		Mounts: []senroexec.Mount{{Name: "deps", Path: cache, At: "/cache", Scratch: true}},
	})
	exit, out, err := run(t, sb, "sh", "-c",
		"cat /cache/restored.txt /cache/node_modules/left-pad/index.js; "+
			"echo downloaded > /cache/fetched.txt")
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if exit != 0 {
		t.Fatalf("exit = %d: %s", exit, out)
	}
	if !strings.Contains(out, "from an earlier run") || !strings.Contains(out, "module.exports") {
		t.Errorf("the pod received a scratch cache missing its own contents: %q", out)
	}

	reader, ok := sb.(senroexec.MountReader)
	if !ok {
		t.Fatal("a k8s sandbox cannot read a mount back, so a scratch cache here could only be " +
			"saved from the coordinator's own stale copy")
	}
	dest := filepath.Join(t.TempDir(), "readback")
	if err := os.MkdirAll(dest, 0o755); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	if err := reader.ReadMount(ctx, "deps", dest); err != nil {
		t.Fatalf("ReadMount: %v", err)
	}
	if got := read(t, filepath.Join(dest, "fetched.txt")); strings.TrimSpace(got) != "downloaded" {
		t.Errorf("fetched.txt = %q, want what the step in the pod left in the cache", got)
	}
	if _, err := os.Stat(filepath.Join(dest, "node_modules", "left-pad", "index.js")); err != nil {
		t.Errorf("node_modules did not come back out of the pod: %v", err)
	}
	if _, err := os.Stat(filepath.Join(cache, "fetched.txt")); !os.IsNotExist(err) {
		t.Error("ReadMount wrote into the coordinator's own cache directory; a sibling step may " +
			"be tarring that directory out at this moment")
	}
}

// TestSnapshotNamesAMountItDoesNotHave keeps the one refusal that is still
// true: a name no mount carries has no directory to read, and inventing a
// digest for it would put a confident wrong answer in the ledger.
func TestSnapshotNamesAMountItDoesNotHave(t *testing.T) {
	sb := sandbox(t, newExec(t), senroexec.SandboxSpec{
		Mounts: []senroexec.Mount{{Name: "build", Path: t.TempDir(), At: "/work"}},
	})
	_, err := sb.Snapshot(context.Background(), "not-a-mount")
	if err == nil {
		t.Fatal("Snapshot returned a digest for a mount that does not exist")
	}
	if !senroexec.IsInfra(err) {
		t.Errorf("the refusal does not carry ErrInfra: %v", err)
	}
}

// TestTheStepSeesItsWorkspaceBeforeItsFirstInstruction is the ordering the
// whole design rests on. Content arrives through an init container, and an
// init container finishing is what lets the step's own container start, so
// there is no window in which the step could observe an empty directory.
//
// The step's very first command is the read, so a transfer that happened
// concurrently would fail here rather than pass by a margin.
func TestTheStepSeesItsWorkspaceBeforeItsFirstInstruction(t *testing.T) {
	ex := newExec(t)
	ws := t.TempDir()
	write(t, filepath.Join(ws, "must-be-there"), "yes\n")

	sb := sandbox(t, ex, senroexec.SandboxSpec{
		Mounts: []senroexec.Mount{{Name: "src", Path: ws, At: "/src"}},
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	var out bytes.Buffer
	exit, err := sb.Run(ctx, senroexec.Cmd{Args: []string{"cat", "/src/must-be-there"}}, &out, &out)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if exit != 0 {
		t.Fatalf("the step's first instruction could not read its workspace: exit %d, %q",
			exit, out.String())
	}
}
