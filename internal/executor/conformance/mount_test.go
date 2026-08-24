package conformance_test

import (
	"context"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/xavidop/senro/internal/cas"
	senroexec "github.com/xavidop/senro/internal/executor"
	"github.com/xavidop/senro/internal/executor/mountsnap"
	"github.com/xavidop/senro/internal/workspace"
)

// seed builds a coordinator-side workspace directory, snapshots it, and
// returns the mount every executor is handed for it: Path for the ones that
// share this filesystem, Digest for the ones that do not. That pairing is
// exactly what engine.wsManager.mounts builds, so a divergence here is a
// divergence a real run would hit.
func seed(
	t *testing.T, snap *workspace.Snapshotter, name, at string, write func(dir string),
) senroexec.Mount {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "ws-"+name)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	write(dir)

	m := senroexec.Mount{Name: name, Path: dir, At: at}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	s, err := mountsnap.Snapshot(ctx, snap, m)
	if err != nil {
		t.Fatalf("seeding %q: %v", name, err)
	}
	m.Digest = s.Digest
	return m
}

func snapshotOf(t *testing.T, sb senroexec.Sandbox, name string) senroexec.Snapshot {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	s, err := sb.Snapshot(ctx, name)
	if err != nil {
		t.Fatalf("Snapshot(%q): %v", name, err)
	}
	return s
}

func write(t *testing.T, path, content string, mode os.FileMode) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), mode); err != nil {
		t.Fatal(err)
	}
	// WriteFile respects the umask, and the executable bit is what half of
	// these cases are about.
	if err := os.Chmod(path, mode); err != nil {
		t.Fatal(err)
	}
}

// TestEveryExecutorRealizesAMountedWorkspaceIdentically is the transfer
// promise: whatever the coordinator holds, the step sees, byte for byte and
// mode for mode, whether the executor binds the directory or streams a tar
// of it across a network.
func TestEveryExecutorRealizesAMountedWorkspaceIdentically(t *testing.T) {
	for _, tg := range targets() {
		t.Run(tg.name, func(t *testing.T) {
			t.Parallel()
			ex, snap := tg.new(t)

			m := seed(t, snap, "src", "/ws", func(dir string) {
				write(t, filepath.Join(dir, "plain.txt"), "hello\n", 0o644)
				write(t, filepath.Join(dir, "run.sh"), "#!/bin/sh\necho ran\n", 0o755)
				write(t, filepath.Join(dir, "nested", "deep", "file.txt"), "deep\n", 0o644)
				write(t, filepath.Join(dir, ".dotfile"), "dot\n", 0o644)
				write(t, filepath.Join(dir, "with space.txt"), "spaced\n", 0o644)
				write(t, filepath.Join(dir, "日本語.txt"), "unicode\n", 0o644)
				write(t, filepath.Join(dir, "empty.txt"), "", 0o644)
				if err := os.MkdirAll(filepath.Join(dir, "emptydir"), 0o755); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink("plain.txt", filepath.Join(dir, "link.txt")); err != nil {
					t.Fatal(err)
				}
			})

			// WorkDir is the mount point, which is how a pipeline that says
			// Mount(src.At("/repo")).WorkDir("/repo") reaches its files on
			// every executor: localexec realizes At under the sandbox rather
			// than at the absolute path, and sshexec under the attempt's own
			// directory, so an absolute path in the script would only work on
			// two of the four. The step's cwd is the mount everywhere.
			sb := sandboxOn(t, ex, senroexec.SandboxSpec{
				StepID: "realize", WorkDir: "/ws", Mounts: []senroexec.Mount{m},
			})

			// One script, so a divergence is one diff rather than N failures.
			script := `
printf 'plain=[%s]\n' "$(cat plain.txt)"
printf 'nested=[%s]\n' "$(cat nested/deep/file.txt)"
printf 'dotfile=[%s]\n' "$(cat .dotfile)"
printf 'spaced=[%s]\n' "$(cat 'with space.txt')"
printf 'unicode=[%s]\n' "$(cat '日本語.txt')"
printf 'emptysize=[%s]\n' "$(wc -c < empty.txt | tr -d ' ')"
if [ -x run.sh ]; then printf 'exec=[yes]\n'; else printf 'exec=[no]\n'; fi
if [ -d emptydir ]; then printf 'emptydir=[yes]\n'; else printf 'emptydir=[no]\n'; fi
if [ -L link.txt ]; then printf 'symlink=[yes]\n'; else printf 'symlink=[no]\n'; fi
printf 'linktarget=[%s]\n' "$(readlink link.txt 2>/dev/null)"
`
			exit, stdout, stderr, err := runOn(t, sb, senroexec.Cmd{
				Args: []string{tg.shell, "-c", script},
				Dir:  senroexec.CmdDir("/ws", []senroexec.Mount{m}),
			})
			if err != nil {
				t.Fatalf("Run: %v (stderr: %s)", err, stderr)
			}
			if exit != 0 {
				t.Fatalf("exit = %d, want 0 (stdout: %s stderr: %s)", exit, stdout, stderr)
			}

			want := strings.Join([]string{
				"plain=[hello]",
				"nested=[deep]",
				"dotfile=[dot]",
				"spaced=[spaced]",
				"unicode=[unicode]",
				"emptysize=[0]",
				"exec=[yes]",
				"emptydir=[yes]",
				"symlink=[yes]",
				"linktarget=[plain.txt]",
				"",
			}, "\n")
			if stdout != want {
				t.Errorf("the mounted workspace is not what the coordinator holds.\n got:\n%s\nwant:\n%s",
					stdout, want)
			}
		})
	}
}

// TestSnapshottingAnUntouchedWorkspaceReturnsTheDigestItWasSeededWith is the
// round trip: an executor that mangled a mode, a symlink or an empty
// directory on the way over and back would return a different content
// address for content nothing changed, and every downstream cache key would
// move for a reason nobody can see.
func TestSnapshottingAnUntouchedWorkspaceReturnsTheDigestItWasSeededWith(t *testing.T) {
	for _, tg := range targets() {
		t.Run(tg.name, func(t *testing.T) {
			t.Parallel()
			ex, snap := tg.new(t)

			m := seed(t, snap, "src", "/ws", func(dir string) {
				write(t, filepath.Join(dir, "plain.txt"), "hello\n", 0o644)
				write(t, filepath.Join(dir, "run.sh"), "#!/bin/sh\n", 0o755)
				write(t, filepath.Join(dir, "nested", "deep", "file.txt"), "deep\n", 0o644)
				write(t, filepath.Join(dir, ".dotfile"), "dot\n", 0o644)
				write(t, filepath.Join(dir, "empty.txt"), "", 0o644)
				if err := os.MkdirAll(filepath.Join(dir, "emptydir"), 0o755); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink("plain.txt", filepath.Join(dir, "link.txt")); err != nil {
					t.Fatal(err)
				}
			})
			seeded := m.Digest

			sb := sandboxOn(t, ex, senroexec.SandboxSpec{
				StepID: "roundtrip", Mounts: []senroexec.Mount{m},
			})
			exit, _, stderr, err := runOn(t, sb, senroexec.Cmd{
				Args: []string{tg.shell, "-c", "true"},
			})
			if err != nil || exit != 0 {
				t.Fatalf("Run: exit=%d err=%v (stderr: %s)", exit, err, stderr)
			}

			got := snapshotOf(t, sb, "src")
			if got.Digest != seeded {
				t.Errorf("a workspace nothing touched came back with a different content address.\n"+
					"seeded: %s\n   got: %s (%d files, %d bytes)",
					seeded, got.Digest, got.Files, got.Bytes)
			}
		})
	}
}

// TestAStepsWritesReachTheSnapshotAndTheCoordinator is the other direction:
// what the step produced has to come back, as a digest AND in the
// coordinator's own copy, which is what the next step on a DIFFERENT
// executor mounts.
func TestAStepsWritesReachTheSnapshotAndTheCoordinator(t *testing.T) {
	for _, tg := range targets() {
		t.Run(tg.name, func(t *testing.T) {
			t.Parallel()
			ex, snap := tg.new(t)

			m := seed(t, snap, "out", "/ws", func(dir string) {
				write(t, filepath.Join(dir, "keep.txt"), "kept\n", 0o644)
			})
			before := m.Digest

			sb := sandboxOn(t, ex, senroexec.SandboxSpec{
				StepID: "write", WorkDir: "/ws", Mounts: []senroexec.Mount{m},
			})
			script := `printf 'produced\n' > made.txt && mkdir -p sub && ` +
				`printf 'nested\n' > sub/inner.txt && printf '#!/bin/sh\n' > tool.sh && chmod 755 tool.sh`
			exit, _, stderr, err := runOn(t, sb, senroexec.Cmd{
				Args: []string{tg.shell, "-c", script},
				Dir:  senroexec.CmdDir("/ws", []senroexec.Mount{m}),
			})
			if err != nil || exit != 0 {
				t.Fatalf("Run: exit=%d err=%v (stderr: %s)", exit, err, stderr)
			}

			got := snapshotOf(t, sb, "out")
			if got.Digest == before {
				t.Fatalf("the digest did not move after the step wrote three files: %s", got.Digest)
			}

			// The coordinator's own copy is what the next step mounts.
			for _, f := range []struct{ rel, content string }{
				{"keep.txt", "kept\n"},
				{"made.txt", "produced\n"},
				{"sub/inner.txt", "nested\n"},
			} {
				b, err := os.ReadFile(filepath.Join(m.Path, f.rel))
				if err != nil {
					t.Errorf("the step's %s did not reach the coordinator: %v", f.rel, err)
					continue
				}
				if string(b) != f.content {
					t.Errorf("%s = %q, want %q", f.rel, b, f.content)
				}
			}
			if fi, err := os.Stat(filepath.Join(m.Path, "tool.sh")); err != nil {
				t.Errorf("tool.sh did not reach the coordinator: %v", err)
			} else if fi.Mode().Perm()&0o111 == 0 {
				t.Errorf("tool.sh came back mode %v, and the step made it executable", fi.Mode().Perm())
			}
		})
	}
}

// TestAnExcludedPathNeverEntersASnapshot holds Mount.Exclude, which travels
// with the mount precisely because the EXECUTOR takes the snapshot. An
// executor that ignored it would put a build directory into a cache key.
func TestAnExcludedPathNeverEntersASnapshot(t *testing.T) {
	for _, tg := range targets() {
		t.Run(tg.name, func(t *testing.T) {
			t.Parallel()
			ex, snap := tg.new(t)

			// Seeded WITHOUT the excludes, so the digest below is compared
			// against a tree that genuinely contains the noise.
			m := seed(t, snap, "src", "/ws", func(dir string) {
				write(t, filepath.Join(dir, "keep.txt"), "kept\n", 0o644)
			})
			m.Exclude = []string{"build/", "*.log"}

			sb := sandboxOn(t, ex, senroexec.SandboxSpec{
				StepID: "exclude", WorkDir: "/ws", Mounts: []senroexec.Mount{m},
			})
			script := `mkdir -p build node_modules .git && ` +
				`printf 'x\n' > build/artifact.bin && printf 'x\n' > noisy.log && ` +
				`printf 'x\n' > node_modules/dep.js && printf 'x\n' > .git/HEAD && ` +
				`printf 'also\n' > second.txt`
			exit, _, stderr, err := runOn(t, sb, senroexec.Cmd{
				Args: []string{tg.shell, "-c", script},
				Dir:  senroexec.CmdDir("/ws", []senroexec.Mount{m}),
			})
			if err != nil || exit != 0 {
				t.Fatalf("Run: exit=%d err=%v (stderr: %s)", exit, err, stderr)
			}

			got := snapshotOf(t, sb, "src")
			idx, err := snap.LoadIndex(context.Background(), casDigest(got.Index))
			if err != nil {
				t.Fatalf("LoadIndex: %v", err)
			}
			var paths []string
			for _, e := range idx.Entries {
				paths = append(paths, e.Path)
			}
			sort.Strings(paths)
			joined := strings.Join(paths, "\n")

			for _, unwanted := range []string{
				"build/artifact.bin", "noisy.log", "node_modules/dep.js", ".git/HEAD",
			} {
				if containsPath(paths, unwanted) {
					t.Errorf("%s entered the snapshot; the index holds:\n%s", unwanted, joined)
				}
			}
			for _, wanted := range []string{"keep.txt", "second.txt"} {
				if !containsPath(paths, wanted) {
					t.Errorf("%s is MISSING from the snapshot; the index holds:\n%s", wanted, joined)
				}
			}
		})
	}
}

// TestTwoMountsOnOneStepStayDistinct: a step that mounts two workspaces must
// see two, and snapshotting one must not capture the other.
//
// Both paths come from MountLocator rather than from the declared At,
// because two of the four executors relocate a mount (localexec under the
// sandbox, sshexec under the attempt directory) and the name is what
// survives that. localexec implements no MountLocator (nothing asks it to:
// a func step runs in the coordinator's own process there), so the second
// mount is reached through the sandbox's own working directory instead.
func TestTwoMountsOnOneStepStayDistinct(t *testing.T) {
	for _, tg := range targets() {
		t.Run(tg.name, func(t *testing.T) {
			t.Parallel()
			ex, snap := tg.new(t)

			a := seed(t, snap, "alpha", "/a", func(dir string) {
				write(t, filepath.Join(dir, "who.txt"), "alpha\n", 0o644)
			})
			b := seed(t, snap, "beta", "/b", func(dir string) {
				write(t, filepath.Join(dir, "who.txt"), "beta\n", 0o644)
			})

			mounts := []senroexec.Mount{a, b}
			sb := sandboxOn(t, ex, senroexec.SandboxSpec{
				StepID: "twomounts", WorkDir: "/a", Mounts: mounts,
			})

			// alpha is the working directory on every executor; beta needs a
			// path, and only the sandbox knows where it put one.
			betaPath := "/b"
			if loc, ok := sb.(senroexec.MountLocator); ok {
				p, found := loc.MountPath("beta")
				if !found {
					t.Fatalf("MountPath(%q) found nothing, and the sandbox mounts it", "beta")
				}
				betaPath = p
			} else if tg.name == "local" {
				// localexec symlinks every non-workdir mount under the
				// sandbox directory at the declared At; the step's cwd is
				// alpha, so beta is not reachable by a relative path from
				// there and this half of the case does not apply.
				t.Skip("localexec implements no MountLocator; see the interface doc")
			}

			exit, stdout, stderr, err := runOn(t, sb, senroexec.Cmd{
				Args: []string{tg.shell, "-c",
					`printf 'a=[%s] b=[%s]\n' "$(cat who.txt)" "$(cat "$1/who.txt")"; ` +
						`printf 'made\n' > only-in-a.txt`, "senro-step", betaPath},
				Dir: senroexec.CmdDir("/a", mounts),
			})
			if err != nil || exit != 0 {
				t.Fatalf("Run: exit=%d err=%v (stderr: %s)", exit, err, stderr)
			}
			if got := strings.TrimSpace(stdout); got != "a=[alpha] b=[beta]" {
				t.Errorf("the two mounts did not arrive distinct: %q", got)
			}

			if got := snapshotOf(t, sb, "beta").Digest; got != b.Digest {
				t.Errorf("beta's digest moved to %s, and the step only wrote into alpha", got)
			}
			if snapshotOf(t, sb, "alpha").Digest == a.Digest {
				t.Errorf("alpha's digest did not move, and the step wrote a file into it")
			}
		})
	}
}

// TestSnapshottingAnUnmountedNameIsAnError: an empty digest is a valid
// content address for "nothing", so an executor that returned one for a name
// it never mounted would write a stable, wrong value into a cache key.
func TestSnapshottingAnUnmountedNameIsAnError(t *testing.T) {
	for _, tg := range targets() {
		t.Run(tg.name, func(t *testing.T) {
			t.Parallel()
			ex, _ := tg.new(t)
			sb := sandboxOn(t, ex, senroexec.SandboxSpec{StepID: "unmounted"})

			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			s, err := sb.Snapshot(ctx, "never-mounted")
			if err == nil {
				t.Errorf("Snapshot of an unmounted name returned %+v and no error", s)
			}
		})
	}
}

func containsPath(paths []string, want string) bool {
	for _, p := range paths {
		if p == want || strings.TrimPrefix(p, "./") == want {
			return true
		}
	}
	return false
}

func casDigest(s string) cas.Digest { return cas.Digest(s) }
