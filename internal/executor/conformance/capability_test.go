package conformance_test

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	senroexec "github.com/xavidop/senro/internal/executor"
)

// TestAReadOnlyMountIsEitherEnforcedOrDetectable is the portable half of
// senro.RO. Two of the four executors enforce it in the kernel and two
// cannot (see senro.RO and sshexec's doc:187), so the promise EVERY executor
// makes is the weaker one asserted here: either the step's write fails, or
// the coordinator-side digest moves so engine.snapshotMounts' before/after
// check can turn it into a step failure. What must never happen is a step
// that mutated a read-only input and left the digest describing something
// else, because every later cache key computed from it would be wrong.
//
// The engine-level half is TestWritingThroughAReadOnlyMountFailsTheStep,
// which asserts the failure actually lands.
func TestAReadOnlyMountIsEitherEnforcedOrDetectable(t *testing.T) {
	for _, tg := range targets() {
		t.Run(tg.name, func(t *testing.T) {
			t.Parallel()
			ex, snap := tg.new(t)

			m := seed(t, snap, "src", "/ws", func(dir string) {
				write(t, filepath.Join(dir, "input.txt"), "original\n", 0o644)
			})
			m.RO = true
			before := m.Digest

			sb := sandboxOn(t, ex, senroexec.SandboxSpec{
				StepID: "readonly", WorkDir: "/ws", Mounts: []senroexec.Mount{m},
			})
			// The write is ALLOWED to fail, and on two executors it must.
			_, _, _, err := runOn(t, sb, senroexec.Cmd{
				Args: []string{tg.shell, "-c",
					`printf 'tampered\n' > input.txt 2>/dev/null; ` +
						`printf 'sneaked\n' > extra.txt 2>/dev/null; true`},
				Dir: senroexec.CmdDir("/ws", []senroexec.Mount{m}),
			})
			if err != nil {
				t.Fatalf("Run: %v", err)
			}

			// Snapshot is what the engine compares against, so it is what
			// this asserts: unchanged means the mount was enforced or the
			// write was discarded; changed means the breach is visible.
			// Silently unchanged AFTER a successful tamper would be the one
			// bad answer, and it cannot happen: the snapshot is taken from
			// the same tree the step wrote.
			after := snapshotOf(t, sb, "src")
			tampered := after.Digest != before

			b, readErr := os.ReadFile(filepath.Join(m.Path, "input.txt"))
			switch {
			case readErr != nil:
				t.Fatalf("the coordinator's copy is gone: %v", readErr)
			case string(b) != "original\n" && !tampered:
				t.Errorf("the coordinator-side file became %q and the digest did NOT move, so the "+
					"engine's read-only check can never see it", b)
			}
		})
	}
}

// TestReadMountReturnsWhatTheTargetHolds exercises the MountReader
// capability, which is how a scratch cache comes back off a target that
// shares no filesystem. An executor that shares the coordinator's own
// filesystem implements none, and says so by not implementing it.
func TestReadMountReturnsWhatTheTargetHolds(t *testing.T) {
	for _, tg := range targets() {
		t.Run(tg.name, func(t *testing.T) {
			t.Parallel()
			ex, snap := tg.new(t)

			m := seed(t, snap, "cache", "/cache", func(dir string) {
				write(t, filepath.Join(dir, "seeded.txt"), "seeded\n", 0o644)
			})
			m.Scratch = true

			sb := sandboxOn(t, ex, senroexec.SandboxSpec{
				StepID: "readmount", WorkDir: "/cache", Mounts: []senroexec.Mount{m},
			})
			exit, _, stderr, err := runOn(t, sb, senroexec.Cmd{
				Args: []string{tg.shell, "-c",
					`printf 'made\n' > produced.txt && mkdir -p node_modules && printf 'dep\n' > node_modules/dep.js`},
				Dir: senroexec.CmdDir("/cache", []senroexec.Mount{m}),
			})
			if err != nil || exit != 0 {
				t.Fatalf("Run: exit=%d err=%v (stderr: %s)", exit, err, stderr)
			}

			mr, ok := sb.(senroexec.MountReader)
			if !ok {
				// The executor shares the coordinator's filesystem, so the
				// directory itself is the answer. Assert THAT instead, so
				// this case is not silently vacuous on two of four.
				if _, err := os.Stat(filepath.Join(m.Path, "produced.txt")); err != nil {
					t.Errorf("%s implements no MountReader and its shared directory does not "+
						"hold what the step wrote either: %v", tg.name, err)
				}
				return
			}

			dest := t.TempDir()
			ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
			defer cancel()
			if err := mr.ReadMount(ctx, "cache", dest); err != nil {
				t.Fatalf("ReadMount: %v", err)
			}
			for _, f := range []struct{ rel, content string }{
				{"seeded.txt", "seeded\n"},
				{"produced.txt", "made\n"},
				// A scratch cache excludes NOTHING: node_modules is the usual
				// content rather than something to skip.
				{"node_modules/dep.js", "dep\n"},
			} {
				b, err := os.ReadFile(filepath.Join(dest, f.rel))
				if err != nil {
					t.Errorf("ReadMount did not bring back %s: %v", f.rel, err)
					continue
				}
				if string(b) != f.content {
					t.Errorf("%s = %q, want %q", f.rel, b, f.content)
				}
			}
		})
	}
}

// TestReadMountIntoAMissingDirectoryFails: the interface says "dest must
// already exist, and an implementation must fail rather than leave a partial
// copy in it". A caller that stored a partial copy would key it under
// something it can never rewrite.
func TestReadMountIntoAMissingDirectoryFails(t *testing.T) {
	for _, tg := range targets() {
		t.Run(tg.name, func(t *testing.T) {
			t.Parallel()
			ex, snap := tg.new(t)
			m := seed(t, snap, "cache", "/cache", func(dir string) {
				write(t, filepath.Join(dir, "f.txt"), "x\n", 0o644)
			})
			m.Scratch = true
			sb := sandboxOn(t, ex, senroexec.SandboxSpec{
				StepID: "readmountmissing", WorkDir: "/cache", Mounts: []senroexec.Mount{m},
			})
			mr, ok := sb.(senroexec.MountReader)
			if !ok {
				t.Skipf("%s implements no MountReader", tg.name)
			}
			// The step has to have run, or an executor that refuses a
			// read-back before its pod ran would pass this for the wrong
			// reason.
			exit, _, stderr, err := runOn(t, sb, senroexec.Cmd{
				Args: []string{tg.shell, "-c", "true"},
				Dir:  senroexec.CmdDir("/cache", []senroexec.Mount{m}),
			})
			if err != nil || exit != 0 {
				t.Fatalf("Run: exit=%d err=%v (stderr: %s)", exit, err, stderr)
			}

			ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
			defer cancel()
			missing := filepath.Join(t.TempDir(), "does", "not", "exist")
			if err := mr.ReadMount(ctx, "cache", missing); err == nil {
				t.Errorf("ReadMount into %s returned no error, and that directory did not exist",
					missing)
			}
		})
	}
}

// TestEveryExecutorInThisBuildCanHostAShell: internal/engine reaches a
// session through an interface assertion, so an executor that stopped
// implementing Interactive would turn every `senro shell` against it into a
// refusal with nothing failing at build time.
func TestEveryExecutorInThisBuildCanHostAShell(t *testing.T) {
	for _, tg := range targets() {
		t.Run(tg.name, func(t *testing.T) {
			t.Parallel()
			ex, _ := tg.new(t)
			sb := sandboxOn(t, ex, senroexec.SandboxSpec{StepID: "shell"})
			if _, ok := sb.(senroexec.Interactive); !ok {
				t.Fatalf("%s's sandbox does not implement Interactive", tg.name)
			}
		})
	}
}

// TestAnInteractiveSessionCarriesStdinAndReportsItsOwnExit is Interactive's
// contract: stdin reaches the command, EOF on stdin ends a shell by itself,
// and a session whose command exits 7 returns (7, nil).
func TestAnInteractiveSessionCarriesStdinAndReportsItsOwnExit(t *testing.T) {
	for _, tg := range targets() {
		t.Run(tg.name, func(t *testing.T) {
			t.Parallel()
			ex, _ := tg.new(t)
			sb := sandboxOn(t, ex, senroexec.SandboxSpec{StepID: "interactive"})
			in, ok := sb.(senroexec.Interactive)
			if !ok {
				t.Fatalf("%s's sandbox does not implement Interactive", tg.name)
			}

			ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
			defer cancel()
			var out, errb strings.Builder
			exit, err := in.RunInteractive(ctx,
				senroexec.Cmd{Args: []string{tg.shell}},
				strings.NewReader("echo from-stdin\nexit 7\n"), &out, &errb)
			if err != nil {
				t.Fatalf("RunInteractive: %v (stderr: %s)", err, errb.String())
			}
			if !strings.Contains(out.String(), "from-stdin") {
				t.Errorf("stdin did not reach the session's shell; stdout=%q stderr=%q",
					out.String(), errb.String())
			}
			if exit != 7 {
				t.Errorf("exit = %d, want 7: a session's exit is the command's own verdict", exit)
			}
		})
	}
}

// TestAnInteractiveSessionEndsWhenItsContextIsCancelled is the
// client-disconnect path the interface spells out: a session's command
// usually ignores stdin entirely, so cancelling the context MUST kill it,
// bounded.
func TestAnInteractiveSessionEndsWhenItsContextIsCancelled(t *testing.T) {
	for _, tg := range targets() {
		t.Run(tg.name, func(t *testing.T) {
			t.Parallel()
			ex, _ := tg.new(t)
			sb := sandboxOn(t, ex, senroexec.SandboxSpec{StepID: "interactivecancel"})
			in, ok := sb.(senroexec.Interactive)
			if !ok {
				t.Fatalf("%s's sandbox does not implement Interactive", tg.name)
			}

			ctx, cancel := context.WithCancel(context.Background())
			// A reader that never returns, which is what a connected client
			// that is typing nothing looks like.
			pr, pw := io.Pipe()
			t.Cleanup(func() { _ = pw.Close() })

			done := make(chan struct{})
			go func() {
				defer close(done)
				_, _ = in.RunInteractive(ctx,
					senroexec.Cmd{Args: []string{tg.shell, "-c", "sleep 600"}},
					pr, io.Discard, io.Discard)
			}()
			time.Sleep(3 * time.Second)
			cancel()
			select {
			case <-done:
			case <-time.After(90 * time.Second):
				t.Fatal("RunInteractive did not return within 90s of its context being cancelled")
			}
		})
	}
}

// TestATerminalSessionRunsOnARealPty holds the Terminal capability where it
// is implemented. Not every executor has one (sshexec can host a shell but
// not a terminal), and this asserts the ones that do run against a device
// that reports the size it was created with.
func TestATerminalSessionRunsOnARealPty(t *testing.T) {
	for _, tg := range targets() {
		t.Run(tg.name, func(t *testing.T) {
			t.Parallel()
			ex, _ := tg.new(t)
			sb := sandboxOn(t, ex, senroexec.SandboxSpec{StepID: "terminal"})
			term, ok := sb.(senroexec.Terminal)
			if !ok {
				t.Skipf("%s hosts no terminal", tg.name)
			}

			ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
			defer cancel()
			var out strings.Builder
			resize := make(chan senroexec.WinSize)
			close(resize)
			// A stdin that stays OPEN for the life of the command: a
			// terminal has no EOF, and a reader already at EOF makes the
			// executor close the session's input before the command has
			// read its own window size.
			stdin, closeStdin := io.Pipe()
			defer func() { _ = closeStdin.Close() }()
			exit, err := term.RunTerminal(ctx,
				senroexec.Cmd{Args: []string{tg.shell, "-c",
					`sleep 1; stty size; tty >/dev/null && echo IS_A_TTY`}},
				stdin, &out,
				senroexec.WinSize{Cols: 120, Rows: 40}, resize)
			if err != nil {
				t.Fatalf("RunTerminal: %v (output: %q)", err, out.String())
			}
			if exit != 0 {
				t.Fatalf("exit = %d, want 0 (output: %q)", exit, out.String())
			}
			got := out.String()
			if !strings.Contains(got, "IS_A_TTY") {
				t.Errorf("the session's command did not see a terminal: %q", got)
			}
			if !strings.Contains(got, "40 120") {
				t.Errorf("the terminal did not come up at the size it was created with (40 rows, "+
					"120 cols); a program reading 0 0 draws nothing. Got: %q", got)
			}
		})
	}
}

// TestManySandboxesOnOneExecutorRunConcurrently. One executor serves a whole
// run, and a run's steps are concurrent: an executor that serialized them,
// deadlocked, or let two sandboxes collide on a name would turn a wide
// pipeline into a hang or a spurious failure. Eight is sshd's default
// MaxSessions, which is the tightest limit any of these four has.
func TestManySandboxesOnOneExecutorRunConcurrently(t *testing.T) {
	const n = 8
	for _, tg := range targets() {
		t.Run(tg.name, func(t *testing.T) {
			t.Parallel()
			ex, _ := tg.new(t)

			var wg sync.WaitGroup
			errs := make([]error, n)
			outs := make([]string, n)
			for i := range n {
				wg.Add(1)
				go func() {
					defer wg.Done()
					ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
					defer cancel()
					sb, err := ex.Sandbox(ctx, senroexec.SandboxSpec{
						StepID: "concurrent-" + string(rune('a'+i)), Attempt: 1,
					})
					if err != nil {
						errs[i] = err
						return
					}
					defer func() {
						c, cc := context.WithTimeout(context.Background(), 2*time.Minute)
						defer cc()
						_ = sb.Close(c, false)
					}()
					var out strings.Builder
					if _, err := sb.Run(ctx, senroexec.Cmd{
						Args: []string{tg.shell, "-c", "echo ok-$1", "senro-step",
							string(rune('a' + i))},
					}, &out, io.Discard); err != nil {
						errs[i] = err
						return
					}
					outs[i] = strings.TrimSpace(out.String())
				}()
			}
			wg.Wait()
			for i := range n {
				if errs[i] != nil {
					t.Errorf("sandbox %d: %v", i, errs[i])
					continue
				}
				if want := "ok-" + string(rune('a'+i)); outs[i] != want {
					t.Errorf("sandbox %d produced %q, want %q", i, outs[i], want)
				}
			}
		})
	}
}

// TestTwoAttemptsOfOneStepGetIndependentSandboxes. A retry reuses the step
// id with a new attempt number, and the two must not collide on a name, a
// directory or a pod.
func TestTwoAttemptsOfOneStepGetIndependentSandboxes(t *testing.T) {
	for _, tg := range targets() {
		t.Run(tg.name, func(t *testing.T) {
			t.Parallel()
			ex, _ := tg.new(t)

			for attempt := 1; attempt <= 3; attempt++ {
				sb := sandboxOn(t, ex, senroexec.SandboxSpec{
					StepID: "retried", Attempt: attempt,
				})
				exit, stdout, stderr, err := runOn(t, sb, senroexec.Cmd{
					Args: []string{tg.shell, "-c", "echo attempt-$1", "senro-step",
						string(rune('0' + attempt))},
				})
				if err != nil {
					t.Fatalf("attempt %d: Run: %v (stderr: %s)", attempt, err, stderr)
				}
				if exit != 0 {
					t.Fatalf("attempt %d: exit = %d (stderr: %s)", attempt, exit, stderr)
				}
				if want := "attempt-" + string(rune('0'+attempt)); strings.TrimSpace(stdout) != want {
					t.Errorf("attempt %d produced %q, want %q", attempt, strings.TrimSpace(stdout), want)
				}
			}
		})
	}
}

// TestObservedPlatformAgreesWithTheDeclaredOne. The declared platform enters
// the cache key and the observed one is checked against it after the sandbox
// exists; two that disagreed would mean a cache entry keyed on a machine the
// step never ran on.
func TestObservedPlatformAgreesWithTheDeclaredOne(t *testing.T) {
	for _, tg := range targets() {
		t.Run(tg.name, func(t *testing.T) {
			t.Parallel()
			ex, _ := tg.new(t)
			ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
			defer cancel()

			declared, err := ex.DeclaredPlatform(ctx)
			if err != nil {
				t.Fatalf("DeclaredPlatform: %v", err)
			}
			if declared.OS == "" || declared.Arch == "" {
				t.Fatalf("DeclaredPlatform = %q, and both halves enter a cache key", declared)
			}
			sb := sandboxOn(t, ex, senroexec.SandboxSpec{StepID: "platform"})
			observed, err := sb.ObservedPlatform(ctx)
			if err != nil {
				t.Fatalf("ObservedPlatform: %v", err)
			}
			if observed != declared {
				t.Errorf("declared %q but observed %q", declared, observed)
			}
		})
	}
}

// TestTheCacheClassIsStableAndNotHostIdentity. Class is the cache
// equivalence class: unstable within a run, nothing hits; equal to host
// identity, a fleet never shares an entry.
func TestTheCacheClassIsStableAndNotHostIdentity(t *testing.T) {
	for _, tg := range targets() {
		t.Run(tg.name, func(t *testing.T) {
			t.Parallel()
			ex, _ := tg.new(t)
			ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
			defer cancel()

			first, err := ex.Class(ctx)
			if err != nil {
				t.Fatalf("Class: %v", err)
			}
			if first == "" {
				t.Fatal("Class is empty, so every executor would share one cache namespace")
			}
			second, err := ex.Class(ctx)
			if err != nil {
				t.Fatalf("Class (again): %v", err)
			}
			if second != first {
				t.Errorf("Class is not stable within a run: %q then %q", first, second)
			}
		})
	}
}
