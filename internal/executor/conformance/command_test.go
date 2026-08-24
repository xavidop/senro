package conformance_test

import (
	"bytes"
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	senroexec "github.com/xavidop/senro/internal/executor"
)

// runOn is one command on one sandbox, with the two streams captured
// separately, which is the whole point of Run's two-writer signature.
func runOn(
	t *testing.T, sb senroexec.Sandbox, c senroexec.Cmd,
) (exit int, stdout, stderr string, err error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	var out, errb bytes.Buffer
	exit, err = sb.Run(ctx, c, &out, &errb)
	return exit, out.String(), errb.String(), err
}

// trickyArgs are argument values that a shell, an ssh command line, a
// container's Cmd array or a pod's args array could each mangle in its own
// way. Every executor promises the step receives what the pipeline wrote.
var trickyArgs = []string{
	"plain",
	"with space",
	"with'single",
	`with"double`,
	"with$dollar",
	"with`backtick`",
	"with;semicolon",
	"with|pipe",
	"with*glob",
	"with\\backslash",
	"with\ttab",
	"with\nnewline",
	"--looks-like-a-flag",
	"-",
	"=equals=sign=",
	"日本語",
	"trailing space ",
	" leading space",
	"$(echo substituted)",
	"${HOME}",
}

// TestEveryExecutorDeliversArgvExactlyAsWritten is the portability promise at
// its most basic: exec.Command's arguments are an argv, not a command line,
// so nothing between the pipeline and the process may re-split, expand or
// drop one.
func TestEveryExecutorDeliversArgvExactlyAsWritten(t *testing.T) {
	for _, tg := range targets() {
		t.Run(tg.name, func(t *testing.T) {
			t.Parallel()
			ex, _ := tg.new(t)
			sb := sandboxOn(t, ex, senroexec.SandboxSpec{StepID: "argv"})

			args := append([]string{
				tg.shell, "-c", `for a in "$@"; do printf '<%s>\n' "$a"; done`, "senro-step",
			}, trickyArgs...)

			exit, stdout, stderr, err := runOn(t, sb, senroexec.Cmd{Args: args})
			if err != nil {
				t.Fatalf("Run: %v (stderr: %s)", err, stderr)
			}
			if exit != 0 {
				t.Fatalf("exit = %d, want 0 (stderr: %s)", exit, stderr)
			}

			want := ""
			for _, a := range trickyArgs {
				want += "<" + a + ">\n"
			}
			if stdout != want {
				t.Errorf("argv was not delivered verbatim.\n got: %q\nwant: %q", stdout, want)
			}
		})
	}
}

// trickyEnv are environment values with the same hazards as the arguments
// above. A value is bytes: nothing may trim, expand or split one.
var trickyEnv = map[string]string{
	"SENRO_PLAIN":     "plain",
	"SENRO_EMPTY":     "",
	"SENRO_SPACE":     "a b c",
	"SENRO_EQUALS":    "a=b=c",
	"SENRO_QUOTE":     `a'b"c`,
	"SENRO_DOLLAR":    "$NOT_EXPANDED",
	"SENRO_SUBST":     "$(echo no)",
	"SENRO_NEWLINE":   "line1\nline2",
	"SENRO_TAB":       "a\tb",
	"SENRO_UNICODE":   "日本語",
	"SENRO_TRAILING":  "value ",
	"SENRO_BACKSLASH": `a\b`,
}

// TestEveryExecutorDeliversTheDeclaredEnvironmentExactly holds every executor
// to the same promise for Cmd.Env that the one above holds for Cmd.Args.
func TestEveryExecutorDeliversTheDeclaredEnvironmentExactly(t *testing.T) {
	for _, tg := range targets() {
		t.Run(tg.name, func(t *testing.T) {
			t.Parallel()
			ex, _ := tg.new(t)
			sb := sandboxOn(t, ex, senroexec.SandboxSpec{StepID: "env"})

			names := make([]string, 0, len(trickyEnv))
			env := make([]string, 0, len(trickyEnv))
			for k, v := range trickyEnv {
				names = append(names, k)
				env = append(env, k+"="+v)
			}
			// Sorted, so the expected output is a fixed string rather than a
			// set comparison that could hide a duplicate.
			slicesSort(names)
			slicesSort(env)

			script := `for n in "$@"; do eval "v=\${$n-<<UNSET>>}"; printf '%s=[%s]\n' "$n" "$v"; done`
			args := append([]string{tg.shell, "-c", script, "senro-step"}, names...)

			exit, stdout, stderr, err := runOn(t, sb, senroexec.Cmd{Args: args, Env: env})
			if err != nil {
				t.Fatalf("Run: %v (stderr: %s)", err, stderr)
			}
			if exit != 0 {
				t.Fatalf("exit = %d, want 0 (stderr: %s)", exit, stderr)
			}

			var want strings.Builder
			for _, n := range names {
				fmt.Fprintf(&want, "%s=[%s]\n", n, trickyEnv[n])
			}
			if stdout != want.String() {
				t.Errorf("the declared environment did not arrive verbatim.\n got: %q\nwant: %q",
					stdout, want.String())
			}
		})
	}
}

// exitCodes are the verdicts a workload can report. 255 is here because it is
// ssh's own failure code, and the executor promises to tell a step that
// exited 255 from a connection that did.
var exitCodes = []int{0, 1, 2, 7, 42, 126, 127, 128, 254, 255}

// TestEveryExecutorReportsTheWorkloadsOwnExitCode holds the exit/err split:
// a non-zero exit is the WORKLOAD's verdict and never an infrastructure
// error.
func TestEveryExecutorReportsTheWorkloadsOwnExitCode(t *testing.T) {
	for _, tg := range targets() {
		t.Run(tg.name, func(t *testing.T) {
			t.Parallel()
			ex, _ := tg.new(t)

			for i, code := range exitCodes {
				// One sandbox per case: the k8s executor names its pod after
				// the step and attempt, so a second Run on one sandbox
				// collides with the pod the first created. The engine calls
				// Run once per sandbox, which is what makes that legal.
				sb := sandboxOn(t, ex, senroexec.SandboxSpec{StepID: "exit", Attempt: i + 1})
				exit, _, stderr, err := runOn(t, sb, senroexec.Cmd{
					Args: []string{tg.shell, "-c", "exit " + strconv.Itoa(code)},
				})
				if err != nil {
					t.Errorf("exit %d: Run returned an infrastructure error: %v (stderr: %s)",
						code, err, stderr)
					continue
				}
				if exit != code {
					t.Errorf("a command that exited %d was reported as %d", code, exit)
				}
			}
		})
	}
}

// TestEveryExecutorKeepsStdoutAndStderrApart is Run's two-writer contract.
// A step whose stderr reached stdout would corrupt any log a machine reads.
func TestEveryExecutorKeepsStdoutAndStderrApart(t *testing.T) {
	for _, tg := range targets() {
		t.Run(tg.name, func(t *testing.T) {
			t.Parallel()
			ex, _ := tg.new(t)
			sb := sandboxOn(t, ex, senroexec.SandboxSpec{StepID: "streams"})

			exit, stdout, stderr, err := runOn(t, sb, senroexec.Cmd{
				Args: []string{tg.shell, "-c", `printf 'OUT'; printf 'ERR' >&2`},
			})
			if err != nil {
				t.Fatalf("Run: %v", err)
			}
			if exit != 0 {
				t.Fatalf("exit = %d, want 0", exit)
			}
			if tg.mergedStreams {
				// Kubernetes keeps ONE log per container, so everything the
				// step writes lands in the stdout writer and stderr carries
				// only senro's own diagnostics. Documented at
				// k8sexec/doc.go:175; asserted rather than skipped, so the
				// day it stops being true this says so.
				if stdout != "OUTERR" && stdout != "ERROUT" {
					t.Errorf("stdout = %q, want both streams merged into it", stdout)
				}
				if stderr != "" {
					t.Errorf("stderr = %q, want senro's own diagnostics only", stderr)
				}
				return
			}
			if stdout != "OUT" {
				t.Errorf("stdout = %q, want %q", stdout, "OUT")
			}
			if stderr != "ERR" {
				t.Errorf("stderr = %q, want %q", stderr, "ERR")
			}
		})
	}
}

// TestEveryExecutorDeliversAWholeLargeOutput is the one that catches a
// truncating or deadlocking pipe. 4MB is past every pipe buffer involved and
// past the 16KB frames a Kubernetes log stream and a docker attach both use.
func TestEveryExecutorDeliversAWholeLargeOutput(t *testing.T) {
	const lines = 40000
	for _, tg := range targets() {
		t.Run(tg.name, func(t *testing.T) {
			t.Parallel()
			ex, _ := tg.new(t)
			sb := sandboxOn(t, ex, senroexec.SandboxSpec{StepID: "large"})

			// yes/head is portable across BusyBox and GNU; each line is 100
			// bytes, so this is 4MB.
			script := fmt.Sprintf(
				`i=0; line=$(printf '%%0100d' 0); while [ $i -lt %d ]; do printf '%%s\n' "$line"; i=$((i+1)); done`,
				lines)
			exit, stdout, stderr, err := runOn(t, sb, senroexec.Cmd{
				Args: []string{tg.shell, "-c", script},
			})
			if err != nil {
				t.Fatalf("Run: %v (stderr: %s)", err, stderr)
			}
			if exit != 0 {
				t.Fatalf("exit = %d, want 0 (stderr: %s)", exit, stderr)
			}
			if got := strings.Count(stdout, "\n"); got != lines {
				t.Errorf("stdout carries %d lines, want %d (%d bytes)",
					got, lines, len(stdout))
			}
		})
	}
}

// TestEveryExecutorDeliversOutputWithNoTrailingNewline catches a reader that
// only emits whole lines: a step's last partial line is still its output.
func TestEveryExecutorDeliversOutputWithNoTrailingNewline(t *testing.T) {
	for _, tg := range targets() {
		t.Run(tg.name, func(t *testing.T) {
			t.Parallel()
			ex, _ := tg.new(t)
			sb := sandboxOn(t, ex, senroexec.SandboxSpec{StepID: "partial"})

			exit, stdout, _, err := runOn(t, sb, senroexec.Cmd{
				Args: []string{tg.shell, "-c", `printf 'no-newline-here'`},
			})
			if err != nil {
				t.Fatalf("Run: %v", err)
			}
			if exit != 0 {
				t.Fatalf("exit = %d, want 0", exit)
			}
			if stdout != "no-newline-here" {
				t.Errorf("stdout = %q, want %q", stdout, "no-newline-here")
			}
		})
	}
}

// TestEveryExecutorRunsInTheDeclaredWorkingDirectory holds Cmd.Dir. The
// directory is one every target has, so this asks only whether Dir is
// honoured at all.
func TestEveryExecutorRunsInTheDeclaredWorkingDirectory(t *testing.T) {
	for _, tg := range targets() {
		t.Run(tg.name, func(t *testing.T) {
			t.Parallel()
			ex, _ := tg.new(t)
			sb := sandboxOn(t, ex, senroexec.SandboxSpec{StepID: "workdir"})

			exit, stdout, stderr, err := runOn(t, sb, senroexec.Cmd{
				Args: []string{tg.shell, "-c", "pwd"}, Dir: "/tmp",
			})
			if err != nil {
				t.Fatalf("Run: %v (stderr: %s)", err, stderr)
			}
			if exit != 0 {
				t.Fatalf("exit = %d, want 0 (stderr: %s)", exit, stderr)
			}
			// Darwin's /tmp is a symlink to /private/tmp, and pwd reports the
			// resolved path there; the question is whether the command landed
			// in the declared directory at all.
			if got := strings.TrimSpace(stdout); got != "/tmp" && got != "/private/tmp" {
				t.Errorf("pwd = %q, want the declared /tmp", got)
			}
		})
	}
}

// TestEveryExecutorFailsAStepWhoseProgramDoesNotExist. A missing program is
// the workload's problem (127 by shell convention), not the substrate's, and
// an executor that reported it as ErrInfra would have retry.OnInfra retrying
// a typo forever.
func TestEveryExecutorFailsAStepWhoseProgramDoesNotExist(t *testing.T) {
	for _, tg := range targets() {
		t.Run(tg.name, func(t *testing.T) {
			t.Parallel()
			ex, _ := tg.new(t)
			sb := sandboxOn(t, ex, senroexec.SandboxSpec{StepID: "missing"})

			exit, _, _, err := runOn(t, sb, senroexec.Cmd{
				Args: []string{"senro-no-such-program-anywhere"},
			})
			if senroexec.IsInfra(err) {
				t.Fatalf("a missing program was reported as an infrastructure failure: %v", err)
			}
			if err != nil {
				t.Fatalf("Run: %v", err)
			}
			if exit == 0 {
				t.Errorf("exit = 0 for a program that does not exist")
			}
		})
	}
}

// TestEveryExecutorReportsACommandKilledBySignal. A step killed by the OOM
// killer or by a SIGTERM is not a step that succeeded, and the shell
// convention that carries that verdict (128+signal) has to survive every
// transport between the process and the engine.
//
// The signalled process is a CHILD rather than the command itself: three of
// these four sandboxes run the command as pid 1 of a namespace, and the
// kernel discards a signal with no handler sent to pid 1 from inside it, so
// a shell that killed itself would exit 0 on those and 137 on the local
// executor for a reason that has nothing to do with senro.
func TestEveryExecutorReportsACommandKilledBySignal(t *testing.T) {
	for _, tg := range targets() {
		t.Run(tg.name, func(t *testing.T) {
			t.Parallel()
			ex, _ := tg.new(t)
			sb := sandboxOn(t, ex, senroexec.SandboxSpec{StepID: "signal"})

			// A real child, signalled from outside, with its status captured
			// before anything else can overwrite $?.
			const script = `sleep 30 & p=$!; kill -9 "$p"; wait "$p"; c=$?; ` +
				`printf 'child=%s\n' "$c"; exit "$c"`
			exit, stdout, stderr, err := runOn(t, sb, senroexec.Cmd{
				Args: []string{tg.shell, "-c", script},
			})
			if err != nil {
				t.Fatalf("Run: %v (stderr: %s)", err, stderr)
			}
			if !strings.Contains(stdout, "child=137") {
				t.Errorf("a child killed by SIGKILL reported %q, want the shell's 137", stdout)
			}
			if exit != 137 {
				t.Errorf("the step reported exit %d after carrying its child's 137 out", exit)
			}
		})
	}
}

// TestCancellingRunKillsTheCommand is Run's cancellation contract: a
// cancelled context must end the command, bounded, on every executor.
func TestCancellingRunKillsTheCommand(t *testing.T) {
	for _, tg := range targets() {
		t.Run(tg.name, func(t *testing.T) {
			t.Parallel()
			ex, _ := tg.new(t)
			sb := sandboxOn(t, ex, senroexec.SandboxSpec{StepID: "cancel"})

			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan struct{})
			var runErr error
			go func() {
				defer close(done)
				_, runErr = sb.Run(ctx, senroexec.Cmd{
					Args: []string{tg.shell, "-c", "sleep 600"},
				}, &bytes.Buffer{}, &bytes.Buffer{})
			}()
			time.Sleep(3 * time.Second)
			cancel()
			select {
			case <-done:
			case <-time.After(90 * time.Second):
				t.Fatal("Run did not return within 90s of its context being cancelled")
			}
			_ = runErr
		})
	}
}

func slicesSort(s []string) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j] < s[j-1]; j-- {
			s[j], s[j-1] = s[j-1], s[j]
		}
	}
}
