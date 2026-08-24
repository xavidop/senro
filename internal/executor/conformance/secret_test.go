package conformance_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	senroexec "github.com/xavidop/senro/internal/executor"
)

// TestASecretArrivesAsExactlyTheBytesItWasGiven. A credential is bytes, not
// a line: a delivery that trimmed, re-encoded or newline-terminated one
// would hand a step a token that does not authenticate, and the failure
// would look like the credential being wrong.
func TestASecretArrivesAsExactlyTheBytesItWasGiven(t *testing.T) {
	// Every value here is one a real credential takes: a PEM key has
	// newlines, a base64 token has trailing "=", a password can hold a quote,
	// and nothing guarantees a trailing newline.
	values := map[string]string{
		"plain":       "s3cr3t",
		"newlines":    "-----BEGIN KEY-----\nline\n-----END KEY-----\n",
		"notrailing":  "no-trailing-newline",
		"leadingdash": "-----",
		"padding":     "dG9rZW4=",
		"quotes":      `a'b"c`,
		"spaces":      "  padded  ",
		"unicode":     "秘密",
		"empty":       "",
		"long":        strings.Repeat("x", 100000),
	}

	for _, tg := range targets() {
		t.Run(tg.name, func(t *testing.T) {
			t.Parallel()
			ex, _ := tg.new(t)

			names := make([]string, 0, len(values))
			refs := make([]senroexec.SecretRef, 0, len(values))
			for n := range values {
				names = append(names, n)
			}
			slicesSort(names)
			for _, n := range names {
				refs = append(refs, senroexec.SecretRef{Name: n})
			}

			sb := sandboxOn(t, ex, senroexec.SandboxSpec{StepID: "secret", Secrets: refs})

			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			env := []string{}
			for _, n := range names {
				p, err := sb.PutSecret(ctx, n, []byte(values[n]))
				if err != nil {
					t.Fatalf("PutSecret(%q): %v", n, err)
				}
				env = append(env, "SEC_"+strings.ToUpper(n)+"="+p)
			}

			// The step reads every file and reports its byte count and a
			// checksum of the content, so a value that arrived mangled shows
			// up as a mismatch rather than as a log full of credentials.
			script := `for n in "$@"; do
  eval "p=\$SEC_$n"
  if [ ! -f "$p" ]; then printf '%s=MISSING\n' "$n"; continue; fi
  printf '%s=%s/%s\n' "$n" "$(wc -c < "$p" | tr -d ' ')" "$(cksum < "$p" | cut -d' ' -f1)"
done`
			args := []string{tg.shell, "-c", script, "senro-step"}
			for _, n := range names {
				args = append(args, strings.ToUpper(n))
			}

			exit, stdout, stderr, err := runOn(t, sb, senroexec.Cmd{Args: args, Env: env})
			if err != nil {
				t.Fatalf("Run: %v (stderr: %s)", err, stderr)
			}
			if exit != 0 {
				t.Fatalf("exit = %d, want 0 (stderr: %s)", exit, stderr)
			}

			var want strings.Builder
			for _, n := range names {
				fmt.Fprintf(&want, "%s=%d/%d\n",
					strings.ToUpper(n), len(values[n]), cksum([]byte(values[n])))
			}
			if stdout != want.String() {
				t.Errorf("a secret did not arrive as the bytes it was given "+
					"(name=length/cksum).\n got:\n%s\nwant:\n%s", stdout, want.String())
			}
		})
	}
}

// TestASecretFileIsNotReadableByOtherAccounts. The value is delivered as a
// file precisely so it stays out of argv and the environment; a file another
// account on the box can read gives that back.
func TestASecretFileIsNotReadableByOtherAccounts(t *testing.T) {
	for _, tg := range targets() {
		t.Run(tg.name, func(t *testing.T) {
			t.Parallel()
			ex, _ := tg.new(t)
			sb := sandboxOn(t, ex, senroexec.SandboxSpec{
				StepID: "secretmode", Secrets: []senroexec.SecretRef{{Name: "token"}},
			})

			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			p, err := sb.PutSecret(ctx, "token", []byte("value"))
			if err != nil {
				t.Fatalf("PutSecret: %v", err)
			}

			exit, stdout, stderr, err := runOn(t, sb, senroexec.Cmd{
				// -L dereferences: Kubernetes projects a Secret volume as a
				// symlink into a "..data" directory, and the mode that
				// matters is the one on the file the link resolves to.
				Args: []string{tg.shell, "-c", `ls -lL "$1" | cut -c1-10`, "senro-step", p},
			})
			if err != nil {
				t.Fatalf("Run: %v (stderr: %s)", err, stderr)
			}
			if exit != 0 {
				t.Fatalf("exit = %d, want 0 (stderr: %s)", exit, stderr)
			}
			mode := strings.TrimSpace(stdout)
			if len(mode) != 10 {
				t.Fatalf("could not read the secret file's mode: %q", stdout)
			}
			// Group and other bits: positions 5-10 of "-rw-------".
			if rest := mode[4:]; strings.Trim(rest, "-") != "" {
				t.Errorf("the secret file is mode %q; group and other must hold no bit at all", mode)
			}
		})
	}
}

// TestTheSecretValueIsNeverInTheStepsEnvironmentOrArgv. What crosses is a
// PATH; the value itself must not be reachable from /proc or from `ps`.
func TestTheSecretValueIsNeverInTheStepsEnvironmentOrArgv(t *testing.T) {
	const value = "conformance-canary-9f2c1d"
	for _, tg := range targets() {
		t.Run(tg.name, func(t *testing.T) {
			t.Parallel()
			ex, _ := tg.new(t)
			sb := sandboxOn(t, ex, senroexec.SandboxSpec{
				StepID: "secretenv", Secrets: []senroexec.SecretRef{{Name: "canary"}},
			})

			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			p, err := sb.PutSecret(ctx, "canary", []byte(value))
			if err != nil {
				t.Fatalf("PutSecret: %v", err)
			}

			exit, stdout, stderr, err := runOn(t, sb, senroexec.Cmd{
				Args: []string{tg.shell, "-c", `env; echo "---"; cat /proc/self/environ 2>/dev/null | tr '\0' '\n'`},
				Env:  []string{"SENRO_SECRET_CANARY=" + p},
			})
			if err != nil {
				t.Fatalf("Run: %v (stderr: %s)", err, stderr)
			}
			if exit != 0 {
				t.Fatalf("exit = %d, want 0 (stderr: %s)", exit, stderr)
			}
			if strings.Contains(stdout, value) {
				t.Errorf("the secret VALUE is in the step's environment:\n%s", stdout)
			}
			if !strings.Contains(stdout, p) {
				t.Errorf("the secret PATH is not in the step's environment, so nothing could read it:\n%s",
					stdout)
			}
		})
	}
}

// TestClosingASandboxRemovesItsSecretFile is the promise every executor
// makes in its own words: a credential does not outlive the step.
func TestClosingASandboxRemovesItsSecretFile(t *testing.T) {
	for _, tg := range targets() {
		t.Run(tg.name, func(t *testing.T) {
			t.Parallel()
			ex, _ := tg.new(t)

			ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
			defer cancel()
			sb, err := ex.Sandbox(ctx, senroexec.SandboxSpec{
				StepID: "secretgone", Attempt: 1,
				Secrets: []senroexec.SecretRef{{Name: "token"}},
			})
			if err != nil {
				t.Fatalf("Sandbox: %v", err)
			}
			p, err := sb.PutSecret(ctx, "token", []byte("value"))
			if err != nil {
				t.Fatalf("PutSecret: %v", err)
			}
			// The file has to exist first, or the check below proves nothing.
			exit, _, _, err := runOn(t, sb, senroexec.Cmd{
				Args: []string{tg.shell, "-c", `test -f "$1"`, "senro-step", p},
			})
			if err != nil || exit != 0 {
				t.Fatalf("the secret file was not there to begin with: exit=%d err=%v", exit, err)
			}
			if err := sb.Close(ctx, false); err != nil {
				t.Fatalf("Close: %v", err)
			}

			// A second sandbox on the same executor is what can still look at
			// the target's filesystem after the first is gone.
			probe := sandboxOn(t, ex, senroexec.SandboxSpec{StepID: "secretprobe"})
			exit, stdout, _, err := runOn(t, probe, senroexec.Cmd{
				Args: []string{tg.shell, "-c",
					`if [ -e "$1" ]; then printf 'STILL THERE\n'; else printf 'gone\n'; fi`,
					"senro-step", p},
			})
			if err != nil {
				t.Fatalf("probe Run: %v", err)
			}
			if exit != 0 {
				t.Fatalf("probe exit = %d", exit)
			}
			if strings.TrimSpace(stdout) != "gone" {
				t.Errorf("the secret file at %s outlived the sandbox that delivered it", p)
			}
		})
	}
}

// cksum is the POSIX cksum(1) CRC the script above reports, so the expected
// value is computed here rather than trusted from the target.
func cksum(b []byte) uint32 {
	var crc uint32
	for _, c := range b {
		crc = crc<<8 ^ cksumTable[byte(crc>>24)^c]
	}
	for n := len(b); n != 0; n >>= 8 {
		crc = crc<<8 ^ cksumTable[byte(crc>>24)^byte(n)]
	}
	return ^crc
}

var cksumTable = func() [256]uint32 {
	const poly = 0x04C11DB7
	var t [256]uint32
	for i := range t {
		c := uint32(i) << 24
		for range 8 {
			if c&0x80000000 != 0 {
				c = c<<1 ^ poly
			} else {
				c <<= 1
			}
		}
		t[i] = c
	}
	return t
}()
