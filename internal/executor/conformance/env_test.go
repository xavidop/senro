package conformance_test

import (
	"context"
	"strings"
	"testing"
	"time"

	senroexec "github.com/xavidop/senro/internal/executor"
)

// TestEffectiveEnvIsWhatTheStepActuallyReceives.
//
// EffectiveEnv exists for exactly one reason, in its own words: "so a cache
// key's env component reflects what the step actually receives; the declared
// env alone misses executor-injected entries, which never appear in the
// plan". engine.cacheLookup feeds its answer straight into the key.
//
// So an entry EffectiveEnv reports and the step does NOT receive is not a
// cosmetic disagreement: it is a cache key that describes a step nobody ran.
// Two runs would share an entry they should not, or miss one they should
// hit, and nothing anywhere would report an error.
//
// Asserted in one direction only, deliberately. Every entry EffectiveEnv
// names must reach the step with that exact value; the step may ALSO receive
// things the executor could not have known about (a container's HOSTNAME,
// the service variables kubelet injects), and claiming those would be the
// opposite mistake.
func TestEffectiveEnvIsWhatTheStepActuallyReceives(t *testing.T) {
	// A declaration with a hole in it on purpose: PATH is the variable
	// localexec injects when a step declares none, and it is the one
	// CacheEnv("PATH") has to be able to see.
	declared := []string{"SENRO_DECLARED=yes", "SENRO_EMPTY="}

	for _, tg := range targets() {
		t.Run(tg.name, func(t *testing.T) {
			t.Parallel()
			ex, _ := tg.new(t)

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			defer cancel()
			effective, err := ex.EffectiveEnv(ctx, declared)
			if err != nil {
				t.Fatalf("EffectiveEnv: %v", err)
			}
			if len(effective) == 0 {
				t.Fatal("EffectiveEnv reported nothing at all for a step that declared two entries")
			}

			// One name must appear once: a duplicate makes "the value that
			// enters the key" ambiguous, and the two readers could disagree.
			seen := map[string]int{}
			for _, kv := range effective {
				name, _, ok := strings.Cut(kv, "=")
				if !ok {
					t.Errorf("EffectiveEnv reported %q, which is not NAME=VALUE", kv)
					continue
				}
				seen[name]++
			}
			for name, n := range seen {
				if n > 1 {
					t.Errorf("EffectiveEnv reports %s %d times; a cache key cannot digest two "+
						"values for one name", name, n)
				}
			}

			// The step reports what it ACTUALLY has, one NUL-free line per
			// variable, read back here rather than compared by a script:
			// values contain newlines and equals signs in the wild.
			sb := sandboxOn(t, ex, senroexec.SandboxSpec{StepID: "effectiveenv"})
			names := make([]string, 0, len(seen))
			for name := range seen {
				names = append(names, name)
			}
			slicesSort(names)

			// The assignment happens inside the eval and the PRINT outside
			// it: a printf built inside an eval is one level of quoting
			// deeper and does not survive every shell the same way. This is
			// the same shape TestEveryExecutorDeliversTheDeclaredEnvironment
			// uses. "-" substitutes only for an UNSET name, so a variable
			// that is set and empty is reported as set and empty.
			script := `for n in "$@"; do eval "v=\${$n-<<UNSET>>}"; printf '%s=[%s]\n' "$n" "$v"; done`
			args := append([]string{tg.shell, "-c", script, "senro-step"}, names...)

			exit, stdout, stderr, err := runOn(t, sb, senroexec.Cmd{Args: args, Env: declared})
			if err != nil {
				t.Fatalf("Run: %v (stderr: %s)", err, stderr)
			}
			if exit != 0 {
				t.Fatalf("exit = %d (stderr: %s)", exit, stderr)
			}

			actual := map[string]string{}
			var missing []string
			for _, line := range strings.Split(strings.TrimSuffix(stdout, "\n"), "\n") {
				name, rest, ok := strings.Cut(line, "=")
				if !ok {
					continue
				}
				if rest == "[<<UNSET>>]" {
					missing = append(missing, name)
					continue
				}
				actual[name] = strings.TrimSuffix(strings.TrimPrefix(rest, "["), "]")
			}
			if len(missing) > 0 {
				t.Errorf("EffectiveEnv named %v, and the step received none of them. Every name it "+
					"reports enters the cache key as a fact about the step's environment.", missing)
			}
			for _, kv := range effective {
				name, want, _ := strings.Cut(kv, "=")
				got, ok := actual[name]
				if !ok {
					continue // already reported above
				}
				if got != want {
					t.Errorf("EffectiveEnv says %s=%q and the step received %s=%q; the cache key "+
						"is built from the first", name, want, name, got)
				}
			}
		})
	}
}

// TestEffectiveEnvIsStableForOneDeclaration. It is asked once per cache
// lookup, and a key that moved between two lookups of the same step would
// miss forever with nothing reporting why.
func TestEffectiveEnvIsStableForOneDeclaration(t *testing.T) {
	for _, tg := range targets() {
		t.Run(tg.name, func(t *testing.T) {
			t.Parallel()
			ex, _ := tg.new(t)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			defer cancel()

			declared := []string{"CI=1"}
			first, err := ex.EffectiveEnv(ctx, declared)
			if err != nil {
				t.Fatalf("EffectiveEnv: %v", err)
			}
			second, err := ex.EffectiveEnv(ctx, declared)
			if err != nil {
				t.Fatalf("EffectiveEnv (again): %v", err)
			}
			if strings.Join(first, "\n") != strings.Join(second, "\n") {
				t.Errorf("EffectiveEnv is not stable within a run:\n%v\nthen\n%v", first, second)
			}
		})
	}
}

// TestEffectiveEnvDoesNotMutateTheDeclaredSlice. The slice belongs to the
// plan, and a node's Env is read again on every attempt: an executor that
// appended to it in place would give the second attempt an environment the
// pipeline never declared, and the cache key would move with it.
func TestEffectiveEnvDoesNotMutateTheDeclaredSlice(t *testing.T) {
	for _, tg := range targets() {
		t.Run(tg.name, func(t *testing.T) {
			t.Parallel()
			ex, _ := tg.new(t)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			defer cancel()

			// Capacity to spare, which is what makes an in-place append
			// possible and therefore worth checking for.
			declared := make([]string, 2, 16)
			declared[0], declared[1] = "CI=1", "SENRO_A=b"
			before := strings.Join(declared, "\n")

			if _, err := ex.EffectiveEnv(ctx, declared); err != nil {
				t.Fatalf("EffectiveEnv: %v", err)
			}
			if after := strings.Join(declared, "\n"); after != before {
				t.Errorf("EffectiveEnv rewrote the declared slice: %q became %q", before, after)
			}
			if len(declared) != 2 {
				t.Errorf("the declared slice grew to %d entries", len(declared))
			}
		})
	}
}
