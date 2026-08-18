package retry_test

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/xavidop/senro/internal/executor"
	"github.com/xavidop/senro/retry"
)

// The distinction the whole retry model rests on: infrastructure failed, or
// the workload returned a verdict. Retrying the second deletes information.
func TestOnInfra(t *testing.T) {
	infra := retry.Attempt{Number: 1, Err: fmt.Errorf("ssh reset: %w", executor.ErrInfra)}
	if !retry.OnInfra().Match(infra) {
		t.Error("an infrastructure failure must be retryable")
	}

	verdict := retry.Attempt{Number: 1, ExitCode: 1}
	if retry.OnInfra().Match(verdict) {
		t.Error("a non-zero exit is the workload's verdict, not an infrastructure failure")
	}

	// An ordinary error that is not ErrInfra is also not retryable by OnInfra.
	other := retry.Attempt{Number: 1, Err: errors.New("go test failed")}
	if retry.OnInfra().Match(other) {
		t.Error("a plain error must not match OnInfra")
	}

	// A lexical match would pass every other assertion in this test. Only an
	// error whose *message* matches while its *chain* does not can tell
	// errors.Is from strings.Contains.
	lookalike := retry.Attempt{Number: 1, Err: errors.New("infrastructure failure")}
	if retry.OnInfra().Match(lookalike) {
		t.Error("OnInfra must match the error chain, not the error text")
	}

	if got := retry.OnInfra().Serial(); got != "infra" {
		t.Errorf("Serial() = %q, want %q", got, "infra")
	}
}

func TestOnExitCode(t *testing.T) {
	p := retry.OnExitCode(75, 111)
	if !p.Match(retry.Attempt{ExitCode: 75}) {
		t.Error("75 should match")
	}
	if p.Match(retry.Attempt{ExitCode: 1}) {
		t.Error("1 should not match")
	}
	// Exit 0 is success and must never be treated as retryable, even if listed.
	if retry.OnExitCode(0).Match(retry.Attempt{ExitCode: 0}) {
		t.Error("exit 0 is success; it must never match a retry predicate")
	}

	if got := p.Serial(); got != "exit_code:75,111" {
		t.Errorf("Serial() = %q, want %q", got, "exit_code:75,111")
	}
}

// The plan's digest hashes Serial, so two calls that describe the same set
// of codes (in a different order, or with a repeat) must produce the same
// string, or two behaviourally identical policies would look different.
func TestOnExitCodeSerialIsOrderAndDuplicateIndependent(t *testing.T) {
	a := retry.OnExitCode(75, 111).Serial()
	b := retry.OnExitCode(111, 75).Serial()
	c := retry.OnExitCode(75, 75, 111, 111).Serial()
	if a != "exit_code:75,111" {
		t.Fatalf("Serial() = %q, want %q", a, "exit_code:75,111")
	}
	if a != b {
		t.Errorf("order changed Serial(): %q vs %q", a, b)
	}
	if a != c {
		t.Errorf("duplicates changed Serial(): %q vs %q", a, c)
	}
}

func TestOnLogMatch(t *testing.T) {
	p, err := retry.OnLogMatch(`connection refused`)
	if err != nil {
		t.Fatalf("OnLogMatch: %v", err)
	}
	if !p.Match(retry.Attempt{ExitCode: 1, LogTail: "dial tcp: connection refused\n"}) {
		t.Error("should match the log tail")
	}
	if p.Match(retry.Attempt{ExitCode: 1, LogTail: "assertion failed\n"}) {
		t.Error("should not match unrelated output")
	}
	if _, err := retry.OnLogMatch(`([`); err == nil {
		t.Error("an invalid pattern must be rejected at construction, not at retry time")
	}

	if got := p.Serial(); got != "log_match:connection refused" {
		t.Errorf("Serial() = %q, want %q", got, "log_match:connection refused")
	}
}

func TestAnyComposes(t *testing.T) {
	p := retry.Any(retry.OnInfra(), retry.OnExitCode(75))
	if !p.Match(retry.Attempt{ExitCode: 75}) {
		t.Error("Any should match its second predicate")
	}
	if p.Match(retry.Attempt{ExitCode: 1}) {
		t.Error("Any should not match when no predicate does")
	}

	if got := p.Serial(); got != `any:["infra","exit_code:75"]` {
		t.Errorf("Serial() = %q, want %q", got, `any:["infra","exit_code:75"]`)
	}
}

// Func adapts a bare closure. It still works as a Predicate (Match still
// runs it) but it has no serialized form, since there is nothing to
// derive one from beyond the closure itself.
func TestFuncHasNoSerial(t *testing.T) {
	p := retry.Func(func(a retry.Attempt) bool { return a.ExitCode == 42 })
	if !p.Match(retry.Attempt{ExitCode: 42}) {
		t.Error("Func's closure must still run")
	}
	if p.Match(retry.Attempt{ExitCode: 1}) {
		t.Error("Func's closure must still run")
	}
	if got := p.Serial(); got != "" {
		t.Errorf("Serial() = %q, want empty — Func has nothing to serialize", got)
	}
}

// The zero Predicate (a struct field or var that was declared but never
// assigned) must not panic. "Match nothing" is the safe reading of "no
// predicate was ever specified."
func TestZeroPredicateMatchesNothingAndHasNoSerial(t *testing.T) {
	var p retry.Predicate
	if p.Match(retry.Attempt{ExitCode: 1, Err: errors.New("boom")}) {
		t.Error("the zero Predicate must not match anything")
	}
	if got := p.Serial(); got != "" {
		t.Errorf("Serial() = %q, want empty", got)
	}
}

// A composite is only as storable as its least storable part: if even one
// sub-predicate has no serialized form, Any's result must not have one
// either: there is no way to write down "some of this, but not which."
func TestAnyWithAnUnserializableComponentHasNoSerial(t *testing.T) {
	custom := retry.Func(func(a retry.Attempt) bool { return true })
	p := retry.Any(retry.OnInfra(), custom)

	// The composite must still work at run time...
	if !p.Match(retry.Attempt{ExitCode: 1}) {
		t.Error("Any must still match through its Func component")
	}
	// ...it just cannot be written into a plan.
	if got := p.Serial(); got != "" {
		t.Errorf("Serial() = %q, want empty", got)
	}
}

// An empty retry.Any() must have no serialized form: "any:[]" would look
// storable to senro.Build but Parse refuses it, and a plan that builds and
// then cannot be executed is worse than one Build refuses outright.
func TestAnyWithNoComponentsHasNoSerial(t *testing.T) {
	p := retry.Any()
	if got := p.Serial(); got != "" {
		t.Errorf("Serial() = %q, want empty — Any with no components cannot be written into a plan", got)
	}
	// It must still behave safely at run time: zero components match nothing.
	if p.Match(retry.Attempt{ExitCode: 1, Err: errors.New("boom")}) {
		t.Error("Any with no components must not match anything")
	}
}

// Parse is Serial's inverse: the engine reconstructs a Predicate from a
// plan's stored string. It must round-trip every constructor's behaviour,
// not just the string: a round-trip that keeps the string and silently
// changes what matches is worse than an error.
func TestParseRoundTripsEveryConstructor(t *testing.T) {
	cases := []struct {
		name    string
		build   func() (retry.Predicate, error)
		match   retry.Attempt // Match(this) must be true on the original
		nomatch retry.Attempt // Match(this) must be false on the original
	}{
		{
			name:    "infra",
			build:   func() (retry.Predicate, error) { return retry.OnInfra(), nil },
			match:   retry.Attempt{Err: fmt.Errorf("ssh reset: %w", executor.ErrInfra)},
			nomatch: retry.Attempt{ExitCode: 1},
		},
		{
			name:    "exit_code",
			build:   func() (retry.Predicate, error) { return retry.OnExitCode(75, 111), nil },
			match:   retry.Attempt{ExitCode: 111},
			nomatch: retry.Attempt{ExitCode: 1},
		},
		{
			name:    "log_match",
			build:   func() (retry.Predicate, error) { return retry.OnLogMatch("connection refused") },
			match:   retry.Attempt{LogTail: "dial tcp: connection refused"},
			nomatch: retry.Attempt{LogTail: "assertion failed"},
		},
		{
			name: "any",
			build: func() (retry.Predicate, error) {
				return retry.Any(retry.OnInfra(), retry.OnExitCode(75)), nil
			},
			match:   retry.Attempt{ExitCode: 75},
			nomatch: retry.Attempt{ExitCode: 1},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			p, err := c.build()
			if err != nil {
				t.Fatalf("build: %v", err)
			}
			// Guard the test fixture itself: if these don't hold, the case is
			// broken and the assertions below prove nothing.
			if !p.Match(c.match) {
				t.Fatalf("test bug: the original predicate does not match c.match")
			}
			if p.Match(c.nomatch) {
				t.Fatalf("test bug: the original predicate matches c.nomatch")
			}

			parsed, err := retry.Parse(p.Serial())
			if err != nil {
				t.Fatalf("Parse(%q): %v", p.Serial(), err)
			}
			if got := parsed.Serial(); got != p.Serial() {
				t.Errorf("Serial round-trip: got %q, want %q", got, p.Serial())
			}
			if got, want := parsed.Match(c.match), p.Match(c.match); got != want {
				t.Errorf("Match round-trip disagreed on a matching attempt: parsed=%v original=%v", got, want)
			}
			if got, want := parsed.Match(c.nomatch), p.Match(c.nomatch); got != want {
				t.Errorf("Match round-trip disagreed on a non-matching attempt: parsed=%v original=%v", got, want)
			}
		})
	}
}

func TestParseRejectsUnrecognizedInput(t *testing.T) {
	for _, s := range []string{"", "sorcery", "exit_code:not-a-number", "any:", "log_match"} {
		if _, err := retry.Parse(s); err == nil {
			t.Errorf("Parse(%q) must fail", s)
		}
	}
}

// A component's own Serial can contain "|", ":" and "," (a log_match
// pattern legitimately does). Under a "|"-delimited join for Any's Serial
// these cases would round-trip to the wrong predicate or fail to parse;
// asserting behaviour, not just the string, is what makes that visible.
func TestParseRoundTripsAnyWithTrickyComponents(t *testing.T) {
	cases := []struct {
		name    string
		build   func() (retry.Predicate, error)
		match   retry.Attempt
		nomatch retry.Attempt
	}{
		{
			// A log_match pattern that is itself a "|" alternation, composed
			// inside Any: exactly the shape that breaks a "|"-delimited join.
			name: "pipe_in_pattern",
			build: func() (retry.Predicate, error) {
				lm, err := retry.OnLogMatch("connection refused|timed out")
				if err != nil {
					return retry.Predicate{}, err
				}
				return retry.Any(lm, retry.OnInfra()), nil
			},
			match:   retry.Attempt{LogTail: "operation timed out"},
			nomatch: retry.Attempt{ExitCode: 1, LogTail: "assertion failed"},
		},
		{
			// ":" is what exit_code:/log_match:/any: splitting relies on to
			// find the grammar's own prefix; a pattern containing one must
			// not confuse a component nested inside Any either.
			name: "colon_in_pattern",
			build: func() (retry.Predicate, error) {
				lm, err := retry.OnLogMatch("response code: 500")
				if err != nil {
					return retry.Predicate{}, err
				}
				return retry.Any(lm, retry.OnInfra()), nil
			},
			match:   retry.Attempt{LogTail: "got response code: 500 from upstream"},
			nomatch: retry.Attempt{ExitCode: 1, LogTail: "ok"},
		},
		{
			name: "comma_in_pattern",
			build: func() (retry.Predicate, error) {
				lm, err := retry.OnLogMatch("a,b,c")
				if err != nil {
					return retry.Predicate{}, err
				}
				return retry.Any(lm, retry.OnInfra()), nil
			},
			match:   retry.Attempt{LogTail: "found a,b,c in the log"},
			nomatch: retry.Attempt{ExitCode: 1, LogTail: "clean"},
		},
		{
			// Any nested inside Any: the outer component list must be able
			// to carry an inner "any:[...]" serial as one opaque element.
			name: "nested_any",
			build: func() (retry.Predicate, error) {
				inner := retry.Any(retry.OnInfra(), retry.OnExitCode(75))
				lm, err := retry.OnLogMatch("boom")
				if err != nil {
					return retry.Predicate{}, err
				}
				return retry.Any(inner, lm), nil
			},
			match:   retry.Attempt{ExitCode: 75},
			nomatch: retry.Attempt{ExitCode: 1, LogTail: "fine"},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			p, err := c.build()
			if err != nil {
				t.Fatalf("build: %v", err)
			}
			if !p.Match(c.match) {
				t.Fatalf("test bug: the original predicate does not match c.match")
			}
			if p.Match(c.nomatch) {
				t.Fatalf("test bug: the original predicate matches c.nomatch")
			}

			parsed, err := retry.Parse(p.Serial())
			if err != nil {
				t.Fatalf("Parse(%q): %v", p.Serial(), err)
			}
			if got, want := parsed.Match(c.match), p.Match(c.match); got != want {
				t.Errorf("Match round-trip disagreed on a matching attempt: parsed=%v original=%v (serial=%q)",
					got, want, p.Serial())
			}
			if got, want := parsed.Match(c.nomatch), p.Match(c.nomatch); got != want {
				t.Errorf("Match round-trip disagreed on a non-matching attempt: parsed=%v original=%v (serial=%q)",
					got, want, p.Serial())
			}
		})
	}
}

// Jitter is not optional. Without it, 37 fan-out children that all hit a
// throttled registry retry in lockstep at 2s, 4s, 8s: a self-inflicted
// outage on top of the original one.
func TestBackoffIsExponentialAndJittered(t *testing.T) {
	b := retry.Backoff{Base: 100 * time.Millisecond, Max: 10 * time.Second, Factor: 2}

	// rnd is the jitter fraction in [0,1); the caller supplies it so the delay
	// is a pure function and therefore testable.
	if got := b.Delay(1, 0); got != 100*time.Millisecond {
		t.Errorf("attempt 1 with no jitter = %v, want 100ms", got)
	}
	if got := b.Delay(2, 0); got != 200*time.Millisecond {
		t.Errorf("attempt 2 with no jitter = %v, want 200ms", got)
	}
	if got := b.Delay(3, 0); got != 400*time.Millisecond {
		t.Errorf("attempt 3 with no jitter = %v, want 400ms", got)
	}

	// Full jitter spreads the herd across the whole window.
	lo, hi := b.Delay(3, 0), b.Delay(3, 0.999)
	if hi <= lo {
		t.Errorf("jitter must widen the delay: %v vs %v", lo, hi)
	}
	if hi > 800*time.Millisecond {
		t.Errorf("jittered delay %v exceeded twice the base window", hi)
	}
}

func TestBackoffRespectsMax(t *testing.T) {
	b := retry.Backoff{Base: time.Second, Max: 3 * time.Second, Factor: 10}
	if got := b.Delay(5, 0.999); got > 3*time.Second {
		t.Errorf("delay %v exceeded Max", got)
	}
}

func TestZeroBackoffHasSaneDefaults(t *testing.T) {
	// A Policy built with retry.Retry(3, pred) leaves Backoff zero. It must
	// still produce a growing, bounded, non-zero delay rather than hot-looping.
	var b retry.Backoff
	d1, d2 := b.Delay(1, 0), b.Delay(3, 0)
	if d1 <= 0 {
		t.Errorf("zero Backoff produced %v; a retry must not hot-loop", d1)
	}
	if d2 <= d1 {
		t.Errorf("zero Backoff is not growing: %v then %v", d1, d2)
	}
}
