// Package retry is the policy a pipeline author writes against to decide
// whether a failed step is worth running again.
//
// A step can fail because the substrate it ran on broke, or because the
// workload itself returned a verdict; internal/executor keeps those two
// facts separate on purpose (Sandbox.Run returns an exit code and an error,
// never collapsed into one). This package lets a predicate act on that
// distinction: retrying a non-zero exit until it happens to pass is not
// resilience, it is deleting the information the workload just gave you.
package retry

import (
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/xavidop/senro/internal/executor"
)

// Attempt describes one finished try of a step, as seen by a Predicate.
// ExitCode is the workload's verdict; Err is set when the attempt failed to
// run at all (an infrastructure failure, wrapping executor.ErrInfra, or
// something else). LogTail is the last portion of the step's combined
// output, kept small deliberately: a predicate is a classifier, not a log
// viewer.
type Attempt struct {
	Number   int
	ExitCode int
	Err      error
	LogTail  string
}

// Predicate decides whether a failed attempt is worth retrying.
//
// It carries its own serialized form: a Plan is JSON and cannot carry a
// closure across the process boundary the engine executes it in, so Serial
// is what actually crosses it. A predicate built from a bare function has
// no serialized form and cannot be stored in a plan; see Func.
//
// Identity comparison of closures is not a safe substitute: two different
// OnExitCode(...) calls can observably share a reflect pointer despite
// carrying different codes. The serialized form is decided once at
// construction, when the arguments are still in scope.
type Predicate struct {
	match  func(Attempt) bool
	serial string
	// err is a construction failure carried to the point a plan is built,
	// the way senro's own funcAction carries one: Named cannot return an
	// error without breaking every call site that passes a Predicate
	// straight into Retry, and a predicate whose parameters would not
	// marshal must not become one that silently never matches.
	err error
}

// Match reports whether the attempt is retryable. A zero Predicate matches
// nothing rather than panicking on a nil match function: the zero value is
// what a Predicate field looks like before anything sets it, and "never
// retry" is the safer reading of "nothing was ever specified" than a crash
// partway through a run.
func (p Predicate) Match(a Attempt) bool {
	if p.match == nil {
		return false
	}
	return p.match(a)
}

// Serial is the plan-storable form of p ("infra", "exit_code:75,111", and
// so on), or "" if p cannot be stored in a plan at all. That is exactly the
// predicates Func builds, and any Any whose composition includes one: there
// is no way to write down "retry on something, but I can't say what."
func (p Predicate) Serial() string { return p.serial }

// Err reports a failure that happened while p was being constructed, which
// Build surfaces rather than executing a policy that does not do what was
// asked. Nil for every predicate built successfully.
func (p Predicate) Err() error { return p.err }

// Func adapts a bare function into a Predicate. The result has no
// serialized form, so a step using it cannot be built into a plan through
// senro's builder: Build refuses it rather than silently dropping the
// closure and building a policy that does not do what was asked. Use Func
// only for a Policy applied directly, in code that never crosses the plan
// boundary.
func Func(f func(Attempt) bool) Predicate {
	return Predicate{match: f}
}

// Policy is what a pipeline step retries against: how many times, how long
// to wait between tries, and which failures qualify at all.
type Policy struct {
	MaxAttempts int
	Backoff     Backoff
	On          Predicate
}

// OnInfra matches only infrastructure failures (an SSH reset, an image
// that will not pull, a pod evicted out from under the step). These are
// retryable without any judgement about the work itself: the substrate
// failed the step, not the other way around.
//
// A non-zero exit is never matched here, deliberately. It is the workload's
// verdict, and OnInfra's whole job is to leave that verdict alone.
func OnInfra() Predicate {
	return Predicate{
		match:  func(a Attempt) bool { return executor.IsInfra(a.Err) },
		serial: "infra",
	}
}

// OnExitCode matches attempts whose exit code is one of codes.
//
// Exit 0 is success, never a failure to retry: the retry loop should never
// even ask about a successful attempt, but OnExitCode(0) is a predicate a
// pipeline author could still hand it by mistake, so it is refused here
// rather than trusted to say yes.
//
// codes is deduplicated and sorted before it becomes part of Serial, so two
// calls naming the same set serialize identically: a plan's digest must
// depend on what the pipeline does, not on how the argument list was
// written.
func OnExitCode(codes ...int) Predicate {
	set := make(map[int]bool, len(codes))
	for _, c := range codes {
		if c == 0 {
			continue
		}
		set[c] = true
	}
	sorted := make([]int, 0, len(set))
	for c := range set {
		sorted = append(sorted, c)
	}
	sort.Ints(sorted)

	strs := make([]string, len(sorted))
	for i, c := range sorted {
		strs[i] = strconv.Itoa(c)
	}
	return Predicate{
		match:  func(a Attempt) bool { return a.ExitCode != 0 && set[a.ExitCode] },
		serial: "exit_code:" + strings.Join(strs, ","),
	}
}

// OnLogMatch matches attempts whose LogTail contains a match for pattern.
// The regexp is compiled here, at construction, so a broken pattern fails
// when the plan is built rather than on a failing host at 3am.
//
// Treat this as a last resort. Matching on log text couples a retry
// decision to a message that someone will eventually reword, at which point
// the retry silently stops firing. Prefer OnInfra or OnExitCode wherever the
// failure can be identified structurally instead.
func OnLogMatch(pattern string) (Predicate, error) {
	re, err := regexp.Compile(pattern)
	if err != nil {
		return Predicate{}, err
	}
	return Predicate{
		match:  func(a Attempt) bool { return re.MatchString(a.LogTail) },
		serial: "log_match:" + pattern,
	}, nil
}

// Any matches an Attempt that any of preds matches.
//
// The result's Serial is "any:" followed by its parts' own Serials,
// JSON-encoded as an array, but only if every part has one: a composite
// with even one unstorable part (see Func) cannot be stored either.
//
// JSON rather than a "|" join, because a component's own Serial can contain
// "|" (a log_match pattern legitimately does): "any:log_match:a|b|infra"
// re-parses as three components instead of two, silently truncating the
// pattern. A JSON array element may contain any character.
func Any(preds ...Predicate) Predicate {
	match := func(a Attempt) bool {
		for _, p := range preds {
			if p.Match(a) {
				return true
			}
		}
		return false
	}

	if len(preds) == 0 {
		// An empty Any must have no serialized form: falling through would
		// produce "any:[]", which Build accepts as storable but Parse
		// refuses, so the plan could never be executed. Same treatment as
		// retry.Func: a working match (matches nothing) and no serial.
		return Predicate{match: match}
	}

	names := make([]string, len(preds))
	for i, p := range preds {
		// A component's construction error is the composite's too, and it
		// travels rather than being swallowed: Any(OnInfra(), Named("typo",
		// nil)) must fail the build naming "typo", not quietly become a
		// predicate that only matches infrastructure failures.
		if err := p.Err(); err != nil {
			return Predicate{match: match, err: err}
		}
		s := p.Serial()
		if s == "" {
			return Predicate{match: match}
		}
		names[i] = s
	}
	// []string of arbitrary text always marshals, but the check stays so a
	// future change to what's stored fails loudly instead of silently
	// producing a broken plan.
	encoded, err := json.Marshal(names)
	if err != nil {
		return Predicate{match: match}
	}
	return Predicate{match: match, serial: "any:" + string(encoded)}
}

// Parse reconstructs a Predicate from its serialized form, the inverse of
// Serial. Kept beside the constructors so the writer and the reader of this
// grammar cannot drift apart.
//
// Parse must round-trip every constructor: Parse(p.Serial()) has to behave
// the same as p. TestParseRoundTripsEveryConstructor checks Match, not just
// Serial: preserving the string while losing the behaviour is the failure
// mode that actually matters.
func Parse(s string) (Predicate, error) {
	switch {
	case s == "infra":
		return OnInfra(), nil

	case strings.HasPrefix(s, "exit_code:"):
		rest := strings.TrimPrefix(s, "exit_code:")
		var codes []int
		if rest != "" {
			for _, part := range strings.Split(rest, ",") {
				c, err := strconv.Atoi(part)
				if err != nil {
					return Predicate{}, fmt.Errorf("retry: parse %q: invalid exit code %q: %w", s, part, err)
				}
				codes = append(codes, c)
			}
		}
		return OnExitCode(codes...), nil

	case strings.HasPrefix(s, "log_match:"):
		return OnLogMatch(strings.TrimPrefix(s, "log_match:"))

	case strings.HasPrefix(s, "func:"):
		return parseNamed(s)

	case strings.HasPrefix(s, "any:"):
		rest := strings.TrimPrefix(s, "any:")
		var names []string
		if err := json.Unmarshal([]byte(rest), &names); err != nil {
			return Predicate{}, fmt.Errorf("retry: parse %q: invalid any component list: %w", s, err)
		}
		if len(names) == 0 {
			return Predicate{}, fmt.Errorf("retry: parse %q: any with no sub-predicates", s)
		}
		preds := make([]Predicate, len(names))
		for i, name := range names {
			p, err := Parse(name)
			if err != nil {
				return Predicate{}, err
			}
			preds[i] = p
		}
		return Any(preds...), nil

	default:
		return Predicate{}, fmt.Errorf("retry: parse %q: unrecognized predicate", s)
	}
}

// Default backoff parameters, applied when a Backoff's field is the zero
// value. They exist so that retry.Policy{MaxAttempts: 3, On: retry.OnInfra()}
// (a Backoff nobody filled in) still waits between attempts instead of
// hot-looping.
const (
	defaultBase   = 500 * time.Millisecond
	defaultFactor = 2
	defaultMax    = 30 * time.Second
)

// Backoff computes the delay between retry attempts as exponential growth
// from Base by Factor, capped at Max.
//
// The zero Backoff is valid: Delay resolves Base, Factor and Max to sane
// defaults itself rather than requiring a constructor, so a Policy built
// with a partially-filled or zero Backoff still behaves.
type Backoff struct {
	Base   time.Duration
	Max    time.Duration
	Factor float64
}

// Delay returns how long to wait before the given attempt (1-based). rnd is
// the jitter fraction in [0, 1); the caller supplies it (the engine passes
// rand.Float64(), tests pass constants), so Delay stays a pure function.
//
// Delay first computes the exponential window
// min(Max, Base*Factor^(attempt-1)), then spreads the actual delay across
// [window, min(Max, 2*window)] using rnd. The floor at window, not zero, is
// deliberate: jitter that can return ~0 defeats the point of backing off.
// Every fan-out sibling still gets a different point in the window, which
// is what actually prevents the thundering herd.
func (b Backoff) Delay(attempt int, rnd float64) time.Duration {
	base, factor, max := b.Base, b.Factor, b.Max
	if base <= 0 {
		base = defaultBase
	}
	if factor <= 0 {
		factor = defaultFactor
	}
	if max <= 0 {
		max = defaultMax
	}

	window := float64(base)
	for i := 1; i < attempt; i++ {
		if window >= float64(max) {
			window = float64(max)
			break
		}
		window *= factor
	}
	if window > float64(max) {
		window = float64(max)
	}

	delay := window * (1 + rnd)
	if delay > float64(max) {
		delay = float64(max)
	}
	return time.Duration(delay)
}
