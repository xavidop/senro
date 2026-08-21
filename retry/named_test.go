package retry_test

import (
	"strconv"
	"strings"
	"testing"

	"github.com/xavidop/senro/retry"
)

type httpStatus struct {
	Codes []int `json:"codes"`
}

type weekday struct {
	Day string `json:"day"`
}

func init() {
	retry.RegisterPredicate("namedtest/http-status", func(p httpStatus, a retry.Attempt) bool {
		for _, c := range p.Codes {
			if strings.Contains(a.LogTail, strconv.Itoa(c)) {
				return true
			}
		}
		return false
	})
	retry.RegisterPredicate("namedtest/first-two", func(_ struct{}, a retry.Attempt) bool {
		return a.Number <= 2
	})
	retry.RegisterPredicate("namedtest/day", func(p weekday, _ retry.Attempt) bool {
		return p.Day == "monday"
	})
}

// The point of the whole feature: a Go predicate that a plan can carry,
// unlike retry.Func.
func TestANamedPredicateIsStorable(t *testing.T) {
	p := retry.Named("namedtest/http-status", httpStatus{Codes: []int{502, 503}})
	if err := p.Err(); err != nil {
		t.Fatalf("Named: %v", err)
	}
	if got := p.Serial(); got != `func:namedtest/http-status:{"codes":[502,503]}` {
		t.Errorf("Serial = %q", got)
	}
	if !p.Match(retry.Attempt{LogTail: "got 503 Service Unavailable"}) {
		t.Error("503 must match")
	}
	if p.Match(retry.Attempt{LogTail: "got 404 Not Found"}) {
		t.Error("404 must not match")
	}
}

// A predicate with no parameters records the bare name, so the common case
// does not carry a "null" nobody wrote.
func TestANamedPredicateWithNoParams(t *testing.T) {
	p := retry.Named("namedtest/first-two", nil)
	if err := p.Err(); err != nil {
		t.Fatalf("Named: %v", err)
	}
	if got := p.Serial(); got != "func:namedtest/first-two" {
		t.Errorf("Serial = %q, want the bare name", got)
	}
	if !p.Match(retry.Attempt{Number: 2}) || p.Match(retry.Attempt{Number: 3}) {
		t.Error("first-two must match attempts 1 and 2 only")
	}
}

// Parse is the engine's side: the same behaviour has to come back out of a
// plan, parameters included. Losing the parameters while keeping the name
// is the failure mode that matters, so the assertion is on Match.
func TestParseRoundTripsANamedPredicate(t *testing.T) {
	for _, params := range []any{
		httpStatus{Codes: []int{502, 503}},
		nil,
	} {
		orig := retry.Named("namedtest/http-status", params)
		if err := orig.Err(); err != nil {
			t.Fatalf("Named: %v", err)
		}
		back, err := retry.Parse(orig.Serial())
		if err != nil {
			t.Fatalf("Parse(%q): %v", orig.Serial(), err)
		}
		if back.Serial() != orig.Serial() {
			t.Errorf("Serial = %q, want %q", back.Serial(), orig.Serial())
		}
		hit := retry.Attempt{LogTail: "got 503 Service Unavailable"}
		if back.Match(hit) != orig.Match(hit) {
			t.Errorf("params %v: Match disagreed after a round trip", params)
		}
	}
}

// Object keys are sorted by the canonicalization, so two pipelines writing
// the same parameters produce the same plan and therefore the same digest.
func TestNamedParamsAreCanonical(t *testing.T) {
	type twoFields struct {
		B int `json:"b"`
		A int `json:"a"`
	}
	retry.RegisterPredicate("namedtest/two", func(twoFields, retry.Attempt) bool { return false })
	p := retry.Named("namedtest/two", twoFields{B: 2, A: 1})
	if got := p.Serial(); got != `func:namedtest/two:{"a":1,"b":2}` {
		t.Errorf("Serial = %q, want object keys sorted", got)
	}
}

// A typo has to be a build error, not a policy that silently never fires.
func TestNamedRefusesAnUnregisteredName(t *testing.T) {
	p := retry.Named("namedtest/nope", nil)
	err := p.Err()
	if err == nil {
		t.Fatal("an unregistered name must be an error")
	}
	if !strings.Contains(err.Error(), "namedtest/nope") {
		t.Errorf("the error must name what was asked for: %v", err)
	}
	if !strings.Contains(err.Error(), "RegisterPredicate") {
		t.Errorf("the error must say how to fix it: %v", err)
	}
	// And it must list what this binary does know, so a typo is one glance.
	if !strings.Contains(err.Error(), "namedtest/http-status") {
		t.Errorf("the error must list the registered names: %v", err)
	}
}

// The same failure, but on the reading side: a plan built by a binary that
// registered a predicate this one does not have.
func TestParseRefusesAnUnregisteredName(t *testing.T) {
	_, err := retry.Parse("func:namedtest/absent")
	if err == nil {
		t.Fatal("parsing an unregistered predicate must fail")
	}
	if !strings.Contains(err.Error(), "namedtest/absent") {
		t.Errorf("the error must name the predicate: %v", err)
	}
}

// Parameters that will not marshal must not become a predicate that
// silently never matches.
func TestNamedRefusesUnserializableParams(t *testing.T) {
	p := retry.Named("namedtest/day", make(chan int))
	if p.Err() == nil {
		t.Fatal("a parameter that cannot be marshalled must be an error")
	}
	if p.Serial() != "" {
		t.Errorf("a failed predicate must not be storable, got %q", p.Serial())
	}
}

// A name with ':' in it would re-parse as a name plus parameters.
func TestNamedRefusesAColonInTheName(t *testing.T) {
	if retry.Named("bad:name", nil).Err() == nil {
		t.Error("a ':' in a predicate name must be refused")
	}
}

// The whole reason Named exists rather than Func: it composes, and the
// composite is still storable.
func TestNamedComposesWithAny(t *testing.T) {
	p := retry.Any(
		retry.OnInfra(),
		retry.Named("namedtest/http-status", httpStatus{Codes: []int{503}}),
	)
	if err := p.Err(); err != nil {
		t.Fatalf("Any: %v", err)
	}
	if p.Serial() == "" {
		t.Fatal("Any over storable predicates must itself be storable")
	}
	back, err := retry.Parse(p.Serial())
	if err != nil {
		t.Fatalf("Parse(%q): %v", p.Serial(), err)
	}
	hit := retry.Attempt{LogTail: "503 Service Unavailable"}
	if !back.Match(hit) {
		t.Error("the parsed composite lost its named half")
	}
}

// A broken component fails the composite rather than being dropped, or the
// build succeeds with a policy nobody asked for.
func TestAnyPropagatesANamedError(t *testing.T) {
	p := retry.Any(retry.OnInfra(), retry.Named("namedtest/nope", nil))
	if p.Err() == nil {
		t.Fatal("Any must carry a component's construction error")
	}
	if !strings.Contains(p.Err().Error(), "namedtest/nope") {
		t.Errorf("the error must name the broken component: %v", p.Err())
	}
}

// Registration is init-time API: a duplicate name makes every plan naming
// it ambiguous, so it panics rather than silently taking the last one.
func TestRegisterPredicateRefusesADuplicate(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Error("registering a name twice must panic")
		}
	}()
	retry.RegisterPredicate("namedtest/http-status", func(struct{}, retry.Attempt) bool { return true })
}

func TestRegisterPredicateRefusesAColonInTheName(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Error("a ':' in a registered name must panic")
		}
	}()
	retry.RegisterPredicate("bad:name", func(struct{}, retry.Attempt) bool { return true })
}

// A recorded parameter field the struct does not have must not be read as a
// reason to retry: no match, never a blind yes.
func TestNamedDoesNotMatchOnUndecodableParams(t *testing.T) {
	p, err := retry.Parse(`func:namedtest/day:{"weekday":"monday"}`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if p.Match(retry.Attempt{Number: 1}) {
		t.Error("parameters that do not decode must not produce a match")
	}
}
