package retry

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
)

// A registered predicate is a Go function a plan CAN name, because what the
// plan records is the name and the arguments rather than the closure.
//
// Func is the other half of this story and stays what it is: a bare
// function with nothing to write down. RegisterPredicate is what a pipeline
// reaches for when the decision genuinely needs Go, since a name plus
// JSON-encodable parameters survives being written to plan.json and
// reconstructed by the engine in another process, which is the whole reason
// Func cannot be built into a plan.
//
// It is deliberately the same shape as senro.RegisterFunc: register once
// from an init function, name it at the call site, and the name is API.

// rawPredicate is a registered predicate as the registry holds it:
// parameters arrive as JSON, because that is what a plan can carry.
// RegisterPredicate is the typed front door that decodes them.
type rawPredicate func(params json.RawMessage, a Attempt) bool

var (
	namedMu sync.RWMutex
	named   = map[string]rawPredicate{}
)

// RegisterPredicate registers a Go function as a retry predicate, under a
// stable name:
//
//	type HTTPStatus struct {
//		Codes []int `json:"codes"`
//	}
//
//	func init() {
//		retry.RegisterPredicate("http-status", func(p HTTPStatus, a retry.Attempt) bool {
//			for _, c := range p.Codes {
//				if strings.Contains(a.LogTail, strconv.Itoa(c)) {
//					return true
//				}
//			}
//			return false
//		})
//	}
//
//	// in the pipeline
//	verify.Step("publish", exec.Command("./publish.sh")).
//		Retry(3, retry.Named("http-status", HTTPStatus{Codes: []int{502, 503}}))
//
// The name is API, exactly as senro.RegisterFunc's is: it is what plan.json
// records and what the engine looks up to reconstruct the predicate, so
// renaming it breaks any recorded plan that still names it. Registering the
// same name twice panics, as does an empty name, a nil function, or a name
// containing ':' (the separator between the name and its parameters in the
// stored form).
//
// P must be JSON-serializable and is decoded strictly: a recorded parameter
// field that P does not have is an error, which the predicate reports by
// not matching rather than by retrying blind.
//
// The function must not block and must not have side effects. It is asked
// whether a settled attempt is worth running again, inside the engine's
// retry loop, once per failed attempt.
func RegisterPredicate[P any](name string, fn func(P, Attempt) bool) {
	switch {
	case name == "":
		panic("retry: RegisterPredicate with an empty name")
	case strings.Contains(name, ":"):
		panic("retry: RegisterPredicate(" + name + "): a name may not contain ':', " +
			"which separates the name from its parameters in a plan")
	case fn == nil:
		panic("retry: RegisterPredicate(" + name + ", nil)")
	}
	namedMu.Lock()
	defer namedMu.Unlock()
	if _, dup := named[name]; dup {
		panic("retry: RegisterPredicate(" + name + ") called twice; a registered name is stable API")
	}
	named[name] = func(raw json.RawMessage, a Attempt) bool {
		var p P
		if len(raw) > 0 {
			dec := json.NewDecoder(bytes.NewReader(raw))
			dec.DisallowUnknownFields()
			if err := dec.Decode(&p); err != nil {
				// No match, rather than a match: a predicate that cannot
				// read its own parameters has not decided that this
				// failure is worth retrying, and defaulting to "retry" on
				// a decode error is how a broken policy becomes a loop.
				return false
			}
		}
		return fn(p, a)
	}
}

// Named is a predicate registered under name, with params.
//
// Pass nil for a predicate that takes none. params must be JSON-serializable;
// if it is not, the resulting Predicate carries the error and Build reports
// it rather than running a policy that does not do what was asked.
//
//	retry.Named("http-status", HTTPStatus{Codes: []int{502, 503}})
//	retry.Named("weekday-only", nil)
//
// Unlike Func, this composes with everything else: Any(OnInfra(),
// Named("http-status", p)) is storable, because both halves are.
func Named(name string, params any) Predicate {
	if name == "" {
		return Predicate{err: fmt.Errorf("retry: Named with an empty predicate name")}
	}
	if strings.Contains(name, ":") {
		return Predicate{err: fmt.Errorf(
			"retry: Named(%q): a predicate name may not contain ':', which separates the name "+
				"from its parameters in a plan", name)}
	}
	raw, err := canonicalParams(params)
	if err != nil {
		return Predicate{err: fmt.Errorf("retry: Named(%q): %w", name, err)}
	}
	if _, ok := lookupNamed(name); !ok {
		return Predicate{err: fmt.Errorf(
			"retry: Named(%q): no predicate is registered under that name. Call "+
				"retry.RegisterPredicate(%q, ...) in an init function of the package that "+
				"defines it.%s", name, name, registeredHint())}
	}
	return fromNamed(name, raw)
}

// fromNamed builds the Predicate for an already-validated name. The lookup
// is deferred to Match rather than captured here, so a predicate parsed out
// of a plan behaves identically to one written in a pipeline even if the
// registry is still filling up when Parse runs.
func fromNamed(name string, raw json.RawMessage) Predicate {
	serial := "func:" + name
	if len(raw) > 0 && string(raw) != "null" {
		serial += ":" + string(raw)
	} else {
		raw = nil
	}
	return Predicate{
		serial: serial,
		match: func(a Attempt) bool {
			fn, ok := lookupNamed(name)
			if !ok {
				return false
			}
			return fn(raw, a)
		},
	}
}

// parseNamed reconstructs a Named predicate from its stored form,
// "func:<name>" or "func:<name>:<params>". Parse calls it; see Parse.
func parseNamed(s string) (Predicate, error) {
	rest := strings.TrimPrefix(s, "func:")
	name, params, _ := strings.Cut(rest, ":")
	if name == "" {
		return Predicate{}, fmt.Errorf("retry: parse %q: no predicate name", s)
	}
	if _, ok := lookupNamed(name); !ok {
		return Predicate{}, fmt.Errorf(
			"retry: parse %q: no predicate is registered under %q in this binary. A plan names a "+
				"predicate rather than carrying it, so the binary executing the plan has to "+
				"register the same name the binary that built it did.%s", s, name, registeredHint())
	}
	if params != "" && !json.Valid([]byte(params)) {
		return Predicate{}, fmt.Errorf(
			"retry: parse %q: the parameters recorded for predicate %q are not valid JSON", s, name)
	}
	return fromNamed(name, json.RawMessage(params)), nil
}

func lookupNamed(name string) (rawPredicate, bool) {
	namedMu.RLock()
	defer namedMu.RUnlock()
	fn, ok := named[name]
	return fn, ok
}

// registeredHint lists what this binary does know, which is what turns a
// typo from a puzzle into a one-line fix. Empty when nothing is registered:
// "registered: " followed by nothing reads like a bug in the message.
func registeredHint() string {
	namedMu.RLock()
	defer namedMu.RUnlock()
	if len(named) == 0 {
		return ""
	}
	names := make([]string, 0, len(named))
	for n := range named {
		names = append(names, n)
	}
	sort.Strings(names)
	return " Registered in this binary: " + strings.Join(names, ", ") + "."
}

// canonicalParams is plan.CanonicalParams, reimplemented here rather than
// imported: a predicate's parameters are part of a plan's digest, so they
// must encode the same way whoever writes them, and this package is a leaf
// that internal/plan is free to depend on. Marshal, decode into a generic
// value with UseNumber, marshal again: the round trip sorts object keys and
// preserves number literals exactly, so two calls with the same parameters
// produce the same bytes and therefore the same plan.
func canonicalParams(v any) (json.RawMessage, error) {
	if v == nil {
		return nil, nil
	}
	b, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("predicate parameters are not serializable: %w", err)
	}
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.UseNumber()
	var generic any
	if err := dec.Decode(&generic); err != nil {
		return nil, fmt.Errorf("predicate parameters: %w", err)
	}
	out, err := json.Marshal(generic)
	if err != nil {
		return nil, fmt.Errorf("predicate parameters: %w", err)
	}
	return out, nil
}
