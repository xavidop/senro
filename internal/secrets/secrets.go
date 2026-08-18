// Package secrets holds a run's resolved credentials.
//
// This package resolves nothing: mamori does, before senro.Run is called,
// and this package takes the values out of the struct mamori.Load returned.
// senro defines no provider interface, hence no registry, pinning or watch
// loop anywhere.
//
// A value is unexported inside a Secret and leaves this package through
// exactly one accessor, Value, so every route to a plaintext credential is
// one grep away.
package secrets

import (
	"log/slog"
	"sort"

	"github.com/xavidop/senro/internal/redact"
)

// Secret is one resolved credential and the identity of where it came from.
type Secret struct {
	// Name is the Go struct field a step references with SecretEnv. A field
	// inside a named nested struct is qualified with a dot ("Registry.Token");
	// a field promoted from an embedded struct keeps its bare name, matching
	// Go's own promotion.
	Name string
	// Source is the mamori source: tag with any userinfo and any query
	// removed (see sourceIdentity). It is identity, never content, and it is
	// what reaches a secret.resolved event and a cache key's Secrets
	// component.
	Source string
	// Version is the provider's version for this value. Always "": mamori
	// surfaces Value.Version to a provider, not to Load's caller. Declared
	// because the cache key's secret identity keys on it and
	// api.SecretResolvedBody already publishes the field.
	Version string

	value []byte
}

// String renders a Secret for a %v, a %s and a panic dump without its value.
func (s Secret) String() string { return s.Name + "=" + redact.Placeholder }

// LogValue keeps a Secret out of a structured log line, the same way mamori's
// own secret.String does.
func (s Secret) LogValue() slog.Value {
	return slog.GroupValue(slog.String("name", s.Name), slog.String("source", s.Source))
}

// Identity is a Secret with no value at all: the form that leaves this
// package for an event payload or a cache key, making "no value here" a
// property of the type rather than of a caller's discipline.
type Identity struct {
	Name    string `json:"name"`
	Source  string `json:"source,omitempty"`
	Version string `json:"version,omitempty"`
}

// String renders an Identity, which by construction has nothing to hide.
func (i Identity) String() string { return i.Name + " " + i.Source }

// Set is a run's resolved secrets. The nil *Set is a run with none, and every
// method treats it as empty so the engine's call sites never branch.
type Set struct {
	order  []*Secret
	byName map[string]*Secret
}

// add registers sec and reports whether it was newly added. A duplicate
// name becomes the caller's loud refusal: keeping only the first of two
// colliding names would leave the second neither delivered nor redacted,
// while its author believes it is both.
func (s *Set) add(sec *Secret) bool {
	if s.byName == nil {
		s.byName = make(map[string]*Secret)
	}
	if _, dup := s.byName[sec.Name]; dup {
		return false
	}
	s.byName[sec.Name] = sec
	s.order = append(s.order, sec)
	return true
}

// Len is how many secrets resolved to a non-empty value.
func (s *Set) Len() int {
	if s == nil {
		return 0
	}
	return len(s.order)
}

// Has reports whether name resolved to a value, which is what the engine's
// reference check asks before a step declares it needs one.
func (s *Set) Has(name string) bool {
	if s == nil {
		return false
	}
	_, ok := s.byName[name]
	return ok
}

// Names is every resolved secret's name, sorted, so an error message that
// lists them is stable.
func (s *Set) Names() []string {
	if s == nil || len(s.order) == 0 {
		return nil
	}
	out := make([]string, 0, len(s.order))
	for _, sec := range s.order {
		out = append(out, sec.Name)
	}
	sort.Strings(out)
	return out
}

// Identities is every secret's identity, sorted by name. This is what the
// engine emits as secret.resolved and what the cache key builder folds in.
func (s *Set) Identities() []Identity {
	if s == nil || len(s.order) == 0 {
		return nil
	}
	out := make([]Identity, 0, len(s.order))
	for _, sec := range s.order {
		out = append(out, Identity{Name: sec.Name, Source: sec.Source, Version: sec.Version})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// Identity looks one secret's identity up by name, for the cache key
// builder, which needs source and version beside a digest of the value and
// must never hold the two in one struct.
func (s *Set) Identity(name string) (Identity, bool) {
	if s == nil {
		return Identity{}, false
	}
	sec, ok := s.byName[name]
	if !ok {
		return Identity{}, false
	}
	return Identity{Name: sec.Name, Source: sec.Source, Version: sec.Version}, true
}

// Value is the one accessor that hands back a plaintext credential.
//
// It has THREE callers, all in package engine, none keeping a copy beyond
// immediate use; TestSetValueCallSitesAreAllowlisted (reveal_static_test.go
// at the repository root) pins them by name, so a fourth caller fails the
// build. It returns a copy, so a caller that mutates or retains the slice
// cannot reach back into this Set.
func (s *Set) Value(name string) ([]byte, bool) {
	if s == nil {
		return nil, false
	}
	sec, ok := s.byName[name]
	if !ok {
		return nil, false
	}
	return append([]byte(nil), sec.value...), true
}

// RedactValues is the seed for the run's redactor: every value labelled by
// name, so redact.Set.Match can say which secret it found without printing
// it. Called once in engine.Run, before the first event, so redaction is
// live before anything can write to the ledger.
func (s *Set) RedactValues() []redact.Value {
	if s == nil || len(s.order) == 0 {
		return nil
	}
	out := make([]redact.Value, 0, len(s.order))
	for _, sec := range s.order {
		out = append(out, redact.Value{Label: sec.Name, Value: sec.value})
	}
	return out
}
