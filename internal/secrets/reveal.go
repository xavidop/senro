package secrets

import (
	"fmt"
	"reflect"

	"github.com/xavidop/mamori/secret"
)

// maxDepth bounds the walk. A config struct nested deeper than this is a
// mistake worth naming rather than a shape to support, and the bound removes
// any question about what a pathological type could do to run start.
const maxDepth = 8

// revealer and byteRevealer are the two shapes a self-redacting value takes.
// sensitive is mamori's marker, and a field must satisfy it AND one of the
// revealers to count, which is what keeps a plain `DeployEnv string` with a
// source tag out of the redactor. A DeployEnv of "staging" registered as a
// secret would redact the word "staging" out of every log in the run.
type (
	revealer     interface{ Reveal() string }
	byteRevealer interface{ Reveal() []byte }
	sensitive    interface{ Sensitive() bool }
)

// These four lines are the whole of senro's PRODUCTION coupling to mamori,
// deliberately compile-time: a future mamori changing Reveal's receiver or
// signature stops this file compiling instead of silently reclassifying
// every credential as configuration. Reveal's VALUE receiver is what lets
// the walk call it on a non-addressable field.
var (
	_ revealer     = secret.String{}
	_ sensitive    = secret.String{}
	_ byteRevealer = secret.Bytes{}
	_ sensitive    = secret.Bytes{}
)

// FromConfig walks the struct mamori.Load returned and collects every
// secret in it: the whole seam between mamori and senro's redaction. This
// is the only place Reveal() should appear; reveal_static_test.go at the
// repository root mechanises that grep and fails the build on a second call
// site.
//
// cfg may be a struct or a pointer to one; anything else is refused by
// name, rather than a run that starts with an empty set and delivers
// nothing.
func FromConfig(cfg any) (*Set, error) {
	if cfg == nil {
		return nil, fmt.Errorf("secrets: WithSecrets was given nil; pass the struct mamori.Load returned")
	}
	rv := reflect.ValueOf(cfg)
	for rv.Kind() == reflect.Pointer {
		if rv.IsNil() {
			return nil, fmt.Errorf("secrets: WithSecrets was given a nil %s", rv.Type())
		}
		rv = rv.Elem()
	}
	if rv.Kind() != reflect.Struct {
		return nil, fmt.Errorf(
			"secrets: WithSecrets takes the struct mamori.Load returned; got %s", rv.Type())
	}
	s := &Set{}
	if err := s.walk(rv, "", 0); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *Set) walk(rv reflect.Value, prefix string, depth int) error {
	if depth > maxDepth {
		return fmt.Errorf("secrets: config struct nests more than %d levels deep at %q", maxDepth, prefix)
	}
	rt := rv.Type()
	for i := 0; i < rt.NumField(); i++ {
		f := rt.Field(i)
		name := prefix + f.Name
		tag := f.Tag.Get("source")

		if !f.IsExported() {
			// reflect cannot read this field's value, so a secret here can be
			// neither delivered nor redacted. Refusing by name beats skipping
			// silently: the author believes it is covered, and it is not.
			if tag != "" {
				return fmt.Errorf(
					"secrets: config field %q is unexported but declares source:%q; "+
						"senro cannot read it, so it can be neither delivered nor redacted; "+
						"export the field", name, tag)
			}
			continue
		}

		fv := rv.Field(i)
		if v, ok := revealValue(fv); ok {
			if len(v) == 0 {
				// An optional secret nobody set. Not an error: a step that
				// references it gets a named refusal at run start, and a
				// step that does not is unaffected.
				continue
			}
			if !s.add(&Secret{Name: name, Source: sourceIdentity(tag), value: v}) {
				// Two fields resolved to one name, usually two embedded
				// structs promoting the same bare name (which Go allows to
				// declare). Dropping the second would leave it neither
				// delivered nor redacted while its author believes it is
				// both.
				return fmt.Errorf(
					"secrets: two secrets both resolve to the name %q; "+
						"rename one of the colliding fields or its enclosing struct", name)
			}
			continue
		}

		// Not a secret. Recurse, because mamori itself recurses into an
		// untagged nested struct, so a grouped config would otherwise be
		// populated by mamori and invisible to senro. An embedded field
		// keeps the current prefix, matching Go's own field promotion.
		sub := prefix
		if !f.Anonymous {
			sub = name + "."
		}
		switch fv.Kind() {
		case reflect.Struct:
			if err := s.walk(fv, sub, depth+1); err != nil {
				return err
			}
		case reflect.Pointer:
			if !fv.IsNil() && fv.Elem().Kind() == reflect.Struct {
				if err := s.walk(fv.Elem(), sub, depth+1); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// revealValue is THE ONE PLACE in senro where a secret value is taken out of
// its wrapper. Nothing else in this repository may call Reveal; see
// FromConfig's doc and reveal_static_test.go.
func revealValue(fv reflect.Value) ([]byte, bool) {
	if !fv.CanInterface() {
		return nil, false
	}
	// Value receivers put Reveal and Sensitive in a pointer type's method
	// set too, so a nil *secret.String still satisfies the assertion below
	// and calling Sensitive() on it would dereference nil. Refusing here is
	// the same "optional secret nobody set" outcome, one step earlier.
	if fv.Kind() == reflect.Pointer && fv.IsNil() {
		return nil, false
	}
	iv := fv.Interface()
	sens, ok := iv.(sensitive)
	if !ok || !sens.Sensitive() {
		return nil, false
	}
	switch r := iv.(type) {
	case revealer:
		return []byte(r.Reveal()), true
	case byteRevealer:
		return append([]byte(nil), r.Reveal()...), true
	}
	return nil, false
}
