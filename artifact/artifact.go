// Package artifact selects the files a step reads and the files it produces.
//
// You cannot hash what you have not declared. Inputs are declared explicitly
// with globs; the Observed mode that learns a read set by watching a run is
// deliberately later, because an implicit input set that changes per run is
// a debugging nightmare.
package artifact

import (
	"fmt"
	"strings"
)

// Selector names files. It carries its own serialized form for the same
// reason retry.Predicate does: a plan is JSON and cannot carry a closure or
// an interface value across the process boundary the engine executes it in.
type Selector struct{ serial string }

// Glob selects every file matching pattern. "*" and "?" match within a path
// segment and "**" spans segments, matching the workspace excluder's syntax
// exactly, so a pattern reads the same wherever it appears in a pipeline.
func Glob(pattern string) Selector { return Selector{serial: "glob:" + pattern} }

// File selects one path.
func File(p string) Selector { return Selector{serial: "file:" + p} }

// Serial is the form a plan records.
func (s Selector) Serial() string { return s.serial }

// Kind is "glob" or "file", and "" for a zero Selector.
func (s Selector) Kind() string {
	k, _, ok := strings.Cut(s.serial, ":")
	if !ok {
		return ""
	}
	return k
}

// Pattern is the selector's text without its kind.
func (s Selector) Pattern() string {
	_, p, _ := strings.Cut(s.serial, ":")
	return p
}

// Parse reads back what Serial wrote. It refuses anything else rather than
// treating an unknown kind as a literal path, which would silently select
// nothing and make a Pure() step's input set empty without saying so.
func Parse(s string) (Selector, error) {
	kind, pattern, ok := strings.Cut(s, ":")
	if !ok {
		return Selector{}, fmt.Errorf("artifact: %q has no kind prefix, want \"glob:\" or \"file:\"", s)
	}
	if pattern == "" {
		return Selector{}, fmt.Errorf("artifact: %q has an empty pattern", s)
	}
	switch kind {
	case "glob", "file":
		return Selector{serial: s}, nil
	default:
		return Selector{}, fmt.Errorf("artifact: unknown selector kind %q in %q, want \"glob\" or \"file\"", kind, s)
	}
}
