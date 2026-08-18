// Package unit is what an expansion expands over.
//
// Two interfaces, not one: Graph has Units, all static fan-out needs;
// Affector adds Owns and ReverseDeps, implemented only by a graph with a
// basis for answering them (gowork does, via the Go toolchain; glob does
// not, since matching paths says nothing about imports). Split because
// Graph is published as senro.UnitGraph, and a published interface that
// grows a method breaks every outside implementation. Units takes a context
// because gowork shells out to `go list` over a whole workspace, exactly
// the call a person interrupts.
package unit

import (
	"context"
	"path"

	"github.com/xavidop/senro/artifact"
)

// Unit is one thing an expansion produces a step for: a Go module or package,
// a Rust crate, a pnpm workspace package, a Maven or Gradle project, a Python
// distribution, a Bazel package, or a plain directory.
type Unit struct {
	// ID is the unit's stable identity, landing in the child step's
	// identifier ("verify/lint[unit=apps/web]"). Stable across runs and
	// machines, which is why the glob graph uses a root-relative slash path.
	ID string
	// Name is what a tool calls this unit: a pnpm package name, a module path.
	// The glob graph has no source for one and sets it to ID.
	Name string
	// Dir is the unit's directory, relative to the root the graph was given,
	// with forward slashes on every platform.
	Dir string
}

// Base is the last path segment of the unit's directory, which is what a
// deployment usually names: "web" for "apps/web".
func (u Unit) Base() string { return path.Base(u.Dir) }

// Sources selects every file under the unit's directory: the declaration a
// Pure() template needs so its cache key moves with the unit's own files. A
// template needing something narrower declares its own Inputs.
func (u Unit) Sources() []artifact.Selector {
	if u.Dir == "" || u.Dir == "." {
		return []artifact.Selector{artifact.Glob("**")}
	}
	return []artifact.Selector{artifact.Glob(u.Dir + "/**")}
}

// Shard is one bucket of a partitioned expansion: the units that landed in it,
// and where it sits among its siblings. It is what a partitioned expansion's
// template is called with, once per bucket, in place of a single Unit.
type Shard struct {
	// Index is this shard's position, from zero, and the only thing reaching
	// its step's identifier ("test[shard=0]"). Deliberately not derived from
	// the CONTENTS: contents move with recorded durations, and an identifier
	// moving with them would take every cache key along.
	Index int
	// Total is how many shards this expansion produced, which is
	// min(requested, number of units) and depends on the unit set alone.
	Total int
	// Units are this shard's units, in unit order.
	Units []Unit
}

// IDs are the shard's units' ids, in unit order, which is what a command that
// takes a list of packages wants.
func (s Shard) IDs() []string {
	out := make([]string, 0, len(s.Units))
	for _, u := range s.Units {
		out = append(out, u.ID)
	}
	return out
}

// Names are the shard's units' names, in unit order: the pnpm package names,
// the module paths.
func (s Shard) Names() []string {
	out := make([]string, 0, len(s.Units))
	for _, u := range s.Units {
		out = append(out, u.Name)
	}
	return out
}

// Dirs are the shard's units' directories, in unit order.
func (s Shard) Dirs() []string {
	out := make([]string, 0, len(s.Units))
	for _, u := range s.Units {
		out = append(out, u.Dir)
	}
	return out
}

// Sources selects every file under every one of the shard's units. The key
// therefore moves when the PARTITION moves, which is honest: a step running
// three modules is not the step that ran two, and a surviving key would
// hand back the output of work never done.
func (s Shard) Sources() []artifact.Selector {
	out := make([]artifact.Selector, 0, len(s.Units))
	for _, u := range s.Units {
		out = append(out, u.Sources()...)
	}
	return out
}

// Graph discovers units. A Graph that can also answer ownership and
// dependency questions implements Affector as well; see Affector for why
// that is a second interface.
type Graph interface {
	// Units reports every unit under root, in a deterministic order: child
	// identifiers derive from it and must not differ between two builds.
	Units(ctx context.Context, root string) ([]Unit, error)
	// Describe names this graph for an error message and a readable plan:
	// "glob dirs apps/*".
	Describe() string
}
