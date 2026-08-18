package unit

import (
	"path"
	"path/filepath"
	"sort"
	"strings"
)

// CleanRel normalises a changed path to the slash-separated, root-relative
// form a graph's maps are keyed by, and reports whether it is inside the
// root at all.
//
// It NEVER touches the filesystem, so a deletion (the change whose
// dependents most need rebuilding) behaves like any other edit. An absolute
// path, an empty one, and one climbing out with ".." are all "not ours",
// which Owns turns into "no owner" and unit.Affected into "run everything".
func CleanRel(p string) (string, bool) {
	p = filepath.ToSlash(strings.TrimSpace(p))
	if p == "" || strings.HasPrefix(p, "/") {
		return "", false
	}
	p = path.Clean(p)
	if p == "." || p == ".." || strings.HasPrefix(p, "../") {
		return "", false
	}
	return p, true
}

// LongestFirst sorts directories for Nearest: longest first, so a nested
// unit's directory is tried before the unit it sits inside.
//
// "." sorts LAST whatever its length: it is an ancestor of every path and
// would otherwise beat another one-character directory on a tiebreak, and a
// root unit swallowing the units under it is a wrong affected set. Other
// ties break by name; that changes no answer, but a deterministic slice is
// one fewer source of nondeterministic unit order.
func LongestFirst(dirs []string) []string {
	out := append([]string(nil), dirs...)
	sort.Slice(out, func(i, j int) bool {
		if (out[i] == ".") != (out[j] == ".") {
			return out[j] == "."
		}
		if len(out[i]) != len(out[j]) {
			return len(out[i]) > len(out[j])
		}
		return out[i] < out[j]
	})
	return out
}

// Nearest is the longest entry of dirs that is rel or a parent of it, or ""
// when none is. dirs must have been through LongestFirst.
//
// "." is the root and is a parent of everything, so a graph that includes it
// gets a catch-all and one that does not gets "no owner".
func Nearest(dirs []string, rel string) string {
	for _, d := range dirs {
		if d == "." || d == rel || strings.HasPrefix(rel, d+"/") {
			return d
		}
	}
	return ""
}

// PathOwner answers Affector.Owns for a graph whose units are directories
// marked by a manifest (a Cargo crate, an npm package). gowork needs more:
// a Go module holds many packages.
//
// Three rules, in order, each ambiguity resolved towards attributing MORE,
// because an unattributed change is a skipped build:
//
//  1. A file DIRECTLY in the workspace root belongs to EVERY unit: the
//     lockfile, workspace manifest and shared configs live there. Holds
//     even when the root is itself a unit; a little over-running buys never
//     mistaking a lockfile for one package's business.
//  2. Otherwise the nearest unit directory at or above the file owns it,
//     giving a NESTED unit its own files rather than its parent's.
//  3. Otherwise NO unit owns it (empty entry), which unit.Affected reads as
//     "this could have affected anything".
//
// A root that is itself a unit is a parent of every path, so rule 2 catches
// everything rule 1 did not and rule 3 never fires.
type PathOwner struct {
	// all is every unit ID in Units order: rule 1's answer, ordered because
	// the ids flow into unit.Affected, whose output order decides child ids.
	all []string
	// dirs are the unit directories, longest first, for Nearest.
	dirs []string
	// byDir maps a unit directory to its ID; usually equal, not required to
	// be.
	byDir map[string]string
}

// NewPathOwner indexes units. It keeps no reference to the slice.
func NewPathOwner(units []Unit) *PathOwner {
	o := &PathOwner{
		all:   make([]string, 0, len(units)),
		dirs:  make([]string, 0, len(units)),
		byDir: make(map[string]string, len(units)),
	}
	for _, u := range units {
		o.all = append(o.all, u.ID)
		if _, seen := o.byDir[u.Dir]; !seen {
			o.byDir[u.Dir] = u.ID
			o.dirs = append(o.dirs, u.Dir)
		}
	}
	o.dirs = LongestFirst(o.dirs)
	return o
}

// Owners is the owning unit IDs of one path, per the three rules above.
func (o *PathOwner) Owners(file string) []string {
	rel, ok := CleanRel(file)
	if !ok {
		return nil
	}
	if path.Dir(rel) == "." {
		return o.all // rule 1
	}
	if d := Nearest(o.dirs, path.Dir(rel)); d != "" {
		return []string{o.byDir[d]} // rule 2
	}
	return nil // rule 3
}

// OwnersOf answers for a whole batch: the shape Affector.Owns is asked in.
func (o *PathOwner) OwnersOf(files []string) [][]string {
	out := make([][]string, len(files))
	for i, f := range files {
		out[i] = o.Owners(f)
	}
	return out
}
