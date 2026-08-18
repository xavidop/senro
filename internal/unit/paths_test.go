package unit_test

import (
	"testing"

	"github.com/xavidop/senro/internal/unit"
)

func TestCleanRel(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want string
		ok   bool
	}{
		{"a/b.txt", "a/b.txt", true},
		{"./a/b.txt", "a/b.txt", true},
		{"a//b.txt", "a/b.txt", true},
		{"a/./b.txt", "a/b.txt", true},
		{"  a/b.txt  ", "a/b.txt", true},
		{"", "", false},
		{"/etc/passwd", "", false},
		{"..", "", false},
		{"../x", "", false},
		{"a/../../x", "", false},
		{".", "", false},
	} {
		got, ok := unit.CleanRel(tc.in)
		if got != tc.want || ok != tc.ok {
			t.Errorf("CleanRel(%q) = %q, %v; want %q, %v", tc.in, got, ok, tc.want, tc.ok)
		}
	}
}

func TestNearestPrefersTheLongestMatch(t *testing.T) {
	dirs := unit.LongestFirst([]string{"a", "a/b", "c"})
	for _, tc := range []struct{ rel, want string }{
		{"a/x.txt", "a"},
		{"a/b/x.txt", "a/b"},
		{"a/b/c/d/x.txt", "a/b"},
		{"c", "c"},
		{"d/x.txt", ""},
		{"ab/x.txt", ""}, // a prefix of the STRING is not a prefix of the PATH
	} {
		if got := unit.Nearest(dirs, tc.rel); got != tc.want {
			t.Errorf("Nearest(%q) = %q, want %q", tc.rel, got, tc.want)
		}
	}
}

func TestNearestTreatsTheRootAsAnAncestorOfEverything(t *testing.T) {
	dirs := unit.LongestFirst([]string{".", "a"})
	if got := unit.Nearest(dirs, "z/x.txt"); got != "." {
		t.Errorf("Nearest = %q, want the root", got)
	}
	if got := unit.Nearest(dirs, "a/x.txt"); got != "a" {
		t.Errorf("Nearest = %q, want a", got)
	}
}

func units(dirs ...string) []unit.Unit {
	out := make([]unit.Unit, 0, len(dirs))
	for _, d := range dirs {
		out = append(out, unit.Unit{ID: d, Name: d, Dir: d})
	}
	return out
}

func sameIDs(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}

func TestPathOwnerAttributesAFileToItsUnit(t *testing.T) {
	o := unit.NewPathOwner(units("packages/core", "packages/web"))
	if got := o.Owners("packages/web/src/index.ts"); !sameIDs(got, []string{"packages/web"}) {
		t.Errorf("Owners = %v", got)
	}
	if got := o.Owners("packages/core/package.json"); !sameIDs(got, []string{"packages/core"}) {
		t.Errorf("Owners = %v", got)
	}
}

// TestPathOwnerGivesEveryUnitAFileAtTheWorkspaceRoot: the lockfile and
// shared configs live directly in the root and can change what every unit
// builds.
func TestPathOwnerGivesEveryUnitAFileAtTheWorkspaceRoot(t *testing.T) {
	o := unit.NewPathOwner(units("packages/core", "packages/web"))
	for _, f := range []string{"package.json", "pnpm-lock.yaml", "Makefile"} {
		if got := o.Owners(f); !sameIDs(got, []string{"packages/core", "packages/web"}) {
			t.Errorf("Owners(%q) = %v, want every unit", f, got)
		}
	}
}

// TestPathOwnerSaysNothingOwnsAFileUnderNoUnit, which unit.Affected reads as
// "this could have affected anything".
func TestPathOwnerSaysNothingOwnsAFileUnderNoUnit(t *testing.T) {
	o := unit.NewPathOwner(units("packages/core"))
	for _, f := range []string{"docs/design.md", "packages/README.md", ".github/workflows/ci.yml"} {
		if got := o.Owners(f); len(got) != 0 {
			t.Errorf("Owners(%q) = %v, want no owner", f, got)
		}
	}
}

func TestPathOwnerSaysNothingOwnsAPathOutsideTheRoot(t *testing.T) {
	o := unit.NewPathOwner(units("packages/core"))
	for _, f := range []string{"", "/etc/passwd", "../elsewhere/x.ts", ".."} {
		if got := o.Owners(f); len(got) != 0 {
			t.Errorf("Owners(%q) = %v, want no owner", f, got)
		}
	}
}

// TestPathOwnerAnswersForADeletedFile: Owns is path arithmetic and never
// stats, and a deletion's dependents most need rebuilding.
func TestPathOwnerAnswersForADeletedFile(t *testing.T) {
	o := unit.NewPathOwner(units("crates/core"))
	if got := o.Owners("crates/core/src/gone.rs"); !sameIDs(got, []string{"crates/core"}) {
		t.Errorf("Owners = %v", got)
	}
}

// TestPathOwnerWithAUnitAtTheRoot: a workspace root that is itself a package
// owns the subdirectories no other unit claims, exactly as gowork's root
// package does, but a file DIRECTLY in the root still belongs to every unit.
func TestPathOwnerWithAUnitAtTheRoot(t *testing.T) {
	o := unit.NewPathOwner(units(".", "crates/core"))
	if got := o.Owners("src/main.rs"); !sameIDs(got, []string{"."}) {
		t.Errorf("Owners(src/main.rs) = %v, want the root unit", got)
	}
	if got := o.Owners("crates/core/src/lib.rs"); !sameIDs(got, []string{"crates/core"}) {
		t.Errorf("Owners = %v", got)
	}
	if got := o.Owners("Cargo.lock"); !sameIDs(got, []string{".", "crates/core"}) {
		t.Errorf("Owners(Cargo.lock) = %v, want every unit", got)
	}
}

// TestPathOwnerKeepsUnitsOrder: Owns feeds unit.Affected, whose output
// order decides child step ids, so units order, not map order.
func TestPathOwnerKeepsUnitsOrder(t *testing.T) {
	o := unit.NewPathOwner(units("z", "a", "m"))
	if got := o.Owners("root.txt"); !sameIDs(got, []string{"z", "a", "m"}) {
		t.Errorf("Owners = %v, want units order", got)
	}
}

// TestPathOwnerHandlesANestedUnit: a unit inside another unit's directory
// takes its own files, because the nearest ancestor wins.
func TestPathOwnerHandlesANestedUnit(t *testing.T) {
	o := unit.NewPathOwner(units("app", "app/plugin"))
	if got := o.Owners("app/plugin/src/x.ts"); !sameIDs(got, []string{"app/plugin"}) {
		t.Errorf("Owners = %v", got)
	}
	if got := o.Owners("app/src/x.ts"); !sameIDs(got, []string{"app"}) {
		t.Errorf("Owners = %v", got)
	}
}
