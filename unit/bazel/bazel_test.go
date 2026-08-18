package bazel_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/xavidop/senro/internal/unit"
	"github.com/xavidop/senro/unit/bazel"
)

// workspace is the checked-in fixture: a bzlmod Bazel workspace whose
// packages exercise every rule this graph has.
//
//	//                    a BUILD.bazel at the root
//	//apps/web            a BUILD that loads a macro from //tools
//	//both                BUILD and BUILD.bazel side by side, one package
//	//libs/core           BUILD.bazel
//	//libs/store          BUILD
//	//tools               BUILD, beside the .bzl the macro comes from
//
// and three directories that are deliberately NOT packages: docs (no BUILD
// file), vendor/generated (.bazelignore), and third_party/rules_foo with its
// sub-package (a nested MODULE.bazel, so a different repository).
const workspace = "testdata/workspace"

func units(t *testing.T, g unit.Graph, root string) []unit.Unit {
	t.Helper()
	us, err := g.Units(context.Background(), root)
	if err != nil {
		t.Fatalf("Units: %v", err)
	}
	return us
}

func ids(t *testing.T, g unit.Graph, root string) []string {
	t.Helper()
	us := units(t, g, root)
	out := make([]string, 0, len(us))
	for _, u := range us {
		out = append(out, u.ID)
	}
	return out
}

func same(got, want []string) bool {
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

func write(t *testing.T, files map[string]string) string {
	t.Helper()
	root := t.TempDir()
	for p, body := range files {
		full := filepath.Join(root, filepath.FromSlash(p))
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return root
}

// ─────────────────────────────────────────────────────────────────────────────
// What a unit is.
// ─────────────────────────────────────────────────────────────────────────────

func TestPackagesDiscoversEveryBazelPackage(t *testing.T) {
	got := ids(t, bazel.Packages(), workspace)
	want := []string{".", "apps/web", "both", "libs/core", "libs/store", "tools"}
	if !same(got, want) {
		t.Fatalf("Units = %v, want %v", got, want)
	}
}

// TestAUnitIsNamedByItsPackageLabel: Name is what a command takes.
// `bazel test //apps/web/...` is the step a template writes, and it wants the
// label, not the directory.
func TestAUnitIsNamedByItsPackageLabel(t *testing.T) {
	want := map[string]string{
		".":          "//",
		"apps/web":   "//apps/web",
		"both":       "//both",
		"libs/core":  "//libs/core",
		"libs/store": "//libs/store",
		"tools":      "//tools",
	}
	for _, u := range units(t, bazel.Packages(), workspace) {
		if w, ok := want[u.ID]; !ok {
			t.Errorf("unexpected unit %q", u.ID)
		} else if u.Name != w {
			t.Errorf("unit %q is named %q, want %q", u.ID, u.Name, w)
		}
		if u.Dir != u.ID {
			t.Errorf("unit %q has dir %q; for this graph they are the same directory", u.ID, u.Dir)
		}
	}
}

// TestBothBuildFileNamesAreOnePackage. Bazel accepts BUILD and BUILD.bazel
// and prefers the latter when both are present. Two units for one directory
// would be two steps building the same targets, and, worse, two children with
// the same id.
func TestBothBuildFileNamesAreOnePackage(t *testing.T) {
	got := ids(t, bazel.Packages(), workspace)
	var n int
	for _, id := range got {
		if id == "both" {
			n++
		}
	}
	if n != 1 {
		t.Fatalf("the directory holding BUILD and BUILD.bazel produced %d units, want 1: %v", n, got)
	}
}

// TestADirectoryWithNoBuildFileIsNotAPackage. A directory is a Bazel package
// if and only if it holds a BUILD file. docs/ holds prose.
func TestADirectoryWithNoBuildFileIsNotAPackage(t *testing.T) {
	for _, id := range ids(t, bazel.Packages(), workspace) {
		if id == "docs" {
			t.Fatal("docs/ became a unit; it holds no BUILD file and Bazel does not know it exists")
		}
	}
}

// TestABazelignoredDirectoryIsNotAPackage. .bazelignore is the one file that
// says outright which directories are not part of the build, and Bazel does
// not look inside them at all. A unit for one would be a step for targets no
// label can name.
func TestABazelignoredDirectoryIsNotAPackage(t *testing.T) {
	for _, id := range ids(t, bazel.Packages(), workspace) {
		if strings.HasPrefix(id, "vendor/") {
			t.Fatalf("%q became a unit despite .bazelignore", id)
		}
	}
}

func TestABazelignoreEntryIsARealPrefixAndNotAStringOne(t *testing.T) {
	root := write(t, map[string]string{
		"MODULE.bazel":         `module(name = "a")`,
		".bazelignore":         "vendor\n",
		"vendor/x/BUILD":       "filegroup(name = \"x\")\n",
		"vendored/y/BUILD":     "filegroup(name = \"y\")\n",
		"keep/BUILD":           "filegroup(name = \"keep\")\n",
		"keep/vendor/z/BUILD":  "filegroup(name = \"z\")\n",
		"tools/BUILD.bazel":    "exports_files([])\n",
		"tools/defs/BUILD":     "filegroup(name = \"defs\")\n",
		"tools/defs/rules.bzl": "def r(): pass\n",
	})
	got := ids(t, bazel.Packages(), root)
	want := []string{"keep", "keep/vendor/z", "tools", "tools/defs", "vendored/y"}
	if !same(got, want) {
		t.Fatalf("Units = %v, want %v: an ignored \"vendor\" is that directory, not every "+
			"path whose name starts with those six letters", got, want)
	}
}

// TestANestedRepositoryIsNotThisWorkspacesPackages. A directory holding its
// own MODULE.bazel, WORKSPACE or REPO.bazel is a different repository, and
// //third_party/rules_foo does not resolve from here: Bazel would refuse the
// label, so a step for it could only ever fail.
func TestANestedRepositoryIsNotThisWorkspacesPackages(t *testing.T) {
	for _, id := range ids(t, bazel.Packages(), workspace) {
		if strings.HasPrefix(id, "third_party/") {
			t.Fatalf("%q became a unit; it is inside a nested repository", id)
		}
	}
}

func TestEveryRepositoryBoundaryFileStopsTheWalk(t *testing.T) {
	for _, marker := range []string{
		"WORKSPACE", "WORKSPACE.bazel", "WORKSPACE.bzlmod", "MODULE.bazel", "REPO.bazel",
	} {
		t.Run(marker, func(t *testing.T) {
			root := write(t, map[string]string{
				"MODULE.bazel":          `module(name = "a")`,
				"app/BUILD":             "filegroup(name = \"app\")\n",
				"nested/" + marker:      "\n",
				"nested/BUILD":          "filegroup(name = \"nested\")\n",
				"nested/deep/BUILD":     "filegroup(name = \"deep\")\n",
				"nested/deep/x/BUILD":   "filegroup(name = \"x\")\n",
				"beside/BUILD.bazel":    "filegroup(name = \"beside\")\n",
				"beside/in/BUILD.bazel": "filegroup(name = \"in\")\n",
			})
			got := ids(t, bazel.Packages(), root)
			want := []string{"app", "beside", "beside/in"}
			if !same(got, want) {
				t.Fatalf("Units = %v, want %v", got, want)
			}
		})
	}
}

// TestTheRootIsAUnitOnlyWhenItHasABuildFile. A workspace whose root is an
// aggregator with no targets of its own is ordinary, and a unit for it would
// be a step with nothing to build.
func TestTheRootIsAUnitOnlyWhenItHasABuildFile(t *testing.T) {
	root := write(t, map[string]string{
		"MODULE.bazel": `module(name = "a")`,
		"app/BUILD":    "filegroup(name = \"app\")\n",
	})
	if got := ids(t, bazel.Packages(), root); !same(got, []string{"app"}) {
		t.Fatalf("Units = %v, want just app", got)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// It runs nothing.
// ─────────────────────────────────────────────────────────────────────────────

// TestNoBazelIsNeeded is the whole reason this graph reads the tree instead
// of asking `bazel query`. Emptying PATH takes every executable away,
// including bazel and bazelisk, and the graph is unchanged: a plan computed
// on a laptop with bazel installed and one computed in a container without it
// are the same plan.
func TestNoBazelIsNeeded(t *testing.T) {
	t.Setenv("PATH", "")
	abs, err := filepath.Abs(workspace)
	if err != nil {
		t.Fatal(err)
	}
	got := ids(t, bazel.Packages(), abs)
	want := []string{".", "apps/web", "both", "libs/core", "libs/store", "tools"}
	if !same(got, want) {
		t.Fatalf("Units with an empty PATH = %v, want %v", got, want)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// It refuses to compute an affected set.
// ─────────────────────────────────────────────────────────────────────────────

// TestAffectedIsRefused. A BUILD file is a Starlark program and its deps can
// come from a macro this graph cannot see, so an affected set read out of one
// statically would be missing edges, and a missing edge is a skipped build.
// See the package doc, which argues it at length.
func TestAffectedIsRefused(t *testing.T) {
	_, err := unit.Affected(context.Background(), bazel.Packages(), workspace,
		[]string{"libs/core/core.go"})
	if !errors.Is(err, unit.ErrNoAffectedSet) {
		t.Fatalf("Affected = %v, want ErrNoAffectedSet", err)
	}
	if !strings.Contains(err.Error(), "bazel packages") {
		t.Errorf("the error does not name the graph that refused:\n%v", err)
	}
}

// TestItIsNotAnAffector is the same thing as a type assertion, so a future
// edit that adds Owns and ReverseDeps in good faith fails here and has to
// read the package doc first.
func TestItIsNotAnAffector(t *testing.T) {
	var g unit.Graph = bazel.Packages()
	if _, ok := g.(unit.Affector); ok {
		t.Fatal("bazel implements Affector; see the package doc for why it must not " +
			"until it can answer honestly")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Being wrong out loud.
// ─────────────────────────────────────────────────────────────────────────────

func TestNoWorkspaceMarkerIsAnError(t *testing.T) {
	root := write(t, map[string]string{"app/BUILD": "filegroup(name = \"app\")\n"})
	_, err := bazel.Packages().Units(context.Background(), root)
	if err == nil {
		t.Fatal("Units accepted a tree that is not a Bazel workspace")
	}
	if !strings.Contains(err.Error(), "MODULE.bazel") {
		t.Errorf("the error does not say what marks a workspace root:\n%v", err)
	}
}

func TestNoBuildFileAnywhereIsAnError(t *testing.T) {
	root := write(t, map[string]string{
		"MODULE.bazel":   `module(name = "a")`,
		"docs/design.md": "prose\n",
	})
	_, err := bazel.Packages().Units(context.Background(), root)
	if err == nil {
		t.Fatal("Units returned an empty graph rather than an error")
	}
	if !strings.Contains(err.Error(), "BUILD") {
		t.Errorf("the error does not say what it looked for:\n%v", err)
	}
}

func TestRootThatIsNotADirectoryIsAnError(t *testing.T) {
	root := write(t, map[string]string{"MODULE.bazel": `module(name = "a")`})
	_, err := bazel.Packages().Units(context.Background(), filepath.Join(root, "MODULE.bazel"))
	if err == nil {
		t.Fatal("Units accepted a file as a root")
	}
}

func TestUnitsIsCancellable(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := bazel.Packages().Units(ctx, workspace); !errors.Is(err, context.Canceled) {
		t.Fatalf("Units = %v, want context.Canceled", err)
	}
}

func TestDescribe(t *testing.T) {
	if got := bazel.Packages().Describe(); got != "bazel packages" {
		t.Errorf("Describe() = %q", got)
	}
}

func TestUnitIDsAreSafeForAStepID(t *testing.T) {
	for _, id := range ids(t, bazel.Packages(), workspace) {
		if strings.ContainsAny(id, "[]=,@") {
			t.Errorf("unit id %q would corrupt an expanded step id", id)
		}
	}
}

// TestUnitsAreSortedAndRepeatable. An expansion derives every child step's id
// from the unit set IN ORDER, so an order that varies between two builds of
// one pipeline varies the plan digest and with it every cache key hanging off
// it.
func TestUnitsAreSortedAndRepeatable(t *testing.T) {
	g := bazel.Packages()
	first := ids(t, g, workspace)
	for i := 1; i < len(first); i++ {
		if first[i-1] >= first[i] {
			t.Fatalf("Units is not sorted: %v", first)
		}
	}
	// A second Graph, so this is the discovery order and not the memo's.
	if second := ids(t, bazel.Packages(), workspace); !same(first, second) {
		t.Fatalf("two graphs disagree: %v and %v", first, second)
	}
}
