package jswork_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/xavidop/senro/internal/unit"
	"github.com/xavidop/senro/unit/jswork"
)

// Two checked-in fixtures, because the two halves of this ecosystem declare
// their members in two different files. Both are the same shape, four
// packages and a chain three deep, each resolved by its own manager:
//
//	@acme/core  <-  @acme/store  <-  @acme/web       (dependencies)
//	@acme/testkit  <-  @acme/web                     (a DEV dependency)
//
// pnpm-workspace declares members in pnpm-workspace.yaml, and its root
// package.json has no "workspaces" field at all: the ordinary pnpm layout,
// the one a package.json-only reader finds nothing in. npm-workspace
// declares them in the root package.json's "workspaces" array, which Yarn
// and Bun also read.
//
// Both exclude a package on disk with a "!" pattern. In npm-workspace the
// store's DIRECTORY is packages/data-store while its NAME is @acme/store,
// because the two differ often enough to matter and the edge is on the name.
const (
	pnpmWorkspace = "testdata/pnpm-workspace"
	npmWorkspace  = "testdata/npm-workspace"
)

// fixture is one of the two, with the ids its own layout produces.
type fixture struct {
	name string
	root string
	// units are every unit id, sorted, as Units reports them.
	units []string
	// leaf, mid and top are the three-deep chain: top depends on mid, mid
	// depends on leaf.
	leaf, mid, top string
	// devOnly is the package nothing but a devDependency reaches.
	devOnly string
	// excluded is the package on disk that a "!" pattern keeps out.
	excluded string
	// rootFiles are the files at the workspace root that belong to every
	// package, this manager's lockfile among them.
	rootFiles []string
}

var fixtures = []fixture{
	{
		name:      "pnpm",
		root:      pnpmWorkspace,
		units:     []string{"apps/web", "packages/core", "packages/store", "packages/testkit"},
		leaf:      "packages/core",
		mid:       "packages/store",
		top:       "apps/web",
		devOnly:   "packages/testkit",
		excluded:  "packages/deprecated",
		rootFiles: []string{"package.json", "pnpm-lock.yaml", "pnpm-workspace.yaml"},
	},
	{
		name:      "npm",
		root:      npmWorkspace,
		units:     []string{"apps/web", "packages/core", "packages/data-store", "packages/testkit"},
		leaf:      "packages/core",
		mid:       "packages/data-store",
		top:       "apps/web",
		devOnly:   "packages/testkit",
		excluded:  "packages/deprecated",
		rootFiles: []string{"package.json", "package-lock.json"},
	},
}

func ids(t *testing.T, g unit.Graph, root string) []string {
	t.Helper()
	us, err := g.Units(context.Background(), root)
	if err != nil {
		t.Fatalf("Units: %v", err)
	}
	out := make([]string, 0, len(us))
	for _, u := range us {
		out = append(out, u.ID)
	}
	return out
}

func affected(t *testing.T, g unit.Graph, root string, files ...string) []string {
	t.Helper()
	res, err := unit.Affected(context.Background(), g, root, files)
	if err != nil {
		t.Fatalf("Affected(%v): %v", files, err)
	}
	out := make([]string, 0, len(res.Units))
	for _, u := range res.Units {
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

// sorted is the unit order Affected reports, which is the order Units used.
func sorted(ids ...string) []string {
	out := append([]string(nil), ids...)
	sort.Strings(out)
	return out
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

// Every case below runs against BOTH fixtures. A graph that only worked on
// the pnpm layout, or only on the npm one, would pass half of these and ship
// broken for half the ecosystem.
func TestPackagesDiscoversEveryWorkspacePackage(t *testing.T) {
	for _, f := range fixtures {
		t.Run(f.name, func(t *testing.T) {
			if got := ids(t, jswork.Packages(), f.root); !same(got, f.units) {
				t.Fatalf("Units = %v, want %v", got, f.units)
			}
		})
	}
}

// TestANegatedPatternExcludesAPackage: both npm and pnpm honour a "!" pattern
// and leave the package out of the workspace, and a graph that ignored the
// "!" would fan a step out over a package nobody installs.
func TestANegatedPatternExcludesAPackage(t *testing.T) {
	for _, f := range fixtures {
		t.Run(f.name, func(t *testing.T) {
			for _, id := range ids(t, jswork.Packages(), f.root) {
				if id == f.excluded {
					t.Fatalf("%s is a unit; the workspace excludes it", f.excluded)
				}
			}
		})
	}
}

// TestAUnitIsNamedByItsPackageName, which is NOT its directory name: in the
// npm fixture packages/data-store is called @acme/store, and the edges are on
// the name.
func TestAUnitIsNamedByItsPackageName(t *testing.T) {
	want := map[string]map[string]string{
		"pnpm": {
			"apps/web": "@acme/web", "packages/core": "@acme/core",
			"packages/store": "@acme/store", "packages/testkit": "@acme/testkit",
		},
		"npm": {
			"apps/web": "@acme/web", "packages/core": "@acme/core",
			"packages/data-store": "@acme/store", "packages/testkit": "@acme/testkit",
		},
	}
	for _, f := range fixtures {
		t.Run(f.name, func(t *testing.T) {
			us, err := jswork.Packages().Units(context.Background(), f.root)
			if err != nil {
				t.Fatal(err)
			}
			for _, u := range us {
				if want[f.name][u.ID] != u.Name {
					t.Errorf("unit %q is named %q, want %q", u.ID, u.Name, want[f.name][u.ID])
				}
				if u.Dir != u.ID {
					t.Errorf("unit %q has Dir %q", u.ID, u.Dir)
				}
			}
		})
	}
}

// TestAffectedIsTransitiveOverThreeUnits is the case the whole feature turns
// on: web depends on store, store depends on core, and a change to core has
// to run all three. The pnpm fixture writes its ranges as "workspace:*" and
// "workspace:^", the npm one as "^0.1.0" and "*", and neither of those is
// something a version-range reader gets to discard.
func TestAffectedIsTransitiveOverThreeUnits(t *testing.T) {
	for _, f := range fixtures {
		t.Run(f.name, func(t *testing.T) {
			got := affected(t, jswork.Packages(), f.root, f.leaf+"/src/index.js")
			want := sorted(f.leaf, f.mid, f.top)
			if !same(got, want) {
				t.Fatalf("Affected(%s) = %v, want %v", f.leaf, got, want)
			}
		})
	}
}

// TestAffectedSeesADevDependency: nothing but web's devDependencies mentions
// testkit, and a test-only edge is still an edge.
func TestAffectedSeesADevDependency(t *testing.T) {
	for _, f := range fixtures {
		t.Run(f.name, func(t *testing.T) {
			got := affected(t, jswork.Packages(), f.root, f.devOnly+"/src/index.js")
			want := sorted(f.devOnly, f.top)
			if !same(got, want) {
				t.Fatalf("Affected(%s) = %v, want %v", f.devOnly, got, want)
			}
		})
	}
}

func TestAffectedDoesNotRunUnitsDownstreamOfNothing(t *testing.T) {
	for _, f := range fixtures {
		t.Run(f.name, func(t *testing.T) {
			got := affected(t, jswork.Packages(), f.root, f.top+"/src/index.js")
			if want := []string{f.top}; !same(got, want) {
				t.Fatalf("Affected(%s) = %v, want %v", f.top, got, want)
			}
		})
	}
}

// TestTheRootManifestAndLockfileAffectEverything. Both decide what every
// package installs, and each fixture carries its own manager's lockfile.
func TestTheRootManifestAndLockfileAffectEverything(t *testing.T) {
	for _, f := range fixtures {
		t.Run(f.name, func(t *testing.T) {
			for _, file := range f.rootFiles {
				res, err := unit.Affected(context.Background(), jswork.Packages(), f.root,
					[]string{file})
				if err != nil {
					t.Fatal(err)
				}
				if !res.All || res.Total != len(f.units) {
					t.Errorf("Affected(%q) = %d of %d units (all=%v), want every one",
						file, len(res.Units), res.Total, res.All)
				}
			}
		})
	}
}

func TestAFileUnderNoPackageAffectsEverything(t *testing.T) {
	for _, f := range fixtures {
		t.Run(f.name, func(t *testing.T) {
			res, err := unit.Affected(context.Background(), jswork.Packages(), f.root,
				[]string{"docs/design.md"})
			if err != nil {
				t.Fatal(err)
			}
			if !res.All {
				t.Fatalf("Affected(docs/design.md) = %v, want everything", res.Units)
			}
		})
	}
}

// TestAChangeToAnExcludedPackageAffectsEverything: the excluded package is not
// a unit, so no unit owns its files, and "no owner" is exactly the answer that
// means "this could have changed anything".
func TestAChangeToAnExcludedPackageAffectsEverything(t *testing.T) {
	for _, f := range fixtures {
		t.Run(f.name, func(t *testing.T) {
			res, err := unit.Affected(context.Background(), jswork.Packages(), f.root,
				[]string{f.excluded + "/package.json"})
			if err != nil {
				t.Fatal(err)
			}
			if !res.All {
				t.Fatalf("Affected(an excluded package) = %v, want everything", res.Units)
			}
		})
	}
}

func TestOwnsAnswersForADeletedFile(t *testing.T) {
	for _, f := range fixtures {
		t.Run(f.name, func(t *testing.T) {
			got, err := jswork.Packages().Owns(context.Background(), f.root,
				[]string{f.mid + "/src/gone.js"})
			if err != nil {
				t.Fatal(err)
			}
			if len(got) != 1 || !same(got[0], []string{f.mid}) {
				t.Fatalf("Owns = %v", got)
			}
		})
	}
}

func TestReverseDepsAreDirectAndDeterministic(t *testing.T) {
	for _, f := range fixtures {
		t.Run(f.name, func(t *testing.T) {
			rd, err := jswork.Packages().ReverseDeps(context.Background(), f.root)
			if err != nil {
				t.Fatal(err)
			}
			if !same(rd[f.leaf], []string{f.mid}) {
				t.Errorf("ReverseDeps[%q] = %v, want %v", f.leaf, rd[f.leaf], f.mid)
			}
			if !same(rd[f.mid], []string{f.top}) {
				t.Errorf("ReverseDeps[%q] = %v, want %v", f.mid, rd[f.mid], f.top)
			}
			if !same(rd[f.devOnly], []string{f.top}) {
				t.Errorf("ReverseDeps[%q] = %v, want %v", f.devOnly, rd[f.devOnly], f.top)
			}
			if len(rd[f.top]) != 0 {
				t.Errorf("ReverseDeps[%q] = %v, want nothing", f.top, rd[f.top])
			}
			// The closure is unit.Affected's job. An implementation that
			// pre-flattened its edges here would hide a bug in that closure.
			if len(rd[f.leaf]) > 1 {
				t.Errorf("ReverseDeps[%q] = %v; the edges must be direct only", f.leaf, rd[f.leaf])
			}
		})
	}
}

// TestAnNpmWorkspacesArray is the other half of the ecosystem: npm and Yarn
// declare members in the root package.json, not in a file of their own. The
// array here is the one `npm init -w packages/core` writes.
func TestAnNpmWorkspacesArray(t *testing.T) {
	root := write(t, map[string]string{
		"package.json":            `{"name":"root","private":true,"workspaces":["packages/*"]}`,
		"packages/a/package.json": `{"name":"a","version":"1.0.0"}`,
		"packages/b/package.json": `{"name":"b","version":"1.0.0","dependencies":{"a":"^1.0.0"}}`,
		"package-lock.json":       `{"lockfileVersion":3}`,
	})
	g := jswork.Packages()
	if got := ids(t, g, root); !same(got, []string{"packages/a", "packages/b"}) {
		t.Fatalf("Units = %v", got)
	}
	if got := affected(t, g, root, "packages/a/index.js"); !same(got,
		[]string{"packages/a", "packages/b"}) {
		t.Fatalf("Affected = %v", got)
	}
}

// TestAYarnWorkspacesObject: Yarn v1 wraps the same list in an object so it
// can carry nohoist beside it.
func TestAYarnWorkspacesObject(t *testing.T) {
	root := write(t, map[string]string{
		"package.json": `{"name":"root","private":true,
			"workspaces":{"packages":["packages/*"],"nohoist":["**/react-native"]}}`,
		"packages/a/package.json": `{"name":"a","version":"1.0.0"}`,
		"yarn.lock":               "# yarn lockfile v1\n",
	})
	if got := ids(t, jswork.Packages(), root); !same(got, []string{"packages/a"}) {
		t.Fatalf("Units = %v", got)
	}
}

// TestEveryDependencyFieldIsAnEdge. All four of them, because each describes
// a package that has to be rebuilt when the one it names changes: a peer
// dependency says so in as many words, and an optional dependency that IS
// installed is an ordinary dependency.
func TestEveryDependencyFieldIsAnEdge(t *testing.T) {
	for _, field := range []string{
		"dependencies", "devDependencies", "peerDependencies", "optionalDependencies",
	} {
		t.Run(field, func(t *testing.T) {
			root := write(t, map[string]string{
				"package.json":            `{"name":"root","private":true,"workspaces":["packages/*"]}`,
				"packages/a/package.json": `{"name":"a","version":"1.0.0"}`,
				"packages/b/package.json": `{"name":"b","version":"1.0.0","` + field +
					`":{"a":"^1.0.0"}}`,
			})
			if got := affected(t, jswork.Packages(), root, "packages/a/index.js"); !same(got,
				[]string{"packages/a", "packages/b"}) {
				t.Fatalf("Affected = %v, want both", got)
			}
		})
	}
}

// TestAWorkspaceProtocolRangeIsAnEdge. pnpm and Yarn Berry write
// "workspace:*", "workspace:^" and "workspace:~", and a reader that tried to
// parse those as a semver range and discarded what it could not understand
// would drop every intra-repository edge in a pnpm monorepo.
func TestAWorkspaceProtocolRangeIsAnEdge(t *testing.T) {
	for _, spec := range []string{"workspace:*", "workspace:^", "workspace:~", "workspace:^1.0.0"} {
		t.Run(spec, func(t *testing.T) {
			root := write(t, map[string]string{
				"pnpm-workspace.yaml":     "packages:\n  - packages/*\n",
				"package.json":            `{"name":"root","private":true}`,
				"packages/a/package.json": `{"name":"a","version":"1.0.0"}`,
				"packages/b/package.json": `{"name":"b","version":"1.0.0","dependencies":{"a":"` +
					spec + `"}}`,
			})
			if got := affected(t, jswork.Packages(), root, "packages/a/index.js"); !same(got,
				[]string{"packages/a", "packages/b"}) {
				t.Fatalf("Affected = %v, want both", got)
			}
		})
	}
}

// TestBothDeclarationsAreUnioned: a repository migrating between package
// managers carries a "workspaces" array AND a pnpm-workspace.yaml for a
// while, and reading only one of them would come out missing half its
// packages.
func TestBothDeclarationsAreUnioned(t *testing.T) {
	root := write(t, map[string]string{
		"package.json":            `{"name":"root","private":true,"workspaces":["packages/*"]}`,
		"pnpm-workspace.yaml":     "packages:\n  - \"apps/*\"\n",
		"packages/a/package.json": `{"name":"a","version":"1.0.0"}`,
		"apps/web/package.json":   `{"name":"web","version":"1.0.0"}`,
	})
	if got := ids(t, jswork.Packages(), root); !same(got, []string{"apps/web", "packages/a"}) {
		t.Fatalf("Units = %v, want both declarations honoured", got)
	}
}

// TestAFlowSequenceOfPackages: the other way a short pnpm member list gets
// written, negation included.
func TestAFlowSequenceOfPackages(t *testing.T) {
	root := write(t, map[string]string{
		"package.json":              `{"name":"root","private":true}`,
		"pnpm-workspace.yaml":       "packages: [\"packages/*\", '!packages/old']\n",
		"packages/a/package.json":   `{"name":"a","version":"1.0.0"}`,
		"packages/old/package.json": `{"name":"old","version":"1.0.0"}`,
	})
	if got := ids(t, jswork.Packages(), root); !same(got, []string{"packages/a"}) {
		t.Fatalf("Units = %v", got)
	}
}

// TestYamlThisReaderDoesNotUnderstandIsRefused. This is a reader for one key
// of one file, not a YAML implementation, and the failure mode of guessing is
// a member list that comes out short and a build that skips what it did not
// see. Anything outside the shape it reads is an error naming the file.
func TestYamlThisReaderDoesNotUnderstandIsRefused(t *testing.T) {
	for _, tc := range []struct{ name, yaml string }{
		{"an anchor reference", "defaults: &d\n  - packages/*\npackages: *d\n"},
		{"a nested mapping", "packages:\n  include:\n    - packages/*\n"},
		{"a block scalar", "packages: |\n  packages/*\n"},
		{"an alias in the list", "base: &b packages/*\npackages:\n  - *b\n"},
		{"a plain scalar", "packages: packages/*\n"},
		{"no packages key at all", "catalog:\n  react: ^18.0.0\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := write(t, map[string]string{
				"package.json":            `{"name":"root","private":true}`,
				"pnpm-workspace.yaml":     tc.yaml,
				"packages/a/package.json": `{"name":"a","version":"1.0.0"}`,
			})
			_, err := jswork.Packages().Units(context.Background(), root)
			if err == nil {
				t.Fatalf("Units returned no error for %q; guessing at a member list is how a "+
					"build silently skips a package", tc.yaml)
			}
			if !strings.Contains(err.Error(), "pnpm-workspace.yaml") {
				t.Errorf("error %q does not name the file", err)
			}
		})
	}
}

// TestAnAliasedDependencyIsAnEdge: `"legacy": "npm:@acme/core@^1"` installs a
// workspace package under a different local name, and only the alias target
// names it.
func TestAnAliasedDependencyIsAnEdge(t *testing.T) {
	root := write(t, map[string]string{
		"package.json":            `{"name":"root","private":true,"workspaces":["packages/*"]}`,
		"packages/a/package.json": `{"name":"@acme/core","version":"1.0.0"}`,
		"packages/b/package.json": `{"name":"b","version":"1.0.0","dependencies":{"legacy":"npm:@acme/core@^1.0.0"}}`,
	})
	if got := affected(t, jswork.Packages(), root, "packages/a/index.js"); !same(got,
		[]string{"packages/a", "packages/b"}) {
		t.Fatalf("Affected = %v, want both", got)
	}
}

// TestAFileProtocolDependencyIsAnEdge: `"core": "file:../a"` names a
// directory rather than a package, and the directory is what resolves it.
func TestAFileProtocolDependencyIsAnEdge(t *testing.T) {
	root := write(t, map[string]string{
		"package.json":            `{"name":"root","private":true,"workspaces":["packages/*"]}`,
		"packages/a/package.json": `{"name":"@acme/core","version":"1.0.0"}`,
		"packages/b/package.json": `{"name":"b","version":"1.0.0","dependencies":{"core":"link:../a"}}`,
	})
	if got := affected(t, jswork.Packages(), root, "packages/a/index.js"); !same(got,
		[]string{"packages/a", "packages/b"}) {
		t.Fatalf("Affected = %v, want both", got)
	}
}

// TestAPackageInsideNodeModulesIsNotAUnit. An installed tree holds tens of
// thousands of package.json files, and a graph that walked into it would turn
// a six-package monorepo into a plan too wide to build.
func TestAPackageInsideNodeModulesIsNotAUnit(t *testing.T) {
	root := write(t, map[string]string{
		"package.json":            `{"name":"root","private":true,"workspaces":["packages/**"]}`,
		"packages/a/package.json": `{"name":"a","version":"1.0.0"}`,
		"packages/a/node_modules/left-pad/package.json": `{"name":"left-pad","version":"1.3.0"}`,
	})
	if got := ids(t, jswork.Packages(), root); !same(got, []string{"packages/a"}) {
		t.Fatalf("Units = %v, want only the workspace package", got)
	}
}

func TestNoWorkspaceDeclarationIsAnError(t *testing.T) {
	root := write(t, map[string]string{
		"package.json": `{"name":"solo","version":"1.0.0"}`,
	})
	_, err := jswork.Packages().Units(context.Background(), root)
	if err == nil {
		t.Fatal("Units of a single-package tree returned no error")
	}
	if !strings.Contains(err.Error(), "workspaces") {
		t.Errorf("error %q does not say what it looked for", err)
	}
}

// TestAWorkspaceFileWithNoPackagesKeyIsAnError, rather than an empty graph: a
// pnpm-workspace.yaml this reader could not find the member list in is a
// reader that would silently produce no units at all.
func TestAWorkspaceFileWithNoPackagesKeyIsAnError(t *testing.T) {
	root := write(t, map[string]string{
		"package.json":        `{"name":"root","private":true}`,
		"pnpm-workspace.yaml": "catalog:\n  react: ^18.0.0\n",
	})
	if _, err := jswork.Packages().Units(context.Background(), root); err == nil {
		t.Fatal("Units returned no error for a workspace file with no packages key")
	}
}

// TestAMalformedManifestIsAnError, not a smaller graph.
func TestAMalformedManifestIsAnError(t *testing.T) {
	root := write(t, map[string]string{
		"package.json":            `{"name":"root","private":true,"workspaces":["packages/*"]}`,
		"packages/a/package.json": `{"name":"a",`,
	})
	_, err := jswork.Packages().Units(context.Background(), root)
	if err == nil {
		t.Fatal("Units over a malformed manifest returned no error")
	}
	if !strings.Contains(err.Error(), "packages/a/package.json") {
		t.Errorf("error %q does not name the file", err)
	}
}

// TestAPatternThatMatchesNothingIsAnError: a workspace declaring
// "packages/*" with no package under it is a typo, and an expansion that
// silently produced no steps is indistinguishable from one whose root was
// wrong.
func TestAPatternThatMatchesNothingIsAnError(t *testing.T) {
	root := write(t, map[string]string{
		"package.json": `{"name":"root","private":true,"workspaces":["packages/*"]}`,
	})
	if _, err := jswork.Packages().Units(context.Background(), root); err == nil {
		t.Fatal("Units returned no error for a workspace with no members")
	}
}

func TestRootThatIsNotADirectoryIsAnError(t *testing.T) {
	root := write(t, map[string]string{"f.txt": "x"})
	if _, err := jswork.Packages().Units(context.Background(), filepath.Join(root, "f.txt")); err == nil {
		t.Fatal("Units of a file returned no error")
	}
}

func TestUnitsIsCancellable(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := jswork.Packages().Units(ctx, pnpmWorkspace); !errors.Is(err, context.Canceled) {
		t.Fatalf("Units on a cancelled context = %v, want context.Canceled", err)
	}
}

func TestDescribe(t *testing.T) {
	if got := jswork.Packages().Describe(); got != "jswork packages" {
		t.Errorf("Describe = %q", got)
	}
}

func TestUnitIDsAreSafeForAStepID(t *testing.T) {
	for _, id := range ids(t, jswork.Packages(), pnpmWorkspace) {
		if strings.ContainsAny(id, "[]=,@") {
			t.Errorf("unit id %q would corrupt an expanded step id", id)
		}
	}
}
