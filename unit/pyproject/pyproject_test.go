package pyproject_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/xavidop/senro/internal/unit"
	"github.com/xavidop/senro/unit/pyproject"
)

// workspace is the checked-in fixture: a Python monorepo with all three ways
// a distribution declares itself, which is what a real one looks like.
//
//	pyproject.toml            a VIRTUAL uv workspace root: members and tool
//	                          configuration, not a distribution
//	packages/{core,store,web} PEP 621 [project] distributions
//	packages/legacy           on disk, excluded by the workspace
//	services/reports          a Poetry distribution
//	services/worker           setuptools, declared in setup.cfg
//
// Every manifest in it parses with Python's own tomllib, and `poetry check`
// passes on the Poetry one.
const workspace = "testdata/workspace"

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

func TestPackagesDiscoversEveryDistribution(t *testing.T) {
	got := ids(t, pyproject.Packages(), workspace)
	want := []string{
		"packages/core", "packages/store", "packages/web",
		"services/reports", "services/worker",
	}
	if !same(got, want) {
		t.Fatalf("Units = %v, want %v", got, want)
	}
}

// TestAVirtualWorkspaceRootIsNotAUnit. The root pyproject.toml here holds
// [tool.uv.workspace] and [tool.ruff] and no [project] at all: it configures
// the build and is not a thing to build.
func TestAVirtualWorkspaceRootIsNotAUnit(t *testing.T) {
	for _, id := range ids(t, pyproject.Packages(), workspace) {
		if id == "." {
			t.Fatal("the virtual workspace root is a unit; it declares no distribution")
		}
	}
}

// TestAnExcludedMemberIsNotAUnit: [tool.uv.workspace] exclude is the one
// explicit statement the manifests make that a directory is out.
func TestAnExcludedMemberIsNotAUnit(t *testing.T) {
	for _, id := range ids(t, pyproject.Packages(), workspace) {
		if id == "packages/legacy" {
			t.Fatal("packages/legacy is a unit; the workspace excludes it")
		}
	}
}

// TestAUnitIsNamedByItsDistributionName, from whichever of the three places
// this repository's manifests declare it.
func TestAUnitIsNamedByItsDistributionName(t *testing.T) {
	us, err := pyproject.Packages().Units(context.Background(), workspace)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{
		"packages/core":    "acme-core",    // [project] name
		"packages/store":   "acme-store",   // [project] name
		"packages/web":     "acme-web",     // [project] name
		"services/reports": "acme-reports", // [tool.poetry] name
		"services/worker":  "acme-worker",  // setup.cfg [metadata] name
	}
	for _, u := range us {
		if want[u.ID] != u.Name {
			t.Errorf("unit %q is named %q, want %q", u.ID, u.Name, want[u.ID])
		}
		if u.Dir != u.ID {
			t.Errorf("unit %q has Dir %q", u.ID, u.Dir)
		}
	}
}

// TestAffectedIsRefused is the whole point of this package's shape. It
// implements Graph and NOT Affector, deliberately, so an expansion that asks
// it to narrow a fan-out is told no at build time rather than handed a
// plausible-looking answer that silently misses the distribution a change
// broke.
func TestAffectedIsRefused(t *testing.T) {
	_, err := unit.Affected(context.Background(), pyproject.Packages(), workspace,
		[]string{"packages/core/src/acme_core/__init__.py"})
	if !errors.Is(err, unit.ErrNoAffectedSet) {
		t.Fatalf("Affected = %v, want ErrNoAffectedSet", err)
	}
	if !strings.Contains(err.Error(), "pyproject") {
		t.Errorf("error %q does not name the graph", err)
	}
}

// TestItIsNotAnAffector is the same thing as a type assertion, so a future
// change that adds Owns and ReverseDeps to this graph has to delete this test
// on purpose rather than by accident.
func TestItIsNotAnAffector(t *testing.T) {
	var g unit.Graph = pyproject.Packages()
	if _, ok := g.(unit.Affector); ok {
		t.Fatal("pyproject implements Affector; see the package doc for why it must not " +
			"unless the reasoning there has genuinely stopped being true")
	}
}

// TestASetupPyOnlyDistributionIsNamedAfterItsDirectory. The name is an
// argument to a function call inside a Python program, and reading it would
// mean running that program; the directory is the honest answer.
func TestASetupPyOnlyDistributionIsNamedAfterItsDirectory(t *testing.T) {
	root := write(t, map[string]string{
		"tools/linter/setup.py": "from setuptools import setup\n\nsetup(name=\"acme-linter\")\n",
	})
	us, err := pyproject.Packages().Units(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	if len(us) != 1 || us[0].ID != "tools/linter" || us[0].Name != "linter" {
		t.Fatalf("Units = %+v, want one unit named after its directory", us)
	}
}

// TestARootThatIsItselfADistributionIsAUnit, unlike the virtual root in the
// checked-in fixture.
func TestARootThatIsItselfADistributionIsAUnit(t *testing.T) {
	root := write(t, map[string]string{
		"pyproject.toml": "[project]\nname = \"solo\"\nversion = \"1.0.0\"\n\n" +
			"[tool.uv.workspace]\nmembers = [\"packages/*\"]\n",
		"packages/a/pyproject.toml": "[project]\nname = \"a\"\nversion = \"1.0.0\"\n",
	})
	if got := ids(t, pyproject.Packages(), root); !same(got, []string{".", "packages/a"}) {
		t.Fatalf("Units = %v", got)
	}
}

// TestAVirtualEnvironmentIsNotSearched. A .venv holds a pyproject.toml per
// installed dependency, and a graph that walked into one would turn a
// four-package monorepo into a plan of hundreds.
func TestAVirtualEnvironmentIsNotSearched(t *testing.T) {
	root := write(t, map[string]string{
		"packages/a/pyproject.toml":                                  "[project]\nname = \"a\"\nversion = \"1.0.0\"\n",
		".venv/lib/python3.12/site-packages/requests/pyproject.toml": "[project]\nname = \"requests\"\nversion = \"2.0\"\n",
		"venv/lib/python3.12/site-packages/click/pyproject.toml":     "[project]\nname = \"click\"\nversion = \"8.0\"\n",
	})
	if got := ids(t, pyproject.Packages(), root); !same(got, []string{"packages/a"}) {
		t.Fatalf("Units = %v, want only the real distribution", got)
	}
}

func TestNoDistributionAnywhereIsAnError(t *testing.T) {
	root := write(t, map[string]string{"README.md": "hi\n"})
	_, err := pyproject.Packages().Units(context.Background(), root)
	if err == nil {
		t.Fatal("Units of a tree with no distribution returned no error")
	}
	if !strings.Contains(err.Error(), "pyproject.toml") {
		t.Errorf("error %q does not say what it looked for", err)
	}
}

// TestAMalformedManifestIsAnError, not a smaller graph.
func TestAMalformedManifestIsAnError(t *testing.T) {
	root := write(t, map[string]string{
		"a/pyproject.toml": "[project]\nname = \"a\"\nversion = \"1.0.0\"\n",
		"b/pyproject.toml": "[project\nname = \"b\"\n",
	})
	_, err := pyproject.Packages().Units(context.Background(), root)
	if err == nil {
		t.Fatal("Units over a malformed manifest returned no error")
	}
	if !strings.Contains(err.Error(), "b/pyproject.toml") {
		t.Errorf("error %q does not name the file", err)
	}
}

func TestRootThatIsNotADirectoryIsAnError(t *testing.T) {
	root := write(t, map[string]string{"f.txt": "x"})
	if _, err := pyproject.Packages().Units(context.Background(), filepath.Join(root, "f.txt")); err == nil {
		t.Fatal("Units of a file returned no error")
	}
}

func TestUnitsIsCancellable(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := pyproject.Packages().Units(ctx, workspace); !errors.Is(err, context.Canceled) {
		t.Fatalf("Units on a cancelled context = %v, want context.Canceled", err)
	}
}

func TestDescribe(t *testing.T) {
	if got := pyproject.Packages().Describe(); got != "pyproject packages" {
		t.Errorf("Describe = %q", got)
	}
}

func TestUnitIDsAreSafeForAStepID(t *testing.T) {
	for _, id := range ids(t, pyproject.Packages(), workspace) {
		if strings.ContainsAny(id, "[]=,@") {
			t.Errorf("unit id %q would corrupt an expanded step id", id)
		}
	}
}
