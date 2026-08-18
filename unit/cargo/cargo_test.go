package cargo_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/xavidop/senro/internal/unit"
	"github.com/xavidop/senro/unit/cargo"
)

// workspace is the checked-in fixture: a real Cargo workspace, five crates
// and a chain three deep.
//
//	core  <-  store  <-  cli          (ordinary dependencies)
//	testutil  <-  cli                 (a DEV dependency, nothing else)
//	platform  <-  cli                 (a TARGET-specific dependency)
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

func TestCratesDiscoversEveryCrate(t *testing.T) {
	got := ids(t, cargo.Crates(), workspace)
	want := []string{"apps/cli", "crates/core", "crates/platform", "crates/store", "crates/testutil"}
	if !same(got, want) {
		t.Fatalf("Units = %v, want %v", got, want)
	}
}

// TestAnExcludedCrateIsNotAUnit: [workspace] exclude is the one explicit
// statement a manifest makes that a directory is not part of the build, and
// crates/scratch is in it.
func TestAnExcludedCrateIsNotAUnit(t *testing.T) {
	for _, id := range ids(t, cargo.Crates(), workspace) {
		if id == "crates/scratch" {
			t.Fatal("crates/scratch is a unit; the workspace excludes it")
		}
	}
}

func TestAUnitIsNamedByItsCrateName(t *testing.T) {
	us, err := cargo.Crates().Units(context.Background(), workspace)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{
		"apps/cli":        "cli",
		"crates/core":     "core",
		"crates/store":    "store",
		"crates/testutil": "testutil",
		"crates/platform": "platform",
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

// TestAffectedIsTransitiveOverThreeUnits is the case the whole feature turns
// on: cli has never heard of core, but it depends on store and store depends
// on core.
func TestAffectedIsTransitiveOverThreeUnits(t *testing.T) {
	got := affected(t, cargo.Crates(), workspace, "crates/core/src/lib.rs")
	want := []string{"apps/cli", "crates/core", "crates/store"}
	if !same(got, want) {
		t.Fatalf("Affected(core) = %v, want %v", got, want)
	}
}

// TestAffectedFollowsAWorkspaceInheritedDependency: store declares
// `core.workspace = true`, with the path over in the root's
// [workspace.dependencies], so a graph reading only a member's own `path =`
// entries would see no edge and skip store and cli on a change to core.
//
// The mutation that reddens this drops BOTH halves: the inherited-path
// lookup AND the name-match rule. Either alone gets a valid manifest right,
// because Cargo requires an inherited dependency's key to be the crate's
// own name unless it carries a `package` rename, which this graph reads too.
func TestAffectedFollowsAWorkspaceInheritedDependency(t *testing.T) {
	g := cargo.Crates()
	rd, err := g.ReverseDeps(context.Background(), workspace)
	if err != nil {
		t.Fatal(err)
	}
	if !same(rd["crates/core"], []string{"crates/store"}) {
		t.Fatalf("ReverseDeps[crates/core] = %v, want crates/store", rd["crates/core"])
	}
}

// TestAffectedSeesADevDependency: nothing but cli's [dev-dependencies]
// mentions testutil, and a test-only edge is still an edge. This is the same
// shape of bug gowork was bitten by with a test-only Go import.
func TestAffectedSeesADevDependency(t *testing.T) {
	got := affected(t, cargo.Crates(), workspace, "crates/testutil/src/lib.rs")
	want := []string{"apps/cli", "crates/testutil"}
	if !same(got, want) {
		t.Fatalf("Affected(testutil) = %v, want %v", got, want)
	}
}

// TestAffectedSeesATargetSpecificDependency: cli depends on platform only
// under [target.'cfg(unix)'.dependencies]. The graph cannot know which target
// a step will build for, so a target-specific dependency is an edge like any
// other.
func TestAffectedSeesATargetSpecificDependency(t *testing.T) {
	got := affected(t, cargo.Crates(), workspace, "crates/platform/src/lib.rs")
	want := []string{"apps/cli", "crates/platform"}
	if !same(got, want) {
		t.Fatalf("Affected(platform) = %v, want %v", got, want)
	}
}

// TestAffectedDoesNotRunUnitsDownstreamOfNothing stops the
// over-approximation from swallowing the feature.
func TestAffectedDoesNotRunUnitsDownstreamOfNothing(t *testing.T) {
	got := affected(t, cargo.Crates(), workspace, "apps/cli/src/main.rs")
	if want := []string{"apps/cli"}; !same(got, want) {
		t.Fatalf("Affected(cli) = %v, want %v", got, want)
	}
}

// TestTheWorkspaceManifestAndLockfileAffectEverything. Both decide what every
// crate compiles against.
func TestTheWorkspaceManifestAndLockfileAffectEverything(t *testing.T) {
	for _, f := range []string{"Cargo.toml", "Cargo.lock", "README.md"} {
		res, err := unit.Affected(context.Background(), cargo.Crates(), workspace, []string{f})
		if err != nil {
			t.Fatal(err)
		}
		if !res.All || res.Total != 5 {
			t.Errorf("Affected(%q) = %d units (all=%v), want every one of 5", f, len(res.Units), res.All)
		}
	}
}

// TestAFileUnderNoCrateAffectsEverything: docs/ sits beside the crates and
// belongs to none of them, so nothing can be concluded about it.
func TestAFileUnderNoCrateAffectsEverything(t *testing.T) {
	res, err := unit.Affected(context.Background(), cargo.Crates(), workspace,
		[]string{"docs/design.md"})
	if err != nil {
		t.Fatal(err)
	}
	if !res.All {
		t.Fatalf("Affected(docs/design.md) = %v, want everything", res.Units)
	}
}

// TestAMemberManifestAffectsItsOwnDependents, and only those: a crate's own
// Cargo.toml is a file in that crate's directory like any other.
func TestAMemberManifestAffectsItsOwnDependents(t *testing.T) {
	got := affected(t, cargo.Crates(), workspace, "crates/store/Cargo.toml")
	want := []string{"apps/cli", "crates/store"}
	if !same(got, want) {
		t.Fatalf("Affected(store/Cargo.toml) = %v, want %v", got, want)
	}
}

// TestOwnsAnswersForADeletedFile: Owns is path arithmetic and never stats.
func TestOwnsAnswersForADeletedFile(t *testing.T) {
	g := cargo.Crates()
	got, err := g.Owns(context.Background(), workspace, []string{"crates/store/src/gone.rs"})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || !same(got[0], []string{"crates/store"}) {
		t.Fatalf("Owns = %v", got)
	}
}

// TestReverseDepsAreDirectAndDeterministic pins the contract Affected relies
// on: direct edges only, sorted, no self-edge, and no pre-flattened closure
// that would hide a bug in Affected's own.
func TestReverseDepsAreDirectAndDeterministic(t *testing.T) {
	rd, err := cargo.Crates().ReverseDeps(context.Background(), workspace)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string][]string{
		"crates/core":     {"crates/store"},
		"crates/store":    {"apps/cli"},
		"crates/testutil": {"apps/cli"},
		"crates/platform": {"apps/cli"},
	}
	for k, v := range want {
		if !same(rd[k], v) {
			t.Errorf("ReverseDeps[%q] = %v, want %v", k, rd[k], v)
		}
	}
	if len(rd["apps/cli"]) != 0 {
		t.Errorf("ReverseDeps[apps/cli] = %v, want nothing", rd["apps/cli"])
	}
	if same(rd["crates/core"], []string{"apps/cli", "crates/store"}) {
		t.Error("ReverseDeps[crates/core] includes cli; the edges must be direct only")
	}
}

// TestAStandaloneCrateWithNoWorkspace: plenty of repositories hold a couple
// of unrelated crates and no [workspace] table anywhere.
func TestAStandaloneCrateWithNoWorkspace(t *testing.T) {
	root := write(t, map[string]string{
		"services/api/Cargo.toml":  "[package]\nname = \"api\"\nversion = \"0.1.0\"\nedition = \"2021\"\n",
		"services/api/src/main.rs": "fn main() {}\n",
		"tools/lint/Cargo.toml": "[package]\nname = \"lint\"\nversion = \"0.1.0\"\nedition = \"2021\"\n\n" +
			"[dependencies]\napi = { path = \"../../services/api\" }\n",
		"tools/lint/src/main.rs": "fn main() {}\n",
	})
	g := cargo.Crates()
	if got := ids(t, g, root); !same(got, []string{"services/api", "tools/lint"}) {
		t.Fatalf("Units = %v", got)
	}
	if got := affected(t, g, root, "services/api/src/main.rs"); !same(got,
		[]string{"services/api", "tools/lint"}) {
		t.Fatalf("Affected = %v", got)
	}
}

// TestARenamedDependencyIsStillAnEdge. `internal = { package = "core", path
// = ... }` gives the dependency a local name that matches no crate, and only
// the path resolves it.
func TestARenamedDependencyIsStillAnEdge(t *testing.T) {
	root := write(t, map[string]string{
		"Cargo.toml":   "[workspace]\nmembers = [\"a\", \"b\"]\n",
		"a/Cargo.toml": "[package]\nname = \"core\"\nversion = \"0.1.0\"\n",
		"a/src/lib.rs": "pub fn a() {}\n",
		"b/Cargo.toml": "[package]\nname = \"b\"\nversion = \"0.1.0\"\n\n[dependencies]\ninternal = { package = \"core\", path = \"../a\" }\n",
		"b/src/lib.rs": "pub fn b() {}\n",
	})
	if got := affected(t, cargo.Crates(), root, "a/src/lib.rs"); !same(got, []string{"a", "b"}) {
		t.Fatalf("Affected = %v, want both", got)
	}
}

// TestADependencyNamedAfterAWorkspaceCrateIsAnEdge is the deliberate
// over-approximation: a bare `version = ` dependency whose name matches a
// crate in this tree gets an edge, because [patch] and [replace] can point it
// at that crate and this graph does not read either table.
func TestADependencyNamedAfterAWorkspaceCrateIsAnEdge(t *testing.T) {
	root := write(t, map[string]string{
		"Cargo.toml":   "[workspace]\nmembers = [\"a\", \"b\"]\n\n[patch.crates-io]\ncore = { path = \"a\" }\n",
		"a/Cargo.toml": "[package]\nname = \"core\"\nversion = \"0.1.0\"\n",
		"a/src/lib.rs": "pub fn a() {}\n",
		"b/Cargo.toml": "[package]\nname = \"b\"\nversion = \"0.1.0\"\n\n[dependencies]\ncore = \"0.1\"\n",
		"b/src/lib.rs": "pub fn b() {}\n",
	})
	if got := affected(t, cargo.Crates(), root, "a/src/lib.rs"); !same(got, []string{"a", "b"}) {
		t.Fatalf("Affected = %v, want both: a patched dependency is still a dependency", got)
	}
}

func TestNoCrateAnywhereIsAnError(t *testing.T) {
	root := write(t, map[string]string{"README.md": "hi\n"})
	_, err := cargo.Crates().Units(context.Background(), root)
	if err == nil {
		t.Fatal("Units of a tree with no Cargo.toml returned no error")
	}
	if !strings.Contains(err.Error(), "Cargo.toml") {
		t.Errorf("error %q does not say what it looked for", err)
	}
}

// TestAMalformedManifestIsAnError, not a smaller graph. A manifest half-read
// is an edge missing, and an edge missing is a skipped build.
func TestAMalformedManifestIsAnError(t *testing.T) {
	root := write(t, map[string]string{
		"a/Cargo.toml": "[package]\nname = \"a\"\n",
		"b/Cargo.toml": "[package\nname = \"b\"\n",
	})
	_, err := cargo.Crates().Units(context.Background(), root)
	if err == nil {
		t.Fatal("Units over a malformed manifest returned no error")
	}
	if !strings.Contains(err.Error(), "b/Cargo.toml") {
		t.Errorf("error %q does not name the file", err)
	}
}

func TestRootThatIsNotADirectoryIsAnError(t *testing.T) {
	root := write(t, map[string]string{"f.txt": "x"})
	if _, err := cargo.Crates().Units(context.Background(), filepath.Join(root, "f.txt")); err == nil {
		t.Fatal("Units of a file returned no error")
	}
}

func TestUnitsIsCancellable(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := cargo.Crates().Units(ctx, workspace); !errors.Is(err, context.Canceled) {
		t.Fatalf("Units on a cancelled context = %v, want context.Canceled", err)
	}
}

func TestDescribe(t *testing.T) {
	if got := cargo.Crates().Describe(); got != "cargo crates" {
		t.Errorf("Describe = %q", got)
	}
}

// TestUnitIDsAreSafeForAStepID: an expansion builds "test[unit=<id>]" out of
// these and the grammar has no escape for "[]=,@".
func TestUnitIDsAreSafeForAStepID(t *testing.T) {
	for _, id := range ids(t, cargo.Crates(), workspace) {
		if strings.ContainsAny(id, "[]=,@") {
			t.Errorf("unit id %q would corrupt an expanded step id", id)
		}
	}
}
