package gowork_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/xavidop/senro/internal/unit"
	"github.com/xavidop/senro/unit/gowork"
)

// write lays out a tree from a map of slash-separated path to content, for a
// test that needs one of its own.
func write(t *testing.T, files map[string]string) string {
	t.Helper()
	root := t.TempDir()
	if err := writeTree(root, files); err != nil {
		t.Fatal(err)
	}
	return root
}

func writeTree(root string, files map[string]string) error {
	for p, body := range files {
		full := filepath.Join(root, filepath.FromSlash(p))
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			return err
		}
		if err := os.WriteFile(full, []byte(body), 0o644); err != nil {
			return err
		}
	}
	return nil
}

// TestMain builds the shared fixture once, not per test: every question
// costs a `go list` per module, and a fixture per test meant forty
// toolchain subprocesses, slow and enough CPU pressure to destabilise
// timing-sensitive tests elsewhere. Nothing here mutates the tree, and the
// two shared Graphs memoize one listing each. A test that needs its own
// tree still builds one with write.
func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "senro-gowork-")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := writeTree(dir, workspaceFiles); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	sharedRoot = dir
	code := m.Run()
	_ = os.RemoveAll(dir)
	os.Exit(code)
}

var (
	sharedRoot string
	// One Graph of each granularity, shared. A Graph is safe for concurrent
	// use and memoizes per root, which is exactly what makes sharing one
	// cheap; a test that needs a COLD graph builds its own.
	sharedPkgs = gowork.Packages()
	sharedMods = gowork.Modules()
)

// workspaceFiles is the fixture nearly every test here uses: three modules
// stitched together by a go.work, an import chain three deep, and a
// TEST-ONLY import from the top of the chain to a package nothing else
// mentions.
//
//	liba  <-  libb  <-  appc          (ordinary imports)
//	libb/sub  <-  appc                (appc's TEST imports it, nothing else)
//
// `go list -deps` without -test does not report libb/sub at all, so a graph
// built without the test edge would skip the build that catches the break.
var workspaceFiles = func() map[string]string {
	return map[string]string{
		"go.work": "go 1.24\n\nuse (\n\t./liba\n\t./libb\n\t./appc\n)\n",
		// Owned by no module at all: the case that has to over-approximate.
		"Makefile": "all:\n\techo hi\n",

		"liba/go.mod":          "module example.com/liba\n\ngo 1.24\n",
		"liba/a.go":            "package liba\n\nfunc A() string { return \"a\" }\n",
		"liba/testdata/in.txt": "fixture\n",

		"libb/go.mod":    "module example.com/libb\n\ngo 1.24\n\nrequire example.com/liba v0.0.0\n",
		"libb/b.go":      "package libb\n\nimport \"example.com/liba\"\n\nfunc B() string { return liba.A() }\n",
		"libb/sub/s.go":  "package sub\n\nfunc S() string { return \"s\" }\n",
		"libb/docs/x.md": "notes\n",

		"appc/go.mod": "module example.com/appc\n\ngo 1.24\n\nrequire example.com/libb v0.0.0\n",
		"appc/main.go": "package main\n\nimport \"example.com/libb\"\n\n" +
			"func main() { println(libb.B()) }\n",
		"appc/main_test.go": "package main\n\nimport (\n\t\"testing\"\n\n\t\"example.com/libb/sub\"\n)\n\n" +
			"func TestX(t *testing.T) { _ = sub.S() }\n",
	}
}()

// workspaceTree is the shared fixture's root.
func workspaceTree(t *testing.T) string {
	t.Helper()
	return sharedRoot
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

func TestPackagesDiscoversEveryPackageInEveryModule(t *testing.T) {
	root := workspaceTree(t)
	got := ids(t, sharedPkgs, root)
	want := []string{"appc", "liba", "libb", "libb/sub"}
	if !same(got, want) {
		t.Fatalf("Units = %v, want %v", got, want)
	}
}

func TestModulesDiscoversOneUnitPerGoMod(t *testing.T) {
	root := workspaceTree(t)
	g := gowork.Modules()
	got := ids(t, g, root)
	want := []string{"appc", "liba", "libb"}
	if !same(got, want) {
		t.Fatalf("Units = %v, want %v", got, want)
	}
	us, err := g.Units(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	if us[1].Name != "example.com/liba" {
		t.Errorf("Name = %q, want the module path", us[1].Name)
	}
	if us[1].Dir != "liba" {
		t.Errorf("Dir = %q, want liba", us[1].Dir)
	}
}

func TestPackagesNamesAUnitByItsImportPath(t *testing.T) {
	root := workspaceTree(t)
	us, err := gowork.Packages().Units(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	for _, u := range us {
		if u.ID == "libb/sub" {
			if u.Name != "example.com/libb/sub" {
				t.Fatalf("Name = %q, want the import path", u.Name)
			}
			return
		}
	}
	t.Fatal("no libb/sub unit")
}

// TestAffectedIsTransitiveOverThreeUnits is the case the whole feature turns
// on. A change to liba, which appc does not import and has never heard of,
// still has to run appc: appc imports libb and libb imports liba.
func TestAffectedIsTransitiveOverThreeUnits(t *testing.T) {
	root := workspaceTree(t)
	got := affected(t, sharedPkgs, root, "liba/a.go")
	want := []string{"appc", "liba", "libb"}
	if !same(got, want) {
		t.Fatalf("Affected(liba/a.go) = %v, want %v", got, want)
	}
}

// TestAffectedIsTransitiveOverModulesToo proves the collapse to module
// granularity keeps the chain rather than flattening it to direct requires.
func TestAffectedIsTransitiveOverModulesToo(t *testing.T) {
	root := workspaceTree(t)
	got := affected(t, sharedMods, root, "liba/a.go")
	want := []string{"appc", "liba", "libb"}
	if !same(got, want) {
		t.Fatalf("Affected(liba/a.go) = %v, want %v", got, want)
	}
}

// TestAffectedSeesATestOnlyImport: nothing but appc's _test.go mentions
// libb/sub, so a graph reading only ordinary Imports would report appc
// unaffected and let a break through. The mutation that reddens this drops
// BOTH sources of a test edge (the -test flag and TestImports/XTestImports);
// removing one alone leaves it green, which is the point of having both.
func TestAffectedSeesATestOnlyImport(t *testing.T) {
	root := workspaceTree(t)
	got := affected(t, sharedPkgs, root, "libb/sub/s.go")
	want := []string{"appc", "libb/sub"}
	if !same(got, want) {
		t.Fatalf("Affected(libb/sub/s.go) = %v, want %v; a test-only import is still an import", got, want)
	}
}

// TestAffectedDoesNotRunUnitsDownstreamOfNothing is the check that stops the
// over-approximation from swallowing the feature: appc is at the top of the
// chain, and nothing imports it.
func TestAffectedDoesNotRunUnitsDownstreamOfNothing(t *testing.T) {
	root := workspaceTree(t)
	got := affected(t, sharedPkgs, root, "appc/main.go")
	if want := []string{"appc"}; !same(got, want) {
		t.Fatalf("Affected(appc/main.go) = %v, want %v", got, want)
	}
}

func TestOwnsAttributesAFileToItsPackage(t *testing.T) {
	root := workspaceTree(t)
	got := owns(t, sharedPkgs, root, "libb/b.go", "libb/sub/s.go", "liba/testdata/in.txt")
	want := [][]string{{"libb"}, {"libb/sub"}, {"liba"}}
	if !sameOwners(got, want) {
		t.Fatalf("Owns = %v, want %v", got, want)
	}
}

// TestOwnsAttributesAFileInNoPackageToItsWholeModule: libb/docs is not a Go
// package, and its nearest package ancestor inside the module is libb.
func TestOwnsAttributesAFileInNoPackageToItsWholeModule(t *testing.T) {
	root := workspaceTree(t)
	got := owns(t, sharedPkgs, root, "libb/docs/x.md")
	if want := [][]string{{"libb"}}; !sameOwners(got, want) {
		t.Fatalf("Owns = %v, want %v", got, want)
	}
}

// TestOwnsSaysNothingOwnsAFileAtTheWorkspaceRoot. A Makefile above every
// module changes what every unit builds and belongs to none of them, and the
// honest answer here is "no unit", which is what makes Affected run
// everything.
func TestOwnsSaysNothingOwnsAFileAtTheWorkspaceRoot(t *testing.T) {
	root := workspaceTree(t)
	got := owns(t, sharedPkgs, root, "Makefile")
	if len(got) != 1 || len(got[0]) != 0 {
		t.Fatalf("Owns(Makefile) = %v, want no owner", got)
	}
	res, err := unit.Affected(context.Background(), sharedPkgs, root, []string{"Makefile"})
	if err != nil {
		t.Fatal(err)
	}
	if !res.All || len(res.Units) != 4 {
		t.Fatalf("Affected(Makefile) = %v (all=%v), want every unit", res.Units, res.All)
	}
}

// TestGoModAffectsEveryPackageInItsModule: a dependency bump changes what
// every package of the module compiles against, so attributing go.mod to the
// module's root package alone would skip its siblings.
func TestGoModAffectsEveryPackageInItsModule(t *testing.T) {
	root := workspaceTree(t)
	got := owns(t, sharedPkgs, root, "libb/go.mod")
	if want := [][]string{{"libb", "libb/sub"}}; !sameOwners(got, want) {
		t.Fatalf("Owns(libb/go.mod) = %v, want every package of libb", got)
	}
	// And through the dependents, appc.
	if a := affected(t, sharedPkgs, root, "libb/go.mod"); !same(a, []string{"appc", "libb", "libb/sub"}) {
		t.Fatalf("Affected(libb/go.mod) = %v", a)
	}
	if a := affected(t, sharedPkgs, root, "libb/go.sum"); !same(a, []string{"appc", "libb", "libb/sub"}) {
		t.Fatalf("Affected(libb/go.sum) = %v, want the same as go.mod", a)
	}
}

// TestAModuleRootConfigFileAffectsTheWholeModule is the rule that stops the
// quiet under-approximation: a linter config or a Makefile at a module's root
// changes what every package in it builds, and attributing it to the module's
// root PACKAGE (which is what "nearest unit above" would do) would skip every
// sibling. A .go file at the same level is compiled into the root package and
// must NOT drag the siblings in, which is the other half of the same test.
func TestAModuleRootConfigFileAffectsTheWholeModule(t *testing.T) {
	root := workspaceTree(t)
	got := owns(t, sharedPkgs, root, "libb/.golangci.yml", "libb/Makefile", "libb/b.go")
	want := [][]string{{"libb", "libb/sub"}, {"libb", "libb/sub"}, {"libb"}}
	if !sameOwners(got, want) {
		t.Fatalf("Owns = %v, want %v", got, want)
	}
}

// TestGoWorkAffectsEverything: go.work decides which modules resolve to
// which directories, so a change to it can change what any unit compiles
// against.
func TestGoWorkAffectsEverything(t *testing.T) {
	root := workspaceTree(t)
	res, err := unit.Affected(context.Background(), sharedPkgs, root, []string{"go.work"})
	if err != nil {
		t.Fatal(err)
	}
	if !res.All || len(res.Units) != 4 {
		t.Fatalf("Affected(go.work) = %v (all=%v), want every unit", res.Units, res.All)
	}
}

// TestOwnsAnswersForADeletedFile. Owns must be pure path arithmetic: the file
// is gone from disk by the time a plan is built, and the change that deleted
// it is exactly the one whose dependents most need rebuilding.
func TestOwnsAnswersForADeletedFile(t *testing.T) {
	root := workspaceTree(t)
	got := owns(t, sharedPkgs, root, "libb/gone.go", "libb/sub/also_gone.go")
	if want := [][]string{{"libb"}, {"libb/sub"}}; !sameOwners(got, want) {
		t.Fatalf("Owns = %v, want %v", got, want)
	}
	if a := affected(t, sharedPkgs, root, "libb/gone.go"); !same(a, []string{"appc", "libb"}) {
		t.Fatalf("Affected(deleted libb/gone.go) = %v", a)
	}
}

// TestADeletedPackageAffectsEverything: when a change removes a whole
// package, nothing on disk owns its files any more, and the units that used
// to import it are no longer connected to it by any edge. Over-approximating
// to everything is the only answer that cannot skip the importer.
func TestADeletedPackageAffectsEverything(t *testing.T) {
	root := workspaceTree(t)
	res, err := unit.Affected(context.Background(), sharedPkgs, root,
		[]string{"libd/deleted.go"})
	if err != nil {
		t.Fatal(err)
	}
	if !res.All {
		t.Fatalf("Affected(a file in a package that no longer exists) = %v, want everything", res.Units)
	}
}

func TestSingleModuleAtTheRootIsAUnitCalledDot(t *testing.T) {
	root := write(t, map[string]string{
		"go.mod":         "module example.com/solo\n\ngo 1.24\n",
		"main.go":        "package main\n\nimport \"example.com/solo/lib\"\n\nfunc main() { println(lib.L()) }\n",
		"lib/lib.go":     "package lib\n\nfunc L() string { return \"l\" }\n",
		"README.md":      "hi\n",
		"lib/notes.txt":  "n\n",
		"vendorish/x.md": "x\n",
	})
	got := ids(t, sharedPkgs, root)
	if want := []string{".", "lib"}; !same(got, want) {
		t.Fatalf("Units = %v, want %v", got, want)
	}
	// A file at the root of a module that IS a package belongs to it, and a
	// change to lib runs the root package that imports it.
	if a := affected(t, sharedPkgs, root, "lib/lib.go"); !same(a, []string{".", "lib"}) {
		t.Fatalf("Affected(lib/lib.go) = %v", a)
	}
	if a := affected(t, sharedPkgs, root, "README.md"); !same(a, []string{".", "lib"}) {
		t.Fatalf("Affected(README.md) = %v, want everything: the root package owns the root directory", a)
	}
	if a := affected(t, sharedPkgs, root, "go.mod"); !same(a, []string{".", "lib"}) {
		t.Fatalf("Affected(go.mod) = %v, want every unit of the module", a)
	}
}

// TestModulesWithoutAGoWork proves module discovery is a walk for go.mod and
// not a parse of go.work: plenty of monorepos hold several independent
// modules with no workspace file at all.
func TestModulesWithoutAGoWork(t *testing.T) {
	root := write(t, map[string]string{
		"services/api/go.mod":     "module example.com/api\n\ngo 1.24\n",
		"services/api/api.go":     "package api\n\nfunc A() {}\n",
		"services/worker/go.mod":  "module example.com/worker\n\ngo 1.24\n",
		"services/worker/w.go":    "package worker\n\nfunc W() {}\n",
		"services/worker/i/i.go":  "package i\n\nfunc I() {}\n",
		"node_modules/junk/go.go": "package junk\n",
	})
	if got := ids(t, sharedMods, root); !same(got, []string{"services/api", "services/worker"}) {
		t.Fatalf("Units = %v", got)
	}
	if got := ids(t, sharedPkgs, root); !same(got,
		[]string{"services/api", "services/worker", "services/worker/i"}) {
		t.Fatalf("Units = %v", got)
	}
	// Two modules that do not import each other are independent.
	if a := affected(t, sharedMods, root, "services/api/api.go"); !same(a, []string{"services/api"}) {
		t.Fatalf("Affected = %v", a)
	}
}

// TestUnitsIsCancellable is what Graph.Units taking a context was reserved
// for: `go list` over a whole workspace is the call a person interrupts.
func TestUnitsIsCancellable(t *testing.T) {
	root := workspaceTree(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := gowork.Packages().Units(ctx, root); !errors.Is(err, context.Canceled) {
		t.Fatalf("Units on a cancelled context = %v, want context.Canceled", err)
	}
}

// TestABrokenPackageIsAnError, not a smaller graph. A listing that quietly
// dropped the package it could not resolve would drop its dependents with it.
func TestABrokenPackageIsAnError(t *testing.T) {
	root := write(t, map[string]string{
		"go.mod":  "module example.com/broken\n\ngo 1.24\n",
		"main.go": "package main\n\nimport \"example.com/nope/missing\"\n\nfunc main() { missing.X() }\n",
	})
	_, err := gowork.Packages().Units(context.Background(), root)
	if err == nil {
		t.Fatal("Units of a workspace with an unresolvable import returned no error")
	}
	if !strings.Contains(err.Error(), "gowork") {
		t.Errorf("error %q does not name the graph", err)
	}
}

func TestRootThatIsNotADirectoryIsAnError(t *testing.T) {
	root := write(t, map[string]string{"f.txt": "x"})
	if _, err := gowork.Packages().Units(context.Background(), filepath.Join(root, "f.txt")); err == nil {
		t.Fatal("Units of a file returned no error")
	}
}

func TestNoModulesAtAllIsAnError(t *testing.T) {
	root := write(t, map[string]string{"README.md": "hi\n"})
	_, err := gowork.Packages().Units(context.Background(), root)
	if err == nil {
		t.Fatal("Units of a tree with no go.mod returned no error")
	}
	if !strings.Contains(err.Error(), "go.mod") {
		t.Errorf("error %q does not say what it looked for", err)
	}
}

func TestDescribe(t *testing.T) {
	if got := gowork.Packages().Describe(); got != "gowork packages" {
		t.Errorf("Describe = %q", got)
	}
	if got := gowork.Modules().Describe(); got != "gowork modules" {
		t.Errorf("Describe = %q", got)
	}
}

// TestReverseDepsAreDirectAndDeterministic pins the contract Affected relies
// on: direct edges only, sorted, and no self-edge.
func TestReverseDepsAreDirectAndDeterministic(t *testing.T) {
	root := workspaceTree(t)
	g := gowork.Packages()
	rd, err := g.ReverseDeps(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string][]string{
		"liba":     {"libb"},
		"libb":     {"appc"},
		"libb/sub": {"appc"},
	}
	for k, v := range want {
		if !same(rd[k], v) {
			t.Errorf("ReverseDeps[%q] = %v, want %v", k, rd[k], v)
		}
	}
	if len(rd["appc"]) != 0 {
		t.Errorf("ReverseDeps[appc] = %v, want nothing", rd["appc"])
	}
	// liba is two hops from appc and must NOT be a direct edge: the closure
	// is Affected's job, and an implementation that pre-flattened here would
	// hide a bug in it.
	if same(rd["liba"], []string{"appc", "libb"}) {
		t.Error("ReverseDeps[liba] includes appc; the edges must be direct only")
	}
}

// TestUnitIDsAreSafeForAStepID: an expansion builds "test[unit=<id>]" out of
// these, and the grammar has no escape for "[]=,@".
func TestUnitIDsAreSafeForAStepID(t *testing.T) {
	root := workspaceTree(t)
	for _, id := range ids(t, sharedPkgs, root) {
		if strings.ContainsAny(id, "[]=,@") {
			t.Errorf("unit id %q would corrupt an expanded step id", id)
		}
	}
}

func owns(t *testing.T, g unit.Affector, root string, files ...string) [][]string {
	t.Helper()
	got, err := g.Owns(context.Background(), root, files)
	if err != nil {
		t.Fatalf("Owns: %v", err)
	}
	return got
}

func sameOwners(got, want [][]string) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if !same(got[i], want[i]) {
			return false
		}
	}
	return true
}
