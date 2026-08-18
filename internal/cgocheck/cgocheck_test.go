package cgocheck_test

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/xavidop/senro/internal/cgocheck"
)

// TestCheckFindsACgoTaintedPackageAndTheChainThatPulledItIn pins the
// requirement in full: walk go list -deps -json for packages with non-empty
// CgoFiles, and fail with the offending import path AND THE CHAIN THAT
// PULLED IT IN. The chain is the part that makes the report actionable:
// "net is cgo-tainted" is not something a person can fix, and "yours ->
// internal/api -> net" is.
func TestCheckFindsACgoTaintedPackageAndTheChainThatPulledItIn(t *testing.T) {
	dir := writeModule(t, map[string]string{
		"go.mod":  "module example.com/probe\n\ngo 1.26\n",
		"main.go": "package main\n\nimport _ \"example.com/probe/inner\"\n\nfunc main() {}\n",
		"inner/inner.go": "package inner\n\n" +
			"// #include <stdlib.h>\nimport \"C\"\n\n" +
			"func Free() { C.free(nil) }\n",
	})
	// "." rather than "./...": go list marks every package a pattern
	// matches directly as NOT DepOnly. "./..." would match "inner" directly
	// too (it walks every package under the module, regardless of who
	// imports it), which would make inner its own root and collapse the
	// chain to length one before the BFS this test exists to check ever
	// runs an edge. "." matches only the module's entry package, so inner
	// is reached purely as a dependency - the same shape a real offender
	// (os/user, net, a C library wrapper) actually has: never a package
	// the caller's own pattern names directly.
	got, err := cgocheck.Check(context.Background(), dir, ".")
	if err != nil {
		t.Fatalf("Check: %v", err)
	}
	var found *cgocheck.Offender
	for i := range got {
		if got[i].ImportPath == "example.com/probe/inner" {
			found = &got[i]
		}
	}
	if found == nil {
		t.Fatalf("the cgo package was not reported; got %v", got)
	}
	if len(found.CgoFiles) == 0 {
		t.Error("the offender names no cgo file")
	}
	if len(found.Chain) < 2 || found.Chain[len(found.Chain)-1] != "example.com/probe/inner" {
		t.Errorf("chain = %v, want it to end at the offender", found.Chain)
	}
	if found.Chain[0] != "example.com/probe" {
		t.Errorf("chain = %v, want it to start at the module's own root package", found.Chain)
	}
}

// TestCheckReportsNothingForAPureModule is the sibling absence case. On its
// own, "len(got) != 0" can pass for the wrong reason: a Check that
// short-circuited to (nil, nil) without ever invoking go list would satisfy
// it too. Guard against that the way this project's secret canaries prove
// they searched real data (internal/secrets/reveal_test.go): before
// trusting the empty result, independently confirm - with a bare go list
// this test does not route through cgocheck's own code at all - that the
// fixture's dependency graph is not trivial.
func TestCheckReportsNothingForAPureModule(t *testing.T) {
	dir := writeModule(t, map[string]string{
		"go.mod":  "module example.com/pure\n\ngo 1.26\n",
		"main.go": "package main\n\nimport \"fmt\"\n\nfunc main() { fmt.Println(1) }\n",
	})

	canary := exec.CommandContext(context.Background(), "go", "list", "-deps", "./...")
	canary.Dir = dir
	out, err := canary.Output()
	if err != nil {
		t.Fatalf("canary go list -deps ./...: %v", err)
	}
	if n := len(strings.Fields(string(out))); n < 5 {
		t.Fatalf("fixture module's real dependency graph has only %d packages; too trivial for an empty Check result to prove anything", n)
	}

	got, err := cgocheck.Check(context.Background(), dir, "./...")
	if err != nil {
		t.Fatalf("Check: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("a module with no cgo reported %v", got)
	}
}

func TestCheckReportsAnErrorForADirectoryThatIsNotAModule(t *testing.T) {
	if _, err := cgocheck.Check(context.Background(), t.TempDir(), "./..."); err == nil {
		t.Fatal("Check accepted a directory with no go.mod")
	}
}

// TestCheckFindsCgoInTheRootPackageItself is the "cgo directly" case: the
// package the pattern names is itself the offender, not something it pulled
// in. Nothing pulled it in, so the correct chain is the package alone -
// this pins that a direct offender does not get a synthetic multi-hop chain
// grafted onto it.
func TestCheckFindsCgoInTheRootPackageItself(t *testing.T) {
	dir := writeModule(t, map[string]string{
		"go.mod": "module example.com/direct\n\ngo 1.26\n",
		"main.go": "package main\n\n" +
			"// #include <stdlib.h>\nimport \"C\"\n\n" +
			"func main() { C.free(nil) }\n",
	})
	got, err := cgocheck.Check(context.Background(), dir, "./...")
	if err != nil {
		t.Fatalf("Check: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("got %d offenders, want exactly 1: %v", len(got), got)
	}
	o := got[0]
	if o.ImportPath != "example.com/direct" {
		t.Errorf("ImportPath = %q, want the root package itself", o.ImportPath)
	}
	if len(o.CgoFiles) == 0 {
		t.Error("the offender names no cgo file")
	}
	if !slices.Equal(o.Chain, []string{"example.com/direct"}) {
		t.Errorf("Chain = %v, want [%q]: the offender IS the root, nothing pulled it in", o.Chain, "example.com/direct")
	}
}

// TestCheckExcludesRuntimeCgoAsARedundantSecondOffender pins a toolchain
// quirk not documented anywhere in cmd/go: the toolchain always adds
// runtime/cgo as a dependency of any package using `import "C"`, and
// runtime/cgo always compiles its own cgo file in turn. Left unfiltered,
// Check reports it as a second "offender" alongside the actual cause in
// every single case that has cgo at all, which is not a mistake anyone can
// fix and defeats the report's one-thing-per-cause goal. This reuses the
// same "cgo directly" fixture as the test above because that is the fixture
// that first surfaced the extra entry.
func TestCheckExcludesRuntimeCgoAsARedundantSecondOffender(t *testing.T) {
	dir := writeModule(t, map[string]string{
		"go.mod": "module example.com/direct2\n\ngo 1.26\n",
		"main.go": "package main\n\n" +
			"// #include <stdlib.h>\nimport \"C\"\n\n" +
			"func main() { C.free(nil) }\n",
	})
	got, err := cgocheck.Check(context.Background(), dir, "./...")
	if err != nil {
		t.Fatalf("Check: %v", err)
	}
	for _, o := range got {
		if o.ImportPath == "runtime/cgo" {
			t.Fatalf("runtime/cgo reported as its own offender: %v", got)
		}
	}
}

// TestCheckFindsCgoSeveralLevelsDownAndReportsTheFullChain is the
// "transitively several levels down" negative case: three hops (a -> b ->
// c) between the module's root package and the cgo file, so a
// chain-builder that only records the LAST edge
// (e.g. one that seeds every locally-reachable package as its own root
// instead of doing a real BFS from a single entry point) cannot pass this
// by accident the way it could against a two-package fixture.
func TestCheckFindsCgoSeveralLevelsDownAndReportsTheFullChain(t *testing.T) {
	dir := writeModule(t, map[string]string{
		"go.mod":  "module example.com/deep\n\ngo 1.26\n",
		"main.go": "package main\n\nimport _ \"example.com/deep/a\"\n\nfunc main() {}\n",
		"a/a.go":  "package a\n\nimport _ \"example.com/deep/b\"\n",
		"b/b.go":  "package b\n\nimport _ \"example.com/deep/c\"\n",
		"c/c.go": "package c\n\n" +
			"// #include <stdlib.h>\nimport \"C\"\n\n" +
			"func Free() { C.free(nil) }\n",
	})
	// "." for the same DepOnly reason as the first test above: "./..."
	// would match a, b and c directly too (they are all local packages
	// under this module), turning each hop into its own root and
	// collapsing the reported chain to the single edge nearest the
	// offender instead of the full path from the module's entry point.
	got, err := cgocheck.Check(context.Background(), dir, ".")
	if err != nil {
		t.Fatalf("Check: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("got %d offenders, want exactly 1: %v", len(got), got)
	}
	want := []string{
		"example.com/deep", "example.com/deep/a", "example.com/deep/b", "example.com/deep/c",
	}
	if !slices.Equal(got[0].Chain, want) {
		t.Errorf("Chain = %v, want %v", got[0].Chain, want)
	}
}

// TestCheckReportsAnErrorForAPackageThatDoesNotBuild is the "package that
// does not build" negative case. A go file with no package clause at all
// fails even go list's own lightweight parse. A deeper syntax error inside
// a function body does NOT fail go list, since it does not parse function
// bodies to list a package; only a source file go list cannot parse at all
// does. This is the reliable way to force that path rather than the one
// that happens to look most broken to a human reader.
func TestCheckReportsAnErrorForAPackageThatDoesNotBuild(t *testing.T) {
	dir := writeModule(t, map[string]string{
		"go.mod":  "module example.com/broken\n\ngo 1.26\n",
		"main.go": "!!! not even go source, no package clause at all\n",
	})
	if _, err := cgocheck.Check(context.Background(), dir, "./..."); err == nil {
		t.Fatal("Check accepted a package that does not build")
	}
}

// TestCheckReportsAnErrorWhenGoListItselfFails is a negative case distinct
// from the one above: here the module and its packages are fine, but the
// go toolchain itself cannot be invoked at all (no "go" on PATH), which is
// a different failure than "go list ran and reported a problem with the
// target."
func TestCheckReportsAnErrorWhenGoListItselfFails(t *testing.T) {
	dir := writeModule(t, map[string]string{
		"go.mod":  "module example.com/nogotool\n\ngo 1.26\n",
		"main.go": "package main\n\nfunc main() {}\n",
	})
	t.Setenv("PATH", t.TempDir()) // guaranteed to hold no "go" binary
	if _, err := cgocheck.Check(context.Background(), dir, "./..."); err == nil {
		t.Fatal("Check succeeded with no go toolchain reachable on PATH")
	}
}

// writeModule writes files (path relative to the module root -> content)
// under a fresh temp directory and returns that directory, so a test fixture
// reads as the module layout it represents rather than as filesystem
// plumbing.
func writeModule(t *testing.T, files map[string]string) string {
	t.Helper()
	dir := t.TempDir()
	for rel, content := range files {
		full := filepath.Join(dir, rel)
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatalf("MkdirAll %s: %v", filepath.Dir(full), err)
		}
		if err := os.WriteFile(full, []byte(content), 0o644); err != nil {
			t.Fatalf("WriteFile %s: %v", full, err)
		}
	}
	return dir
}
