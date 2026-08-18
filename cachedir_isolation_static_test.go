package senro_test

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// TestEveryTestPackageThatCanReachTheDefaultCacheRootHasIsolation is the
// static half of cachedir_isolation_test.go: a new package could call
// senro.Run with no WithCacheDir and silently reintroduce the leak TestMain
// prevents; this makes it fail loudly instead.
//
// It walks every _test.go file in this module (skipping api, a separate
// module that cannot import senro) for two shapes of risk: a senro.Run or
// senro.RunPlan call without WithCacheDir (both share the same fallback to
// storage.DefaultRoot()), and a direct storage.DefaultRoot() call outside
// the storage package (whose own tests call it on purpose and isolate with
// t.Setenv, the one deliberate exclusion). Every other package with a call
// site must declare a TestMain among its own _test.go files; in-package and
// external files both count, since they compile into one binary.
func TestEveryTestPackageThatCanReachTheDefaultCacheRootHasIsolation(t *testing.T) {
	repoRoot := moduleRoot(t)

	sites, testMainDirs, err := scanForCacheRootRisk(repoRoot)
	if err != nil {
		t.Fatalf("scanning %s for _test.go files: %v", repoRoot, err)
	}

	dirs := make([]string, 0, len(sites))
	for dir := range sites {
		dirs = append(dirs, dir)
	}
	sort.Strings(dirs)

	for _, dir := range dirs {
		if testMainDirs[dir] {
			continue
		}
		here := sites[dir]
		sort.Slice(here, func(i, j int) bool {
			if here[i].file != here[j].file {
				return here[i].file < here[j].file
			}
			return here[i].line < here[j].line
		})

		var b strings.Builder
		fmt.Fprintf(&b, "package %q can reach the developer's or CI's real cache directory but has no TestMain to isolate it:\n", dir)
		for _, s := range here {
			fmt.Fprintf(&b, "  %s:%d: %s\n", s.file, s.line, s.kind)
		}
		fmt.Fprintf(&b,
			"Add a TestMain to a _test.go file in %s that points SENRO_CACHE_DIR at a "+
				"throwaway directory for the life of the whole test binary, the same way "+
				"cachedir_isolation_test.go does it in this package, internal/source, and "+
				"cmd/senro:\n\n"+
				"    func TestMain(m *testing.M) {\n"+
				"        scratch, err := os.MkdirTemp(\"\", \"senro-test-cache\")\n"+
				"        if err != nil { os.Exit(1) }\n"+
				"        if err := os.Setenv(\"SENRO_CACHE_DIR\", scratch); err != nil { os.Exit(1) }\n"+
				"        code := m.Run()\n"+
				"        _ = os.RemoveAll(scratch)\n"+
				"        os.Exit(code)\n"+
				"    }\n",
			dir)
		t.Error(b.String())
	}
}

// callSite is one place a _test.go file reaches toward the real default
// cache root, either directly or through senro.Run's own fallback.
type callSite struct {
	file string // relative to the module root
	line int
	kind string // "senro.Run(...) without WithCacheDir", its RunPlan twin, or "storage.DefaultRoot()"
}

// runFuncNames are the senro package functions that fall back to
// storage.DefaultRoot() when WithCacheDir is not among their options.
var runFuncNames = map[string]bool{"Run": true, "RunPlan": true}

// senroImportPath and storageImportPath are the two import paths this scan
// cares about, matched by path rather than name so a local alias still
// resolves.
const (
	senroImportPath   = "github.com/xavidop/senro"
	storageImportPath = "github.com/xavidop/senro/internal/storage"
	storagePackageDir = "internal/storage"
)

// moduleRoot is this file's own directory (next to go.mod), resolved via
// runtime.Caller so a sibling test's t.Chdir cannot move it.
func moduleRoot(t *testing.T) string {
	t.Helper()
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller(0) failed to resolve this file's own path")
	}
	return filepath.Dir(thisFile)
}

// scanForCacheRootRisk walks root for every _test.go file outside a
// nested module, and returns every risky call site found, grouped by the
// directory (relative to root) it lives in, alongside the set of
// directories that declare their own TestMain.
func scanForCacheRootRisk(root string) (sites map[string][]callSite, testMainDirs map[string]bool, err error) {
	sites = map[string][]callSite{}
	testMainDirs = map[string]bool{}
	fset := token.NewFileSet()

	walkErr := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if path == root {
				return nil
			}
			name := d.Name()
			if strings.HasPrefix(name, ".") || name == "testdata" || name == "vendor" {
				return filepath.SkipDir
			}
			if _, statErr := os.Stat(filepath.Join(path, "go.mod")); statErr == nil {
				// A nested module, such as api: it cannot import senro or
				// internal/storage, so it is structurally exempt.
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, "_test.go") {
			return nil
		}

		rel, relErr := filepath.Rel(root, path)
		if relErr != nil {
			return relErr
		}
		dir := filepath.Dir(rel)

		src, readErr := os.ReadFile(path)
		if readErr != nil {
			return readErr
		}
		astFile, parseErr := parser.ParseFile(fset, path, src, 0)
		if parseErr != nil {
			return fmt.Errorf("parse %s: %w", rel, parseErr)
		}

		if declaresTestMain(astFile) {
			testMainDirs[dir] = true
		}

		senroAlias := importAlias(astFile, senroImportPath)
		storageAlias := importAlias(astFile, storageImportPath)

		ast.Inspect(astFile, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			recv, ok := sel.X.(*ast.Ident)
			if !ok {
				return true
			}

			switch {
			case senroAlias != "" && recv.Name == senroAlias && runFuncNames[sel.Sel.Name]:
				if !hasWithCacheDirArg(call.Args, senroAlias) {
					sites[dir] = append(sites[dir], callSite{
						file: rel,
						line: fset.Position(call.Pos()).Line,
						kind: fmt.Sprintf("senro.%s(...) without WithCacheDir", sel.Sel.Name),
					})
				}
			case storageAlias != "" && recv.Name == storageAlias && sel.Sel.Name == "DefaultRoot" && dir != storagePackageDir:
				sites[dir] = append(sites[dir], callSite{
					file: rel,
					line: fset.Position(call.Pos()).Line,
					kind: "storage.DefaultRoot()",
				})
			}
			return true
		})
		return nil
	})
	if walkErr != nil {
		return nil, nil, walkErr
	}
	return sites, testMainDirs, nil
}

// importAlias returns the local identifier file uses for importPath, or
// "" if file does not import it at all.
func importAlias(file *ast.File, importPath string) string {
	for _, imp := range file.Imports {
		path, err := strconv.Unquote(imp.Path.Value)
		if err != nil || path != importPath {
			continue
		}
		if imp.Name != nil {
			return imp.Name.Name
		}
		parts := strings.Split(path, "/")
		return parts[len(parts)-1]
	}
	return ""
}

// declaresTestMain reports whether file declares the special
// func TestMain(m *testing.M) hook go test recognizes.
func declaresTestMain(file *ast.File) bool {
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Recv != nil || fn.Name.Name != "TestMain" {
			continue
		}
		if fn.Type.Params != nil && len(fn.Type.Params.List) == 1 &&
			(fn.Type.Results == nil || len(fn.Type.Results.List) == 0) {
			return true
		}
	}
	return false
}

// hasWithCacheDirArg reports whether one of a senro.Run call's args is
// itself a call to senro.WithCacheDir(...), the option that stops that
// Run from ever resolving storage.DefaultRoot() at all.
func hasWithCacheDirArg(args []ast.Expr, senroAlias string) bool {
	for _, arg := range args {
		call, ok := arg.(*ast.CallExpr)
		if !ok {
			continue
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			continue
		}
		recv, ok := sel.X.(*ast.Ident)
		if ok && recv.Name == senroAlias && sel.Sel.Name == "WithCacheDir" {
			return true
		}
	}
	return false
}

// TestEveryTestPackageThatCanReachSenroRunClearsTheSharedCacheEnvironment is
// the shared-cache half of the check above, and a strictly wider net: the
// SHARED cache is reached by every senro.Run and RunPlan there is, because a
// run with no WithRemoteCache reads SENRO_REMOTE_CACHE (run.go), and
// WithCacheDir says nothing about it. A developer with that variable
// exported would have this suite writing into their team's bucket.
//
// So any package with a senro.Run or RunPlan call site in its tests must
// call remotecache.ClearEnv somewhere in its own test files. A textual check
// is enough: the call is one line with one spelling, and the failure this
// prevents is forgetting it entirely.
func TestEveryTestPackageThatCanReachSenroRunClearsTheSharedCacheEnvironment(t *testing.T) {
	repoRoot := moduleRoot(t)

	sites, err := scanForRunCallSites(repoRoot)
	if err != nil {
		t.Fatalf("scanning %s for _test.go files: %v", repoRoot, err)
	}
	if len(sites) == 0 {
		t.Fatal("found no senro.Run or senro.RunPlan call sites in any test file, which means " +
			"this scan is broken rather than that the module has none")
	}

	dirs := make([]string, 0, len(sites))
	for dir := range sites {
		dirs = append(dirs, dir)
	}
	sort.Strings(dirs)

	for _, dir := range dirs {
		clears, err := packageClearsRemoteCacheEnv(dir)
		if err != nil {
			t.Fatalf("reading %s: %v", dir, err)
		}
		if clears {
			continue
		}
		rel, _ := filepath.Rel(repoRoot, sites[dir])
		t.Errorf(`package %s runs a pipeline (%s) but never calls remotecache.ClearEnv.

senro.Run reads SENRO_REMOTE_CACHE from the environment when a caller passes
no WithRemoteCache, so on a machine where that is exported this package's
tests write into a real shared cache. WithCacheDir does not prevent it: that
isolates the LOCAL cache root only.

Add a TestMain to this package, or extend the one it has:

	func TestMain(m *testing.M) {
		remotecache.ClearEnv()
		os.Exit(m.Run())
	}
`, filepath.Base(dir), rel)
	}
}

// scanForRunCallSites maps each directory holding a _test.go file that calls
// senro.Run or senro.RunPlan to the first such file found in it.
func scanForRunCallSites(repoRoot string) (map[string]string, error) {
	sites := map[string]string{}
	err := filepath.WalkDir(repoRoot, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			// api is a package tree that cannot import senro at all, and the
			// worktree's own build output is not source.
			if d.Name() == "api" || d.Name() == "testdata" || strings.HasPrefix(d.Name(), ".") {
				return fs.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, "_test.go") {
			return nil
		}
		b, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		src := string(b)
		if !strings.Contains(src, "senro.Run(") && !strings.Contains(src, "senro.RunPlan(") {
			return nil
		}
		dir := filepath.Dir(path)
		if _, seen := sites[dir]; !seen {
			sites[dir] = path
		}
		return nil
	})
	return sites, err
}

// packageClearsRemoteCacheEnv reports whether any _test.go file in dir calls
// remotecache.ClearEnv.
func packageClearsRemoteCacheEnv(dir string) (bool, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return false, err
	}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), "_test.go") {
			continue
		}
		b, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			return false, err
		}
		if strings.Contains(string(b), "remotecache.ClearEnv()") {
			return true, nil
		}
	}
	return false, nil
}
