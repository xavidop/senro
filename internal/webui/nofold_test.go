package webui_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/xavidop/senro/api"
)

// This file enforces, mechanically, the one property the browser UI exists
// to have: there is exactly one implementation of what an event means,
// api.RunState.Apply, and the browser calls it. The failure it guards
// against is quiet: a `case api.StepFinished:` added to a renderer, one
// fold rule subtly wrong, and a browser that reports a pass while the
// terminal reports a fail. Nothing about that fails a test unless
// something is watching for it. This is that.

// browserPackages is every package the WebAssembly client is built from,
// plus the two it shares with the terminal client.
var browserPackages = []string{
	".",         // internal/webui, the server
	"./present", // state to strings
	"./client",  // the WebAssembly client itself
	"../tail",   // the resume loop
	"../ndjson", // the wire decoder
}

// No package the browser UI is built from may name an event type. Naming
// one is the first line of a second fold: there is no reason to compare
// against api.StepFinished except to decide what it means, and deciding
// what it means is Apply's job.
func TestNoBrowserPackageInterpretsAnEventType(t *testing.T) {
	idents := eventTypeIdentifiers(t)
	if len(idents) < 20 {
		t.Fatalf("only found %d event type constants in api; this test is not looking at what it thinks it is", len(idents))
	}

	for _, pkg := range browserPackages {
		for _, file := range goFiles(t, pkg) {
			src := readFile(t, file)
			for _, id := range idents {
				// "api.StepFinished" and not merely "StepFinished": the
				// latter appears inside prose in doc comments, which is
				// exactly where these packages SHOULD be talking about
				// events.
				if strings.Contains(stripComments(t, file, src), "api."+id) {
					t.Errorf("%s names the event type api.%s. "+
						"Interpreting an event is api.RunState.Apply's job and there is one implementation of it; "+
						"if the folded state is missing something, add it to the fold rather than deciding here", file, id)
				}
			}
			for _, wire := range api.DeclaredTypes() {
				if strings.Contains(stripComments(t, file, src), `"`+string(wire)+`"`) {
					t.Errorf("%s contains the wire string %q, which is an event type spelled out by hand", file, wire)
				}
			}
		}
	}
}

// The fold is called from exactly one place in the browser's own code, and
// that place is the resume loop. Two call sites would not be wrong on their
// own, but they are how a second state, folded slightly differently, gets
// introduced without anybody deciding to introduce one.
func TestTheFoldIsCalledFromExactlyOnePlace(t *testing.T) {
	var sites []string
	for _, pkg := range browserPackages {
		for _, file := range goFiles(t, pkg) {
			if callsApply(t, file) {
				sites = append(sites, file)
			}
		}
	}
	if len(sites) != 1 {
		t.Fatalf("api.RunState.Apply is called from %d files (%v), want exactly 1", len(sites), sites)
	}
	if filepath.Base(sites[0]) != "tail.go" {
		t.Errorf("the fold is called from %s, want internal/tail/tail.go", sites[0])
	}
}

// The whole point of api being standard-library only is that a WebAssembly
// client can import it. A browser package that pulled in a third-party
// module, or the engine, would break that at the first transitive import.
func TestTheBrowserClientDependsOnNothingItCannotAfford(t *testing.T) {
	forbidden := []string{
		"github.com/xavidop/senro/internal/engine",
		"github.com/xavidop/senro/internal/source", // net/http, and with it 7MB of wasm
		"github.com/charmbracelet",
		"github.com/klauspost",
		"github.com/xavidop/mamori",
	}
	// internal/webui itself is the SERVER and legitimately speaks net/http;
	// the packages the wasm binary is built from are the ones that must not.
	wasmPackages := []string{"./present", "./client", "../tail", "../ndjson"}
	for _, pkg := range wasmPackages {
		for _, file := range goFiles(t, pkg) {
			src := readFile(t, file)
			for _, f := range forbidden {
				if strings.Contains(src, `"`+f) {
					t.Errorf("%s imports %s, which the WebAssembly client cannot carry", file, f)
				}
			}
			if strings.Contains(src, `"net/http"`) {
				t.Errorf("%s imports net/http. On js/wasm that links the whole HTTP client and server tree "+
					"to reach a wrapper around fetch, and takes the client from 4.0MB to 10.9MB", file)
			}
		}
	}
}

// callsApply reports whether a file contains a call to something's Apply
// method. Detected as a call expression through the AST rather than by
// searching for the text, so a doc comment that mentions Apply (and these
// packages mention it constantly, on purpose) is not counted as a call
// site, and so a call written across a line break still is.
func callsApply(t *testing.T, path string) bool {
	t.Helper()
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, path, nil, 0)
	if err != nil {
		t.Fatalf("parsing %s: %v", path, err)
	}
	var found bool
	ast.Inspect(file, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		if sel, ok := call.Fun.(*ast.SelectorExpr); ok && sel.Sel.Name == "Apply" {
			found = true
		}
		return true
	})
	return found
}

// eventTypeIdentifiers reads api's own source for the Go names of its event
// type constants, rather than hardcoding a list here that would silently
// stop covering a type somebody added.
func eventTypeIdentifiers(t *testing.T) []string {
	t.Helper()
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, filepath.Join("..", "..", "api", "event.go"), nil, 0)
	if err != nil {
		t.Fatalf("parsing api/event.go: %v", err)
	}
	var out []string
	for _, decl := range file.Decls {
		gen, ok := decl.(*ast.GenDecl)
		if !ok || gen.Tok != token.CONST {
			continue
		}
		for _, spec := range gen.Specs {
			vs, ok := spec.(*ast.ValueSpec)
			if !ok {
				continue
			}
			ident, ok := vs.Type.(*ast.Ident)
			if !ok || ident.Name != "Type" {
				continue
			}
			for _, name := range vs.Names {
				out = append(out, name.Name)
			}
		}
	}
	return out
}

// goFiles lists a package's non-test Go sources.
func goFiles(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("reading %s: %v", dir, err)
	}
	var out []string
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		out = append(out, filepath.Join(dir, name))
	}
	if len(out) == 0 {
		t.Fatalf("no Go files under %s: this test is looking in the wrong place", dir)
	}
	return out
}

func readFile(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading %s: %v", path, err)
	}
	return string(b)
}

// stripComments removes comments so a doc comment explaining WHY a package
// does not interpret events is not mistaken for it interpreting one. These
// packages talk about the fold at length, on purpose, and a check that
// could not tell prose from code would either be useless or would push that
// prose out of the source.
func stripComments(t *testing.T, path, src string) string {
	t.Helper()
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, path, src, 0) // 0: comments not retained
	if err != nil {
		t.Fatalf("parsing %s: %v", path, err)
	}
	var b strings.Builder
	ast.Inspect(file, func(n ast.Node) bool {
		switch v := n.(type) {
		case *ast.Ident:
			b.WriteString(v.Name)
			b.WriteString(" ")
		case *ast.SelectorExpr:
			if x, ok := v.X.(*ast.Ident); ok {
				b.WriteString(x.Name + "." + v.Sel.Name)
				b.WriteString(" ")
			}
		case *ast.BasicLit:
			b.WriteString(v.Value)
			b.WriteString(" ")
		}
		return true
	})
	return b.String()
}
