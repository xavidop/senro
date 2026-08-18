package api_test

import (
	"bytes"
	"os"
	"os/exec"
	"strings"
	"testing"
)

// TestNoDependencies enforces that the api package tree carries no
// dependencies beyond the standard library, so third-party clients and the
// WASM browser UI never pull in the engine's transitive tree. A hard
// contract, not a preference: if it fails, do not add the dependency.
//
// api is a plain package tree of the root module, with no per-package go.mod
// to inspect; TestNoDependenciesResolved below does the real enforcement,
// via the resolved import graph.
func TestNoDependencies(t *testing.T) {
	if _, err := os.Stat("go.mod"); err == nil {
		t.Fatal("api/go.mod exists again: api is supposed to be a plain package tree of the root module now, not its own module; see this test's doc comment")
	}
}

// TestNoDependenciesResolved asks the toolchain what api's package tree
// actually depends on, rather than trusting a declaration: api shares the
// root go.mod, so a disallowed import compiles with no new require line for
// a reviewer to catch. GOWORK=off keeps the result independent of the
// machine's GOWORK.
//
// It resolves twice, with and without -test: plain `go list -deps` covers
// only non-test files, so an import confined to a _test.go file would
// otherwise be invisible.
func TestNoDependenciesResolved(t *testing.T) {
	const self = "github.com/xavidop/senro/api"

	for _, tc := range []struct {
		name string
		args []string
	}{
		{"non-test", []string{"list", "-deps", "-f", "{{if not .Standard}}{{.ImportPath}}{{end}}", "./..."}},
		{"test", []string{"list", "-deps", "-test", "-f", "{{if not .Standard}}{{.ImportPath}}{{end}}", "./..."}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cmd := exec.Command("go", tc.args...)
			cmd.Env = append(os.Environ(), "GOWORK=off")

			var stderr bytes.Buffer
			cmd.Stderr = &stderr
			out, err := cmd.Output()
			if err != nil {
				t.Fatalf("go %s: %v\n%s", strings.Join(tc.args, " "), err, stderr.String())
			}

			for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
				pkg := strings.TrimSpace(line)
				if pkg == "" {
					continue
				}
				if isSelfImportPath(pkg, self) {
					continue // api's own packages, including synthetic test binaries
				}
				t.Errorf("api must depend only on the standard library, found %q (either a third-party module or a senro package outside api, both are disallowed here)", pkg)
			}
		})
	}
}

// isSelfImportPath reports whether pkg is api's own package, including the
// synthetic package identifiers `go list -test` invents for compiled test
// binaries, e.g.:
//
//	github.com/xavidop/senro/api.test
//	github.com/xavidop/senro/api_test [github.com/xavidop/senro/api.test]
//	github.com/xavidop/senro/api/schema_test [github.com/xavidop/senro/api/schema.test]
func isSelfImportPath(pkg, self string) bool {
	if i := strings.Index(pkg, " ["); i >= 0 {
		pkg = pkg[:i]
	}
	pkg = strings.TrimSuffix(pkg, ".test")
	pkg = strings.TrimSuffix(pkg, "_test")
	return pkg == self || strings.HasPrefix(pkg, self+"/")
}
