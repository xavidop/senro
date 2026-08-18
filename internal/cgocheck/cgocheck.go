// Package cgocheck finds cgo in a module's transitive dependencies.
//
// Cross-compiling a remote func step requires CGO_ENABLED=0, and the
// offenders are non-obvious (os/user under some build configurations, net
// without the netgo tag, anything wrapping a C library). Detected at plan
// time rather than at runtime on a remote host, and shipped as `senro func
// check` so binary provisioning inherits a tested detector.
package cgocheck

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"sort"
)

// Offender is one cgo-tainted package and how it got into the build.
type Offender struct {
	ImportPath string
	CgoFiles   []string
	// Chain is ONE import path from a root package to this one, root first:
	// the shortest is what a person needs to break the dependency, and
	// enumerating every path produces a report nobody reads.
	Chain []string
}

// runtimeCgo is the standard library's cgo glue package. The go tool adds
// it automatically wherever a graph uses `import "C"`, and it compiles its
// own cgo file in turn (undocumented in cmd/go, but visible in every
// cgo-bearing fixture here). It is not an offender anybody can act on, so
// excluding it keeps a report to the one thing worth fixing.
const runtimeCgo = "runtime/cgo"

type listPackage struct {
	ImportPath string   `json:"ImportPath"`
	CgoFiles   []string `json:"CgoFiles"`
	Imports    []string `json:"Imports"`
	DepOnly    bool     `json:"DepOnly"`
}

// Check runs `go list -deps -json` over patterns in dir and reports every
// package that compiles a cgo file.
//
// CGO_ENABLED=1 deliberately: the question is "does this graph CONTAIN
// cgo", and listing with cgo disabled drops the standard library's
// conditional cgo files, coming back clean for exactly the packages that
// are dangerous to cross-compile.
func Check(ctx context.Context, dir string, patterns ...string) ([]Offender, error) {
	if len(patterns) == 0 {
		patterns = []string{"./..."}
	}
	args := append([]string{"list", "-deps", "-json"}, patterns...)
	cmd := exec.CommandContext(ctx, "go", args...)
	cmd.Dir = dir
	cmd.Env = append(cmd.Environ(), "CGO_ENABLED=1")
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("cgocheck: go list in %s: %w: %s", dir, err, stderr.String())
	}

	var roots []string
	pkgs := map[string]listPackage{}
	dec := json.NewDecoder(&stdout)
	for dec.More() {
		var p listPackage
		if err := dec.Decode(&p); err != nil {
			return nil, fmt.Errorf("cgocheck: decoding go list output: %w", err)
		}
		pkgs[p.ImportPath] = p
		if !p.DepOnly {
			roots = append(roots, p.ImportPath)
		}
	}

	var out []Offender
	for path, p := range pkgs {
		if len(p.CgoFiles) == 0 || path == runtimeCgo {
			continue
		}
		out = append(out, Offender{
			ImportPath: path, CgoFiles: p.CgoFiles,
			Chain: shortestChain(pkgs, roots, path),
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ImportPath < out[j].ImportPath })
	return out, nil
}

// shortestChain is a breadth-first search from the roots to target, which
// yields the fewest links a person has to understand. A target that is
// itself a root matches immediately and returns just itself: nothing pulled
// it in, so nothing else belongs in the chain.
func shortestChain(pkgs map[string]listPackage, roots []string, target string) []string {
	prev := map[string]string{}
	seen := map[string]bool{}
	queue := append([]string(nil), roots...)
	for _, r := range roots {
		seen[r] = true
	}
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		if cur == target {
			var chain []string
			for at := cur; at != ""; at = prev[at] {
				chain = append([]string{at}, chain...)
			}
			return chain
		}
		imports := append([]string(nil), pkgs[cur].Imports...)
		sort.Strings(imports) // deterministic output for one graph
		for _, imp := range imports {
			if seen[imp] {
				continue
			}
			seen[imp] = true
			prev[imp] = cur
			queue = append(queue, imp)
		}
	}
	return []string{target}
}
