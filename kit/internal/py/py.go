// Package py tells how to run a Python tool in a directory.
//
// A project locked with uv or poetry keeps its dependencies in an environment
// the bare `python` on PATH knows nothing about, so a kit asks here for the
// prefix that runs a tool inside that environment.
package py

import (
	"os"
	"path/filepath"
)

// Runner is a Python environment manager and the command prefix that runs a
// module or tool through it.
type Runner struct {
	// Name is one of uv, poetry or python.
	Name string
	// Argv is the command that precedes the tool: {"uv", "run"},
	// {"poetry", "run"} or {"python", "-m"}.
	Argv []string
}

// lockfiles are checked in order within one directory.
var lockfiles = []struct {
	file string
	r    Runner
}{
	{"uv.lock", Runner{"uv", []string{"uv", "run"}}},
	{"poetry.lock", Runner{"poetry", []string{"poetry", "run"}}},
}

// Detect walks from dir up to root inclusive and returns the runner of the
// first lockfile it finds, since a workspace keeps one lockfile at the top
// and the nearest one governs dir. With none it returns plain python. dir and
// root are filesystem paths, dir at or under root; both are cleaned first, so
// a trailing slash on root still stops the search there.
func Detect(dir, root string) Runner {
	root = filepath.Clean(root)
	for d := filepath.Clean(dir); ; d = filepath.Dir(d) {
		for _, l := range lockfiles {
			if _, err := os.Stat(filepath.Join(d, l.file)); err == nil {
				return l.r
			}
		}
		if d == root || d == filepath.Dir(d) {
			break
		}
	}
	return Runner{"python", []string{"python", "-m"}}
}
