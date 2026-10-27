// Package pm tells which JavaScript package manager a directory uses.
//
// The lockfile is the answer, because it is what the team actually installs
// with. Running `npx` in a pnpm workspace works until it resolves a different
// version than the lockfile pins, so a kit asks here before it builds a
// command line.
package pm

import (
	"os"
	"path/filepath"
)

// Manager is a package manager and the command prefix that runs a locally
// installed binary through it.
type Manager struct {
	// Name is one of pnpm, yarn, bun or npm.
	Name string
	// Exec is the command that precedes a binary's name: {"pnpm", "exec"},
	// {"yarn"}, {"bunx"} or {"npx"}.
	Exec []string
}

// lockfiles are checked in order within one directory, so a directory holding
// two lockfiles resolves the same way every time.
var lockfiles = []struct {
	file string
	m    Manager
}{
	{"pnpm-lock.yaml", Manager{"pnpm", []string{"pnpm", "exec"}}},
	{"yarn.lock", Manager{"yarn", []string{"yarn"}}},
	{"bun.lockb", Manager{"bun", []string{"bunx"}}},
	{"bun.lock", Manager{"bun", []string{"bunx"}}},
	{"package-lock.json", Manager{"npm", []string{"npx"}}},
}

// Detect walks from dir up to root inclusive and returns the manager of the
// first lockfile it finds. A monorepo keeps one lockfile at the top, so the
// nearest one is the one that governs dir. With none it returns npm, which is
// what a bare package.json means. dir and root are filesystem paths, dir at
// or under root; both are cleaned first, so a trailing slash on root still
// stops the search there.
func Detect(dir, root string) Manager {
	root = filepath.Clean(root)
	for d := filepath.Clean(dir); ; d = filepath.Dir(d) {
		for _, l := range lockfiles {
			if _, err := os.Stat(filepath.Join(d, l.file)); err == nil {
				return l.m
			}
		}
		if d == root || d == filepath.Dir(d) {
			break
		}
	}
	return Manager{"npm", []string{"npx"}}
}
