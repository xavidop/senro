//go:build !unix

package persist

import (
	"errors"
	"os"
)

// tryLock refuses on a platform with no advisory file lock this package
// knows how to take. A refusal rather than a no-op: no exclusion looks
// exactly like exclusion until two runs overlap and corrupt the tree.
// senro's supported targets (linux, darwin) both take the unix path; this
// file makes the unsupported case say so.
func tryLock(*os.File) (bool, error) {
	return false, errors.New(
		"persist: this build has no file lock, so it cannot guarantee that one run at a time " +
			"holds a persistent workspace; build senro for linux or darwin, or use " +
			"senro.ScopeRun workspaces")
}

func unlock(*os.File) error { return nil }
