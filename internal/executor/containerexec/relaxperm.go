package containerexec

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
)

// relaxOtherBits widens a bind-mounted tree so a container's declared,
// non-default User (plan.ExecutorSpec.User) can reach it.
//
// A bind mount preserves host ownership verbatim; there is no Docker
// equivalent to Kubernetes' fsGroup (see k8sexec.securityContext), and
// os.Chown to an arbitrary uid needs a privilege the coordinator does not
// have merely because a pipeline declared one. Widening the "other" bits
// needs none of that: only a path's OWNER may change its own mode, which
// the coordinator already is for everything it creates.
//
// Every existing bit is kept — this only ADDS bits, via a bitwise OR, so an
// already-open file never gets narrower and a deliberately restrictive one
// gains only what this feature requires. Symlinks are left alone: chmod on
// one follows it to a target that may sit outside the tree senro owns.
func relaxOtherBits(root string, dirBits, fileBits fs.FileMode) error {
	return filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.Type()&fs.ModeSymlink != 0 {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		bits := fileBits
		if d.IsDir() {
			bits = dirBits
		}
		if mode := info.Mode().Perm(); mode&bits != bits {
			if err := os.Chmod(path, mode|bits); err != nil {
				return err
			}
		}
		return nil
	})
}

// relaxMountForDeclaredUser widens one sandbox mount's host tree per ro: a
// read-only mount only ever needs to be OPENED by the declared user, a
// writable one needs a place to create files too.
func relaxMountForDeclaredUser(path string, ro bool) error {
	dirBits, fileBits := fs.FileMode(0o007), fs.FileMode(0o006)
	if ro {
		dirBits, fileBits = 0o005, 0o004
	}
	if err := relaxOtherBits(path, dirBits, fileBits); err != nil {
		return fmt.Errorf("widening %q for the declared container user: %w", path, err)
	}
	return nil
}

// relaxSecretDirForDeclaredUser lets a declared, non-default container user
// traverse INTO the secret directory without being able to list it: adding
// only the execute bit keeps `ls` refused while `cat <known path>` works,
// the same "openable only if you already know the name" boundary the
// 0700-around-0600 scheme draws for the coordinator's own account.
func relaxSecretDirForDeclaredUser(dir string) error {
	fi, err := os.Stat(dir)
	if err != nil {
		return err
	}
	return os.Chmod(dir, fi.Mode().Perm()|0o001)
}

// relaxSecretFileForDeclaredUser adds the read bit a declared, non-default
// container user needs to open a secret PutSecret already wrote at 0600.
func relaxSecretFileForDeclaredUser(path string) error {
	fi, err := os.Stat(path)
	if err != nil {
		return err
	}
	return os.Chmod(path, fi.Mode().Perm()|0o004)
}
