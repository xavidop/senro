package workspace

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
)

// Measure reports the total size of the regular files under root that
// would survive ex: exactly Snapshot.Bytes for the same tree, and what a
// persistent workspace's MaxSize is enforced against.
//
// It shares collect with WriteTar on purpose: a size bound enforced against
// a different file set than the digest describes is a bound nobody can
// reason about (TestMeasureAgreesWithASnapshotOfTheSameTree pins the
// equivalence). Symlinks and directories contribute nothing, matching
// Index.Bytes. A missing root measures zero, not an error: eviction removes
// the directory in the ordinary course of events. Nothing is hashed or
// stored, so this costs a walk and a stat per entry, which is what makes it
// affordable at the end of every run.
func Measure(root string, ex *Excluder) (int64, error) {
	if _, err := os.Stat(root); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return 0, nil
		}
		return 0, err
	}
	items, err := collect(root, ex)
	if err != nil {
		return 0, err
	}
	var total int64
	for _, it := range items {
		// Lstat, not Stat: a symlink contributes nothing (see the doc above),
		// and following one here would both double-count its target and let a
		// link pointing outside the workspace add bytes the snapshot never
		// carries.
		fi, err := os.Lstat(filepath.Join(root, filepath.FromSlash(it.rel)))
		if err != nil {
			// Gone since collect walked. A measurement is a bound check, not
			// a digest, so a vanished file contributes nothing rather than
			// failing the run.
			if errors.Is(err, fs.ErrNotExist) {
				continue
			}
			return 0, err
		}
		if fi.Mode().IsRegular() {
			total += fi.Size()
		}
	}
	return total, nil
}
