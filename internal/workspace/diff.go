package workspace

import "sort"

// Kind is what an index entry describes. It is derived rather than stored:
// an Entry records a mode, a size, a digest and a link target, and those
// four already say which of the three shapes a snapshot can hold this is.
// Deriving it here rather than adding a field keeps the index's stored bytes
// (and therefore every index digest ever written) exactly as they are.
type Kind string

const (
	// KindFile is a regular file. It is the only kind that carries a digest,
	// which is also how it is told apart from a directory: an EMPTY regular
	// file still has the digest of no bytes, while a directory has none at
	// all.
	KindFile Kind = "file"
	// KindDir is a directory: no digest, no link, mode always 0755.
	KindDir Kind = "dir"
	// KindSymlink is a symlink, identified by a non-empty Link. A symlink's
	// target is the whole of its content; nothing else about it is stored.
	KindSymlink Kind = "symlink"
)

// Kind reports what this entry describes. See the Kind constants for why the
// classification is in this order.
func (e Entry) Kind() Kind {
	switch {
	case e.Link != "":
		return KindSymlink
	case e.Digest == "":
		return KindDir
	default:
		return KindFile
	}
}

// Status is what happened to one path between two snapshots.
//
// The values are the strings a machine-readable consumer reads, so they are
// part of `senro ws diff --json`'s contract and must not be reworded.
type Status string

const (
	// Added is a path present in the second snapshot and not the first.
	Added Status = "added"
	// Removed is a path present in the first snapshot and not the second.
	Removed Status = "removed"
	// Modified is a path whose content changed: a regular file whose digest
	// moved, or a symlink repointed at a different target. A symlink counts
	// because its target IS its content; there is nothing else in it.
	Modified Status = "modified"
	// ModeChanged is a path whose content is byte-identical and whose mode
	// is not: in practice chmod +x or its reversal, since 0644 and 0755 are
	// the only modes an index entry can hold. Separate from Modified
	// because "the bytes differ" would be false, and it is the change most
	// easily missed by eye.
	ModeChanged Status = "mode"
	// KindChanged is a path that is a different kind of thing than it was:
	// a file replaced by a directory, a directory by a symlink, and so on.
	// Kept apart from Modified for the same reason: a content digest
	// comparison is meaningless across two different kinds.
	KindChanged Status = "kind"
)

// Change is one path's difference between two indexes.
//
// A and B are the entries from the first and second index. Exactly one of
// them is the zero Entry for Added (no A) and Removed (no B); both are
// populated for every other status, so a caller can render sizes, modes,
// digests and symlink targets without holding either index open.
type Change struct {
	Path   string
	Status Status
	A      Entry
	B      Entry
}

// Diff reports what changed between two snapshots, from their indexes
// alone: the whole reason the index is a separate CAS object, so a diff
// between two multi-gigabyte workspaces costs two small JSON reads. It
// deliberately does NOT report what changed inside a file (that needs both
// bodies; `senro ws pull` each side). The result is always ordered by path,
// whatever order the inputs were built in.
func Diff(a, b Index) []Change {
	prev := make(map[string]Entry, len(a.Entries))
	for _, e := range a.Entries {
		prev[e.Path] = e
	}

	var out []Change
	seen := make(map[string]struct{}, len(b.Entries))
	for _, cur := range b.Entries {
		seen[cur.Path] = struct{}{}
		old, existed := prev[cur.Path]
		if !existed {
			out = append(out, Change{Path: cur.Path, Status: Added, B: cur})
			continue
		}
		if s, changed := compare(old, cur); changed {
			out = append(out, Change{Path: cur.Path, Status: s, A: old, B: cur})
		}
	}
	for _, old := range a.Entries {
		if _, stillThere := seen[old.Path]; !stillThere {
			out = append(out, Change{Path: old.Path, Status: Removed, A: old})
		}
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Path < out[j].Path })
	return out
}

// compare classifies one path present on both sides. The order of the tests
// is the point: kind first (a digest comparison across two different kinds
// is meaningless), then content, then mode, so a file that was chmod'd AND
// rewritten is reported as Modified rather than as a mode change that
// happens to also have moved every byte.
func compare(a, b Entry) (Status, bool) {
	if a.Kind() != b.Kind() {
		return KindChanged, true
	}
	switch a.Kind() {
	case KindSymlink:
		if a.Link != b.Link {
			return Modified, true
		}
	case KindFile:
		if a.Digest != b.Digest {
			return Modified, true
		}
	case KindDir:
		// A directory carries nothing but its mode, and normalize() fixes
		// that at 0755, so two directories at the same path are always the
		// same directory. The mode test below still runs and would report
		// an index written by some future build that widened this.
	}
	if a.Mode != b.Mode {
		return ModeChanged, true
	}
	return "", false
}
