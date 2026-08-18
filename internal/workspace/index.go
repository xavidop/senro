// Package workspace turns a directory into a digest and a digest back into a
// directory.
//
// A workspace is a named, versioned directory with a content digest, not a
// mount. A snapshot is a normalized tar plus a separate file index: the tar
// is the body, the index carries path, mode, size, digest and symlink target
// so `ws ls` never has to download the body.
//
// # Why normalization is the whole point
//
// tar records mtime, uid, gid and whatever order the walk produced. Every one
// of those is a fact about the machine that took the snapshot rather than
// about the files in it. `go build` rewrites files it did not change, so an
// unnormalized tar digests differently on every run, and because a workspace
// digest is an input to the next step's cache key, the cache stops hitting
// and nothing anywhere reports an error. This is the single most likely way
// to ship a cache that appears to work and never hits: normalization is not
// a tidiness measure, it is the correctness condition for everything
// downstream.
package workspace

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/xavidop/senro/internal/cas"
)

// IndexVersion is the index layout version. A reader that meets a version it
// does not know refuses rather than guessing: an index is how `ws ls` reports
// what is in a snapshot, and a wrong answer there sends someone debugging the
// wrong build.
const IndexVersion = 1

// Entry is one path in a snapshot.
type Entry struct {
	// Path is relative to the workspace root and always uses forward
	// slashes, on every platform, so an index taken on one host reads
	// identically on another.
	Path string `json:"path"`
	// Mode is the normalized permission bits: 0644 or 0755 for a regular
	// file, 0755 for a directory, 0777 for a symlink. Nothing else survives,
	// because nothing else is portable across executors.
	Mode uint32 `json:"mode"`
	// Size and Digest are set for regular files only.
	Size   int64      `json:"size,omitempty"`
	Digest cas.Digest `json:"digest,omitempty"`
	// Link is a symlink's target, verbatim.
	Link string `json:"link,omitempty"`
}

// Index is the file list of one snapshot.
type Index struct {
	Version int     `json:"version"`
	Entries []Entry `json:"entries"`
}

// Marshal encodes the index canonically: entries sorted by path, no HTML
// escaping, and a trailing newline. The bytes are the index's address in the
// CAS, so two indexes describing the same tree must encode identically or the
// digest naming one of them is wrong.
func (ix Index) Marshal() ([]byte, error) {
	out := Index{Version: ix.Version, Entries: append([]Entry(nil), ix.Entries...)}
	sort.Slice(out.Entries, func(i, j int) bool { return out.Entries[i].Path < out.Entries[j].Path })

	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	// Without this, encoding/json rewrites <, > and & as their unicode
	// escapes, so a path containing one of them would encode differently
	// from the same path assembled another way.
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "  ")
	if err := enc.Encode(out); err != nil {
		return nil, fmt.Errorf("workspace: marshal index: %w", err)
	}
	return buf.Bytes(), nil
}

// UnmarshalIndex decodes what Marshal produced.
func UnmarshalIndex(b []byte) (Index, error) {
	var ix Index
	if err := json.Unmarshal(b, &ix); err != nil {
		return Index{}, fmt.Errorf("workspace: unmarshal index: %w", err)
	}
	if ix.Version != IndexVersion {
		return Index{}, fmt.Errorf(
			"workspace: index version %d, this build understands %d: upgrade senro rather than reading a layout it does not know",
			ix.Version, IndexVersion)
	}
	return ix, nil
}

// Bytes is the total size of the regular files in the index. It is what
// ws.snapshot reports, and what the size-warning threshold (2 GiB by
// default) measures against.
func (ix Index) Bytes() int64 {
	var n int64
	for _, e := range ix.Entries {
		n += e.Size
	}
	return n
}
