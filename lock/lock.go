// Package lock reads and writes senro.lock, the file that pins every image a
// pipeline names by tag to the digest the tag pointed at when the lock was
// written. A tag moves; a digest does not, so a locked run executes the same
// bytes on every machine until somebody asks for a new lock.
//
// The package is standard library only and knows nothing about registries:
// resolving a tag to a digest is the caller's job, recording it is this one's.
package lock

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// Version is the only file format this package reads and writes.
const Version = 1

// ErrDrift is what a lock check wraps when the lock file no longer matches
// what the pipeline's tags resolve to: errors.Is(err, lock.ErrDrift) tells
// drift from a check that could not run.
var ErrDrift = errors.New("lock file is out of date")

// DriftDetected is the stderr line a lock check prints, on its own, when it
// finds drift. `senro lock --check` reads it to exit 1 whatever code the
// pipeline's main chose, and a shell can grep for it.
const DriftDetected = "senro: drift detected"

// File is the content of a lock file.
type File struct {
	Version int `json:"version"`
	// Images maps an image reference as the pipeline wrote it ("golang:1.26")
	// to the digest it resolved to ("sha256:...").
	Images map[string]string `json:"images"`
}

// Read loads a lock file. A missing file is an error wrapping
// fs.ErrNotExist, so a caller can tell "not locked" from "unreadable".
func Read(path string) (File, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return File{}, fmt.Errorf("lock: %w", err)
	}
	var f File
	if err := json.Unmarshal(b, &f); err != nil {
		return File{}, fmt.Errorf("lock: %s: %w", path, err)
	}
	if f.Version != Version {
		return File{}, fmt.Errorf("lock: %s is version %d, this senro reads version %d",
			path, f.Version, Version)
	}
	if f.Images == nil {
		f.Images = map[string]string{}
	}
	return f, nil
}

// Write stores f at path. The bytes depend only on the content (sorted keys,
// two-space indent, trailing newline), so an unchanged lock produces an
// unchanged diff. It is written to a temporary file and renamed, so a reader
// never sees half a lock. A zero Version is written as Version.
func Write(path string, f File) error {
	if f.Version == 0 {
		f.Version = Version
	}
	images := f.Images
	if images == nil {
		images = map[string]string{}
	}
	keys := make([]string, 0, len(images))
	for k := range images {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var sb strings.Builder
	fmt.Fprintf(&sb, "{\n  \"version\": %d,\n  \"images\": {", f.Version)
	for i, k := range keys {
		kb, err := json.Marshal(k)
		if err != nil {
			return fmt.Errorf("lock: %w", err)
		}
		vb, err := json.Marshal(images[k])
		if err != nil {
			return fmt.Errorf("lock: %w", err)
		}
		if i > 0 {
			sb.WriteString(",")
		}
		fmt.Fprintf(&sb, "\n    %s: %s", kb, vb)
	}
	if len(keys) > 0 {
		sb.WriteString("\n  ")
	}
	sb.WriteString("}\n}\n")

	tmp, err := os.CreateTemp(filepath.Dir(path), ".senro.lock-*")
	if err != nil {
		return fmt.Errorf("lock: %w", err)
	}
	defer func() { _ = os.Remove(tmp.Name()) }()
	if _, err := tmp.WriteString(sb.String()); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("lock: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("lock: %w", err)
	}
	if err := os.Chmod(tmp.Name(), 0o644); err != nil { //nolint:gosec // a lock file is meant to be committed and read
		return fmt.Errorf("lock: %w", err)
	}
	if err := os.Rename(tmp.Name(), path); err != nil {
		return fmt.Errorf("lock: %w", err)
	}
	return nil
}

// Pinned returns ref with its tag replaced by the locked digest
// ("golang:1.26" becomes "golang@sha256:..."), and false when ref is not
// locked. A ref that already names a digest is pinned as written: it is
// returned unchanged with true.
func (f File) Pinned(ref string) (string, bool) {
	if IsDigestRef(ref) {
		return ref, true
	}
	digest, ok := f.Images[ref]
	if !ok {
		return "", false
	}
	return stripTag(ref) + "@" + digest, true
}

// IsDigestRef reports whether ref already names an image by digest.
func IsDigestRef(ref string) bool {
	return strings.Contains(ref, "@sha256:")
}

// stripTag drops a trailing ":tag". A colon before the last slash belongs to
// a registry port ("localhost:5000/x"), not a tag.
func stripTag(ref string) string {
	slash := strings.LastIndex(ref, "/")
	if colon := strings.LastIndex(ref, ":"); colon > slash {
		return ref[:colon]
	}
	return ref
}
