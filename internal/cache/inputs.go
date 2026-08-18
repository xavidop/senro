package cache

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/xavidop/senro/artifact"
	"github.com/xavidop/senro/internal/cas"
	"github.com/xavidop/senro/internal/workspace"
)

// Resolve turns a step's declared selectors into sorted (path, digest)
// pairs, relative to root.
//
// A selector that matches nothing is an ERROR: a typo would otherwise leave
// a key that cannot change when the sources do, and the step would be
// served from cache forever.
//
// ex must be the SAME excluder the workspace's own snapshot uses. A
// selector matching a snapshot-excluded file would hash something the
// author believes does not exist, and since the excluded file changes every
// run while the workspace digest stays stable, the step would miss silently
// forever. ex may be nil, falling back to the two mandatory defaults, for a
// root that is not a declared workspace (a step with no workspace mount
// resolves Inputs against the coordinator's working directory).
func Resolve(root string, selectors []string, ex *workspace.Excluder) ([]FileDigest, error) {
	if len(selectors) == 0 {
		return nil, nil
	}
	if ex == nil {
		ex = workspace.NewExcluder(workspace.DefaultExcludes...)
	}
	sels := make([]artifact.Selector, 0, len(selectors))
	for _, s := range selectors {
		sel, err := artifact.Parse(s)
		if err != nil {
			return nil, fmt.Errorf("cache: %w", err)
		}
		sels = append(sels, sel)
	}

	// Refused before any I/O, so the error names the real problem instead
	// of "matched nothing": a file selector that leaves root would put an
	// undeclared file into the key and fail on the next machine.
	for _, sel := range sels {
		if sel.Kind() == "file" {
			if err := SafeRelative(sel.Pattern()); err != nil {
				return nil, err
			}
		}
	}

	found := make(map[string]cas.Digest)
	matched := make([]bool, len(sels))

	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if p == root {
			return nil
		}
		relOS, err := filepath.Rel(root, p)
		if err != nil {
			return err
		}
		rel := filepath.ToSlash(relOS)
		if ex.Match(rel, d.IsDir()) {
			if d.IsDir() {
				return fs.SkipDir
			}
			return nil
		}
		if d.IsDir() || !d.Type().IsRegular() {
			// Directories carry no content of their own, and an irregular
			// file is not portable, so neither belongs in a key.
			return nil
		}
		for i, sel := range sels {
			if !selects(sel, rel) {
				continue
			}
			matched[i] = true
			if _, seen := found[rel]; seen {
				continue
			}
			dg, err := digestFile(p)
			if err != nil {
				return err
			}
			found[rel] = dg
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("cache: resolve inputs under %s: %w", root, err)
	}

	for i, ok := range matched {
		if !ok {
			return nil, fmt.Errorf(
				"cache: declared selector %q matched no files under %s; a selector that matches nothing "+
					"leaves a key that cannot change when the sources do", sels[i].Serial(), root)
		}
	}

	out := make([]FileDigest, 0, len(found))
	for p, dg := range found {
		out = append(out, FileDigest{Path: p, Digest: dg})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Path < out[j].Path })
	return out, nil
}

// selects reports whether sel picks rel. A file selector is an exact
// relative path and never a pattern, so a path containing a glob character
// still means itself.
func selects(sel artifact.Selector, rel string) bool {
	switch sel.Kind() {
	case "file":
		return sel.Pattern() == rel
	default:
		return workspace.MatchGlob(sel.Pattern(), rel)
	}
}

func digestFile(p string) (cas.Digest, error) {
	f, err := os.Open(p)
	if err != nil {
		return "", err
	}
	defer func() { _ = f.Close() }()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return cas.Digest(cas.Prefix + hex.EncodeToString(h.Sum(nil))), nil
}

// SafeRelative rejects a declared path that leaves the input root.
//
// It checks filepath.Clean(rel) rather than raw bytes, catching every shape
// of escape (leading, trailing or interior "..") while still accepting a
// file that merely starts with two dots, such as "..keep".
func SafeRelative(rel string) error {
	if filepath.IsAbs(rel) {
		return fmt.Errorf("cache: %q leaves the input root", rel)
	}
	clean := filepath.ToSlash(filepath.Clean(rel))
	if clean == ".." || strings.HasPrefix(clean, "../") {
		return fmt.Errorf("cache: %q leaves the input root", rel)
	}
	return nil
}
