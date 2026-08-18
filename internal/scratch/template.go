package scratch

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
	"text/template"

	"github.com/xavidop/senro/internal/workspace"
)

// ExpandKey evaluates a scratch cache key template.
//
// One function, hashFiles, with patterns relative to root (the pipeline
// process's working directory): lock files are repository files, and the
// key must be computable BEFORE the first step runs. Deliberately not a
// general template environment: env would put machine state into a shared
// key, date would guarantee a miss every midnight, and anything unknown is
// an error so a typo cannot collapse every project's key to one prefix.
func ExpandKey(tmpl, root string) (string, error) {
	t, err := template.New("scratch-key").
		Option("missingkey=error").
		Funcs(template.FuncMap{
			"hashFiles": func(patterns ...string) (string, error) { return hashFiles(root, patterns) },
		}).
		Parse(tmpl)
	if err != nil {
		return "", fmt.Errorf("scratch: key template %q: %w", tmpl, err)
	}
	var b strings.Builder
	if err := t.Execute(&b, nil); err != nil {
		return "", fmt.Errorf("scratch: key template %q: %w", tmpl, err)
	}
	return b.String(), nil
}

// hashFiles digests every file matching any pattern, in sorted path order so
// the walk order cannot reach the key.
//
// A pattern matching nothing is an error. The alternative is a key that
// quietly becomes its own prefix, which on a shared cache root collides with
// every other project whose lock file is also missing.
func hashFiles(root string, patterns []string) (string, error) {
	if len(patterns) == 0 {
		return "", fmt.Errorf("scratch: hashFiles needs at least one pattern")
	}
	var paths []string
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
		if d.IsDir() {
			for _, skip := range workspace.DefaultExcludes {
				if workspace.MatchGlob(strings.TrimSuffix(skip, "/"), rel) {
					return fs.SkipDir
				}
			}
			return nil
		}
		if !d.Type().IsRegular() {
			return nil
		}
		for _, pat := range patterns {
			if workspace.MatchGlob(pat, rel) {
				paths = append(paths, rel)
				break
			}
		}
		return nil
	})
	if err != nil {
		return "", fmt.Errorf("scratch: hashFiles under %s: %w", root, err)
	}
	if len(paths) == 0 {
		return "", fmt.Errorf(
			"scratch: hashFiles matched no files under %s for %v; a key that silently drops its hash "+
				"collides with every other project's", root, patterns)
	}
	sort.Strings(paths)

	h := sha256.New()
	for _, rel := range paths {
		// The path goes into the hash as well as the content, so moving a
		// file changes the key even when the bytes do not.
		h.Write([]byte(rel))
		h.Write([]byte{0})
		f, err := os.Open(filepath.Join(root, filepath.FromSlash(rel)))
		if err != nil {
			return "", fmt.Errorf("scratch: hashFiles: %w", err)
		}
		_, copyErr := io.Copy(h, f)
		_ = f.Close()
		if copyErr != nil {
			return "", fmt.Errorf("scratch: hashFiles: %w", copyErr)
		}
	}
	return hex.EncodeToString(h.Sum(nil))[:32], nil
}
