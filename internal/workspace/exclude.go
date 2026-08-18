package workspace

import (
	"bufio"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

// IgnoreFile is the per-workspace exclusion list, read from the workspace
// root. One glob per line; "#" starts a comment; blank lines are ignored.
const IgnoreFile = ".senroignore"

// DefaultExcludes are the directories excluded from every workspace unless a
// caller builds an Excluder without them. The list is deliberately two
// entries rather than "and friends": every addition is a directory
// somebody's pipeline can no longer snapshot, and silently dropping a path
// is the worst failure this package has.
var DefaultExcludes = []string{".git/", "node_modules/"}

// DefaultExcludesFor returns the mandatory default excludes for one
// workspace's snapshot. With PreserveSymlinks, "node_modules/" is left out:
// a symlink tree like pnpm's has its real content in directories the store
// also names "node_modules", which the ordinary default would strip out
// from under the symlinks. ".git/" is excluded either way.
// DefaultExcludesFor(false) is byte-for-byte DefaultExcludes.
func DefaultExcludesFor(preserveSymlinks bool) []string {
	if preserveSymlinks {
		return []string{".git/"}
	}
	return append([]string(nil), DefaultExcludes...)
}

// Excluder decides which paths stay out of a snapshot.
//
// Pattern syntax, and only this syntax: "*" within one segment, "?" one
// character, "**" any number of segments, a trailing "/" a directory and
// everything under it at any depth. A pattern with a "/" matches the whole
// relative path. A pattern with NEITHER a "/" nor a trailing "/" is
// anchored to the workspace root, deliberately narrower than .gitignore
// (where a bare "top.log" reaches every directory): write "**/top.log" to
// match at any depth.
//
// Negation ("!pattern") is NOT supported: a half-implementation is how a
// file silently enters or leaves a snapshot and moves a digest for a reason
// nobody can see. LoadIgnoreFile refuses one by name.
type Excluder struct{ pats []string }

// NewExcluder compiles patterns. An Excluder with no patterns matches
// nothing at all: that is what an unfiltered snapshot needs, and an excluder
// that quietly matched everything would produce a stable, empty, wrong digest.
func NewExcluder(patterns ...string) *Excluder {
	pats := make([]string, 0, len(patterns))
	for _, p := range patterns {
		if p = strings.TrimSpace(p); p != "" {
			pats = append(pats, p)
		}
	}
	return &Excluder{pats: pats}
}

// ExcluderFor is what "part of this workspace" means for one directory: the
// mandatory defaults (widened by preserveSymlinks), the declared Exclude
// patterns, and any .senroignore at its root.
//
// It exists so the three places needing that answer cannot disagree: the
// snapshot, input/output resolution (engine's excluderFor), and
// re-execution for verification (internal/verify). A file excluded from the
// snapshot but still hashed into input_digests changes the key every run
// while the workspace digest stays stable, so the step misses forever and
// nothing says why; see cache.Resolve for the same trap from the other
// side.
//
// root is only read for the ignore file, so a directory that does not exist
// yet is not an error.
func ExcluderFor(root string, exclude []string, preserveSymlinks bool) (*Excluder, error) {
	patterns := append(DefaultExcludesFor(preserveSymlinks), exclude...)
	extra, err := LoadIgnoreFile(root)
	if err != nil {
		return nil, err
	}
	return NewExcluder(append(patterns, extra...)...), nil
}

// Match reports whether rel is excluded. rel is a forward-slash relative
// path; isDir says whether it is a directory, which is what a trailing "/"
// in a pattern keys off.
func (e *Excluder) Match(rel string, isDir bool) bool {
	if e == nil {
		return false
	}
	base := rel
	if i := strings.LastIndex(rel, "/"); i >= 0 {
		base = rel[i+1:]
	}
	for _, p := range e.pats {
		dirOnly := strings.HasSuffix(p, "/")
		pat := strings.TrimSuffix(p, "/")
		if dirOnly {
			// A directory pattern matches the directory and everything under
			// it, wherever it appears in the tree. That is what makes
			// "node_modules/" mean what everyone expects it to mean.
			if matchSegments(pat, base) && isDir {
				return true
			}
			for _, seg := range strings.Split(rel, "/") {
				if matchSegments(pat, seg) {
					return true
				}
			}
			continue
		}
		if strings.Contains(pat, "/") {
			if matchPath(pat, rel) {
				return true
			}
			continue
		}
		// No "/" anywhere in the pattern: anchored to the whole relative
		// path, not just the last segment. See the type doc for why a bare
		// pattern does not reach into subdirectories the way "**/pattern"
		// does.
		if matchSegments(pat, rel) {
			return true
		}
	}
	return false
}

// MatchGlob reports whether pattern matches the forward-slash relative path
// rel, using senro's one glob syntax (see Excluder), with a pattern lacking
// "/" anchored to the whole relative path. Exported so input selection
// (internal/cache) and workspace exclusion share one matcher: two
// implementations of "what does this pattern mean" is how a file ends up in
// a cache key but out of a snapshot.
func MatchGlob(pattern, rel string) bool {
	if strings.Contains(pattern, "/") {
		return matchPath(pattern, rel)
	}
	return matchSegments(pattern, rel)
}

// matchSegments matches a pattern with no "/" against a single segment.
func matchSegments(pat, seg string) bool {
	ok, err := filepath.Match(pat, seg)
	return err == nil && ok
}

// matchPath matches a pattern containing "/" against a whole relative path,
// with "**" spanning any number of segments. filepath.Match cannot do "**",
// so the path is split and matched segment by segment.
func matchPath(pat, rel string) bool {
	return matchParts(strings.Split(pat, "/"), strings.Split(rel, "/"))
}

func matchParts(pat, in []string) bool {
	for len(pat) > 0 {
		if pat[0] == "**" {
			// "**" matches zero or more segments: try every split point.
			for i := 0; i <= len(in); i++ {
				if matchParts(pat[1:], in[i:]) {
					return true
				}
			}
			return false
		}
		if len(in) == 0 {
			return false
		}
		if !matchSegments(pat[0], in[0]) {
			return false
		}
		pat, in = pat[1:], in[1:]
	}
	return len(in) == 0
}

// LoadIgnoreFile reads root/.senroignore. A workspace without one is the
// ordinary case, not an error.
func LoadIgnoreFile(root string) ([]string, error) {
	f, err := os.Open(filepath.Join(root, IgnoreFile))
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("workspace: %w", err)
	}
	defer func() { _ = f.Close() }()

	var out []string
	sc := bufio.NewScanner(f)
	for line := 1; sc.Scan(); line++ {
		s := strings.TrimSpace(sc.Text())
		if s == "" || strings.HasPrefix(s, "#") {
			continue
		}
		if strings.HasPrefix(s, "!") {
			return nil, fmt.Errorf(
				"workspace: %s line %d: %q is a negation pattern, which this build does not implement: "+
					"remove the pattern the negation was cancelling instead", IgnoreFile, line, s)
		}
		out = append(out, s)
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("workspace: read %s: %w", IgnoreFile, err)
	}
	return out, nil
}
