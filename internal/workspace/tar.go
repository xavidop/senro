package workspace

import (
	"archive/tar"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/xavidop/senro/internal/cas"
)

// Epoch is the mtime every header in every snapshot carries. The value is
// arbitrary; that it is FIXED is not. See the package doc.
var Epoch = time.Unix(0, 0).UTC()

// ErrUnsafePath marks a tar entry whose destination, or whose symlink
// target, leaves the directory being restored into.
var ErrUnsafePath = errors.New("workspace: entry escapes the destination")

// WriteTar writes a normalized tar of root to w and returns its index.
//
// Normalization, in full, and every item is load-bearing:
//
//   - Entries are emitted in lexicographic order of their TAR name (a
//     directory's name carries its trailing "/"), by an explicit sort, so
//     the order cannot drift with a filesystem or a Go release.
//   - ModTime is Epoch on every entry; AccessTime and ChangeTime are the
//     ZERO time, not Epoch, because archive/tar writes a PAX record for a
//     non-zero one, carrying the producing machine's clock into the digest.
//   - Uid/Gid are 0 and Uname/Gname empty, or the digest would depend on
//     which account ran the build.
//   - Mode keeps only the executable bit for regular files (0644 or 0755)
//     and is fixed for directories and symlinks, so umask cannot move a
//     digest.
//   - PAXRecords are cleared, so no extended attribute reaches the body.
//
// Only regular files, directories and symlinks are emitted: devices,
// sockets and fifos do not survive a restore onto a different executor. An
// unreadable file is a hard error, not a silent omission: a tar missing
// content it promised is worse than a WriteTar that fails loudly.
func WriteTar(w io.Writer, root string, ex *Excluder) (Index, error) {
	items, err := collect(root, ex)
	if err != nil {
		return Index{}, err
	}

	tw := tar.NewWriter(w)
	ix := Index{Version: IndexVersion}
	for _, it := range items {
		e, err := writeEntry(tw, root, it.rel)
		if err != nil {
			return Index{}, err
		}
		ix.Entries = append(ix.Entries, e)
	}
	if err := tw.Close(); err != nil {
		return Index{}, fmt.Errorf("workspace: close tar for %s: %w", root, err)
	}
	return ix, nil
}

// walked is one path collect found: rel is the workspace-relative path used
// to look the entry back up on disk, name is what that entry's name will be
// IN THE TAR (directories get a trailing "/") and is therefore the correct
// key to sort by, since it is what a reader of the tar actually observes.
type walked struct {
	rel  string
	name string
}

// collect returns every non-excluded entry under root, sorted by tar name.
func collect(root string, ex *Excluder) ([]walked, error) {
	var out []walked
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
		if !d.IsDir() && !d.Type().IsRegular() && d.Type()&fs.ModeSymlink == 0 {
			// Device, socket or fifo: not portable across executors, so not
			// part of a workspace's identity either.
			return nil
		}
		name := rel
		if d.IsDir() {
			name = rel + "/"
		}
		out = append(out, walked{rel: rel, name: name})
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("workspace: walk %s: %w", root, err)
	}
	// Explicit, so ordering is this function's guarantee rather than
	// WalkDir's incidental one, and keyed on the TAR name so a directory
	// "m" and a file "m.txt" sort exactly the way the tar reader will see
	// them, not the way their bare relative paths happen to compare.
	sort.Slice(out, func(i, j int) bool { return out[i].name < out[j].name })
	return out, nil
}

func writeEntry(tw *tar.Writer, root, rel string) (Entry, error) {
	full := filepath.Join(root, filepath.FromSlash(rel))
	fi, err := os.Lstat(full)
	if err != nil {
		return Entry{}, fmt.Errorf("workspace: stat %s: %w", rel, err)
	}

	var link string
	if fi.Mode()&fs.ModeSymlink != 0 {
		if link, err = os.Readlink(full); err != nil {
			return Entry{}, fmt.Errorf("workspace: readlink %s: %w", rel, err)
		}
	}
	hdr, err := tar.FileInfoHeader(fi, link)
	if err != nil {
		return Entry{}, fmt.Errorf("workspace: header %s: %w", rel, err)
	}
	normalize(hdr, rel, fi)

	if err := tw.WriteHeader(hdr); err != nil {
		return Entry{}, fmt.Errorf("workspace: write header %s: %w", rel, err)
	}

	e := Entry{Path: rel, Mode: uint32(hdr.Mode), Link: link}
	if hdr.Typeflag != tar.TypeReg {
		return e, nil
	}

	f, err := os.Open(full)
	if err != nil {
		return Entry{}, fmt.Errorf("workspace: open %s: %w", rel, err)
	}
	defer func() { _ = f.Close() }()

	h := sha256.New()
	n, err := io.Copy(io.MultiWriter(tw, h), f)
	if err != nil {
		return Entry{}, fmt.Errorf("workspace: copy %s: %w", rel, err)
	}
	if n != hdr.Size {
		// The file changed under the snapshot. Every byte after this point
		// would be misaligned in the tar, so this is fatal rather than a
		// warning.
		return Entry{}, fmt.Errorf(
			"workspace: %s changed size during the snapshot (%d bytes declared, %d copied)", rel, hdr.Size, n)
	}
	e.Size = n
	e.Digest = cas.Digest(cas.Prefix + hex.EncodeToString(h.Sum(nil)))
	return e, nil
}

// normalize strips every field that records something about the machine that
// produced the file rather than the file itself. See WriteTar's doc for the
// full list; this function is why the cache can hit at all.
func normalize(hdr *tar.Header, rel string, fi fs.FileInfo) {
	hdr.Name = rel
	hdr.Format = tar.FormatPAX
	hdr.ModTime = Epoch
	hdr.AccessTime = time.Time{}
	hdr.ChangeTime = time.Time{}
	hdr.Uid, hdr.Gid = 0, 0
	hdr.Uname, hdr.Gname = "", ""
	hdr.PAXRecords = nil
	hdr.Devmajor, hdr.Devminor = 0, 0

	switch {
	case fi.IsDir():
		hdr.Typeflag = tar.TypeDir
		hdr.Name = rel + "/"
		hdr.Mode = 0o755
		hdr.Size = 0
	case fi.Mode()&fs.ModeSymlink != 0:
		hdr.Typeflag = tar.TypeSymlink
		hdr.Mode = 0o777
		hdr.Size = 0
	default:
		hdr.Typeflag = tar.TypeReg
		if fi.Mode().Perm()&0o111 != 0 {
			hdr.Mode = 0o755
		} else {
			hdr.Mode = 0o644
		}
	}
}

// ReadTar materializes a tar into dest, which must already exist, and
// returns the index of what it wrote.
//
// Restored mtimes are Epoch, matching the tar, so snapshotting a restored
// workspace reproduces the digest it came from. A build tool that keys off
// mtime rather than content sees every file as old, which is the safe
// direction: it rebuilds rather than skipping work it should have done.
func ReadTar(r io.Reader, dest string) (Index, error) {
	tr := tar.NewReader(r)
	ix := Index{Version: IndexVersion}
	var dirs []string

	for {
		hdr, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return Index{}, fmt.Errorf("workspace: read tar: %w", err)
		}
		rel := strings.TrimSuffix(hdr.Name, "/")
		target, err := safeJoin(dest, rel)
		if err != nil {
			return Index{}, err
		}

		switch hdr.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(target, 0o755); err != nil {
				return Index{}, fmt.Errorf("workspace: mkdir %s: %w", rel, err)
			}
			dirs = append(dirs, target)
			ix.Entries = append(ix.Entries, Entry{Path: rel, Mode: 0o755})

		case tar.TypeSymlink:
			if err := checkLinkTarget(rel, hdr.Linkname); err != nil {
				return Index{}, err
			}
			if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
				return Index{}, fmt.Errorf("workspace: mkdir for %s: %w", rel, err)
			}
			if err := os.Symlink(hdr.Linkname, target); err != nil {
				return Index{}, fmt.Errorf("workspace: symlink %s: %w", rel, err)
			}
			ix.Entries = append(ix.Entries, Entry{Path: rel, Mode: 0o777, Link: hdr.Linkname})

		case tar.TypeReg:
			e, err := restoreFile(tr, hdr, target, rel)
			if err != nil {
				return Index{}, err
			}
			ix.Entries = append(ix.Entries, e)

		default:
			// WriteTar emits nothing else, so this is a tarball from
			// somewhere unexpected. Skipping is right: refusing would make a
			// future additive entry type break every older reader.
			continue
		}
	}

	// Directory mtimes are set last, because writing a child updates its
	// parent. Deepest first, so a parent set now is not disturbed by a child
	// written after it.
	sort.Sort(sort.Reverse(sort.StringSlice(dirs)))
	for _, d := range dirs {
		if err := os.Chtimes(d, Epoch, Epoch); err != nil {
			return Index{}, fmt.Errorf("workspace: set times on %s: %w", d, err)
		}
	}
	return ix, nil
}

func restoreFile(tr io.Reader, hdr *tar.Header, target, rel string) (Entry, error) {
	if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
		return Entry{}, fmt.Errorf("workspace: mkdir for %s: %w", rel, err)
	}
	mode := fs.FileMode(hdr.Mode).Perm()
	f, err := os.OpenFile(target, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, mode)
	if err != nil {
		return Entry{}, fmt.Errorf("workspace: create %s: %w", rel, err)
	}
	h := sha256.New()
	n, copyErr := io.Copy(io.MultiWriter(f, h), tr)
	closeErr := f.Close()
	if copyErr != nil {
		return Entry{}, fmt.Errorf("workspace: write %s: %w", rel, copyErr)
	}
	if closeErr != nil {
		return Entry{}, fmt.Errorf("workspace: close %s: %w", rel, closeErr)
	}
	// OpenFile's mode is masked by umask, so it is not enough on its own.
	if err := os.Chmod(target, mode); err != nil {
		return Entry{}, fmt.Errorf("workspace: chmod %s: %w", rel, err)
	}
	if err := os.Chtimes(target, Epoch, Epoch); err != nil {
		return Entry{}, fmt.Errorf("workspace: set times on %s: %w", rel, err)
	}
	return Entry{
		Path:   rel,
		Mode:   uint32(mode),
		Size:   n,
		Digest: cas.Digest(cas.Prefix + hex.EncodeToString(h.Sum(nil))),
	}, nil
}

// safeJoin resolves rel under dest and refuses anything that leaves it. A
// workspace tarball is content from a previous run and, once one exists, from a shared
// cache backend, so this is untrusted input by construction.
func safeJoin(dest, rel string) (string, error) {
	clean := path.Clean("/" + rel)
	if clean == "/" {
		return "", fmt.Errorf("%w: empty entry name", ErrUnsafePath)
	}
	if strings.HasPrefix(rel, "/") || strings.Contains(rel, "..") {
		// Checked on the raw name as well as the cleaned one, so an entry
		// that cleans to something harmless but was written to look like a
		// traversal is still refused rather than silently rewritten.
		return "", fmt.Errorf("%w: %q", ErrUnsafePath, rel)
	}
	return filepath.Join(dest, filepath.FromSlash(strings.TrimPrefix(clean, "/"))), nil
}

// checkLinkTarget refuses a symlink pointing outside the workspace. A
// relative target is resolved against the link's own directory before it is
// judged, so "a/../b" inside the workspace is fine and "../../etc" is not.
func checkLinkTarget(rel, link string) error {
	if link == "" {
		return fmt.Errorf("%w: %q has an empty symlink target", ErrUnsafePath, rel)
	}
	if path.IsAbs(link) {
		return fmt.Errorf("%w: %q points at the absolute path %q", ErrUnsafePath, rel, link)
	}
	resolved := path.Clean(path.Join(path.Dir(rel), link))
	if resolved == ".." || strings.HasPrefix(resolved, "../") {
		return fmt.Errorf("%w: %q points at %q, outside the workspace", ErrUnsafePath, rel, link)
	}
	return nil
}
