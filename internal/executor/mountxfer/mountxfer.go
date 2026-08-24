// Package mountxfer moves one mounted workspace between the coordinator and
// an execution target that does not share its filesystem.
//
// It is the twin of internal/executor/mountsnap: mountsnap owns "what counts
// as part of this workspace when it is captured", this package owns "what
// crosses the wire and what comes back", and the two must agree exactly or a
// round trip is not one.
//
// sshexec and k8sexec both use it; they differ only in how the process on
// the far side is started, so Capture takes the pull as a function.
package mountxfer

import (
	"archive/tar"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/xavidop/senro/internal/executor"
	"github.com/xavidop/senro/internal/executor/mountsnap"
	"github.com/xavidop/senro/internal/workspace"
)

// ExcluderFor is the exclusion set for one mount, and it is
// mountsnap.Excluder's answer rather than a second computation of it: a file
// sent out but excluded from the snapshot would vanish from the
// coordinator's copy, and a file excluded out but included back would appear
// from nowhere.
//
// An unreadable .senroignore is not fatal here: mountsnap.Snapshot reads the
// same file and reports the failure at snapshot time, where it belongs to
// the step's result rather than its setup.
func ExcluderFor(m executor.Mount) *workspace.Excluder {
	ex, _ := mountsnap.Excluder(m)
	return ex
}

// Send writes m's current content to w as a normalized tar (sorted, fixed
// mtimes, no uid or gid, modes reduced to the executable bit): the same
// bytes a snapshot would digest, so a round trip reproduces a digest and not
// merely a directory. workspace.WriteTar produces it, so the coordinator
// needs no tar binary. Exclusions apply on the way out, so .git and
// node_modules do not cross; see Capture for what that costs on the way
// back.
func Send(w io.Writer, m executor.Mount) error {
	fi, err := os.Stat(m.Path)
	if err != nil {
		return fmt.Errorf("mount %q source %q: %w", m.Name, m.Path, err)
	}
	if !fi.IsDir() {
		return fmt.Errorf("mount %q source %q is not a directory", m.Name, m.Path)
	}
	_, err = workspace.WriteTar(w, m.Path, ExcluderFor(m))
	return err
}

// Capture reads one workspace back off the target and captures it.
//
// pull is the executor's half: it must write the target's copy of the
// workspace into the directory it is given, and must fail rather than return
// a partial copy. Everything else is the same on every executor.
//
// The sequence: stage into a fresh directory, pull into it, snapshot the
// STAGING directory through the same function the local and container
// executors use, then swap staging into the coordinator's workspace
// directory. Snapshotting staging matters: a digest of the coordinator's own
// copy would describe bytes the step never touched, and the digest feeds the
// ledger, ws.snapshot and the next step's cache key.
//
// The swap is replacement, not a merge: a merge would keep every file the
// step deleted, so the directory would no longer be what the digest
// describes. workspace.Snapshotter.Restore makes the same choice.
//
// The cost: excluded paths (.git, node_modules) are not sent, so they are
// not in what comes back, and the swap removes them. A cache hit already
// behaves this way. A step that needs the repository's history on the far
// side should declare PreserveSymlinks and its own exclusions, or fetch what
// it needs over there.
//
// A read-only mount is snapshotted and NOT swapped: the snapshot is what
// lets engine.snapshotMounts compare digests before and after to catch a
// step that wrote through a read-only mount. Skipping the read would hide
// the breach; swapping would carry it home.
func Capture(
	ctx context.Context, snap *workspace.Snapshotter, m executor.Mount,
	pull func(ctx context.Context, dest string) error,
) (executor.Snapshot, error) {
	// Beside the workspace, not in a temp directory: the swap below removes
	// dest before renaming, so the rename must stay within one filesystem.
	staging, err := os.MkdirTemp(filepath.Dir(m.Path), ".senro-xfer-"+safeName(m.Name)+"-")
	if err != nil {
		return executor.Snapshot{}, fmt.Errorf("staging workspace %q: %w", m.Name, err)
	}
	// MkdirTemp creates at 0700, and this directory becomes the workspace.
	// The engine creates workspaces at 0755 (engine.newWSManager); leaving
	// 0700 behind would quietly narrow a directory later steps share, such
	// as a container step running as its own user.
	if err := os.Chmod(staging, 0o755); err != nil {
		_ = os.RemoveAll(staging)
		return executor.Snapshot{}, fmt.Errorf("staging workspace %q: %w", m.Name, err)
	}
	keep := false
	defer func() {
		if !keep {
			_ = os.RemoveAll(staging)
		}
	}()

	if err := pull(ctx, staging); err != nil {
		return executor.Snapshot{}, err
	}

	// The mount handed to mountsnap points at STAGING, so the digest is of
	// what the target actually holds; exclusions and symlink policy travel
	// unchanged so every executor computes the digest by the same rule.
	staged := m
	staged.Path = staging
	out, err := mountsnap.Snapshot(ctx, snap, staged)
	if err != nil {
		return executor.Snapshot{}, err
	}
	if m.RO {
		return out, nil
	}
	if err := swap(staging, m.Path); err != nil {
		return executor.Snapshot{}, fmt.Errorf("putting workspace %q back: %w", m.Name, err)
	}
	keep = true
	return out, nil
}

// swap replaces dest with staging. RemoveAll then Rename, as
// workspace.Snapshotter.Restore does: POSIX has no atomic directory
// replacement, so the window between the two is real and the engine's
// per-workspace locking keeps sibling steps out of it.
func swap(staging, dest string) error {
	if err := os.RemoveAll(dest); err != nil {
		return err
	}
	if err := os.Rename(staging, dest); err != nil {
		// A cross-device rename cannot happen: staging sits in dest's own
		// parent. Reported rather than worked around, because dest is
		// already removed and the run needs to know.
		return fmt.Errorf("renaming %s to %s: %w", staging, dest, err)
	}
	return nil
}

// safeName reduces a workspace name (an arbitrary Go string) to something
// bounded that can appear in a directory name beside the workspace.
func safeName(name string) string {
	b := make([]byte, 0, len(name))
	for i := 0; i < len(name) && len(b) < 40; i++ {
		c := name[i]
		switch {
		case (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9'),
			c == '_' || c == '-' || c == '.':
			b = append(b, c)
		default:
			b = append(b, '_')
		}
	}
	return string(b)
}

// Receive extracts a tarball a REMOTE `tar` produced, through
// workspace.ReadTar.
//
// `tar -cf - .` names its entries "./src"; the adapter rewrites them to the
// prefix-free form workspace.WriteTar produces rather than relaxing ReadTar,
// because this tarball arrives from a machine a step just ran arbitrary
// commands on and ReadTar's ".." refusal and symlink target check must stay
// exactly as they are.
//
// ReadTar drops entry types other than a regular file, a directory or a
// symlink. A hard link is dropped rather than followed: senro's snapshots
// never carry one, so a workspace relying on one would not survive a cache
// restore either.
func Receive(r io.Reader, dest string) error {
	pr, pw := io.Pipe()
	go func() { _ = pw.CloseWithError(stripDotSlash(r, pw)) }()
	_, err := workspace.ReadTar(pr, dest)
	// Close the read side too: an early return from ReadTar would otherwise
	// leave the rewriter blocked writing into a pipe with no reader, and
	// that goroutine holds the far side's output pipe.
	_ = pr.CloseWithError(err)
	return err
}

// RequireDir is executor.MountReader's precondition, in one place: dest must
// already exist and be a directory.
//
// The interface asks for it because a ReadMount that created its own
// destination would silently succeed for a caller that got the path wrong,
// and the caller would then store a tree rooted somewhere it never meant
// under a key it can never rewrite. workspace.ReadTar creates every
// directory it needs, so nothing below this would ever notice.
//
// A directory that exists and is not one is the same mistake and gets the
// same refusal; the caller wraps this with its own prefix and with ErrInfra.
func RequireDir(dest string) error {
	fi, err := os.Stat(dest)
	if err != nil {
		return fmt.Errorf("the destination directory must exist: %w", err)
	}
	if !fi.IsDir() {
		return fmt.Errorf("the destination %s is not a directory", dest)
	}
	return nil
}

// stripDotSlash copies a tar stream, rewriting "./x" to "x" and dropping the
// root entry, and refuses an absolute name outright rather than rewriting it
// into a relative one that would then look harmless.
func stripDotSlash(r io.Reader, w io.Writer) error {
	tr := tar.NewReader(r)
	tw := tar.NewWriter(w)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("reading the tar from the far side: %w", err)
		}
		name := strings.TrimPrefix(hdr.Name, "./")
		if name == "" || name == "." || name == "./" {
			continue
		}
		if strings.HasPrefix(name, "/") {
			return fmt.Errorf(
				"%w: the tar from the far side carries an absolute entry %q",
				workspace.ErrUnsafePath, hdr.Name)
		}
		out := *hdr
		out.Name = name
		if err := tw.WriteHeader(&out); err != nil {
			return err
		}
		if hdr.Typeflag == tar.TypeReg {
			if _, err := io.Copy(tw, tr); err != nil {
				return err
			}
		}
	}
	return tw.Close()
}
