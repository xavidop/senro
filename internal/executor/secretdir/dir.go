// Package secretdir owns the host directory a step's secret files live in.
//
// Shared by every executor that delivers a secret as a file on this machine:
// localexec hands the step the path, containerexec bind-mounts the directory
// read-only at /run/senro/secrets. One implementation, because the promises
// are the point: tmpfs where the platform has one, 0700 around a 0600 file,
// and removed on every Close path including keep, so a kept sandbox does not
// leave a credential on disk.
package secretdir

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
)

// Root is the directory tree secret files are created under.
//
// Secrets must not live under the sandbox directory: that sits inside the
// run directory, which users tar up and attach to bug reports.
//
// In order:
//
//   - $XDG_RUNTIME_DIR when set and a directory: per-user, 0700 by
//     convention, tmpfs-backed wherever it is set.
//   - /dev/shm on linux: tmpfs by definition; its own mode is 1777, so
//     isolation comes from the 0700 directory created inside it.
//   - os.TempDir() otherwise, the darwin case. It is DISK backed; senro does
//     not claim to shred, and the README says so.
func Root() string {
	if d := os.Getenv("XDG_RUNTIME_DIR"); d != "" {
		if fi, err := os.Stat(d); err == nil && fi.IsDir() {
			return d
		}
	}
	if runtime.GOOS == "linux" {
		if fi, err := os.Stat("/dev/shm"); err == nil && fi.IsDir() {
			return "/dev/shm"
		}
	}
	return os.TempDir()
}

// FileName reduces name to a single safe path element: a name like
// "Registry.Token" containing a separator would otherwise write outside the
// secret directory.
//
// No collision handling needed: two names cannot collide here without also
// colliding under plan.SecretEnvVar, which is strictly coarser and which
// plan.Validate already refuses.
func FileName(name string) string {
	b := make([]byte, 0, len(name))
	for i := 0; i < len(name); i++ {
		c := name[i]
		switch {
		case (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
			(c >= '0' && c <= '9') || c == '_' || c == '-':
			b = append(b, c)
		default:
			b = append(b, '_')
		}
	}
	if len(b) == 0 {
		return "secret"
	}
	return string(b)
}

// Dir is one sandbox's secret directory, created on demand and removed once.
// The zero value is ready to use and has created nothing.
type Dir struct {
	mu   sync.Mutex
	path string
}

// Ensure creates the directory if needed and returns its path. Separate from
// Put because the container executor needs the path BEFORE it has a value to
// write: a bind mount's source must exist when the container is created.
func (d *Dir) Ensure() (string, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.ensureLocked()
}

func (d *Dir) ensureLocked() (string, error) {
	if d.path != "" {
		return d.path, nil
	}
	p, err := os.MkdirTemp(Root(), "senro-secret-")
	if err != nil {
		return "", err
	}
	// MkdirTemp already creates at 0700; setting it explicitly means the mode
	// does not depend on a umask or on a future change to MkdirTemp.
	if err := os.Chmod(p, 0o700); err != nil {
		_ = os.RemoveAll(p)
		return "", err
	}
	d.path = p
	return p, nil
}

// Path is the directory, or "" when nothing has created it yet.
func (d *Dir) Path() string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.path
}

// Put writes one value and returns the file's path on THIS host. An executor
// whose step sees a different path (a container's bind target) translates it
// itself; this package only ever speaks about the host.
func (d *Dir) Put(name string, v []byte) (string, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	dir, err := d.ensureLocked()
	if err != nil {
		return "", err
	}
	p := filepath.Join(dir, FileName(name))
	if err := os.WriteFile(p, v, 0o600); err != nil {
		return "", err
	}
	return p, nil
}

// Remove deletes the directory and everything in it; safe to call twice.
//
// Removal, not shredding: on tmpfs the unlink frees the pages, on the darwin
// fallback the bytes may persist in free space (see Root).
func (d *Dir) Remove() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.path == "" {
		return nil
	}
	p := d.path
	d.path = ""
	if err := os.RemoveAll(p); err != nil {
		return fmt.Errorf("secretdir: removing %s: %w", p, err)
	}
	return nil
}
