package attachsrv

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"syscall"
	"time"
)

// Entry is one running engine's discovery record: everything a client needs
// to find and identify a run before ever dialing its socket. It is the JSON
// shape written to <registry dir>/<pid>.json by Register and read back by
// Discover.
type Entry struct {
	// PID is the engine process's own pid, also the entry's file name
	// (<PID>.json) and, by this package's convention, the stem of its
	// socket's default name (<PID>.sock, chosen by whoever calls Register;
	// Register itself never binds anything).
	PID int `json:"pid"`
	// Socket is the unix socket path Listen bound for this run, and ONLY
	// that: empty for a TCP run, where Addr carries the host:port.
	//
	// Kept apart from Addr rather than overloaded: Discover removes the
	// file this field names when it reaps a dead pid, and a host:port here
	// would turn that into an os.Remove of an arbitrary relative path.
	Socket string `json:"socket"`
	// Network is the transport: NetworkUnix or NetworkTCP. Empty means
	// NetworkUnix, so an entry written by a build that predates this field
	// still reads correctly.
	Network string `json:"network,omitempty"`
	// Addr is the address to dial: the same path as Socket for a unix run,
	// or the resolved host:port (with the real port, never :0) for a TCP
	// one. Register requires either this or Socket.
	Addr string `json:"addr,omitempty"`
	// TLS reports whether a TCP listener speaks TLS, which is what tells a
	// client to dial https rather than http.
	TLS bool `json:"tls,omitempty"`
	// Token is the per-run bearer credential a TCP listener requires on
	// every request. Empty for a unix run.
	//
	// A secret in a file, defensible here and only here: written 0600
	// inside a 0700 directory (see Dir), the same boundary the unix socket
	// has, so anybody who can read it could already have attached as this
	// user. It buys `senro attach` finding the credential itself, with
	// nothing for a person to copy and paste somewhere it does not belong.
	//
	// So this file must not be moved, copied or logged anywhere with weaker
	// permissions, and a TCP token must never be written into the RUN
	// directory, the artifact people attach to bug reports. See
	// attach.Attach.Token.
	Token string `json:"token,omitempty"`
	// RunID identifies the run, for a client (or a person) choosing between
	// several concurrently registered engines.
	RunID string `json:"run_id"`
	// Pipeline is the pipeline's name (senro.Pipeline.Name()).
	Pipeline string `json:"pipeline"`
	// CWD is the directory the engine was started from.
	CWD string `json:"cwd"`
	// StartedAt is when the engine registered itself.
	StartedAt time.Time `json:"started_at"`
	// EngineVersion identifies the build of senro that is running, so a
	// client can tell a version skew apart from an ordinary connection
	// failure.
	EngineVersion string `json:"engine_version"`
}

// registryRoot resolves the directory registry entries (and, by
// convention, each engine's own socket) live under: <runtime-dir>/senro.
// A plain function, not a method: there is exactly one registry per user
// per machine. Tests control it through the same environment variables the
// real resolution reads, not a package-private seam; see isolateRegistry.
func registryRoot() (string, error) {
	base, err := runtimeBaseDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(base, "senro"), nil
}

// runtimeBaseDir resolves the platform runtime directory registry entries
// (and sockets) are rooted under: $XDG_RUNTIME_DIR then /dev/shm on linux;
// os.UserCacheDir() everywhere else, since /dev/shm and $XDG_RUNTIME_DIR do
// not exist on darwin and os.UserCacheDir already resolves to
// $HOME/Library/Caches there.
func runtimeBaseDir() (string, error) {
	if runtime.GOOS == "linux" {
		return linuxRuntimeDir(os.Getenv("XDG_RUNTIME_DIR"), dirExists("/dev/shm"))
	}
	dir, err := os.UserCacheDir()
	if err != nil {
		return "", fmt.Errorf("attachsrv: resolve runtime dir: %w", err)
	}
	return dir, nil
}

// linuxRuntimeDir is runtimeBaseDir's linux decision as a pure function of
// plain values, so it is testable on any platform the suite runs on.
func linuxRuntimeDir(xdgRuntimeDir string, devShmExists bool) (string, error) {
	if xdgRuntimeDir != "" {
		return xdgRuntimeDir, nil
	}
	if devShmExists {
		return "/dev/shm", nil
	}
	return "", errors.New("attachsrv: neither $XDG_RUNTIME_DIR nor /dev/shm is available to resolve a runtime dir")
}

func dirExists(path string) bool {
	fi, err := os.Stat(path)
	return err == nil && fi.IsDir()
}

// Dir resolves and ensures the registry directory exists, mode 0700. Both
// registry entries (<pid>.json) and, by convention, each engine's socket
// (<pid>.sock) live here, so a caller choosing a socket path before
// Register exists can call this to learn where.
//
// The socket's own 0600 is the primary boundary for a connection; this
// directory's mode is a second, independent one: without it another local
// user could confirm a socket exists and attempt to connect before
// CheckPeer ever got a say. Applied unconditionally, since os.MkdirAll is
// a no-op on an existing directory that would keep looser permissions.
func Dir() (string, error) {
	dir, err := registryRoot()
	if err != nil {
		return "", err
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", fmt.Errorf("attachsrv: create registry dir: %w", err)
	}
	if err := os.Chmod(dir, 0o700); err != nil {
		return "", fmt.Errorf("attachsrv: chmod registry dir: %w", err)
	}
	return dir, nil
}

// Register writes entry as <registry dir>/<entry.PID>.json, mode 0600, and
// returns an idempotent func that removes it again. PID defaults to
// os.Getpid(), StartedAt to time.Now(); every other field is taken as
// given.
//
// Temp-file-then-rename, not a direct write: a concurrent Discover must
// never see a partially written entry, and os.Rename preserves the temp
// file's 0600.
func Register(entry Entry) (func(), error) {
	if entry.Socket == "" && entry.Addr == "" {
		return nil, errors.New("attachsrv: Entry.Socket or Entry.Addr is required: an entry with nothing to dial is not discoverable")
	}
	if entry.Addr == "" {
		entry.Addr = entry.Socket
	}
	if entry.PID == 0 {
		entry.PID = os.Getpid()
	}
	if entry.StartedAt.IsZero() {
		entry.StartedAt = time.Now().UTC()
	}

	dir, err := Dir()
	if err != nil {
		return nil, err
	}

	b, err := json.Marshal(entry)
	if err != nil {
		return nil, fmt.Errorf("attachsrv: marshal registry entry: %w", err)
	}

	path := entryPath(dir, entry.PID)
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o600); err != nil {
		return nil, fmt.Errorf("attachsrv: write registry entry: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return nil, fmt.Errorf("attachsrv: install registry entry: %w", err)
	}

	// Best-effort: "latest" is a convenience for callers choosing among
	// registered runs, not something Discover depends on, so a symlink
	// failure must not fail Register.
	//
	// Symlink-to-a-temp-then-rename, not remove-then-symlink: the latter
	// lets two concurrent Registers interleave into a window where "latest"
	// points at nothing and one of them fails on an unexpectedly present
	// path. Rename onto an existing path is atomic, so the update is
	// all-or-nothing; the temp name is per-pid so two Registers cannot
	// collide before either reaches its rename.
	latest := filepath.Join(dir, "latest")
	latestTmp := filepath.Join(dir, fmt.Sprintf(".latest.%d.tmp", entry.PID))
	_ = os.Remove(latestTmp) // a previous, interrupted attempt for this same pid, if any
	if err := os.Symlink(filepath.Base(path), latestTmp); err == nil {
		_ = os.Rename(latestTmp, latest)
	}

	cleanup := func() {
		_ = os.Remove(path)
		if target, err := os.Readlink(latest); err == nil && target == filepath.Base(path) {
			_ = os.Remove(latest)
		}
	}
	return cleanup, nil
}

// Discover lists every currently live registered entry, reaping any whose
// pid no longer names a running process: both its <pid>.json and the
// socket file it names, so nothing is left for a later process to trip
// over.
//
// The socket half matters on its own: a hard-killed engine (SIGKILL,
// os.Exit, an unrecovered panic) never unlinks its socket, and that file
// sits at the exact path a future process with the same recycled pid would
// derive for its own (attach.resolveBind's <pid>.sock convention), making
// its net.Listen fail with a bare "address already in use" long after this
// package's last chance to explain why.
//
// An unparseable entry is skipped rather than failing the whole scan: one
// bad entry must not hide every genuinely live run. A registry directory
// that has never been created returns empty and no error: that is the
// ordinary state before the first run.
func Discover() ([]Entry, error) {
	dir, err := registryRoot()
	if err != nil {
		return nil, err
	}

	dirEntries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("attachsrv: read registry dir: %w", err)
	}

	var out []Entry
	for _, de := range dirEntries {
		if de.IsDir() {
			continue
		}
		name := de.Name()
		if filepath.Ext(name) != ".json" {
			continue // "latest" symlink, a stray ".tmp" from an interrupted Register, etc.
		}

		path := filepath.Join(dir, name)
		b, err := os.ReadFile(path)
		if err != nil {
			continue // vanished mid-scan (a concurrent unregister), or unreadable: not this call's problem to solve
		}
		var e Entry
		if err := json.Unmarshal(b, &e); err != nil {
			continue // corrupt entry; skip it, do not fail every other live run's discovery over it
		}

		if !pidAlive(e.PID) {
			_ = os.Remove(path)
			// Best-effort: e.Socket may already be gone (a graceful Close
			// got there first) or empty (a malformed entry).
			if e.Socket != "" {
				_ = os.Remove(e.Socket)
			}
			continue
		}
		out = append(out, e)
	}
	return out, nil
}

// DialNetwork and DialAddr are what a client connects with, resolving the
// compatibility case in one place: an entry written before Network existed
// has neither field set and is a unix socket at Socket.
func (e Entry) DialNetwork() string {
	if e.Network == "" {
		return NetworkUnix
	}
	return e.Network
}

func (e Entry) DialAddr() string {
	if e.Addr != "" {
		return e.Addr
	}
	return e.Socket
}

func entryPath(dir string, pid int) string {
	return filepath.Join(dir, fmt.Sprintf("%d.json", pid))
}

// pidAlive reports whether pid names a running process, via
// syscall.Kill(pid, 0), which delivers no signal and only performs the
// kernel's existence check.
//
// pid <= 0 addresses a process group under kill(2), never a single engine,
// and is treated as dead: such an Entry is malformed.
//
// An unclassifiable error (anything but ESRCH or EPERM) is treated as
// ALIVE: a wrongly-kept stale entry merely fails to connect later, while a
// wrongly-reaped live one is gone for good.
func pidAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	err := syscall.Kill(pid, 0)
	switch {
	case err == nil:
		return true
	case errors.Is(err, syscall.ESRCH):
		return false
	default:
		// syscall.EPERM (exists, owned by someone else, still alive) and
		// every other, unclassified errno fall here.
		return true
	}
}
