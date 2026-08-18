package dockerd

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
)

// runtimeCandidate names one well-known Engine API socket location and the
// runtime that puts a socket there, so an error naming every path tried can
// also say what each one is.
type runtimeCandidate struct {
	path    string
	runtime string
}

// candidates lists the well-known Engine API socket locations in probe
// order. /var/run/docker.sock first: the Linux default and the target of
// Docker Desktop's and OrbStack's compatibility symlinks, so one fast path
// covers three runtimes. Then Docker Desktop's own macOS socket, then each
// runtime's documented default.
func candidates() []runtimeCandidate {
	cs := []runtimeCandidate{
		{path: "/var/run/docker.sock",
			runtime: "Docker (Linux), or the path Docker Desktop and OrbStack both symlink on macOS"},
	}
	if home, err := os.UserHomeDir(); err == nil && home != "" {
		cs = append(cs,
			runtimeCandidate{path: home + "/.docker/run/docker.sock", runtime: "Docker Desktop (macOS)"},
			runtimeCandidate{path: home + "/.orbstack/run/docker.sock", runtime: "OrbStack"},
			runtimeCandidate{path: home + "/.colima/default/docker.sock", runtime: "colima (default profile)"},
			runtimeCandidate{path: home + "/.rd/docker.sock", runtime: "Rancher Desktop"},
			runtimeCandidate{
				path:    home + "/.local/share/containers/podman/machine/podman.sock",
				runtime: "Podman machine (macOS)",
			},
		)
	}
	cs = append(cs,
		runtimeCandidate{path: xdgRuntimeDir() + "/podman/podman.sock", runtime: "Podman (rootless, Linux)"},
		runtimeCandidate{path: "/run/podman/podman.sock", runtime: "Podman (rootful, Linux)"},
	)
	return cs
}

// xdgRuntimeDir is $XDG_RUNTIME_DIR, or Podman's own documented fallback
// when that is unset: /run/user/<uid>, the directory systemd's logind
// creates for every login session on the Linux distributions Podman targets.
func xdgRuntimeDir() string {
	if v := os.Getenv("XDG_RUNTIME_DIR"); v != "" {
		return v
	}
	return "/run/user/" + strconv.Itoa(os.Getuid())
}

// isSocket reports whether path exists and is a unix domain socket: a stray
// file or directory at a candidate path must not be mistaken for a daemon.
func isSocket(path string) bool {
	fi, err := os.Stat(path)
	if err != nil {
		return false
	}
	return fi.Mode()&os.ModeSocket != 0
}

// firstSocket returns the first candidate that is actually a socket on
// disk. false means no daemon at a well-known path, not no daemon anywhere,
// which is what the caller goes on to explain.
func firstSocket(cs []runtimeCandidate) (string, bool) {
	for _, c := range cs {
		if isSocket(c.path) {
			return c.path, true
		}
	}
	return "", false
}

// containerdSocketDefault is where containerd itself listens. Passed to
// noSocketError only to decide whether the error should mention it; see the
// package doc for why senro does not speak to containerd.
const containerdSocketDefault = "/run/containerd/containerd.sock"

// noSocketError reports every candidate tried and how to point senro at a
// daemon explicitly, so a Podman or colima user can see their socket was
// looked for. containerdPath is named in the message when it exists,
// because a containerd-only machine is exactly the state where every Engine
// API candidate is absent, and that user needs telling why.
func noSocketError(cs []runtimeCandidate, containerdPath string) error {
	var b strings.Builder
	b.WriteString("dockerd: no container runtime socket found. Tried:\n")
	for _, c := range cs {
		fmt.Fprintf(&b, "  - %s (%s)\n", c.path, c.runtime)
	}
	b.WriteString(
		"Start Docker, Podman, colima, OrbStack or Rancher Desktop, or point senro at a daemon " +
			"explicitly: DOCKER_HOST=unix:///path/to/your.sock")
	if isSocket(containerdPath) {
		fmt.Fprintf(&b,
			"\n\nA containerd socket exists at %s, but containerd is not Docker Engine API "+
				"compatible: it speaks gRPC, with containers, tasks and snapshots rather than the "+
				"Engine API's images and containers, so senro cannot talk to it directly. Install "+
				"Docker, Podman, or another Engine-API-compatible runtime to run senro's container "+
				"executor.", containerdPath)
	}
	return errors.New(b.String())
}

// resolve is SocketPath's implementation with every filesystem-dependent
// input passed explicitly, which is what lets discover_internal_test.go
// drive every discovery outcome deterministically, regardless of what is
// installed on the machine running `go test`.
func resolve(dockerHost string, cs []runtimeCandidate, containerdPath string) (string, error) {
	if dockerHost != "" {
		if !strings.HasPrefix(dockerHost, "unix://") {
			return "", fmt.Errorf(
				"dockerd: DOCKER_HOST is %q, and senro's container executor needs a daemon on this "+
					"machine: it bind-mounts coordinator directories for workspaces and secrets, which "+
					"a remote daemon cannot see. Unset DOCKER_HOST or point it at a unix:// socket",
				dockerHost)
		}
		return strings.TrimPrefix(dockerHost, "unix://"), nil
	}
	if p, ok := firstSocket(cs); ok {
		return p, nil
	}
	return "", noSocketError(cs, containerdPath)
}

// SocketPath resolves the daemon socket, refusing anything but a unix one.
//
// DOCKER_HOST wins when set: a unix:// value is used as-is, and tcp://,
// ssh:// or npipe:// is refused rather than silently falling through to
// discovery. Unset, the well-known locations are probed in candidates'
// order and the first actual socket wins; when none is found, the error
// names every path tried.
func SocketPath() (string, error) {
	return resolve(os.Getenv("DOCKER_HOST"), candidates(), containerdSocketDefault)
}
