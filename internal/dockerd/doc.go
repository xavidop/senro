// Package dockerd speaks the Docker Engine API over its unix socket.
//
// It exists so senro's container executor needs no module dependency: every
// request is net/http over a custom DialContext, and every payload is
// encoding/json. github.com/docker/docker would bring a very large tree into
// a project whose api module must have no dependencies at all.
//
// # Only a local daemon
//
// Open refuses a DOCKER_HOST that is not a unix socket. Every mount the
// container executor realizes is a bind mount of a coordinator directory, so
// a daemon on another host would create a container that cannot see any of
// it. A refusal at connect time says that once; an empty workspace says it
// as a mysterious build failure.
//
// # Any runtime that speaks the Engine API
//
// Podman, colima, OrbStack, Rancher Desktop and Docker Desktop all expose
// the Engine API over a unix socket at their own well-known paths.
// SocketPath probes those paths in a fixed order (see candidates in
// discover.go); a DOCKER_HOST the user set wins over all of them. When none
// is found, the error names every path tried and how to point senro at a
// daemon explicitly.
//
// # Not containerd
//
// containerd speaks gRPC with a genuinely different surface, so supporting
// it means the large dependency tree this package exists to avoid. When
// SocketPath finds nothing else but a containerd socket exists, the error
// says so plainly.
//
// # Not a general client
//
// The surface is exactly what one executor needs: inspect, pull, create,
// start, stream, wait, kill, remove. No build, push, exec or swarm.
//
// # Registry credentials
//
// ImagePull takes an optional RegistryAuth and sends it as the Engine API's
// X-Registry-Auth header. This package resolves nothing: the value arrives
// already resolved from senro's secrets, the way internal/oci's does, and
// senro runs no credential helper and reads no ~/.docker/config.json.
// A refused credential is reported as ErrRegistryAuth, because "the
// credential was refused" and "no such image" send two different people
// looking in two different places.
package dockerd
