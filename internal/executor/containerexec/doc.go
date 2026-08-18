// Package containerexec runs steps inside containers on the coordinator's own
// Docker daemon.
//
// Workspaces are bind mounts of coordinator directories: bind gives host-side
// visibility for debugging, which is worth more than the macOS perf win from
// volumes. Everything else follows from that, including the requirement for a
// local daemon (see internal/dockerd).
//
// # Secrets
//
// A secret's value is written to a per-sandbox directory under secretdir.Root
// (tmpfs on linux), 0600 inside 0700, and bind-mounted READ-ONLY at
// /run/senro/secrets. Not tar-to-stdin under a tmpfs mount: a tmpfs is
// mounted at container start, so a file copied in beforehand lands in the
// writable layer and is hidden when the tmpfs appears over it. The value is
// never in an image layer, a build arg, -e, --env-file, or any docker
// inspect field; inspect shows only the bind's source path. The directory is
// removed when the sandbox closes, on every path including keep.
//
// # Identity
//
// A container step runs as the coordinator's own uid:gid unless the pipeline
// declares container.User: a step running as root would leave root-owned
// files in runs/<id>/ws/<name> that the coordinator must snapshot and nobody
// can delete without sudo, and the caller's uid keeps the 0600 secret file
// readable by the step and nobody else.
//
// # Read-only mounts
//
// This executor ENFORCES senro.RO, because a bind mount can carry it (the
// local executor cannot; see senro.RO). A write through a read-only mount
// fails at the write, so engine.snapshotMounts' breach check never fires for
// a container step; it is the local executor's backstop.
//
// # Working directories
//
// A working directory must be absolute, exactly as a mount's At must be: a
// relative path inside a container has nothing to be relative to. Refused
// here rather than at the daemon so the error names the step (see
// checkWorkDir). A working directory that does not EXIST diverges the other
// way: the daemon creates it and the step runs in it, where the local
// executor's chdir fails. Detecting that would mean inspecting the image's
// filesystem first, so a WorkDir typo runs the step in an empty directory.
// Noted here rather than fixed.
//
// # The image's own entrypoint
//
// A step's command is sent as the container's Cmd and the image's ENTRYPOINT
// is left alone, as `docker run <image> <cmd>` does, so on an entrypoint
// image the command arrives as ARGUMENTS to the entrypoint and the exit code
// senro reports is the entrypoint's. The local executor executes Cmd.Args[0]
// directly. Clearing Entrypoint would change behaviour for every pipeline
// relying on one, so it is not done.
//
// # Func steps
//
// A func step re-enters the coordinator's own binary in the container as
// `senro-sha256-<digest> __step --state-fd 0`, state on stdin, frames on
// stdout (see internal/stepwire). The binary is made visible by a read-only
// BIND of the coordinator's own file, not a transfer: the daemon is on this
// filesystem by requirement. See staging.go. The entrypoint divergence
// matters most here: an entrypoint that swallows its arguments never runs
// the staged binary at all, so a func step wants an image with no entrypoint
// or one that execs its arguments.
//
// # Registry credentials
//
// A private image is pulled with the credential the target declared
// (executor/container.RegistryAuth), resolved from the run's secrets before
// the executor is constructed and handed here as bytes, the way a step's own
// secret arrives through PutSecret. It reaches one place: the pull's
// X-Registry-Auth header. A refused credential is reported apart from "no
// such image", naming the registry; see pullError. The credential is NOT
// part of the cache equivalence class, and Class says why.
//
// # What it is not
//
// No build, no push, no credential helper and no ~/.docker/config.json, no
// network configuration, no resource limits, no volumes. A step's writable
// layer is the analogue of the local executor's sandbox directory and is
// discarded with the container.
package containerexec
