package containerexec

import (
	"context"
	"fmt"
	"os"

	"github.com/xavidop/senro/internal/dockerd"
	senroexec "github.com/xavidop/senro/internal/executor"
)

// BinDir is where a staged step binary appears INSIDE the container.
//
// A private root beside SecretMountPath, chosen the same way: /senro is not
// a path any base image populates, so nothing collides either way. The leaf
// name is senroexec.StagedName's, the same leaf sshexec writes on a remote
// host, so one digest is one file name wherever senro puts it.
const BinDir = "/senro/bin"

// StageBinary makes the coordinator's own binary reachable inside the
// containers this executor creates: senroexec.BinaryStager.
//
// A bind, not a copy. The target is the coordinator's own machine by this
// executor's founding requirement (every workspace is a bind, and
// internal/dockerd refuses a non-local daemon socket), so "staging" is
// telling the daemon to show the container the file that is already there,
// read-only, at a path named by its digest. Streaming a tar in, as sshexec
// does, would move 40 MiB per STEP into a writable layer discarded with the
// container; and an image cannot carry the binary, because it is the USER's
// pipeline binary with their registered functions compiled in.
//
// The file must exist and be a regular file: the daemon's answer to an
// absent bind source is to CREATE it, as a directory, so a missing binary
// would reach the step as a container whose command is a directory. The
// bytes are checked end to end instead: the re-entered child reports the
// digest of the file it actually IS on handshake, and the coordinator
// aborts the step on a mismatch.
func (e *Executor) StageBinary(
	_ context.Context, b senroexec.StagedBinary,
) (senroexec.Staging, error) {
	name, err := senroexec.StagedName(b.Digest)
	if err != nil {
		return senroexec.Staging{}, fmt.Errorf("containerexec: %w: %w", senroexec.ErrInfra, err)
	}
	fi, err := os.Stat(b.Path)
	if err != nil {
		return senroexec.Staging{}, fmt.Errorf(
			"containerexec: %w: reading %s to bind it into this step's container: %w",
			senroexec.ErrInfra, b.Path, err)
	}
	if !fi.Mode().IsRegular() {
		return senroexec.Staging{}, fmt.Errorf(
			"containerexec: %w: %s is not a regular file, so it cannot be bound into a container "+
				"as this step's binary", senroexec.ErrInfra, b.Path)
	}

	path := BinDir + "/" + name
	e.stageMu.Lock()
	defer e.stageMu.Unlock()
	if e.staged == nil {
		e.staged = map[string]string{}
	}
	e.staged[path] = b.Path
	// Reused is true from the first call onwards, because no call transfers
	// anything. See senroexec.Staging.
	return senroexec.Staging{Path: path, Reused: true}, nil
}

// binaryBind is the bind that makes a staged binary visible to one command,
// or no bind at all. Keyed on the command being run, deliberately: the bind
// puts senro's own executable inside an image somebody else built, and only
// the container whose command IS that binary has any business seeing it.
func (e *Executor) binaryBind(args []string) (dockerd.Bind, bool) {
	if len(args) == 0 {
		return dockerd.Bind{}, false
	}
	e.stageMu.Lock()
	defer e.stageMu.Unlock()
	host, ok := e.staged[args[0]]
	if !ok {
		return dockerd.Bind{}, false
	}
	return dockerd.Bind{Source: host, Target: args[0], ReadOnly: true}, true
}

// MountPath reports where a mount was realized inside the container:
// senroexec.MountLocator. This executor binds every mount at the declared
// path, so the answer is the declaration; it is still asked rather than
// assumed because a re-entered func step cannot open the coordinator-side
// directory on the other side of the bind.
func (s *sandbox) MountPath(name string) (string, bool) {
	m, ok := s.mounts[name]
	if !ok {
		return "", false
	}
	return m.At, true
}

var (
	_ senroexec.BinaryStager = (*Executor)(nil)
	_ senroexec.MountLocator = (*sandbox)(nil)
)
