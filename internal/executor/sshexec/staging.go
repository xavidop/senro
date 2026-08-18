package sshexec

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"strconv"

	senroexec "github.com/xavidop/senro/internal/executor"
)

// BinDirName is the directory, under the workspace root, that staged
// binaries live in. A SIBLING of the attempt directories, not a child of
// one: an attempt's directory is removed by Close or the reaper, and a
// binary inside one would be transferred again for every step. Here it
// survives the run, so the transfer is once per host per release.
const BinDirName = "bin"

// StageBinary puts a copy of the coordinator's binary on this host, at a
// path named after its digest, and reports whether anything was actually
// transferred: senroexec.BinaryStager.
//
// The path is <root>/bin/senro-<hex>, so two runs, two coordinators and two
// steps that name one digest name one file: a 40 MiB transfer is a per-host
// cost rather than a per-step one. Before uploading, one small session asks
// the host whether that path already holds a file of exactly the right
// size.
//
// Size, not a sha256sum over ssh: hashing 40 MiB on the far side costs real
// time per step. The bytes are verified end to end by the re-entered child,
// which reports the digest of the file it actually IS on handshake; the
// size check catches the one thing that cannot cheaply, a truncated upload
// left by a coordinator that died mid-transfer, which would otherwise be
// reused forever.
//
// The in-process memo only saves a round trip inside one run; the on-host
// check is what makes staging amortize across RUNS. See
// TestAFreshExecutorFindsAnAlreadyStagedBinaryWithoutTransferringIt.
func (e *Executor) StageBinary(
	ctx context.Context, b senroexec.StagedBinary,
) (senroexec.Staging, error) {
	name, err := stagedName(b.Digest)
	if err != nil {
		return senroexec.Staging{}, err
	}
	if err := e.resolve(ctx); err != nil {
		return senroexec.Staging{}, err
	}
	path := e.binDir() + "/" + name

	e.stageMu.Lock()
	defer e.stageMu.Unlock()
	if e.staged[b.Digest] {
		return senroexec.Staging{Path: path, Reused: true}, nil
	}

	present, err := e.hasStaged(ctx, path, b.Size)
	if err != nil {
		return senroexec.Staging{}, err
	}
	if !present {
		if err := e.pushBinary(ctx, b, path); err != nil {
			return senroexec.Staging{}, err
		}
	}
	if e.staged == nil {
		e.staged = map[string]bool{}
	}
	e.staged[b.Digest] = true
	return senroexec.Staging{Path: path, Reused: present}, nil
}

// binDir is where staged binaries live on this host.
func (e *Executor) binDir() string {
	root := e.spec.Root
	if root == "" {
		root = e.facts.home + "/" + DefaultRootName
	}
	return root + "/" + BinDirName
}

// hasStaged asks the host whether the binary is already there, whole.
func (e *Executor) hasStaged(ctx context.Context, path string, size int64) (bool, error) {
	var out, errb bytes.Buffer
	res := e.run(ctx, stagedCheckScript(path), nil, &out, &errb)
	if res.err != nil {
		return false, fmt.Errorf("sshexec: %w: checking %s on %s: %w%s",
			senroexec.ErrInfra, path, e.spec.Host, res.err, detail(errb.String()))
	}
	// The script exits 0 whether the file is there or not; a non-zero
	// status is the host refusing to answer, which must not be read as
	// "not there" and answered with a 40 MiB upload that fails the same
	// way.
	if res.exit != 0 {
		return false, fmt.Errorf("sshexec: %w: could not check %s on %s (exit %d)%s",
			senroexec.ErrInfra, path, e.spec.Host, res.exit, detail(errb.String()))
	}
	got, ok := parseFacts(out.String())["staged"]
	if !ok {
		return false, fmt.Errorf(
			"sshexec: %w: %s answered nothing about %s, so senro does not know whether the "+
				"step binary is there", senroexec.ErrInfra, e.spec.Host, path)
	}
	return got == strconv.FormatInt(size, 10), nil
}

// pushBinary streams the file across and publishes it with a rename.
func (e *Executor) pushBinary(ctx context.Context, b senroexec.StagedBinary, path string) error {
	f, err := os.Open(b.Path)
	if err != nil {
		return fmt.Errorf("sshexec: %w: reading %s to stage it on %s: %w",
			senroexec.ErrInfra, b.Path, e.spec.Host, err)
	}
	defer func() { _ = f.Close() }()

	// A nonce in the temporary name, so two coordinators staging the same
	// digest concurrently write two files and each renames its own whole
	// copy; without it they would interleave into one.
	nonce, err := newNonce()
	if err != nil {
		return fmt.Errorf("sshexec: %w: %w", senroexec.ErrInfra, err)
	}

	var errb bytes.Buffer
	res := e.run(ctx, stagePushScript(e.binDir(), path, nonce), f, io.Discard, &errb)
	if res.err != nil {
		return fmt.Errorf("sshexec: %w: staging the step binary on %s: %w%s",
			senroexec.ErrInfra, e.spec.Host, res.err, detail(errb.String()))
	}
	if res.exit != 0 {
		return fmt.Errorf("sshexec: %w: staging the step binary at %s on %s failed (exit %d)%s",
			senroexec.ErrInfra, path, e.spec.Host, res.exit, detail(errb.String()))
	}
	return nil
}

// stagedName is senroexec.StagedName plus what a malformed digest MEANS to
// this executor: an infrastructure failure, like every other refusal on the
// staging path.
func stagedName(digest string) (string, error) {
	name, err := senroexec.StagedName(digest)
	if err != nil {
		return "", fmt.Errorf("sshexec: %w: %w", senroexec.ErrInfra, err)
	}
	return name, nil
}

// stagedCheckScript prints the size of the staged binary, or nothing at all,
// and exits 0 either way. See hasStaged for why the two are not collapsed
// into an exit status.
func stagedCheckScript(path string) string {
	p := quote(path)
	return "if [ -f " + p + " ] && [ -x " + p + " ]; then\n" +
		"printf '" + factPrefix + "staged %s\\n' \"$(wc -c < " + p + " | tr -d ' ')\"\n" +
		"else\n" +
		"printf '" + factPrefix + "staged %s\\n' none\n" +
		"fi\n"
}

// stagePushScript receives the binary on stdin and publishes it atomically.
//
// umask 077 comes before anything is created, so the file is private from
// its first byte: this is an executable in somebody else's home directory,
// and a window in which it is world-readable is one in which it is
// world-RUNNABLE. The chmod only ADDS the execute bit to a file the umask
// already made 0600; the directory chmod covers one an older senro left
// wider.
//
// The rename is what makes a killed transfer harmless: a coordinator that
// dies mid-push leaves a .senro-staging- file nothing looks for, never a
// half-written binary at the path a later run will execute.
func stagePushScript(dir, path, nonce string) string {
	tmp := quote(dir + "/.senro-staging-" + nonce)
	return "umask 077\n" +
		"mkdir -p " + quote(dir) + " || exit 1\n" +
		"chmod 700 " + quote(dir) + " || exit 1\n" +
		"cat > " + tmp + " || exit 1\n" +
		"chmod 700 " + tmp + " || exit 1\n" +
		"mv -f " + tmp + " " + quote(path) + " || { rm -f " + tmp + "; exit 1; }\n"
}

// MountPath reports where a mount was realized on the remote host:
// senroexec.MountLocator. A command step never needs it (runScript sends it
// into the mount's own directory); a re-entered func child does, because
// ctx.Workspace(name) must be THIS host's path, not the coordinator's.
func (s *sandbox) MountPath(name string) (string, bool) {
	p, ok := s.mountPath[name]
	return p, ok
}

var (
	_ senroexec.BinaryStager = (*Executor)(nil)
	_ senroexec.MountLocator = (*sandbox)(nil)
)
