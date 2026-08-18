package sshexec

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"

	senroexec "github.com/xavidop/senro/internal/executor"
	"github.com/xavidop/senro/internal/executor/mountxfer"
)

// copyIn streams one workspace's current content to the remote host: tar
// over the connection. What must be identical with k8sexec (the exclusion
// rule, the shape of the tar, what the digest is taken from) lives in
// internal/executor/mountxfer. The tar is workspace.WriteTar's normalized
// form, so the coordinator needs no tar of its own; see mountxfer.Send.
func (s *sandbox) copyIn(ctx context.Context, m senroexec.Mount) error {
	dir, ok := s.mountPath[m.Name]
	if !ok {
		return fmt.Errorf("sshexec: %w: step %q has no realized path for mount %q",
			senroexec.ErrInfra, s.spec.StepID, m.Name)
	}

	pr, pw := io.Pipe()
	done := make(chan error, 1)
	go func() {
		err := mountxfer.Send(pw, m)
		// CloseWithError(nil) is Close: the reader sees EOF only once Send
		// has finished, and the writer's error otherwise, so the remote tar
		// does not report a truncated archive as a corrupt one.
		_ = pw.CloseWithError(err)
		done <- err
	}()

	var errb bytes.Buffer
	res := s.ex.run(ctx, tarInScript(dir), pr, io.Discard, &errb)
	_ = pr.Close()
	writeErr := <-done

	if writeErr != nil {
		return fmt.Errorf("sshexec: %w: step %q: reading workspace %q to send it to %s: %w",
			senroexec.ErrInfra, s.spec.StepID, m.Name, s.ex.spec.Host, writeErr)
	}
	if res.err != nil {
		return fmt.Errorf("sshexec: %w: step %q: sending workspace %q to %s: %w%s",
			senroexec.ErrInfra, s.spec.StepID, m.Name, s.ex.spec.Host, res.err, detail(errb.String()))
	}
	if res.exit != 0 {
		return fmt.Errorf(
			"sshexec: %w: step %q: sending workspace %q to %s failed (exit %d). This executor needs "+
				"tar on the remote host%s",
			senroexec.ErrInfra, s.spec.StepID, m.Name, s.ex.spec.Host, res.exit, detail(errb.String()))
	}
	return nil
}

// Snapshot reads one workspace back off the remote host and captures it.
// The digest-from-what-came-back rule, the replacement, the excluded-path
// cost and the read-only rule are all mountxfer.Capture's; this executor's
// own part is copyOut, how the bytes are fetched.
func (s *sandbox) Snapshot(ctx context.Context, name string) (senroexec.Snapshot, error) {
	m, ok := s.mounts[name]
	if !ok {
		return senroexec.Snapshot{}, fmt.Errorf(
			"sshexec: %w: step %q has no mount named %q to snapshot",
			senroexec.ErrInfra, s.spec.StepID, name)
	}
	snap, err := mountxfer.Capture(ctx, s.ex.snap, m,
		func(ctx context.Context, dest string) error { return s.copyOut(ctx, m, dest) })
	if err != nil {
		return senroexec.Snapshot{}, fmt.Errorf("sshexec: %w: %w", senroexec.ErrInfra, err)
	}
	return snap, nil
}

// ReadMount pulls one mount's copy off the host into dest, without a digest
// and without replacing the coordinator's own directory. It is Snapshot's
// read-back with mountxfer.Capture's two workspace halves left off, over the
// same copyOut and therefore the same tar: a scratch cache is never evidence,
// so a digest of it would be a number nothing may read, and the engine keeps
// what came back aside rather than swapping it over a directory a sibling
// step may be sending out at the same moment.
//
// The cost is the transfer itself, once more per step: a scratch cache is
// often a large dependency tree, and it crosses the connection twice per
// attempt exactly as a workspace does. See internal/engine's readScratch.
func (s *sandbox) ReadMount(ctx context.Context, name, dest string) error {
	m, ok := s.mounts[name]
	if !ok {
		return fmt.Errorf("sshexec: %w: step %q has no mount named %q to read back",
			senroexec.ErrInfra, s.spec.StepID, name)
	}
	return s.copyOut(ctx, m, dest)
}

// copyOut streams one remote directory into dest.
func (s *sandbox) copyOut(ctx context.Context, m senroexec.Mount, dest string) error {
	dir, ok := s.mountPath[m.Name]
	if !ok {
		return fmt.Errorf("sshexec: %w: step %q has no realized path for mount %q",
			senroexec.ErrInfra, s.spec.StepID, m.Name)
	}
	pr, pw := io.Pipe()
	done := make(chan error, 1)
	go func() {
		err := mountxfer.Receive(pr, dest)
		// Closed with the error so the ssh process's own writes fail rather
		// than filling a pipe nobody drains, which would hold this open
		// until the whole workspace had been sent to a reader that gave up
		// on the first entry.
		_ = pr.CloseWithError(err)
		done <- err
	}()

	var errb bytes.Buffer
	res := s.ex.run(ctx, tarOutScript(dir), nil, pw, &errb)
	_ = pw.Close()
	readErr := <-done

	// Both halves are reported when both failed: they cause each other in
	// both directions, and reporting only one would print "read on closed
	// pipe" for a host that went away, or "unexpected EOF" for a tarball
	// senro refused, sending the reader to the wrong end.
	if err := errors.Join(res.err, exitErr(res.exit), readErr); err != nil {
		return fmt.Errorf("sshexec: %w: step %q: reading workspace %q back from %s: %w%s",
			senroexec.ErrInfra, s.spec.StepID, m.Name, s.ex.spec.Host, err, detail(errb.String()))
	}
	return nil
}
