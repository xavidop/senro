package containerexec

import (
	"context"
	"fmt"
	"io"

	senroexec "github.com/xavidop/senro/internal/executor"
)

// This executor hosts a terminal as well as a shell. See
// internal/executor.Terminal for why the two are separate capabilities.
var _ senroexec.Terminal = (*sandbox)(nil)

// RunTerminal runs one command on a pseudo-terminal the DAEMON allocates.
//
// Three differences from RunInteractive, each a consequence of a terminal
// being one device rather than two streams:
//
//   - Tty is set on the container. For a STEP that is non-negotiable (senro
//     records a step's output as two streams); a session is the one caller
//     for which it is negotiable.
//   - The attach stream is read RAW: with a TTY the daemon stops
//     multiplexing, so Demux would parse the container's own output as
//     frame headers and show garbage.
//   - Resizes go to the daemon's resize endpoint, not an ioctl: the pty is
//     on the daemon's side and this process never holds its master.
//
// The size is set at CREATE, through HostConfig.ConsoleSize, and not only
// after start. ContainerResize is too late by construction: the endpoint
// answers 500 for a container that is not running, so a terminal sized only
// afterwards has already let the command read whatever the device reported
// first — and it does, every time, not occasionally (`stty size` in a
// session sized after start fails outright). A full-screen program reads
// its size once, at startup. The resize call after start stays: it costs
// one request and covers a daemon too old for the create-time field.
func (s *sandbox) RunTerminal(
	ctx context.Context, c senroexec.Cmd, stdin io.Reader, out io.Writer,
	initial senroexec.WinSize, resize <-chan senroexec.WinSize,
) (int, error) {
	if len(c.Args) == 0 {
		return 0, fmt.Errorf("containerexec: %w: empty command", senroexec.ErrInfra)
	}
	spec, err := s.containerSpecFor(c)
	if err != nil {
		return 0, err
	}
	spec.Stdin = true
	spec.Tty = true
	if initial.Cols > 0 && initial.Rows > 0 {
		// {rows, cols}: the daemon's own order, which dockerd.ConsoleSize
		// carries unchanged.
		spec.ConsoleSize = [2]uint16{initial.Rows, initial.Cols}
	}

	id, err := s.ex.cli.ContainerCreate(ctx, spec)
	if err != nil {
		return 0, fmt.Errorf("containerexec: %w: %w", senroexec.ErrInfra, err)
	}
	s.mu.Lock()
	s.id = id
	s.mu.Unlock()

	// The same background context Run uses: reaping a killed container must
	// not be cancelled by the cancellation that killed it.
	bg := context.WithoutCancel(ctx)

	stream, err := s.ex.cli.ContainerAttach(ctx, id)
	if err != nil {
		return 0, fmt.Errorf("containerexec: %w: %w", senroexec.ErrInfra, err)
	}
	defer func() { _ = stream.Close() }()

	if err := s.ex.cli.ContainerStart(ctx, id); err != nil {
		return 0, fmt.Errorf("containerexec: %w: %w", senroexec.ErrInfra, err)
	}

	// Again after start, and errors ignored for the reason the resize loop
	// below gives. Redundant against the create-time size above on a daemon
	// that honours it, and the whole of the sizing on one that does not.
	if initial.Cols > 0 && initial.Rows > 0 {
		_ = s.ex.cli.ContainerResize(ctx, id, initial.Cols, initial.Rows)
	}

	sessionOver := make(chan struct{})
	defer close(sessionOver)
	go func() {
		select {
		case <-ctx.Done():
			_ = s.ex.cli.ContainerKill(bg, id)
			_ = stream.Close()
		case <-sessionOver:
		}
	}()
	go pumpResize(ctx, sessionOver, s, id, resize)

	go func() {
		// Errors discarded for RunInteractive's reasons.
		_, _ = io.Copy(stream, stdin)
		// The VEOF byte first (a terminal has no EOF; end of input is ^D,
		// see localexec), then the half-close, which with StdinOnce the
		// daemon uses to end the attach.
		_, _ = stream.Write([]byte{veof})
		_ = stream.CloseWrite()
	}()

	// Raw, NOT Demux: see this function's doc. Blocks until the container
	// exits or the watchdog closes the stream.
	_, copyErr := io.Copy(out, stream)

	code, waitErr := s.ex.cli.ContainerWait(bg, id)
	switch {
	case ctx.Err() != nil:
		return code, fmt.Errorf("containerexec: %w: %w", senroexec.ErrInfra, ctx.Err())
	case waitErr != nil:
		return code, fmt.Errorf("containerexec: %w: %w", senroexec.ErrInfra, waitErr)
	case copyErr != nil:
		return code, fmt.Errorf("containerexec: %w: %w", senroexec.ErrInfra, copyErr)
	default:
		return code, nil
	}
}

// veof is ^D. See localexec's constant of the same name: the two executors
// deliver end-of-input identically.
const veof = 0x04

// pumpResize forwards every window size to the daemon until the session
// ends. Failures are dropped, as localexec's equivalent drops them: the size
// is advisory, and ending a session over a cosmetic problem would be worse.
func pumpResize(
	ctx context.Context, done <-chan struct{}, s *sandbox, id string,
	resize <-chan senroexec.WinSize,
) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-done:
			return
		case ws, ok := <-resize:
			if !ok {
				return
			}
			if ws.Cols > 0 && ws.Rows > 0 {
				_ = s.ex.cli.ContainerResize(ctx, id, ws.Cols, ws.Rows)
			}
		}
	}
}
