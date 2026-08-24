package k8sexec

import (
	"bytes"
	"context"
	"io"

	senroexec "github.com/xavidop/senro/internal/executor"
	"github.com/xavidop/senro/internal/kubeapi"
)

// This executor hosts a terminal as well as a shell. See
// internal/executor.Terminal for why the two are separate capabilities, and
// interactive.go for why a session is a pod of its own.
var _ senroexec.Terminal = (*sandbox)(nil)

// RunTerminal runs one command on a pseudo-terminal the CONTAINER RUNTIME
// allocates.
//
// Three differences from RunInteractive, each a consequence of a terminal
// being one device rather than two streams:
//
//   - The exec asks for a tty and does not ask for stderr: the runtime
//     merges it into the pty, so out is the one writer.
//   - Sizes travel on the exec's own resize channel, in order against the
//     input they belong to.
//   - End of input is the VEOF byte rather than a closed stream, because a
//     terminal has no EOF.
//
// The pod's container is created without a tty of its own and needs none:
// an exec allocates one, which is why `kubectl exec -it` works against any
// pod.
//
// The size, though, is a real limitation and not a detail. The exec
// subresource takes no initial size: the kubelet allocates the pty and
// starts the command, and the first size reaches it as a frame afterwards.
// kubeapi.Exec sends that frame synchronously, before the read loop, which
// is as early as this API permits — and on a loaded machine the command
// still usually reads the device before it arrives, so `stty size` in a
// fresh session reports nothing useful and a full-screen program draws at
// whatever the runtime's default was. Every later size is exact, and the
// program redraws on SIGWINCH.
//
// containerexec does NOT pay this cost: Docker's HostConfig.ConsoleSize
// sizes the pty at create, before anything runs (see its RunTerminal). The
// asymmetry is the platform's, not senro's, and internal/executor's
// conformance suite asserts each side of it rather than pretending they
// match.
func (s *sandbox) RunTerminal(
	ctx context.Context, c senroexec.Cmd, stdin io.Reader, out io.Writer,
	initial senroexec.WinSize, resize <-chan senroexec.WinSize,
) (int, error) {
	// Buffered by one and seeded with the initial size, so the terminal is
	// sized from the first frame the far side reads rather than after a
	// round trip through the pump below.
	sizes := make(chan kubeapi.TermSize, 1)
	if initial.Cols > 0 && initial.Rows > 0 {
		sizes <- kubeapi.TermSize{Width: initial.Cols, Height: initial.Rows}
	}
	sessionOver := make(chan struct{})
	defer close(sessionOver)
	go pumpResize(ctx, sessionOver, sizes, resize)

	return s.exec(ctx, c, kubeapi.ExecSpec{
		TTY: true, Resize: sizes,
		// The VEOF byte after the client's own input, then the stream ends
		// and kubeapi closes the far side's stdin. A terminal answers ^D by
		// letting the shell exit; a closed descriptor it would never see.
		Stdin: io.MultiReader(stdin, bytes.NewReader([]byte{veof})), Stdout: out,
	})
}

// veof is ^D. See localexec's and containerexec's constants of the same
// name: all three executors deliver end of input to a terminal identically.
const veof = 0x04

// pumpResize forwards the caller's window sizes onto the channel the exec
// reads, translating between the two representations, and closes that
// channel when the session ends.
//
// It owns the closing because it is the only writer: the caller's channel is
// fed by whoever holds the operator's terminal and may outlive this session
// or never close at all, which is what done and the context are for. A zero
// dimension is dropped rather than sent, as internal/shellwire refuses to
// send one: a pty told it is 0 columns wide draws nothing.
func pumpResize(
	ctx context.Context, done <-chan struct{},
	out chan<- kubeapi.TermSize, in <-chan senroexec.WinSize,
) {
	defer close(out)
	for {
		select {
		case <-ctx.Done():
			return
		case <-done:
			return
		case ws, ok := <-in:
			// A nil in channel means the size never changes; this case then
			// never fires and the two above end the pump.
			if !ok {
				return
			}
			if ws.Cols == 0 || ws.Rows == 0 {
				continue
			}
			select {
			case out <- kubeapi.TermSize{Width: ws.Cols, Height: ws.Rows}:
			case <-ctx.Done():
				return
			case <-done:
				return
			}
		}
	}
}
