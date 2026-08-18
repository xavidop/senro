package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/internal/render"
	"github.com/xavidop/senro/internal/source"
	"github.com/xavidop/senro/internal/tui"
)

// watch renders src per mode until the run reaches a terminal state, the
// user detaches (tui's 'q'), or ctx is cancelled, and returns the status
// folded so far (empty if run.finished never arrived). Every mode is a
// Source client and nothing else, so TTY and non-TTY runs, and live versus
// offline attach, all report identical facts.
func watch(ctx context.Context, src source.Source, mode uiMode, stdout io.Writer) (api.RunStatus, error) {
	switch mode {
	case uiNone:
		return watchNone(ctx, src)
	case uiPlain:
		return render.Plain(ctx, src, stdout)
	case uiTUI:
		return tui.Run(ctx, src)
	default:
		return "", fmt.Errorf("senro: internal error: unresolved ui mode %q", mode)
	}
}

// watchNone folds the run's events silently and returns the final status:
// render.Plain's own fold with the printing removed. The Subscribe is
// deliberately NOT short-circuited when st.Run.Done is already true, since
// render.Plain does not short-circuit either and diverging would make
// --ui=none and --ui=plain disagree about a just-finished live run.
func watchNone(ctx context.Context, src source.Source) (api.RunStatus, error) {
	st, err := src.State(ctx)
	if err != nil {
		return "", fmt.Errorf("watch: %w", err)
	}
	ch, err := src.Subscribe(ctx, st.Seq+1)
	if err != nil {
		return "", fmt.Errorf("watch: %w", err)
	}
	for e := range ch {
		if err := st.Apply(e); err != nil {
			return "", fmt.Errorf("watch: %w", err)
		}
	}
	return st.Run.Status, nil
}

// newInterruptibleContext derives a cancellable context that also cancels,
// and marks interrupted, the moment a value arrives on sig. It takes an
// explicit channel rather than wiring os/signal inside, so a test can
// simulate Ctrl-C without raising a real signal against the test process;
// attachSignalContext is the production wiring.
//
// interrupted is set ONLY on the signal branch, never merely because ctx
// became Done. Built on signal.NotifyContext instead, whose stop() also
// marks the context Done, "was this an interrupt" would be
// indistinguishable from "did the caller run its cleanup".
func newInterruptibleContext(parent context.Context, sig <-chan os.Signal) (context.Context, context.CancelFunc, *atomic.Bool) {
	ctx, cancel := context.WithCancel(parent)
	var interrupted atomic.Bool
	go func() {
		select {
		case <-sig:
			interrupted.Store(true)
			cancel()
		case <-ctx.Done():
		}
	}()
	return ctx, cancel, &interrupted
}

// attachSignalContext is newInterruptibleContext wired to real SIGINT and
// SIGTERM: Ctrl-C cancels. The returned CancelFunc both cancels the
// context and stops relaying further signals to it (signal.Stop); callers
// defer it exactly once.
func attachSignalContext(parent context.Context) (context.Context, context.CancelFunc, *atomic.Bool) {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	ctx, cancel, interrupted := newInterruptibleContext(parent, sigCh)
	return ctx, func() {
		cancel()
		signal.Stop(sigCh)
	}, interrupted
}

// bestEffortCancelTimeout bounds how long a SIGINT-triggered run.cancel may
// wait for the engine to accept it. Short: the process is already on its
// way out, so a wedged connection must not make the operator wait.
const bestEffortCancelTimeout = 2 * time.Second

// bestEffortCancel issues run.cancel against src and discards the result.
// "Ctrl-C cancels" applies to every ui mode: the TUI's raw mode already
// sends this op as a keystroke (internal/tui/model.go), so this path is for
// --ui=plain, --ui=none, and an externally delivered SIGTERM the TUI would
// never see as a keypress. A read-only or offline Source answers
// ErrReadOnly or ErrClosed, expected and ignored: there was no engine to
// cancel.
func bestEffortCancel(src source.Source) {
	ctx, cancel := context.WithTimeout(context.Background(), bestEffortCancelTimeout)
	defer cancel()
	_, _ = src.Control(ctx, api.Frame{
		V: api.Version, Kind: api.KindReq, ID: "sigint-cancel", Type: api.OpRunCancel,
	})
}
