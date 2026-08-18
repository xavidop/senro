// Package source defines the seam between a client (the TUI, the WASM
// browser UI, a scripted debugger) and wherever a run's events actually
// live: an attach server watching a live engine, or a directory left behind
// by one that already finished.
package source

import (
	"context"
	"errors"
	"io"

	"github.com/xavidop/senro/api"
)

// ErrReadOnly is returned by Control when a Source can only observe a run,
// not act on it. FileSource returns it for every op: a run on disk has no
// engine behind it to carry out a cancel or a retry.
var ErrReadOnly = errors.New("source: control is not supported by this source")

// Source is everything a client needs to render a run and, where the
// underlying run is still live, to act on it.
//
// Control stays on the interface even though FileSource can never honour it:
// one client code path renders live and finished runs alike, and only the
// result of an attempted action differs. A capability query would put a
// "which kind of Source" branch in every client.
type Source interface {
	// State folds the run's whole event history and returns the resulting
	// RunState together with the sequence number it was folded at. To keep
	// watching, Subscribe(state.Seq+1): a snapshot plus a tail rather than
	// a full replay.
	State(ctx context.Context) (*api.RunState, error)

	// Subscribe streams events with Seq >= fromSeq, in order, on the returned
	// channel. fromSeq == 0 replays the whole run; a caller resuming from a
	// snapshot passes snapshot.Seq+1. The channel is closed when delivery is
	// done: after a plain replay drains, or, for a live run, when ctx is
	// cancelled or the Source is closed.
	Subscribe(ctx context.Context, fromSeq uint64) (<-chan api.Event, error)

	// Logs opens one step attempt's log stream starting at byte offset from.
	// The caller owns the returned ReadCloser and must Close it.
	Logs(ctx context.Context, step string, attempt int, stream string, from int64) (io.ReadCloser, error)

	// Control issues a control-plane request (cancel, retry, live log
	// subscribe) and waits for the correlated response. A Source with no
	// engine to act on the request returns ErrReadOnly.
	Control(ctx context.Context, req api.Frame) (api.Frame, error)

	// Close releases the Source's resources. Idempotent: repeat calls
	// return nil.
	Close() error
}
