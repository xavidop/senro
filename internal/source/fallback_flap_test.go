package source

import (
	"context"
	"io"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xavidop/senro/api"
)

// productiveFlapper delivers exactly one event per connection and then ends
// the stream markerless, forever. Every reconnect makes progress, so the
// no-progress budget never trips: the real test of two properties, that
// relay is genuinely iterative and that reconnection is rate-limited even
// when every attempt is productive.
type productiveFlapper struct {
	calls atomic.Int64
	seq   atomic.Uint64
}

func (p *productiveFlapper) SubscribeStream(ctx context.Context, fromSeq uint64) (<-chan api.Event, <-chan StreamEnd, error) {
	p.calls.Add(1)
	ev := make(chan api.Event, 1)
	end := make(chan StreamEnd)
	ev <- api.Event{Seq: p.seq.Add(1), Type: "step.started"}
	close(ev)
	close(end) // markerless
	return ev, end, nil
}

func (p *productiveFlapper) Subscribe(ctx context.Context, fromSeq uint64) (<-chan api.Event, error) {
	ev, _, err := p.SubscribeStream(ctx, fromSeq)
	return ev, err
}
func (p *productiveFlapper) State(ctx context.Context) (*api.RunState, error) {
	return &api.RunState{}, nil
}
func (p *productiveFlapper) Logs(ctx context.Context, step string, attempt int, stream string, from int64) (io.ReadCloser, error) {
	return nil, nil
}
func (p *productiveFlapper) Control(ctx context.Context, req api.Frame) (api.Frame, error) {
	return api.Frame{}, nil
}
func (p *productiveFlapper) Close() error { return nil }

// TestProductiveReconnectKeepsStackFlat asserts: productive reconnection
// is not clipped by the no-progress bound; the stack stays flat regardless
// of reconnect count; and reconnects are rate-limited, which iteration
// alone does not give. The 100-reconnect ceiling is generous against the
// ~30 a 100ms floor implies for the 3s window, yet orders of magnitude
// below an unbounded implementation's millions per second.
func TestProductiveReconnectKeepsStackFlat(t *testing.T) {
	live := &productiveFlapper{}
	fs := Fallback(live, t.TempDir())
	defer func() { _ = fs.Close() }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch, err := fs.Subscribe(ctx, 0)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	var got atomic.Int64
	done := make(chan struct{})
	go func() {
		for range ch {
			got.Add(1)
		}
		close(done)
	}()

	sample := func() uint64 {
		var ms runtime.MemStats
		runtime.ReadMemStats(&ms)
		return ms.StackInuse
	}

	time.Sleep(500 * time.Millisecond)
	early := sample()
	earlyCalls := live.calls.Load()

	time.Sleep(3 * time.Second)
	late := sample()
	lateCalls := live.calls.Load()

	t.Logf("reconnects: %d -> %d; stack in use: %d KiB -> %d KiB; events delivered: %d",
		earlyCalls, lateCalls, early/1024, late/1024, got.Load())

	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Error("relay goroutine did not exit after cancel")
	}

	if lateCalls <= earlyCalls {
		t.Fatalf("productive reconnection stopped: %d -> %d (the bound clipped a working session)", earlyCalls, lateCalls)
	}
	// Iterative relay holds stack flat no matter how many reconnects happen.
	if late > early*4 {
		t.Fatalf("stack grew with reconnect count: %d KiB -> %d KiB over %d reconnects (recursion survives)",
			early/1024, late/1024, lateCalls-earlyCalls)
	}
	// See the doc comment for the ceiling's rationale.
	const ceiling = 100
	if got := lateCalls - earlyCalls; got > ceiling {
		t.Fatalf("reconnected %d times in 3s (>%d) — productive reconnection has no rate limit, "+
			"just like the unbounded stack did before round 3's fix; a remote peer that ends every "+
			"stream after one event can still induce a request storm", got, ceiling)
	}
}
