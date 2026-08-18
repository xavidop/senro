package remotecache

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// archiveWorkers is how many uploads run at once. More than one because a
// wide plan settles several steps at the same moment; few, because these
// are background copies of what is already on local disk, and saturating
// the uplink would slow the fetches the build IS waiting for.
const archiveWorkers = 4

// archiveQueue bounds how many uploads may be waiting. A bound because the
// alternative to dropping is blocking, which would put the object store
// back inside a step's completion path. Overrunning it loses the oldest
// logs from the ARCHIVE, never from local disk, and says so.
const archiveQueue = 256

// Archiver uploads a run's logs in the background. The rule it keeps: a
// step's execution never waits on an upload. Enqueuing is a non-blocking
// send; everything after happens on this type's own goroutines, and every
// failure there degrades rather than propagating.
//
// One Archiver serves one run. Close drains it with a bounded grace: "a
// finished run has uploaded its logs" without "a run cannot finish until
// its uploads do".
type Archiver struct {
	logs  *RunLogs
	runID string
	deg   *degrader

	jobs chan archiveJob
	wg   sync.WaitGroup

	// ctx is cancelled when the grace runs out, making an in-flight upload
	// interruptible. Rooted at Background, not the run's context: a
	// cancelled run is exactly the one whose logs somebody wants, and an
	// inherited cancellation would skip the upload.
	ctx    context.Context
	cancel context.CancelFunc

	closeOnce sync.Once
	dropped   atomic.Int64
	uploaded  atomic.Int64
}

type archiveJob struct {
	key  string
	path string
}

// DefaultArchiveGrace is how long Close waits for the queue to drain.
// Generous, because losing logs at the last moment defeats archiving;
// finite, because a CI job that will not exit is worse.
const DefaultArchiveGrace = 60 * time.Second

// Archive starts an archiver for one run. It returns nil when there is no
// remote, so a caller writes no branch: every method below tolerates a nil
// receiver.
func (r *Remote) Archive(runID string) *Archiver {
	if r == nil {
		return nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	a := &Archiver{
		logs:   r.runLogs,
		runID:  runID,
		deg:    r.deg,
		jobs:   make(chan archiveJob, archiveQueue),
		ctx:    ctx,
		cancel: cancel,
	}
	a.wg.Add(archiveWorkers)
	for range archiveWorkers {
		go a.work()
	}
	return a
}

// Stream queues one completed log stream for upload. Call it once the
// attempt's writers have closed: uploading a file still being written
// would archive a prefix of it, and the archive is the only copy that
// survives the runner.
func (a *Archiver) Stream(step string, attempt int, stream, path string) {
	if a == nil {
		return
	}
	a.enqueue(archiveJob{key: a.logs.StreamKey(a.runID, step, attempt, stream), path: path})
}

// Ledger queues the run's event ledger for upload. Without it the archived
// logs are unreadable in practice: a store full of anonymous log files is
// not a record of a run.
func (a *Archiver) Ledger(path string) {
	if a == nil {
		return
	}
	a.enqueue(archiveJob{key: a.logs.LedgerKey(a.runID), path: path})
}

// enqueue is a non-blocking send. See archiveQueue.
func (a *Archiver) enqueue(j archiveJob) {
	if !a.deg.live() {
		return
	}
	select {
	case a.jobs <- j:
	default:
		// Report the first drop and count the rest: the point is to say it
		// is happening, not to say it several hundred times.
		if a.dropped.Add(1) == 1 {
			a.deg.notice("archive", errQueueFull)
		}
	}
}

var errQueueFull = errQueue{}

type errQueue struct{}

func (errQueue) Error() string {
	return "remote cache: the log archive queue is full, so some of this run's logs are on local " +
		"disk only; the run is unaffected"
}

func (a *Archiver) work() {
	defer a.wg.Done()
	for j := range a.jobs {
		if !a.deg.live() {
			continue // drain without trying: the store is already known to be down
		}
		if err := a.logs.PutFile(a.ctx, j.key, j.path); err != nil {
			a.deg.classify("archive", err)
			continue
		}
		a.uploaded.Add(1)
	}
}

// Close stops accepting work and waits, up to grace, for what is queued.
//
// Zero or negative grace means DefaultArchiveGrace. It is idempotent, and
// safe on a nil Archiver.
func (a *Archiver) Close(grace time.Duration) {
	if a == nil {
		return
	}
	if grace <= 0 {
		grace = DefaultArchiveGrace
	}
	a.closeOnce.Do(func() {
		close(a.jobs)
		done := make(chan struct{})
		go func() {
			a.wg.Wait()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(grace):
			// Out of time: cancelling makes in-flight requests return so
			// the run can exit.
			a.cancel()
			<-done
			a.deg.notice("archive", errGraceExpired)
		}
		a.cancel()
	})
}

var errGraceExpired = errGrace{}

type errGrace struct{}

func (errGrace) Error() string {
	return "remote cache: the log archive did not finish within its grace period, so some of " +
		"this run's logs are on local disk only; the run is unaffected"
}

// Uploaded and Dropped report what happened, for a test and for anything that
// wants to say so at the end of a run.
func (a *Archiver) Uploaded() int64 {
	if a == nil {
		return 0
	}
	return a.uploaded.Load()
}

func (a *Archiver) Dropped() int64 {
	if a == nil {
		return 0
	}
	return a.dropped.Load()
}
