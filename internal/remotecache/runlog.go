package remotecache

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"

	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/internal/cas"
	"github.com/xavidop/senro/internal/eventlog"
	"github.com/xavidop/senro/internal/stepid"
)

// RunLogs is a run's archived record in the shared store: its event ledger
// and the output of every step attempt.
//
// Archival rather than live, deliberately: neither backend has append, so
// "live logs in S3" delivers no actual live reading (the attach server
// already serves that from the local file) while putting a network round
// trip, and so a shared-store outage, inside every log line's write path.
// Uploading each stream once it completes fully covers the case that
// motivates archiving: a CI runner destroyed when the job ends, including
// a run that crashed halfway.
//
// The bytes go into the same content-addressed store the cache uses, and a
// small mutable pointer names the digest. In a bucket:
//
//	<prefix>cas/sha256/<aa>/<bb>/<hex>                        the bytes
//	<prefix>runs/<run>/logs/<step>/<attempt>/<stream>         the digest
//	<prefix>runs/<run>/events                                 the ledger's digest
//
// and in a registry, where every name is a tag and a tag has an alphabet:
//
//	senro-v1-sha256-<hex>                                     the bytes
//	senro-v1-log-sha256-<hex>                                 the digest
//	senro-v1-run-sha256-<hex>                                 the ledger's digest
//
// Content addressing makes the read path safe (a fetched log is verified
// by the same code that verifies a cached object), stores identical logs
// once, and lets two uploaders never conflict. The cost: a stream is
// fetched whole, not by byte range, the right trade for files of
// kilobytes to a few megabytes.
type RunLogs struct {
	objects  RemoteObjects
	docs     docs
	readOnly bool
	deg      *degrader
}

// StreamKey is the name of the pointer naming one stream of one attempt.
// In a bucket the step id is percent-encoded into a single path segment
// (ids contain slashes and brackets); in a registry the four parts are
// hashed into one tag, which holds neither those characters nor that
// length. See ociDocs.
func (r *RunLogs) StreamKey(runID, step string, attempt int, stream string) string {
	return r.docs.streamName(runID, step, attempt, stream)
}

// LedgerKey is the name of the pointer naming a run's event ledger.
func (r *RunLogs) LedgerKey(runID string) string { return r.docs.ledgerName(runID) }

// PutFile uploads path's bytes and writes the pointer at key: bytes first,
// pointer second (the action cache's order, for the same reason: a pointer
// without content is a promise nothing can keep). A file that does not
// exist is not an error: a step that wrote nothing to stderr has no stderr
// file.
func (r *RunLogs) PutFile(ctx context.Context, key, path string) error {
	f, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("remote cache: %w", err)
	}
	defer func() { _ = f.Close() }()

	d, err := r.objects.Put(ctx, f)
	if err != nil {
		return err
	}
	if r.readOnly {
		return nil
	}
	return r.docs.put(ctx, key, []byte(d))
}

// Get returns the archived bytes the pointer at key names, verified. A
// missing pointer, or one naming an object no longer there, is
// cas.ErrNotFound: a log expired by a lifecycle rule is absent, not
// broken.
func (r *RunLogs) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	raw, err := r.docs.get(ctx, key, maxPointerBytes)
	if err != nil {
		return nil, err
	}

	d := cas.Digest(raw)
	if !d.Valid() {
		err := fmt.Errorf("remote cache: %w: the pointer at %s is not a digest", cas.ErrCorrupt, key)
		r.deg.notice("get", err)
		return nil, err
	}
	return r.objects.Get(ctx, d)
}

// Fetch materializes an archived run back into dir, in the layout the run
// wrote it in: senro's readers of a finished run are all built on a
// directory of files, so restoring the directory makes every one of them
// work on an archived run with no second implementation. Every byte is
// verified against its digest on the way in.
//
// The stream set comes from the ledger, never a bucket listing (see
// StreamsFromLedger); a caller that must read the ledger first to decide
// uses FetchLedger and FetchStreams directly.
func (r *RunLogs) Fetch(ctx context.Context, runID, dir string, streams []StreamRef) error {
	if err := r.FetchLedger(ctx, runID, dir); err != nil {
		return err
	}
	_, err := r.FetchStreams(ctx, runID, dir, streams)
	return err
}

// FetchLedger materializes a run's event ledger, and nothing else, into
// dir: Fetch's first half, exported because the ledger is what NAMES the
// streams, so "fetch the ledger, decide, fetch the rest" is the only
// possible order, and going through Fetch would download the ledger twice.
func (r *RunLogs) FetchLedger(ctx context.Context, runID, dir string) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("remote cache: %w", err)
	}
	return r.fetchTo(ctx, r.LedgerKey(runID), filepath.Join(dir, "events.jsonl"))
}

// FetchStreams materializes the named streams into dir and reports the
// ones the archive does not hold. A missing stream is not an error (an
// unfinished upload, an expired object; the rest of the run is still worth
// having), but it is returned rather than silently skipped: the caller is
// the one who can say the log somebody came for is the absent one.
func (r *RunLogs) FetchStreams(
	ctx context.Context, runID, dir string, streams []StreamRef,
) (missing []StreamRef, err error) {
	for _, s := range streams {
		key := r.StreamKey(runID, s.Step, s.Attempt, s.Stream)
		dest := filepath.Join(dir, "logs", stepid.Encode(s.Step), strconv.Itoa(s.Attempt), s.Stream)
		if err := r.fetchTo(ctx, key, dest); err != nil {
			if errors.Is(err, cas.ErrNotFound) {
				missing = append(missing, s)
				continue
			}
			return missing, err
		}
	}
	return missing, nil
}

// StreamsFromLedger returns every archived stream the ledger at path
// names: the streams come from the run's own record, never a bucket
// listing, so reading an archive needs no permission beyond GetObject.
//
// Two kinds of event name a stream, and both are needed. step.log.appended
// names one that actually produced output (live, retried, or replayed from
// a cache hit). A handler emits NO markers, so handler.started is the only
// thing that can name its output; both streams are claimed for it since
// the ledger does not say which one it wrote, and the silent one simply
// comes back from FetchStreams as missing, at the cost of one refused GET.
//
// A torn final line is tolerated: the killed run is exactly what this
// feature exists for.
func StreamsFromLedger(path string) ([]StreamRef, error) {
	events, err := eventlog.Read(path)
	if err != nil && !errors.Is(err, eventlog.ErrTruncated) {
		return nil, fmt.Errorf("remote cache: reading %s: %w", path, err)
	}
	seen := make(map[StreamRef]struct{})
	for _, e := range events {
		// Attempt numbering starts at 1 wherever a log file is written; an
		// event without one is not step-scoped and names no file.
		if e.Attempt < 1 {
			continue
		}
		switch e.Type {
		case api.StepLogAppended:
			var b api.StepLogAppendedBody
			if err := e.Decode(&b); err != nil || b.Stream == "" {
				continue
			}
			seen[StreamRef{Step: e.Step, Attempt: e.Attempt, Stream: b.Stream}] = struct{}{}
		case api.HandlerStarted:
			for _, stream := range []string{api.StreamStdout, api.StreamStderr} {
				seen[StreamRef{Step: e.Step, Attempt: e.Attempt, Stream: stream}] = struct{}{}
			}
		}
	}

	out := make([]StreamRef, 0, len(seen))
	for s := range seen {
		out = append(out, s)
	}
	// Sorted: fetching the same run twice should do the same thing in the
	// same order.
	sort.Slice(out, func(i, j int) bool {
		if out[i].Step != out[j].Step {
			return out[i].Step < out[j].Step
		}
		if out[i].Attempt != out[j].Attempt {
			return out[i].Attempt < out[j].Attempt
		}
		return out[i].Stream < out[j].Stream
	})
	return out, nil
}

// StreamRef names one archived log stream.
type StreamRef struct {
	Step    string
	Attempt int
	Stream  string
}

// fetchTo writes one archived object to a path, through a temp file and a
// rename so a reader never sees a partial file and an interrupted Fetch
// leaves nothing that looks complete.
func (r *RunLogs) fetchTo(ctx context.Context, key, dest string) error {
	body, err := r.Get(ctx, key)
	if err != nil {
		return err
	}
	defer func() { _ = body.Close() }()

	if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
		return fmt.Errorf("remote cache: %w", err)
	}
	tmp, err := os.CreateTemp(filepath.Dir(dest), ".fetch-")
	if err != nil {
		return fmt.Errorf("remote cache: %w", err)
	}
	defer func() {
		_ = tmp.Close()
		_ = os.Remove(tmp.Name())
	}()

	// The copy is what verifies: Get's reader fails at EOF on a digest
	// mismatch, so a lying body never reaches the rename below.
	if _, err := io.Copy(tmp, body); err != nil {
		return err
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("remote cache: %w", err)
	}
	if err := os.Rename(tmp.Name(), dest); err != nil {
		return fmt.Errorf("remote cache: %w", err)
	}
	return nil
}
