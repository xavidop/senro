package remotecache

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"

	"github.com/xavidop/senro/internal/cas"
	"github.com/xavidop/senro/internal/s3"
	"github.com/xavidop/senro/internal/stepid"
)

// docs is the small, MUTABLE half of a shared cache.
//
// Everything else in this package is content-addressed. Three things are
// not: an action-cache entry replaces its predecessor under the same key,
// a step's "most recent" pointer moves every run, and a run's archived
// ledger and log streams are named by run/step/attempt/stream because a
// reader knows those and not the digest.
//
// All three are bytes under a name where a rewrite replaces. The NAMING is
// each backend's own, the one thing they genuinely disagree about (a
// bucket key takes any string; a registry tag is at most 128 chars of a
// restricted alphabet): see s3Docs and ociDocs. Everything above naming
// ("a down cache never fails a run"; nothing is believed unchecked) is
// enforced once, in the callers (Entries and RunLogs), for both backends.
type docs interface {
	// entryName is where the action-cache entry for one key digest is kept.
	// Returns "" for a malformed digest: digests arrive from event logs,
	// plans and command-line arguments, none trusted.
	entryName(key cas.Digest) string
	// recentName is where a step's most recent key digest is recorded.
	recentName(step string) string
	// streamName is where the pointer naming one archived log stream is kept.
	streamName(runID, step string, attempt int, stream string) string
	// ledgerName is where the pointer naming a run's event ledger is kept.
	ledgerName(runID string) string

	// get returns what is stored under name, reading at most max bytes. A
	// name never written is an error matching cas.ErrNotFound, whatever the
	// backend's own spelling; bytes that are not what the backend recorded
	// are cas.ErrCorrupt, never a value.
	get(ctx context.Context, name string, max int64) ([]byte, error)
	// put stores b under name, replacing whatever was there.
	put(ctx context.Context, name string, b []byte) error
}

// maxEntryBytes bounds how much of a stored action-cache entry is read: an
// entry is a small JSON document, but the bytes come off a network that
// may be serving something else entirely (a proxy's error page, an
// overwritten name), and an unbounded read is how a cache lookup becomes
// an out-of-memory kill.
const maxEntryBytes = 1 << 20

// maxPointerBytes bounds a run-log pointer, which is one digest: 71 bytes. Same
// reasoning as maxEntryBytes, with far less room needed.
const maxPointerBytes = 512

// s3Docs keeps each document at its own key in the bucket: an object
// store's key space takes any string, so the name IS the key, and the only
// encoding needed is the percent-encoding that keeps a step id inside one
// path segment.
//
//	<prefix>action/entries/<aa>/<hex>                      an entry
//	<prefix>action/recent/<encoded step id>                a step's latest key
//	<prefix>runs/<run>/logs/<step>/<attempt>/<stream>      one stream's digest
//	<prefix>runs/<run>/events                              the ledger's digest
type s3Docs struct {
	client *s3.Client
	// prefix ends in a slash and already carries the layout version.
	prefix string
}

var _ docs = (*s3Docs)(nil)

func (d *s3Docs) entryName(key cas.Digest) string {
	if !key.Valid() {
		return ""
	}
	h := key.Hex()
	return d.prefix + "action/entries/" + h[0:2] + "/" + h
}

func (d *s3Docs) recentName(step string) string {
	return d.prefix + "action/recent/" + stepid.Encode(step)
}

func (d *s3Docs) streamName(runID, step string, attempt int, stream string) string {
	return d.prefix + "runs/" + stepid.Encode(runID) + "/logs/" +
		stepid.Encode(step) + "/" + strconv.Itoa(attempt) + "/" + stepid.Encode(stream)
}

func (d *s3Docs) ledgerName(runID string) string {
	return d.prefix + "runs/" + stepid.Encode(runID) + "/events"
}

func (d *s3Docs) get(ctx context.Context, name string, max int64) ([]byte, error) {
	if name == "" {
		return nil, fmt.Errorf("remote cache: %w: no document was named", cas.ErrNotFound)
	}
	body, err := d.client.Get(ctx, name)
	if err != nil {
		if errors.Is(err, s3.ErrNotFound) {
			return nil, fmt.Errorf("remote cache: %w: %s", cas.ErrNotFound, name)
		}
		return nil, err
	}
	defer func() { _ = body.Close() }()
	return readBounded(body, name, max)
}

func (d *s3Docs) put(ctx context.Context, name string, b []byte) error {
	if name == "" {
		return errors.New("remote cache: refusing to store a document with no name")
	}
	return d.client.PutBytes(ctx, name, b)
}

// readBounded reads at most max bytes and refuses a body that holds more:
// one byte past the limit, so an over-long body is detected rather than
// silently truncated into something that might still parse.
func readBounded(r io.Reader, name string, max int64) ([]byte, error) {
	b, err := io.ReadAll(io.LimitReader(r, max+1))
	if err != nil {
		return nil, fmt.Errorf("remote cache: reading %s: %w", name, err)
	}
	if int64(len(b)) > max {
		return nil, fmt.Errorf("remote cache: %w: what is stored at %s is larger than it can be",
			cas.ErrCorrupt, name)
	}
	return b, nil
}
