// Package remotecache puts a shared, remote cache behind senro's local
// one, so two machines can reuse each other's work: a fresh CI runner
// starts warm instead of empty. The store is an S3-compatible bucket
// (Open) or an OCI registry repository (OpenOCI); both produce the same
// *Remote and differ only in naming, confined to RemoteObjects and docs.
//
// The remote is a second tier behind the local one, never a replacement:
// reads try disk first, a remote hit is written through to disk, writes go
// to disk first. Both the CAS and the action cache are tiered; either
// alone is useless.
//
// Two rules are not negotiable. Nothing is served without verifying it:
// every object goes through cas.DecodeVerify and every entry is checked
// against its key, because a store returning wrong bytes is ordinary and
// serving them would silently poison every downstream build. And a cache
// that is down never fails a run: unreachable, unauthenticated, slow or
// erroring all mean "no remote cache" and nothing more; the run says so
// loudly once, then stops trying.
package remotecache

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/xavidop/senro/internal/cache"
	"github.com/xavidop/senro/internal/cas"
	"github.com/xavidop/senro/internal/oci"
	"github.com/xavidop/senro/internal/s3"
)

// layout is the version segment every key carries, so a future layout
// change is a clean namespace break rather than a fleet of machines
// misreading each other's objects.
const layout = "v1"

// DefaultPrefix is where senro's objects live in a bucket that may hold other
// things. Named rather than empty so that pointing senro at a shared bucket
// does not scatter opaque hex directories across its root.
const DefaultPrefix = "senro"

// Config is everything needed to open one remote cache.
type Config struct {
	// Endpoint is the object store's URL, such as
	// "https://s3.eu-west-1.amazonaws.com" or "http://minio.internal:9000".
	Endpoint string
	// Region scopes the request signature. Required, because it is signed
	// over, and a store that does not care about regions still has to be told
	// which one to expect.
	Region string
	Bucket string
	// Prefix is the key prefix inside the bucket. Empty means DefaultPrefix.
	Prefix string

	AccessKeyID     string
	SecretAccessKey string
	// SessionToken accompanies temporary credentials, which is what an
	// OIDC-assumed role in CI produces.
	SessionToken string

	// PathStyle selects bucket-in-path over bucket-in-host addressing. Nil
	// means "decide from the endpoint": Amazon expects bucket-in-host, and
	// essentially every other implementation needs bucket-in-path.
	PathStyle *bool

	// Timeout bounds one request. Zero means s3.DefaultTimeout.
	Timeout time.Duration

	// ReadOnly reads the shared cache and never writes to it: what a fork's
	// pull-request build should use, gaining the trunk-filled cache without
	// being able to put anything into a cache others trust.
	ReadOnly bool

	// Scratch shares scratch caches through this remote as well. Off by
	// default and ignored by the registry backend; see EnvScratch for the
	// three reasons it is not simply part of turning the cache on.
	Scratch bool

	// Report receives every degradation. Optional: the report also goes to
	// ReportWriter regardless, so a caller that wires nothing still finds out.
	Report func(Degradation)

	// ReportWriter is where the human-readable degradation line goes. Zero
	// means os.Stderr: a run with no attached client and no configured sink
	// must still be able to say its cache went away.
	ReportWriter io.Writer

	// Transport is the HTTP round tripper requests go through. Zero means
	// the client's own. Exists for tests, for failures a real store will
	// not produce on demand (an upload refused while reads still work); it
	// wraps the suite's real store rather than replacing it.
	Transport http.RoundTripper
}

// Degradation is one report that the remote cache did not do its job.
type Degradation struct {
	// Store names the remote without naming its credentials, e.g.
	// "s3 bucket team-cache at s3.eu-west-1.amazonaws.com".
	Store string
	// Op is what was being attempted: "get", "put", "head", "lookup", "save".
	Op string
	// Err is why it failed, with any credential scrubbed out of it.
	Err error
	// Disabled reports whether the remote was switched off for the rest of
	// this run. False means one object was bad and the store is still in
	// use (one corrupt object says nothing about the rest); true means the
	// store itself stopped answering.
	Disabled bool
}

func (d Degradation) String() string {
	verdict := "not used for this object"
	if d.Disabled {
		verdict = "not used for the rest of this run"
	}
	return fmt.Sprintf("senro: remote cache %s failed on %s and is %s: %v",
		d.Store, d.Op, verdict, d.Err)
}

// Remote is an opened shared cache, backed by a bucket or by a registry.
// One type for both: everything a run does with a shared cache is the same
// either way, and what differs is confined to RemoteObjects and docs. A
// second Remote type would mean a second engine branch and a second place
// for "a down cache never fails a run" to be got wrong.
type Remote struct {
	// name is what a degraded run prints. Safe to put in front of anyone: it
	// names the store and never its credentials.
	name    string
	objects RemoteObjects
	entries *Entries
	runLogs *RunLogs
	deg     *degrader
	// scratch is nil unless sharing scratch caches was asked for AND the
	// backend is a bucket. Nil is the whole switch: TierScratch hands back
	// the local cache untouched, so nothing downstream branches on it. A
	// registry leaves it nil however the variable is set, because the
	// prefix fallback is a listing and OCI cannot list by prefix.
	scratch         *scratchDocs
	scratchReadOnly bool
}

// Open validates the config and prepares the remote. No I/O: reachability
// is discovered on first use and answered by degrading. A configuration
// that cannot possibly work (no bucket, a non-URL endpoint) is an error
// here, at startup: an operator's mistake, not a network condition, and
// reporting it as "your cache is down" would mislead. See OpenOCI for the
// registry form.
func Open(cfg Config) (*Remote, error) {
	pathStyle := false
	if cfg.PathStyle != nil {
		pathStyle = *cfg.PathStyle
	} else {
		var err error
		pathStyle, err = defaultPathStyle(cfg.Endpoint)
		if err != nil {
			return nil, err
		}
	}

	client, err := s3.New(s3.Config{
		Endpoint:        cfg.Endpoint,
		Region:          cfg.Region,
		Bucket:          cfg.Bucket,
		AccessKeyID:     cfg.AccessKeyID,
		SecretAccessKey: cfg.SecretAccessKey,
		SessionToken:    cfg.SessionToken,
		PathStyle:       pathStyle,
		Timeout:         cfg.Timeout,
		Transport:       cfg.Transport,
	})
	if err != nil {
		return nil, fmt.Errorf("remote cache: %w", err)
	}

	w := cfg.ReportWriter
	if w == nil {
		w = os.Stderr
	}
	deg := &degrader{store: client.String(), report: cfg.Report, w: w}

	prefix := strings.Trim(cfg.Prefix, "/")
	if prefix == "" {
		prefix = DefaultPrefix
	}
	prefix += "/" + layout + "/"

	docs := &s3Docs{client: client, prefix: prefix}
	objects := &Objects{client: client, prefix: prefix + "cas/", readOnly: cfg.ReadOnly}
	var sc *scratchDocs
	if cfg.Scratch {
		sc = &scratchDocs{client: client, prefix: prefix + "scratch/"}
	}
	return &Remote{
		name:            client.String(),
		deg:             deg,
		objects:         objects,
		entries:         &Entries{docs: docs, readOnly: cfg.ReadOnly, deg: deg},
		runLogs:         &RunLogs{objects: objects, docs: docs, readOnly: cfg.ReadOnly, deg: deg},
		scratch:         sc,
		scratchReadOnly: cfg.ReadOnly,
	}, nil
}

// SharesScratch reports whether scratch caches travel through this remote,
// which decides whether the scratch backend snapshots into the tiered object
// store or the local one alone. False for a registry however it is
// configured. See Storage.Scratch.
func (r *Remote) SharesScratch() bool { return r != nil && r.scratch != nil }

// String names the remote without naming its credentials.
func (r *Remote) String() string { return r.name }

// Live reports whether the remote is still being used. False once it has
// degraded.
func (r *Remote) Live() bool { return r.deg.live() }

// Objects is the shared content-addressed store, on its own. Callers in the
// engine want TierObjects instead; this is for tests and for a tool that
// deliberately wants to talk only to the shared store.
func (r *Remote) Objects() RemoteObjects { return r.objects }

// Entries is the remote action cache, on its own. See Objects.
func (r *Remote) Entries() *Entries { return r.entries }

// RunLogs is the archive of a run's ledger and step output. See its own doc
// for why archiving is per completed attempt rather than live.
func (r *Remote) RunLogs() *RunLogs { return r.runLogs }

// TierObjects returns the store the engine should read and write objects
// through: local first, remote behind it.
func (r *Remote) TierObjects(local *cas.Dir) *TieredObjects {
	return &TieredObjects{local: local, remote: r.objects, deg: r.deg}
}

// TierEntries returns the action cache the engine should use: local first,
// remote behind it.
func (r *Remote) TierEntries(local cache.ActionCache) *TieredEntries {
	return &TieredEntries{local: local, remote: r.entries, deg: r.deg}
}

// Observe redirects degradation reports to fn, in addition to
// Config.Report and the stderr line, and returns a function that stops
// doing so. Config.Report is wired before a run exists; the run's ledger
// exists only once the engine starts. Observe is how the engine subscribes
// for one run and unsubscribes after, so a Storage reused across runs
// never appends to a sealed ledger.
//
// fn may be called from any goroutine, and never from inside a Sink's Emit.
func (r *Remote) Observe(fn func(Degradation)) (stop func()) { return r.deg.observe(fn) }

// Close releases the connections the remote holds.
func (r *Remote) Close() error { return nil }

// defaultPathStyle decides an addressing style from the endpoint: host
// style for Amazon (which requires it), path style for everyone else
// (MinIO, Ceph and gateways rarely have the wildcard DNS host style
// needs). An explicit setting overrides.
func defaultPathStyle(endpoint string) (bool, error) {
	u, err := url.Parse(endpoint)
	if err != nil {
		return false, fmt.Errorf("remote cache: endpoint is not a URL: %w", err)
	}
	host := u.Hostname()
	return host != "amazonaws.com" && !strings.HasSuffix(host, ".amazonaws.com"), nil
}

// degrader decides, at most once per run, that the remote is not worth
// talking to any more, and respects that: a build whose cache is down pays
// the failure once and proceeds at local speed, rather than paying a
// timeout on every one of several hundred objects.
type degrader struct {
	store  string
	report func(Degradation)
	w      io.Writer

	mu  sync.Mutex
	off bool
	// notices counts the per-object complaints made so far, bounded by
	// maxNotices.
	notices int
	// observer is the run-scoped subscriber, set and cleared by Observe.
	observer func(Degradation)
}

// maxNotices bounds the per-object complaints one run will print: they do
// not disable the store, so a bucket filled with rubbish would otherwise
// print one line per lookup. Eight shows a pattern and stays readable.
const maxNotices = 8

func (d *degrader) live() bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return !d.off
}

// disable records that the store itself stopped working, and stops using it.
// Reports exactly once however many operations fail afterwards.
func (d *degrader) disable(op string, err error) {
	d.mu.Lock()
	if d.off {
		d.mu.Unlock()
		return
	}
	d.off = true
	d.mu.Unlock()
	d.emit(Degradation{Store: d.store, Op: op, Err: err, Disabled: true})
}

// notice records that one object or entry was unusable, without condemning
// the store. See Degradation.Disabled.
func (d *degrader) notice(op string, err error) {
	d.mu.Lock()
	if d.notices >= maxNotices {
		d.mu.Unlock()
		return
	}
	d.notices++
	last := d.notices == maxNotices
	d.mu.Unlock()

	d.emit(Degradation{Store: d.store, Op: op, Err: err})
	if last {
		_, _ = fmt.Fprintf(d.w,
			"senro: remote cache %s: further per-object complaints suppressed\n", d.store)
	}
}

func (d *degrader) emit(rep Degradation) {
	// Standard error first and unconditionally, so a Report that panics or
	// blocks has not already swallowed the only notice.
	_, _ = fmt.Fprintln(d.w, rep.String())
	if d.report != nil {
		d.report(rep)
	}
	d.mu.Lock()
	observer := d.observer
	d.mu.Unlock()
	// Called outside the lock: the observer appends to the run's ledger,
	// which takes the engine's own append lock; holding both in a novel
	// order is how a deadlock is built.
	if observer != nil {
		observer(rep)
	}
}

// observe redirects reports to fn until the returned function is called. See
// Remote.Observe, which is what a caller uses and where this is explained.
func (d *degrader) observe(fn func(Degradation)) (stop func()) {
	d.mu.Lock()
	d.observer = fn
	d.mu.Unlock()
	return func() {
		d.mu.Lock()
		d.observer = nil
		d.mu.Unlock()
	}
}

// classify decides what an error from the remote store means for the store
// as a whole. A missing object is an ordinary answer, not a degradation;
// everything else means the store could not answer. Both backends' "not
// there" sentinels are named here so both degrade on the same terms.
func (d *degrader) classify(op string, err error) {
	switch {
	case err == nil,
		errors.Is(err, s3.ErrNotFound),
		errors.Is(err, oci.ErrNotFound),
		errors.Is(err, cas.ErrNotFound):
		return
	case errors.Is(err, cas.ErrCorrupt):
		// One bad object says nothing about the others.
		d.notice(op, err)
	default:
		d.disable(op, err)
	}
}
