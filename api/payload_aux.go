package api

import "time"

// CacheHitBody is the payload of a cache.hit event.
type CacheHitBody struct {
	Key     string `json:"key"`
	FromRun string `json:"from_run,omitempty"`
}

// CacheMissBody is the payload of a cache.miss event.
//
// Differing names the first key component that changed, which is what makes
// `senro cache explain` possible and stops the cache acquiring a reputation
// for being broken.
type CacheMissBody struct {
	Key       string `json:"key"`
	Reason    string `json:"reason"`
	Differing string `json:"differing,omitempty"`
}

// CacheSavedBody is the payload of a cache.saved event.
type CacheSavedBody struct {
	Key   string `json:"key"`
	Bytes int64  `json:"bytes"`
}

// CacheDegradedBody is the payload of a cache.degraded event.
//
// Every field is chosen so an operator can act on the line without opening a
// dashboard, and nothing here may ever carry a credential: this body is
// persisted to events.jsonl, streamed to every attached client and routinely
// pasted into a bug report. Store is a name, never a URL with a query string;
// Error has the store's own message with the credentials scrubbed out of it.
type CacheDegradedBody struct {
	// Store names the shared cache without naming how to authenticate to it,
	// e.g. "s3 bucket team-cache at s3.eu-west-1.amazonaws.com".
	Store string `json:"store"`
	// Op is what was being attempted when it failed: "get", "put", "head",
	// "lookup", "save", "previous".
	Op string `json:"op"`
	// Error is why, in the store's own words.
	Error string `json:"error"`
	// Disabled reports whether the shared cache was switched off for the rest
	// of the run. False means one object was unusable and the store is still
	// in use, which is the ordinary response to a single corrupt object: it
	// says nothing about the rest of the bucket.
	Disabled bool `json:"disabled,omitempty"`
}

// WSSnapshotBody is the payload of a ws.snapshot event.
//
// Two digests, and they address different objects. Digest is the workspace's
// identity, the content address of the normalized tar, and it is what enters
// the next step's cache key and what `senro ws` commands take as an argument.
// Index addresses the file list, stored separately so a client can show what
// is in a snapshot without downloading the body.
type WSSnapshotBody struct {
	Name   string `json:"name"`
	Digest string `json:"digest"`
	Index  string `json:"index,omitempty"`
	Bytes  int64  `json:"bytes"`
	Files  int    `json:"files"`
	// Forced marks a capture the OpWSSnapshot control operation asked for,
	// rather than one a step settling or a persistent workspace opening
	// produced. Diagnostic only: the digests are real and `senro ws pull`
	// works on them, but the capture entered no cache key and replaced no
	// workspace's recorded state, so `senro ws ls`, `pull` and `diff` skip
	// it and report what the run actually produced.
	//
	// omitempty, so every snapshot the engine takes on its own puts exactly
	// the bytes on the wire it always did: a run's ledger does not change
	// shape because this field exists.
	Forced bool `json:"forced,omitempty"`
}

// WSRestoredBody is the payload of a ws.restored event.
type WSRestoredBody struct {
	Name   string `json:"name"`
	Digest string `json:"digest"`
}

// WSEvictedBody is the payload of a ws.evicted event: one persistent
// workspace emptied because it hit a bound.
//
// It carries the measurement AND the bound it was measured against, always
// both. "Your workspace was evicted" is not actionable; "4.2 GiB against a
// 1 GiB MaxSize" names the number to change. The pair that does not apply to
// a given reason stays zero (a max_age eviction never measured a size),
// which omitempty shows as an absent field rather than as a confident zero.
type WSEvictedBody struct {
	Name string `json:"name"`
	// Reason is "max_age" or "max_size".
	Reason string `json:"reason"`
	// Bytes is what the workspace held and MaxBytes the MaxSize it was
	// measured against. Set for a "max_size" eviction.
	Bytes    int64 `json:"bytes,omitempty"`
	MaxBytes int64 `json:"max_bytes,omitempty"`
	// AgeMS is how long the workspace had gone unused and MaxAgeMS the MaxAge
	// it was measured against, both in milliseconds. Set for a "max_age"
	// eviction.
	AgeMS    int64 `json:"age_ms,omitempty"`
	MaxAgeMS int64 `json:"max_age_ms,omitempty"`
	// When is where in the run the eviction happened: "acquire" for one made
	// before the first step, "release" for one made after the last. An
	// eviction at acquire is a run starting cold; one at release is a run
	// that built something too large to keep. Opposite problems, same
	// reason code.
	When string `json:"when"`
}

// BinaryStagedBody is the payload of a binary.staged event: one copy of the
// engine's own binary, now on an execution target.
//
// Nothing here is a credential and nothing here may become one. This body is
// persisted to events.jsonl, streamed to every attached client and routinely
// pasted into a bug report, so Target is a host or a class exactly as the
// plan already records it, and Path is a directory senro itself chose.
type BinaryStagedBody struct {
	// Digest is the staged binary's content address and its name on the
	// target, so two runs naming the same digest name the same file. The
	// child reports it back on handshake; a disagreement aborts the step.
	Digest string `json:"digest"`
	// Platform is the target's GOOS/GOARCH, which is what decided whether
	// this binary could be shipped as it is or had to be built for it.
	Platform string `json:"platform"`
	// Strategy is how the coordinator obtained it: "identity" for its own
	// executable shipped unchanged, "cross-build" for a fresh compile. "This
	// run spent forty seconds staging" has two very different causes, and
	// only one of them is the network.
	Strategy string `json:"strategy"`
	// Target names the execution target in the terms the plan already uses: a
	// host, for the ssh executor.
	Target string `json:"target"`
	// Path is where it landed on the target.
	Path string `json:"path"`
	// Bytes is its size.
	Bytes int64 `json:"bytes"`
	// Reused says the binary was ALREADY on the target and nothing was
	// transferred: a run whose every func step reports false is paying the
	// transfer once per step rather than once per host.
	Reused bool `json:"reused,omitempty"`
	// DurationNS is how long staging took, transfer included.
	DurationNS int64 `json:"duration_ns,omitempty"`
}

// SecretResolvedBody is the payload of a secret.resolved event.
//
// Identity only: name, source URI, provider version. A secret value must
// never enter the event stream under any circumstances.
type SecretResolvedBody struct {
	Name    string `json:"name"`
	Source  string `json:"source"`
	Version string `json:"version,omitempty"`
}

// SecretRedactedBody is the payload of a secret.redacted event, reporting how
// many values the stream redactor replaced so the UI can show redaction is
// live.
type SecretRedactedBody struct {
	Count int `json:"count"`
}

// ClientBody is the payload of client.attached and client.detached events.
type ClientBody struct {
	ClientID string `json:"client_id"`
	Kind     string `json:"kind,omitempty"` // "tui", "plain", "browser"
	Peer     string `json:"peer,omitempty"`
}

// NotifyBody is the payload of notify.delivered, notify.failed and
// notify.dropped.
//
// One body for all three, the way HandlerBody serves three handler events:
// each needs the same facts, and the event type already says what happened.
//
// Every field describes the DELIVERY. The event being delivered is named by
// Event and Seq, never inlined: inlining would double every payload in the
// ledger and give a redactor a second surface to get right.
type NotifyBody struct {
	// Destination is the notifier's name for where this was going
	// ("webhook", "slack", or whatever the caller named it). Never a URL:
	// a webhook URL is frequently the credential itself.
	Destination string `json:"destination"`

	// Event is the type of the event this delivery was about, and Seq is
	// that event's sequence number in this same ledger, so a reader can
	// find it without decoding anything else.
	Event Type   `json:"event"`
	Seq   uint64 `json:"seq"`

	// Attempts is how many HTTP requests were made, so a third-try success
	// stays distinguishable from a first-try one. Zero on notify.dropped,
	// which never reached a request at all.
	Attempts int `json:"attempts,omitempty"`

	// Status is the HTTP status code of the request that settled the
	// outcome. Omitted when no response was ever received.
	Status int `json:"status,omitempty"`

	// Duration is the wall-clock time spent on the whole delivery,
	// including retry waits.
	Duration time.Duration `json:"duration_ns,omitempty"`

	// Error is set on notify.failed alone, and names why the last attempt
	// did not succeed.
	Error string `json:"error,omitempty"`

	// Dropped is set on notify.dropped alone: how many events this
	// destination has lost to a full queue so far in this run, the running
	// total rather than a per-event flag, so a slow endpoint shows up as a
	// climbing number.
	Dropped int `json:"dropped,omitempty"`
}

// ControlAppliedBody is the payload of a control.applied event.
//
// Every accepted control operation is also an event carrying the originating
// client, so all attached clients see who did what and the run's audit trail
// stays complete.
type ControlAppliedBody struct {
	Op       string            `json:"op"`
	ClientID string            `json:"client_id"`
	Args     map[string]string `json:"args,omitempty"`
}

// BreakpointHitBody is the payload of a breakpoint.hit event.
//
// The step being held is on the envelope (Event.Step), not in here, exactly
// like every other step-scoped event: a client filtering the stream must be
// able to route it without decoding a payload.
type BreakpointHitBody struct {
	// ClientID is the client that ARMED this breakpoint, not necessarily the
	// one that will clear it. Recorded for the same reason ControlAppliedBody
	// carries one: a run that stopped needs to say who stopped it.
	ClientID string `json:"client_id,omitempty"`
}

// HandlerBody is the payload of handler.started, handler.succeeded and
// handler.failed events.
//
// Parent names the step whose failure triggered the handler. A handler failure
// is recorded alongside the original cause, never in place of it.
//
// Error is set on handler.failed alone. One body type serves all three events
// because a client filtering the stream needs the same two routing facts:
// which list this came from, and whose, regardless of outcome.
type HandlerBody struct {
	Kind   string `json:"kind"` // "on_failure" or "always"
	Parent string `json:"parent"`
	Error  string `json:"error,omitempty"`
	// Panicked reports that the handler's registered function panicked
	// rather than returning an error: the handler equivalent of
	// StatePanicked. An error is a result the author considered, a panic is
	// one they did not. The panic's stack is in the handler's own stderr
	// log, not here.
	Panicked bool `json:"panicked,omitempty"`

	// SpanID is this handler run's span, identical on the handler.started
	// that opens it and the handler.succeeded/failed that closes it. A
	// handler runs exactly once, so unlike a step there is no attempt for
	// this to be per.
	//
	// A handler emits no step.log.appended markers, so anything modeling a
	// run by what steps did skips it entirely; a cleanup handler that ran
	// thirty seconds and then failed is exactly the span somebody wants when
	// the next run cannot take the lock.
	SpanID string `json:"span_id,omitempty"`

	// ParentSpanID is the span of the ATTEMPT whose outcome triggered this
	// handler: the latest attempt at Parent. The attempt rather than the
	// run, because the handler ran BECAUSE that attempt ended the way it
	// did, and hanging it off the run would discard that fact.
	ParentSpanID string `json:"parent_span_id,omitempty"`
}

// HandlerSupersededBody is the payload of a handler.superseded event.
//
// Emitted once per accepted step.retry (internal/engine/control.go) for a
// step whose prior attempt already ran its OnFailure and/or Always handlers.
// Those handler runs are not re-run and stay in the ledger exactly as they
// happened (see the event Type's own doc); this is the marker that lets a
// reader tell they no longer describe the step's final outcome.
type HandlerSupersededBody struct {
	// SupersededAttempt is the step attempt number whose handler runs this
	// event marks as stale. The event's own envelope Attempt field names the
	// NEW attempt that superseded it.
	SupersededAttempt int `json:"superseded_attempt"`
	// OnFailure and Always each report whether that handler list was
	// non-empty for the superseded attempt, i.e., whether it actually ran.
	// Both may be true: a step can declare both lists.
	OnFailure bool `json:"on_failure,omitempty"`
	Always    bool `json:"always,omitempty"`
}
