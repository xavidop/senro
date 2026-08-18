package engine

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/internal/cache"
	"github.com/xavidop/senro/internal/cas"
	"github.com/xavidop/senro/internal/eventlog"
	"github.com/xavidop/senro/internal/plan"
)

// cacheDecision is what one lookup concluded, carried from the lookup to the
// hit or to the save so neither has to recompute a key.
type cacheDecision struct {
	key    cache.Key
	digest cas.Digest
	result *cache.Result
	hit    bool
	reason string
	prev   cas.Digest
	diffs  []cache.Diff
}

// cacheable reports whether this build will consult the action cache for n.
func (rc *runCore) cacheable(n *plan.Node) bool {
	return n.Pure && rc.cache != nil && rc.ws != nil
}

// cacheLookup builds n's key, consults the store, and records the decision.
// The key is built HERE, immediately before the step would run, not at plan
// time: the workspace and input digests are only knowable once upstream
// steps have written.
func (rc *runCore) cacheLookup(ctx context.Context, n *plan.Node, opts Options) (cacheDecision, error) {
	// nodeExec, not ex: this function already uses ex for the workspace
	// excluder below, and the two must not collide.
	nodeExec, err := rc.executorFor(n)
	if err != nil {
		return cacheDecision{}, err
	}
	class, err := nodeExec.Class(ctx)
	if err != nil {
		return cacheDecision{}, fmt.Errorf("engine: step %q: executor class: %w", n.ID, err)
	}
	platform, err := nodeExec.DeclaredPlatform(ctx)
	if err != nil {
		return cacheDecision{}, fmt.Errorf("engine: step %q: declared platform: %w", n.ID, err)
	}
	// effectiveEnv is what the step will actually run with: CacheEnv("PATH")
	// must see the executor's injected PATH, not come up empty because PATH
	// never appeared in n.Env.
	effectiveEnv, err := nodeExec.EffectiveEnv(ctx, n.Env)
	if err != nil {
		return cacheDecision{}, fmt.Errorf("engine: step %q: effective env: %w", n.ID, err)
	}

	root := rc.ws.inputRoot(n)
	// ex is the same excluder the workspace's own snapshot uses. See
	// excluderFor's own doc for why Resolve must not fall back to its own
	// bare defaults here.
	ex, err := rc.ws.excluderFor(n)
	if err != nil {
		return cacheDecision{}, fmt.Errorf("engine: step %q: %w", n.ID, err)
	}
	// Shared hold: reading declared Inputs off the workspace is exactly the
	// use lockMounts protects against a concurrent lockRestore. Released
	// right after Resolve; nothing else here touches the content.
	unlock := rc.ws.lockMounts(workspaceMountNames(n))
	inputs, err := cache.Resolve(root, n.Inputs, ex)
	unlock()
	if err != nil {
		return cacheDecision{}, fmt.Errorf("engine: step %q: %w", n.ID, err)
	}

	var wsDigests []cache.WorkspaceDigest
	for _, s := range rc.ws.digests(n) {
		wsDigests = append(wsDigests, cache.WorkspaceDigest{Name: s.Name, Digest: s.Digest})
	}

	// mountShapes comes from n.Mounts directly: Mode and At are
	// plan-declared, not content. Scratch mounts are skipped, as everywhere
	// a cache key is built: never an input to it.
	var mountShapes []cache.MountShape
	for _, ms := range n.Mounts {
		if ms.Workspace == "" {
			continue
		}
		mountShapes = append(mountShapes, cache.MountShape{Name: ms.Workspace, Mode: ms.Mode, At: ms.At})
	}

	// Identities only, never values: a step with no secrets produces the
	// empty component, which is why cache.KeyVersion does not move. The
	// value is read only to digest it, salted with the source so a cache
	// directory is not a rainbow table for a low-entropy credential (see
	// cache.SecretDigest).
	var secretIDs []cache.SecretIdentity
	for _, sec := range n.Secrets {
		id, ok := rc.secrets.Identity(sec.Name)
		if !ok {
			return cacheDecision{}, fmt.Errorf(
				"engine: step %q needs secret %q, which was not resolved", n.ID, sec.Name)
		}
		v, _ := rc.secrets.Value(sec.Name)
		secretIDs = append(secretIDs, cache.SecretIdentity{
			Name: id.Name, Source: id.Source, Version: id.Version,
			Digest8: cache.SecretDigest(id.Source, v),
		})
	}

	// funcIdentity is a Func step's binary digest, registered name and
	// parameter digest; empty for an exec step, which is why
	// cache.KeyVersion does not move (see
	// TestAKeyWithNoFuncIdentityDigestsExactlyAsItAlwaysHas). The binary
	// digest comes through rc.binaryDigest, memoized per run.
	funcIdentity := ""
	if n.Kind == "func" && n.Func != nil {
		bd, err := rc.binaryDigest()
		if err != nil {
			return cacheDecision{}, err
		}
		funcIdentity = cache.FuncIdentityComponent(bd, n.Func.Name, n.Func.Params)
	}

	k := cache.Key{
		Command:          cache.CommandComponent(n.Kind, n.Cmd, n.WorkDir),
		Env:              cache.EnvComponent(effectiveEnv, n.CacheEnv),
		Secrets:          cache.SecretsComponent(secretIDs),
		ExecutorClass:    class,
		Platform:         platform.String(),
		InputDigests:     cache.InputsComponent(inputs),
		WorkspaceDigests: cache.WorkspacesComponent(wsDigests),
		// MountShape and StepShape guard a real trap: a step's Outputs,
		// NoSnapshot, and each mount's Mode and At all change what a saved
		// Result contains or means without moving any of the key's other
		// components, so without them a step corrected after getting one of
		// them wrong would keep hitting the same stale entry forever. See
		// cache.KeyVersion's doc for why this bumped it.
		MountShape:   cache.MountShapeComponent(mountShapes),
		StepShape:    cache.StepShapeComponent(n.NoSnapshot, n.Outputs),
		FuncIdentity: funcIdentity,
		ToolVersions: "", // no tool declarations in this build
		Version:      cache.KeyVersion,
	}
	dec := cacheDecision{key: k, digest: k.Digest()}

	res, ok, err := rc.cache.Lookup(ctx, n.ID, k)
	if err != nil {
		return cacheDecision{}, fmt.Errorf("engine: step %q: cache lookup: %w", n.ID, err)
	}
	dec.hit, dec.result = ok, res

	if !ok {
		// Why it missed is computed HERE, against the entry most recent at
		// lookup time: after a save the most recent entry is this key, and
		// the same comparison later would report nothing.
		prev, hasPrev, err := rc.cache.Previous(ctx, n.ID)
		if err != nil {
			return cacheDecision{}, fmt.Errorf("engine: step %q: cache history: %w", n.ID, err)
		}
		if hasPrev {
			dec.reason = cache.ReasonKeyChanged
			dec.prev = prev.Key.Digest()
			dec.diffs = cache.Explain(prev.Key, k)
		} else {
			dec.reason = cache.ReasonNoPreviousEntry
		}
	}
	return dec, nil
}

// recordDecision writes the run's copy of a key and its verdict, which is
// what `senro cache explain` reads.
func (rc *runCore) recordDecision(dir string, n *plan.Node, dec cacheDecision) {
	rec := cache.Record{
		Step: n.ID, Digest: dec.digest, Key: dec.key, Hit: dec.hit,
		Reason: dec.reason, PreviousDigest: dec.prev, Diffs: dec.diffs,
	}
	if err := cache.WriteRecord(filepath.Join(dir, "cache"), rec); err != nil {
		// A record is a diagnostic. Failing a run because one could not be
		// written would make `cache explain` able to break a build, which is
		// exactly backwards.
		rc.emit(api.Event{
			Type: api.CacheMiss, Step: n.ID,
			Payload: mustMarshal(api.CacheMissBody{
				Key: string(dec.digest), Reason: "record_write_failed", Differing: err.Error(),
			}),
		})
	}
}

// emitMiss records a miss and the component that moved.
func (rc *runCore) emitMiss(n *plan.Node, dec cacheDecision) {
	var differing string
	if len(dec.diffs) > 0 {
		// The FIRST differing component in canonical order. cache explain
		// shows all of them.
		differing = dec.diffs[0].Name
	}
	rc.emit(api.Event{
		Type: api.CacheMiss, Step: n.ID,
		Payload: mustMarshal(api.CacheMissBody{
			Key: string(dec.digest), Reason: dec.reason, Differing: differing,
		}),
	})
}

// serveFromCache reproduces a cached result: the workspaces the step would
// have written, the logs it would have printed, and its exit code.
//
// It reports whether it succeeded. A false return means the entry could not
// be reproduced, the entry has been forgotten, and the caller must run the
// step. That degradation is deliberate and covers every way an entry can be
// incomplete at once: they all have the same right answer, and failing the
// step instead would let a cache sweep break a build.
func (rc *runCore) serveFromCache(
	ctx context.Context, n *plan.Node, opts Options, logs *eventlog.LogSet, dec cacheDecision,
) bool {
	// Every object this hit references is verified present BEFORE anything
	// observable happens: a hit that degraded midway would leave cache.hit
	// in the ledger next to the cache.miss it degraded into, and a
	// workspace already overwritten with cached content would have the step
	// run ON TOP of it rather than from what a genuine miss starts from.
	// Checked with Has, not Get: nothing about this run has changed yet
	// when the decision to degrade is made.
	if err := rc.hitIsReproducible(ctx, opts, dec.result); err != nil {
		return rc.degradeToMiss(ctx, n, dec, err)
	}

	rc.emit(api.Event{
		Type: api.CacheHit, Step: n.ID,
		Payload: mustMarshal(api.CacheHitBody{Key: string(dec.digest), FromRun: dec.result.RunID}),
	})

	// Exclusive hold covering BOTH loops below: restoring a workspace's
	// body and placing cached output files back into the same directory.
	// One continuous hold rather than one per operation, so a sibling
	// cannot slip a mount in between "workspace restored" and "output
	// placed".
	unlock := rc.ws.lockRestore(workspaceMountNames(n))
	defer unlock()

	for _, w := range dec.result.Workspaces {
		if err := rc.ws.restore(ctx, w.Name, w.Digest); err != nil {
			// The presence check passed, so the object existed but could
			// not be reproduced faithfully (a corrupt tar, an unsafe entry,
			// a racing GC): same class of problem, same right answer.
			return rc.degradeToMiss(ctx, n, dec, err)
		}
		rc.emit(api.Event{
			Type: api.WSRestored, Step: n.ID,
			Payload: mustMarshal(api.WSRestoredBody{Name: w.Name, Digest: string(w.Digest)}),
		})
	}

	root := rc.ws.inputRoot(n)
	for _, o := range dec.result.Outputs {
		if err := rc.restoreOutput(ctx, opts, root, o); err != nil {
			return rc.degradeToMiss(ctx, n, dec, err)
		}
	}

	for _, l := range dec.result.Logs {
		if err := rc.replayLog(ctx, opts, logs, n, l); err != nil {
			return rc.degradeToMiss(ctx, n, dec, err)
		}
	}
	// Replayed output is archived exactly as a step that really ran would
	// be: an archive reader should not be able to tell which steps hit the
	// cache from which streams are present (the ledger's cache.hit says
	// that). replayLog closed its writer, so the files are final; attempt
	// 1, matching what replayLog writes under.
	rc.archiveAttempt(logs, n.ID, 1)
	return true
}

// hitIsReproducible reports whether every CAS object res references is
// present, without reading any of them. The action cache holds no CAS
// handle, so this falls to the one caller with both (see
// ReasonEntryIncomplete and Dir.Lookup). Has, not Get: this runs before the
// hit is committed to, and must not start pulling bytes for an entry that
// may still turn out incomplete.
func (rc *runCore) hitIsReproducible(ctx context.Context, opts Options, res *cache.Result) error {
	for _, w := range res.Workspaces {
		if err := requirePresent(ctx, opts, "workspace", w.Name, w.Digest); err != nil {
			return err
		}
	}
	for _, o := range res.Outputs {
		if err := requirePresent(ctx, opts, "output", o.Path, o.Digest); err != nil {
			return err
		}
	}
	for _, l := range res.Logs {
		if err := requirePresent(ctx, opts, "log", l.Stream, l.Digest); err != nil {
			return err
		}
	}
	return nil
}

// requirePresent is one hitIsReproducible check: name and kind are only for
// the error message, so a degraded miss's cache.miss.differing says which
// piece of the result went missing.
func requirePresent(ctx context.Context, opts Options, kind, name string, d cas.Digest) error {
	ok, err := opts.Storage.Objects.Has(ctx, d)
	if err != nil {
		return fmt.Errorf("%s %q: %w", kind, name, err)
	}
	if !ok {
		return fmt.Errorf("%s %q: object %s not found", kind, name, d.Short())
	}
	return nil
}

// degradeToMiss turns a broken entry into an ordinary miss.
func (rc *runCore) degradeToMiss(ctx context.Context, n *plan.Node, dec cacheDecision, cause error) bool {
	_ = rc.cache.Forget(ctx, dec.key)
	rc.emit(api.Event{
		Type: api.CacheMiss, Step: n.ID,
		Payload: mustMarshal(api.CacheMissBody{
			Key: string(dec.digest), Reason: cache.ReasonEntryIncomplete, Differing: cause.Error(),
		}),
	})
	return false
}

// restoreOutput puts a declared output file back where the step would have
// written it.
func (rc *runCore) restoreOutput(ctx context.Context, opts Options, root string, o cache.FileDigest) error {
	if err := cache.SafeRelative(o.Path); err != nil {
		return err
	}
	rc2, err := opts.Storage.Objects.Get(ctx, o.Digest)
	if err != nil {
		return err
	}
	defer func() { _ = rc2.Close() }()

	target := filepath.Join(root, filepath.FromSlash(o.Path))
	if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
		return err
	}
	f, err := os.OpenFile(target, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	_, copyErr := io.Copy(f, rc2)
	closeErr := f.Close()
	if copyErr != nil {
		return copyErr
	}
	return closeErr
}

// replayLog writes a cached step's stored output into this run's own log
// files and emits the byte-range markers for it.
//
// Attempt 1, always: a cached step has exactly one notional attempt, and
// carrying the stored run's attempt number across would produce a log path
// that does not match this run's step.finished.
func (rc *runCore) replayLog(
	ctx context.Context, opts Options, logs *eventlog.LogSet, n *plan.Node, l cache.LogRef,
) (err error) {
	body, err := opts.Storage.Objects.Get(ctx, l.Digest)
	if err != nil {
		return err
	}
	defer func() { _ = body.Close() }()

	w, err := logs.Writer(n.ID, 1, l.Stream)
	if err != nil {
		return err
	}
	defer func() { _ = w.Close() }()

	// Through logMarker, exactly as a live step's output goes. And through
	// THIS RUN's redactor: the stored bytes were redacted for the secrets
	// that existed then, and a value that became a secret afterwards would
	// otherwise be replayed into this run's log and out to every client.
	m := &logMarker{rc: rc, w: w, step: n.ID, attempt: 1, stream: l.Stream}
	rw := rc.redact.Writer(m)
	_, err = io.Copy(rw, body)
	// Flush unconditionally, deferred: a copy error would otherwise strand
	// the partial match redact.Writer is holding back, up to Set.max bytes
	// of genuine output. The copy's own error wins when both fail, being
	// the more informative one.
	defer func() {
		if ferr := rw.Flush(); ferr != nil && err == nil {
			err = ferr
		}
	}()
	return err
}

// cacheSave stores what a successful pure step produced.
//
// Only on success, and only for a step that actually ran: a cached step has
// nothing new to store, and a failed step must never be saved, or the failure
// is served to every future run with the same key.
func (rc *runCore) cacheSave(
	ctx context.Context, n *plan.Node, opts Options, logs *eventlog.LogSet,
	dec cacheDecision, res attemptResult, attempt int, dur time.Duration,
) error {
	result := &cache.Result{
		ExitCode:    res.exitCode,
		DurationNS:  dur.Nanoseconds(),
		RunID:       rc.runID,
		Hermeticity: cache.HermeticityTrusted,
		SavedAt:     time.Now().UTC(),
	}

	for _, s := range res.snapshots {
		result.Workspaces = append(result.Workspaces,
			cache.WorkspaceDigest{Name: s.Name, Digest: s.Digest})
		result.Bytes += s.Bytes
	}

	root := rc.ws.inputRoot(n)
	// Same excluder as cacheLookup's Inputs resolution (see excluderFor),
	// so Inputs and Outputs share one definition of "part of this
	// workspace".
	ex, err := rc.ws.excluderFor(n)
	if err != nil {
		return fmt.Errorf("engine: step %q: %w", n.ID, err)
	}
	// Shared hold across both resolving n's declared Outputs and reading
	// each one's bytes into the CAS: a sibling's cache-hit restore of the
	// same workspace must not land in the middle.
	unlock := rc.ws.lockMounts(workspaceMountNames(n))
	defer unlock()
	outputs, err := cache.Resolve(root, n.Outputs, ex)
	if err != nil {
		return fmt.Errorf("engine: step %q: declared outputs: %w", n.ID, err)
	}
	for _, o := range outputs {
		if _, err := putFile(ctx, opts, filepath.Join(root, filepath.FromSlash(o.Path))); err != nil {
			return fmt.Errorf("engine: step %q: store output %s: %w", n.ID, o.Path, err)
		}
		result.Outputs = append(result.Outputs, o)
	}

	for _, stream := range []string{api.StreamStdout, api.StreamStderr} {
		p := logs.Path(n.ID, attempt, stream)
		d, size, err := putFileIfPresent(ctx, opts, p)
		if err != nil {
			return fmt.Errorf("engine: step %q: store %s log: %w", n.ID, stream, err)
		}
		if d == "" {
			continue
		}
		result.Logs = append(result.Logs, cache.LogRef{Stream: stream, Digest: d, Bytes: size})
		result.Bytes += size
	}

	if err := rc.cache.Save(ctx, n.ID, dec.key, result); err != nil {
		return fmt.Errorf("engine: step %q: cache save: %w", n.ID, err)
	}
	rc.emit(api.Event{
		Type: api.CacheSaved, Step: n.ID, Attempt: attempt,
		Payload: mustMarshal(api.CacheSavedBody{Key: string(dec.digest), Bytes: result.Bytes}),
	})
	return nil
}

func putFile(ctx context.Context, opts Options, p string) (cas.Digest, error) {
	f, err := os.Open(p)
	if err != nil {
		return "", err
	}
	defer func() { _ = f.Close() }()
	return opts.Storage.Objects.Put(ctx, f)
}

// putFileIfPresent stores a file that may legitimately not exist, which is
// the case for a stream a step never wrote to. It returns an empty digest
// for that case rather than an error.
func putFileIfPresent(ctx context.Context, opts Options, p string) (cas.Digest, int64, error) {
	fi, err := os.Stat(p)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return "", 0, nil
		}
		return "", 0, err
	}
	d, err := putFile(ctx, opts, p)
	if err != nil {
		return "", 0, err
	}
	return d, fi.Size(), nil
}
