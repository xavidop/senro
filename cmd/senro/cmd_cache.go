package main

import (
	"context"
	"fmt"
	"io"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/xavidop/senro"
	"github.com/xavidop/senro/internal/cache"
	"github.com/xavidop/senro/internal/scratch"
	"github.com/xavidop/senro/internal/stepid"
	"github.com/xavidop/senro/internal/storage"
)

// defaultKeepFailed is how long a failed run's workspaces survive a sweep.
// A week, because "why did this break" is usually asked days later by
// someone who was not there.
const defaultKeepFailed = 7 * 24 * time.Hour

// cacheUsage documents both subcommands cmdCache dispatches to.
const cacheUsage = `Usage:
  senro cache gc [--max-size 50G] [--keep-failed 168h] [--dry-run] [--cache-dir DIR]
      Reclaim disk. Least recently used entries go first; the workspaces of a
      failed run are kept for --keep-failed so the snapshot you are debugging
      is still there.

  senro cache explain [--run RUN] [STEP]
      Explain why a Pure() step hit or missed the action cache against the
      run's own record: every key component that changed, both sides, and
      what stayed the same. With no STEP, summarise every step and scratch
      cache the run touched.

  senro cache scratch [--pipeline NAME] [--limit N] [KEY-PREFIX]
      List the scratch cache entries in the shared bucket, newest first.
      Reads the same SENRO_REMOTE_* environment a run does. With no
      --pipeline, shows every pipeline sharing the store.
`

// cmdCache implements `senro cache <subcommand>`.
func cmdCache(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		_, _ = fmt.Fprint(stderr, cacheUsage)
		return exitUsage
	}
	switch args[0] {
	case "gc":
		return cmdCacheGC(args[1:], stdout, stderr)
	case "explain":
		return cmdCacheExplain(args[1:], stdout, stderr)
	case "scratch":
		return cmdCacheScratch(args[1:], stdout, stderr)
	default:
		_, _ = fmt.Fprintf(stderr, "senro cache: unknown subcommand %q\n\n%s", args[0], cacheUsage)
		return exitUsage
	}
}

// cmdCacheExplain implements `senro cache explain`: a pure formatter over
// facts the engine recorded to <run>/cache, with no re-planning or
// re-hashing that could disagree with what the run concluded. Hence no
// --cache-dir flag: every record it reads is in the RUN directory.
func cmdCacheExplain(args []string, stdout, stderr io.Writer) int {
	var run, step string
	for i := 0; i < len(args); i++ {
		switch a := args[i]; {
		case a == "--run" && i+1 < len(args):
			run = args[i+1]
			i++
		case strings.HasPrefix(a, "--"):
			_, _ = fmt.Fprintf(stderr, "senro cache explain: unknown flag %q\n\n%s", a, cacheUsage)
			return exitUsage
		case step == "":
			step = a
		default:
			_, _ = fmt.Fprintf(stderr, "senro cache explain: unexpected argument %q\n\n%s", a, cacheUsage)
			return exitUsage
		}
	}

	dir, err := resolveRunDir(run)
	if err != nil {
		_, _ = fmt.Fprintln(stderr, "senro cache explain:", err)
		return exitUsage
	}
	cacheDir := filepath.Join(dir, "cache")

	if step != "" {
		// An address may carry an attempt suffix at the CLI boundary and
		// never inside a record, so it is stripped here and nowhere else.
		id, _, err := stepid.ParseAddress(step)
		if err != nil {
			_, _ = fmt.Fprintln(stderr, "senro cache explain:", err)
			return exitUsage
		}
		rec, err := cache.ReadRecord(cacheDir, id)
		if err != nil {
			_, _ = fmt.Fprintf(stderr,
				"senro cache explain: no cache record for step %q in %s "+
					"(only Pure() steps have one, and only once they are actually attempted: "+
					"a step skipped because a dependency failed never reaches the cache at all)\n", id, dir)
			return exitUsage
		}
		if err := cache.FormatExplain(stdout, rec); err != nil {
			_, _ = fmt.Fprintln(stderr, "senro cache explain:", err)
			return exitRunFailed
		}
		return exitSuccess
	}

	recs, err := cache.ReadRecords(cacheDir)
	if err != nil {
		_, _ = fmt.Fprintln(stderr, "senro cache explain:", err)
		return exitUsage
	}
	for _, r := range recs {
		if err := cache.FormatExplain(stdout, r); err != nil {
			_, _ = fmt.Fprintln(stderr, "senro cache explain:", err)
			return exitRunFailed
		}
	}

	// The scratch cache emits no events, so this is the one place its
	// behaviour is visible.
	sr, err := scratch.ReadRecords(cacheDir)
	if err != nil {
		_, _ = fmt.Fprintln(stderr, "senro cache explain:", err)
		return exitUsage
	}
	for _, r := range sr {
		state := "cold"
		switch {
		case r.Restored && r.RestoredFrom == r.Key:
			state = "restored (exact)"
		case r.Restored:
			state = "restored from " + r.RestoredFrom
		case r.Saved:
			state = "cold, saved"
		}
		if r.Unread {
			// The one scratch outcome that is neither a hit nor an ordinary
			// miss, and the only place it is ever visible: a step on a machine
			// of its own mounted this cache and its copy never came back, so
			// the run stored nothing rather than storing a stale one under a
			// key nothing can rewrite.
			state += "; not saved, the step's own copy never came back"
		}
		if _, err := fmt.Fprintf(stdout, "scratch  %s  key %s  %s\n", r.Name, r.Key, state); err != nil {
			return exitRunFailed
		}
	}
	if len(recs) == 0 && len(sr) == 0 {
		_, _ = fmt.Fprintf(stdout, "no cache activity recorded in %s: no step declared Pure() and no scratch cache was mounted\n", dir)
	}
	return exitSuccess
}

// cmdCacheGC implements `senro cache gc`.
func cmdCacheGC(args []string, stdout, stderr io.Writer) int {
	var (
		cacheDir   string
		maxSize    int64
		keepFailed = defaultKeepFailed
		dryRun     bool
	)
	for i := 0; i < len(args); i++ {
		switch a := args[i]; {
		case a == "--dry-run":
			dryRun = true
		case a == "--cache-dir" && i+1 < len(args):
			cacheDir = args[i+1]
			i++
		case a == "--max-size" && i+1 < len(args):
			n, err := parseSize(args[i+1])
			if err != nil {
				_, _ = fmt.Fprintln(stderr, "senro cache gc:", err)
				return exitUsage
			}
			maxSize = n
			i++
		case a == "--keep-failed" && i+1 < len(args):
			d, err := time.ParseDuration(args[i+1])
			if err != nil {
				_, _ = fmt.Fprintln(stderr, "senro cache gc:", err)
				return exitUsage
			}
			keepFailed = d
			i++
		default:
			_, _ = fmt.Fprintf(stderr, "senro cache gc: unknown argument %q\n\n%s", a, cacheUsage)
			return exitUsage
		}
	}

	root, err := resolveCacheDir(cacheDir)
	if err != nil {
		_, _ = fmt.Fprintln(stderr, "senro cache gc:", err)
		return exitUsage
	}
	store, err := storage.Open(root)
	if err != nil {
		_, _ = fmt.Fprintln(stderr, "senro cache gc:", err)
		return exitUsage
	}
	defer func() { _ = store.Close() }()

	stats, err := cache.GC(context.Background(), cache.GCOptions{
		CAS: store.CAS, Action: store.Action, Scratch: store.Scratch,
		PinsDir:     filepath.Join(root, "pins"),
		InFlightDir: filepath.Join(root, "inflight"),
		MaxSize:     maxSize, KeepFailed: keepFailed, Now: time.Now(), DryRun: dryRun,
	})
	if err != nil {
		_, _ = fmt.Fprintln(stderr, "senro cache gc:", err)
		return exitRunFailed
	}

	prefix := ""
	if dryRun {
		prefix = "dry run: "
	}
	_, _ = fmt.Fprintf(stdout,
		"%s%d of %d objects deleted, %s freed of %s; %d of %d entries evicted; "+
			"%d pinned objects and %d scratch-referenced objects kept, %d pins expired, "+
			"%d leaked temp files swept\n",
		prefix, stats.ObjectsDeleted, stats.ObjectsScanned,
		humanBytes(stats.BytesFreed), humanBytes(stats.BytesBefore),
		stats.EntriesEvicted, stats.EntriesScanned,
		stats.PinnedObjects, stats.ScratchProtectedObjects, stats.PinsExpired, stats.TmpFilesSwept)
	if stats.DeferredForInFlightSave {
		_, _ = fmt.Fprintln(stdout,
			"note: a scratch cache save was in progress, so no object was deleted this run; "+
				"run again once it finishes")
	}
	if stats.DeferredForInFlightRun {
		_, _ = fmt.Fprintln(stdout,
			"note: a pipeline run was in progress against this cache root, so no object was deleted "+
				"this run; run again once it finishes")
	}
	return exitSuccess
}

// resolveCacheDir is the one place a --cache-dir flag becomes a path, so
// the CLI and senro.Run agree on where a cache lives.
func resolveCacheDir(flag string) (string, error) {
	if flag != "" {
		return flag, nil
	}
	return storage.DefaultRoot()
}

// parseSize reads "50G", "500M", "1K" or a plain byte count. Integer-only:
// "1.5G" is refused rather than rounded into a budget nobody set.
func parseSize(s string) (int64, error) {
	if s == "" {
		return 0, fmt.Errorf("empty size")
	}
	mult := int64(1)
	switch strings.ToUpper(s[len(s)-1:]) {
	case "K":
		mult, s = 1024, s[:len(s)-1]
	case "M":
		mult, s = 1024*1024, s[:len(s)-1]
	case "G":
		mult, s = 1024*1024*1024, s[:len(s)-1]
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("bad size %q: want a byte count, optionally suffixed K, M or G", s)
	}
	if n < 0 {
		return 0, fmt.Errorf("size must not be negative")
	}
	return n * mult, nil
}

// humanBytes renders n as a short, human-scaled byte count for the gc
// summary line.
func humanBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for v := n / unit; v >= unit; v /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(n)/float64(div), "KMGTPE"[exp])
}

// cmdCacheScratch implements `senro cache scratch`: what the shared bucket
// actually holds, which is otherwise invisible.
//
// A scratch cache is best-effort and never explains itself during a run, so
// without this the only way to answer "is anything in there, and is my
// RestoreKeys prefix matching it" is a bucket browser and a mental model of
// senro's key layout. Reads the store directly rather than a run's records:
// the question is about the bucket, not about one run's view of it.
func cmdCacheScratch(args []string, stdout, stderr io.Writer) int {
	var pipeline, keyPrefix string
	limit := 50
	for i := 0; i < len(args); i++ {
		switch a := args[i]; {
		case a == "--pipeline" && i+1 < len(args):
			i++
			pipeline = args[i]
		case a == "--limit" && i+1 < len(args):
			i++
			n, err := strconv.Atoi(args[i])
			if err != nil || n <= 0 {
				_, _ = fmt.Fprintf(stderr, "senro cache scratch: --limit %q is not a positive number\n", args[i])
				return exitUsage
			}
			limit = n
		case strings.HasPrefix(a, "-"):
			_, _ = fmt.Fprintf(stderr, "senro cache scratch: unknown flag %q\n\n%s", a, cacheUsage)
			return exitUsage
		default:
			keyPrefix = a
		}
	}

	rc, ok, err := senro.RemoteCacheFromEnv()
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "senro cache scratch: %v\n", err)
		return exitUsage
	}
	if !ok {
		_, _ = fmt.Fprintf(stderr,
			"senro cache scratch: no shared cache is configured, so there is no bucket to list.\n"+
				"  export %s=s3://<bucket>          # or s3://<bucket>/<prefix>\n"+
				"  export %s=https://s3.<region>.amazonaws.com\n"+
				"  export %s=<region>\n"+
				"  export %s=1\n"+
				"See https://senro.dev/docs/data/scratch-sharing/\n",
			senro.EnvRemoteCache, senro.EnvRemoteCacheEndpoint,
			senro.EnvRemoteCacheRegion, senro.EnvRemoteScratch)
		return exitUsage
	}
	if rc.Registry.Host != "" {
		_, _ = fmt.Fprintf(stderr,
			"senro cache scratch: scratch caches are not shared through an OCI registry, so there "+
				"is nothing to list. The RestoreKeys fallback is a prefix listing, which the "+
				"registry API cannot do; only an s3:// target shares them.\n")
		return exitUsage
	}

	remote, err := openRemote(rc, true)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "senro cache scratch: %v\n", err)
		return exitUsage
	}
	defer func() { _ = remote.Close() }()

	entries, err := remote.ListScratch(context.Background(), pipeline, keyPrefix, limit)
	if err != nil {
		_, _ = fmt.Fprintf(stderr,
			"senro cache scratch: listing the bucket failed: %v\n"+
				"Listing needs s3:ListBucket, which the rest of senro does not use, so a "+
				"credential scoped to GetObject and PutObject alone reaches everything else "+
				"and fails here.\n", err)
		return exitRunFailed
	}
	if len(entries) == 0 {
		if pipeline == "" && keyPrefix == "" {
			_, _ = fmt.Fprintln(stdout, "No scratch entries in the shared cache.")
		} else {
			_, _ = fmt.Fprintf(stdout, "No scratch entries match %s.\n", scratchFilterLabel(pipeline, keyPrefix))
		}
		return exitSuccess
	}

	_, _ = fmt.Fprintf(stdout, "%-24s  %-40s  %10s  %s\n", "PIPELINE", "KEY", "SIZE", "STORED")
	for _, e := range entries {
		_, _ = fmt.Fprintf(stdout, "%-24s  %-40s  %10s  %s\n",
			truncate(e.Namespace, 24), truncate(e.Key, 40),
			humanBytes(e.Size), e.Stored.UTC().Format(time.RFC3339))
	}
	return exitSuccess
}

// scratchFilterLabel names what a caller narrowed by, so an empty listing
// says which filter produced it rather than just "nothing".
func scratchFilterLabel(pipeline, keyPrefix string) string {
	switch {
	case pipeline != "" && keyPrefix != "":
		return fmt.Sprintf("pipeline %q and key prefix %q", pipeline, keyPrefix)
	case pipeline != "":
		return fmt.Sprintf("pipeline %q", pipeline)
	default:
		return fmt.Sprintf("key prefix %q", keyPrefix)
	}
}

// truncate keeps a column a column. A scratch key holds a content hash and
// is routinely longer than any terminal, and one wrapped row makes the whole
// listing unreadable.
func truncate(s string, max int) string {
	if len(s) <= max {
		return s
	}
	if max <= 1 {
		return s[:max]
	}
	return s[:max-1] + "…"
}
