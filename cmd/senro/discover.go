package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/internal/attachsrv"
	"github.com/xavidop/senro/internal/source"
)

// runDir is the on-disk convention this CLI uses to find a run's recorded
// events.jsonl and logs when there is no live engine to ask; it mirrors
// attach.Options.Dir. An embedder may use a different Dir, so --run's disk
// lookup assumes this default in one place rather than three call sites.
func runDir(runID string) string {
	return filepath.Join("runs", runID)
}

// selectEntry resolves which registered live engine a bare `senro attach`
// (or one given --pid) attaches to: the only live one, or a listing when
// there are several. pid == 0 means no --pid was given. Four failure cases
// get distinct messages: no runs; several runs; a pid never registered
// here; and a pid whose entry is stale (see probeRegistryEntry). A socket
// that exists but refuses connection is connectLive's concern, not this
// one's.
func selectEntry(pid int) (attachsrv.Entry, error) {
	if pid != 0 {
		// Checked BEFORE Discover, which reaps a dead entry as a side
		// effect of listing: afterwards, "never existed" and "just reaped"
		// would be indistinguishable.
		if existed, alive := probeRegistryEntry(pid); existed && !alive {
			return attachsrv.Entry{}, fmt.Errorf(
				"senro: pid %d was registered but its process is no longer running: "+
					"a stale registry entry (now reaped). Run `senro attach` with no --pid "+
					"to see what IS currently live", pid)
		}
	}

	entries, err := attachsrv.Discover()
	if err != nil {
		return attachsrv.Entry{}, fmt.Errorf("senro: discovering live runs: %w", err)
	}

	if pid != 0 {
		for _, e := range entries {
			if e.PID == pid {
				return e, nil
			}
		}
		return attachsrv.Entry{}, fmt.Errorf(
			"senro: no live run with pid %d: it was never registered on this machine "+
				"(check the pid, or run `senro attach` with no --pid to list what IS live)", pid)
	}

	switch len(entries) {
	case 0:
		return attachsrv.Entry{}, errors.New(
			"senro: no live senro runs found. Start one with `senro run <pkg>`, " +
				"or a pipeline built with attach.Listen, then attach again")
	case 1:
		return entries[0], nil
	default:
		return attachsrv.Entry{}, fmt.Errorf(
			"senro: %d live runs found, none specified: pass --pid or --run to pick one:\n%s",
			len(entries), formatEntries(entries))
	}
}

// probeRegistryEntry checks, WITHOUT triggering Discover's reaping, whether
// pid has a registry entry on disk and whether its process is alive.
// attachsrv exports no such primitive, so this reads the registry directory
// directly.
func probeRegistryEntry(pid int) (existed, alive bool) {
	dir, err := attachsrv.Dir()
	if err != nil {
		return false, false
	}
	path := filepath.Join(dir, fmt.Sprintf("%d.json", pid))
	if _, err := os.Stat(path); err != nil {
		return false, false
	}
	// Mirrors attachsrv's unexported pidAlive: signal 0 performs only the
	// kernel's existence and permission check. Two lines, not worth
	// widening that package's surface for one diagnostic.
	return true, syscall.Kill(pid, 0) == nil
}

// formatEntries renders every entry, sorted by pid, for the "several runs
// found, none specified" message.
func formatEntries(entries []attachsrv.Entry) string {
	sorted := append([]attachsrv.Entry(nil), entries...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].PID < sorted[j].PID })
	var b strings.Builder
	for _, e := range sorted {
		fmt.Fprintf(&b, "  pid %d  run %s  pipeline %s  cwd %s  started %s\n",
			e.PID, orDash(e.RunID), orDash(e.Pipeline), orDash(e.CWD),
			e.StartedAt.Format(time.RFC3339))
	}
	return b.String()
}

func orDash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

// connectLive dials a discovered entry over whichever transport it
// registered (see endpointForEntry) and, when the entry names a run, wraps
// it with disk fallback rooted at runDir(e.RunID): switching to the file
// source when the live connection drops is the default, not an opt-in.
//
// A dial failure is reported distinctly: the entry was found but its socket
// refuses the connection (a process that died between the liveness check
// and this dial, a stale socket, a permission mismatch). This is the one
// failure mode that happens before there is a Source to ask at all;
// FallbackSource covers everything after.
func connectLive(ctx context.Context, e attachsrv.Entry) (source.Source, error) {
	ep, err := endpointForEntry(e, tokenFromEnv())
	if err != nil {
		return nil, err
	}
	ls, err := source.DialEndpoint(ctx, ep)
	if err != nil {
		return nil, fmt.Errorf(
			"senro: found a registered run (pid %d) but could not connect to it at %s: %w. "+
				"the process may have exited without cleaning up its registry entry, or may still be "+
				"starting up; try again in a moment",
			e.PID, ep.Address, err)
	}
	if e.RunID == "" {
		// No RunID to key a fallback directory on: stay live-only rather
		// than guess a path that cannot be right.
		return ls, nil
	}
	return source.Fallback(ls, runDir(e.RunID)), nil
}

// negotiateVersion runs on first contact with a live engine (src.State,
// which the renderers call first anyway) and acts per api.CheckVersion:
// equal is silent, a minor mismatch warns on stderr with a nil error, and a
// major mismatch returns one so the caller stops, in place of the decode
// garbage a stale CLI would otherwise hit.
//
// ProtoMajor == 0 means the Source has no version to report (a post-mortem
// FileSource) and is skipped, not treated as a mismatch.
func negotiateVersion(ctx context.Context, src source.Source, stderr io.Writer) error {
	st, err := src.State(ctx)
	if err != nil {
		return fmt.Errorf("senro: %w", err)
	}
	if st.ProtoMajor == 0 {
		return nil
	}
	warn, err := api.CheckVersion(api.Version, api.VersionMinor, st.ProtoMajor, st.ProtoMinor)
	if err != nil {
		return fmt.Errorf("senro: %w", err)
	}
	if warn != "" {
		// Best-effort: a stderr write failure does not change whether the
		// caller should proceed.
		_, _ = fmt.Fprintln(stderr, warn)
	}
	return nil
}
