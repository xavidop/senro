package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/internal/cas"
	"github.com/xavidop/senro/internal/eventlog"
	"github.com/xavidop/senro/internal/storage"
	"github.com/xavidop/senro/internal/workspace"
)

// largeWorkspaceBytes is where ws ls starts flagging a workspace as large.
// In the CLI rather than the engine: there is no warning event type in the
// api module, and inventing one would be a schema change for a diagnostic.
const largeWorkspaceBytes = 2 << 30

// wsUsage documents every senro ws subcommand.
//
// --cache-dir mirrors `senro cache gc`'s flag: senro.WithCacheDir lets a
// library caller put a run's cache anywhere, and without this flag there is
// no way to point these commands at a run produced that way.
//
// The three shapes below are an interface: argument order and exit codes
// are documented here, in site/src/pages/docs/cli/workspaces.md and in
// skills/senro/references/cli.md, and changing either is a breaking change.
const wsUsage = `Usage:
  senro ws ls [--cache-dir DIR] [RUN] [NAME]
      List a run's workspaces with their digests and sizes. With a workspace
      name, list its files from the stored index, without downloading the
      body.

  senro ws pull [--cache-dir DIR] [--force] RUN NAME [DEST]
      Write a workspace's stored body out to DEST (default ./NAME), so the
      files a failed step left behind can be read with ordinary tools. RUN and
      NAME are both required here, unlike ws ls: with DEST optional too, a
      bare pair of arguments would be ambiguous. DEST is REPLACED, not merged
      into, so an existing DEST that has anything in it is refused unless
      --force is given.

  senro ws diff [--cache-dir DIR] [--json] RUN-A RUN-B [NAME]
      Compare two runs' workspaces and report what changed: files added,
      removed, rewritten, changed in mode alone, or replaced by a different
      kind of thing. Answered from the two stored indexes, so no body is
      downloaded on either side however large the workspaces are. With no
      NAME, every workspace both runs have; a workspace only one of them has
      is reported as such. Exits 0 whether or not there are differences,
      unlike diff(1): exit 1 means "the run failed" throughout this CLI.

A workspace whose most recent state in a run came from a cache hit (see cache
explain) rather than a fresh snapshot has no recorded index, since cache.Result
only ever stores a workspace's body digest. ws ls and ws diff report that
rather than erroring. ws pull is unaffected, because a body digest is the only
thing it ever needed.
`

// cmdWS implements `senro ws <subcommand>`.
func cmdWS(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		// The "senro ws:" prefix tells cmdWS's answer apart from the generic
		// `senro: unknown command`.
		_, _ = fmt.Fprintf(stderr, "senro ws: no subcommand given\n\n%s", wsUsage)
		return exitUsage
	}
	switch args[0] {
	case "ls":
		return cmdWSLs(args[1:], stdout, stderr)
	case "pull":
		return cmdWSPull(args[1:], stdout, stderr)
	case "diff":
		return cmdWSDiff(args[1:], stdout, stderr)
	default:
		_, _ = fmt.Fprintf(stderr, "senro ws: unknown subcommand %q\n\n%s", args[0], wsUsage)
		return exitUsage
	}
}

// cmdWSLs implements `senro ws ls`.
func cmdWSLs(args []string, stdout, stderr io.Writer) int {
	var cacheDir string
	var positional []string
	for i := 0; i < len(args); i++ {
		switch a := args[i]; {
		case a == "--cache-dir" && i+1 < len(args):
			cacheDir = args[i+1]
			i++
		default:
			positional = append(positional, a)
		}
	}

	var run, name string
	switch len(positional) {
	case 0:
	case 1:
		run = positional[0]
	case 2:
		run, name = positional[0], positional[1]
	default:
		_, _ = fmt.Fprintf(stderr, "senro ws ls: unexpected arguments %v\n\n%s", positional[2:], wsUsage)
		return exitUsage
	}

	dir, err := resolveRunDir(run)
	if err != nil {
		_, _ = fmt.Fprintln(stderr, "senro ws ls:", err)
		return exitUsage
	}
	snaps, err := latestSnapshots(dir)
	if err != nil {
		_, _ = fmt.Fprintln(stderr, "senro ws ls:", err)
		return exitUsage
	}
	if len(snaps) == 0 {
		_, _ = fmt.Fprintf(stdout, "no workspaces in %s\n", dir)
		return exitSuccess
	}

	names := sortedNames(snaps)

	if name == "" {
		for _, n := range names {
			if _, err := fmt.Fprintln(stdout, formatWSLine(n, snaps[n], run)); err != nil {
				return exitRunFailed
			}
		}
		return exitSuccess
	}

	s, ok := snaps[name]
	if !ok {
		_, _ = fmt.Fprintf(stderr, "senro ws ls: %s has no workspace %q (it has %v)\n", dir, name, names)
		return exitUsage
	}
	if s.Index == "" {
		// An old pre-index build or, commonly, a cache hit: serveFromCache
		// restores a workspace to a cached body digest and never
		// re-snapshots it, and cache.Result has no Index field, so nothing
		// ever learned an index digest. A usage error, not a run failure;
		// the file list cannot be produced without unpacking the body,
		// which `senro ws pull` does on request and this command will not
		// do behind your back.
		_, _ = fmt.Fprintf(stderr,
			"senro ws ls: workspace %q has no recorded file index in %s: its most recent state in this "+
				"run came from a cache hit (or an older build, before indexing), and neither carries one; "+
				"`senro ws pull %s %s DEST` works on it, since the body digest %s is all a pull needs\n",
			name, dir, runLabel(run, dir), name, cas.Digest(s.Digest).Short())
		return exitUsage
	}

	root, err := resolveCacheDir(cacheDir)
	if err != nil {
		_, _ = fmt.Fprintln(stderr, "senro ws ls:", err)
		return exitUsage
	}
	store, err := storage.Open(root)
	if err != nil {
		_, _ = fmt.Fprintln(stderr, "senro ws ls:", err)
		return exitUsage
	}
	defer func() { _ = store.Close() }()

	ix, err := store.Snapshotter.LoadIndex(context.Background(), cas.Digest(s.Index))
	if err != nil {
		if errors.Is(err, cas.ErrNotFound) {
			// Expected (see internal/cache/gc.go): only a failed run's
			// workspaces are pinned against a sweep, so an old successful
			// one's index can be gone. exitUsage, not exitRunFailed: this
			// run did not fail, its diagnostic data is simply gone.
			_, _ = fmt.Fprintf(stderr,
				"senro ws ls: the file index for workspace %q is gone from %s: most likely collected by a "+
					"'senro cache gc' sweep, since only a failed run's workspaces are protected against one; "+
					"the tarball's own digest is still %s\n",
				name, root, cas.Digest(s.Digest).Short())
			return exitUsage
		}
		_, _ = fmt.Fprintln(stderr, "senro ws ls:", err)
		return exitRunFailed
	}
	if len(ix.Entries) == 0 {
		_, _ = fmt.Fprintf(stdout, "workspace %q has no files\n", name)
		return exitSuccess
	}
	for _, e := range ix.Entries {
		suffix := ""
		if e.Link != "" {
			suffix = " -> " + e.Link
		}
		if _, err := fmt.Fprintf(stdout, "%04o  %10d  %-12s %s%s\n",
			e.Mode, e.Size, cas.Digest(e.Digest).Short(), e.Path, suffix); err != nil {
			return exitRunFailed
		}
	}
	return exitSuccess
}

// cmdWSPull implements `senro ws pull`, the one ws command that writes to a
// path a person names. It refuses a non-empty destination unless --force (a
// pull REPLACES; a merged tree would not hash to the digest it was pulled
// from), and it never writes outside the destination: a workspace tarball
// is content from another run, or eventually another machine, and
// workspace.ReadTar refuses "../escaped" entries before any byte is
// written. Extraction stages beside the destination and moves into place
// only once verified (workspace.Snapshotter.RestoreTree), so a refusal
// leaves neither a half-populated destination nor a stray staging dir.
func cmdWSPull(args []string, stdout, stderr io.Writer) int {
	var cacheDir string
	var force bool
	var positional []string
	for i := 0; i < len(args); i++ {
		switch a := args[i]; {
		case a == "--cache-dir" && i+1 < len(args):
			cacheDir = args[i+1]
			i++
		case a == "--force":
			force = true
		case strings.HasPrefix(a, "--"):
			_, _ = fmt.Fprintf(stderr, "senro ws pull: unknown flag %q\n\n%s", a, wsUsage)
			return exitUsage
		default:
			positional = append(positional, a)
		}
	}
	if len(positional) < 2 || len(positional) > 3 {
		_, _ = fmt.Fprintf(stderr,
			"senro ws pull: want RUN NAME [DEST], got %d argument(s) %v\n\n%s",
			len(positional), positional, wsUsage)
		return exitUsage
	}
	run, name := positional[0], positional[1]
	dest := name
	if len(positional) == 3 {
		dest = positional[2]
	}

	dir, err := resolveRunDir(run)
	if err != nil {
		_, _ = fmt.Fprintln(stderr, "senro ws pull:", err)
		return exitUsage
	}
	snaps, err := latestSnapshots(dir)
	if err != nil {
		_, _ = fmt.Fprintln(stderr, "senro ws pull:", err)
		return exitUsage
	}
	s, ok := snaps[name]
	if !ok {
		_, _ = fmt.Fprintf(stderr, "senro ws pull: %s has no workspace %q (it has %v)\n",
			dir, name, sortedNames(snaps))
		return exitUsage
	}
	// No s.Index check here, deliberately: a pull needs the BODY, whose
	// digest is recorded for a cache-restored workspace exactly as for a
	// fresh one. The file count printed below therefore comes from the
	// extraction, not the ledger.
	digest := cas.Digest(s.Digest)
	if !digest.Valid() {
		_, _ = fmt.Fprintf(stderr,
			"senro ws pull: workspace %q in %s records %q, which is not a content digest\n",
			name, dir, s.Digest)
		return exitUsage
	}

	absDest, err := filepath.Abs(dest)
	if err != nil {
		_, _ = fmt.Fprintln(stderr, "senro ws pull:", err)
		return exitUsage
	}
	if code := checkPullDest(absDest, force, stderr); code != exitSuccess {
		return code
	}

	root, err := resolveCacheDir(cacheDir)
	if err != nil {
		_, _ = fmt.Fprintln(stderr, "senro ws pull:", err)
		return exitUsage
	}
	store, err := storage.Open(root)
	if err != nil {
		_, _ = fmt.Fprintln(stderr, "senro ws pull:", err)
		return exitUsage
	}
	defer func() { _ = store.Close() }()

	ix, err := store.Snapshotter.RestoreTree(context.Background(), digest, absDest)
	if err != nil {
		switch {
		case errors.Is(err, cas.ErrNotFound):
			// The same swept-object condition ws ls reports, one object
			// over.
			_, _ = fmt.Fprintf(stderr,
				"senro ws pull: the body of workspace %q is gone from %s: most likely collected by a "+
					"'senro cache gc' sweep, since only a failed run's workspaces are protected against one; "+
					"its digest was %s\n", name, root, digest)
			return exitUsage
		case errors.Is(err, workspace.ErrUnsafePath):
			// Not a usage error: the stored object is what is wrong, and
			// nothing was written.
			_, _ = fmt.Fprintf(stderr,
				"senro ws pull: refusing to unpack workspace %q: %v. Nothing was written to %s. "+
					"A snapshot senro produced cannot contain such an entry, so this body was "+
					"assembled or altered by something else\n", name, err, absDest)
			return exitRunFailed
		default:
			_, _ = fmt.Fprintln(stderr, "senro ws pull:", err)
			return exitRunFailed
		}
	}

	var b bytes.Buffer
	fmt.Fprintf(&b, "pulled workspace %q from %s into %s\n", name, dir, absDest)
	fmt.Fprintf(&b, "  body      %s\n", digest)
	fmt.Fprintf(&b, "  restored  %d entries, %s\n", len(ix.Entries), humanBytes(ix.Bytes()))
	b.WriteString(wsPullNormalizationNote)
	if _, err := stdout.Write(b.Bytes()); err != nil {
		return exitRunFailed
	}
	return exitSuccess
}

// wsPullNormalizationNote is printed by every successful pull: the first
// thing someone does with a pulled workspace is `ls -l` it and conclude
// senro mangled their permissions. It did not; a snapshot never recorded
// them. See workspace.WriteTar for why dropping them is the cache's
// correctness condition.
const wsPullNormalizationNote = "  modes     0644 or 0755 for files, 0755 for directories, 0777 for symlinks: " +
	"a snapshot carries the executable bit and nothing else\n" +
	"  mtimes    1970-01-01T00:00:00Z on every restored file and directory, fixed so a digest cannot " +
	"depend on when a compiler ran\n" +
	"  dropped   uid, gid, extended attributes, ACLs, hard links, devices, sockets and fifos are not stored " +
	"by a snapshot at all, so they are not restored\n"

// pullReplacesDest is why a pull will not merge into a directory that already
// holds something. See checkReplaceableDest.
const pullReplacesDest = "A pull REPLACES its destination rather than merging into it, since a " +
	"merged tree would not be the snapshot it claims to be"

// checkPullDest decides whether absDest may be written to. It reports
// exitSuccess when the pull may proceed.
func checkPullDest(absDest string, force bool, stderr io.Writer) int {
	return checkReplaceableDest("senro ws pull", absDest, force, pullReplacesDest, stderr)
}

// checkReplaceableDest is where the "refuse to overwrite" rule lives, for
// every command that writes into a directory somebody named. One function,
// because the rule is a promise about this CLI, not any one subcommand: a
// destination holding anything is refused unless --force, and one that is
// not a directory is refused even with it. replaces is the one clause that
// legitimately differs: WHY merging would produce something wrong.
func checkReplaceableDest(cmd, absDest string, force bool, replaces string, stderr io.Writer) int {
	fi, err := os.Stat(absDest)
	switch {
	case errors.Is(err, fs.ErrNotExist):
		return exitSuccess
	case err != nil:
		_, _ = fmt.Fprintln(stderr, cmd+":", err)
		return exitUsage
	case !fi.IsDir():
		// Refused even with --force: replacing a file with a directory is
		// not something --force can reasonably be read as authorising.
		_, _ = fmt.Fprintf(stderr,
			"%s: %s exists and is not a directory; name a directory, or a path that does not exist yet\n",
			cmd, absDest)
		return exitUsage
	}
	entries, err := os.ReadDir(absDest)
	if err != nil {
		_, _ = fmt.Fprintln(stderr, cmd+":", err)
		return exitUsage
	}
	if len(entries) == 0 || force {
		return exitSuccess
	}
	_, _ = fmt.Fprintf(stderr,
		"%s: %s already has %d entries in it. %s; pass --force to replace what is there, or name a "+
			"destination that is empty or does not exist\n",
		cmd, absDest, len(entries), replaces)
	return exitUsage
}

// ---------------------------------------------------------------------------
// senro ws diff
// ---------------------------------------------------------------------------

// wsDiffMarkers is the legend printed above a text diff. The markers are not
// diff(1)'s, because this compares trees rather than lines, and three of the
// five differences it reports have no diff(1) equivalent at all.
const wsDiffMarkers = "+ added   - removed   M content changed   P mode changed   K kind changed"

// cmdWSDiff implements `senro ws diff`. It answers from the two stored
// INDEXES and never opens a body: all five change kinds are decidable from
// what an index records, which is the whole reason the index is a separate
// CAS object from the tarball. What it cannot tell you is what changed
// inside a file; `senro ws pull` each side for that.
func cmdWSDiff(args []string, stdout, stderr io.Writer) int {
	var cacheDir string
	var asJSON bool
	var positional []string
	for i := 0; i < len(args); i++ {
		switch a := args[i]; {
		case a == "--cache-dir" && i+1 < len(args):
			cacheDir = args[i+1]
			i++
		case a == "--json":
			asJSON = true
		case strings.HasPrefix(a, "--"):
			_, _ = fmt.Fprintf(stderr, "senro ws diff: unknown flag %q\n\n%s", a, wsUsage)
			return exitUsage
		default:
			positional = append(positional, a)
		}
	}
	if len(positional) < 2 || len(positional) > 3 {
		_, _ = fmt.Fprintf(stderr,
			"senro ws diff: want RUN-A RUN-B [NAME], got %d argument(s) %v\n\n%s",
			len(positional), positional, wsUsage)
		return exitUsage
	}
	runA, runB := positional[0], positional[1]
	name := ""
	if len(positional) == 3 {
		name = positional[2]
	}

	sideA, code := loadDiffSide(runA, stderr)
	if code != exitSuccess {
		return code
	}
	sideB, code := loadDiffSide(runB, stderr)
	if code != exitSuccess {
		return code
	}

	names, onlyA, onlyB, code := selectDiffNames(name, sideA, sideB, stderr)
	if code != exitSuccess {
		return code
	}

	root, err := resolveCacheDir(cacheDir)
	if err != nil {
		_, _ = fmt.Fprintln(stderr, "senro ws diff:", err)
		return exitUsage
	}
	store, err := storage.Open(root)
	if err != nil {
		_, _ = fmt.Fprintln(stderr, "senro ws diff:", err)
		return exitUsage
	}
	defer func() { _ = store.Close() }()

	report := wsDiffReport{Workspaces: []wsDiffWorkspace{}}
	// incomplete records that at least one workspace could not be compared;
	// the others are still reported, but the exit code says the answer is
	// partial.
	incomplete := false
	for _, n := range names {
		w, ok := diffOneWorkspace(store, root, n, runA, runB, sideA, sideB)
		if !ok {
			incomplete = true
		}
		report.Workspaces = append(report.Workspaces, w)
	}
	for _, n := range onlyA {
		report.Workspaces = append(report.Workspaces, wsDiffWorkspace{
			Name: n, Changes: []wsDiffChange{},
			A:    &wsDiffSide{Run: runA, Dir: sideA.dir, Digest: sideA.snaps[n].Digest, Index: sideA.snaps[n].Index},
			Note: fmt.Sprintf("only %s has this workspace, so there is nothing to compare it against", runA),
		})
	}
	for _, n := range onlyB {
		report.Workspaces = append(report.Workspaces, wsDiffWorkspace{
			Name: n, Changes: []wsDiffChange{},
			B:    &wsDiffSide{Run: runB, Dir: sideB.dir, Digest: sideB.snaps[n].Digest, Index: sideB.snaps[n].Index},
			Note: fmt.Sprintf("only %s has this workspace, so there is nothing to compare it against", runB),
		})
	}

	// A workspace that could not be compared goes to stderr, never into the
	// text report. The JSON document carries it too (the "error" field), so
	// a consumer reading stdout alone can tell "no differences" from "no
	// answer".
	for _, w := range report.Workspaces {
		if w.Error != "" {
			_, _ = fmt.Fprintf(stderr, "senro ws diff: %s\n", w.Error)
		}
	}
	if code := writeDiffReport(report, asJSON, stdout, stderr); code != exitSuccess {
		return code
	}
	if incomplete {
		return exitUsage
	}
	return exitSuccess
}

// diffSide is one run's half of a diff: where it is and what it recorded.
type diffSide struct {
	dir   string
	snaps map[string]api.WSSnapshotBody
}

func loadDiffSide(run string, stderr io.Writer) (diffSide, int) {
	dir, err := resolveRunDir(run)
	if err != nil {
		_, _ = fmt.Fprintln(stderr, "senro ws diff:", err)
		return diffSide{}, exitUsage
	}
	snaps, err := latestSnapshots(dir)
	if err != nil {
		_, _ = fmt.Fprintln(stderr, "senro ws diff:", err)
		return diffSide{}, exitUsage
	}
	return diffSide{dir: dir, snaps: snaps}, exitSuccess
}

// selectDiffNames decides which workspaces this invocation compares. Named
// explicitly, a workspace missing from either side is refused, naming what
// each run does have. Unnamed, the answer is every workspace both runs
// have, one-sided ones reported rather than dropped; only "none in common"
// is refused, since there is then no comparison to print.
func selectDiffNames(name string, a, b diffSide, stderr io.Writer) (both, onlyA, onlyB []string, code int) {
	namesA, namesB := sortedNames(a.snaps), sortedNames(b.snaps)
	if name != "" {
		_, inA := a.snaps[name]
		_, inB := b.snaps[name]
		if !inA || !inB {
			_, _ = fmt.Fprintf(stderr,
				"senro ws diff: workspace %q is not in both runs: %s has %v, %s has %v\n",
				name, a.dir, namesA, b.dir, namesB)
			return nil, nil, nil, exitUsage
		}
		return []string{name}, nil, nil, exitSuccess
	}
	for _, n := range namesA {
		if _, ok := b.snaps[n]; ok {
			both = append(both, n)
		} else {
			onlyA = append(onlyA, n)
		}
	}
	for _, n := range namesB {
		if _, ok := a.snaps[n]; !ok {
			onlyB = append(onlyB, n)
		}
	}
	if len(both) == 0 {
		_, _ = fmt.Fprintf(stderr,
			"senro ws diff: these two runs have no workspace in common, so there is nothing to compare: "+
				"%s has %v, %s has %v\n", a.dir, namesA, b.dir, namesB)
		return nil, nil, nil, exitUsage
	}
	return both, onlyA, onlyB, exitSuccess
}

// diffOneWorkspace compares one name across both runs. The bool reports
// whether an actual comparison was produced; false means the returned
// workspace carries an Error explaining why it could not be.
func diffOneWorkspace(store *storage.Storage, root, name, runA, runB string, a, b diffSide) (wsDiffWorkspace, bool) {
	sa, sb := a.snaps[name], b.snaps[name]
	w := wsDiffWorkspace{
		Name:    name,
		A:       &wsDiffSide{Run: runA, Dir: a.dir, Digest: sa.Digest, Index: sa.Index},
		B:       &wsDiffSide{Run: runB, Dir: b.dir, Digest: sb.Digest, Index: sb.Index},
		Changes: []wsDiffChange{},
	}
	if sa.Digest != "" && sa.Digest == sb.Digest {
		// Answered without touching the store: two snapshots with the same
		// content address ARE the same tree.
		w.Identical = true
		w.Summary = &wsDiffSummary{Unchanged: sa.Files}
		return w, true
	}

	ixA, ok := loadDiffIndex(store, root, name, runA, a, &w)
	if !ok {
		return w, false
	}
	ixB, ok := loadDiffIndex(store, root, name, runB, b, &w)
	if !ok {
		return w, false
	}

	changes := workspace.Diff(ixA, ixB)
	paths := make(map[string]struct{}, len(ixA.Entries)+len(ixB.Entries))
	for _, e := range ixA.Entries {
		paths[e.Path] = struct{}{}
	}
	for _, e := range ixB.Entries {
		paths[e.Path] = struct{}{}
	}
	sum := wsDiffSummary{Unchanged: len(paths) - len(changes)}
	for _, c := range changes {
		switch c.Status {
		case workspace.Added:
			sum.Added++
		case workspace.Removed:
			sum.Removed++
		case workspace.Modified:
			sum.Modified++
		case workspace.ModeChanged:
			sum.Mode++
		case workspace.KindChanged:
			sum.Kind++
		}
		w.Changes = append(w.Changes, newWSDiffChange(c))
	}
	w.Summary = &sum
	w.Identical = len(changes) == 0
	return w, true
}

// loadDiffIndex fetches one side's index, writing the reason into w and
// returning false when it cannot.
func loadDiffIndex(store *storage.Storage, root, name, run string, side diffSide, w *wsDiffWorkspace) (workspace.Index, bool) {
	s := side.snaps[name]
	if s.Index == "" {
		// The cache-hit case. Unlike ws pull, this command genuinely cannot
		// proceed: a body digest says two snapshots differ but not how, and
		// not downloading them is the entire point of the command.
		w.Error = fmt.Sprintf(
			"workspace %q has no recorded file index in %s: its most recent state in that run came from a "+
				"cache hit (or an older build, before indexing), and neither carries one. Its body digest %s "+
				"says whether the two snapshots differ but not how. `senro ws pull %s %s DEST` works here, "+
				"since a pull needs only the body; pull both sides and compare the trees",
			name, side.dir, cas.Digest(s.Digest).Short(), run, name)
		return workspace.Index{}, false
	}
	ix, err := store.Snapshotter.LoadIndex(context.Background(), cas.Digest(s.Index))
	if err != nil {
		if errors.Is(err, cas.ErrNotFound) {
			w.Error = fmt.Sprintf(
				"the file index for workspace %q in %s is gone from %s: most likely collected by a "+
					"'senro cache gc' sweep, since only a failed run's workspaces are protected against one; "+
					"the tarball's own digest is still %s",
				name, side.dir, root, cas.Digest(s.Digest).Short())
			return workspace.Index{}, false
		}
		w.Error = fmt.Sprintf("read the file index for workspace %q in %s: %v", name, side.dir, err)
		return workspace.Index{}, false
	}
	return ix, true
}

// ---------------------------------------------------------------------------
// senro ws diff: the reported shape
// ---------------------------------------------------------------------------

// The types below are `senro ws diff --json`'s wire contract: field names
// and workspace.Status strings are what a script reads, so both are
// additive-only. A mode is an OCTAL STRING ("0644") rather than the decimal
// workspace.Entry's JSON uses, because 420 is not a thing anyone
// recognises.
type wsDiffReport struct {
	Workspaces []wsDiffWorkspace `json:"workspaces"`
}

type wsDiffWorkspace struct {
	Name string      `json:"name"`
	A    *wsDiffSide `json:"a,omitempty"`
	B    *wsDiffSide `json:"b,omitempty"`
	// Identical is true when the two snapshots have the same content, which
	// is decided from the body digests alone when both are known.
	Identical bool           `json:"identical"`
	Changes   []wsDiffChange `json:"changes"`
	Summary   *wsDiffSummary `json:"summary,omitempty"`
	// Note is a remark about a workspace not compared for a reason that is
	// itself an answer (today only "one run has it"). It does not affect
	// the exit code and prints on stdout with the report.
	Note string `json:"note,omitempty"`
	// Error is why this workspace could not be compared at all; any makes
	// the command exit 2. Printed on stderr, never in the text report.
	Error string `json:"error,omitempty"`
}

type wsDiffSide struct {
	Run    string `json:"run"`
	Dir    string `json:"dir"`
	Digest string `json:"digest,omitempty"`
	Index  string `json:"index,omitempty"`
}

type wsDiffChange struct {
	Path   string       `json:"path"`
	Status string       `json:"status"`
	A      *wsDiffEntry `json:"a,omitempty"`
	B      *wsDiffEntry `json:"b,omitempty"`
}

type wsDiffEntry struct {
	Kind   string `json:"kind"`
	Mode   string `json:"mode"`
	Size   int64  `json:"size"`
	Digest string `json:"digest,omitempty"`
	Link   string `json:"link,omitempty"`
}

type wsDiffSummary struct {
	Added     int `json:"added"`
	Removed   int `json:"removed"`
	Modified  int `json:"modified"`
	Mode      int `json:"mode"`
	Kind      int `json:"kind"`
	Unchanged int `json:"unchanged"`
}

func newWSDiffChange(c workspace.Change) wsDiffChange {
	out := wsDiffChange{Path: c.Path, Status: string(c.Status)}
	if c.A.Path != "" {
		out.A = newWSDiffEntry(c.A)
	}
	if c.B.Path != "" {
		out.B = newWSDiffEntry(c.B)
	}
	return out
}

func newWSDiffEntry(e workspace.Entry) *wsDiffEntry {
	return &wsDiffEntry{
		Kind:   string(e.Kind()),
		Mode:   fmt.Sprintf("%04o", e.Mode),
		Size:   e.Size,
		Digest: string(e.Digest),
		Link:   e.Link,
	}
}

// writeDiffReport renders the report, JSON or text, in one write so a
// partial document can never reach a consumer that is parsing it.
func writeDiffReport(r wsDiffReport, asJSON bool, stdout, stderr io.Writer) int {
	var b bytes.Buffer
	if asJSON {
		enc := json.NewEncoder(&b)
		enc.SetEscapeHTML(false)
		enc.SetIndent("", "  ")
		if err := enc.Encode(r); err != nil {
			_, _ = fmt.Fprintln(stderr, "senro ws diff:", err)
			return exitRunFailed
		}
	} else {
		formatDiffReport(&b, r)
	}
	if _, err := stdout.Write(b.Bytes()); err != nil {
		return exitRunFailed
	}
	return exitSuccess
}

func formatDiffReport(b *bytes.Buffer, r wsDiffReport) {
	anyChanges := false
	for _, w := range r.Workspaces {
		if w.Error == "" && len(w.Changes) > 0 {
			anyChanges = true
		}
	}
	// Only when there is something to read it against: a legend above a
	// report that says "identical" is noise.
	if anyChanges {
		fmt.Fprintf(b, "%s\n\n", wsDiffMarkers)
	}
	first := true
	for _, w := range r.Workspaces {
		if w.Error != "" {
			continue
		}
		if !first {
			b.WriteByte('\n')
		}
		first = false
		formatDiffWorkspace(b, w)
	}
}

func formatDiffWorkspace(b *bytes.Buffer, w wsDiffWorkspace) {
	if w.Note != "" {
		fmt.Fprintf(b, "workspace %q: %s\n", w.Name, w.Note)
		return
	}
	// The one-liner is reserved for the case the digests themselves settle:
	// a body digest that disagrees while the indexes agree means something
	// is off, and flattening that into "identical" would hide it.
	if w.Identical && w.A != nil && w.B != nil && w.A.Digest == w.B.Digest {
		fmt.Fprintf(b, "workspace %q: identical in both runs (%s)\n", w.Name, w.A.Digest)
		return
	}

	fmt.Fprintf(b, "workspace %q\n", w.Name)
	if w.A != nil {
		fmt.Fprintf(b, "  a  %-10s %s  %s\n", w.A.Run, relToCwd(w.A.Dir), w.A.Digest)
	}
	if w.B != nil {
		fmt.Fprintf(b, "  b  %-10s %s  %s\n", w.B.Run, relToCwd(w.B.Dir), w.B.Digest)
	}

	width := 0
	for _, c := range w.Changes {
		if len(c.Path) > width {
			width = len(c.Path)
		}
	}
	// Capped so one deeply nested path does not push every detail column off
	// the right edge for all the others.
	if width > 48 {
		width = 48
	}
	if len(w.Changes) == 0 {
		b.WriteString("  no differences in the stored file indexes, even though the two bodies " +
			"have different digests\n")
	}
	for _, c := range w.Changes {
		fmt.Fprintf(b, "  %s %-*s  %s\n", diffMarker(c.Status), width, c.Path, describeChange(c))
	}
	if w.Summary != nil {
		s := *w.Summary
		fmt.Fprintf(b, "  %d added, %d removed, %d modified, %d mode, %d kind, %d unchanged\n",
			s.Added, s.Removed, s.Modified, s.Mode, s.Kind, s.Unchanged)
	}
}

func diffMarker(status string) string {
	switch workspace.Status(status) {
	case workspace.Added:
		return "+"
	case workspace.Removed:
		return "-"
	case workspace.Modified:
		return "M"
	case workspace.ModeChanged:
		return "P"
	case workspace.KindChanged:
		return "K"
	default:
		return "?"
	}
}

// describeChange is the detail column: what actually moved, on one line, so
// the common questions (how big is it, did it get bigger, which digest do I
// paste somewhere) are answered without a second command.
func describeChange(c wsDiffChange) string {
	switch workspace.Status(c.Status) {
	case workspace.Added:
		return describeEntry(c.B)
	case workspace.Removed:
		return describeEntry(c.A)
	case workspace.Modified:
		if c.A != nil && c.B != nil && c.A.Kind == string(workspace.KindSymlink) {
			return fmt.Sprintf("symlink target %s -> %s", c.A.Link, c.B.Link)
		}
		if c.A == nil || c.B == nil {
			return ""
		}
		return fmt.Sprintf("%s -> %s  %s -> %s",
			humanBytes(c.A.Size), humanBytes(c.B.Size),
			cas.Digest(c.A.Digest).Short(), cas.Digest(c.B.Digest).Short())
	case workspace.ModeChanged:
		if c.A == nil || c.B == nil {
			return ""
		}
		return fmt.Sprintf("%s -> %s", c.A.Mode, c.B.Mode)
	case workspace.KindChanged:
		if c.A == nil || c.B == nil {
			return ""
		}
		return fmt.Sprintf("%s -> %s", c.A.Kind, c.B.Kind)
	default:
		return ""
	}
}

func describeEntry(e *wsDiffEntry) string {
	if e == nil {
		return ""
	}
	switch workspace.Kind(e.Kind) {
	case workspace.KindDir:
		return "dir"
	case workspace.KindSymlink:
		return "symlink -> " + e.Link
	default:
		return fmt.Sprintf("%s  %s  %s", e.Mode, humanBytes(e.Size), cas.Digest(e.Digest).Short())
	}
}

// ---------------------------------------------------------------------------
// shared
// ---------------------------------------------------------------------------

// formatWSLine renders one workspace's line for `ws ls RUN`, pulled out so
// the size threshold has a unit test that needs no 2 GiB workspace. The
// digest is printed in full, unlike `cache explain`'s: it is the literal
// argument `senro shell --ws sha256:...` takes, so truncating it would
// print an address nobody can paste.
func formatWSLine(name string, s api.WSSnapshotBody, run string) string {
	if s.Index == "" {
		// A cache-hit restore or an old build (see cmdWSLs). Reported
		// plainly rather than as "0 files, 0 bytes", which would read as an
		// empty workspace instead of an unknown one.
		return fmt.Sprintf("%-20s %s  cached (no index recorded; 'senro ws pull %s %s DEST' still works)",
			name, s.Digest, run, name)
	}
	line := fmt.Sprintf("%-20s %s  %d files  %s", name, s.Digest, s.Files, humanBytes(s.Bytes))
	if s.Bytes > largeWorkspaceBytes {
		line += fmt.Sprintf("  LARGE (over %s; senro ws ls %s %s lists what is in it)",
			humanBytes(largeWorkspaceBytes), run, name)
	}
	return line
}

// relToCwd shortens an absolute run directory to a relative path when it is
// under the working directory. Purely cosmetic (an unshortened line wraps
// in an 80-column terminal); the absolute path is what --json carries and
// every error message prints.
func relToCwd(dir string) string {
	cwd, err := os.Getwd()
	if err != nil {
		return dir
	}
	rel, err := filepath.Rel(cwd, dir)
	if err != nil || rel == "." || strings.HasPrefix(rel, "..") {
		return dir
	}
	return rel
}

// runLabel is what to call a run inside a suggested command line: a bare
// `senro ws ls` has no run argument to echo back, and the resolved
// directory is a path `ws pull` also accepts.
func runLabel(run, dir string) string {
	if run != "" {
		return run
	}
	return dir
}

// sortedNames is the workspace names in a run, in a stable order.
func sortedNames(snaps map[string]api.WSSnapshotBody) []string {
	names := make([]string, 0, len(snaps))
	for n := range snaps {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}

// latestSnapshots folds a run's ledger down to the last known state per
// workspace. Two event types contribute, in chronological order:
// ws.snapshot (a workspace actually captured) and ws.restored (a Pure()
// cache hit put it back instead). Without the second, a workspace mounted
// only by a step that hits would have no event at all and silently vanish
// from `ws ls`. The later event wins for a given name.
//
// A FORCED ws.snapshot is skipped outright. It is a mid-run capture an
// operator asked for (api.OpWSSnapshot), never part of what the run
// produced: it entered no cache key and replaced no workspace's recorded
// state inside the engine, so letting it win here would make `ws ls`,
// `ws pull` and `ws diff` report a digest the run itself never used. Its
// digest is in the ledger for anyone who wants to pull it by hand.
func latestSnapshots(dir string) (map[string]api.WSSnapshotBody, error) {
	events, err := eventlog.Read(filepath.Join(dir, "events.jsonl"))
	if err != nil && len(events) == 0 {
		return nil, fmt.Errorf("read %s: %w", dir, err)
	}
	out := map[string]api.WSSnapshotBody{}
	for _, e := range events {
		switch e.Type {
		case api.WSSnapshot:
			var b api.WSSnapshotBody
			if err := e.Decode(&b); err != nil {
				continue
			}
			if b.Forced {
				continue
			}
			out[b.Name] = b
		case api.WSRestored:
			var b api.WSRestoredBody
			if err := e.Decode(&b); err != nil {
				continue
			}
			// Index, Bytes and Files are left zero: api.WSRestoredBody never
			// carries them, for the same reason cache.Result never does. See
			// cmdWSLs's s.Index == "" branch.
			out[b.Name] = api.WSSnapshotBody{Name: b.Name, Digest: b.Digest}
		}
	}
	return out, nil
}
