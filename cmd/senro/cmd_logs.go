package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/xavidop/senro"
	"github.com/xavidop/senro/internal/cas"
	"github.com/xavidop/senro/internal/oci"
	"github.com/xavidop/senro/internal/remotecache"
	"github.com/xavidop/senro/internal/s3"
	"github.com/xavidop/senro/internal/source"
)

// logsUsage documents every senro logs subcommand. A noun then a verb, as
// `cache` and `ws` already are; the noun is the run's archived record, which
// the shared cache uploads under runs/<id>/.
//
// The shape below is an interface: argument order and exit codes are
// documented here, in site/src/pages/docs/cli/workspaces.md and in
// skills/senro/references/cli.md, and changing either is a breaking change.
const logsUsage = `Usage:
  senro logs fetch [--force] RUN [DEST]
      Fetch a run archived in the shared cache back into a local run
      directory, so a run whose machine no longer exists can be read with
      the tools that read any other run. DEST defaults to ./runs/RUN,
      which is exactly where 'senro attach --run RUN' looks.

      Configured from the same environment as the shared cache itself:
      SENRO_REMOTE_CACHE, and then SENRO_REMOTE_CACHE_ENDPOINT,
      SENRO_REMOTE_CACHE_REGION, AWS_ACCESS_KEY_ID and
      AWS_SECRET_ACCESS_KEY for a bucket, or SENRO_REMOTE_CACHE_USERNAME
      and SENRO_REMOTE_CACHE_PASSWORD for a registry. What a fetch reads
      is what a run wrote, so it is pointed at the store the same way.
      Only reads are ever needed, and never a listing: s3:GetObject on a
      bucket, pull on a repository.

      DEST is REPLACED, not merged into, so an existing DEST that has
      anything in it is refused unless --force is given, exactly as
      'senro ws pull' refuses one.

Exit codes here follow the CLI's own contract, and 1 never means "the
archived run failed": this command's outcome describes the FETCH. A run that
is not in the store, a bucket that does not exist, and credentials the store
refuses are all 2, because no retry of the same command can change any of
them; a store that would not answer, or an archive whose bytes do not match
their digest, is 1. The archived run's own status is printed, and
'senro attach --run' is what turns it into an exit code.
`

// cmdLogs implements `senro logs <subcommand>`.
func cmdLogs(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		// The "senro logs:" prefix tells cmdLogs's answer apart from the
		// generic `senro: unknown command`.
		_, _ = fmt.Fprintf(stderr, "senro logs: no subcommand given\n\n%s", logsUsage)
		return exitUsage
	}
	switch args[0] {
	case "fetch":
		return cmdLogsFetch(args[1:], stdout, stderr)
	default:
		_, _ = fmt.Fprintf(stderr, "senro logs: unknown subcommand %q\n\n%s", args[0], logsUsage)
		return exitUsage
	}
}

// cmdLogsFetch implements `senro logs fetch`, the read half of log archival.
//
// What it produces is a RUN DIRECTORY, not a special archive format, so
// every reader senro already has (the plain renderer, `senro attach --run`,
// anything walking runs/<id>) reads what this writes, with no second
// implementation to disagree with. DEST defaults to ./runs/RUN because that
// is the one path `senro attach --run RUN` resolves on its own.
//
// The order of the three steps is forced: the ledger names the streams (see
// remotecache.StreamsFromLedger), so it is fetched and read first.
func cmdLogsFetch(args []string, stdout, stderr io.Writer) int {
	var force bool
	var positional []string
	for _, a := range args {
		switch {
		case a == "--force":
			force = true
		case strings.HasPrefix(a, "--"):
			_, _ = fmt.Fprintf(stderr, "senro logs fetch: unknown flag %q\n\n%s", a, logsUsage)
			return exitUsage
		default:
			positional = append(positional, a)
		}
	}
	if len(positional) < 1 || len(positional) > 2 {
		_, _ = fmt.Fprintf(stderr,
			"senro logs fetch: want RUN [DEST], got %d argument(s) %v\n\n%s",
			len(positional), positional, logsUsage)
		return exitUsage
	}
	runID := positional[0]
	if code := checkRunID(runID, stderr); code != exitSuccess {
		return code
	}

	dest := filepath.Join("runs", runID)
	if len(positional) == 2 {
		dest = positional[1]
	}
	absDest, err := filepath.Abs(dest)
	if err != nil {
		_, _ = fmt.Fprintln(stderr, "senro logs fetch:", err)
		return exitUsage
	}
	// Configuration first, then the destination, then the network. Not
	// about cost (both checks are free) but about which problem to report
	// when there are two: with no bucket configured there is nothing to put
	// anywhere, so being sent to clear a directory first would be being
	// sent to do the wrong thing.
	remote, code := openRemoteFromEnv(stderr)
	if remote == nil {
		return code
	}
	defer func() { _ = remote.Close() }()

	// Still before the network: a refusal after the download has already
	// created the directory it is about to complain about is worse than
	// useless.
	if code := checkReplaceableDest("senro logs fetch", absDest, force, fetchReplacesDest, stderr); code != exitSuccess {
		return code
	}

	ctx, stop, interrupted := attachSignalContext(context.Background())
	defer stop()

	// So a fetch that fails can put the filesystem back as it found it.
	// os.Remove refuses a non-empty directory, which is the condition
	// wanted: anything that did land stays.
	made := dirsCreatedBy(absDest)

	logs := remote.RunLogs()
	if err := logs.FetchLedger(ctx, runID, absDest); err != nil {
		// Only empty directories can exist here: the ledger is the first
		// object fetched, and fetchTo renames a verified temp file into
		// place. Removing them stops a later `senro attach --run` from
		// reporting a broken run where there is simply no run.
		for _, d := range made {
			_ = os.Remove(d)
		}
		return reportFetchFailure(stderr, remote, runID, logs.LedgerKey(runID), absDest, err, interrupted.Load(), false)
	}

	streams, err := remotecache.StreamsFromLedger(filepath.Join(absDest, "events.jsonl"))
	if err != nil {
		_, _ = fmt.Fprintf(stderr,
			"senro logs fetch: run %s came back from %s and its ledger cannot be read: %v. "+
				"%s holds the ledger as the archive has it, and nothing else was fetched; "+
				"the bytes matched the digest the archive recorded, so this is what the run wrote\n",
			runID, remote, err, absDest)
		return exitRunFailed
	}

	missing, err := logs.FetchStreams(ctx, runID, absDest, streams)
	if err != nil {
		return reportFetchFailure(stderr, remote, runID, "", absDest, err, interrupted.Load(), true)
	}

	return reportFetched(stdout, stderr, remote, runID, absDest, len(streams), missing)
}

// fetchReplacesDest is why a fetch will not merge into a directory that
// already holds something: it would produce a run directory whose ledger
// describes one run and whose logs are two runs' worth.
const fetchReplacesDest = "A fetch REPLACES its destination rather than merging into it, since a " +
	"directory holding one run's ledger and another run's logs is a record of neither"

// checkRunID refuses an argument that is a path rather than a run ID. Easy
// to confuse, because every other run-taking command accepts both; this one
// cannot, since the argument names a key in a bucket. Caught by the store
// instead, it would be indistinguishable from a run never archived.
func checkRunID(runID string, stderr io.Writer) int {
	if runID == "" {
		_, _ = fmt.Fprintf(stderr, "senro logs fetch: RUN is empty\n\n%s", logsUsage)
		return exitUsage
	}
	if strings.ContainsRune(runID, os.PathSeparator) || runID == "." || runID == ".." {
		_, _ = fmt.Fprintf(stderr,
			"senro logs fetch: %q is a path, and this takes the run ID it was archived under "+
				"(the last segment: `senro logs fetch %s`). Unlike the other run-taking commands, "+
				"this one is not reading a directory on this machine; there is nothing here to read yet\n",
			runID, filepath.Base(runID))
		return exitUsage
	}
	return exitSuccess
}

// openRemoteFromEnv opens the object store from the same environment a run
// reads. A nil Remote means it could not, and the int is the exit code.
// "Nothing is configured" and "half of it is configured" get deliberately
// different messages: the second names only the missing variable.
func openRemoteFromEnv(stderr io.Writer) (*remotecache.Remote, int) {
	rc, ok, err := senro.RemoteCacheFromEnv()
	if err != nil {
		// RemoteCacheFromEnv's errors already name the variable, and are
		// the same ones a run would fail to start with; restating them here
		// would be two spellings of one condition.
		_, _ = fmt.Fprintf(stderr, "senro logs fetch: %v\n", err)
		return nil, exitUsage
	}
	if !ok {
		_, _ = fmt.Fprintf(stderr,
			"senro logs fetch: no shared cache is configured, so there is nowhere to fetch an "+
				"archived run from. A run's logs are archived only into a store it was told "+
				"about, and this command reads the same environment. For a bucket:\n"+
				"  export %s=s3://<bucket>          # or s3://<bucket>/<prefix>\n"+
				"  export %s=https://s3.<region>.amazonaws.com\n"+
				"  export %s=<region>\n"+
				"  export %s=...\n"+
				"  export %s=...\n"+
				"or for an OCI registry:\n"+
				"  export %s=oci://<registry>/<repository>\n"+
				"  export %s=...\n"+
				"  export %s=...\n"+
				"Point them at the store the run that produced these logs used. "+
				"See https://senro.dev/docs/shared-cache/\n",
			senro.EnvRemoteCache, senro.EnvRemoteCacheEndpoint, senro.EnvRemoteCacheRegion,
			"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY",
			senro.EnvRemoteCache, senro.EnvRemoteCacheUsername, senro.EnvRemoteCachePassword)
		return nil, exitUsage
	}

	// A run treats a store that stops answering as a degradation and
	// carries on; here the fetch IS the point, and there is nothing to
	// carry on with. So the degradation channel is silenced in both
	// branches and this command reports the failure with an exit code.
	var remote *remotecache.Remote
	var err2 error
	if rc.Registry.Host != "" {
		remote, err2 = remotecache.OpenOCI(remotecache.OCIConfig{
			Registry:     rc.Registry.Host,
			Repository:   rc.Registry.Repository,
			Username:     rc.Registry.Username,
			Password:     rc.Registry.Password,
			PlainHTTP:    rc.Registry.PlainHTTP,
			Timeout:      rc.Timeout,
			ReportWriter: io.Discard,
		})
	} else {
		remote, err2 = remotecache.Open(remotecache.Config{
			Endpoint:        rc.Endpoint,
			Region:          rc.Region,
			Bucket:          rc.Bucket,
			Prefix:          rc.Prefix,
			AccessKeyID:     rc.AccessKeyID,
			SecretAccessKey: rc.SecretAccessKey,
			SessionToken:    rc.SessionToken,
			PathStyle:       rc.PathStyle,
			Timeout:         rc.Timeout,
			ReportWriter:    io.Discard,
		})
	}
	if err2 != nil {
		_, _ = fmt.Fprintf(stderr, "senro logs fetch: %v\n", err2)
		return nil, exitUsage
	}
	return remote, exitSuccess
}

// reportFetchFailure turns one failure from the object store into a message
// and an exit code. The classes below are separate problems with separate
// answers: "the run is not in the bucket" is answered by checking a run ID
// and "your credentials are wrong" by checking a policy, so a message
// covering both would send half its readers to the wrong place.
//
// wrote says whether anything of the run is already on disk, which decides
// whether the last sentence of each message is true.
func reportFetchFailure(
	stderr io.Writer, remote *remotecache.Remote,
	runID, key, dest string, err error, interrupted, wrote bool,
) int {
	left := "Nothing was written to " + dest + "."
	if wrote {
		left = "The ledger, and whatever streams had already come back, are in " + dest +
			": it is readable, and incomplete."
	}

	switch {
	case interrupted:
		// The operator asked for this; not a failure of anything.
		_, _ = fmt.Fprintf(stderr, "senro logs fetch: interrupted. %s\n", left)
		return exitCancelled

	case errors.Is(err, cas.ErrNotFound):
		// The store answered authoritatively that it does not have this: an
		// answer, not a fault, and not "the run failed", which is what exit
		// 1 means in this CLI.
		lookedFor := ""
		if key != "" {
			// The key is the only thing that shows what was searched for:
			// the PREFIX in a bucket, and the TAG in a registry, which is a
			// hash of the run ID and otherwise invisible.
			lookedFor = fmt.Sprintf("  looked for  %s\n", key)
		}
		_, _ = fmt.Fprintf(stderr,
			"senro logs fetch: %s has no archived run %s. %s\n%s"+
				"A run is archived only if a shared cache was configured while it ran, and only into "+
				"the store that run used, so check the run ID, and check %s names the same store "+
				"(and prefix, or repository) that run wrote to. An expiry rule on the store expires "+
				"old runs too, in which case the record is gone rather than misplaced\n",
			remote, runID, left, lookedFor, senro.EnvRemoteCache)
		return exitUsage

	case errors.Is(err, s3.ErrDenied):
		// Also an answer and also not retryable, but a different thing to
		// go and look at.
		_, _ = fmt.Fprintf(stderr,
			"senro logs fetch: %s refused the request for run %s: the credentials this process is "+
				"using are not allowed to read it. %s\n"+
				"  the store said  %v\n"+
				"Check AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY are a credential for this bucket, "+
				"that AWS_SESSION_TOKEN is set and unexpired if they are temporary (an assumed role "+
				"in CI), and that the policy grants s3:GetObject on the prefix. Reading an archived "+
				"run needs nothing else: no listing, and no write\n",
			remote, runID, left, err)
		return exitUsage

	case errors.Is(err, oci.ErrDenied):
		// Separate from the bucket's refusal: the credentials to check are
		// different ones in different variables, and a message naming the
		// wrong pair is worse than none.
		_, _ = fmt.Fprintf(stderr,
			"senro logs fetch: %s refused the request for run %s: the credentials this process is "+
				"using are not allowed to read it. %s\n"+
				"  the registry said  %v\n"+
				"Check %s and %s are a credential for this repository, that it has not expired "+
				"(a forge's job token lasts only as long as the job that was issued it), and that "+
				"it grants pull on the repository. Reading an archived run needs nothing else: no "+
				"push, and no listing\n",
			remote, runID, left, err,
			senro.EnvRemoteCacheUsername, senro.EnvRemoteCachePassword)
		return exitUsage

	case isNoSuchBucket(err):
		_, _ = fmt.Fprintf(stderr,
			"senro logs fetch: %s does not exist. %s\n"+
				"  the store said  %v\n"+
				"%s names the bucket, and %s the endpoint it lives behind; a bucket that is really "+
				"there but addressed the wrong way answers like this too, which is what "+
				"%s exists to override\n",
			remote, left, err, senro.EnvRemoteCache, senro.EnvRemoteCacheEndpoint,
			senro.EnvRemoteCachePathStyle)
		return exitUsage

	case errors.Is(err, cas.ErrCorrupt):
		// Refused rather than written: handing over bytes that are not what
		// was uploaded, without saying so, is worse than handing over
		// nothing.
		_, _ = fmt.Fprintf(stderr,
			"senro logs fetch: run %s came back from %s as something other than what was archived, "+
				"so it was refused rather than written: %v. %s\n"+
				"Every object is checked against the digest the archive recorded for it, so this is a "+
				"truncated download, an object something else overwrote, or a proxy answering in the "+
				"store's place. Trying again is worth one attempt; a second identical failure means "+
				"the object in the bucket is the damaged one\n",
			runID, remote, err, left)
		return exitRunFailed

	default:
		// The store never formed an opinion: unreachable, timed out, 5xx,
		// TLS refused. Nothing visible is misconfigured and the same
		// command may work in a minute.
		_, _ = fmt.Fprintf(stderr,
			"senro logs fetch: could not read run %s from %s: %v. %s\n"+
				"The store did not answer. Check the endpoint is reachable from here and try again; "+
				"nothing about this run or this bucket has been shown to be wrong\n",
			runID, remote, err, left)
		return exitRunFailed
	}
}

// isNoSuchBucket reports whether the store said the BUCKET is not there: a
// 404 like a missing object, but every fetch against this configuration
// will fail the same way.
func isNoSuchBucket(err error) bool {
	var se *s3.Error
	return errors.As(err, &se) && se.Code == "NoSuchBucket"
}

// reportFetched prints what came back and, above all, the command that
// reads it: a person who has fetched a run and does not know how to open it
// has been helped halfway, so the next step is printed every time.
func reportFetched(
	stdout, stderr io.Writer, remote *remotecache.Remote,
	runID, dest string, asked int, missing []remotecache.StreamRef,
) int {
	// Opened with the reader `senro attach --run` itself uses: a real check
	// that what was written is an ordinary run directory, and where a
	// ledger that will not fold shows up before somebody finds it.
	src, err := source.OpenFile(dest, false)
	if err != nil {
		_, _ = fmt.Fprintf(stderr,
			"senro logs fetch: run %s was written to %s and cannot be opened as a run: %v\n",
			runID, dest, err)
		return exitRunFailed
	}
	defer func() { _ = src.Close() }()
	st, err := src.State(context.Background())
	if err != nil {
		_, _ = fmt.Fprintf(stderr,
			"senro logs fetch: run %s was written to %s and its ledger does not fold: %v\n",
			runID, dest, err)
		return exitRunFailed
	}

	files, bytesOnDisk := countLogFiles(dest)

	var b bytes.Buffer
	fmt.Fprintf(&b, "fetched run %s from %s into %s\n", runID, remote, dest)
	outcome := "run " + string(st.Run.Status)
	if st.Run.Status == "" {
		// No run.finished: killed, or archived while still going. Said out
		// loud, because the alternative is a blank where the verdict is.
		outcome = "no outcome recorded (the run did not finish, or its ledger was archived before it did)"
	}
	fmt.Fprintf(&b, "  ledger    %s, %s\n", plural(len(st.Steps), "step"), outcome)
	fmt.Fprintf(&b, "  logs      %d of %s, %s\n", files, plural(asked, "stream"), humanBytes(bytesOnDisk))
	if len(missing) > 0 {
		fmt.Fprintf(&b, "  missing   %s\n", describeMissing(missing))
	}
	b.WriteString("read it with\n")
	b.WriteString(readItWith(dest, runID))
	if _, err := stdout.Write(b.Bytes()); err != nil {
		return exitRunFailed
	}
	return exitSuccess
}

// plural renders a count with its noun, so a fetch of one step does not
// report "1 steps".
func plural(n int, noun string) string {
	if n == 1 {
		return "1 " + noun
	}
	return fmt.Sprintf("%d %ss", n, noun)
}

// dirsCreatedBy lists the directories a fetch to dest would have to create,
// deepest first, so a fetch that fails can remove exactly what it made and
// nothing else. A path that already exists is not in the list, and neither is
// anything above the first one that does.
func dirsCreatedBy(dest string) []string {
	var made []string
	for p := dest; ; p = filepath.Dir(p) {
		if _, err := os.Stat(p); err == nil {
			break
		}
		made = append(made, p)
		if parent := filepath.Dir(p); parent == p {
			break
		}
	}
	return made
}

// maxNamedMissing bounds how many absent streams are named one by one: a
// run whose whole archive expired would otherwise print a line per stream.
const maxNamedMissing = 3

func describeMissing(missing []remotecache.StreamRef) string {
	named := missing
	suffix := ""
	if len(named) > maxNamedMissing {
		named, suffix = named[:maxNamedMissing], fmt.Sprintf(" and %d more", len(missing)-maxNamedMissing)
	}
	parts := make([]string, 0, len(named))
	for _, s := range named {
		parts = append(parts, fmt.Sprintf("%s attempt %d %s", s.Step, s.Attempt, s.Stream))
	}
	verb := "are"
	if len(missing) == 1 {
		verb = "is"
	}
	return fmt.Sprintf("%s the ledger names %s not in the archive: %s%s "+
		"(a stream a step never wrote to has no file and was never uploaded; an upload that did "+
		"not finish before the run's machine went away, and one a lifecycle rule expired, look "+
		"the same from here)", plural(len(missing), "stream"), verb, strings.Join(parts, ", "), suffix)
}

// readItWith is the next command, worked out from where the run actually
// landed. `senro attach --run ID` resolves runs/ID under the working
// directory and nothing else (see discover.go's runDir), so a fixed line
// would be wrong: a DEST under another runs/ directory needs a cd, and
// anything else cannot be attached to by ID at all and is told so.
func readItWith(dest, runID string) string {
	if filepath.Base(dest) == runID && filepath.Base(filepath.Dir(dest)) == "runs" {
		parent := filepath.Dir(filepath.Dir(dest))
		if cwd, err := os.Getwd(); err == nil && sameDir(cwd, parent) {
			return fmt.Sprintf("  senro attach --run %s\n", runID)
		}
		return fmt.Sprintf("  cd %s && senro attach --run %s\n", parent, runID)
	}
	return fmt.Sprintf(
		"  senro attach --run %s reads runs/%s under the working directory, and this went to %s "+
			"instead, so move it there or fetch it again with no DEST.\n"+
			"  Either way the files are ordinary: %s is the run's ledger, and\n"+
			"  %s is each step's output.\n",
		runID, runID, dest,
		filepath.Join(dest, "events.jsonl"),
		filepath.Join(dest, "logs", "<step>", "<attempt>", "<stream>"))
}

// sameDir compares two directory paths through the filesystem, not as
// strings: /var and /private/var are the same directory on macOS, and a
// symlinked working directory would otherwise print a needless cd.
func sameDir(a, b string) bool {
	if a == b {
		return true
	}
	fa, err := os.Stat(a)
	if err != nil {
		return false
	}
	fb, err := os.Stat(b)
	if err != nil {
		return false
	}
	return os.SameFile(fa, fb)
}

// countLogFiles reports how many stream files a fetch actually produced and
// their size, by walking what is on disk rather than counting what was
// asked for: the two differ whenever the archive is missing something.
func countLogFiles(dest string) (files int, size int64) {
	_ = filepath.WalkDir(filepath.Join(dest, "logs"), func(_ string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			// A run with no logs has no logs directory, which is not an
			// error and reports as zero.
			return nil //nolint:nilerr // a walk error means one entry is uncountable, not that the fetch failed
		}
		if info, err := d.Info(); err == nil {
			files++
			size += info.Size()
		}
		return nil
	})
	return files, size
}
