package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"sort"
	"strings"
	"syscall"

	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/internal/cas"
	"github.com/xavidop/senro/internal/eventlog"
	"github.com/xavidop/senro/internal/plan"
	"github.com/xavidop/senro/internal/storage"
	"github.com/xavidop/senro/internal/workspace"
)

// The offline shell is `senro shell` on a run with no engine behind it: a
// finished run, or one somebody asked for with --offline. There is nothing
// to host a session, so the CLI does what `senro ws pull` does for every
// workspace the step snapshotted, into one fresh temporary directory laid
// out at the step's own mount paths, and runs a shell there.
//
// It is the coordinator's own shell over restored files. The step's image,
// executor and host are not involved, so a tool the step had and this
// machine lacks is simply missing; what this answers is "what did the step
// leave behind", not "what was it like to stand in its sandbox".
//
// The directory is the operator's to edit: nothing is written back, read-only
// mounts are restored writable like any other, and the whole tree is removed
// on exit unless --keep. Secrets are never delivered, by the same rule a
// live session follows (plan.ShellEnv).

// cmdShellOffline is the offline form of cmdShell, from the --run value
// onward. fellBack reports that --run named no live run and --offline was
// not given, which changes only how a missing run is described.
func cmdShellOffline(runFlag, step, cacheDir string, keep, fellBack bool, cmd []string,
	stdin io.Reader, stdout, stderr io.Writer) int {
	runDir, err := resolveRunDir(runFlag)
	if err != nil {
		if fellBack {
			_, _ = fmt.Fprintf(stderr, "senro shell: no live run named %q, and no recorded one either: %v\n",
				runFlag, err)
		} else {
			_, _ = fmt.Fprintln(stderr, "senro shell:", err)
		}
		return exitUsage
	}
	p, err := readRunPlan(runDir)
	if err != nil {
		_, _ = fmt.Fprintln(stderr, "senro shell:", err)
		return exitUsage
	}
	n, ok := p.Node(step)
	if !ok {
		_, _ = fmt.Fprintf(stderr, "senro shell: run %s has no step %q\n", runDir, step)
		return exitUsage
	}
	snaps, err := stepSnapshots(runDir, step)
	if err != nil {
		_, _ = fmt.Fprintln(stderr, "senro shell:", err)
		return exitUsage
	}
	if len(snaps) == 0 {
		_, _ = fmt.Fprintf(stderr, "senro shell: step %q recorded no workspace snapshots in run %s\n", step, runDir)
		return exitUsage
	}

	cacheRoot, err := resolveCacheDir(cacheDir)
	if err != nil {
		_, _ = fmt.Fprintln(stderr, "senro shell:", err)
		return exitUsage
	}
	store, err := storage.Open(cacheRoot)
	if err != nil {
		_, _ = fmt.Fprintln(stderr, "senro shell:", err)
		return exitUsage
	}
	defer func() { _ = store.Close() }()

	// SIGTERM, and SIGHUP from a closed terminal, end the shell and still
	// clean up behind it. SIGINT is
	// swallowed rather than acted on: an interactive ^C reaches the shell
	// through the terminal's own process group, and a CLI that died on it
	// would orphan the shell and leave the directory behind. Notify rather
	// than Ignore, because an ignored signal stays ignored in the child.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGHUP)
	defer stop()
	intCh := make(chan os.Signal, 1)
	signal.Notify(intCh, os.Interrupt)
	defer signal.Stop(intCh)

	runID := recordedRunID(runDir)
	root, workDir, err := restoreStepWorkspaces(ctx, store, p, runID, step, snaps)
	if err != nil {
		if errors.Is(err, cas.ErrNotFound) {
			// The condition `senro ws pull` reports, for the same reason.
			_, _ = fmt.Fprintf(stderr,
				"senro shell: a workspace body step %q snapshotted is gone from %s: most likely collected "+
					"by a 'senro cache gc' sweep, since only a failed run's workspaces are protected against one: %v\n",
				step, cacheRoot, err)
			return exitUsage
		}
		_, _ = fmt.Fprintln(stderr, "senro shell:", err)
		return exitRunFailed
	}
	_, _ = fmt.Fprintf(stderr, "senro shell: offline, workspaces restored to %s\n", root)
	defer func() {
		if keep {
			_, _ = fmt.Fprintf(stderr, "senro shell: kept %s\n", root)
			return
		}
		if err := os.RemoveAll(root); err != nil {
			_, _ = fmt.Fprintf(stderr, "senro shell: removing %s: %v\n", root, err)
		}
	}()

	return runOfflineShell(ctx, root, workDir, offlineShellEnv(n, runID, step), cmd, stdin, stdout, stderr)
}

// offlineShellEnv is what the offline shell runs with: the step's declared
// environment under plan.ShellEnv's rule, the caller's PATH, HOME and TERM
// so the shell is usable on this machine, and which run and step it stands
// in. The caller's three come after the step's, so a step that declared a
// PATH for its image does not leave this shell unable to find ls; the
// environment is otherwise not inherited, so nothing of the caller's own
// leaks in under a step's name.
//
// The step's variables are also filtered by shellEnvDenied: they were
// recorded for a sandbox, and are applied here to the operator's own shell
// on the operator's own machine.
func offlineShellEnv(n *plan.Node, runID, step string) []string {
	var env []string
	for _, kv := range plan.ShellEnv(n) {
		name, _, _ := strings.Cut(kv, "=")
		if !shellEnvDenied(name) {
			env = append(env, kv)
		}
	}
	for _, k := range []string{"PATH", "HOME", "TERM"} {
		if v, ok := os.LookupEnv(k); ok {
			env = append(env, k+"="+v)
		}
	}
	return append(env, "SENRO_SHELL_RUN="+runID, "SENRO_SHELL_STEP="+step)
}

// shellEnvDenied reports whether a recorded step variable is kept out of
// the offline shell. Each one makes the shell, or every program it starts,
// run code or change parsing before the operator types anything:
//
//   - LD_* and DYLD_*: the dynamic loader's preload and search paths
//     (LD_PRELOAD, DYLD_INSERT_LIBRARIES), code injected into every
//     process;
//   - BASH_ENV, ENV, ZDOTDIR: a startup file the shell sources;
//   - PROMPT_COMMAND, PS4: a command run at every prompt or trace line;
//   - SHELLOPTS, BASHOPTS: shell options set before any rc file;
//   - IFS: how every unquoted expansion splits.
//
// A step's environment is the pipeline's data, written for a sandbox that
// may not even be this machine; a recorded run directory can come from
// anywhere (a CI artifact, a colleague). Standing in it must not mean
// running whatever it says.
func shellEnvDenied(name string) bool {
	if strings.HasPrefix(name, "LD_") || strings.HasPrefix(name, "DYLD_") {
		return true
	}
	switch name {
	case "BASH_ENV", "ENV", "ZDOTDIR", "PROMPT_COMMAND", "SHELLOPTS", "BASHOPTS", "IFS", "PS4":
		return true
	}
	return false
}

// stepSnapshots is the latest ws.snapshot per workspace name among the
// events step emitted, by Seq, or an empty map when it emitted none. A
// forced capture is skipped, as `senro ws` skips one: it is diagnostic, and
// not what the step left behind. Unlike latestSnapshots it reads no
// ws.restored: a step's own snapshot is what its workspaces held once it
// had run, and a restore names a body some earlier step produced.
func stepSnapshots(runDir, step string) (map[string]api.WSSnapshotBody, error) {
	events, err := eventlog.Read(filepath.Join(runDir, "events.jsonl"))
	if err != nil && len(events) == 0 && !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("read %s: %w", runDir, err)
	}
	out := map[string]api.WSSnapshotBody{}
	seq := map[string]uint64{}
	for _, e := range events {
		if e.Type != api.WSSnapshot || e.Step != step {
			continue
		}
		var b api.WSSnapshotBody
		if err := e.Decode(&b); err != nil || b.Forced {
			continue
		}
		if prev, ok := seq[b.Name]; ok && prev > e.Seq {
			continue
		}
		out[b.Name], seq[b.Name] = b, e.Seq
	}
	return out, nil
}

// restoreStepWorkspaces restores snaps into one fresh directory under
// os.TempDir(), each workspace at its mount path in step's node, and
// reports that directory and the one to start the shell in: the restored
// mount at the step's WorkDir, else the first restored mount in plan
// order, else root. A snapshot of a workspace the step does not mount is
// skipped, since there is no path to put it at.
//
// Shallower mounts are restored first, because a restore REPLACES its
// destination: a workspace mounted inside another's path must land after
// the outer one, or the outer restore would remove it. On any error root
// is removed and nothing is returned.
func restoreStepWorkspaces(ctx context.Context, store *storage.Storage, p *plan.Plan, run, step string,
	snaps map[string]api.WSSnapshotBody) (root, workDir string, err error) {
	n, ok := p.Node(step)
	if !ok {
		return "", "", fmt.Errorf("the plan has no step %q", step)
	}
	// A path separator in a pattern is refused by MkdirTemp, and a step or
	// run name can contain one.
	clean := strings.NewReplacer(string(os.PathSeparator), "_", "/", "_").Replace
	root, err = os.MkdirTemp(os.TempDir(), "senro-shell-"+clean(run)+"-"+clean(step)+"-*")
	if err != nil {
		return "", "", fmt.Errorf("create a directory to restore into: %w", err)
	}
	defer func() {
		if err != nil {
			_ = os.RemoveAll(root)
			root, workDir = "", ""
		}
	}()

	type target struct {
		ms   plan.MountSpec
		path string
		s    api.WSSnapshotBody
	}
	var targets []target
	for _, ms := range n.Mounts {
		s, ok := snaps[ms.Workspace]
		if ms.Workspace == "" || !ok {
			continue
		}
		// Rooted before joining, so an At of "../x" stays inside root.
		targets = append(targets, target{ms: ms, path: filepath.Join(root, filepath.Clean("/"+ms.At)), s: s})
	}

	order := make([]target, len(targets))
	copy(order, targets)
	sort.SliceStable(order, func(i, j int) bool { return len(order[i].path) < len(order[j].path) })
	for _, t := range order {
		d := cas.Digest(t.s.Digest)
		if !d.Valid() {
			return "", "", fmt.Errorf("workspace %q records %q, which is not a content digest", t.ms.Workspace, t.s.Digest)
		}
		if _, err := store.Snapshotter.RestoreTree(ctx, d, t.path); err != nil {
			if errors.Is(err, workspace.ErrUnsafePath) {
				return "", "", fmt.Errorf("refusing to unpack workspace %q: %w. A snapshot senro produced "+
					"cannot contain such an entry, so this body was assembled or altered by something else",
					t.ms.Workspace, err)
			}
			return "", "", fmt.Errorf("restore workspace %q: %w", t.ms.Workspace, err)
		}
	}

	workDir = root
	if len(targets) > 0 {
		workDir = targets[0].path
	}
	for _, t := range targets {
		if t.ms.At == n.WorkDir {
			workDir = t.path
			return root, workDir, nil
		}
	}
	restored := make([]restoredMount, len(targets))
	for i, t := range targets {
		restored[i] = restoredMount{at: t.ms.At, path: t.path}
	}
	if d, ok := workDirInside(n.WorkDir, root, restored); ok {
		workDir = d
	}
	return root, workDir, nil
}

// restoredMount is one mount's declared path and where it was restored.
type restoredMount struct{ at, path string }

// workDirInside resolves a WorkDir that lies INSIDE a restored mount
// ("/src/app" under a mount at "/src") to that path under the mount's
// restored directory. The deepest enclosing mount wins, as it would in the
// sandbox, where an inner mount shadows the outer one's subtree. ok is false
// when no mount encloses workDir, or when the subdirectory was not restored
// (the step created it outside any snapshot, say), in which case the
// enclosing mount itself is the answer.
//
// Both sides are rooted and cleaned before comparing, so "../x" cannot climb
// out of root: Clean("/../x") is "/x".
func workDirInside(workDir, root string, mounts []restoredMount) (string, bool) {
	if workDir == "" {
		return "", false
	}
	wd := filepath.Clean("/" + workDir)
	best, bestAt := "", ""
	for _, m := range mounts {
		at := filepath.Clean("/" + m.at)
		prefix := strings.TrimSuffix(at, "/") + "/"
		if (wd == at || strings.HasPrefix(wd, prefix)) && len(at) > len(bestAt) {
			best, bestAt = m.path, at
		}
	}
	if best == "" {
		return "", false
	}
	rel, err := filepath.Rel(bestAt, wd)
	if err != nil {
		return best, true
	}
	cand := filepath.Join(best, rel)
	if r, err := filepath.Rel(root, cand); err != nil || r == ".." || strings.HasPrefix(r, ".."+string(filepath.Separator)) {
		return best, true
	}
	if fi, err := os.Stat(cand); err != nil || !fi.IsDir() {
		return best, true
	}
	return cand, true
}

// runOfflineShell runs cmd, or $SHELL (else /bin/sh) when cmd is empty, in
// workDir (root when workDir is empty) with exactly env and the three
// streams, and reports its exit status as this process's: the same mapping
// the live form uses (shellExitCode). A command that never started is
// reported on stderr and is exitRunFailed.
func runOfflineShell(ctx context.Context, root, workDir string, env, cmd []string,
	stdin io.Reader, stdout, stderr io.Writer) int {
	if len(cmd) == 0 {
		sh := os.Getenv("SHELL")
		if sh == "" {
			sh = "/bin/sh"
		}
		cmd = []string{sh}
	}
	dir := workDir
	if dir == "" {
		dir = root
	}
	// The coordinator's own shell over restored files, not the step's image.
	c := exec.CommandContext(ctx, cmd[0], cmd[1:]...)
	c.Dir, c.Env = dir, env
	c.Stdin, c.Stdout, c.Stderr = stdin, stdout, stderr
	err := c.Run()
	var exitErr *exec.ExitError
	switch {
	case err == nil:
		return exitSuccess
	case errors.As(err, &exitErr):
		return shellExitCode(exitErr.ExitCode())
	default:
		_, _ = fmt.Fprintln(stderr, "senro shell:", err)
		return exitRunFailed
	}
}

// recordedRunID is the run ID a run directory's own events carry, or the
// directory's name when none does: `runs/<id>` is the convention, and only
// a directory chosen through senro.WithDir can differ from it.
func recordedRunID(runDir string) string {
	events, _ := eventlog.Read(filepath.Join(runDir, "events.jsonl"))
	for _, e := range events {
		if e.Run != "" {
			return e.Run
		}
	}
	return filepath.Base(runDir)
}
