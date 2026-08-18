// Package change answers what a run is asked to build: everything, or the
// files a change touched.
//
// It is the half of an affected set that has nothing to do with Go. A unit
// graph knows which unit owns which file (see
// github.com/xavidop/senro/unit/gowork); this knows what changed, a question
// about the event that started the run, and hands the answer to an
// expansion via Affected(change.FromTrigger(ev)).
//
// It consumes a base, it does not invent one. The trigger's Mode says
// whether the run covers everything or only what changed, and its Base says
// which two commits to diff. FromTrigger never computes a merge base,
// resolves a ref, or picks a "since" of its own: a base senro guessed would
// be a base nobody declared.
//
// Not knowing is everything, never nothing. A source that cannot answer
// says All; an empty file list would mean no unit runs, which on a day the
// wiring is wrong is a build reporting green without compiling the thing
// that broke. The one case that cannot land on All (a base commit not in
// the clone) is a loud error instead.
package change

import (
	"bytes"
	"context"
	"fmt"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"

	"github.com/xavidop/senro/internal/workspace"
	"github.com/xavidop/senro/trigger"
)

// Set is what changed.
//
// All and Files are not two ways of saying the same thing. All is "build
// everything", and an empty Files with All false is "genuinely nothing
// changed, build nothing". A source that does not know which of those it is
// looking at must say All; see the package doc.
type Set struct {
	// All covers every unit, whatever Files says. A source that sets it
	// leaves Files empty.
	All bool
	// Files are the changed paths, slash-separated and relative to the root
	// the source was asked about. A path outside the root keeps its leading
	// "../", which is how a unit graph can tell that it owns none of it.
	Files []string
}

// Source answers what changed, when the pipeline is built.
//
// Lazily, taking a context, because the answer can cost a subprocess:
// FromTrigger runs `git diff` over two commits. That is the same reason
// unit.Graph.Units takes one.
type Source interface {
	// Changed reports what changed under root.
	Changed(ctx context.Context, root string) (Set, error)
	// Describe names this source for a plan and for an error message:
	// "trigger (pull_request, a1b2c3d..e4f5a6b)".
	Describe() string
}

// Everything covers every unit, which is what a run with no change
// information at all has to do.
func Everything() Source { return everything{} }

type everything struct{}

func (everything) Changed(context.Context, string) (Set, error) { return Set{All: true}, nil }
func (everything) Describe() string                             { return "everything" }

// Paths is a literal list of changed files, relative to the root and
// slash-separated. It is what a caller with its own idea of what changed
// reaches for, and what a test uses to pin an affected set without a
// checkout.
//
// Paths() with no arguments is "nothing changed", NOT "everything": use
// Everything for that. The two are different answers and a run acts very
// differently on them.
func Paths(files ...string) Source {
	return literal{files: append([]string(nil), files...)}
}

type literal struct{ files []string }

func (l literal) Changed(context.Context, string) (Set, error) {
	return Set{Files: append([]string(nil), l.files...)}, nil
}
func (l literal) Describe() string { return fmt.Sprintf("%d paths", len(l.files)) }

// FromTrigger reads what the event that started this run already recorded.
//
// The event decides, in this order:
//
//   - No event at all is Everything: `./pipeline` with no --trigger-event
//     gates nothing, and a dispatcher that forgot the flag over-runs
//     visibly instead of silently running nothing.
//   - trigger.ModeAll is Everything: a default-branch push, a tag and a
//     schedule cover the repository by definition.
//   - A Base with both ends set is `git diff` between exactly those two
//     commits.
//   - Otherwise the event's own changed-file list, if it carried one.
//   - Otherwise Everything.
//
// The base wins over the event's file list because a GitHub push payload
// truncates its commits array at twenty: the file list UNDER-counts a
// larger push, while the base's two commit ids are exact at any size.
//
// The diff is two-dot, deliberately: three-dot finds the merge base first,
// which a shallow CI clone often lacks and which is the guess this package
// exists not to make. For a pull request whose base branch has moved on,
// two-dot reports the base's own drift as changed too: more units than
// strictly necessary, never fewer. Renames are off for the same reason, so
// a moved file is reported at both its old path and its new one.
func FromTrigger(ev *trigger.Event) Source { return fromTrigger{ev: ev} }

type fromTrigger struct{ ev *trigger.Event }

func (f fromTrigger) Describe() string {
	if f.ev == nil {
		return "trigger (no event)"
	}
	if f.ev.Mode() == trigger.ModeAll {
		return fmt.Sprintf("trigger (%s, everything)", f.ev.Kind)
	}
	if b := f.ev.Base; b.From != "" && b.To != "" {
		return fmt.Sprintf("trigger (%s, %s..%s)", f.ev.Kind, short(b.From), short(b.To))
	}
	if f.ev.Files != nil {
		return fmt.Sprintf("trigger (%s, %d paths from the event)", f.ev.Kind, len(f.ev.Files))
	}
	return fmt.Sprintf("trigger (%s, nothing to diff)", f.ev.Kind)
}

func short(sha string) string {
	if len(sha) > 7 {
		return sha[:7]
	}
	return sha
}

func (f fromTrigger) Changed(ctx context.Context, root string) (Set, error) {
	if f.ev == nil || f.ev.Mode() == trigger.ModeAll {
		return Set{All: true}, nil
	}
	if b := f.ev.Base; b.From != "" && b.To != "" {
		files, err := gitDiff(ctx, root, b.From, b.To)
		if err != nil {
			return Set{}, err
		}
		return Set{Files: files}, nil
	}
	if f.ev.Files != nil {
		out := make([]string, 0, len(f.ev.Files))
		for _, p := range f.ev.Files {
			out = append(out, filepath.ToSlash(p))
		}
		sort.Strings(out)
		return Set{Files: out}, nil
	}
	// A push that created a ref carries no "before", and a provider-neutral
	// event need carry neither. There is nothing here to narrow a build with.
	return Set{All: true}, nil
}

// Ignoring drops paths matching any of patterns from what src reported.
//
// The one thing in this package that deliberately runs LESS, and the
// caller's decision, not senro's: a change to docs/architecture.md cannot
// break a Go build, and a file no unit owns is otherwise a file that could
// have changed anything.
//
// It is also the one thing here that can make an affected set WRONG: a
// pattern that matches something that does change a unit's build ("*.yml")
// turns a broken build green. Write the narrowest pattern that does the job.
//
// An All set passes through untouched: filtering "build everything" would
// quietly turn it into "build nothing".
//
// Patterns use senro's own glob syntax: "*" and "?" match within a path
// segment, "**" spans segments, matched against the slash-separated path
// relative to the root.
func Ignoring(src Source, patterns ...string) Source {
	return ignoring{src: src, pats: append([]string(nil), patterns...)}
}

type ignoring struct {
	src  Source
	pats []string
}

func (i ignoring) Describe() string {
	return fmt.Sprintf("%s, ignoring [%s]", i.src.Describe(), strings.Join(i.pats, " "))
}

func (i ignoring) Changed(ctx context.Context, root string) (Set, error) {
	set, err := i.src.Changed(ctx, root)
	if err != nil || set.All {
		return set, err
	}
	kept := make([]string, 0, len(set.Files))
	for _, f := range set.Files {
		if !matchAny(i.pats, f) {
			kept = append(kept, f)
		}
	}
	set.Files = kept
	return set, nil
}

func matchAny(pats []string, f string) bool {
	for _, p := range pats {
		if workspace.MatchGlob(p, f) {
			return true
		}
	}
	return false
}

// gitDiff lists the paths that differ between two commits, relative to root.
func gitDiff(ctx context.Context, root, from, to string) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	top, err := gitTopLevel(ctx, root)
	if err != nil {
		return nil, err
	}
	// git reports the top level with every symlink already resolved, and root
	// arrives however the caller spelled it. Resolving both is what keeps the
	// two comparable: on macOS a checkout under /var is really under
	// /private/var, and a Rel of the two unresolved forms is a path made
	// almost entirely of "..".
	base := resolve(root)
	top = resolve(top)
	// -z: NUL-separated, so a path with a space, a quote or a newline in it
	// arrives intact rather than C-quoted, which is what --name-only does by
	// default and which nothing downstream would unquote.
	//
	// --no-renames: a rename is reported as a delete plus an add, at both
	// paths. Rename detection would report only the new one, and the unit
	// that lost the file would not be rebuilt.
	out, err := git(ctx, root, "diff", "--name-only", "--no-renames", "-z", from, to, "--")
	if err != nil {
		return nil, err
	}
	var files []string
	for _, p := range strings.Split(out, "\x00") {
		if p == "" {
			continue
		}
		rel, err := filepath.Rel(base, filepath.Join(top, filepath.FromSlash(p)))
		if err != nil {
			// Unrelatable to the root at all. Keeping the path git gave is the
			// safe direction: a unit graph will not own it, and not owning a
			// changed file means everything runs.
			files = append(files, p)
			continue
		}
		files = append(files, filepath.ToSlash(rel))
	}
	sort.Strings(files)
	return files, nil
}

// resolve is p absolute and with symlinks followed, falling back to whatever
// it managed: this only ever feeds a path comparison, and a path that cannot
// be resolved is better compared as it stands than not compared at all.
func resolve(p string) string {
	if abs, err := filepath.Abs(p); err == nil {
		p = abs
	}
	if real, err := filepath.EvalSymlinks(p); err == nil {
		return real
	}
	return p
}

func gitTopLevel(ctx context.Context, root string) (string, error) {
	out, err := git(ctx, root, "rev-parse", "--show-toplevel")
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(out), nil
}

// git runs one git command in root and returns its stdout. A failure
// carries git's own stderr plus advice for the overwhelmingly common cause:
// a shallow CI checkout missing the commit the event named. Not papered
// over with "then build everything": the run cannot tell what changed, and
// a person has to change the checkout depth.
func git(ctx context.Context, root string, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, "git", append([]string{"-C", root}, args...)...) // #nosec G204 -- commit ids from the event that started the run, passed as argv and never through a shell
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return "", ctxErr
		}
		msg := strings.TrimSpace(stderr.String())
		return "", fmt.Errorf("change: git %s in %s: %w: %s; if the base commit is not in the "+
			"clone, deepen the checkout (fetch-depth: 0) so the diff has something to compare "+
			"against", strings.Join(args, " "), root, err, msg)
	}
	return stdout.String(), nil
}
