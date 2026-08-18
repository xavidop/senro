package change_test

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/xavidop/senro/change"
	"github.com/xavidop/senro/trigger"
)

func TestEverythingAndPaths(t *testing.T) {
	set, err := change.Everything().Changed(context.Background(), t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if !set.All {
		t.Error("Everything().Changed is not All")
	}
	set, err = change.Paths("a/x.go", "b/y.go").Changed(context.Background(), t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if set.All || !eq(set.Files, []string{"a/x.go", "b/y.go"}) {
		t.Errorf("Paths = %v (all=%v)", set.Files, set.All)
	}
}

// TestPathsOfNothingIsNotEverything. "Nothing changed" and "I do not know
// what changed" are different answers, and collapsing the first into the
// second would make an empty push build the world; collapsing the second
// into the first would make an unknown build nothing at all, which is the
// dangerous direction.
func TestPathsOfNothingIsNotEverything(t *testing.T) {
	set, err := change.Paths().Changed(context.Background(), t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if set.All || len(set.Files) != 0 {
		t.Errorf("Paths() = %v (all=%v), want an empty, non-All set", set.Files, set.All)
	}
}

func TestFromTriggerWithNoEventIsEverything(t *testing.T) {
	set, err := change.FromTrigger(nil).Changed(context.Background(), t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if !set.All {
		t.Error("no event did not produce an All set; the local loop has to build everything")
	}
}

// TestFromTriggerHonoursTheModeTheTriggerAlreadyDecided: a push to the
// default branch, a tag and a schedule are ModeAll, and this must not
// second-guess that by diffing anything.
func TestFromTriggerHonoursTheModeTheTriggerAlreadyDecided(t *testing.T) {
	for name, ev := range map[string]*trigger.Event{
		"push to default": {
			Kind: trigger.Push, Branch: "main", DefaultBranch: "main",
			Base: trigger.Base{From: "dead", To: "beef"},
		},
		"tag":      {Kind: trigger.Tag, Tag: "v1.0.0"},
		"schedule": {Kind: trigger.Schedule, Schedule: "0 3 * * *"},
		"manual":   {Kind: trigger.Manual},
	} {
		t.Run(name, func(t *testing.T) {
			set, err := change.FromTrigger(ev).Changed(context.Background(), t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			if !set.All {
				t.Errorf("mode %s did not produce an All set", ev.Mode())
			}
		})
	}
}

// TestFromTriggerDiffsTheBaseTheEventCarried is the end of the wire: the
// trigger recorded before/after, and this turns exactly those two commits
// into the changed-file list. Nothing here computes a merge base or guesses
// a ref.
func TestFromTriggerDiffsTheBaseTheEventCarried(t *testing.T) {
	repo, first, second := repoWithTwoCommits(t)
	ev := &trigger.Event{
		Kind: trigger.PullRequest, Action: "opened", Branch: "main", DefaultBranch: "main",
		Base: trigger.Base{From: first, To: second},
	}
	set, err := change.FromTrigger(ev).Changed(context.Background(), repo)
	if err != nil {
		t.Fatal(err)
	}
	if set.All {
		t.Fatal("a pull request produced an All set")
	}
	if !eq(set.Files, []string{"libb/b.go", "removed.txt"}) {
		t.Fatalf("Changed = %v, want the added and the removed path", set.Files)
	}
}

// TestFromTriggerPrefersTheBaseOverTheEventsOwnFileList. A GitHub push
// payload truncates its commit list at twenty, so Event.Files can be an
// UNDER-count of what the push contained, and preferring it would skip units.
// The two ends of the base are exact.
func TestFromTriggerPrefersTheBaseOverTheEventsOwnFileList(t *testing.T) {
	repo, first, second := repoWithTwoCommits(t)
	ev := &trigger.Event{
		Kind: trigger.Push, Branch: "topic", DefaultBranch: "main",
		Files: []string{"a-lie.txt"},
		Base:  trigger.Base{From: first, To: second},
	}
	set, err := change.FromTrigger(ev).Changed(context.Background(), repo)
	if err != nil {
		t.Fatal(err)
	}
	if !eq(set.Files, []string{"libb/b.go", "removed.txt"}) {
		t.Fatalf("Changed = %v, want the diff and not the event's own list", set.Files)
	}
}

// TestFromTriggerFallsBackToTheEventsFilesWithNoBase: GitHub sends the
// all-zero SHA for a push that CREATED a branch, which trigger blanks, so
// there is nothing to diff against and the event's own list is all there is.
func TestFromTriggerFallsBackToTheEventsFilesWithNoBase(t *testing.T) {
	ev := &trigger.Event{
		Kind: trigger.Push, Branch: "topic", DefaultBranch: "main",
		Files: []string{"services/api/main.go"},
		Base:  trigger.Base{To: "beef"},
	}
	set, err := change.FromTrigger(ev).Changed(context.Background(), t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if set.All || !eq(set.Files, []string{"services/api/main.go"}) {
		t.Fatalf("Changed = %v (all=%v)", set.Files, set.All)
	}
}

// TestFromTriggerWithNothingToGoOnIsEverything. No base and no file list is
// "I do not know what changed", and the only safe answer to that is all of
// it.
func TestFromTriggerWithNothingToGoOnIsEverything(t *testing.T) {
	ev := &trigger.Event{Kind: trigger.Push, Branch: "topic", DefaultBranch: "main"}
	set, err := change.FromTrigger(ev).Changed(context.Background(), t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if !set.All {
		t.Fatalf("Changed = %v (all=%v), want everything", set.Files, set.All)
	}
}

// TestAMissingBaseCommitIsAnError, and one that says what to do about it. A
// shallow CI clone is the ordinary way to hit this, and answering "nothing
// changed" (which is what an unchecked git failure would eventually become)
// would run no units at all and report the run green.
func TestAMissingBaseCommitIsAnError(t *testing.T) {
	repo, _, second := repoWithTwoCommits(t)
	ev := &trigger.Event{
		Kind: trigger.PullRequest, Branch: "main", DefaultBranch: "main",
		Base: trigger.Base{From: "0000000000000000000000000000000000000001", To: second},
	}
	_, err := change.FromTrigger(ev).Changed(context.Background(), repo)
	if err == nil {
		t.Fatal("diffing against a commit that is not in the clone returned no error")
	}
	if !strings.Contains(err.Error(), "fetch") {
		t.Errorf("error %q does not say what to do about it", err)
	}
}

func TestNotAGitRepositoryIsAnError(t *testing.T) {
	ev := &trigger.Event{
		Kind: trigger.PullRequest, Branch: "main", DefaultBranch: "main",
		Base: trigger.Base{From: "a", To: "b"},
	}
	if _, err := change.FromTrigger(ev).Changed(context.Background(), t.TempDir()); err == nil {
		t.Fatal("a root that is not a git checkout returned no error")
	}
}

// TestPathsAreRelativeToTheRootAndNotToTheRepository: an expansion's root is
// wherever the pipeline was built from, which is not always the top of the
// checkout, and a path that git reported from the top would be attributed to
// the wrong unit or to none.
func TestPathsAreRelativeToTheRootAndNotToTheRepository(t *testing.T) {
	repo, first, second := repoWithTwoCommits(t)
	ev := &trigger.Event{
		Kind: trigger.PullRequest, Branch: "main", DefaultBranch: "main",
		Base: trigger.Base{From: first, To: second},
	}
	set, err := change.FromTrigger(ev).Changed(context.Background(), filepath.Join(repo, "libb"))
	if err != nil {
		t.Fatal(err)
	}
	// b.go is inside libb and is reported relative to it; removed.txt is
	// above it and stays visibly above it, so whatever consumes this can see
	// that it owns none of the units under this root.
	if !eq(set.Files, []string{"../removed.txt", "b.go"}) {
		t.Fatalf("Changed = %v", set.Files)
	}
}

func TestIgnoringDropsMatchingPaths(t *testing.T) {
	src := change.Ignoring(change.Paths("docs/x.md", "libb/b.go", "README.md"), "docs/**", "**/*.md")
	set, err := src.Changed(context.Background(), t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if !eq(set.Files, []string{"libb/b.go"}) {
		t.Fatalf("Changed = %v", set.Files)
	}
}

// TestIgnoringLeavesAnAllSetAlone. All means "build everything", and
// filtering a file list out of it would turn it into "build nothing".
func TestIgnoringLeavesAnAllSetAlone(t *testing.T) {
	set, err := change.Ignoring(change.Everything(), "**").Changed(context.Background(), t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if !set.All {
		t.Fatal("Ignoring turned an All set into a file list")
	}
}

func TestDescribe(t *testing.T) {
	cases := []struct {
		src  change.Source
		want string
	}{
		{change.Everything(), "everything"},
		{change.Paths("a", "b"), "2 paths"},
		{change.FromTrigger(nil), "trigger (no event)"},
		{change.Ignoring(change.Everything(), "a/**"), "everything, ignoring [a/**]"},
	}
	for _, c := range cases {
		if got := c.src.Describe(); got != c.want {
			t.Errorf("Describe = %q, want %q", got, c.want)
		}
	}
	ev := &trigger.Event{Kind: trigger.PullRequest, Base: trigger.Base{From: "aaaaaaaaaaaa", To: "bbbbbbbbbbbb"}}
	if got := change.FromTrigger(ev).Describe(); !strings.Contains(got, "aaaaaaa") {
		t.Errorf("Describe = %q, want it to name the base", got)
	}
}

func TestChangedIsCancellable(t *testing.T) {
	repo, first, second := repoWithTwoCommits(t)
	ev := &trigger.Event{
		Kind: trigger.PullRequest, Branch: "main", DefaultBranch: "main",
		Base: trigger.Base{From: first, To: second},
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := change.FromTrigger(ev).Changed(ctx, repo); err == nil {
		t.Fatal("a cancelled context still produced a change set")
	}
}

// repoWithTwoCommits builds a real checkout: the first commit holds
// removed.txt, the second removes it and adds libb/b.go, so a diff of the two
// has to report both an addition and a deletion.
func repoWithTwoCommits(t *testing.T) (root, first, second string) {
	t.Helper()
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not on PATH")
	}
	root = t.TempDir()
	git := func(args ...string) string {
		t.Helper()
		cmd := exec.Command("git", append([]string{"-C", root}, args...)...)
		cmd.Env = append(os.Environ(),
			"GIT_AUTHOR_NAME=senro", "GIT_AUTHOR_EMAIL=senro@example.com",
			"GIT_COMMITTER_NAME=senro", "GIT_COMMITTER_EMAIL=senro@example.com",
			"GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_SYSTEM=/dev/null",
		)
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("git %v: %v: %s", args, err, out)
		}
		return strings.TrimSpace(string(out))
	}
	write := func(p, body string) {
		t.Helper()
		full := filepath.Join(root, filepath.FromSlash(p))
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	git("init", "-q", "-b", "main")
	write("removed.txt", "gone soon\n")
	write("keep.txt", "kept\n")
	git("add", "-A")
	git("commit", "-q", "--no-gpg-sign", "-m", "first")
	first = git("rev-parse", "HEAD")

	if err := os.Remove(filepath.Join(root, "removed.txt")); err != nil {
		t.Fatal(err)
	}
	write("libb/b.go", "package libb\n")
	git("add", "-A")
	git("commit", "-q", "--no-gpg-sign", "-m", "second")
	second = git("rev-parse", "HEAD")
	return root, first, second
}

func eq(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}
