package glob_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/xavidop/senro/unit/glob"
)

func tree(t *testing.T, files ...string) string {
	t.Helper()
	root := t.TempDir()
	for _, f := range files {
		p := filepath.Join(root, filepath.FromSlash(f))
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte("x\n"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return root
}

func ids(t *testing.T, g interface {
	Units(context.Context, string) ([]glob.Unit, error)
}, root string) []string {
	t.Helper()
	us, err := g.Units(context.Background(), root)
	if err != nil {
		t.Fatalf("Units: %v", err)
	}
	out := make([]string, 0, len(us))
	for _, u := range us {
		out = append(out, u.ID)
	}
	return out
}

func TestDirsMatchesDirectoriesInASortedOrder(t *testing.T) {
	root := tree(t, "apps/web/index.ts", "apps/api/main.ts", "apps/admin/app.tsx", "packages/ui/x.ts")
	got := ids(t, glob.Dirs("apps/*"), root)
	want := []string{"apps/admin", "apps/api", "apps/web"}
	if len(got) != len(want) {
		t.Fatalf("Units = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("Units = %v, want %v (sorted)", got, want)
		}
	}
}

func TestFilesMatchesAFileAndReportsItsDirectory(t *testing.T) {
	root := tree(t, "services/api/go.mod", "services/worker/go.mod", "services/api/internal/x.go")
	us, err := glob.Files("services/*/go.mod").Units(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	if len(us) != 2 {
		t.Fatalf("Units = %v, want two services", us)
	}
	if us[0].ID != "services/api" || us[0].Dir != "services/api" {
		t.Errorf("first unit = %+v", us[0])
	}
	// us[1] is checked too: pinning only us[0] would still pass a Units
	// that silently dropped or duplicated the second
	// service as long as the first slot came out right.
	if us[1].ID != "services/worker" || us[1].Dir != "services/worker" {
		t.Errorf("second unit = %+v", us[1])
	}
}

// TestFilesInTheSameDirectoryProduceOneUnit pins the doc comment's own claim
// ("Two matches in one directory produce one unit, not two") with an actual
// case that exercises it: two files in the same directory both matching the
// pattern must collapse to one Unit, not two copies of it.
func TestFilesInTheSameDirectoryProduceOneUnit(t *testing.T) {
	root := tree(t, "services/api/a.proto", "services/api/b.proto")
	got := ids(t, glob.Files("**/*.proto"), root)
	if len(got) != 1 || got[0] != "services/api" {
		t.Fatalf("Units = %v, want exactly one unit for services/api", got)
	}
}

// TestTheWalkSkipsTheMandatoryExcludes keeps a glob from descending into
// .git and node_modules. It is a performance property in a monorepo and a
// correctness one for the pnpm case: node_modules/*/package.json would
// otherwise expand into one step per installed dependency.
func TestTheWalkSkipsTheMandatoryExcludes(t *testing.T) {
	root := tree(t,
		"apps/web/package.json",
		"node_modules/left-pad/package.json",
		".git/hooks/package.json",
	)
	got := ids(t, glob.Files("**/package.json"), root)
	if len(got) != 1 || got[0] != "apps/web" {
		t.Fatalf("Units = %v, want only apps/web", got)
	}
}

func TestAPatternThatMatchesNothingIsNotAnError(t *testing.T) {
	root := tree(t, "README.md")
	us, err := glob.Dirs("apps/*").Units(context.Background(), root)
	if err != nil {
		t.Fatalf("Units: %v", err)
	}
	if len(us) != 0 {
		t.Fatalf("Units = %v, want none", us)
	}
}

func TestAMissingRootIsAnError(t *testing.T) {
	if _, err := glob.Dirs("apps/*").Units(context.Background(), filepath.Join(t.TempDir(), "nope")); err == nil {
		t.Fatal("Units accepted a root that does not exist")
	}
}

// TestDescribeNamesTheKindAndPattern pins the error-message contract
// (*Pipeline).Build relies on: MaxNodes' refusal names g.Describe(), so a
// Describe that dropped the pattern or conflated Dirs and Files would make
// that refusal unreadable.
func TestDescribeNamesTheKindAndPattern(t *testing.T) {
	if got := glob.Dirs("apps/*").Describe(); got != "glob dirs apps/*" {
		t.Errorf("Dirs Describe = %q", got)
	}
	if got := glob.Files("**/go.mod").Describe(); got != "glob files **/go.mod" {
		t.Errorf("Files Describe = %q", got)
	}
}
