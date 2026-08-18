package scratch_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/xavidop/senro/internal/scratch"
)

func TestExpandKeyHashesFiles(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "go.sum"), []byte("h1:abc\n"), 0o644); err != nil {
		t.Fatalf("seed: %v", err)
	}
	got, err := scratch.ExpandKey(`gomod-{{ hashFiles "go.sum" }}`, root)
	if err != nil {
		t.Fatalf("ExpandKey: %v", err)
	}
	if !strings.HasPrefix(got, "gomod-") || len(got) <= len("gomod-") {
		t.Fatalf("ExpandKey = %q", got)
	}

	again, err := scratch.ExpandKey(`gomod-{{ hashFiles "go.sum" }}`, root)
	if err != nil {
		t.Fatalf("ExpandKey again: %v", err)
	}
	if again != got {
		t.Errorf("the same tree hashed to %q then %q", got, again)
	}
}

func TestExpandKeyChangesWhenAHashedFileChanges(t *testing.T) {
	root := t.TempDir()
	p := filepath.Join(root, "go.sum")
	if err := os.WriteFile(p, []byte("one\n"), 0o644); err != nil {
		t.Fatalf("seed: %v", err)
	}
	before, err := scratch.ExpandKey(`k-{{ hashFiles "go.sum" }}`, root)
	if err != nil {
		t.Fatalf("ExpandKey: %v", err)
	}
	if err := os.WriteFile(p, []byte("two\n"), 0o644); err != nil {
		t.Fatalf("rewrite: %v", err)
	}
	after, err := scratch.ExpandKey(`k-{{ hashFiles "go.sum" }}`, root)
	if err != nil {
		t.Fatalf("ExpandKey again: %v", err)
	}
	if before == after {
		t.Error("changing a hashed file did not change the key")
	}
}

func TestExpandKeyHashesGlobsInAStableOrder(t *testing.T) {
	root := t.TempDir()
	for _, n := range []string{"b.lock", "a.lock"} {
		if err := os.WriteFile(filepath.Join(root, n), []byte(n+"\n"), 0o644); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}
	a, err := scratch.ExpandKey(`k-{{ hashFiles "*.lock" }}`, root)
	if err != nil {
		t.Fatalf("ExpandKey: %v", err)
	}
	b, err := scratch.ExpandKey(`k-{{ hashFiles "*.lock" }}`, root)
	if err != nil {
		t.Fatalf("ExpandKey again: %v", err)
	}
	if a != b {
		t.Errorf("a glob hashed to %q then %q, so the walk order reached the key", a, b)
	}
}

// A key that quietly becomes "gomod-" when go.sum is missing would collide
// with every other project's empty key on a shared cache.
func TestExpandKeyRefusesAPatternThatMatchesNothing(t *testing.T) {
	if _, err := scratch.ExpandKey(`k-{{ hashFiles "absent.lock" }}`, t.TempDir()); err == nil {
		t.Error("hashFiles over a pattern matching nothing returned no error")
	}
}

func TestExpandKeyRefusesAnUnknownFunction(t *testing.T) {
	if _, err := scratch.ExpandKey(`k-{{ env "HOME" }}`, t.TempDir()); err == nil {
		t.Error("an unknown template function was accepted")
	}
}

func TestExpandKeyPassesAPlainStringThrough(t *testing.T) {
	got, err := scratch.ExpandKey("plain-key", t.TempDir())
	if err != nil {
		t.Fatalf("ExpandKey: %v", err)
	}
	if got != "plain-key" {
		t.Errorf("ExpandKey = %q, want the literal", got)
	}
}
