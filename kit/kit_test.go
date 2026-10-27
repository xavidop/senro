package kit

import (
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func mkdirs(t *testing.T, root string, dirs ...string) {
	t.Helper()
	for _, d := range dirs {
		if err := os.MkdirAll(filepath.Join(root, filepath.FromSlash(d)), 0o755); err != nil {
			t.Fatal(err)
		}
	}
}

func TestWalkSkipsIgnoredDirectories(t *testing.T) {
	root := t.TempDir()
	mkdirs(t, root, "a/node_modules/x", ".git", "_gen", "b/vendor", "b/testdata", "b/ok")
	var got []string
	err := Walk(root, func(_, rel string) error {
		got = append(got, rel)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{".", "a", "b", "b/ok"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("visited %v, want %v", got, want)
	}
}

func TestWalkHonoursSkipDir(t *testing.T) {
	root := t.TempDir()
	mkdirs(t, root, "a/ok", "b")
	var got []string
	err := Walk(root, func(_, rel string) error {
		got = append(got, rel)
		if rel == "a" {
			return fs.SkipDir
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{".", "a", "b"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("visited %v, want %v", got, want)
	}
}

func TestWalkVisitsRootEvenWhenItsNameIsSkipped(t *testing.T) {
	root := filepath.Join(t.TempDir(), "node_modules")
	mkdirs(t, root, "a")
	var got []string
	if err := Walk(root, func(_, rel string) error {
		got = append(got, rel)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if want := []string{".", "a"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("visited %v, want %v", got, want)
	}
}

func TestReportPath(t *testing.T) {
	if got := ReportPath("pytest"); got != ".senro/pytest.xml" {
		t.Fatalf("got %q", got)
	}
}

func TestSortMatches(t *testing.T) {
	ms := []Match{{Kit: "pytest", Dir: "."}, {Kit: "gotest", Dir: "svc"}, {Kit: "gotest", Dir: "."}}
	SortMatches(ms)
	want := []Match{{Kit: "gotest", Dir: "."}, {Kit: "gotest", Dir: "svc"}, {Kit: "pytest", Dir: "."}}
	if !reflect.DeepEqual(ms, want) {
		t.Fatalf("got %v, want %v", ms, want)
	}
}
