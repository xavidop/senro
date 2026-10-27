package kit

import (
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
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

func TestWalkSkipsAnUnreadableDirectory(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("directory permissions do not deny reads on Windows")
	}
	if os.Geteuid() == 0 {
		t.Skip("root reads a directory whatever its mode")
	}
	root := t.TempDir()
	mkdirs(t, root, "a/hidden", "b")
	locked := filepath.Join(root, "a")
	if err := os.Chmod(locked, 0); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(locked, 0o755) })
	var got []string
	if err := Walk(root, func(_, rel string) error {
		got = append(got, rel)
		return nil
	}); err != nil {
		t.Fatalf("an unreadable directory stopped the walk: %v", err)
	}
	if want := []string{".", "a", "b"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("visited %v, want %v", got, want)
	}
}

func TestWalkRefusesAMissingRoot(t *testing.T) {
	root := filepath.Join(t.TempDir(), "nope")
	calls := 0
	err := Walk(root, func(_, _ string) error {
		calls++
		return nil
	})
	if err == nil || !strings.Contains(err.Error(), root) {
		t.Fatalf("err = %v, want one naming %s", err, root)
	}
	if calls != 0 {
		t.Fatalf("fn called %d times", calls)
	}
}

func TestWalkRefusesAFileRoot(t *testing.T) {
	root := filepath.Join(t.TempDir(), "f.txt")
	if err := os.WriteFile(root, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	err := Walk(root, func(_, _ string) error { return nil })
	if err == nil || !strings.Contains(err.Error(), root) {
		t.Fatalf("err = %v, want one naming %s", err, root)
	}
}

func TestWalkVisitsInLexicalOrder(t *testing.T) {
	root := t.TempDir()
	mkdirs(t, root, "b", "a", "c")
	var got []string
	if err := Walk(root, func(_, rel string) error {
		got = append(got, rel)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if want := []string{".", "a", "b", "c"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("visited %v, want %v", got, want)
	}
}

func TestFirstFile(t *testing.T) {
	dir := t.TempDir()
	mkdirs(t, dir, "dir.cfg")
	for _, n := range []string{"b.cfg", "c.cfg"} {
		if err := os.WriteFile(filepath.Join(dir, n), nil, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if got, ok := FirstFile(dir, "a.cfg", "dir.cfg", "c.cfg", "b.cfg"); !ok || got != "c.cfg" {
		t.Fatalf("got %q, %v; want c.cfg (given order, directories skipped)", got, ok)
	}
	if got, ok := FirstFile(dir, "a.cfg"); ok || got != "" {
		t.Fatalf("got %q, %v; want none", got, ok)
	}
	if _, ok := FirstFile(dir); ok {
		t.Fatal("no names must find nothing")
	}
}

func TestUnitDir(t *testing.T) {
	for in, want := range map[string]string{"": "", ".": "", "svc/api": "svc/api", "./x": "./x"} {
		if got := UnitDir(in); got != want {
			t.Errorf("UnitDir(%q) = %q, want %q", in, got, want)
		}
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
