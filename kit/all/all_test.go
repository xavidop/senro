package all

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/xavidop/senro/kit"
)

func write(t *testing.T, root, rel, body string) {
	t.Helper()
	p := filepath.Join(root, filepath.FromSlash(rel))
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestDetectReturnsEveryKitsMatchesSorted(t *testing.T) {
	root := t.TempDir()
	write(t, root, "go.mod", "module example.com/x\n\ngo 1.22\n")
	write(t, root, "playwright.config.ts", "export default {};\n")
	got, err := Detect(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	want := []kit.Match{
		{Kit: "gotest", Dir: ".", Reason: "go.mod"},
		{Kit: "playwright", Dir: ".", Reason: "playwright.config.ts"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestDetectStopsOnACancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := Detect(ctx, t.TempDir()); err == nil {
		t.Fatal("Detect ran on a cancelled context")
	}
}

func TestNamesListsTheFourKitsInOrder(t *testing.T) {
	want := []string{"gotest", "pytest", "eslint", "playwright"}
	if got := Names(); !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}
