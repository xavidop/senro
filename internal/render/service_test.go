package render_test

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/xavidop/senro/internal/render"
	"github.com/xavidop/senro/internal/source"
)

// A service still up at the snapshot is named once with where it listens. The
// fixture is cut at service.ready so db has not yet finished.
func TestPlainNamesAServicesEndpoints(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "..", "api", "testdata", "fixtures", "service-and-hold.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "events.jsonl"), []byte(strings.Join(lines[:6], "\n")+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	fs, err := source.OpenFile(dir, false)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	defer func() { _ = fs.Close() }()

	var buf bytes.Buffer
	if _, err := render.Plain(context.Background(), fs, &buf); err != nil {
		t.Fatalf("Plain: %v", err)
	}
	if n := strings.Count(buf.String(), "db ready at 127.0.0.1:5432 (port 5432)\n"); n != 1 {
		t.Errorf("want the endpoint named exactly once, got %d:\n%s", n, buf.String())
	}
}
