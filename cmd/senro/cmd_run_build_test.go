package main

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

// TestBuildPipelineBuildsANestedModule is senro run ./ci from a parent with
// no go.mod of its own, where ci/ holds its own module: the layout senro init
// writes for every repository that is not a Go module.
func TestBuildPipelineBuildsANestedModule(t *testing.T) {
	goBin, err := exec.LookPath("go")
	if err != nil {
		t.Skip("no go on PATH")
	}
	repo, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	parent := t.TempDir()
	ci := filepath.Join(parent, "ci")
	if err := os.MkdirAll(ci, 0o755); err != nil {
		t.Fatal(err)
	}
	mod := "module parent/ci\n\ngo 1.26\n\nreplace github.com/xavidop/senro => " + repo + "\n"
	if err := os.WriteFile(filepath.Join(ci, "go.mod"), []byte(mod), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(ci, "main.go"), []byte("package main\n\nfunc main() {}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Setenv("GOFLAGS", "-mod=mod")
	t.Setenv("GOPROXY", "off")
	t.Chdir(parent)

	for _, pkg := range []string{"./ci", "ci", ci} {
		var stderr bytes.Buffer
		bin, cleanup, err := buildPipeline(context.Background(), goBin, pkg, &stderr)
		if err != nil {
			t.Fatalf("buildPipeline(%q): %v\n%s", pkg, err, stderr.String())
		}
		if _, err := os.Stat(bin); err != nil {
			t.Errorf("buildPipeline(%q): no binary: %v", pkg, err)
		}
		cleanup()
	}
}
