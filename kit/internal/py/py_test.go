package py

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestDetect(t *testing.T) {
	cases := []struct {
		name     string
		lockfile string
		dir      string
		want     Runner
	}{
		{"uv", "uv.lock", ".", Runner{"uv", []string{"uv", "run"}}},
		{"poetry", "poetry.lock", ".", Runner{"poetry", []string{"poetry", "run"}}},
		{"lockfile in parent", "uv.lock", "services/api", Runner{"uv", []string{"uv", "run"}}},
		{"none", "", "services/api", Runner{"python", []string{"python", "-m"}}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			root := t.TempDir()
			dir := filepath.Join(root, filepath.FromSlash(c.dir))
			if err := os.MkdirAll(dir, 0o755); err != nil {
				t.Fatal(err)
			}
			if c.lockfile != "" {
				if err := os.WriteFile(filepath.Join(root, c.lockfile), nil, 0o644); err != nil {
					t.Fatal(err)
				}
			}
			if got := Detect(dir, root); !reflect.DeepEqual(got, c.want) {
				t.Fatalf("got %+v, want %+v", got, c.want)
			}
		})
	}
}

func TestDetectStopsAtARootWithATrailingSlash(t *testing.T) {
	parent := t.TempDir()
	root := filepath.Join(parent, "repo")
	dir := filepath.Join(root, "services", "api")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(parent, "uv.lock"), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	if got := Detect(dir, root+"/"); got.Name != "python" {
		t.Fatalf("got %+v, want python: the search went past root", got)
	}
}

func TestDetectLockfilePrecedence(t *testing.T) {
	dir := t.TempDir()
	for _, f := range []string{"uv.lock", "poetry.lock"} {
		if err := os.WriteFile(filepath.Join(dir, f), nil, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if got := Detect(dir, dir); got.Name != "uv" {
		t.Fatalf("got %+v, want uv before poetry", got)
	}
}
