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
