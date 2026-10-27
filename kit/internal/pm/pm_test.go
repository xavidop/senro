package pm

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
		want     Manager
	}{
		{"pnpm", "pnpm-lock.yaml", ".", Manager{"pnpm", []string{"pnpm", "exec"}}},
		{"yarn", "yarn.lock", ".", Manager{"yarn", []string{"yarn"}}},
		{"bun", "bun.lockb", ".", Manager{"bun", []string{"bunx"}}},
		{"bun text lockfile", "bun.lock", ".", Manager{"bun", []string{"bunx"}}},
		{"npm", "package-lock.json", ".", Manager{"npm", []string{"npx"}}},
		{"lockfile in parent", "pnpm-lock.yaml", "apps/web", Manager{"pnpm", []string{"pnpm", "exec"}}},
		{"none", "", "apps/web", Manager{"npm", []string{"npx"}}},
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

func TestDetectNearestLockfileWins(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "apps", "web")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	for p, name := range map[string]string{root: "pnpm-lock.yaml", dir: "yarn.lock"} {
		if err := os.WriteFile(filepath.Join(p, name), nil, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if got := Detect(dir, root); got.Name != "yarn" {
		t.Fatalf("got %+v, want yarn", got)
	}
}
