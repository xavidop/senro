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

func TestDetectStopsAtARootWithATrailingSlash(t *testing.T) {
	parent := t.TempDir()
	root := filepath.Join(parent, "repo")
	dir := filepath.Join(root, "apps", "web")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(parent, "pnpm-lock.yaml"), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	if got := Detect(dir, root+"/"); got.Name != "npm" {
		t.Fatalf("got %+v, want npm: the search went past root", got)
	}
}

func TestDetectLockfilePrecedence(t *testing.T) {
	order := []struct{ file, name string }{
		{"pnpm-lock.yaml", "pnpm"}, {"yarn.lock", "yarn"}, {"bun.lockb", "bun"}, {"package-lock.json", "npm"},
	}
	for i := range order {
		dir := t.TempDir()
		for _, l := range order[i:] {
			if err := os.WriteFile(filepath.Join(dir, l.file), nil, 0o644); err != nil {
				t.Fatal(err)
			}
		}
		if got := Detect(dir, dir); got.Name != order[i].name {
			t.Errorf("with %s and everything after it: got %s, want %s", order[i].file, got.Name, order[i].name)
		}
	}
}
