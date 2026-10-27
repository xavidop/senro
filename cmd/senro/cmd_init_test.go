package main

import (
	"bytes"
	"context"
	"errors"
	"go/format"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/xavidop/senro/kit"
	"github.com/xavidop/senro/kit/all"
)

// initFixture is the absolute path of a fixture under testdata/init.
func initFixture(t *testing.T, name string) string {
	t.Helper()
	dir, err := filepath.Abs(filepath.Join("testdata", "init", name))
	if err != nil {
		t.Fatal(err)
	}
	return dir
}

// copyInitFixture copies a fixture into a fresh temp directory named after
// it, so cmdInit can write next to it and the module name is predictable.
func copyInitFixture(t *testing.T, name string) string {
	t.Helper()
	src := initFixture(t, name)
	dst := filepath.Join(t.TempDir(), name)
	err := filepath.WalkDir(src, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		target := filepath.Join(dst, rel)
		if d.IsDir() {
			return os.MkdirAll(target, 0o755)
		}
		b, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		return os.WriteFile(target, b, 0o644)
	})
	if err != nil {
		t.Fatal(err)
	}
	return dst
}

func renderFixture(t *testing.T, name string) string {
	t.Helper()
	ms, err := all.Detect(context.Background(), initFixture(t, name))
	if err != nil {
		t.Fatal(err)
	}
	out, err := renderInit(name, ms)
	if err != nil {
		t.Fatal(err)
	}
	return string(out)
}

func TestInitRendersOneExpandForAGoMonorepo(t *testing.T) {
	out := renderFixture(t, "gomono")
	for _, want := range []string{
		`verify.Expand("gotest", gowork.Modules()).Template(gotest.Template())`,
		`"github.com/xavidop/senro/unit/gowork"`,
		`"github.com/xavidop/senro/kit/gotest"`,
		`senro.New("gomono")`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("output lacks %s:\n%s", want, out)
		}
	}
	for _, bad := range []string{"verify.Add(", "kit/pytest"} {
		if strings.Contains(out, bad) {
			t.Errorf("output contains %s:\n%s", bad, out)
		}
	}
}

func TestInitRendersAddForASingleMatch(t *testing.T) {
	out := renderFixture(t, "single")
	if !strings.Contains(out, `verify.Add("gotest", gotest.Step())`) {
		t.Errorf("no single gotest Add:\n%s", out)
	}
	if strings.Contains(out, "unit/gowork") {
		t.Errorf("imports unit/gowork for a single module:\n%s", out)
	}
}

func TestInitGeneratedMainPrintsRunsError(t *testing.T) {
	out := renderFixture(t, "single")
	if !strings.Contains(out, "log.Print(err)\n\t\tos.Exit(1)") {
		t.Errorf("senro.Run's error is not printed before os.Exit(1):\n%s", out)
	}
}

func TestInitRendersDirForNestedMatches(t *testing.T) {
	out := renderFixture(t, "web")
	for _, want := range []string{
		`verify.Add("eslint", eslint.Step())`,
		`verify.Add("playwright", playwright.Step(playwright.Dir("e2e")))`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("output lacks %s:\n%s", want, out)
		}
	}
}

func TestInitDocCommentListsTheMatches(t *testing.T) {
	out := renderFixture(t, "web")
	for _, want := range []string{
		"senro init wrote this file",
		"//\teslint     .          eslint.config.js\n",
		"//\tplaywright e2e        playwright.config.ts\n",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("output lacks %q:\n%s", want, out)
		}
	}
}

func TestInitRenderedFileIsGofmtClean(t *testing.T) {
	for _, name := range []string{"gomono", "single", "web", "py", "existing"} {
		out := renderFixture(t, name)
		formatted, err := format.Source([]byte(out))
		if err != nil {
			t.Fatalf("%s: %v\n%s", name, err, out)
		}
		if string(formatted) != out {
			t.Errorf("%s: not gofmt clean:\n%s", name, out)
		}
	}
}

func TestInitRenderIsDeterministicAndOrdered(t *testing.T) {
	ms := []kit.Match{
		{Kit: "playwright", Dir: ".", Reason: "playwright.config.ts"},
		{Kit: "eslint", Dir: "web/b", Reason: "eslint.config.js"},
		{Kit: "eslint", Dir: ".", Reason: "eslint.config.js"},
		{Kit: "eslint", Dir: "web/a", Reason: "eslint.config.js"},
		{Kit: "eslint", Dir: "other/a", Reason: "eslint.config.js"},
		{Kit: "pytest", Dir: ".", Reason: "pytest.ini"},
	}
	first, err := renderInit("x", ms)
	if err != nil {
		t.Fatal(err)
	}
	out := string(first)
	order := []string{
		`verify.Add("pytest", pytest.Step())`,
		`verify.Add("eslint", eslint.Step())`,
		`verify.Add("eslint-a", eslint.Step(eslint.Dir("other/a")))`,
		`verify.Add("eslint-a-2", eslint.Step(eslint.Dir("web/a")))`,
		`verify.Add("eslint-b", eslint.Step(eslint.Dir("web/b")))`,
		`verify.Add("playwright", playwright.Step())`,
	}
	last := -1
	for _, want := range order {
		i := strings.Index(out, want)
		if i < 0 {
			t.Fatalf("output lacks %s:\n%s", want, out)
		}
		if i < last {
			t.Fatalf("%s is out of order:\n%s", want, out)
		}
		last = i
	}
	rev := append([]kit.Match(nil), ms...)
	for i, j := 0, len(rev)-1; i < j; i, j = i+1, j-1 {
		rev[i], rev[j] = rev[j], rev[i]
	}
	second, err := renderInit("x", rev)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(first, second) {
		t.Fatalf("render depends on match order:\n%s\n---\n%s", first, second)
	}
}

func TestInitRefusesAnExistingPipeline(t *testing.T) {
	dir := copyInitFixture(t, "existing")
	mainGo := filepath.Join(dir, "ci", "main.go")
	before, err := os.ReadFile(mainGo)
	if err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	if code := cmdInit([]string{dir}, &stdout, &stderr); code != exitUsage {
		t.Fatalf("exit %d, want %d; stderr:\n%s", code, exitUsage, stderr.String())
	}
	msg := stderr.String()
	if !strings.Contains(msg, filepath.Join("ci", "main.go")) || !strings.Contains(msg, "--force") {
		t.Errorf("stderr does not name ci/main.go and --force:\n%s", msg)
	}
	after, err := os.ReadFile(mainGo)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) {
		t.Error("ci/main.go was rewritten")
	}
	if _, err := os.Stat(filepath.Join(dir, "ci", "go.mod")); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("ci/go.mod exists (or stat failed: %v)", err)
	}
}

func TestInitExitsTwoWhenNothingIsDetected(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if code := cmdInit([]string{initFixture(t, "empty")}, &stdout, &stderr); code != exitUsage {
		t.Fatalf("exit %d, want %d; stderr:\n%s", code, exitUsage, stderr.String())
	}
	for _, k := range []string{"gotest", "pytest", "eslint", "playwright"} {
		if !strings.Contains(stderr.String(), k) {
			t.Errorf("stderr does not mention %s:\n%s", k, stderr.String())
		}
	}
}

func TestInitDryRunWritesNothing(t *testing.T) {
	dir := copyInitFixture(t, "single")
	var stdout, stderr bytes.Buffer
	if code := cmdInit([]string{dir, "--dry-run"}, &stdout, &stderr); code != exitSuccess {
		t.Fatalf("exit %d; stderr:\n%s", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), "package main") {
		t.Errorf("stdout lacks the file:\n%s", stdout.String())
	}
	if _, err := os.Stat(filepath.Join(dir, "ci")); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("ci/ exists after --dry-run (or stat failed: %v)", err)
	}
}

func TestInitNoTidyWritesTheModuleAndPrintsTheCommands(t *testing.T) {
	dir := copyInitFixture(t, "py")
	var stdout, stderr bytes.Buffer
	if code := cmdInit([]string{dir, "--no-tidy", "--out", "pipeline"}, &stdout, &stderr); code != exitSuccess {
		t.Fatalf("exit %d; stderr:\n%s", code, stderr.String())
	}
	mod, err := os.ReadFile(filepath.Join(dir, "pipeline", "go.mod"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(mod), "module py/pipeline\n") {
		t.Errorf("go.mod:\n%s", mod)
	}
	msg := stderr.String()
	for _, want := range []string{"go get github.com/xavidop/senro@", "go mod tidy", "senro run ./pipeline", "pytest"} {
		if !strings.Contains(msg, want) {
			t.Errorf("stderr lacks %q:\n%s", want, msg)
		}
	}
}

func TestParseInitOptions(t *testing.T) {
	o, err := parseInitOptions(nil)
	if err != nil {
		t.Fatal(err)
	}
	if o != (initOptions{dir: ".", out: "ci"}) {
		t.Fatalf("defaults = %+v", o)
	}
	o, err = parseInitOptions([]string{"repo", "--out=pipe", "--force", "--dry-run", "--no-tidy"})
	if err != nil {
		t.Fatal(err)
	}
	if o != (initOptions{dir: "repo", out: "pipe", force: true, dryRun: true, noTidy: true}) {
		t.Fatalf("parsed = %+v", o)
	}
	for _, bad := range [][]string{{"--bogus"}, {"a", "b"}, {"--out"}, {"--out="}} {
		if _, err := parseInitOptions(bad); err == nil {
			t.Errorf("%v accepted", bad)
		}
	}
}

func TestInitGeneratedPipelineBuilds(t *testing.T) {
	goBin, err := exec.LookPath("go")
	if err != nil {
		t.Skip("no go on PATH")
	}
	if testing.Short() {
		t.Skip("builds a generated module")
	}
	repo, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("SENRO_INIT_REPLACE", repo)
	dir := copyInitFixture(t, "py")
	var stdout, stderr bytes.Buffer
	if code := cmdInit([]string{dir}, &stdout, &stderr); code != exitSuccess {
		t.Fatalf("exit %d; stderr:\n%s", code, stderr.String())
	}
	if strings.Contains(stderr.String(), "warning") {
		t.Fatalf("init warned:\n%s", stderr.String())
	}
	mod, err := os.ReadFile(filepath.Join(dir, "ci", "go.mod"))
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"replace github.com/xavidop/senro => " + repo, "github.com/xavidop/senro v"} {
		if !strings.Contains(string(mod), want) {
			t.Errorf("go.mod lacks %q:\n%s", want, mod)
		}
	}
	if !strings.Contains(string(mod), "require") {
		t.Errorf("go.mod has no require:\n%s", mod)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, goBin, "build", "./...")
	cmd.Dir = filepath.Join(dir, "ci")
	if b, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("go build: %v\n%s", err, b)
	}
}

func TestInitOutOutsideDirGetsItsOwnModule(t *testing.T) {
	dir := copyInitFixture(t, "single")
	out := filepath.Join(t.TempDir(), "pipe")
	before, err := os.ReadFile(filepath.Join(dir, "go.mod"))
	if err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	if code := cmdInit([]string{dir, "--out", out, "--no-tidy"}, &stdout, &stderr); code != exitSuccess {
		t.Fatalf("exit %d; stderr:\n%s", code, stderr.String())
	}
	mod, err := os.ReadFile(filepath.Join(out, "go.mod"))
	if err != nil {
		t.Fatalf("no go.mod written in an --out outside DIR: %v\n%s", err, stderr.String())
	}
	if !strings.Contains(string(mod), "module single/pipe\n") {
		t.Errorf("go.mod:\n%s", mod)
	}
	if !strings.Contains(stderr.String(), "Run these in "+out) {
		t.Errorf("commands not aimed at --out:\n%s", stderr.String())
	}
	after, err := os.ReadFile(filepath.Join(dir, "go.mod"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) {
		t.Error("DIR's go.mod changed")
	}
}

func TestInitRefusesAnOutThatIsAFile(t *testing.T) {
	dir := copyInitFixture(t, "py")
	ci := filepath.Join(dir, "ci")
	if err := os.WriteFile(ci, []byte("not a directory\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	if code := cmdInit([]string{dir}, &stdout, &stderr); code != exitUsage {
		t.Fatalf("exit %d, want %d; stderr:\n%s", code, exitUsage, stderr.String())
	}
	msg := stderr.String()
	if !strings.Contains(msg, "ci") || !strings.Contains(msg, "not a directory") || strings.Contains(msg, "detected") {
		t.Errorf("stderr:\n%s", msg)
	}
	if b, err := os.ReadFile(ci); err != nil || string(b) != "not a directory\n" {
		t.Errorf("ci changed: %q, %v", b, err)
	}
	if _, err := os.Stat(filepath.Join(dir, "go.mod")); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("go.mod written (or stat failed: %v)", err)
	}
}

func TestInitReportsUpdatingAnExistingModule(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("the fake go is a shell script")
	}
	bin := t.TempDir()
	log := filepath.Join(bin, "calls")
	script := "#!/bin/sh\necho \"$PWD: $*\" >> " + log + "\n"
	if err := os.WriteFile(filepath.Join(bin, "go"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin)
	dir := copyInitFixture(t, "single")
	var stdout, stderr bytes.Buffer
	if code := cmdInit([]string{dir}, &stdout, &stderr); code != exitSuccess {
		t.Fatalf("exit %d; stderr:\n%s", code, stderr.String())
	}
	if !strings.Contains(stderr.String(), "Updated go.mod: go get github.com/xavidop/senro@") {
		t.Errorf("no Updated line:\n%s", stderr.String())
	}
	calls, err := os.ReadFile(log)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(calls), "get github.com/xavidop/senro@") || !strings.Contains(string(calls), "mod tidy") {
		t.Errorf("go calls:\n%s", calls)
	}
}

func TestShellQuote(t *testing.T) {
	for in, want := range map[string]string{
		"./ci":         "./ci",
		"/tmp/a b":     "'/tmp/a b'",
		"it's":         `'it'\''s'`,
		"":             "''",
		"repo/sub-dir": "repo/sub-dir",
	} {
		if got := shellQuote(in); got != want {
			t.Errorf("shellQuote(%q) = %s, want %s", in, got, want)
		}
	}
}
