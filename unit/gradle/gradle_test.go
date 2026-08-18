package gradle_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/xavidop/senro/internal/unit"
	"github.com/xavidop/senro/unit/gradle"
)

// groovyWS and kotlinWS are the checked-in fixtures: two real Gradle builds
// that gradle 9.7.0 configures, one per DSL. `gradle -q projects` over
// groovyWS reports exactly the nine units TestProjectsDiscoversEveryProject
// expects, container projects included, and `gradle -q :apps:web:dependencies`
// over each reports exactly the chain the transitive tests expect.
//
//	:libs:core  <-  :libs:store  <-  :apps:web       (groovy)
//	:libs:core  <-  :libs:data-store  <-  :apps:web  (kotlin, via accessors)
//	:libs:testkit  <-  :apps:web                     (test-only, both)
const (
	groovyWS = "testdata/groovy-workspace"
	kotlinWS = "testdata/kotlin-workspace"
)

func ids(t *testing.T, g unit.Graph, root string) []string {
	t.Helper()
	us, err := g.Units(context.Background(), root)
	if err != nil {
		t.Fatalf("Units: %v", err)
	}
	out := make([]string, 0, len(us))
	for _, u := range us {
		out = append(out, u.ID)
	}
	return out
}

func affected(t *testing.T, g unit.Graph, root string, files ...string) []string {
	t.Helper()
	res, err := unit.Affected(context.Background(), g, root, files)
	if err != nil {
		t.Fatalf("Affected(%v): %v", files, err)
	}
	out := make([]string, 0, len(res.Units))
	for _, u := range res.Units {
		out = append(out, u.ID)
	}
	return out
}

func same(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}

// write builds a workspace in a temp directory. A key ending in "/" is an
// empty directory, which is how a fixture gets a project directory with no
// build script in it.
func write(t *testing.T, files map[string]string) string {
	t.Helper()
	root := t.TempDir()
	for p, body := range files {
		full := filepath.Join(root, filepath.FromSlash(p))
		if strings.HasSuffix(p, "/") {
			if err := os.MkdirAll(full, 0o755); err != nil {
				t.Fatal(err)
			}
			continue
		}
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return root
}

// refusal asserts that reading root fails with ErrNotDeclarative, and returns
// the message so a caller can check it says something useful.
func refusal(t *testing.T, root string) string {
	t.Helper()
	_, err := gradle.Projects().Units(context.Background(), root)
	if err == nil {
		t.Fatal("Units returned no error over a settings file that is not declarative")
	}
	if !errors.Is(err, gradle.ErrNotDeclarative) {
		t.Fatalf("Units returned %v, want ErrNotDeclarative", err)
	}
	return err.Error()
}

// TestProjectsDiscoversEveryProjectGradleCreates, container projects included.
// ':apps', ':libs' and ':tools' are written down nowhere; Gradle creates them
// out of `include ':apps:web'` and friends, `gradle -q projects` lists them,
// and a graph that reported only the included leaves would leave the
// directories they own unattributed.
func TestProjectsDiscoversEveryProjectGradleCreates(t *testing.T) {
	got := ids(t, gradle.Projects(), groovyWS)
	want := []string{
		".", "apps", "apps/web", "build-tools", "build-tools/codegen",
		"libs", "libs/core", "libs/store", "libs/testkit",
	}
	if !same(got, want) {
		t.Fatalf("Units = %v, want %v", got, want)
	}
}

func TestProjectsReadsTheKotlinDsl(t *testing.T) {
	got := ids(t, gradle.Projects(), kotlinWS)
	want := []string{".", "apps", "apps/web", "libs", "libs/core", "libs/data-store", "libs/testkit"}
	if !same(got, want) {
		t.Fatalf("Units = %v, want %v", got, want)
	}
}

// TestAUnitIsNamedByItsProjectPath, which is what a gradle invocation takes:
// `gradle :libs:core:test`. The directory is the ID, as in every other graph
// here, and the two differ whenever settings.gradle moved a project.
func TestAUnitIsNamedByItsProjectPath(t *testing.T) {
	us, err := gradle.Projects().Units(context.Background(), groovyWS)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{
		".":                   ":",
		"apps":                ":apps",
		"apps/web":            ":apps:web",
		"build-tools":         ":tools",
		"build-tools/codegen": ":tools:codegen",
		"libs":                ":libs",
		"libs/core":           ":libs:core",
		"libs/store":          ":libs:store",
		"libs/testkit":        ":libs:testkit",
	}
	for _, u := range us {
		if want[u.ID] != u.Name {
			t.Errorf("unit %q is named %q, want %q", u.ID, u.Name, want[u.ID])
		}
		if u.Dir != u.ID {
			t.Errorf("unit %q has Dir %q", u.ID, u.Dir)
		}
	}
}

// TestAffectedIsTransitiveOverThreeUnits is the case the whole feature turns
// on, and the one a closure that goes a single hop gets wrong: :apps:web has
// never heard of :libs:core, it depends on :libs:store, and :libs:store
// depends on :libs:core. Real Gradle agrees: `gradle :apps:web:dependencies`
// prints project ':libs:store' with project ':libs:core' under it.
func TestAffectedIsTransitiveOverThreeUnits(t *testing.T) {
	got := affected(t, gradle.Projects(), groovyWS,
		"libs/core/src/main/java/com/example/core/Core.java")
	want := []string{"apps/web", "libs/core", "libs/store", "libs/testkit"}
	if !same(got, want) {
		t.Fatalf("Affected(core) = %v, want %v", got, want)
	}
}

// TestAffectedIsTransitiveThroughATypeSafeAccessor is the same three-deep
// chain in the Kotlin workspace, where the middle edge is written
// `api(projects.libs.core)` and the top one `implementation(projects.libs.dataStore)`.
// Nothing in either file contains the string ":libs:core" or ":libs:data-store".
func TestAffectedIsTransitiveThroughATypeSafeAccessor(t *testing.T) {
	got := affected(t, gradle.Projects(), kotlinWS,
		"libs/core/src/main/java/com/example/core/Core.java")
	want := []string{"apps/web", "libs/core", "libs/data-store", "libs/testkit"}
	if !same(got, want) {
		t.Fatalf("Affected(core) = %v, want %v", got, want)
	}
}

// TestATypeSafeAccessorResolvesToOneProject is what makes the accessor worth
// reading at all, and it is the assertion the transitive test above cannot
// make. An accessor this graph fails to resolve does not vanish: it becomes a
// project that depends on EVERY project, which in the Kotlin workspace happens
// to produce the same affected set as the right answer. Only the edges tell
// the two apart.
//
// It also pins the camelCase mapping. "libs.dataStore" has to come back as
// :libs:data-store, and mapping it to :libs:datastore or leaving it as
// :libs:dataStore resolves to nothing and quietly widens the graph.
func TestATypeSafeAccessorResolvesToOneProject(t *testing.T) {
	rd, err := gradle.Projects().ReverseDeps(context.Background(), kotlinWS)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string][]string{
		"libs/core":       {"libs/data-store", "libs/testkit"},
		"libs/data-store": {"apps/web"},
		"libs/testkit":    {"apps/web"},
		"apps":            {"apps/web"},
		"libs":            {"libs/core", "libs/data-store", "libs/testkit"},
		".":               {"apps", "libs"},
	}
	for k, v := range want {
		if !same(rd[k], v) {
			t.Errorf("ReverseDeps[%q] = %v, want %v", k, rd[k], v)
		}
	}
	if len(rd["apps/web"]) != 0 {
		t.Errorf("ReverseDeps[apps/web] = %v, want nothing", rd["apps/web"])
	}
}

// TestAffectedSeesATestOnlyDependency: nothing but web's testImplementation
// mentions testkit, and a test-only edge is still an edge.
func TestAffectedSeesATestOnlyDependency(t *testing.T) {
	got := affected(t, gradle.Projects(), groovyWS,
		"libs/testkit/src/main/java/com/example/testkit/Fixture.java")
	if want := []string{"apps/web", "libs/testkit"}; !same(got, want) {
		t.Fatalf("Affected(testkit) = %v, want %v", got, want)
	}
}

func TestAffectedDoesNotRunUnitsDownstreamOfNothing(t *testing.T) {
	got := affected(t, gradle.Projects(), groovyWS,
		"apps/web/src/main/java/com/example/web/Web.java")
	if want := []string{"apps/web"}; !same(got, want) {
		t.Fatalf("Affected(web) = %v, want %v", got, want)
	}
}

// TestAProjectDirOverrideMovesTheUnit. settings.gradle puts :tools:codegen in
// build-tools/codegen, `gradle -q projects` confirms it, and a graph that
// assumed a project's directory is its path would have looked in tools/ and
// found nothing there to own this file.
func TestAProjectDirOverrideMovesTheUnit(t *testing.T) {
	got := affected(t, gradle.Projects(), groovyWS,
		"build-tools/codegen/src/main/java/com/example/codegen/Codegen.java")
	if want := []string{"build-tools/codegen"}; !same(got, want) {
		t.Fatalf("Affected(codegen) = %v, want %v", got, want)
	}
}

// TestADefaultProjectDirIgnoresAMovedParent. Gradle computes an unset
// projectDir from the ROOT directory and the whole project path, not from
// whatever its parent's directory was moved to: with only :tools moved,
// gradle 9.7.0 looks for :tools:codegen in tools/codegen and fails because it
// is not there. Guessing the other way round is the natural guess and it is
// wrong.
func TestADefaultProjectDirIgnoresAMovedParent(t *testing.T) {
	root := write(t, map[string]string{
		"settings.gradle": "rootProject.name = 'p'\n" +
			"include ':tools:codegen'\n" +
			"project(':tools').projectDir = file('build-tools')\n",
		"build-tools/":   "",
		"tools/codegen/": "",
	})
	us, err := gradle.Projects().Units(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	dirs := map[string]string{}
	for _, u := range us {
		dirs[u.Name] = u.Dir
	}
	if dirs[":tools"] != "build-tools" {
		t.Errorf(":tools is at %q, want build-tools", dirs[":tools"])
	}
	if dirs[":tools:codegen"] != "tools/codegen" {
		t.Errorf(":tools:codegen is at %q, want tools/codegen: an unset projectDir comes from "+
			"the root and the project path, not from the parent's moved directory",
			dirs[":tools:codegen"])
	}
}

// TestAContainerProjectRunsTheProjectsUnderIt. libs/ has no build script, and
// a change to a file there is still a change to a project every library under
// it is configured by.
func TestAContainerProjectRunsTheProjectsUnderIt(t *testing.T) {
	got := affected(t, gradle.Projects(), groovyWS, "libs/build.gradle")
	want := []string{"apps/web", "libs", "libs/core", "libs/store", "libs/testkit"}
	if !same(got, want) {
		t.Fatalf("Affected(libs/build.gradle) = %v, want %v", got, want)
	}
}

// TestARootFileAffectsEverything. The root build script configures every
// subproject, gradle.properties sets the version they all publish under and
// settings.gradle decides which of them exist at all.
func TestARootFileAffectsEverything(t *testing.T) {
	for _, f := range []string{"build.gradle", "gradle.properties", "settings.gradle", "README.md"} {
		res, err := unit.Affected(context.Background(), gradle.Projects(), groovyWS, []string{f})
		if err != nil {
			t.Fatal(err)
		}
		if !res.All || res.Total != 9 {
			t.Errorf("Affected(%q) = %d of %d units (all=%v), want every one",
				f, len(res.Units), res.Total, res.All)
		}
	}
}

// TestAFileUnderNoProjectBelongsToTheRootProject: docs/ holds no project, so
// the nearest project above it is the root, and every project descends from
// the root.
func TestAFileUnderNoProjectBelongsToTheRootProject(t *testing.T) {
	res, err := unit.Affected(context.Background(), gradle.Projects(), groovyWS,
		[]string{"docs/design.md"})
	if err != nil {
		t.Fatal(err)
	}
	if !res.All {
		t.Fatalf("Affected(docs/design.md) = %v, want everything", res.Units)
	}
}

func TestReverseDepsAreDirectAndDeterministic(t *testing.T) {
	rd, err := gradle.Projects().ReverseDeps(context.Background(), groovyWS)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string][]string{
		".":            {"apps", "build-tools", "libs"},
		"apps":         {"apps/web"},
		"build-tools":  {"build-tools/codegen"},
		"libs":         {"libs/core", "libs/store", "libs/testkit"},
		"libs/core":    {"libs/store", "libs/testkit"},
		"libs/store":   {"apps/web"},
		"libs/testkit": {"apps/web"},
	}
	for k, v := range want {
		if !same(rd[k], v) {
			t.Errorf("ReverseDeps[%q] = %v, want %v", k, rd[k], v)
		}
	}
	for _, leaf := range []string{"apps/web", "build-tools/codegen"} {
		if len(rd[leaf]) != 0 {
			t.Errorf("ReverseDeps[%q] = %v, want nothing", leaf, rd[leaf])
		}
	}
	if same(rd["libs/core"], []string{"apps/web", "libs/store", "libs/testkit"}) {
		t.Error("ReverseDeps[libs/core] includes web; the edges must be direct only")
	}
}

func TestOwnsAnswersForADeletedFile(t *testing.T) {
	got, err := gradle.Projects().Owns(context.Background(), groovyWS,
		[]string{"libs/store/src/main/java/com/example/store/Gone.java"})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || !same(got[0], []string{"libs/store"}) {
		t.Fatalf("Owns = %v", got)
	}
}

// TestACommentedOutIncludeIsNotAnInclude. settings.gradle ends with
// "// include ':ghost'", and a reader that matched the word include without
// stripping comments would report a project that does not exist.
func TestACommentedOutIncludeIsNotAnInclude(t *testing.T) {
	for _, id := range ids(t, gradle.Projects(), groovyWS) {
		if id == "ghost" {
			t.Fatal("a commented-out include produced a unit")
		}
	}
}

// TestALoopInSettingsIsRefused is the whole point of this graph's design. A
// settings file that generates its includes is a program, this reader is not
// an interpreter, and the wrong answer here is a SHORTER project list that
// looks exactly like a correct one.
func TestALoopInSettingsIsRefused(t *testing.T) {
	msg := refusal(t, write(t, map[string]string{
		"settings.gradle": "rootProject.name = 'p'\n" +
			"include ':app'\n" +
			"file('modules').eachDir { dir ->\n" +
			"    include \":modules:${dir.name}\"\n" +
			"}\n",
		"app/":             "",
		"modules/billing/": "",
	}))
	if !strings.Contains(msg, "settings.gradle") {
		t.Errorf("the refusal %q does not name the file", msg)
	}
	if !strings.Contains(msg, "line 3") {
		t.Errorf("the refusal %q does not give the line the reader stopped at", msg)
	}
	if !strings.Contains(msg, "eachDir") {
		t.Errorf("the refusal %q does not quote what it could not read", msg)
	}
	if !strings.Contains(msg, "glob") {
		t.Errorf("the refusal %q does not say what to do instead", msg)
	}
}

// TestARefusalIsNotAPartialList. The refusal above is only worth having if it
// replaces the answer rather than accompanying it: ':app' is perfectly
// readable, and reporting it alone would be the silent wrong answer.
func TestARefusalIsNotAPartialList(t *testing.T) {
	root := write(t, map[string]string{
		"settings.gradle": "include ':app'\n" +
			"for (name in ['billing', 'checkout']) {\n" +
			"    include \":services:$name\"\n" +
			"}\n",
		"app/": "",
	})
	if us, err := gradle.Projects().Units(context.Background(), root); err == nil {
		t.Fatalf("Units returned %v and no error; a partial project list is the wrong answer", us)
	}
	_, err := unit.Affected(context.Background(), gradle.Projects(), root, []string{"app/A.java"})
	if !errors.Is(err, gradle.ErrNotDeclarative) {
		t.Fatalf("Affected returned %v, want ErrNotDeclarative", err)
	}
}

func TestAConditionalIncludeIsRefused(t *testing.T) {
	refusal(t, write(t, map[string]string{
		"settings.gradle": "include ':app'\n" +
			"if (System.getenv('WITH_EXTRAS')) {\n" +
			"    include ':extras'\n" +
			"}\n",
		"app/":    "",
		"extras/": "",
	}))
}

// TestAnInterpolatedIncludePathIsRefused. The statement is an include and the
// argument is a string, but it is not a literal: what it names depends on a
// variable, and a reader that took the text as written would invent a project
// called ":modules:${name}".
func TestAnInterpolatedIncludePathIsRefused(t *testing.T) {
	refusal(t, write(t, map[string]string{
		"settings.gradle.kts": "val name = \"billing\"\ninclude(\":modules:$name\")\n",
		"modules/billing/":    "",
	}))
}

// TestApplyFromInSettingsIsRefused: an applied script can include anything,
// and following it would mean answering for a file this reader has not been
// asked about.
func TestApplyFromInSettingsIsRefused(t *testing.T) {
	refusal(t, write(t, map[string]string{
		"settings.gradle":       "include ':app'\napply from: 'settings-extra.gradle'\n",
		"settings-extra.gradle": "include ':extra'\n",
		"app/":                  "",
		"extra/":                "",
	}))
}

// TestIncludeFlatIsRefused. includeFlat puts a project in a SIBLING of the
// root directory, which is outside the tree senro was pointed at: there is no
// root-relative Dir for it and no changed path that could ever be attributed
// to it.
func TestIncludeFlatIsRefused(t *testing.T) {
	msg := refusal(t, write(t, map[string]string{
		"settings.gradle": "include ':app'\nincludeFlat 'sibling'\n",
		"app/":            "",
	}))
	if !strings.Contains(msg, "includeFlat") {
		t.Errorf("the refusal %q does not name includeFlat", msg)
	}
}

func TestANonLiteralProjectDirIsRefused(t *testing.T) {
	refusal(t, write(t, map[string]string{
		"settings.gradle": "include ':app'\n" +
			"project(':app').projectDir = new File(System.getenv('APP_HOME'))\n",
		"app/": "",
	}))
}

// TestARootDirPrefixedProjectDirIsRead. "$rootDir/x" and "${rootDir}/x" are
// interpolations, but they are the one interpolation whose value this graph
// knows exactly: rootDir is the directory it was handed.
func TestARootDirPrefixedProjectDirIsRead(t *testing.T) {
	root := write(t, map[string]string{
		"settings.gradle": "include ':app'\n" +
			"project(':app').projectDir = file(\"$rootDir/vendor/app\")\n",
		"vendor/app/": "",
	})
	if got := ids(t, gradle.Projects(), root); !same(got, []string{".", "vendor/app"}) {
		t.Fatalf("Units = %v, want the moved app", got)
	}
}

// TestInertSettingsBlocksAreNotRefused. Every real settings file carries
// these, none of them can add a project, and a reader that refused on them
// would refuse on every repository there is.
func TestInertSettingsBlocksAreNotRefused(t *testing.T) {
	root := write(t, map[string]string{
		"settings.gradle.kts": `pluginManagement {
    repositories {
        gradlePluginPortal()
        mavenCentral()
    }
    includeBuild("build-logic")
}

plugins {
    id("com.gradle.develocity") version "3.17"
}

dependencyResolutionManagement {
    repositoriesMode.set(RepositoriesMode.FAIL_ON_PROJECT_REPOS)
    repositories { mavenCentral() }
    versionCatalogs {
        create("libs") { from(files("gradle/libs.versions.toml")) }
    }
}

buildCache {
    local { isEnabled = true }
}

enableFeaturePreview("TYPESAFE_PROJECT_ACCESSORS")

rootProject.name = "acme"

include(":app")
`,
		"app/":         "",
		"build-logic/": "",
	})
	if got := ids(t, gradle.Projects(), root); !same(got, []string{".", "app"}) {
		t.Fatalf("Units = %v, want the root and app", got)
	}
}

func TestNoSettingsFileIsAnError(t *testing.T) {
	root := write(t, map[string]string{"app/build.gradle": "// no settings file anywhere\n"})
	_, err := gradle.Projects().Units(context.Background(), root)
	if err == nil {
		t.Fatal("Units of a tree with no settings file returned no error")
	}
	for _, want := range []string{"settings.gradle", "settings.gradle.kts"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not say it looked for %s", err, want)
		}
	}
}

// TestGroovySettingsWinsOverKotlin, which is what gradle 9.7.0 does: with
// both present it read settings.gradle and never looked at the .kts.
func TestGroovySettingsWinsOverKotlin(t *testing.T) {
	root := write(t, map[string]string{
		"settings.gradle":     "rootProject.name = 'g'\ninclude ':a'\n",
		"settings.gradle.kts": "rootProject.name = \"k\"\ninclude(\":a\")\ninclude(\":b\")\n",
		"a/":                  "",
		"b/":                  "",
	})
	if got := ids(t, gradle.Projects(), root); !same(got, []string{".", "a"}) {
		t.Fatalf("Units = %v, want only what settings.gradle includes", got)
	}
}

// TestBuildGradleWinsOverBuildGradleKts, for the same reason and verified the
// same way: gradle ran the Groovy script and ignored the Kotlin one.
func TestBuildGradleWinsOverBuildGradleKts(t *testing.T) {
	root := write(t, map[string]string{
		"settings.gradle":    "include ':a'\ninclude ':b'\ninclude ':c'\n",
		"a/build.gradle":     "dependencies { implementation project(':b') }\n",
		"a/build.gradle.kts": "dependencies { implementation(project(\":c\")) }\n",
		"b/":                 "",
		"c/":                 "",
	})
	rd, err := gradle.Projects().ReverseDeps(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	if !same(rd["b"], []string{"a"}) {
		t.Errorf("ReverseDeps[b] = %v, want a", rd["b"])
	}
	if len(rd["c"]) != 0 {
		t.Errorf("ReverseDeps[c] = %v; build.gradle.kts is not read when build.gradle is there", rd["c"])
	}
}

// TestAnIncludedProjectWithNoDirectoryIsAnError. Gradle 9 refuses to
// configure one ("Configuring project ':gone' without an existing directory
// is not allowed"), so a graph that reported it would build a step for a
// working directory that does not exist, and a graph that silently dropped it
// would be a project list read as shorter than it is.
func TestAnIncludedProjectWithNoDirectoryIsAnError(t *testing.T) {
	root := write(t, map[string]string{
		"settings.gradle": "include ':app'\ninclude ':gone'\n",
		"app/":            "",
	})
	_, err := gradle.Projects().Units(context.Background(), root)
	if err == nil {
		t.Fatal("Units returned no error for a project whose directory is not on disk")
	}
	if !strings.Contains(err.Error(), ":gone") {
		t.Errorf("error %q does not name the project", err)
	}
}

func TestTwoProjectsInOneDirectoryIsAnError(t *testing.T) {
	root := write(t, map[string]string{
		"settings.gradle": "include ':a'\ninclude ':b'\n" +
			"project(':b').projectDir = file('a')\n",
		"a/": "",
	})
	if _, err := gradle.Projects().Units(context.Background(), root); err == nil {
		t.Fatal("Units returned no error for two projects sharing a directory")
	}
}

func TestAProjectDirOutsideTheRootIsAnError(t *testing.T) {
	root := write(t, map[string]string{
		"settings.gradle": "include ':a'\nproject(':a').projectDir = file('../elsewhere')\n",
		"a/":              "",
	})
	if _, err := gradle.Projects().Units(context.Background(), root); err == nil {
		t.Fatal("Units returned no error for a project outside the root")
	}
}

// TestANonLiteralProjectReferenceMakesTheProjectDependOnEverything. This is
// the build-script half of the same honesty: the dependency is plainly there
// and its target is computed, so the only safe reading is that :app could
// depend on anything, and a change to anything reruns it.
func TestANonLiteralProjectReferenceMakesTheProjectDependOnEverything(t *testing.T) {
	root := write(t, map[string]string{
		"settings.gradle": "include ':app'\ninclude ':a'\ninclude ':b'\n",
		"app/build.gradle": "def which = System.getenv('BACKEND')\n" +
			"dependencies { implementation project(\":$which\") }\n",
		"a/": "",
		"b/": "",
	})
	g := gradle.Projects()
	if got := affected(t, g, root, "a/src/A.java"); !same(got, []string{"a", "app"}) {
		t.Errorf("Affected(a) = %v, want app too", got)
	}
	if got := affected(t, g, root, "b/src/B.java"); !same(got, []string{"app", "b"}) {
		t.Errorf("Affected(b) = %v, want app too", got)
	}
}

// TestADynamicProjectReferenceInTheRootScriptIsRefused is the one place the
// over-approximation above must NOT be taken, and it is unit/maven's
// dependencyManagement lesson in Gradle form. Every project already depends on
// the root, so making the root depend on every project makes every change
// anywhere run the whole repository: an answer that covers the repository is
// not a safe answer, it is the feature switched off while still looking
// switched on. Units keeps working, so a plain fan-out is still available.
func TestADynamicProjectReferenceInTheRootScriptIsRefused(t *testing.T) {
	root := write(t, map[string]string{
		"settings.gradle": "include ':a'\ninclude ':b'\n",
		"build.gradle": "subprojects {\n" +
			"    dependencies { implementation project(System.getenv('SHARED')) }\n}\n",
		"a/": "",
		"b/": "",
	})
	g := gradle.Projects()
	if got := ids(t, g, root); !same(got, []string{".", "a", "b"}) {
		t.Fatalf("Units = %v; discovery is unaffected and must keep working", got)
	}
	_, err := unit.Affected(context.Background(), g, root, []string{"a/A.java"})
	if !errors.Is(err, gradle.ErrNotDeclarative) {
		t.Fatalf("Affected returned %v, want ErrNotDeclarative", err)
	}
	if !strings.Contains(err.Error(), "switched off") {
		t.Errorf("the refusal %q does not say why covering everything is not an answer", err)
	}
}

// TestAnUnknownTypeSafeAccessorMakesTheProjectDependOnEverything, for the
// same reason: "projects.something" that resolves to no project means this
// reader and the build disagree about the project set, and the safe reading
// of a disagreement is that the edge could go anywhere.
func TestAnUnknownTypeSafeAccessorMakesTheProjectDependOnEverything(t *testing.T) {
	root := write(t, map[string]string{
		"settings.gradle":  "include ':app'\ninclude ':a'\n",
		"app/build.gradle": "dependencies { implementation projects.mysteryLib }\n",
		"a/":               "",
	})
	if got := affected(t, gradle.Projects(), root, "a/src/A.java"); !same(got, []string{"a", "app"}) {
		t.Fatalf("Affected(a) = %v, want app too", got)
	}
}

// TestAChangeUnderBuildSrcRunsEverything. buildSrc is a separate build whose
// output is on the classpath of every build script in this one.
func TestAChangeUnderBuildSrcRunsEverything(t *testing.T) {
	root := write(t, map[string]string{
		"settings.gradle":       "include ':a'\ninclude ':b'\n",
		"buildSrc/build.gradle": "plugins { id 'groovy-gradle-plugin' }\n",
		"buildSrc/src/main/groovy/acme.conventions.gradle": "// conventions\n",
		"a/": "",
		"b/": "",
	})
	res, err := unit.Affected(context.Background(), gradle.Projects(), root,
		[]string{"buildSrc/src/main/groovy/acme.conventions.gradle"})
	if err != nil {
		t.Fatal(err)
	}
	if !res.All {
		t.Fatalf("Affected(buildSrc) = %v, want everything", res.Units)
	}
}

// TestAConventionPluginProjectReferenceRunsEverything is the hole this graph
// would otherwise have, and it is an UNDER-approximation, which is the kind
// that lies. :b names no dependency at all, and the convention plugin every
// project applies gives it one on :a. Nothing in b/build.gradle says so.
func TestAConventionPluginProjectReferenceRunsEverything(t *testing.T) {
	root := write(t, map[string]string{
		"settings.gradle":       "include ':a'\ninclude ':b'\n",
		"buildSrc/build.gradle": "plugins { id 'groovy-gradle-plugin' }\n",
		"buildSrc/src/main/groovy/acme.conventions.gradle": "dependencies {\n" +
			"    implementation project(':a')\n}\n",
		"a/": "",
		"b/": "",
	})
	res, err := unit.Affected(context.Background(), gradle.Projects(), root,
		[]string{"a/src/main/java/A.java"})
	if err != nil {
		t.Fatal(err)
	}
	if !res.All {
		t.Fatalf("Affected(a) = %v, want everything: a convention plugin can give :b a "+
			"dependency on :a that b/build.gradle does not mention", res.Units)
	}
}

// TestAChangeUnderAnIncludedBuildRunsEverything, and the point of asserting
// it separately from buildSrc is that an included build can live INSIDE a
// project directory, where the nearest-project rule would otherwise have
// attributed it to that one project alone.
func TestAChangeUnderAnIncludedBuildRunsEverything(t *testing.T) {
	root := write(t, map[string]string{
		"settings.gradle": "include ':app'\ninclude ':other'\n" +
			"includeBuild 'app/build-logic'\n",
		"app/build-logic/settings.gradle": "rootProject.name = 'build-logic'\n",
		"app/build-logic/build.gradle":    "plugins { id 'groovy-gradle-plugin' }\n",
		"other/":                          "",
	})
	res, err := unit.Affected(context.Background(), gradle.Projects(), root,
		[]string{"app/build-logic/build.gradle"})
	if err != nil {
		t.Fatal(err)
	}
	if !res.All {
		t.Fatalf("Affected(included build) = %v, want everything", res.Units)
	}
}

// TestAScriptPluginAppliedFromABuildScriptIsRead. `apply from:` is the
// pre-convention-plugin way to share configuration, and the shared script
// routinely adds dependencies. The path is a literal, so the file is read and
// its edges belong to the project that applied it.
func TestAScriptPluginAppliedFromABuildScriptIsRead(t *testing.T) {
	root := write(t, map[string]string{
		"settings.gradle":    "include ':app'\ninclude ':lib'\ninclude ':other'\n",
		"app/build.gradle":   "apply from: \"$rootDir/gradle/java.gradle\"\n",
		"gradle/java.gradle": "dependencies { implementation project(':lib') }\n",
		"lib/":               "",
		"other/":             "",
	})
	got := affected(t, gradle.Projects(), root, "lib/src/main/java/L.java")
	if want := []string{"app", "lib"}; !same(got, want) {
		t.Fatalf("Affected(lib) = %v, want %v", got, want)
	}
}

// TestAnUnresolvableScriptPluginMakesTheProjectDependOnEverything: the same
// statement with a path this reader cannot work out means the project's
// dependencies are somewhere it cannot see.
func TestAnUnresolvableScriptPluginMakesTheProjectDependOnEverything(t *testing.T) {
	root := write(t, map[string]string{
		"settings.gradle":  "include ':app'\ninclude ':lib'\n",
		"app/build.gradle": "apply from: \"${System.getenv('SHARED')}/java.gradle\"\n",
		"lib/":             "",
	})
	if got := affected(t, gradle.Projects(), root, "lib/L.java"); !same(got, []string{"app", "lib"}) {
		t.Fatalf("Affected(lib) = %v, want app too", got)
	}
}

// TestEvaluationDependsOnIsAnEdge. It is not a dependencies {} declaration,
// and it is exactly as real: the named project is configured first because
// this one reads something from it.
func TestEvaluationDependsOnIsAnEdge(t *testing.T) {
	root := write(t, map[string]string{
		"settings.gradle":  "include ':app'\ninclude ':lib'\n",
		"app/build.gradle": "evaluationDependsOn(':lib')\n",
		"lib/":             "",
	})
	rd, err := gradle.Projects().ReverseDeps(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	if !same(rd["lib"], []string{"app"}) {
		t.Fatalf("ReverseDeps[lib] = %v, want app", rd["lib"])
	}
}

// TestAnIncludeWithoutALeadingColonIsTheSameInclude. Gradle treats
// `include 'a:b'` and `include ':a:b'` identically.
func TestAnIncludeWithoutALeadingColonIsTheSameInclude(t *testing.T) {
	root := write(t, map[string]string{
		"settings.gradle": "include 'libs:core'\n",
		"libs/core/":      "",
	})
	if got := ids(t, gradle.Projects(), root); !same(got, []string{".", "libs", "libs/core"}) {
		t.Fatalf("Units = %v", got)
	}
}

// TestAStringContainingIncludeIsNotAnInclude. The reader has to know where a
// string literal ends, or a repository whose settings file mentions the word
// in passing grows a project out of a comment or a name.
func TestAStringContainingIncludeIsNotAnInclude(t *testing.T) {
	root := write(t, map[string]string{
		"settings.gradle": "rootProject.name = 'include :ghost'\ninclude ':app' // include ':ghost2'\n",
		"app/":            "",
	})
	if got := ids(t, gradle.Projects(), root); !same(got, []string{".", "app"}) {
		t.Fatalf("Units = %v, want the root and app", got)
	}
}

func TestUnitsIsCancellable(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := gradle.Projects().Units(ctx, groovyWS); !errors.Is(err, context.Canceled) {
		t.Fatalf("Units on a cancelled context = %v, want context.Canceled", err)
	}
}

func TestDescribe(t *testing.T) {
	if got := gradle.Projects().Describe(); got != "gradle projects" {
		t.Errorf("Describe = %q", got)
	}
}

func TestUnitIDsAreSafeForAStepID(t *testing.T) {
	for _, ws := range []string{groovyWS, kotlinWS} {
		for _, id := range ids(t, gradle.Projects(), ws) {
			if strings.ContainsAny(id, "[]=,@") {
				t.Errorf("unit id %q would corrupt an expanded step id", id)
			}
		}
	}
}

func TestRootThatIsNotADirectoryIsAnError(t *testing.T) {
	root := write(t, map[string]string{"f.txt": "x"})
	_, err := gradle.Projects().Units(context.Background(), filepath.Join(root, "f.txt"))
	if err == nil {
		t.Fatal("Units of a file returned no error")
	}
}
