package maven_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/xavidop/senro/internal/unit"
	"github.com/xavidop/senro/unit/maven"
)

// workspace is the checked-in fixture: a real Maven reactor that `mvn
// validate` builds in six projects, two of them aggregators.
//
//	core  <-  store  <-  web          (ordinary dependencies)
//	testkit  <-  web                  (a TEST-scoped dependency)
//	acme-parent  <-  libs  <-  core, store, testkit   (parent poms)
//
// store's dependency on core is written with ${project.groupId} and
// ${project.version}, which is how a real multi-module pom writes it and
// which a graph that did not interpolate properties would fail to resolve.
const workspace = "testdata/workspace"

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

func write(t *testing.T, files map[string]string) string {
	t.Helper()
	root := t.TempDir()
	for p, body := range files {
		full := filepath.Join(root, filepath.FromSlash(p))
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return root
}

// pom wraps a body in the boilerplate every pom.xml carries, namespace
// included: a reader that matched element names without ignoring the
// namespace would see an empty project.
func pom(body string) string {
	return `<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
  <modelVersion>4.0.0</modelVersion>
` + body + `
</project>
`
}

func TestModulesDiscoversTheWholeReactor(t *testing.T) {
	got := ids(t, maven.Modules(), workspace)
	want := []string{".", "apps/web", "libs", "libs/core", "libs/store", "libs/testkit"}
	if !same(got, want) {
		t.Fatalf("Units = %v, want %v", got, want)
	}
}

// TestAUnitIsNamedByItsCoordinates, which is what `mvn -pl` takes, and the
// groupId is inherited from the parent where the module does not set one.
func TestAUnitIsNamedByItsCoordinates(t *testing.T) {
	us, err := maven.Modules().Units(context.Background(), workspace)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{
		".":            "com.example:acme-parent",
		"libs":         "com.example:libs",
		"libs/core":    "com.example:core",
		"libs/store":   "com.example:store",
		"libs/testkit": "com.example:testkit",
		"apps/web":     "com.example:web",
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
// on: web has never heard of core, but it depends on store and store depends
// on core.
func TestAffectedIsTransitiveOverThreeUnits(t *testing.T) {
	got := affected(t, maven.Modules(), workspace,
		"libs/core/src/main/java/com/example/core/Core.java")
	want := []string{"apps/web", "libs/core", "libs/store"}
	if !same(got, want) {
		t.Fatalf("Affected(core) = %v, want %v", got, want)
	}
}

// TestAffectedSeesATestScopedDependency: nothing but web's test-scoped
// dependency mentions testkit, and a test-only edge is still an edge.
func TestAffectedSeesATestScopedDependency(t *testing.T) {
	got := affected(t, maven.Modules(), workspace,
		"libs/testkit/src/main/java/com/example/testkit/Fixture.java")
	want := []string{"apps/web", "libs/testkit"}
	if !same(got, want) {
		t.Fatalf("Affected(testkit) = %v, want %v", got, want)
	}
}

// TestAParentPomRunsItsChildren. libs/pom.xml is the parent of three modules
// and the aggregator of the same three; a change to it changes what all of
// them build, and store's own dependent comes along behind them.
func TestAParentPomRunsItsChildren(t *testing.T) {
	got := affected(t, maven.Modules(), workspace, "libs/pom.xml")
	want := []string{"apps/web", "libs", "libs/core", "libs/store", "libs/testkit"}
	if !same(got, want) {
		t.Fatalf("Affected(libs/pom.xml) = %v, want %v", got, want)
	}
}

func TestAffectedDoesNotRunUnitsDownstreamOfNothing(t *testing.T) {
	got := affected(t, maven.Modules(), workspace,
		"apps/web/src/main/java/com/example/web/Web.java")
	if want := []string{"apps/web"}; !same(got, want) {
		t.Fatalf("Affected(web) = %v, want %v", got, want)
	}
}

// TestTheRootPomAffectsEverything: it is the parent of the reactor and it
// holds the dependencyManagement and the properties every module resolves
// against.
func TestTheRootPomAffectsEverything(t *testing.T) {
	for _, f := range []string{"pom.xml", "README.md"} {
		res, err := unit.Affected(context.Background(), maven.Modules(), workspace, []string{f})
		if err != nil {
			t.Fatal(err)
		}
		if !res.All || res.Total != 6 {
			t.Errorf("Affected(%q) = %d of %d units (all=%v), want every one",
				f, len(res.Units), res.Total, res.All)
		}
	}
}

// TestDependencyManagementAloneIsNotAnEdge is the over-approximation this
// graph must NOT make. The root pom manages core's version for the whole
// reactor; treating that as a dependency of the root on core would make every
// change to core run the root, and every module descends from the root, so
// every change to core would run the whole repository and the feature would be
// worth nothing.
func TestDependencyManagementAloneIsNotAnEdge(t *testing.T) {
	rd, err := maven.Modules().ReverseDeps(context.Background(), workspace)
	if err != nil {
		t.Fatal(err)
	}
	for _, dependent := range rd["libs/core"] {
		if dependent == "." {
			t.Fatal("the root pom depends on core; <dependencyManagement> is not a dependency")
		}
	}
}

func TestAFileUnderNoModuleBelongsToTheRootProject(t *testing.T) {
	// docs/ holds no pom, so the nearest project above it is the reactor root,
	// and everything descends from the reactor root.
	res, err := unit.Affected(context.Background(), maven.Modules(), workspace,
		[]string{"docs/design.md"})
	if err != nil {
		t.Fatal(err)
	}
	if !res.All {
		t.Fatalf("Affected(docs/design.md) = %v, want everything", res.Units)
	}
}

func TestOwnsAnswersForADeletedFile(t *testing.T) {
	got, err := maven.Modules().Owns(context.Background(), workspace,
		[]string{"libs/store/src/main/java/com/example/store/Gone.java"})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || !same(got[0], []string{"libs/store"}) {
		t.Fatalf("Owns = %v", got)
	}
}

func TestReverseDepsAreDirectAndDeterministic(t *testing.T) {
	rd, err := maven.Modules().ReverseDeps(context.Background(), workspace)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string][]string{
		"libs/core":    {"libs/store"},
		"libs/store":   {"apps/web"},
		"libs/testkit": {"apps/web"},
		"libs":         {"libs/core", "libs/store", "libs/testkit"},
		".":            {"apps/web", "libs"},
	}
	for k, v := range want {
		if !same(rd[k], v) {
			t.Errorf("ReverseDeps[%q] = %v, want %v", k, rd[k], v)
		}
	}
	if len(rd["apps/web"]) != 0 {
		t.Errorf("ReverseDeps[apps/web] = %v, want nothing", rd["apps/web"])
	}
	if same(rd["libs/core"], []string{"apps/web", "libs/store"}) {
		t.Error("ReverseDeps[libs/core] includes web; the edges must be direct only")
	}
}

// TestAModuleDeclaredOnlyInAProfileIsStillAUnit. A profile can add modules,
// this graph cannot know which profiles a build will activate, and leaving one
// out would be a module nothing ever builds.
func TestAModuleDeclaredOnlyInAProfileIsStillAUnit(t *testing.T) {
	root := write(t, map[string]string{
		"pom.xml": pom(`  <groupId>com.example</groupId>
  <artifactId>root</artifactId>
  <version>1.0.0</version>
  <packaging>pom</packaging>
  <modules><module>a</module></modules>
  <profiles>
    <profile>
      <id>extras</id>
      <modules><module>b</module></modules>
    </profile>
  </profiles>`),
		"a/pom.xml": pom(`  <parent><groupId>com.example</groupId><artifactId>root</artifactId>
    <version>1.0.0</version></parent>
  <artifactId>a</artifactId>`),
		"b/pom.xml": pom(`  <parent><groupId>com.example</groupId><artifactId>root</artifactId>
    <version>1.0.0</version></parent>
  <artifactId>b</artifactId>
  <dependencies>
    <dependency><groupId>com.example</groupId><artifactId>a</artifactId>
      <version>1.0.0</version></dependency>
  </dependencies>`),
	})
	g := maven.Modules()
	if got := ids(t, g, root); !same(got, []string{".", "a", "b"}) {
		t.Fatalf("Units = %v", got)
	}
	if got := affected(t, g, root, "a/src/main/java/A.java"); !same(got, []string{"a", "b"}) {
		t.Fatalf("Affected = %v, want the profile module too", got)
	}
}

// TestAnImportedBomIsAnEdge: <scope>import</scope> inside
// dependencyManagement is the one entry there that IS a dependency, because
// the importing pom reads the imported one to build at all.
func TestAnImportedBomIsAnEdge(t *testing.T) {
	root := write(t, map[string]string{
		"pom.xml": pom(`  <groupId>com.example</groupId>
  <artifactId>root</artifactId>
  <version>1.0.0</version>
  <packaging>pom</packaging>
  <modules><module>bom</module><module>app</module></modules>`),
		"bom/pom.xml": pom(`  <parent><groupId>com.example</groupId><artifactId>root</artifactId>
    <version>1.0.0</version></parent>
  <artifactId>bom</artifactId>
  <packaging>pom</packaging>`),
		"app/pom.xml": pom(`  <parent><groupId>com.example</groupId><artifactId>root</artifactId>
    <version>1.0.0</version></parent>
  <artifactId>app</artifactId>
  <dependencyManagement>
    <dependencies>
      <dependency><groupId>com.example</groupId><artifactId>bom</artifactId>
        <version>1.0.0</version><type>pom</type><scope>import</scope></dependency>
    </dependencies>
  </dependencyManagement>`),
	})
	rd, err := maven.Modules().ReverseDeps(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	if !same(rd["bom"], []string{"app"}) {
		t.Fatalf("ReverseDeps[bom] = %v, want app", rd["bom"])
	}
}

// TestAPluginBuiltInTheReactorIsAnEdge: a repository that builds its own
// Maven plugin and uses it has to rebuild the users of the plugin when the
// plugin changes.
func TestAPluginBuiltInTheReactorIsAnEdge(t *testing.T) {
	root := write(t, map[string]string{
		"pom.xml": pom(`  <groupId>com.example</groupId>
  <artifactId>root</artifactId>
  <version>1.0.0</version>
  <packaging>pom</packaging>
  <modules><module>plugin</module><module>app</module></modules>`),
		"plugin/pom.xml": pom(`  <parent><groupId>com.example</groupId><artifactId>root</artifactId>
    <version>1.0.0</version></parent>
  <artifactId>acme-maven-plugin</artifactId>
  <packaging>maven-plugin</packaging>`),
		"app/pom.xml": pom(`  <parent><groupId>com.example</groupId><artifactId>root</artifactId>
    <version>1.0.0</version></parent>
  <artifactId>app</artifactId>
  <build>
    <plugins>
      <plugin><groupId>com.example</groupId><artifactId>acme-maven-plugin</artifactId>
        <version>1.0.0</version></plugin>
    </plugins>
  </build>`),
	})
	rd, err := maven.Modules().ReverseDeps(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	if !same(rd["plugin"], []string{"app"}) {
		t.Fatalf("ReverseDeps[plugin] = %v, want app", rd["plugin"])
	}
}

// TestADependencyWithAnUnresolvableGroupIdStillMatches. A groupId written as
// a property this graph cannot resolve leaves the artifactId as the only
// evidence, and matching on it alone runs MORE, which is the direction every
// ambiguity here goes.
func TestADependencyWithAnUnresolvableGroupIdStillMatches(t *testing.T) {
	root := write(t, map[string]string{
		"pom.xml": pom(`  <groupId>com.example</groupId>
  <artifactId>root</artifactId>
  <version>1.0.0</version>
  <packaging>pom</packaging>
  <modules><module>a</module><module>b</module></modules>`),
		"a/pom.xml": pom(`  <parent><groupId>com.example</groupId><artifactId>root</artifactId>
    <version>1.0.0</version></parent>
  <artifactId>a</artifactId>`),
		"b/pom.xml": pom(`  <parent><groupId>com.example</groupId><artifactId>root</artifactId>
    <version>1.0.0</version></parent>
  <artifactId>b</artifactId>
  <dependencies>
    <dependency><groupId>${some.unknown.group}</groupId><artifactId>a</artifactId>
      <version>1.0.0</version></dependency>
  </dependencies>`),
	})
	if got := affected(t, maven.Modules(), root, "a/src/main/java/A.java"); !same(got,
		[]string{"a", "b"}) {
		t.Fatalf("Affected = %v, want both", got)
	}
}

// TestAParentThatIsNotTheAggregatorStillRunsItsChildren isolates the <parent>
// edge from the <modules> one. In the checked-in fixture libs is both the
// aggregator and the parent of the same three modules, so either edge alone
// gets that right; here the root aggregates both projects and neither
// aggregates the other, so only <parent> connects them. Dropping the parent
// edge reddens this and nothing else, which is exactly what makes it worth
// having.
func TestAParentThatIsNotTheAggregatorStillRunsItsChildren(t *testing.T) {
	root := write(t, map[string]string{
		"pom.xml": pom(`  <groupId>com.example</groupId>
  <artifactId>root</artifactId><version>1.0.0</version><packaging>pom</packaging>
  <modules><module>build-parent</module><module>app</module></modules>`),
		"build-parent/pom.xml": pom(`  <parent><groupId>com.example</groupId><artifactId>root</artifactId>
    <version>1.0.0</version></parent>
  <artifactId>build-parent</artifactId>
  <packaging>pom</packaging>`),
		"app/pom.xml": pom(`  <parent><groupId>com.example</groupId><artifactId>build-parent</artifactId>
    <version>1.0.0</version><relativePath>../build-parent/pom.xml</relativePath></parent>
  <artifactId>app</artifactId>`),
	})
	got := affected(t, maven.Modules(), root, "build-parent/pom.xml")
	if want := []string{"app", "build-parent"}; !same(got, want) {
		t.Fatalf("Affected(build-parent/pom.xml) = %v, want %v", got, want)
	}
}

// TestAnAggregatorThatIsNotTheParentStillRunsItsModules is the other half of
// the pair, and the case <modules> exists for: every module here inherits the
// root build-parent directly, so the intermediate aggregator is nobody's
// <parent>, and only its <modules> list connects it to what it groups.
func TestAnAggregatorThatIsNotTheParentStillRunsItsModules(t *testing.T) {
	root := write(t, map[string]string{
		"pom.xml": pom(`  <groupId>com.example</groupId>
  <artifactId>root</artifactId><version>1.0.0</version><packaging>pom</packaging>
  <modules><module>group</module></modules>`),
		"group/pom.xml": pom(`  <parent><groupId>com.example</groupId><artifactId>root</artifactId>
    <version>1.0.0</version></parent>
  <artifactId>group</artifactId>
  <packaging>pom</packaging>
  <modules><module>a</module></modules>`),
		"group/a/pom.xml": pom(`  <parent><groupId>com.example</groupId><artifactId>root</artifactId>
    <version>1.0.0</version><relativePath>../../pom.xml</relativePath></parent>
  <artifactId>a</artifactId>`),
	})
	got := affected(t, maven.Modules(), root, "group/pom.xml")
	if want := []string{"group", "group/a"}; !same(got, want) {
		t.Fatalf("Affected(group/pom.xml) = %v, want %v", got, want)
	}
}

// TestInterpolationIsWhatMakesTheAnswerPRECISE. Two modules share an
// artifactId under different groupIds, which is legal and which the
// artifactId-only fallback cannot tell apart: without interpolating
// ${project.groupId} the dependency matches both, and a change to the one
// nothing depends on would run a module it never reaches.
func TestInterpolationIsWhatMakesTheAnswerPrecise(t *testing.T) {
	root := write(t, map[string]string{
		"pom.xml": pom(`  <groupId>com.example</groupId>
  <artifactId>root</artifactId><version>1.0.0</version><packaging>pom</packaging>
  <modules><module>mine</module><module>theirs</module><module>app</module></modules>`),
		"mine/pom.xml": pom(`  <parent><groupId>com.example</groupId><artifactId>root</artifactId>
    <version>1.0.0</version></parent>
  <artifactId>shared</artifactId>`),
		"theirs/pom.xml": pom(`  <parent><groupId>com.example</groupId><artifactId>root</artifactId>
    <version>1.0.0</version></parent>
  <groupId>org.other</groupId>
  <artifactId>shared</artifactId>`),
		"app/pom.xml": pom(`  <parent><groupId>com.example</groupId><artifactId>root</artifactId>
    <version>1.0.0</version></parent>
  <artifactId>app</artifactId>
  <dependencies>
    <dependency><groupId>${project.groupId}</groupId><artifactId>shared</artifactId>
      <version>${project.version}</version></dependency>
  </dependencies>`),
	})
	rd, err := maven.Modules().ReverseDeps(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	if !same(rd["mine"], []string{"app"}) {
		t.Errorf("ReverseDeps[mine] = %v, want app", rd["mine"])
	}
	if len(rd["theirs"]) != 0 {
		t.Errorf("ReverseDeps[theirs] = %v; ${project.groupId} is com.example, not org.other",
			rd["theirs"])
	}
}

func TestNoRootPomIsAnError(t *testing.T) {
	root := write(t, map[string]string{"libs/core/pom.xml": pom(`  <groupId>x</groupId>
  <artifactId>core</artifactId><version>1</version>`)})
	_, err := maven.Modules().Units(context.Background(), root)
	if err == nil {
		t.Fatal("Units of a tree with no root pom returned no error")
	}
	if !strings.Contains(err.Error(), "pom.xml") {
		t.Errorf("error %q does not say what it looked for", err)
	}
}

// TestAMissingModuleIsAnError: <modules> is unconditional and Maven itself
// fails on it, and a reactor read as smaller than it is is a build that skips
// what it did not see.
func TestAMissingModuleIsAnError(t *testing.T) {
	root := write(t, map[string]string{
		"pom.xml": pom(`  <groupId>com.example</groupId>
  <artifactId>root</artifactId><version>1.0.0</version><packaging>pom</packaging>
  <modules><module>gone</module></modules>`),
	})
	if _, err := maven.Modules().Units(context.Background(), root); err == nil {
		t.Fatal("Units returned no error for a module that is not on disk")
	}
}

func TestAMalformedPomIsAnError(t *testing.T) {
	root := write(t, map[string]string{
		"pom.xml": pom(`  <groupId>com.example</groupId>
  <artifactId>root</artifactId><version>1.0.0</version><packaging>pom</packaging>
  <modules><module>a</module></modules>`),
		"a/pom.xml": "<project><artifactId>a</artifactId>",
	})
	_, err := maven.Modules().Units(context.Background(), root)
	if err == nil {
		t.Fatal("Units over a malformed pom returned no error")
	}
	if !strings.Contains(err.Error(), "a/pom.xml") {
		t.Errorf("error %q does not name the file", err)
	}
}

func TestRootThatIsNotADirectoryIsAnError(t *testing.T) {
	root := write(t, map[string]string{"f.txt": "x"})
	if _, err := maven.Modules().Units(context.Background(), filepath.Join(root, "f.txt")); err == nil {
		t.Fatal("Units of a file returned no error")
	}
}

func TestUnitsIsCancellable(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := maven.Modules().Units(ctx, workspace); !errors.Is(err, context.Canceled) {
		t.Fatalf("Units on a cancelled context = %v, want context.Canceled", err)
	}
}

func TestDescribe(t *testing.T) {
	if got := maven.Modules().Describe(); got != "maven modules" {
		t.Errorf("Describe = %q", got)
	}
}

func TestUnitIDsAreSafeForAStepID(t *testing.T) {
	for _, id := range ids(t, maven.Modules(), workspace) {
		if strings.ContainsAny(id, "[]=,@") {
			t.Errorf("unit id %q would corrupt an expanded step id", id)
		}
	}
}
