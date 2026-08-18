package senro_test

import (
	"context"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/xavidop/senro"
	"github.com/xavidop/senro/duration"
	"github.com/xavidop/senro/exec"
	"github.com/xavidop/senro/unit/glob"
)

// shardTemplate builds a step whose command IS the shard's unit list, so a
// test can read what landed in each shard straight off the plan.
func shardTemplate(sh senro.Shard) *senro.StepBuilder {
	return senro.NewStep(exec.Command("echo", strings.Join(sh.IDs(), " ")))
}

// shardsOf is what each shard of an expansion covers, in shard order.
func shardsOf(t *testing.T, pl *senro.Plan, expansion string) []string {
	t.Helper()
	var out []string
	for i := 0; ; i++ {
		n, ok := pl.Node(expansion + "[shard=" + itoa(i) + "]")
		if !ok {
			return out
		}
		if len(n.Cmd) != 2 {
			t.Fatalf("shard %d has command %v, and this test's template makes it the unit list",
				i, n.Cmd)
		}
		out = append(out, n.Cmd[1])
	}
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b []byte
	for n > 0 {
		b = append([]byte{byte('0' + n%10)}, b...)
		n /= 10
	}
	return string(b)
}

// writeHistory writes a duration history file under root.
func writeHistory(t *testing.T, root, path string, expansion string, secs map[string]int) {
	t.Helper()
	pairs := make([]string, 0, len(secs))
	keys := make([]string, 0, len(secs))
	for k := range secs {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		pairs = append(pairs, `"`+k+`":`+itoa(secs[k])+`000000000`)
	}
	body := `{"version":1,"expansions":{"` + expansion + `":{` + strings.Join(pairs, ",") + `}}}`
	full := filepath.Join(root, path)
	if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(full, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
}

// TestPartitionMakesOneStepPerShardAndCoversEveryUnit.
func TestPartitionMakesOneStepPerShardAndCoversEveryUnit(t *testing.T) {
	root := t.TempDir()
	mkdirs(t, root, "apps/a", "apps/b", "apps/c", "apps/d", "apps/e", "apps/f")
	t.Chdir(root)

	p := senro.New("mono")
	w := p.Workflow("verify")
	w.Expand("test", glob.Dirs("apps/*")).
		MaxParallel(2).
		Partition(3, duration.None()).
		TemplateShard(shardTemplate)

	pl, err := p.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	got := shardsOf(t, pl, "test")
	if len(got) != 3 {
		t.Fatalf("Partition(3) built %d shards: %v; nodes are %v", len(got), got, nodeIDs(pl))
	}
	seen := map[string]int{}
	for _, s := range got {
		for _, id := range strings.Fields(s) {
			seen[id]++
		}
	}
	if len(seen) != 6 {
		t.Errorf("the shards cover %d of 6 units: %v", len(seen), got)
	}
	for id, n := range seen {
		if n != 1 {
			t.Errorf("unit %q is in %d shards", id, n)
		}
	}
	// A shard is a child of the expansion exactly as a per-unit step is, so
	// every existing thing that keys off a group still works on it.
	for i := 0; i < 3; i++ {
		n, _ := pl.Node("test[shard=" + itoa(i) + "]")
		if n.Group != "test" {
			t.Errorf("shard %d has group %q", i, n.Group)
		}
	}
	if g, ok := pl.Group("test"); !ok || g.MaxParallel != 2 {
		t.Errorf("group = %+v, ok %v", g, ok)
	}
}

// TestPartitionBalancesByTheRecordedDurations is the point: with the slow
// module recorded, it gets a shard to itself instead of dragging one down.
// Split by count instead (which is what no history gives) the same six
// modules would come out three and three.
func TestPartitionBalancesByTheRecordedDurations(t *testing.T) {
	root := t.TempDir()
	mkdirs(t, root, "apps/a", "apps/b", "apps/c", "apps/d")
	t.Chdir(root)
	writeHistory(t, root, ".senro/durations.json", "test", map[string]int{
		"apps/a": 30, "apps/b": 2, "apps/c": 2, "apps/d": 2,
	})

	p := senro.New("mono")
	w := p.Workflow("verify")
	w.Expand("test", glob.Dirs("apps/*")).
		Partition(2, duration.FromFile(".senro/durations.json")).
		TemplateShard(shardTemplate)

	pl, err := p.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	got := shardsOf(t, pl, "test")
	want := []string{"apps/a", "apps/b apps/c apps/d"}
	if !sameStrings(got, want) {
		t.Errorf("shards = %v, want %v: the thirty-second module should be alone", got, want)
	}
}

// TestPartitionOnTheFirstRunHasNoHistoryAndStillBuilds is the cold start
// through the public surface. There is no file, and the expected behaviour is
// a plan, not a refusal: a design that only works on the second run looks
// broken on the first.
func TestPartitionOnTheFirstRunHasNoHistoryAndStillBuilds(t *testing.T) {
	root := t.TempDir()
	mkdirs(t, root, "apps/a", "apps/b", "apps/c", "apps/d")
	t.Chdir(root)

	p := senro.New("mono")
	w := p.Workflow("verify")
	w.Expand("test", glob.Dirs("apps/*")).
		Partition(2, duration.FromFile(".senro/durations.json")).
		TemplateShard(shardTemplate)

	pl, err := p.Build()
	if err != nil {
		t.Fatalf("Build with no history file: %v; the first run of every pipeline is this", err)
	}
	got := shardsOf(t, pl, "test")
	want := []string{"apps/a apps/c", "apps/b apps/d"}
	if !sameStrings(got, want) {
		t.Errorf("cold shards = %v, want the even split by count %v", got, want)
	}
}

// TestPartitionWithOneUnitMissingFromTheHistory is the state a repository is
// in a week after the file was written: somebody added a module and nothing
// has timed it. It must build, cover everything, and not treat the unknown
// module as free.
func TestPartitionWithOneUnitMissingFromTheHistory(t *testing.T) {
	root := t.TempDir()
	mkdirs(t, root, "apps/slow", "apps/mid", "apps/quick", "apps/new")
	t.Chdir(root)
	writeHistory(t, root, "d.json", "test", map[string]int{
		"apps/slow": 20, "apps/mid": 5, "apps/quick": 1,
	})

	p := senro.New("mono")
	w := p.Workflow("verify")
	w.Expand("test", glob.Dirs("apps/*")).
		Partition(2, duration.FromFile("d.json")).
		TemplateShard(shardTemplate)

	pl, err := p.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	got := shardsOf(t, pl, "test")
	if len(got) != 2 {
		t.Fatalf("shards = %v, want 2", got)
	}
	var all []string
	for _, s := range got {
		all = append(all, strings.Fields(s)...)
	}
	sort.Strings(all)
	want := []string{"apps/mid", "apps/new", "apps/quick", "apps/slow"}
	if !sameStrings(all, want) {
		t.Errorf("the shards cover %v, want %v", all, want)
	}
	// The unrecorded module is estimated at the median of what is known, so
	// it does not join the twenty-second one.
	if got[0] != "apps/slow" {
		t.Errorf("shard 0 = %q, want the slow module alone", got[0])
	}
}

// TestPartitionStepIdentifiersDoNotDependOnTheHistory is the determinism rule,
// through the public surface and stated exactly: two machines holding two
// different histories build the same SET of step identifiers for one
// repository. Which unit is in which shard does change, and that is checked
// too, so the assertion above is not passing because nothing moved.
func TestPartitionStepIdentifiersDoNotDependOnTheHistory(t *testing.T) {
	root := t.TempDir()
	mkdirs(t, root, "apps/a", "apps/b", "apps/c", "apps/d", "apps/e")
	t.Chdir(root)
	writeHistory(t, root, "lopsided.json", "test", map[string]int{
		"apps/a": 1, "apps/b": 1, "apps/c": 1, "apps/d": 1, "apps/e": 90,
	})

	build := func(history senro.DurationHistory) *senro.Plan {
		t.Helper()
		p := senro.New("mono")
		w := p.Workflow("verify")
		w.Expand("test", glob.Dirs("apps/*")).
			Partition(2, history).
			TemplateShard(shardTemplate)
		pl, err := p.Build()
		if err != nil {
			t.Fatalf("Build: %v", err)
		}
		return pl
	}
	cold, warm := build(duration.None()), build(duration.FromFile("lopsided.json"))

	if !sameStrings(nodeIDs(cold), nodeIDs(warm)) {
		t.Errorf("two histories built two sets of step ids: %v and %v",
			nodeIDs(cold), nodeIDs(warm))
	}
	if sameStrings(shardsOf(t, cold, "test"), shardsOf(t, warm, "test")) {
		t.Fatalf("both histories produced the same shards (%v), so this test proves nothing; "+
			"make the recorded durations more lopsided", shardsOf(t, cold, "test"))
	}
}

// TestPartitionBuildsTheSamePlanTwice: the shards reach a step's command and
// its declared inputs, so two builds that disagreed would be two plans, two
// digests and two sets of cache keys from one repository.
func TestPartitionBuildsTheSamePlanTwice(t *testing.T) {
	root := t.TempDir()
	mkdirs(t, root, "apps/a", "apps/b", "apps/c", "apps/d", "apps/e", "apps/f", "apps/g")
	t.Chdir(root)
	writeHistory(t, root, "d.json", "test", map[string]int{
		"apps/a": 9, "apps/b": 9, "apps/c": 3, "apps/d": 3, "apps/e": 1, "apps/f": 1,
	})

	build := func() *senro.Plan {
		p := senro.New("mono")
		w := p.Workflow("verify")
		w.Expand("test", glob.Dirs("apps/*")).
			Partition(3, duration.FromFile("d.json")).
			TemplateShard(shardTemplate)
		pl, err := p.Build()
		if err != nil {
			t.Fatalf("Build: %v", err)
		}
		return pl
	}
	first, second := build().Digest(), build().Digest()
	if first != second {
		t.Fatal("two builds of one partitioned pipeline produced two digests")
	}
}

// TestPartitionIntoMoreShardsThanUnitsBuildsNoEmptyOnes. A step with nothing
// in it would run the template's command over an empty list, which for most
// commands means "everything" or an error, and neither is what the pipeline
// asked for.
func TestPartitionIntoMoreShardsThanUnitsBuildsNoEmptyOnes(t *testing.T) {
	root := t.TempDir()
	mkdirs(t, root, "apps/a", "apps/b")
	t.Chdir(root)

	p := senro.New("mono")
	w := p.Workflow("verify")
	w.Expand("test", glob.Dirs("apps/*")).
		Partition(8, duration.None()).
		TemplateShard(shardTemplate)

	pl, err := p.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if got := shardsOf(t, pl, "test"); !sameStrings(got, []string{"apps/a", "apps/b"}) {
		t.Errorf("shards = %v, want one per unit and no empties", got)
	}
}

// TestAPartitionedExpansionThatMatchesNothingIsAnEmptyGroup: the same nothing
// an unpartitioned expansion over an empty graph already produces.
func TestAPartitionedExpansionThatMatchesNothingIsAnEmptyGroup(t *testing.T) {
	root := t.TempDir()
	mkdirs(t, root, "apps/a")
	t.Chdir(root)

	p := senro.New("mono")
	w := p.Workflow("verify")
	w.Expand("test", glob.Dirs("nothing/*")).
		Partition(4, duration.None()).
		TemplateShard(shardTemplate)

	pl, err := p.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if len(pl.Nodes) != 0 {
		t.Errorf("an empty partitioned expansion built %v", nodeIDs(pl))
	}
	if _, ok := pl.Group("test"); !ok {
		t.Error("the empty expansion declares no group, so nothing can report it as skipped")
	}
}

// TestPartitionStillChecksMaxNodesAgainstTheWholeGraph: partitioning is not a
// way around the guard. A glob that matches forty thousand directories is a
// mistake worth refusing whether or not the plan would have collapsed them
// into eight steps.
func TestPartitionStillChecksMaxNodesAgainstTheWholeGraph(t *testing.T) {
	root := t.TempDir()
	for i := 0; i < 12; i++ {
		mkdirs(t, root, "apps/a"+itoa(i))
	}
	t.Chdir(root)

	p := senro.New("mono")
	w := p.Workflow("verify")
	w.Expand("test", glob.Dirs("apps/*")).
		MaxNodes(10).
		Partition(2, duration.None()).
		TemplateShard(shardTemplate)

	_, err := p.Build()
	if err == nil {
		t.Fatal("a partitioned expansion of 12 units was accepted under MaxNodes(10)")
	}
	if !strings.Contains(err.Error(), "12") {
		t.Errorf("the error does not name the count: %v", err)
	}
}

// TestNeedsEachPairsAPartitionedExpansionByUnitSet: a shard covers several
// units, so it waits on every upstream child that covers any of them, which
// is the same rule the one-unit case is a special case of.
func TestNeedsEachPairsAPartitionedExpansionByUnitSet(t *testing.T) {
	root := t.TempDir()
	mkdirs(t, root, "apps/a", "apps/b", "apps/c", "apps/d")
	t.Chdir(root)

	p := senro.New("mono")
	w := p.Workflow("verify")
	w.Expand("build", glob.Dirs("apps/*")).
		Template(func(u senro.Unit) *senro.StepBuilder {
			return senro.NewStep(exec.Command("echo", u.ID))
		})
	w.Expand("test", glob.Dirs("apps/*")).
		Partition(2, duration.None()).
		NeedsEach("build").
		TemplateShard(shardTemplate)

	pl, err := p.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	// The cold split of a, b, c, d into two is [a c] and [b d].
	for i, want := range [][]string{
		{"build[unit=apps/a]", "build[unit=apps/c]"},
		{"build[unit=apps/b]", "build[unit=apps/d]"},
	} {
		id := "test[shard=" + itoa(i) + "]"
		if got := sortedNeedsOf(t, pl, id); !sameStrings(got, want) {
			t.Errorf("%s needs %v, want %v", id, got, want)
		}
	}
}

// TestPartitionRecordedFromARealRunBalancesTheNextBuild is the whole loop:
// run the expansion per-unit, fold the ledger into a history file, and build
// it again partitioned. It is what the documentation tells somebody to do, so
// it is worth proving it works end to end rather than only in pieces.
func TestPartitionRecordedFromARealRunBalancesTheNextBuild(t *testing.T) {
	root := t.TempDir()
	mkdirs(t, root, "apps/slow", "apps/a", "apps/b", "apps/c")
	t.Chdir(root)

	perUnit := senro.New("mono")
	w := perUnit.Workflow("verify")
	w.Expand("test", glob.Dirs("apps/*")).
		Template(func(u senro.Unit) *senro.StepBuilder {
			if u.Base() == "slow" {
				return senro.NewStep(exec.Command("sh", "-c", "sleep 0.6"))
			}
			return senro.NewStep(exec.Command("true"))
		})

	runDir := t.TempDir()
	if err := senro.Run(context.Background(), perUnit,
		senro.WithDir(runDir), senro.WithRunID("record-1"), senro.WithCacheDir(t.TempDir()),
	); err != nil {
		t.Fatalf("Run: %v", err)
	}
	path := filepath.Join(root, ".senro", "durations.json")
	if err := duration.Record(runDir, path); err != nil {
		t.Fatalf("Record: %v", err)
	}
	recorded, err := duration.FromFile(path).Durations(context.Background(), root, "test")
	if err != nil {
		t.Fatalf("reading back what was recorded: %v", err)
	}
	if recorded["apps/slow"] < 500*time.Millisecond {
		t.Fatalf("the slow unit was recorded as %v, and it slept for 600ms; the fold missed it",
			recorded["apps/slow"])
	}

	partitioned := senro.New("mono")
	pw := partitioned.Workflow("verify")
	pw.Expand("test", glob.Dirs("apps/*")).
		Partition(2, duration.FromFile(".senro/durations.json")).
		TemplateShard(shardTemplate)

	pl, err := partitioned.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	got := shardsOf(t, pl, "test")
	want := []string{"apps/slow", "apps/a apps/b apps/c"}
	if !sameStrings(got, want) {
		t.Errorf("shards = %v, want %v: the recorded history did not reach the partition", got, want)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// What Build refuses
// ─────────────────────────────────────────────────────────────────────────────

func buildErr(t *testing.T, configure func(*senro.ExpandBuilder)) error {
	t.Helper()
	root := t.TempDir()
	mkdirs(t, root, "apps/a", "apps/b")
	t.Chdir(root)

	p := senro.New("mono")
	w := p.Workflow("verify")
	configure(w.Expand("test", glob.Dirs("apps/*")))
	_, err := p.Build()
	return err
}

// TestBuildRefusesATemplateAndAShardTemplateTogether: one expansion makes
// either one step per unit or one step per shard, and a pipeline declaring
// both has said two different things about what it wants.
func TestBuildRefusesATemplateAndAShardTemplateTogether(t *testing.T) {
	err := buildErr(t, func(e *senro.ExpandBuilder) {
		e.Partition(2, duration.None()).
			TemplateShard(shardTemplate).
			Template(func(u senro.Unit) *senro.StepBuilder {
				return senro.NewStep(exec.Command("true"))
			})
	})
	if err == nil {
		t.Fatal("Build accepted both templates")
	}
	if !strings.Contains(err.Error(), "Template") || !strings.Contains(err.Error(), "TemplateShard") {
		t.Errorf("the error names neither method: %v", err)
	}
}

// TestBuildRefusesAShardTemplateWithNoPartition: without a partition there are
// no shards, so the template would never be called and the expansion would
// silently produce nothing.
func TestBuildRefusesAShardTemplateWithNoPartition(t *testing.T) {
	err := buildErr(t, func(e *senro.ExpandBuilder) { e.TemplateShard(shardTemplate) })
	if err == nil {
		t.Fatal("Build accepted a TemplateShard with no Partition")
	}
	if !strings.Contains(err.Error(), "Partition") {
		t.Errorf("the error does not name Partition: %v", err)
	}
}

// TestBuildRefusesAPartitionWithNoShardTemplate: the per-unit Template takes a
// Unit and a shard is several, so there is nothing to call.
func TestBuildRefusesAPartitionWithNoShardTemplate(t *testing.T) {
	err := buildErr(t, func(e *senro.ExpandBuilder) {
		e.Partition(2, duration.None()).
			Template(func(u senro.Unit) *senro.StepBuilder {
				return senro.NewStep(exec.Command("true"))
			})
	})
	if err == nil {
		t.Fatal("Build accepted a Partition with only a per-unit Template")
	}
	if !strings.Contains(err.Error(), "TemplateShard") {
		t.Errorf("the error does not name TemplateShard: %v", err)
	}
}

// TestBuildRefusesANilHistory, and says what to write instead, for the same
// reason Affected refuses a nil change source: a nil there reads as an
// oversight, and "there is no history" is worth saying out loud.
func TestBuildRefusesANilHistory(t *testing.T) {
	err := buildErr(t, func(e *senro.ExpandBuilder) {
		e.Partition(2, nil).TemplateShard(shardTemplate)
	})
	if err == nil {
		t.Fatal("Build accepted a nil duration history")
	}
	if !strings.Contains(err.Error(), "duration.None()") {
		t.Errorf("the error does not say what to write instead: %v", err)
	}
}

func TestBuildRefusesAPartitionOfNoShards(t *testing.T) {
	for _, n := range []int{0, -1} {
		err := buildErr(t, func(e *senro.ExpandBuilder) {
			e.Partition(n, duration.None()).TemplateShard(shardTemplate)
		})
		if err == nil {
			t.Errorf("Build accepted Partition(%d)", n)
		}
	}
}

func TestBuildRefusesTwoPartitions(t *testing.T) {
	err := buildErr(t, func(e *senro.ExpandBuilder) {
		e.Partition(2, duration.None()).Partition(3, duration.None()).TemplateShard(shardTemplate)
	})
	if err == nil {
		t.Fatal("Build accepted two Partition calls")
	}
}

func TestBuildRefusesTwoShardTemplates(t *testing.T) {
	err := buildErr(t, func(e *senro.ExpandBuilder) {
		e.Partition(2, duration.None()).TemplateShard(shardTemplate).TemplateShard(shardTemplate)
	})
	if err == nil {
		t.Fatal("Build accepted two TemplateShard calls")
	}
}

// TestBuildReportsAHistoryItCannotRead by name. A corrupt committed file must
// fail the build rather than quietly reverting the whole fleet to balancing
// by count, whose only symptom is a fan-out slower than it was.
func TestBuildReportsAHistoryItCannotRead(t *testing.T) {
	root := t.TempDir()
	mkdirs(t, root, "apps/a", "apps/b")
	t.Chdir(root)
	if err := os.WriteFile(filepath.Join(root, "d.json"), []byte("{oops"), 0o644); err != nil {
		t.Fatal(err)
	}

	p := senro.New("mono")
	w := p.Workflow("verify")
	w.Expand("test", glob.Dirs("apps/*")).
		Partition(2, duration.FromFile("d.json")).
		TemplateShard(shardTemplate)

	_, err := p.Build()
	if err == nil {
		t.Fatal("Build read a corrupt history as an empty one")
	}
	if !strings.Contains(err.Error(), "test") || !strings.Contains(err.Error(), "d.json") {
		t.Errorf("the error names neither the expansion nor the history: %v", err)
	}
}

// TestAnUnpartitionedFanOutIsUnchanged: Partition is an addition. Everything
// that does not call it must build exactly the plan it always built.
func TestAnUnpartitionedFanOutIsUnchanged(t *testing.T) {
	root := t.TempDir()
	mkdirs(t, root, "apps/web", "apps/api")
	t.Chdir(root)

	p := senro.New("mono")
	w := p.Workflow("verify")
	w.Expand("build", glob.Dirs("apps/*")).
		Needs("install").
		Template(func(u senro.Unit) *senro.StepBuilder {
			return senro.NewStep(exec.Command("echo", "build", u.Base()))
		})
	w.Step("install", exec.Command("echo", "install"))

	pl, err := p.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	const want = "sha256:6b09e75ef5b4e0eaa5e84d4ee31e378d254dc9d679d3737ef2a3f3ffb33cae57"
	if got := pl.Digest(); got != want {
		t.Errorf("a fan-out that never partitions digests %s, want %s", got, want)
	}
}
