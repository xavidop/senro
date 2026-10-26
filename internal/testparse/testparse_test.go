package testparse

import (
	"os"
	"strings"
	"testing"
	"time"

	"github.com/xavidop/senro/api"
)

func collect() (*[]Result, Emit) {
	var got []Result
	return &got, func(r Result) { got = append(got, r) }
}

func byName(rs []Result, name string) Result {
	for _, r := range rs {
		if r.Name == name {
			return r
		}
	}
	return Result{}
}

func feed(t *testing.T, s Stream, in string) {
	t.Helper()
	n, err := s.Write([]byte(in))
	if err != nil || n != len(in) {
		t.Fatalf("Write = %d, %v; want %d, nil", n, err, len(in))
	}
}

func TestGoTestEmitsOnePerTestWithOutputOnFailure(t *testing.T) {
	data, err := os.ReadFile("testdata/gotest.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	got, emit := collect()
	s := NewGoTest(emit)
	feed(t, s, string(data))
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if len(*got) != 4 {
		t.Fatalf("got %d results, want 4: %+v", len(*got), *got)
	}
	bad := byName(*got, "TestBad")
	if bad.Outcome != api.TestFailed || bad.Suite != "example.com/m/a" || bad.Duration != 20*time.Millisecond {
		t.Errorf("TestBad = %+v", bad)
	}
	if want := "--- FAIL: TestBad (0.02s)\n    a_test.go:9: nope\n"; bad.Output != want {
		t.Errorf("TestBad.Output = %q, want %q", bad.Output, want)
	}
	if ok := byName(*got, "TestOK"); ok.Outcome != api.TestPassed || ok.Output != "" {
		t.Errorf("TestOK = %+v", ok)
	}
	if sk := byName(*got, "TestSkip"); sk.Outcome != api.TestSkipped {
		t.Errorf("TestSkip = %+v", sk)
	}
	if sub := byName(*got, "TestBad/sub"); sub.Outcome != api.TestPassed {
		t.Errorf("TestBad/sub = %+v", sub)
	}
}

func TestGoTestIgnoresLinesThatAreNotJSON(t *testing.T) {
	got, emit := collect()
	s := NewGoTest(emit)
	feed(t, s, "go: downloading x\n")
	feed(t, s, `{"Action":"pass","Package":"p","Test":"TestA","Elapsed":0}`+"\n")
	if len(*got) != 1 {
		t.Fatalf("got %d results, want 1", len(*got))
	}
}

func TestGoTestHandlesWritesSplitMidLine(t *testing.T) {
	got, emit := collect()
	s := NewGoTest(emit)
	line := `{"Action":"pass","Package":"p","Test":"TestÜ","Elapsed":0}` + "\n"
	cut := strings.Index(line, "Ü") + 1 // inside the two-byte rune
	feed(t, s, line[:cut])
	if len(*got) != 0 {
		t.Fatal("emitted before the line was complete")
	}
	feed(t, s, line[cut:])
	if len(*got) != 1 || (*got)[0].Name != "TestÜ" {
		t.Fatalf("got %+v", *got)
	}
}

func TestGoTestCloseFlushesATrailingLineWithoutNewline(t *testing.T) {
	got, emit := collect()
	s := NewGoTest(emit)
	feed(t, s, `{"Action":"pass","Package":"p","Test":"TestA","Elapsed":0}`)
	if len(*got) != 0 {
		t.Fatal("emitted before Close")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if len(*got) != 1 {
		t.Fatalf("got %d results, want 1", len(*got))
	}
}

func TestGoTestBoundsAccumulatedOutput(t *testing.T) {
	got, emit := collect()
	s := NewGoTest(emit)
	feed(t, s, `{"Action":"run","Package":"p","Test":"TestA"}`+"\n")
	chunk := strings.Repeat("x", 100)
	for i := 0; i < 2000; i++ {
		feed(t, s, `{"Action":"output","Package":"p","Test":"TestA","Output":"`+chunk+`\n"}`+"\n")
	}
	feed(t, s, `{"Action":"output","Package":"p","Test":"TestA","Output":"last line\n"}`+"\n")
	feed(t, s, `{"Action":"fail","Package":"p","Test":"TestA","Elapsed":1}`+"\n")
	out := (*got)[0].Output
	if len(out) > api.MaxTestOutput || !strings.HasSuffix(out, "last line\n") {
		t.Fatalf("len=%d out=%q", len(out), out)
	}
}

func TestTAPParsesOkNotOkAndSkip(t *testing.T) {
	data, err := os.ReadFile("testdata/tap.txt")
	if err != nil {
		t.Fatal(err)
	}
	got, emit := collect()
	s := NewTAP(emit)
	feed(t, s, string(data))
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if len(*got) != 3 {
		t.Fatalf("got %d results, want 3: %+v", len(*got), *got)
	}
	r := *got
	if r[0].Name != "adds" || r[0].Outcome != api.TestPassed {
		t.Errorf("r0 = %+v", r[0])
	}
	if r[1].Name != "subtracts" || r[1].Outcome != api.TestFailed || !strings.Contains(r[1].Output, "expected 1 got 2") {
		t.Errorf("r1 = %+v", r[1])
	}
	if r[2].Name != "divides" || r[2].Outcome != api.TestSkipped {
		t.Errorf("r2 = %+v", r[2])
	}
}

func TestTAPTodoIsSkippedAndNoiseIgnored(t *testing.T) {
	got, emit := collect()
	s := NewTAP(emit)
	feed(t, s, "random noise\nok 1 - a # TODO later\nnot ok 2 - b\nmore noise\n")
	_ = s.Close()
	if len(*got) != 2 || (*got)[0].Outcome != api.TestSkipped || (*got)[1].Output != "" {
		t.Fatalf("got %+v", *got)
	}
}

func TestJUnitParsesSuitesCasesAndFailures(t *testing.T) {
	f, err := os.Open("testdata/junit.xml")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	got, emit := collect()
	if err := ParseJUnit(f, emit); err != nil {
		t.Fatal(err)
	}
	if len(*got) != 4 {
		t.Fatalf("got %d, want 4: %+v", len(*got), *got)
	}
	add := byName(*got, "test_add")
	if add.Outcome != api.TestPassed || add.Suite != "tests.test_math" || add.Duration != time.Millisecond {
		t.Errorf("add = %+v", add)
	}
	sub := byName(*got, "test_sub")
	if sub.Outcome != api.TestFailed || sub.File != "tests/test_math.py" || sub.Line != 7 || sub.Output != "assert 1 == 2\ntrace" {
		t.Errorf("sub = %+v", sub)
	}
	if div := byName(*got, "test_div"); div.Outcome != api.TestFailed {
		t.Errorf("div = %+v", div)
	}
	if sk := byName(*got, "test_skip"); sk.Outcome != api.TestSkipped || sk.Suite != "tests.test_math" {
		t.Errorf("skip = %+v", sk)
	}
}

func TestJUnitAcceptsBareTestsuiteRoot(t *testing.T) {
	got, emit := collect()
	err := ParseJUnit(strings.NewReader(`<testsuite name="s"><testcase name="a" time="x"/></testsuite>`), emit)
	if err != nil || len(*got) != 1 || (*got)[0].Duration != 0 {
		t.Fatalf("err=%v got=%+v", err, *got)
	}
}

func TestJUnitRefusesMalformedXML(t *testing.T) {
	_, emit := collect()
	if err := ParseJUnit(strings.NewReader("<testsuites><testsuite>"), emit); err == nil {
		t.Fatal("want error")
	}
}

func TestJUnitRefusesInputThatIsNotAReport(t *testing.T) {
	for _, in := range []string{"", "hello world", `{"tests":1}`, "<foo/>"} {
		got, emit := collect()
		if err := ParseJUnit(strings.NewReader(in), emit); err == nil || len(*got) != 0 {
			t.Errorf("%q: err=%v results=%d, want error and none", in, err, len(*got))
		}
	}
}

func TestTAPEmptyDescriptionFallsBackToNumber(t *testing.T) {
	got, emit := collect()
	s := NewTAP(emit)
	feed(t, s, "ok 1\nok 2\n")
	_ = s.Close()
	if len(*got) != 2 || (*got)[0].Name != "1" || (*got)[1].Name != "2" {
		t.Fatalf("got %+v", *got)
	}
}

func TestGoTestSameNameInTwoPackagesEmitsBoth(t *testing.T) {
	got, emit := collect()
	s := NewGoTest(emit)
	feed(t, s, `{"Action":"run","Package":"a","Test":"TestX"}`+"\n"+
		`{"Action":"run","Package":"b","Test":"TestX"}`+"\n"+
		`{"Action":"output","Package":"b","Test":"TestX","Output":"boom\n"}`+"\n"+
		`{"Action":"pass","Package":"a","Test":"TestX"}`+"\n"+
		`{"Action":"fail","Package":"b","Test":"TestX"}`+"\n")
	r := *got
	if len(r) != 2 || r[0].Suite != "a" || r[0].Outcome != api.TestPassed || r[1].Suite != "b" || r[1].Output != "boom\n" {
		t.Fatalf("got %+v", r)
	}
}

func TestNewStreamRejectsJUnit(t *testing.T) {
	if _, err := NewStream("junit", func(Result) {}); err == nil {
		t.Fatal("want error")
	}
	for _, f := range []string{"gotest", "tap"} {
		if _, err := NewStream(f, func(Result) {}); err != nil {
			t.Errorf("%s: %v", f, err)
		}
	}
}

func TestTailKeepsTheLastBytes(t *testing.T) {
	var b strings.Builder
	for b.Len() < 5000 {
		b.WriteString("a line of output\n")
	}
	in := b.String() + "final line\n"
	out := Tail(in)
	if len(out) > api.MaxTestOutput || !strings.HasSuffix(out, "final line\n") || !strings.HasPrefix(out, "a line") {
		t.Fatalf("len=%d prefix=%q", len(out), out[:10])
	}
	if Tail("short") != "short" {
		t.Fatal("short input changed")
	}
}
