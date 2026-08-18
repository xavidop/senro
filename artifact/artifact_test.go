package artifact_test

import (
	"testing"

	"github.com/xavidop/senro/artifact"
)

func TestSelectorsSerializeAndParseBack(t *testing.T) {
	for _, s := range []artifact.Selector{
		artifact.Glob("**/*.go"),
		artifact.File("go.sum"),
		artifact.Glob("cmd/*/main.go"),
	} {
		back, err := artifact.Parse(s.Serial())
		if err != nil {
			t.Fatalf("Parse(%q): %v", s.Serial(), err)
		}
		if back.Serial() != s.Serial() {
			t.Errorf("round trip %q gave %q", s.Serial(), back.Serial())
		}
		if back.Kind() != s.Kind() || back.Pattern() != s.Pattern() {
			t.Errorf("round trip lost fields: %s/%s vs %s/%s", back.Kind(), back.Pattern(), s.Kind(), s.Pattern())
		}
	}
}

func TestGlobAndFileAreDistinguishable(t *testing.T) {
	if artifact.Glob("go.sum").Serial() == artifact.File("go.sum").Serial() {
		t.Error("a glob and a file with the same text serialize identically, so a plan cannot tell them apart")
	}
	if artifact.Glob("x").Kind() != "glob" || artifact.File("x").Kind() != "file" {
		t.Error("Kind does not report what the constructor said")
	}
}

func TestParseRejectsWhatItCannotRepresent(t *testing.T) {
	for _, bad := range []string{"", "go.sum", "regex:.*", "glob:", "file:"} {
		if _, err := artifact.Parse(bad); err == nil {
			t.Errorf("Parse(%q) returned no error", bad)
		}
	}
}
