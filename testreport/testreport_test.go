package testreport_test

import (
	"reflect"
	"testing"

	"github.com/xavidop/senro/testreport"
)

func TestGoTestJSONSelectsWithRun(t *testing.T) {
	got := testreport.GoTestJSON()
	want := testreport.Spec{
		Format: testreport.FormatGoTest,
		Select: []string{"-run", "^({tests})$"},
		Join:   "|",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("GoTestJSON() = %+v, want %+v", got, want)
	}
}

func TestSelectingReplacesAndJoinedByReplaces(t *testing.T) {
	base := testreport.GoTestJSON()
	got := base.Selecting("-k", "{tests}").JoinedBy(" or ")
	if !reflect.DeepEqual(got.Select, []string{"-k", "{tests}"}) || got.Join != " or " {
		t.Fatalf("got Select %v Join %q", got.Select, got.Join)
	}
	if base.Select[0] != "-run" || base.Join != "|" {
		t.Fatalf("the receiver was modified: %+v", base)
	}
	if got.Format != testreport.FormatGoTest {
		t.Fatalf("Format lost: %q", got.Format)
	}
}

func TestJUnitXMLCarriesItsPath(t *testing.T) {
	got := testreport.JUnitXML("out/junit.xml")
	if got.Format != testreport.FormatJUnit || got.Path != "out/junit.xml" {
		t.Fatalf("JUnitXML() = %+v", got)
	}
	if tap := testreport.TAP(); tap.Format != testreport.FormatTAP {
		t.Fatalf("TAP() = %+v", tap)
	}
}
