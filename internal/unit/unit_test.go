package unit_test

import (
	"testing"

	"github.com/xavidop/senro/internal/unit"
)

func TestUnitBaseAndSources(t *testing.T) {
	u := unit.Unit{ID: "apps/web", Name: "apps/web", Dir: "apps/web"}
	if u.Base() != "web" {
		t.Errorf("Base = %q", u.Base())
	}
	got := u.Sources()
	if len(got) != 1 || got[0].Serial() != "glob:apps/web/**" {
		t.Errorf("Sources = %v", got)
	}
	root := unit.Unit{ID: ".", Dir: "."}
	if s := root.Sources(); len(s) != 1 || s[0].Serial() != "glob:**" {
		t.Errorf("root Sources = %v", s)
	}
}

// TestUnitSourcesUsesTheDirNotTheID is the mutation check on Sources: ID
// and Dir are equal in every other test here, so a unit whose fields differ
// catches a Sources that read u.ID by mistake.
func TestUnitSourcesUsesTheDirNotTheID(t *testing.T) {
	u := unit.Unit{ID: "module/foo", Name: "foo", Dir: "apps/foo"}
	got := u.Sources()
	if len(got) != 1 || got[0].Serial() != "glob:apps/foo/**" {
		t.Errorf("Sources = %v, want a glob built from Dir, not ID", got)
	}
}
