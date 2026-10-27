package plan_test

import (
	"testing"

	"github.com/xavidop/senro/internal/plan"
	"github.com/xavidop/senro/testreport"
)

// TestTestsConstantsMatchTestreport pins plan's copy of the format names and
// the placeholder to testreport's. plan stays stdlib-only, so it cannot take
// them from testreport; a pipeline built with testreport.TAP() whose "tap"
// plan.Validate spelled differently would be refused as an unknown format.
// The import here is test-only and leaves both packages' graphs as they are.
func TestTestsConstantsMatchTestreport(t *testing.T) {
	for _, c := range []struct{ name, plan, testreport string }{
		{"gotest", plan.TestFormatGoTest, testreport.FormatGoTest},
		{"tap", plan.TestFormatTAP, testreport.FormatTAP},
		{"junit", plan.TestFormatJUnit, testreport.FormatJUnit},
		{"placeholder", plan.TestsPlaceholder, testreport.Placeholder},
	} {
		if c.plan != c.testreport {
			t.Errorf("%s: plan says %q, testreport says %q", c.name, c.plan, c.testreport)
		}
	}
}
