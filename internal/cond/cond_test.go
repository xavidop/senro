package cond_test

import (
	"strings"
	"testing"

	"github.com/xavidop/senro/internal/cond"
)

func TestEveryConstructorRoundTripsThroughItsSerial(t *testing.T) {
	for _, c := range []cond.Condition{
		cond.Branch("main"),
		cond.ParamIs("mode", "affected"),
		cond.EnvIs("DEPLOY_ENV", "prod"),
	} {
		back, err := cond.Parse(c.Serial())
		if err != nil {
			t.Fatalf("Parse(%q): %v", c.Serial(), err)
		}
		if back.Serial() != c.Serial() {
			t.Errorf("round trip changed %q into %q", c.Serial(), back.Serial())
		}
	}
}

func TestParseRefusesAnUnknownCondition(t *testing.T) {
	if _, err := cond.Parse("phase-of-the-moon:waxing"); err == nil {
		t.Fatal("Parse accepted an unknown condition kind")
	}
	if _, err := cond.Parse(""); err == nil {
		t.Fatal("Parse accepted an empty condition")
	}
}

func TestEvalReadsParamsAndTheEnvironment(t *testing.T) {
	sc := cond.Scope{
		Params: map[string]string{"branch": "main", "mode": "affected"},
		Env:    func(k string) string { return map[string]string{"DEPLOY_ENV": "prod"}[k] },
	}
	for _, tc := range []struct {
		c    cond.Condition
		want bool
	}{
		{cond.Branch("main"), true},
		{cond.Branch("release"), false},
		{cond.ParamIs("mode", "affected"), true},
		{cond.ParamIs("mode", "all"), false},
		{cond.ParamIs("absent", ""), true},
		{cond.EnvIs("DEPLOY_ENV", "prod"), true},
		{cond.EnvIs("DEPLOY_ENV", "staging"), false},
	} {
		if got := tc.c.Eval(sc); got != tc.want {
			t.Errorf("%s = %v, want %v", tc.c.Serial(), got, tc.want)
		}
	}
}

func TestEvalAllIsAnAndAndNamesTheFirstFalseOne(t *testing.T) {
	sc := cond.Scope{Params: map[string]string{"branch": "pr-12"}}
	run, because, err := cond.EvalAll([]string{"branch:main", "param:mode=all"}, sc)
	if err != nil {
		t.Fatalf("EvalAll: %v", err)
	}
	if run {
		t.Fatal("EvalAll ran a step whose first condition is false")
	}
	if !strings.Contains(because, "branch:main") {
		t.Errorf("because = %q, want the failing condition named", because)
	}
}

// TestTheReasonNeverCarriesAResolvedValue is the leak this design avoids by
// construction. A param's VALUE could be anything a caller passed, including
// a credential, and the reason string reaches the event log. It names the
// CONDITION, which the pipeline author wrote, and never what the param
// resolved to.
func TestTheReasonNeverCarriesAResolvedValue(t *testing.T) {
	sc := cond.Scope{Params: map[string]string{"branch": "sensitive-value-here"}}
	_, because, err := cond.EvalAll([]string{"branch:main"}, sc)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(because, "sensitive-value-here") {
		t.Fatalf("the reason repeats the resolved value: %q", because)
	}
}
