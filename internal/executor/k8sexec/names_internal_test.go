package k8sexec

import (
	"strings"
	"testing"
)

// A Kubernetes object name is a DNS-1123 label: lowercase alphanumerics and
// "-", alphanumeric at both ends, at most 63 bytes. A senro step id obeys
// none of that, and the apiserver's refusal for one that does not
// ("metadata.name: Invalid value") names neither the step nor the pipeline
// call it came from. So the names are built here and these are the rules.

func validDNS1123(s string) bool {
	if s == "" || len(s) > 63 {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		ok := (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '-'
		if !ok {
			return false
		}
	}
	return isAlnum(s[0]) && isAlnum(s[len(s)-1])
}

func isAlnum(c byte) bool {
	return (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9')
}

func TestAPodNameIsValidForEveryStepIdSenroCanProduce(t *testing.T) {
	ids := []string{
		"build",
		// The shape (*WorkflowBuilder).Expand produces, which is the one that
		// breaks a naive name: brackets, an equals sign and two slashes.
		"verify/lint[unit=apps/web]",
		"deploy/apply",
		"UPPERCASE",
		"---leading-and-trailing---",
		"...",
		strings.Repeat("a-very-long-step-identifier/", 20),
		"", // a plan Validate would refuse, but the name must still be legal
	}
	for _, id := range ids {
		got := podName("run-01JABCDEF", id, 1)
		if !validDNS1123(got) {
			t.Errorf("podName(%q) = %q, which the apiserver would refuse", id, got)
		}
		if !validDNS1123(secretName(got)) {
			t.Errorf("secretName for %q = %q, which the apiserver would refuse", id, secretName(got))
		}
	}
}

// TestPodNamesAreUniquePerAttemptAndPerStep. Two attempts of one step, and
// two children of one expansion, must not collide: the second create would
// answer 409 and the run would fail for a reason that has nothing to do with
// the pipeline.
func TestPodNamesAreUniquePerAttemptAndPerStep(t *testing.T) {
	seen := map[string]string{}
	add := func(label, name string) {
		t.Helper()
		if prev, dup := seen[name]; dup {
			t.Errorf("%s and %s both produce pod name %q", prev, label, name)
		}
		seen[name] = label
	}
	add("attempt 1", podName("r", "build", 1))
	add("attempt 2", podName("r", "build", 2))
	add("another run", podName("r2", "build", 1))
	// Two children of one expansion, whose ids differ only past the point a
	// truncated prefix would keep.
	long := strings.Repeat("verify/lint/", 8)
	add("child a", podName("r", long+"[unit=apps/web]", 1))
	add("child b", podName("r", long+"[unit=apps/admin]", 1))
}

// TestALabelValueIsValid: a label value is a narrower character class than a
// name and a step id fails it too, which is why the unsanitized id goes in an
// annotation instead.
func TestALabelValueIsValid(t *testing.T) {
	for _, in := range []string{"verify/lint[unit=apps/web]", "ok.value_1", strings.Repeat("x", 200)} {
		got := labelValue(in)
		if len(got) > 63 {
			t.Errorf("labelValue(%q) is %d bytes, and the limit is 63", in, len(got))
		}
		if got == "" {
			continue
		}
		if !isAlnum(got[0]) || !isAlnum(got[len(got)-1]) {
			t.Errorf("labelValue(%q) = %q, which does not start and end alphanumeric", in, got)
		}
		for i := 0; i < len(got); i++ {
			c := got[i]
			ok := isAlnum(c) || (c >= 'A' && c <= 'Z') || c == '-' || c == '_' || c == '.'
			if !ok {
				t.Errorf("labelValue(%q) = %q, which contains %q", in, got, string(c))
				break
			}
		}
	}
}
