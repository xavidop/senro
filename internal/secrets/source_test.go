package secrets

import "testing"

// TestSourceIdentity is a whitebox test because sourceIdentity is the exact
// function whose output reaches events.jsonl and a cache key, and its safety
// argument is about bytes rather than about the Set's behaviour.
func TestSourceIdentity(t *testing.T) {
	cases := []struct{ in, want string }{
		{"", ""},
		{"aws-sm://ci/npm#token", "aws-sm://ci/npm#token"},
		{"env:DEPLOY_ENV", "env:DEPLOY_ENV"},
		{"file:///etc/senro/token", "file:///etc/senro/token"},
		{"aws-sm://ci/npm#token?decode=base64", "aws-sm://ci/npm#token"},
		{"vault://user:hunter2@vault.internal/kv/ci#raw", "vault://vault.internal/kv/ci#raw"},
		{"vault://token@vault.internal/kv/ci", "vault://vault.internal/kv/ci"},
		{"vault://user:pw@host", "vault://host"},
	}
	for _, tc := range cases {
		if got := sourceIdentity(tc.in); got != tc.want {
			t.Errorf("sourceIdentity(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

// TestSourceIdentityNeverKeepsAUserinfoSecret is the negative case stated as
// the property rather than as a table row, so a future grammar change that
// broke the cut is caught by intent rather than by one example.
func TestSourceIdentityNeverKeepsAUserinfoSecret(t *testing.T) {
	for _, in := range []string{
		"vault://u:hunter2@h/p",
		"vault://u:hunter2@h/p#k",
		"vault://u:hunter2@h/p#k?decode=hex",
		"postgres://u:hunter2@h:5432/db#password",
	} {
		got := sourceIdentity(in)
		if got == "" {
			t.Fatalf("sourceIdentity(%q) returned empty; the assertion below proves nothing", in)
		}
		if contains(got, "hunter2") {
			t.Errorf("sourceIdentity(%q) = %q kept the password", in, got)
		}
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
