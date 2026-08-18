package sshexec

import (
	"strings"
	"testing"
)

// The control socket has to fit a unix socket address, which is 104 bytes on
// darwin: shorter than a lot of ordinary directories, and past it OpenSSH
// disables multiplexing per invocation while still exiting 0. There is no way
// to make a long $HOME short from a test, so the budget is checked here on
// values a machine cannot be talked into producing on demand.
func TestAControlPathThatCouldNotWorkIsRefusedBeforeItIsUsed(t *testing.T) {
	const nonce = "0123456789abcdef"
	tests := []struct {
		name string
		dir  string
		want string
	}{
		{
			name: "a runtime directory of ordinary length",
			dir:  "/run/user/1000/senro",
		},
		{
			name: "a darwin cache directory",
			dir:  "/Users/somebody-with-a-long-name/Library/Caches/senro",
		},
		{
			name: "past the socket address limit",
			dir:  "/tmp/" + strings.Repeat("d", 90),
			want: "past the",
		},
		{
			name: "a directory ssh would read a token out of",
			dir:  "/tmp/senro-100%-full",
			want: "reads as a token",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			path, err := controlPath(tc.dir, nonce)
			switch {
			case tc.want == "" && err != nil:
				t.Fatalf("controlPath(%q) = %v, want a usable path", tc.dir, err)
			case tc.want == "":
				if !strings.HasPrefix(path, tc.dir+"/") {
					t.Errorf("controlPath(%q) = %q, which is not under the directory it was given",
						tc.dir, path)
				}
				if len(path) > controlPathBudget {
					t.Errorf("controlPath returned %d bytes, over its own %d budget",
						len(path), controlPathBudget)
				}
			case err == nil:
				t.Fatalf("controlPath(%q) = %q, want a refusal naming %q", tc.dir, path, tc.want)
			case !strings.Contains(err.Error(), tc.want):
				t.Errorf("controlPath(%q) said %q, which does not say %q", tc.dir, err, tc.want)
			}
		})
	}
}

// The refusal above is not a run-ending one: it turns multiplexing off and
// says so, because a run that opens a connection per command is the run senro
// had before this file existed. The message has to name the way out.
func TestATooLongControlPathNamesTheWayOut(t *testing.T) {
	_, err := controlPath("/tmp/"+strings.Repeat("d", 90), "0123456789abcdef")
	if err == nil {
		t.Fatal("controlPath accepted a path no unix socket address holds")
	}
	if !strings.Contains(err.Error(), "ssh.NoMultiplexing()") {
		t.Errorf("the refusal does not name what to do instead: %v", err)
	}
}
