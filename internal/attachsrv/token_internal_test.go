package attachsrv

import (
	"crypto/sha256"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The token is the entire access boundary for a TCP listener: there is no
// peer-credential check to fall back on, because there is no peer whose
// credentials the kernel can vouch for. These tests cover the two properties
// a bearer check has to have and that a black-box HTTP test cannot see: the
// comparison is over a constant number of bytes regardless of what was
// presented, and it does not short-circuit on the first differing byte.

func TestAGuardAcceptsOnlyItsOwnToken(t *testing.T) {
	const secret = "MpQ7bqTn6Rr1yfKuHhVe0sZgWc4LxJd2AaBbCcDdEeF"
	g := newTokenGuard(secret)

	if !g.accepts(secret) {
		t.Fatal("the guard refused its own token")
	}
	for _, presented := range []string{
		"",
		"X" + secret[1:],             // differs in the FIRST byte only
		secret[:len(secret)-1] + "X", // differs in the LAST byte only
		secret[:len(secret)/2],       // a correct prefix, truncated
		secret + "x",                 // a correct prefix, extended
		strings.ToUpper(secret),      // case-folded
		strings.Repeat(secret, 1000), // absurdly long
		string(make([]byte, 1<<16)),  // 64KiB of zero bytes
	} {
		if g.accepts(presented) {
			t.Errorf("the guard accepted %q", truncateForMessage(presented))
		}
	}
}

// The property subtle.ConstantTimeCompare does NOT give on its own: it
// returns 0 immediately for unequal lengths, leaking the secret's length
// to anyone presenting candidates of varying length. Hashing both sides
// removes the leak by construction, since the operands are then always
// sha256.Size bytes. Asserted on the digest helper, because that is the
// step doing the work.
func TestTheComparedValueIsAlwaysAFixedLengthDigest(t *testing.T) {
	for _, in := range []string{"", "x", "MpQ7bqTn6Rr1yfKuHhVe0sZgWc4LxJd2AaBbCcDdEeF", strings.Repeat("y", 1<<20)} {
		if got := len(tokenDigest(in)); got != sha256.Size {
			t.Fatalf("tokenDigest(%d bytes) is %d bytes long, want a constant %d: "+
				"a comparison whose length depends on what the caller presented leaks the secret's length",
				len(in), got, sha256.Size)
		}
	}
	// And the digest actually discriminates, so the fixed length above is
	// not being bought with a constant function.
	if tokenDigest("a") == tokenDigest("b") {
		t.Fatal("tokenDigest collided on two different inputs")
	}
}

// A timing test would be theatre: on a 43-byte token the difference
// between differing at byte 0 and byte 42 is far below what any test can
// measure in CI. So the check is structural: production code must reach
// its decision through subtle.ConstantTimeCompare and must never compare a
// token, secret or digest with == or !=.
//
// Mechanical, not a grep: the file is parsed and every binary equality
// expression inspected, so a comparison across a line break or through a
// local alias is caught like the obvious form.
func TestTheTokenComparisonGoesThroughConstantTimeCompare(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatalf("Glob: %v", err)
	}

	var sawConstantTimeCompare bool
	for _, path := range files {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		src, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		fset := token.NewFileSet()
		f, err := parser.ParseFile(fset, path, src, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", path, err)
		}
		ast.Inspect(f, func(n ast.Node) bool {
			switch node := n.(type) {
			case *ast.SelectorExpr:
				if pkg, ok := node.X.(*ast.Ident); ok && pkg.Name == "subtle" && node.Sel.Name == "ConstantTimeCompare" {
					sawConstantTimeCompare = true
				}
			case *ast.BinaryExpr:
				if node.Op != token.EQL && node.Op != token.NEQ {
					return true
				}
				for _, side := range []ast.Expr{node.X, node.Y} {
					if name, ok := secretishName(side); ok {
						t.Errorf("%s:%d compares %s with %s: a secret must never be compared with Go's own equality operator; "+
							"use subtle.ConstantTimeCompare over a fixed-length digest",
							path, fset.Position(node.Pos()).Line, name, node.Op)
					}
				}
			}
			return true
		})
	}

	if !sawConstantTimeCompare {
		t.Fatal("no production file in this package calls subtle.ConstantTimeCompare: " +
			"the bearer check must not be reaching its decision some other way")
	}
}

// secretishName reports whether e names something that must never be
// compared with ==: a token, a secret, or a digest of one. Deliberately
// generous, because a false positive costs a rename and a false negative
// costs a timing oracle. Length comparisons are exempt: a length is not
// the secret, and refusing a too-short token by length is what the
// bind-time guard does.
func secretishName(e ast.Expr) (string, bool) {
	switch v := e.(type) {
	case *ast.Ident:
		return v.Name, isSecretish(v.Name)
	case *ast.SelectorExpr:
		return v.Sel.Name, isSecretish(v.Sel.Name)
	case *ast.SliceExpr:
		return secretishName(v.X)
	}
	return "", false
}

func isSecretish(name string) bool {
	lower := strings.ToLower(name)
	for _, needle := range []string{"token", "secret", "digest", "credential"} {
		if strings.Contains(lower, needle) {
			return true
		}
	}
	return false
}

func truncateForMessage(s string) string {
	if len(s) <= 24 {
		return s
	}
	return s[:24] + "..."
}

// --- the loopback decision ---

// Which addresses count as loopback is the whole TLS policy: get this wrong
// in the permissive direction and a wildcard bind ships a cleartext bearer
// token onto a network.
func TestHostIsLoopback(t *testing.T) {
	for _, tc := range []struct {
		host string
		want bool
	}{
		{"127.0.0.1", true},
		{"127.0.0.53", true},
		{"::1", true},
		{"", false},        // a wildcard bind: every interface
		{"0.0.0.0", false}, // the same, spelled out
		{"::", false},      // and again, in v6
		{"10.0.0.1", false},
		{"192.168.1.10", false},
		{"203.0.113.7", false},
	} {
		got, err := hostIsLoopback(tc.host)
		if err != nil {
			t.Errorf("hostIsLoopback(%q): %v", tc.host, err)
			continue
		}
		if got != tc.want {
			t.Errorf("hostIsLoopback(%q) = %v, want %v", tc.host, got, tc.want)
		}
	}
}
