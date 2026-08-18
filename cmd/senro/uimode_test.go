package main

import (
	"strings"
	"testing"
)

func TestParseUIModeAcceptsTheFourValues(t *testing.T) {
	for _, s := range []string{"auto", "tui", "plain", "none"} {
		mode, err := parseUIMode(s)
		if err != nil {
			t.Errorf("parseUIMode(%q): %v", s, err)
		}
		if string(mode) != s {
			t.Errorf("parseUIMode(%q) = %q, want %q", s, mode, s)
		}
	}
}

func TestParseUIModeRejectsGarbage(t *testing.T) {
	_, err := parseUIMode("curses")
	if err == nil {
		t.Fatal("parseUIMode(\"curses\") err = nil, want a usage error")
	}
}

// TestResolveUIModeAutoPicksPlainOnANonTTY: --ui=auto must never open a
// TUI on a non-interactive stdout, which is the most common way this
// feature ships broken.
func TestResolveUIModeAutoPicksPlainOnANonTTY(t *testing.T) {
	got, err := resolveUIMode(uiAuto, false)
	if err != nil {
		t.Fatalf("resolveUIMode(auto, non-tty): %v", err)
	}
	if got != uiPlain {
		t.Errorf("resolveUIMode(auto, non-tty) = %q, want %q", got, uiPlain)
	}
}

func TestResolveUIModeAutoPicksTUIOnATTY(t *testing.T) {
	got, err := resolveUIMode(uiAuto, true)
	if err != nil {
		t.Fatalf("resolveUIMode(auto, tty): %v", err)
	}
	if got != uiTUI {
		t.Errorf("resolveUIMode(auto, tty) = %q, want %q", got, uiTUI)
	}
}

// TestResolveUIModeTUIOnNonTTYIsAnError: --ui=tui on a non-TTY must be a
// hard error, never a silent downgrade that appears to work while emitting
// garbage escape sequences.
func TestResolveUIModeTUIOnNonTTYIsAnError(t *testing.T) {
	_, err := resolveUIMode(uiTUI, false)
	if err == nil {
		t.Fatal("resolveUIMode(tui, non-tty) err = nil, want an error")
	}
	if !strings.Contains(err.Error(), "TTY") && !strings.Contains(err.Error(), "terminal") {
		t.Errorf("error %q does not explain the TTY requirement", err.Error())
	}
}

func TestResolveUIModeTUIOnATTYSucceeds(t *testing.T) {
	got, err := resolveUIMode(uiTUI, true)
	if err != nil {
		t.Fatalf("resolveUIMode(tui, tty): %v", err)
	}
	if got != uiTUI {
		t.Errorf("resolveUIMode(tui, tty) = %q, want %q", got, uiTUI)
	}
}

// TestResolveUIModePlainAndNoneAreTTYIndependent: plain and none are valid
// whether or not stdout is a terminal, no reason to refuse either.
func TestResolveUIModePlainAndNoneAreTTYIndependent(t *testing.T) {
	for _, tty := range []bool{true, false} {
		if got, err := resolveUIMode(uiPlain, tty); err != nil || got != uiPlain {
			t.Errorf("resolveUIMode(plain, tty=%v) = (%q, %v), want (plain, nil)", tty, got, err)
		}
		if got, err := resolveUIMode(uiNone, tty); err != nil || got != uiNone {
			t.Errorf("resolveUIMode(none, tty=%v) = (%q, %v), want (none, nil)", tty, got, err)
		}
	}
}

// TestResolveUIModeMutationGuard: a resolveUIMode that always returned
// uiPlain would pass a test checking only the auto/non-tty case, so this
// pins every (requested, tty) combination in one table.
func TestResolveUIModeMutationGuard(t *testing.T) {
	cases := []struct {
		requested uiMode
		tty       bool
		want      uiMode
		wantErr   bool
	}{
		{uiAuto, true, uiTUI, false},
		{uiAuto, false, uiPlain, false},
		{uiTUI, true, uiTUI, false},
		{uiTUI, false, "", true},
		{uiPlain, true, uiPlain, false},
		{uiPlain, false, uiPlain, false},
		{uiNone, true, uiNone, false},
		{uiNone, false, uiNone, false},
	}
	for _, c := range cases {
		got, err := resolveUIMode(c.requested, c.tty)
		if c.wantErr {
			if err == nil {
				t.Errorf("resolveUIMode(%q, tty=%v) err = nil, want an error", c.requested, c.tty)
			}
			continue
		}
		if err != nil {
			t.Errorf("resolveUIMode(%q, tty=%v): %v", c.requested, c.tty, err)
			continue
		}
		if got != c.want {
			t.Errorf("resolveUIMode(%q, tty=%v) = %q, want %q", c.requested, c.tty, got, c.want)
		}
	}
}
