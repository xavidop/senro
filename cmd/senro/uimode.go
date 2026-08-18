package main

import (
	"fmt"
	"os"

	"github.com/mattn/go-isatty"
)

// uiMode is the renderer a command selects: --ui=auto|tui|plain|none,
// defaulting to auto.
type uiMode string

const (
	uiAuto  uiMode = "auto"
	uiTUI   uiMode = "tui"
	uiPlain uiMode = "plain"
	uiNone  uiMode = "none"
)

// parseUIMode validates a --ui flag value. It does not consult a TTY, so a
// bad value is reported as a usage error independent of where stdout
// points; resolveUIMode makes the TTY decision.
func parseUIMode(s string) (uiMode, error) {
	switch uiMode(s) {
	case uiAuto, uiTUI, uiPlain, uiNone:
		return uiMode(s), nil
	default:
		return "", fmt.Errorf("senro: invalid --ui value %q (want auto, tui, plain, or none)", s)
	}
}

// resolveUIMode turns a requested mode and whether stdout is a terminal
// into the mode a command must use: auto is tui on a TTY and plain
// otherwise; tui on a non-TTY is an error rather than a silent downgrade,
// which would fill a CI log with escape sequences that nobody notices until
// much later; plain and none are honoured as given.
func resolveUIMode(requested uiMode, stdoutIsTTY bool) (uiMode, error) {
	switch requested {
	case uiAuto:
		if stdoutIsTTY {
			return uiTUI, nil
		}
		return uiPlain, nil
	case uiTUI:
		if !stdoutIsTTY {
			return "", fmt.Errorf(
				"senro: --ui=tui requires a terminal, but stdout is not a TTY. " +
					"use --ui=plain, --ui=auto, or run this in an interactive terminal")
		}
		return uiTUI, nil
	case uiPlain, uiNone:
		return requested, nil
	default:
		// parseUIMode is the only constructor for a valid uiMode, so
		// reaching here means a caller built one some other way.
		return "", fmt.Errorf("senro: invalid --ui value %q", requested)
	}
}

// stdoutIsTTY reports whether os.Stdout is a real terminal: the one place
// this package touches a file descriptor for the check, so every function
// above stays a pure function of a plain bool.
func stdoutIsTTY() bool {
	return isatty.IsTerminal(os.Stdout.Fd()) || isatty.IsCygwinTerminal(os.Stdout.Fd())
}
