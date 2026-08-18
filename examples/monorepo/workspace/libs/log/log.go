// Package log is the bottom of the chain: config imports it, api imports
// config, so a change here reaches all three.
package log

func Line(s string) string { return "[mono] " + s }
