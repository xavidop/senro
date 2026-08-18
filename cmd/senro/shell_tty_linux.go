//go:build linux

package main

import "golang.org/x/sys/unix"

// See the darwin file: the same two operations under different names.
const (
	ioctlReadTermios  = unix.TCGETS
	ioctlWriteTermios = unix.TCSETS
)
