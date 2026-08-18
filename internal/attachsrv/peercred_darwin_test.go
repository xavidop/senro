//go:build darwin

package attachsrv

import (
	"testing"
	"unsafe"
)

// These pin the two checks validateXucred makes without needing to coax a
// genuine short or malformed write out of the kernel. See validateXucred's
// own doc for why that split exists. Each test below is written to fail if
// its corresponding check in peerUID's own code is ever dropped or
// weakened: without them, a short write would silently leave cred at its
// zero value and report uid 0.
func TestValidateXucredAcceptsAFullCorrectWrite(t *testing.T) {
	cred := xucred{Version: xucredVersion, Uid: 501}
	if err := validateXucred(cred, uint32(unsafe.Sizeof(cred))); err != nil {
		t.Errorf("validateXucred(full, correct) = %v, want nil", err)
	}
}

func TestValidateXucredRejectsAShortWrite(t *testing.T) {
	// The exact shape a partial/short getsockopt write leaves behind:
	// cred sitting at its zero value (Uid 0) because the kernel never
	// actually wrote into it, and vallen reporting fewer bytes than a
	// full xucred.
	var cred xucred // zero-valued: Version 0, Uid 0, indistinguishable
	// from a genuine root peer if this check did not exist.
	err := validateXucred(cred, uint32(unsafe.Sizeof(cred))-1)
	if err == nil {
		t.Fatal("validateXucred with a short vallen = nil, want an error — a partial write must not be trusted")
	}
}

func TestValidateXucredRejectsALongWrite(t *testing.T) {
	cred := xucred{Version: xucredVersion, Uid: 501}
	err := validateXucred(cred, uint32(unsafe.Sizeof(cred))+1)
	if err == nil {
		t.Fatal("validateXucred with an oversized vallen = nil, want an error — an unexpected size means an unexpected struct shape")
	}
}

func TestValidateXucredRejectsAnUnexpectedVersion(t *testing.T) {
	cred := xucred{Version: xucredVersion + 1, Uid: 501}
	err := validateXucred(cred, uint32(unsafe.Sizeof(cred)))
	if err == nil {
		t.Fatal("validateXucred with a non-zero xucred version = nil, want an error")
	}
}

// The failure mode the length check closes: a zero-valued cred (what a
// short write leaves) with a vallen claiming the write was complete. With
// only the version check, this would pass, and checkUID would compare a
// fabricated uid 0 against os.Getuid(), accepting an unverified peer on
// any engine running as root.
func TestValidateXucredRejectsAZeroCredWithATruncatedVallen(t *testing.T) {
	var cred xucred
	err := validateXucred(cred, 0)
	if err == nil {
		t.Fatal("validateXucred(zero cred, vallen=0) = nil, want an error")
	}
}
