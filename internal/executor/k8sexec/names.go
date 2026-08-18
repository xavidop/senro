package k8sexec

import (
	"crypto/sha256"
	"encoding/hex"
	"strconv"
	"strings"
)

// Kubernetes object names are DNS-1123 labels: lowercase alphanumerics and
// "-", starting and ending with an alphanumeric, at most 63 bytes for the
// names this executor generates. A senro step id obeys none of that:
// "verify/lint[unit=apps/web]" is an ordinary one.
//
// So a name is a readable PREFIX plus a hash of the full identity. The prefix
// is for the human reading `kubectl get pods` and carries no meaning the code
// depends on; the hash is what makes the name unique, and it is taken over
// the run id, the step id and the attempt together, so two attempts of one
// step and two steps of one expansion never collide.

const (
	// namePrefix marks every object this executor creates, so a leak left by
	// a killed coordinator is greppable.
	namePrefix = "senro-"
	// maxName is the DNS-1123 label limit.
	maxName = 63
	// hashLen is how much of the identity digest goes in the name. 10 hex
	// characters is 40 bits: for the number of pods one run creates, a
	// collision is not a thing that happens, and a collision would be a
	// create that fails with 409 rather than two steps sharing a pod.
	hashLen = 10
)

// podName is the object name for one attempt of one step.
func podName(runID, stepID string, attempt int) string {
	id := runID + "\x00" + stepID + "\x00" + strconv.Itoa(attempt)
	sum := sha256.Sum256([]byte(id))
	suffix := "-" + hex.EncodeToString(sum[:])[:hashLen]

	prefix := namePrefix + dns1123(stepID)
	if room := maxName - len(suffix); len(prefix) > room {
		prefix = prefix[:room]
	}
	return strings.TrimRight(prefix, "-") + suffix
}

// secretName is the object name for one attempt's projected credentials. It
// is derived from the pod's name so the two are obviously a pair in a
// `kubectl get` listing, and so a leaked secret names the pod that owned it.
func secretName(pod string) string {
	// The pod name is already within the limit and already valid, and the
	// suffix keeps it there: podName caps at 63 including its own hash, so
	// trimming the same amount off the front makes room.
	const suffix = "-secrets"
	if len(pod)+len(suffix) <= maxName {
		return pod + suffix
	}
	return strings.TrimRight(pod[:maxName-len(suffix)], "-") + suffix
}

// dns1123 reduces s to lowercase alphanumerics and "-", collapsing runs of
// anything else into a single "-", and guarantees a leading alphanumeric by
// virtue of always being used after a constant prefix.
func dns1123(s string) string {
	b := make([]byte, 0, len(s))
	dash := false
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'A' && c <= 'Z':
			b = append(b, c+('a'-'A'))
			dash = false
		case (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9'):
			b = append(b, c)
			dash = false
		case dash || len(b) == 0:
			// Never start with a separator and never emit two in a row.
		default:
			b = append(b, '-')
			dash = true
		}
	}
	return strings.TrimRight(string(b), "-")
}

// labelValue reduces s to something a Kubernetes label value accepts:
// alphanumerics, "-", "_" and ".", at most 63 bytes, alphanumeric at both
// ends. An empty result is returned as "" and the caller omits the label
// rather than setting it to something invented.
func labelValue(s string) string {
	b := make([]byte, 0, len(s))
	for i := 0; i < len(s) && len(b) < maxName; i++ {
		c := s[i]
		switch {
		case (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9'):
			b = append(b, c)
		case c == '-' || c == '_' || c == '.':
			b = append(b, c)
		default:
			b = append(b, '-')
		}
	}
	return strings.Trim(strings.TrimRight(string(b), "-_."), "-_.")
}
