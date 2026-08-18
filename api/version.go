package api

import "fmt"

// VersionMinor is the current envelope's minor revision: it increases for an
// additive, backward-compatible change within Version's major number (a new
// optional field, a new event Type), never for anything a client must
// understand to keep working. See Version and CheckVersion.
//
// 1: trace context. Event.TraceID's first emit site, plus optional span
// payload fields (span_id, parent_span_id, linked_span_ids, trace_flags,
// tracestate), all ignorable by a v1.0 client.
//
// 2: binary.staged's first emit site and BinaryStagedBody, when a func step
// became able to run somewhere other than the coordinator.
const VersionMinor = 2

// VersionMismatchError reports that a client's and an engine's major
// envelope versions differ and cannot interoperate; see CheckVersion. It is
// a struct, not a formatted string, so a caller can recover the four version
// numbers programmatically rather than having to parse them back out of the
// error's own prose.
type VersionMismatchError struct {
	ClientMajor, ClientMinor int
	ServerMajor, ServerMinor int
}

// Error names which side is behind: "upgrade your CLI" and "upgrade the
// engine" are actionable in ways "version mismatch" alone is not.
func (e *VersionMismatchError) Error() string {
	if e.ClientMajor < e.ServerMajor {
		return fmt.Sprintf(
			"api: engine speaks protocol v%d.%d, this client speaks v%d.%d: upgrade your CLI",
			e.ServerMajor, e.ServerMinor, e.ClientMajor, e.ClientMinor)
	}
	return fmt.Sprintf(
		"api: this client speaks protocol v%d.%d, the engine speaks v%d.%d: upgrade the engine (senro run a newer build)",
		e.ClientMajor, e.ClientMinor, e.ServerMajor, e.ServerMinor)
}

// CheckVersion implements the negotiation rule documented on Version: a
// client and engine must agree on the major version; a minor mismatch warns
// rather than failing.
//
//   - Equal major and minor: both return values are zero; silent by design.
//   - Equal major, different minor: err is nil (explicitly NOT a failure)
//     and warn names the mismatch, for a caller to show once, not act on.
//   - Different major: err is a *VersionMismatchError naming both sides,
//     replacing the JSON decode error a stale CLI would otherwise hit.
//
// A pure function of the four numbers, so both mismatch directions are
// exercised in this package's tests without two different builds of senro.
func CheckVersion(clientMajor, clientMinor, serverMajor, serverMinor int) (warn string, err error) {
	if clientMajor != serverMajor {
		return "", &VersionMismatchError{
			ClientMajor: clientMajor, ClientMinor: clientMinor,
			ServerMajor: serverMajor, ServerMinor: serverMinor,
		}
	}
	if clientMinor != serverMinor {
		return fmt.Sprintf(
			"api: protocol minor version differs (client v%d.%d, engine v%d.%d), so some features may be unavailable",
			clientMajor, clientMinor, serverMajor, serverMinor), nil
	}
	return "", nil
}
