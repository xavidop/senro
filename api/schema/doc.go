// Package schema embeds senro's published JSON Schema documents.
//
// These describe the wire format for third-party clients that do not use the
// Go types. They are part of the public API and evolve additively.
package schema

import "embed"

//go:embed *.json
var Files embed.FS
