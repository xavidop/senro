package webui

import (
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"embed"
	"encoding/base64"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"strconv"
	"strings"
)

// assetPrefix is where everything but the page itself is served from.
const assetPrefix = "/_ui/"

// clientAsset is the compiled WebAssembly client, stored gzipped IN THE
// BINARY, not merely on the wire: embedding the compressed form (~1.0MB
// against 3.6MB) keeps the CLI from growing by the larger figure for a
// feature most invocations never use. A client that does not accept gzip
// is served decompressed bytes, produced on the fly.
const clientAsset = "senro-ui.wasm.gz"

// execAsset is the Go toolchain's own WebAssembly bootstrap
// (GOROOT/lib/wasm/wasm_exec.js), copied in by the build rather than
// committed: a wasm_exec.js from one Go version and a client compiled at
// another fails unintelligibly in a browser console, and building both
// from the same GOROOT makes that unrepresentable.
const execAsset = "wasm_exec.js"

//go:embed assets
var assetsFS embed.FS

// bundle is the served asset set, with each file's content type and
// validator computed once at startup rather than per request.
type bundle struct {
	files map[string]*asset
}

type asset struct {
	body        []byte
	contentType string
	// gzipped reports whether body is gzip-compressed and should be served
	// with Content-Encoding: gzip to a client that accepts it.
	gzipped bool
	// etag is a strong validator over the served bytes: what makes a
	// reload cost a 304 rather than another megabyte of client binary.
	etag string
}

// loadBundle reads the embedded assets, or reports ErrBundleMissing when
// this tree has never run `make wasm`: a fresh checkout compiles and every
// other command works; only this one is unavailable, and it says so
// precisely rather than serving a page that fails in a browser console.
func loadBundle() (*bundle, error) {
	entries, err := fs.ReadDir(assetsFS, "assets")
	if err != nil {
		return nil, fmt.Errorf("webui: reading embedded assets: %w", err)
	}

	b := &bundle{files: make(map[string]*asset, len(entries))}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		body, err := assetsFS.ReadFile("assets/" + e.Name())
		if err != nil {
			return nil, fmt.Errorf("webui: reading embedded asset %s: %w", e.Name(), err)
		}
		sum := sha256.Sum256(body)
		b.files[e.Name()] = &asset{
			body:        body,
			contentType: contentTypeFor(e.Name()),
			gzipped:     strings.HasSuffix(e.Name(), ".gz"),
			etag:        `"` + base64.RawURLEncoding.EncodeToString(sum[:16]) + `"`,
		}
	}

	for _, required := range []string{clientAsset, execAsset} {
		if _, ok := b.files[required]; !ok {
			return nil, fmt.Errorf("%w: %s is not embedded. Build it with `make wasm`, "+
				"which compiles the client for GOOS=js GOARCH=wasm and copies the toolchain's "+
				"own bootstrap next to it", ErrBundleMissing, required)
		}
	}
	return b, nil
}

// contentTypeFor names an asset's type explicitly rather than sniffing it.
// application/wasm is load-bearing: WebAssembly.instantiateStreaming
// refuses any other type outright.
func contentTypeFor(name string) string {
	name = strings.TrimSuffix(name, ".gz")
	switch {
	case strings.HasSuffix(name, ".html"):
		return "text/html; charset=utf-8"
	case strings.HasSuffix(name, ".css"):
		return "text/css; charset=utf-8"
	case strings.HasSuffix(name, ".js"):
		return "text/javascript; charset=utf-8"
	case strings.HasSuffix(name, ".wasm"):
		return "application/wasm"
	case strings.HasSuffix(name, ".svg"):
		return "image/svg+xml"
	default:
		return "application/octet-stream"
	}
}

// serve writes one asset.
//
// name is matched exactly against the embedded set, never joined onto a
// path: there is no traversal to defend against because there is no
// filesystem lookup at all, only a map of names this binary was built with.
func (b *bundle) serve(w http.ResponseWriter, r *http.Request, name string) {
	a, ok := b.files[name]
	if !ok {
		// The client is stored gzipped but requested by its real name, so
		// the page's own URLs say what they mean ("senro-ui.wasm").
		a, ok = b.files[name+".gz"]
	}
	if !ok {
		http.Error(w, refusalBody, http.StatusNotFound)
		return
	}

	h := w.Header()
	h.Set("Content-Type", a.contentType)
	h.Set("ETag", a.etag)
	// no-cache, not no-store: the browser may keep the bytes but must
	// revalidate, so a reload is a 304 with no body; storing without
	// revalidating would pin a stale client across a senro upgrade.
	h.Set("Cache-Control", "no-cache")

	if match := r.Header.Get("If-None-Match"); match != "" && etagMatches(match, a.etag) {
		w.WriteHeader(http.StatusNotModified)
		return
	}

	if !a.gzipped {
		h.Set("Content-Length", strconv.Itoa(len(a.body)))
		_, _ = w.Write(a.body)
		return
	}
	if acceptsGzip(r) {
		h.Set("Content-Encoding", "gzip")
		h.Set("Vary", "Accept-Encoding")
		h.Set("Content-Length", strconv.Itoa(len(a.body)))
		_, _ = w.Write(a.body)
		return
	}
	// No Content-Length: the decompressed size is not known without
	// decompressing, and this path exists for correctness rather than for
	// any real browser.
	h.Set("Vary", "Accept-Encoding")
	zr, err := gzip.NewReader(bytes.NewReader(a.body))
	if err != nil {
		http.Error(w, "senro ui: the embedded client is corrupt", http.StatusInternalServerError)
		return
	}
	defer func() { _ = zr.Close() }()
	_, _ = io.Copy(w, zr)
}

// etagMatches reports whether an If-None-Match header lists this tag. "*"
// matches anything, per the HTTP specification.
func etagMatches(header, etag string) bool {
	for _, candidate := range strings.Split(header, ",") {
		candidate = strings.TrimSpace(candidate)
		if candidate == "*" || candidate == etag {
			return true
		}
	}
	return false
}

// acceptsGzip reports whether the client said it accepts gzip. Deliberately
// a plain containment check rather than a full Accept-Encoding parse: the
// only thing riding on it is whether this server decompresses on the way
// out, and the failure mode of guessing wrong in either direction is a
// larger response, not a broken one.
func acceptsGzip(r *http.Request) bool {
	return strings.Contains(r.Header.Get("Accept-Encoding"), "gzip")
}
