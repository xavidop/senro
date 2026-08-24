package s3

import (
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Object is one entry of a listing: what a scratch cache's prefix fallback
// needs and nothing more.
//
// LastModified is the STORE's clock, which is the whole reason listing is
// worth its permission. The local scratch directory picks the newest match
// under a prefix by the entry file's own mtime, an ordering two machines
// have no way to agree on; every client of one bucket reads the same
// LastModified, so a fleet agrees on which entry is newest.
type Object struct {
	Key          string    `xml:"Key"`
	Size         int64     `xml:"Size"`
	LastModified time.Time `xml:"LastModified"`
}

// Page is one response worth of a listing. Token is empty on the last page.
type Page struct {
	Objects []Object
	Token   string
}

// maxListBody bounds one listing response. A full page of a thousand keys is
// well under this; a store answering with something enormous is a store this
// package will not try to hold in memory.
const maxListBody = 8 << 20

// listPageSize is what a single request asks for. The protocol's own maximum,
// so a prefix walk costs as few round trips as the store allows.
const listPageSize = 1000

// List returns one page of the objects whose key starts with prefix.
//
// Pass an empty token for the first page, then Page.Token for each page after
// it, until the returned token is empty. Callers that need every match walk
// the pages themselves, so the bound on how much of a huge prefix is worth
// reading belongs to the caller rather than to this package.
//
// This is the one call in this package that addresses the BUCKET rather than
// a key, and the one that needs a permission beyond reading and writing
// objects: s3:ListBucket. Nothing else senro does requires it, which is why
// the shared scratch cache it exists for is opt-in (see internal/scratch).
func (c *Client) List(ctx context.Context, prefix, token string) (Page, error) {
	q := [][2]string{
		{"list-type", "2"},
		{"max-keys", strconv.Itoa(listPageSize)},
	}
	if prefix != "" {
		q = append(q, [2]string{"prefix", prefix})
	}
	if token != "" {
		q = append(q, [2]string{"continuation-token", token})
	}

	label := "list " + prefix
	u := c.bucketURL(rawQuery(q))
	resp, err := c.send(ctx, http.MethodGet, label, u, nil, 0, emptyPayloadHash)
	if err != nil {
		return Page{}, err
	}
	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(io.LimitReader(resp.Body, maxListBody))
	if err != nil {
		return Page{}, fmt.Errorf("s3: %s: reading the listing: %w", label, c.clean(err))
	}

	var doc struct {
		IsTruncated bool     `xml:"IsTruncated"`
		Contents    []Object `xml:"Contents"`
		NextToken   string   `xml:"NextContinuationToken"`
	}
	if err := xml.Unmarshal(body, &doc); err != nil {
		return Page{}, fmt.Errorf("s3: %s: the store's listing is not XML this package understands: %w",
			label, err)
	}

	p := Page{Objects: doc.Contents}
	// Both conditions on purpose: the protocol says a truncated listing
	// carries a token, and a store that truncates without one would otherwise
	// send this into a loop re-reading page one forever.
	if doc.IsTruncated && doc.NextToken != "" {
		p.Token = doc.NextToken
	}
	return p, nil
}

// bucketURL addresses the bucket itself, for the one operation that is not
// about a single key. keyURL cannot serve: with an empty key it leaves a
// trailing slash in path style, and a path signed as "/bucket/" is not the
// "/bucket" the store canonicalizes.
func (c *Client) bucketURL(query string) *url.URL {
	u := *c.base
	if c.pathStyle {
		u.Path = c.base.Path + "/" + c.bucket
	} else {
		u.Host = c.bucket + "." + c.base.Host
		u.Path = c.base.Path
		if u.Path == "" {
			u.Path = "/"
		}
	}
	u.RawPath = escapePath(u.Path)
	u.RawQuery = query
	return &u
}

// rawQuery renders the query exactly as the signature is taken over it: this
// package's own escaping, sorted by name, so what is signed and what goes on
// the wire are one string. net/url's Encode escapes a different set and would
// leave a signature the store recomputes differently.
func rawQuery(pairs [][2]string) string {
	sorted := append([][2]string(nil), pairs...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i][0] < sorted[j][0] })
	parts := make([]string, 0, len(sorted))
	for _, kv := range sorted {
		parts = append(parts, escapeQuery(kv[0])+"="+escapeQuery(kv[1]))
	}
	return strings.Join(parts, "&")
}
