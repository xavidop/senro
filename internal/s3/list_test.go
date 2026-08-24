package s3_test

import (
	"fmt"
	"strings"
	"testing"
)

// TestListReturnsOnlyTheMatchingPrefix is the property a scratch cache's
// RestoreKeys fallback rests on: a prefix names a set, and nothing outside it
// may come back, or one project's "gomod-" would restore another's tree.
func TestListReturnsOnlyTheMatchingPrefix(t *testing.T) {
	t.Parallel()
	c := live(t)
	ctx := t.Context()

	for _, k := range []string{
		"listing/gomod-aaa", "listing/gomod-bbb", "listing/npm-ccc",
	} {
		if err := c.PutBytes(ctx, k, []byte(k)); err != nil {
			t.Fatalf("PutBytes %s: %v", k, err)
		}
	}

	page, err := c.List(ctx, "listing/gomod-", "")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(page.Objects) != 2 {
		t.Fatalf("List returned %d objects, want 2: %+v", len(page.Objects), page.Objects)
	}
	for _, o := range page.Objects {
		if !strings.HasPrefix(o.Key, "listing/gomod-") {
			t.Errorf("List returned %q, which is outside the prefix asked for", o.Key)
		}
		if o.LastModified.IsZero() {
			t.Errorf("List returned %q with no LastModified, so a fleet cannot order it", o.Key)
		}
		if o.Size == 0 {
			t.Errorf("List returned %q with size 0, want the bytes just stored", o.Key)
		}
	}
	if page.Token != "" {
		t.Errorf("a two-object listing reported more pages: token %q", page.Token)
	}
}

// A prefix nothing matches is an empty listing, not an error: a cold cache is
// the ordinary state of a fresh bucket, and the scratch cache's contract is
// that a miss costs time and nothing else.
func TestListOfAnUnmatchedPrefixIsEmptyAndNotAnError(t *testing.T) {
	t.Parallel()
	c := live(t)

	page, err := c.List(t.Context(), "nothing-has-this-prefix/", "")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(page.Objects) != 0 {
		t.Errorf("List invented %d objects", len(page.Objects))
	}
	if page.Token != "" {
		t.Errorf("an empty listing reported more pages: token %q", page.Token)
	}
}

// TestListPagesThroughEverything drives the continuation token past one page,
// which is the path a long-lived prefix reaches and the one a single-page
// test would never exercise. listPageSize is 1000, so this asks for more than
// that in one prefix.
func TestListPagesThroughEverything(t *testing.T) {
	if testing.Short() {
		t.Skip("stores 1001 objects")
	}
	t.Parallel()
	c := live(t)
	ctx := t.Context()

	const want = 1001
	for i := range want {
		k := fmt.Sprintf("paged/%04d", i)
		if err := c.PutBytes(ctx, k, []byte("x")); err != nil {
			t.Fatalf("PutBytes %s: %v", k, err)
		}
	}

	seen := map[string]bool{}
	var pages int
	for token := ""; ; {
		page, err := c.List(ctx, "paged/", token)
		if err != nil {
			t.Fatalf("List: %v", err)
		}
		pages++
		for _, o := range page.Objects {
			if seen[o.Key] {
				t.Fatalf("%q came back on two pages", o.Key)
			}
			seen[o.Key] = true
		}
		if page.Token == "" {
			break
		}
		token = page.Token
		if pages > 10 {
			t.Fatal("the listing never reported a last page")
		}
	}
	if len(seen) != want {
		t.Errorf("walked %d objects across %d pages, want %d", len(seen), pages, want)
	}
	if pages < 2 {
		t.Errorf("1001 objects came back in %d page(s), so the token was never exercised", pages)
	}
}
