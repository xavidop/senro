package s3

import (
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"strings"
	"testing"
	"time"
)

// The vectors below are AWS's own published SigV4 worked examples, not a
// frozen signature this package produced, which would only prove the code
// agrees with itself. The vectors prove the arithmetic; a real server
// (MinIO, client_minio_test.go) proves the whole request.
const (
	exampleAccessKey = "AKIAIOSFODNN7EXAMPLE"
	exampleSecretKey = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
	emptyPayload     = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
)

func exampleTime(t *testing.T) time.Time {
	t.Helper()
	ts, err := time.Parse("20060102T150405Z", "20130524T000000Z")
	if err != nil {
		t.Fatalf("parsing the example timestamp: %v", err)
	}
	return ts
}

// TestSignGetObjectMatchesTheAWSWorkedExample pins the whole chain: canonical
// request, string to sign, signing key and signature.
func TestSignGetObjectMatchesTheAWSWorkedExample(t *testing.T) {
	req, err := http.NewRequest(http.MethodGet, "https://examplebucket.s3.amazonaws.com/test.txt", nil)
	if err != nil {
		t.Fatalf("building the request: %v", err)
	}
	req.Header.Set("Range", "bytes=0-9")

	cred := credentials{AccessKeyID: exampleAccessKey, SecretAccessKey: exampleSecretKey}
	sign(req, cred, "us-east-1", emptyPayload, exampleTime(t))

	const want = "AWS4-HMAC-SHA256 " +
		"Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, " +
		"SignedHeaders=host;range;x-amz-content-sha256;x-amz-date, " +
		"Signature=f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41"
	if got := req.Header.Get("Authorization"); got != want {
		t.Errorf("Authorization =\n  %s\nwant\n  %s", got, want)
	}
}

// TestSignPutObjectMatchesTheAWSWorkedExample covers the other half of what
// this package sends: a PUT, with a signed (not UNSIGNED-PAYLOAD) body hash
// and two extra headers, one of which sorts after x-amz-date.
func TestSignPutObjectMatchesTheAWSWorkedExample(t *testing.T) {
	body := "Welcome to Amazon S3."
	sum := sha256.Sum256([]byte(body))
	payload := hex.EncodeToString(sum[:])
	if want := "44ce7dd67c959e0d3524ffac1771dfbba87d2b6b4b4e99e42034a8b803f8b072"; payload != want {
		t.Fatalf("the example body hashes to %s, want %s: the vector below is not about this body", payload, want)
	}

	req, err := http.NewRequest(
		http.MethodPut, "https://examplebucket.s3.amazonaws.com/test%24file.text", strings.NewReader(body))
	if err != nil {
		t.Fatalf("building the request: %v", err)
	}
	req.Header.Set("Date", "Fri, 24 May 2013 00:00:00 GMT")
	req.Header.Set("X-Amz-Storage-Class", "REDUCED_REDUNDANCY")

	cred := credentials{AccessKeyID: exampleAccessKey, SecretAccessKey: exampleSecretKey}
	sign(req, cred, "us-east-1", payload, exampleTime(t))

	const want = "AWS4-HMAC-SHA256 " +
		"Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, " +
		"SignedHeaders=date;host;x-amz-content-sha256;x-amz-date;x-amz-storage-class, " +
		"Signature=98ad721746da40c64f1a55b78f14c238d841ea1380cd77a1b5971af0ece108bd"
	if got := req.Header.Get("Authorization"); got != want {
		t.Errorf("Authorization =\n  %s\nwant\n  %s", got, want)
	}
}

// TestSignSetsTheHeadersS3RequiresRatherThanOnlyAuthorization: a signature
// over headers the request does not actually carry is rejected by every real
// server, so signing has to add them, not merely account for them.
func TestSignSetsTheHeadersS3RequiresRatherThanOnlyAuthorization(t *testing.T) {
	req, err := http.NewRequest(http.MethodGet, "https://example.invalid/b/k", nil)
	if err != nil {
		t.Fatalf("building the request: %v", err)
	}
	cred := credentials{
		AccessKeyID: exampleAccessKey, SecretAccessKey: exampleSecretKey, SessionToken: "SESSION",
	}
	sign(req, cred, "eu-west-1", emptyPayload, exampleTime(t))

	if got := req.Header.Get("X-Amz-Date"); got != "20130524T000000Z" {
		t.Errorf("X-Amz-Date = %q, want %q", got, "20130524T000000Z")
	}
	if got := req.Header.Get("X-Amz-Content-Sha256"); got != emptyPayload {
		t.Errorf("X-Amz-Content-Sha256 = %q, want %q", got, emptyPayload)
	}
	if got := req.Header.Get("X-Amz-Security-Token"); got != "SESSION" {
		t.Errorf("X-Amz-Security-Token = %q, want %q", got, "SESSION")
	}
	if !strings.Contains(req.Header.Get("Authorization"), "x-amz-security-token") {
		t.Errorf("the session token is sent but not signed, which every real server rejects: %s",
			req.Header.Get("Authorization"))
	}
}

// TestSignNeverPutsTheSecretKeyOnTheWire: the secret is an HMAC key and
// nothing else; if it can reach a header, it can reach a proxy log.
func TestSignNeverPutsTheSecretKeyOnTheWire(t *testing.T) {
	req, err := http.NewRequest(http.MethodPut, "https://example.invalid/b/k", strings.NewReader("x"))
	if err != nil {
		t.Fatalf("building the request: %v", err)
	}
	cred := credentials{
		AccessKeyID: exampleAccessKey, SecretAccessKey: exampleSecretKey, SessionToken: "SESSION",
	}
	sign(req, cred, "us-east-1", emptyPayload, exampleTime(t))

	for name, values := range req.Header {
		for _, v := range values {
			if strings.Contains(v, exampleSecretKey) {
				t.Fatalf("header %s carries the secret access key", name)
			}
		}
	}
	if strings.Contains(req.URL.String(), exampleSecretKey) {
		t.Fatal("the URL carries the secret access key")
	}
}

func TestEscapePathEncodesEverythingOutsideTheUnreservedSet(t *testing.T) {
	for _, tc := range []struct{ in, want string }{
		{"/simple/key", "/simple/key"},
		{"/a b", "/a%20b"},
		{"/a+b", "/a%2Bb"},
		{"/a=b", "/a%3Db"},
		{"/a~b.c_d-e", "/a~b.c_d-e"},
		{"/25%", "/25%25"},
		{"/café", "/caf%C3%A9"},
		{"/test$file.text", "/test%24file.text"},
	} {
		if got := escapePath(tc.in); got != tc.want {
			t.Errorf("escapePath(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}
