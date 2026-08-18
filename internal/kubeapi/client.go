package kubeapi

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// Client is a connection to one apiserver.
type Client struct {
	http   *http.Client
	server string
}

// New prepares a client. It dials nothing: whether the apiserver answers is
// Ping's question, asked separately so a caller controls its own timeout for
// that round trip rather than inheriting one buried here. This is the same
// split dockerd.Open makes, for the same reason.
func New(cfg Config) (*Client, error) {
	if err := cfg.check(); err != nil {
		return nil, err
	}
	server := strings.TrimRight(cfg.Server, "/")
	if _, err := url.Parse(server); err != nil {
		return nil, fmt.Errorf("kubeapi: apiserver address %q is not a URL: %w", cfg.Server, err)
	}

	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(cfg.CAData) {
		return nil, fmt.Errorf(
			"kubeapi: the CA bundle for %s contains no PEM certificate", server)
	}
	tlsCfg := &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}
	if len(cfg.CertData) > 0 {
		pair, err := tls.X509KeyPair(cfg.CertData, cfg.KeyData)
		if err != nil {
			return nil, fmt.Errorf("kubeapi: client certificate and key do not form a pair: %w", err)
		}
		tlsCfg.Certificates = []tls.Certificate{pair}
	}

	tr := &http.Transport{
		TLSClientConfig: tlsCfg,
		// A pod's log is followed for the whole life of the step, which is
		// minutes, so no response header or idle timeout can be set here.
		// Per-request deadlines come from the caller's context.
		DisableCompression: true,
		// HTTP/1.1, explicitly: the exec subresource depends on it, since a
		// 101 upgrade never arrives under HTTP/2. Naming it here means a
		// later edit turning HTTP/2 on has to decide about exec rather than
		// discover it. See websocket.go.
		ForceAttemptHTTP2: false,
	}
	c := &Client{server: server, http: &http.Client{Transport: tr}}
	if cfg.Token != "" {
		c.http.Transport = &bearer{token: cfg.Token, next: tr}
	}
	return c, nil
}

// bearer attaches the token to every request. A RoundTripper rather than a
// header set at each call site, so a new endpoint added later cannot forget
// to authenticate.
type bearer struct {
	token string
	next  http.RoundTripper
}

func (b *bearer) RoundTrip(r *http.Request) (*http.Response, error) {
	// Cloning is required: RoundTrip must not modify the request it is given.
	clone := r.Clone(r.Context())
	clone.Header.Set("Authorization", "Bearer "+b.token)
	return b.next.RoundTrip(clone)
}

// Close releases idle connections.
func (c *Client) Close() error {
	c.http.CloseIdleConnections()
	return nil
}

// Server is the address this client talks to, for an error that has to name
// the cluster it means.
func (c *Client) Server() string { return c.server }

// StatusError is a non-2xx answer, carrying the code and the apiserver's own
// Status message as fields rather than folded into one string. IsNotFound and
// IsForbidden below classify by the thing that varies rather than by
// pattern-matching text that also contains a path.
type StatusError struct {
	Method  string
	Path    string
	Code    int
	Status  string
	Reason  string
	Message string
}

func (e *StatusError) Error() string {
	msg := e.Message
	if msg == "" {
		msg = e.Status
	}
	return fmt.Sprintf("kubeapi: %s %s: %d: %s", e.Method, e.Path, e.Code, msg)
}

// IsNotFound reports whether err is a 404.
func IsNotFound(err error) bool {
	var se *StatusError
	return errors.As(err, &se) && se.Code == http.StatusNotFound
}

// IsConflict reports whether err is a 409, which is what creating an object
// that already exists answers.
func IsConflict(err error) bool {
	var se *StatusError
	return errors.As(err, &se) && se.Code == http.StatusConflict
}

// IsForbidden reports whether err is a 401 or 403, which is the answer a
// service account without the right RBAC gets and is worth telling apart
// from a cluster that is merely unwell.
func IsForbidden(err error) bool {
	var se *StatusError
	return errors.As(err, &se) &&
		(se.Code == http.StatusForbidden || se.Code == http.StatusUnauthorized)
}

// status is the apiserver's error document. Every failure answers with one.
type status struct {
	Message string `json:"message"`
	Reason  string `json:"reason"`
	Code    int    `json:"code"`
}

// do issues one request and returns the live response for a 2xx. The caller
// closes the body.
func (c *Client) do(
	ctx context.Context, method, path, contentType string, body []byte,
) (*http.Response, error) {
	var r io.Reader
	if body != nil {
		r = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.server+path, r)
	if err != nil {
		return nil, fmt.Errorf("kubeapi: %s %s: %w", method, path, err)
	}
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", contentType)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("kubeapi: %s %s: %w", method, path, err)
	}
	if resp.StatusCode >= 400 {
		defer func() { _ = resp.Body.Close() }()
		return nil, statusError(method, path, resp)
	}
	return resp, nil
}

func statusError(method, path string, resp *http.Response) error {
	e := &StatusError{Method: method, Path: path, Code: resp.StatusCode, Status: resp.Status}
	// 64 KiB is generous for a Status document and bounds a misbehaving
	// endpoint answering an error with a stream.
	b, err := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	if err != nil {
		return e
	}
	var s status
	if json.Unmarshal(b, &s) == nil && s.Message != "" {
		e.Message, e.Reason = s.Message, s.Reason
		return e
	}
	// A non-JSON body still carries the useful text: the log endpoint answers
	// plain text for a container that has not started.
	e.Message = strings.TrimSpace(string(b))
	return e
}

// json issues a request whose response is decoded into out.
func (c *Client) jsonRequest(
	ctx context.Context, method, path, contentType string, body []byte, out any,
) error {
	resp, err := c.do(ctx, method, path, contentType, body)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if out == nil {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<20))
		return nil
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return fmt.Errorf("kubeapi: decoding %s %s: %w", method, path, err)
	}
	return nil
}

// Ping asks the apiserver for its version, which is the cheapest request that
// proves both TLS and authentication work.
func (c *Client) Ping(ctx context.Context) (string, error) {
	var v versionInfo
	if err := c.jsonRequest(ctx, http.MethodGet, "/version", "", nil, &v); err != nil {
		return "", err
	}
	if v.GitVersion == "" {
		return "", fmt.Errorf("kubeapi: %s answered /version with no gitVersion", c.server)
	}
	return v.GitVersion, nil
}

// Nodes lists the cluster's nodes.
func (c *Client) Nodes(ctx context.Context) ([]Node, error) {
	var l nodeList
	if err := c.jsonRequest(ctx, http.MethodGet, "/api/v1/nodes", "", nil, &l); err != nil {
		return nil, err
	}
	return l.Items, nil
}

// GetNode reads one node, which is how a pod's OBSERVED platform is found
// after the scheduler has placed it.
func (c *Client) GetNode(ctx context.Context, name string) (Node, error) {
	var n Node
	err := c.jsonRequest(ctx, http.MethodGet, "/api/v1/nodes/"+url.PathEscape(name), "", nil, &n)
	return n, err
}

// CreatePod creates a pod and returns it as the apiserver stored it, which is
// where its UID comes from.
func (c *Client) CreatePod(ctx context.Context, ns string, pod Pod) (Pod, error) {
	pod.APIVersion, pod.Kind = "v1", "Pod"
	b, err := json.Marshal(pod)
	if err != nil {
		return Pod{}, fmt.Errorf("kubeapi: encoding pod: %w", err)
	}
	var out Pod
	err = c.jsonRequest(ctx, http.MethodPost, podsPath(ns), "application/json", b, &out)
	return out, err
}

// GetPod reads one pod.
func (c *Client) GetPod(ctx context.Context, ns, name string) (Pod, error) {
	var p Pod
	err := c.jsonRequest(ctx, http.MethodGet, podPath(ns, name), "", nil, &p)
	return p, err
}

// DeletePod removes a pod. grace is the termination grace period in seconds;
// zero means the container is killed immediately rather than asked politely.
//
// A pod that is already gone is not an error: teardown runs on paths where
// something else may have removed it first, and reporting that as a failure
// would turn a clean run into a failed one.
func (c *Client) DeletePod(ctx context.Context, ns, name string, grace int) error {
	p := podPath(ns, name) + "?gracePeriodSeconds=" + strconv.Itoa(grace)
	err := c.jsonRequest(ctx, http.MethodDelete, p, "", nil, nil)
	if IsNotFound(err) {
		return nil
	}
	return err
}

// PodLogs streams one container's log into w.
//
// Kubernetes merges a container's stdout and stderr into a single log, so
// this is one writer rather than two. See k8sexec's doc for what that costs.
//
// follow keeps the stream open until the container exits. The kubelet holds a
// terminated container's log until the pod is deleted, so opening the stream
// after the container has already finished still returns every byte: there is
// no race between starting the container and attaching to it, unlike the
// Docker case internal/dockerd has to work around.
func (c *Client) PodLogs(ctx context.Context, ns, name, container string, follow bool, w io.Writer) error {
	q := url.Values{}
	q.Set("container", container)
	if follow {
		q.Set("follow", "true")
	}
	resp, err := c.do(ctx, http.MethodGet, podPath(ns, name)+"/log?"+q.Encode(), "", nil)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if _, err := io.Copy(w, resp.Body); err != nil {
		return fmt.Errorf("kubeapi: streaming logs of %s/%s: %w", ns, name, err)
	}
	return nil
}

// CreateSecret creates a secret and returns it as stored.
func (c *Client) CreateSecret(ctx context.Context, ns string, s Secret) (Secret, error) {
	s.APIVersion, s.Kind = "v1", "Secret"
	b, err := json.Marshal(s)
	if err != nil {
		return Secret{}, fmt.Errorf("kubeapi: encoding secret: %w", err)
	}
	var out Secret
	err = c.jsonRequest(ctx, http.MethodPost, secretsPath(ns), "application/json", b, &out)
	return out, err
}

// SetSecretOwner points a secret's ownerReferences at owner, so the
// apiserver's garbage collector deletes it when the owner does.
//
// A separate call because of an unavoidable ordering problem: an
// ownerReference needs the owner's UID, a pod has no UID until stored, and
// the pod cannot be created until the secret it mounts exists. So the
// secret is created unowned and adopted once the pod has an identity. A
// coordinator killed inside that one-round-trip window leaves the secret
// behind; the label exists to find it by.
func (c *Client) SetSecretOwner(ctx context.Context, ns, name string, owner OwnerReference) error {
	patch, err := json.Marshal(map[string]any{
		"metadata": map[string]any{"ownerReferences": []OwnerReference{owner}},
	})
	if err != nil {
		return fmt.Errorf("kubeapi: encoding owner patch: %w", err)
	}
	return c.jsonRequest(ctx, http.MethodPatch, secretPath(ns, name),
		"application/merge-patch+json", patch, nil)
}

// DeleteSecret removes a secret, treating an absent one as success.
func (c *Client) DeleteSecret(ctx context.Context, ns, name string) error {
	err := c.jsonRequest(ctx, http.MethodDelete, secretPath(ns, name), "", nil, nil)
	if IsNotFound(err) {
		return nil
	}
	return err
}

func podsPath(ns string) string {
	return "/api/v1/namespaces/" + url.PathEscape(ns) + "/pods"
}

func podPath(ns, name string) string {
	return podsPath(ns) + "/" + url.PathEscape(name)
}

func secretsPath(ns string) string {
	return "/api/v1/namespaces/" + url.PathEscape(ns) + "/secrets"
}

func secretPath(ns, name string) string {
	return secretsPath(ns) + "/" + url.PathEscape(name)
}

// PollInterval is how often pod status is re-read while waiting.
//
// A var so a test can shorten it. 250ms is the trade polling makes instead of
// a watch: up to a quarter second of extra latency at each state change, four
// requests a second per in-flight step against the apiserver, and none of
// resourceVersion bookkeeping, bookmark events or watch re-establishment
// after the connection drops. For a coordinator whose longest wait is one
// pod's lifetime, that is the cheaper side of the trade.
var PollInterval = 250 * time.Millisecond
