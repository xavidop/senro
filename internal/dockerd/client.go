package dockerd

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

// APIVersion is the Engine API version every request is prefixed with.
// Pinning it means a newer daemon does not change this client's behaviour
// under it; the daemon negotiates down for any version it still supports.
// 1.44 ships with Docker Engine 25.0 (January 2024).
const APIVersion = "v1.44"

// Client is a connection to one daemon.
type Client struct {
	http   *http.Client
	socket string
}

// Open resolves the daemon socket and prepares a client to it.
//
// Open does not dial or ping: whether a daemon answers is Ping's job, so a
// caller detecting an absent daemon (see dockertest.Require) controls its
// own timeout. "Exists" means the file's own type is a socket, not merely
// that stat succeeds: a stray file or directory at the path gets a clear
// refusal rather than a confusing dial error moments later.
func Open() (*Client, error) {
	socket, err := SocketPath()
	if err != nil {
		return nil, err
	}
	fi, err := os.Stat(socket)
	if err != nil {
		return nil, fmt.Errorf("dockerd: no daemon socket at %s: %w", socket, err)
	}
	if fi.Mode()&os.ModeSocket == 0 {
		return nil, fmt.Errorf(
			"dockerd: %s exists but is not a unix socket; a container runtime needs to be listening "+
				"there, not a stray file or directory", socket)
	}
	dialer := &net.Dialer{Timeout: 5 * time.Second}
	c := &Client{
		socket: socket,
		http: &http.Client{
			Transport: &http.Transport{
				DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
					return dialer.DialContext(ctx, "unix", socket)
				},
				// A container's output stream is followed for the life of
				// the step, so no global response or idle timeout here;
				// per-request deadlines come from the caller's context.
				DisableCompression: true,
			},
		},
	}
	return c, nil
}

// Close releases idle connections. The daemon needs no goodbye.
func (c *Client) Close() error {
	c.http.CloseIdleConnections()
	return nil
}

// Socket is the path this client dialled, for an error message that has to
// say which daemon it means.
func (c *Client) Socket() string { return c.socket }

// do issues one request. The host in the URL is a placeholder: the transport
// dials a unix socket and ignores it, but net/http still requires a
// syntactically valid absolute URL.
//
// headers is nil for every request but one: the credentialed pull, whose
// X-Registry-Auth must not reach the URL. Spelled as a parameter rather than
// as a second request function so there stays exactly one place a request to
// the daemon is built, and so which requests carry a header is one grep.
func (c *Client) do(
	ctx context.Context, method, path string, body any, headers map[string]string,
) (*http.Response, error) {
	var r io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("dockerd: encoding %s %s: %w", method, path, err)
		}
		r = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, "http://docker/"+APIVersion+path, r)
	if err != nil {
		return nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	for name, value := range headers {
		req.Header.Set(name, value)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("dockerd: %s %s: %w", method, path, err)
	}
	if resp.StatusCode >= 400 {
		defer func() { _ = resp.Body.Close() }()
		return nil, statusError(method, path, resp)
	}
	return resp, nil
}

// apiError is what do returns for a non-2xx response. Status code and
// daemon message stay separate fields so isNotFound and isNotRunning
// classify by the thing that varies instead of pattern-matching a formatted
// string that also contains the method and path.
type apiError struct {
	method, path string
	statusCode   int
	status       string // e.g. "404 Not Found", from http.Response.Status
	message      string
}

func (e *apiError) Error() string {
	return fmt.Sprintf("dockerd: %s %s: %s: %s", e.method, e.path, e.status, e.message)
}

// statusError turns the daemon's own error document into an error: the
// daemon's {"message":"..."} is far more useful than the status code alone
// ("No such image: nosuch:1" rather than "404").
func statusError(method, path string, resp *http.Response) error {
	var doc struct {
		Message string `json:"message"`
	}
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 8<<10))
	_ = json.Unmarshal(b, &doc)
	msg := doc.Message
	if msg == "" {
		msg = strings.TrimSpace(string(b))
	}
	return &apiError{method: method, path: path, statusCode: resp.StatusCode, status: resp.Status, message: msg}
}

// Ping verifies the daemon is answering, and is what Open's callers use to
// decide whether the container executor can run at all.
func (c *Client) Ping(ctx context.Context) error {
	resp, err := c.do(ctx, http.MethodGet, "/_ping", nil, nil)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	_, _ = io.Copy(io.Discard, resp.Body)
	return nil
}

// ref escapes an image reference or container id for a path segment. A
// reference contains "/" and ":" and must not be read as path structure.
func ref(s string) string { return url.PathEscape(s) }

// ImageInfo is what senro needs from an image: its identity, its platform,
// the user it runs as, and the environment it contributes.
type ImageInfo struct {
	// ID is the image's config digest, "sha256:...". Present for every image,
	// including one built locally that was never pushed anywhere.
	ID string
	// RepoDigests are the registry digests, "node@sha256:...". Empty for a
	// locally built image, which is why Digest below falls back to ID.
	RepoDigests []string
	OS          string
	Arch        string
	// User is the image's own default user, from its config. Empty means root.
	User string
	// Env is the image's own environment, merged under the container's, so
	// a cache key's env component can be built from what the step actually
	// receives rather than what the plan declared.
	Env []string
}

// Digest is the content address senro identifies this image by: the
// registry digest for the given repository when there is one, the config
// digest otherwise.
//
// The cache key uses the digest, not the tag, so a mutable tag changing
// invalidates the cache. A locally built image has no registry digest, and
// the config digest is equally content-addressed, so refusing one would
// make `docker build -t local/test .` unusable for no gain.
func (i ImageInfo) Digest(repository string) string {
	for _, rd := range i.RepoDigests {
		if name, digest, ok := strings.Cut(rd, "@"); ok && name == repository {
			return digest
		}
	}
	if len(i.RepoDigests) == 1 {
		if _, digest, ok := strings.Cut(i.RepoDigests[0], "@"); ok {
			return digest
		}
	}
	return i.ID
}

// ImageInspect reports an image the daemon already has. ok is false, with no
// error, when the daemon simply does not have it: that is the ordinary
// pre-pull case, not a failure.
func (c *Client) ImageInspect(ctx context.Context, image string) (ImageInfo, bool, error) {
	resp, err := c.do(ctx, http.MethodGet, "/images/"+ref(image)+"/json", nil, nil)
	if err != nil {
		if isNotFound(err) {
			return ImageInfo{}, false, nil
		}
		return ImageInfo{}, false, err
	}
	defer func() { _ = resp.Body.Close() }()

	var doc struct {
		ID          string   `json:"Id"`
		RepoDigests []string `json:"RepoDigests"`
		Os          string   `json:"Os"`
		Arch        string   `json:"Architecture"`
		Config      struct {
			User string   `json:"User"`
			Env  []string `json:"Env"`
		} `json:"Config"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&doc); err != nil {
		return ImageInfo{}, false, fmt.Errorf("dockerd: decoding image %s: %w", image, err)
	}
	return ImageInfo{
		ID: doc.ID, RepoDigests: doc.RepoDigests,
		OS: doc.Os, Arch: doc.Arch,
		User: doc.Config.User, Env: doc.Config.Env,
	}, true, nil
}

// isNotFound reports whether err is the apiError for a 404: the ordinary
// "daemon does not have it" outcome ImageInspect, ContainerKill and
// ContainerRemove all treat as something other than failure.
func isNotFound(err error) bool {
	var ae *apiError
	return errors.As(err, &ae) && ae.statusCode == http.StatusNotFound
}

// ErrRegistryAuth means the registry refused the credential the pull was
// made with, or demanded one and was given none.
//
// Separate from every other pull failure because the two answers a registry
// gives are one status code apart and worlds apart to whoever has to fix
// them: "no such image" sends somebody to check a tag, "the credential was
// refused" sends them to check a token. See internal/oci.ErrDenied, which
// draws the same line on the cache's own registry path.
var ErrRegistryAuth = errors.New("dockerd: the registry refused the credential")

// RegistryAuth is one credential for one pull.
//
// Password is a resolved secret and lives only as long as the pull: it goes
// into one request header and is never logged, never in a URL, and never in
// a container's configuration. senro runs no credential helper and reads no
// ~/.docker/config.json; the value arrives already resolved, exactly as
// oci.Config's does.
type RegistryAuth struct {
	Username string
	Password string
}

// header renders the credential as the daemon's X-Registry-Auth value:
// base64url of the JSON the Engine API documents. Padded base64url, byte for
// byte what Docker's own client sends, because a daemon that cannot decode
// the header quietly pulls anonymously instead of saying so.
func (a *RegistryAuth) header(registry string) (string, error) {
	doc := struct {
		Username      string `json:"username,omitempty"`
		Password      string `json:"password,omitempty"`
		ServerAddress string `json:"serveraddress,omitempty"`
	}{Username: a.Username, Password: a.Password, ServerAddress: registry}
	b, err := json.Marshal(doc)
	if err != nil {
		// Unreachable for three strings, and reported rather than ignored: a
		// dropped header is an anonymous pull that fails somewhere else.
		return "", fmt.Errorf("dockerd: encoding the registry credential: %w", err)
	}
	return base64.URLEncoding.EncodeToString(b), nil
}

// DefaultRegistry is where a reference naming no host lives. Spelled as the
// index rather than as "docker.io" because that is the server address every
// Docker client puts in an auth document for the Hub.
const DefaultRegistry = "https://index.docker.io/v1/"

// RegistryOf reports which registry a reference is pulled from, for the
// auth document's server address and for an error that has to name the host
// somebody must fix a credential at.
//
// Docker's own rule: the part before the first "/" is a registry when it
// contains a "." or a ":", or is exactly "localhost"; everything else is a
// path on the Hub ("alpine", "library/alpine", "acme/builder").
//
// The ONE place a reference's registry is derived, exported for the same
// reason SplitRef is: two answers to this question would drift.
func RegistryOf(image string) string {
	host, _, ok := strings.Cut(image, "/")
	if !ok || (!strings.ContainsAny(host, ".:") && host != "localhost") {
		return DefaultRegistry
	}
	return host
}

// ImagePull pulls an image and drains the daemon's progress stream. auth is
// nil for a public image, or for one the daemon is already logged in to.
//
// Draining is not optional: the daemon reports pull failures inside the
// stream as {"error":"..."} objects after a 200 header, so closing the body
// early returns nil for a pull that did not happen.
func (c *Client) ImagePull(ctx context.Context, image string, auth *RegistryAuth) error {
	repo, tag := SplitRef(image)
	q := url.Values{"fromImage": {repo}, "tag": {tag}}
	path := "/images/create?" + q.Encode()

	var headers map[string]string
	if auth != nil {
		// The credential travels in a header and NEVER in the query: a URL is
		// what every error in this file quotes back, and what the daemon logs.
		h, err := auth.header(RegistryOf(image))
		if err != nil {
			return err
		}
		headers = map[string]string{"X-Registry-Auth": h}
	}

	resp, err := c.do(ctx, http.MethodPost, path, nil, headers)
	if err != nil {
		return classifyPull(image, err)
	}
	defer func() { _ = resp.Body.Close() }()

	dec := json.NewDecoder(resp.Body)
	for {
		var msg struct {
			Error string `json:"error"`
		}
		if err := dec.Decode(&msg); err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return fmt.Errorf("dockerd: pulling %s: %w", image, err)
		}
		if msg.Error != "" {
			return classifyPull(image, fmt.Errorf("dockerd: pulling %s: %s", image, msg.Error))
		}
	}
}

// classifyPull marks a pull failure that is the registry refusing a
// credential, so a caller can say which of the two answers it got.
//
// By the daemon's own message rather than by status code alone, because the
// codes do not separate the cases: Docker Hub answers 404 for a private
// repository ("pull access denied for x/y, repository does not exist or may
// require 'docker login'") and 404 for a tag that is genuinely absent. The
// same reasoning isNotRunning uses, with the same care about matching a
// phrase rather than a word: "permission denied" from a failed layer
// extraction must not read as a refused credential.
func classifyPull(image string, err error) error {
	if isAuthRefusal(err) {
		return fmt.Errorf("%w: pulling %s: %w", ErrRegistryAuth, image, err)
	}
	return err
}

// authRefusals are the phrases a registry's refusal reaches this client as,
// measured against ghcr.io, Docker Hub, GitLab and Quay rather than guessed.
var authRefusals = []string{
	"unauthorized",
	"authentication required",
	"failed to authorize",
	"access denied",
	": denied",
	"denied: ",
	"access forbidden",
	"incorrect username or password",
	"docker login",
}

func isAuthRefusal(err error) bool {
	var ae *apiError
	if errors.As(err, &ae) &&
		(ae.statusCode == http.StatusUnauthorized || ae.statusCode == http.StatusForbidden) {
		return true
	}
	msg := strings.ToLower(err.Error())
	for _, phrase := range authRefusals {
		if strings.Contains(msg, phrase) {
			return true
		}
	}
	return false
}

// SplitRef splits an image reference into the repository and the tag or
// digest that selects one image within it.
//
// The subtlety is the port: "localhost:5000/senro/x:v1" splits at the last
// colon AFTER the last slash. A digest reference ("node@sha256:...") splits
// on "@", and the digest is returned as the tag because that is what
// /images/create's tag parameter accepts.
//
// This is the ONE place a reference is parsed, exported so both this
// package and containerexec call it: two parsers for one grammar would
// drift.
func SplitRef(image string) (repository, tag string) {
	if repo, digest, ok := strings.Cut(image, "@"); ok {
		return repo, digest
	}
	if i := strings.LastIndex(image, ":"); i > strings.LastIndex(image, "/") {
		return image[:i], image[i+1:]
	}
	return image, "latest"
}

// Bind is one host directory made visible inside a container.
type Bind struct {
	Source   string
	Target   string
	ReadOnly bool
}

// Port is one container port published to the host's loopback interface.
//
// The host port is deliberately not a field: the daemon assigns a free one
// and ContainerHostAddress reads it back. A test picking its own would race
// every other test on the machine for it.
type Port struct {
	// Container is the port inside the container, TCP.
	Container int
}

// ContainerSpec is everything senro sets when it creates a container.
// Anything absent here is deliberately left at the daemon's default.
type ContainerSpec struct {
	Image string
	// Entrypoint overrides the image's own; empty leaves it alone, which is
	// what every pipeline step wants. It exists for test support that must
	// run setup before the image's real program starts (dockertest's MinIO
	// server), since there is no other hook between create and start.
	Entrypoint []string
	Cmd        []string
	Env        []string
	WorkingDir string
	User       string
	Binds      []Bind
	Labels     map[string]string
	// Tty allocates a pseudo-terminal. False for every step (a step's
	// output is two streams and a TTY has one), true only for a terminal
	// session (containerexec's RunTerminal). With it set the daemon stops
	// multiplexing the attach stream: read it RAW, never through Demux.
	Tty bool
	// Ports publishes container ports to 127.0.0.1. Empty for every
	// container senro runs; it exists for tests that reach a real server in
	// a container (dockertest, internal/s3): on Docker Desktop a
	// container's bridge address is not routable from the host, so
	// publishing is the only way in.
	Ports []Port

	// Stdin opens and attaches the container's standard input, for the one
	// caller that needs it: an interactive session (see ContainerAttach).
	//
	// The false default is load-bearing: an open stdin nothing writes to
	// does not read EOF, it BLOCKS, so a step whose command reads stdin
	// would hang forever (TestContainerCreateWithoutStdinLeavesItClosed).
	// It also sets StdinOnce, so the container's stdin closes when the
	// attach stream half-closes: what lets an operator's ^D end a session
	// without killing the container.
	Stdin bool
}

// ContainerCreate creates a container and returns its id.
//
// LogConfig is pinned to the json-file driver rather than the daemon's
// default and is not exposed on ContainerSpec: ContainerLogs is how senro
// reads a step's output, and the journald, syslog and none drivers do not
// serve it. Pinning per container keeps pipelines working on a machine
// whose daemon defaults to journald.
//
// Tty stays false for every step (two streams, not one); it is negotiable
// only for a terminal session. See ContainerSpec.Tty.
func (c *Client) ContainerCreate(ctx context.Context, spec ContainerSpec) (string, error) {
	resp, err := c.do(ctx, http.MethodPost, "/containers/create", createBody(spec), nil)
	if err != nil {
		return "", err
	}
	defer func() { _ = resp.Body.Close() }()
	var doc struct {
		ID string `json:"Id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&doc); err != nil {
		return "", fmt.Errorf("dockerd: decoding create response: %w", err)
	}
	return doc.ID, nil
}

// createBody renders a spec as the daemon's /containers/create document.
// Split out so a test can assert on what would be sent without a daemon:
// the absent fields matter as much as the present ones.
func createBody(spec ContainerSpec) map[string]any {
	binds := make([]string, 0, len(spec.Binds))
	for _, b := range spec.Binds {
		s := b.Source + ":" + b.Target
		if b.ReadOnly {
			s += ":ro"
		}
		binds = append(binds, s)
	}
	hostConfig := map[string]any{
		"Binds":      binds,
		"AutoRemove": false,
		"LogConfig":  map[string]any{"Type": "json-file"},
	}
	body := map[string]any{
		"Image":        spec.Image,
		"Cmd":          spec.Cmd,
		"Env":          spec.Env,
		"WorkingDir":   spec.WorkingDir,
		"User":         spec.User,
		"Tty":          spec.Tty,
		"AttachStdout": true,
		"AttachStderr": true,
		// All three together, never individually: an open stdin nothing
		// writes to blocks rather than reading EOF, and StdinOnce is what
		// lets an operator's ^D end a session without killing the container.
		"OpenStdin":   spec.Stdin,
		"AttachStdin": spec.Stdin,
		"StdinOnce":   spec.Stdin,
		"Labels":      spec.Labels,
		"HostConfig":  hostConfig,
	}
	// Absent unless overridden, so a step's create request is byte-identical
	// to what it was before this field existed.
	if len(spec.Entrypoint) > 0 {
		body["Entrypoint"] = spec.Entrypoint
	}
	// Both fields stay absent when nothing is published, for the same reason.
	if len(spec.Ports) > 0 {
		exposed := make(map[string]any, len(spec.Ports))
		bindings := make(map[string]any, len(spec.Ports))
		for _, p := range spec.Ports {
			key := portKey(p.Container)
			exposed[key] = map[string]any{}
			// An empty HostPort asks the daemon for a free one. 127.0.0.1:
			// a server started for a test has no business being reachable
			// from the network.
			bindings[key] = []map[string]any{{"HostIp": "127.0.0.1", "HostPort": ""}}
		}
		body["ExposedPorts"] = exposed
		hostConfig["PortBindings"] = bindings
	}
	return body
}

func portKey(port int) string { return strconv.Itoa(port) + "/tcp" }

// ContainerHostAddress reports the host address a published container port
// landed on, as "127.0.0.1:<port>".
//
// The second return is false while the daemon has published nothing for that
// port yet, which is an ordinary state for the moments between create and a
// fully started container, not an error. A caller polls.
func (c *Client) ContainerHostAddress(ctx context.Context, id string, port int) (string, bool, error) {
	raw, err := c.ContainerInspectRaw(ctx, id)
	if err != nil {
		return "", false, err
	}
	return hostAddress(raw, port)
}

// hostAddress picks one published port out of an inspect document.
func hostAddress(inspect []byte, port int) (string, bool, error) {
	var doc struct {
		NetworkSettings struct {
			Ports map[string][]struct {
				HostIP   string `json:"HostIp"`
				HostPort string `json:"HostPort"`
			} `json:"Ports"`
		} `json:"NetworkSettings"`
	}
	if err := json.Unmarshal(inspect, &doc); err != nil {
		return "", false, fmt.Errorf("dockerd: decoding published ports: %w", err)
	}
	for _, b := range doc.NetworkSettings.Ports[portKey(port)] {
		if b.HostPort == "" {
			continue
		}
		host := b.HostIP
		// The daemon reports 0.0.0.0 for a binding it made on every
		// interface. Dialling that address is not portable; the loopback
		// alias for it is what a client actually connects to.
		if host == "" || host == "0.0.0.0" || host == "::" {
			host = "127.0.0.1"
		}
		return net.JoinHostPort(host, b.HostPort), true, nil
	}
	return "", false, nil
}

// ContainerStart starts a created container.
func (c *Client) ContainerStart(ctx context.Context, id string) error {
	resp, err := c.do(ctx, http.MethodPost, "/containers/"+ref(id)+"/start", nil, nil)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	return nil
}

// ContainerInspectRaw returns the daemon's own JSON description of a
// container, byte for byte what `docker inspect` prints.
//
// Raw rather than decoded: the caller is a test proving a secret's value
// appears in NO field of the container's configuration, and a struct's
// named fields would be an allowlist a value could slip past. Scanning the
// whole document has no such gap.
func (c *Client) ContainerInspectRaw(ctx context.Context, id string) ([]byte, error) {
	resp, err := c.do(ctx, http.MethodGet, "/containers/"+ref(id)+"/json", nil, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("dockerd: reading inspect response for %s: %w", id, err)
	}
	return b, nil
}

// ContainerLogs follows the container's output from its first byte until the
// container exits, demultiplexing into stdout and stderr.
//
// From the first byte, not from now: follow=1 with no since or tail replays
// everything the log driver already holds, so output produced between start
// and this call is not lost. That race is real and would otherwise drop the
// first line of every fast step.
func (c *Client) ContainerLogs(ctx context.Context, id string, stdout, stderr io.Writer) error {
	q := url.Values{"follow": {"1"}, "stdout": {"1"}, "stderr": {"1"}}
	resp, err := c.do(ctx, http.MethodGet, "/containers/"+ref(id)+"/logs?"+q.Encode(), nil, nil)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	return demux(resp.Body, stdout, stderr)
}

// ContainerWait blocks until the container is not running and reports its
// status code.
//
// The condition is not-running, not next-exit: senro calls ContainerLogs
// first, so the exit has usually already happened by the time this request
// arrives, and next-exit would wait for an event that never comes.
// not-running is correct on either side of the exit.
func (c *Client) ContainerWait(ctx context.Context, id string) (int, error) {
	resp, err := c.do(ctx, http.MethodPost, "/containers/"+ref(id)+"/wait?condition=not-running", nil, nil)
	if err != nil {
		return 0, err
	}
	defer func() { _ = resp.Body.Close() }()
	var doc struct {
		StatusCode int `json:"StatusCode"`
		Error      *struct {
			Message string `json:"Message"`
		} `json:"Error"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&doc); err != nil {
		return 0, fmt.Errorf("dockerd: decoding wait response: %w", err)
	}
	if doc.Error != nil && doc.Error.Message != "" {
		return doc.StatusCode, fmt.Errorf("dockerd: waiting for %s: %s", id, doc.Error.Message)
	}
	return doc.StatusCode, nil
}

// ContainerKill stops a container immediately. A container that has already
// exited is not an error: the caller is tearing down and the outcome it wants
// is already true.
func (c *Client) ContainerKill(ctx context.Context, id string) error {
	resp, err := c.do(ctx, http.MethodPost, "/containers/"+ref(id)+"/kill", nil, nil)
	if err != nil {
		if isNotRunning(err) || isNotFound(err) {
			return nil
		}
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	return nil
}

// isNotRunning reports the daemon's "container is not running" response to
// a kill of an already-exited container: a 409 with no distinct status, so
// the message is the only signal. Checked against the apiError's message
// field, not the whole formatted error, so it cannot fire on an unrelated
// 409 whose path contains the same words.
func isNotRunning(err error) bool {
	var ae *apiError
	return errors.As(err, &ae) && strings.Contains(ae.message, "is not running")
}

// ContainerRemove deletes the container and its writable layer. force,
// because a container senro is removing has already been killed or has
// exited, and a removal that fails because of a race leaves an orphan on the
// host for every step of every run.
func (c *Client) ContainerRemove(ctx context.Context, id string) error {
	resp, err := c.do(ctx, http.MethodDelete, "/containers/"+ref(id)+"?force=1&v=1", nil, nil)
	if err != nil {
		if isNotFound(err) {
			return nil
		}
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	return nil
}

// ContainerList reports the ids of containers matching every label,
// including stopped ones (all=1). The leak tests use it, filtered to the
// "senro.run" label, to prove a closed sandbox left nothing behind.
func (c *Client) ContainerList(ctx context.Context, labels map[string]string) ([]string, error) {
	filters := map[string][]string{}
	for k, v := range labels {
		filters["label"] = append(filters["label"], k+"="+v)
	}
	b, err := json.Marshal(filters)
	if err != nil {
		return nil, err
	}
	q := url.Values{"all": {"1"}, "filters": {string(b)}}
	resp, err := c.do(ctx, http.MethodGet, "/containers/json?"+q.Encode(), nil, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	var docs []struct {
		ID string `json:"Id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&docs); err != nil {
		return nil, fmt.Errorf("dockerd: decoding container list: %w", err)
	}
	out := make([]string, 0, len(docs))
	for _, d := range docs {
		out = append(out, d.ID)
	}
	return out, nil
}

// ContainerResize tells the daemon a container's terminal changed size.
// Only meaningful with Tty; the daemon answers 500 for one not running. The
// size is advisory and a failure is the caller's to ignore: a terminal
// briefly at the wrong width is cosmetic.
func (c *Client) ContainerResize(ctx context.Context, id string, cols, rows uint16) error {
	path := fmt.Sprintf("/containers/%s/resize?w=%d&h=%d", url.PathEscape(id), cols, rows)
	resp, err := c.do(ctx, http.MethodPost, path, nil, nil)
	if err != nil {
		return err
	}
	return resp.Body.Close()
}
