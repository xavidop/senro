package main

// GitHub App mode. Given an App id and its private key, the dispatcher mints
// an installation token per delivery, creates a queued check run before the
// pipeline starts, hands both to the pipeline through the SENRO_GITHUB_*
// environment (read by notify.GitHubChecksFromEnv), and closes the check
// itself when the pipeline never does. Everything provider-specific lives in
// this file so main.go stays provider-neutral and small.
//
// The installation token is a credential: it travels in the pipeline's
// environment and in Authorization headers, and nowhere else. No error or log
// line built here includes it, the JWT, or the key.

import (
	"bytes"
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/xavidop/senro/internal/persist"
	"github.com/xavidop/senro/notify"
)

// githubCallTimeout bounds each GitHub API call, so a slow API delays a
// delivery's 202 by at most a few calls' worth.
const githubCallTimeout = 10 * time.Second

type githubApp struct {
	appID                      string
	key                        *rsa.PrivateKey
	api, checkName, detailsURL string
	client                     *http.Client
}

// appFlagsError is main's validation of the App flags: the id and the key
// file mean nothing apart. A non-nil result is a usage error (exit 2).
func appFlagsError(appID, keyFile string) error {
	if (appID == "") != (keyFile == "") {
		return errors.New("-github-app-id and -github-private-key-file must be given together")
	}
	return nil
}

// loadGitHubApp reads a PEM private key, PKCS1 ("RSA PRIVATE KEY") or PKCS8
// ("PRIVATE KEY"). Errors name the file, never its contents.
func loadGitHubApp(appID, keyFile, api, checkName, detailsURL string) (*githubApp, error) {
	raw, err := os.ReadFile(keyFile)
	if err != nil {
		return nil, fmt.Errorf("reading the GitHub App key: %w", err)
	}
	block, _ := pem.Decode(raw)
	if block == nil {
		return nil, fmt.Errorf("the GitHub App key file %s holds no PEM block", keyFile)
	}
	var key *rsa.PrivateKey
	switch block.Type {
	case "RSA PRIVATE KEY":
		key, err = x509.ParsePKCS1PrivateKey(block.Bytes)
	case "PRIVATE KEY":
		var k any
		k, err = x509.ParsePKCS8PrivateKey(block.Bytes)
		if err == nil {
			var ok bool
			if key, ok = k.(*rsa.PrivateKey); !ok {
				err = errors.New("not an RSA key")
			}
		}
	default:
		err = fmt.Errorf("unsupported PEM type %q", block.Type)
	}
	if err != nil {
		return nil, fmt.Errorf("parsing the GitHub App key file %s: %w", keyFile, err)
	}
	return &githubApp{
		appID:      appID,
		key:        key,
		api:        strings.TrimRight(api, "/"),
		checkName:  checkName,
		detailsURL: detailsURL,
		client:     &http.Client{},
	}, nil
}

// jwt is the App's own credential, good only for exchanging for an
// installation token: iat a minute back for clock skew, exp nine minutes
// ahead (GitHub's ceiling is ten).
func (a *githubApp) jwt(now time.Time) (string, error) {
	enc := base64.RawURLEncoding
	header := enc.EncodeToString([]byte(`{"alg":"RS256","typ":"JWT"}`))
	claims, err := json.Marshal(struct {
		Iat int64  `json:"iat"`
		Exp int64  `json:"exp"`
		Iss string `json:"iss"`
	}{now.Add(-60 * time.Second).Unix(), now.Add(9 * time.Minute).Unix(), a.appID})
	if err != nil {
		return "", err
	}
	signed := header + "." + enc.EncodeToString(claims)
	sum := sha256.Sum256([]byte(signed))
	sig, err := rsa.SignPKCS1v15(rand.Reader, a.key, crypto.SHA256, sum[:])
	if err != nil {
		return "", fmt.Errorf("signing the App JWT: %w", err)
	}
	return signed + "." + enc.EncodeToString(sig), nil
}

// call sends one GitHub API request and decodes a JSON answer into out (when
// non-nil). Errors carry the method, path and status, never the bearer.
func (a *githubApp) call(ctx context.Context, bearer, method, path string, in, out any) error {
	ctx, cancel := context.WithTimeout(ctx, githubCallTimeout)
	defer cancel()
	var body io.Reader
	if in != nil {
		b, err := json.Marshal(in)
		if err != nil {
			return err
		}
		body = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, a.api+path, body)
	if err != nil {
		return fmt.Errorf("%s %s: %w", method, path, err)
	}
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("X-GitHub-Api-Version", "2022-11-28")
	req.Header.Set("Authorization", "Bearer "+bearer)
	if in != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := a.client.Do(req)
	if err != nil {
		// A *url.Error names the URL, which holds no credential.
		return fmt.Errorf("%s %s: %w", method, path, err)
	}
	defer func() { _ = resp.Body.Close() }()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode/100 != 2 {
		// GitHub's own message only; it says what is wrong without echoing
		// anything sent.
		var e struct {
			Message string `json:"message"`
		}
		_ = json.Unmarshal(raw, &e)
		return fmt.Errorf("%s %s: GitHub answered %d %s", method, path, resp.StatusCode, e.Message)
	}
	if out != nil {
		if err := json.Unmarshal(raw, out); err != nil {
			return fmt.Errorf("%s %s: decoding the answer: %w", method, path, err)
		}
	}
	return nil
}

func (a *githubApp) installationToken(ctx context.Context, installationID int64) (string, error) {
	jwt, err := a.jwt(time.Now())
	if err != nil {
		return "", err
	}
	var out struct {
		Token string `json:"token"`
	}
	path := fmt.Sprintf("/app/installations/%d/access_tokens", installationID)
	if err := a.call(ctx, jwt, http.MethodPost, path, nil, &out); err != nil {
		return "", err
	}
	if out.Token == "" {
		return "", fmt.Errorf("POST %s: the answer carried no token", path)
	}
	return out.Token, nil
}

// delivery is what a GitHub App delivery says about where to report.
type delivery struct {
	installation int64
	repo, sha    string
}

// githubDelivery reads a push (head is after) or pull_request (head is
// pull_request.head.sha) payload. ok is false for any other event or a payload
// sent by a plain webhook rather than an App installation.
func githubDelivery(event string, body []byte) (delivery, bool) {
	var p struct {
		After       string `json:"after"`
		PullRequest struct {
			Head struct {
				SHA string `json:"sha"`
			} `json:"head"`
		} `json:"pull_request"`
		Repository struct {
			FullName string `json:"full_name"`
		} `json:"repository"`
		Installation struct {
			ID int64 `json:"id"`
		} `json:"installation"`
	}
	if event != "push" && event != "pull_request" {
		return delivery{}, false
	}
	if err := json.Unmarshal(body, &p); err != nil {
		return delivery{}, false
	}
	d := delivery{installation: p.Installation.ID, repo: p.Repository.FullName, sha: p.After}
	if event == "pull_request" {
		d.sha = p.PullRequest.Head.SHA
	}
	if d.installation == 0 || d.sha == "" || !strings.Contains(d.repo, "/") {
		return delivery{}, false
	}
	return d, true
}

type checkOutput struct {
	Title   string `json:"title"`
	Summary string `json:"summary"`
}

func (a *githubApp) createCheck(ctx context.Context, token string, d delivery, group string) (int64, error) {
	var out struct {
		ID int64 `json:"id"`
	}
	err := a.call(ctx, token, http.MethodPost, "/repos/"+d.repo+"/check-runs", struct {
		Name    string      `json:"name"`
		HeadSHA string      `json:"head_sha"`
		Status  string      `json:"status"`
		Output  checkOutput `json:"output"`
	}{a.checkName, d.sha, "queued", checkOutput{"Queued", "Waiting for dispatcher group " + group}}, &out)
	if err != nil {
		return 0, err
	}
	if out.ID == 0 {
		return 0, errors.New("creating the check run: the answer carried no id")
	}
	return out.ID, nil
}

func (a *githubApp) completeCheck(ctx context.Context, token string, d delivery, id int64, conclusion, title, summary string) error {
	return a.call(ctx, token, http.MethodPatch, fmt.Sprintf("/repos/%s/check-runs/%d", d.repo, id), struct {
		Status      string      `json:"status"`
		Conclusion  string      `json:"conclusion"`
		CompletedAt string      `json:"completed_at"`
		Output      checkOutput `json:"output"`
	}{"completed", conclusion, time.Now().UTC().Format(time.RFC3339), checkOutput{title, summary}}, nil)
}

func (a *githubApp) checkStatus(ctx context.Context, token string, d delivery, id int64) (string, error) {
	var out struct {
		Status string `json:"status"`
	}
	err := a.call(ctx, token, http.MethodGet, fmt.Sprintf("/repos/%s/check-runs/%d", d.repo, id), nil, &out)
	return out.Status, err
}

// env is the seven SENRO_GITHUB_* pairs. An id of 0 (the check could not be
// created) leaves the check-run variable empty, so the pipeline's notifier
// creates its own.
func (a *githubApp) env(token string, d delivery, id int64) []string {
	run := ""
	if id != 0 {
		run = strconv.FormatInt(id, 10)
	}
	return []string{
		notify.EnvGitHubToken + "=" + token,
		notify.EnvGitHubAPI + "=" + a.api,
		notify.EnvGitHubRepo + "=" + d.repo,
		notify.EnvGitHubSHA + "=" + d.sha,
		notify.EnvGitHubCheckRun + "=" + run,
		notify.EnvGitHubCheckName + "=" + a.checkName,
		notify.EnvGitHubDetailsURL + "=" + a.detailsURL,
	}
}

// appCheck is one delivery's check: what serve created and run closes. A nil
// *appCheck is a delivery outside App mode, and every method is a no-op on it.
type appCheck struct {
	app   *githubApp
	token string
	d     delivery
	id    int64 // 0 when the check could not be created
}

// startCheck is spec steps 1 and 2. Failures are logged and give a nil (no
// token) or an id-less check (token, no check): a dead run is worse than a
// missing check.
func (d *dispatcher) startCheck(provider, event string, body []byte) *appCheck {
	if d.app == nil || provider != "github" {
		return nil
	}
	dl, ok := githubDelivery(event, body)
	if !ok {
		return nil
	}
	ctx := context.Background()
	token, err := d.app.installationToken(ctx, dl.installation)
	if err != nil {
		log.Printf("dispatcher: minting an installation token for %s failed, running without a check: %v", dl.repo, err)
		return nil
	}
	c := &appCheck{app: d.app, token: token, d: dl}
	if c.id, err = d.app.createCheck(ctx, token, dl, d.group); err != nil {
		log.Printf("dispatcher: creating the check run on %s failed, running without one: %v", dl.repo, err)
	}
	return c
}

// environ is what the pipeline adds to its environment.
func (c *appCheck) environ() []string {
	if c == nil {
		return nil
	}
	return c.app.env(c.token, c.d, c.id)
}

// notRun is spec step 3: the group could not be taken.
func (c *appCheck) notRun(group string, err error) {
	var held *persist.HeldError
	if errors.As(err, &held) {
		c.complete("neutral", "Not run", "another delivery holds group "+group)
		return
	}
	c.complete("failure", "Not run", "the dispatcher could not take its concurrency group "+group)
}

// finish is spec step 5, given what cmd.Run returned.
func (c *appCheck) finish(runErr error) {
	if c == nil || c.id == 0 {
		return
	}
	var ee *exec.ExitError
	switch {
	case errors.As(runErr, &ee) && ee.ExitCode() == 78:
		c.complete("neutral", "No trigger matched", "No trigger in the pipeline matched this delivery.")
		return
	case runErr != nil && !errors.As(runErr, &ee):
		c.complete("failure", "Pipeline did not start", runErr.Error())
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), githubCallTimeout)
	defer cancel()
	status, err := c.app.checkStatus(ctx, c.token, c.d, c.id)
	if err != nil {
		log.Printf("dispatcher: reading check run %d: %v", c.id, err)
	} else if status == "completed" {
		return
	}
	code := 0
	if ee != nil {
		code = ee.ExitCode()
	}
	title := fmt.Sprintf("Pipeline exited %d", code)
	if code == -1 {
		title = "Pipeline was terminated"
	}
	c.complete("failure", title, "The pipeline ended without completing this check.")
}

func (c *appCheck) complete(conclusion, title, summary string) {
	if c == nil || c.id == 0 {
		return
	}
	if err := c.app.completeCheck(context.Background(), c.token, c.d, c.id, conclusion, title, summary); err != nil {
		log.Printf("dispatcher: completing check run %d: %v", c.id, err)
	}
}
