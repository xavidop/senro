package oci

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
)

// dockerHubRegistry is where a reference naming no registry lives.
const dockerHubRegistry = "registry-1.docker.io"

// resolveAccept lists every manifest type an image can be published as, so a
// registry answers for a multi-platform index as readily as for one manifest.
const resolveAccept = "application/vnd.oci.image.index.v1+json, " +
	"application/vnd.oci.image.manifest.v1+json, " +
	"application/vnd.docker.distribution.manifest.list.v2+json, " +
	"application/vnd.docker.distribution.manifest.v2+json"

// ResolveTag returns the digest a tag in ref points at right now.
//
// ref is an ordinary image reference such as "golang:1.26" or
// "ghcr.io/acme/tool:v1"; cfg.Registry and cfg.Repository are ignored and
// derived from it. Credentials, PlainHTTP, Timeout and Transport apply as
// they do for a Client, including the bearer challenge.
func ResolveTag(ctx context.Context, ref string, cfg Config) (string, error) {
	registry, repo, tag := splitReference(ref)
	cfg.Registry, cfg.Repository = registry, repo
	c, err := New(cfg)
	if err != nil {
		return "", err
	}
	if err := validReference(tag); err != nil {
		return "", err
	}
	resp, err := c.send(ctx, request{
		method: http.MethodHead,
		url:    c.url("manifests/" + tag),
		scope:  c.scope(false),
		what:   "manifest " + ref,
		accept: resolveAccept,
	})
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return "", &notFoundError{ref: ref}
		}
		return "", err
	}
	defer func() { _ = resp.Body.Close() }()
	_, _ = io.Copy(io.Discard, resp.Body)

	digest := resp.Header.Get("Docker-Content-Digest")
	if digest == "" {
		return "", fmt.Errorf("oci: %s: the registry reported no Docker-Content-Digest", ref)
	}
	if err := validDigest(digest); err != nil {
		return "", fmt.Errorf("oci: %s: %w", ref, err)
	}
	return digest, nil
}

// notFoundError is the "tag does not exist" answer, worded for a person
// reading a pipeline's error and still matching ErrNotFound.
type notFoundError struct{ ref string }

func (e *notFoundError) Error() string { return "oci: " + e.ref + ": not found" }
func (e *notFoundError) Unwrap() error { return ErrNotFound }

// splitReference normalises an image reference into registry, repository and
// tag, applying Docker Hub's defaults: no registry means Docker Hub, a
// single-segment Hub name lives under "library/", and no tag means "latest".
func splitReference(ref string) (registry, repo, tag string) {
	name := ref
	tag = "latest"
	slash := strings.LastIndex(name, "/")
	if colon := strings.LastIndex(name, ":"); colon > slash {
		name, tag = name[:colon], name[colon+1:]
	}
	first, rest, hasSlash := strings.Cut(name, "/")
	if hasSlash && (strings.ContainsAny(first, ".:") || first == "localhost") {
		registry, repo = first, rest
	} else {
		registry, repo = dockerHubRegistry, name
	}
	if registry == "docker.io" || registry == "index.docker.io" {
		registry = dockerHubRegistry
	}
	if registry == dockerHubRegistry && !strings.Contains(repo, "/") {
		repo = "library/" + repo
	}
	return registry, repo, tag
}
