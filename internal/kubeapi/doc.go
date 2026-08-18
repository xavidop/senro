// Package kubeapi speaks the Kubernetes API over plain HTTPS and JSON.
//
// It exists so senro's Kubernetes executor needs no module dependency:
// k8s.io/client-go brings a dependency tree larger than senro itself, for a
// client that creates a pod, reads its status, streams its log and deletes
// it again. The Kubernetes API is REST with JSON bodies, and net/http plus
// encoding/json reach all of it: the same trade internal/dockerd made
// against github.com/docker/docker.
//
// # No ambient configuration, ever
//
// Nothing in this package reads $KUBECONFIG, ~/.kube/config, or a
// current-context. Config is passed in whole, and FromEnv reads only
// SENRO_K8S_* variables an operator set on purpose. A developer machine's
// kubeconfig holds dozens of contexts, most of them production, so an
// executor that defaulted to the current one would deploy into whatever
// cluster was last selected for something else. There is no default: a
// pipeline targeting Kubernetes with nothing configured fails at run start,
// by name, with the variables it needs listed.
//
// # Not a general client
//
// The surface is what one executor needs: server version, node list and
// read, pod create, read, log, exec and delete, and secret create, own and
// delete. No watch, informer, apply, CRD support, discovery, or attach. Pod
// status is POLLED rather than watched, which costs bounded latency (see
// PollInterval) and removes resourceVersion bookkeeping, bookmark handling
// and watch re-establishment from a client whose longest wait is a single
// pod's lifetime.
//
// # The one endpoint that is not request/response
//
// Exec is a bidirectional, multiplexed stream the apiserver offers over
// exactly two upgrades: SPDY/3.1, which genuinely is not in the standard
// library, and WebSocket, whose handshake is plain HTTP/1.1: net/http
// performs the upgrade and hands back the raw connection (a 101 response's
// Body also implements io.Writer). What remains is a frame header of at
// most fourteen bytes and an xor mask, which is websocket.go. See exec.go
// for the endpoint and k8sexec's transfer.go for what it is for.
package kubeapi
