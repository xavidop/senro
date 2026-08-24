package k8sexec

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	senroexec "github.com/xavidop/senro/internal/executor"
	"github.com/xavidop/senro/internal/executor/secretdir"
	"github.com/xavidop/senro/internal/kubeapi"
	"github.com/xavidop/senro/internal/persist"
	"github.com/xavidop/senro/internal/persist/kubelock"
	"github.com/xavidop/senro/internal/plan"
	"github.com/xavidop/senro/internal/workspace"
)

// SecretMountPath is where a step reads its delivered secrets inside the
// pod. Fixed, as containerexec.SecretMountPath is: a step reads the path out
// of its environment (SENRO_SECRET_<NAME>), and one path is one thing to
// audit.
const SecretMountPath = "/run/senro/secrets"

// StepContainer is the name of the container that runs the step's own
// command. Fixed so the log endpoint, the status lookup and the classifier
// all name the same thing beside senro's own containers (see transfer.go).
const StepContainer = "step"

// Executor runs steps as pods in one namespace of one cluster.
type Executor struct {
	spec  plan.ExecutorSpec
	runID string
	cli   *kubeapi.Client
	user  *kubeapi.PodSecurityContext
	// snap turns a workspace directory into a digest: the same snapshotter
	// every other executor uses.
	snap *workspace.Snapshotter

	// startTimeout bounds how long Run waits for a pod to start before
	// calling it an infrastructure failure. See Run.
	startTimeout time.Duration

	// resolveMu and resolved memoize a SUCCESSFUL resolve, once per executor,
	// which is once per distinct target per run (see plan.ExecutorSpec.Key).
	// A failure is deliberately not memoized; see resolve.
	resolveMu sync.Mutex
	resolved  bool
	platform  senroexec.Platform

	// staged maps a pod-side step binary path to the coordinator-side file it
	// is sent from. On the EXECUTOR, one target for the life of the run: see
	// senroexec.BinaryStager and staging.go.
	stageMu sync.Mutex
	staged  map[string]string
}

// Option configures New.
type Option func(*Executor)

// WithRunID labels every object this executor creates, so an orphan left by a
// killed coordinator can be found with
// `kubectl get pods -l senro.dev/run=<id>`.
func WithRunID(id string) Option { return func(e *Executor) { e.runID = id } }

// WithClient supplies an already-open connection, which is what a test that
// has already passed the kind guard holds. It is the ONLY way to reach a
// cluster other than through the explicit SENRO_K8S_* environment, and that
// is the point: there is no code path here that discovers a cluster.
func WithClient(c *kubeapi.Client) Option { return func(e *Executor) { e.cli = c } }

// WithStartTimeout bounds the wait for a pod to start.
func WithStartTimeout(d time.Duration) Option {
	return func(e *Executor) { e.startTimeout = d }
}

// DefaultStartTimeout is how long a pod may take to reach a running or
// terminated container before Run gives up and calls it infrastructure.
// Five minutes covers the slowest ordinary case (a cold node pulling a
// large image, an autoscaler bringing up a node); much longer, and a
// cluster with no capacity hangs until the step's own timeout and is
// misreported as a TIMEOUT of the command.
const DefaultStartTimeout = 5 * time.Minute

// New prepares an executor for one image in one namespace.
//
// It does NOT contact the cluster: that happens on the first Class,
// DeclaredPlatform or Sandbox call, through resolve. What it does do is
// refuse a specification it could never honour, before the run has written
// half a run directory.
func New(
	spec plan.ExecutorSpec, snap *workspace.Snapshotter, opts ...Option,
) (*Executor, error) {
	e := &Executor{spec: spec, snap: snap, startTimeout: DefaultStartTimeout}
	for _, o := range opts {
		o(e)
	}
	if err := CheckSpec(spec); err != nil {
		return nil, err
	}
	if e.cli == nil {
		cfg, err := kubeapi.FromEnv()
		if err != nil {
			return nil, err
		}
		c, err := kubeapi.New(cfg)
		if err != nil {
			return nil, err
		}
		e.cli = c
	}
	if spec.User != "" {
		sc, err := securityContext(spec.User)
		if err != nil {
			return nil, err
		}
		e.user = sc
	}
	return e, nil
}

// CheckSpec is the shape rule plan.Validate applies at plan time and New
// re-applies at construction. One function, so a plan assembled by hand
// cannot reach a pod create with a specification Validate would have refused.
func CheckSpec(spec plan.ExecutorSpec) error {
	if spec.Image == "" {
		return fmt.Errorf("k8sexec: no image reference")
	}
	if !strings.Contains(spec.Image, "@sha256:") {
		return fmt.Errorf(
			"k8sexec: image %q is not pinned to a digest. This executor cannot resolve a tag: it "+
				"talks to an apiserver, not to a registry, so it has no way to learn what "+
				"%[1]q means today and the cache key would be built from the tag. A tag that "+
				"moves would then reuse a cache entry computed from a different image. Pin it as "+
				"ghcr.io/acme/runner@sha256:<digest>",
			spec.Image)
	}
	if spec.Namespace == "" {
		return fmt.Errorf(
			"k8sexec: no namespace. senro will not guess one: %q is a real namespace in most "+
				"clusters and creating a pipeline's pods in it by default is how work lands "+
				"somewhere nobody looked", "default")
	}
	if spec.DelegateSecrets && spec.ServiceAccount == "" {
		return fmt.Errorf(
			"k8sexec: k8s.DelegateSecrets() needs k8s.ServiceAccount(name). Delegation means the " +
				"pod fetches its own secrets using the identity its ServiceAccount carries, and the " +
				"namespace's default account is one every other pod in that namespace also has: " +
				"delegating to it would grant this step whatever anybody else there was granted")
	}
	return nil
}

// securityContext parses the declared "uid" or "uid:gid". Numeric only: a
// Kubernetes securityContext takes a uid, and resolving a name would mean
// reading /etc/passwd inside the image, which this executor cannot do.
// Docker resolves names itself, so container.User("node") works and
// k8s.User("node") cannot; the refusal names that difference.
//
// A declared gid also becomes FSGroup, and that is what makes a secret
// readable by a step that is not root: kubelet owns every file in a volume
// it manages as root, so the projected 0400 credential would otherwise be
// readable by uid 0 alone and a step running as 1000 would get "Permission
// denied" on its own token. FSGroup is set from the gid rather than from
// the uid because a gid is what it is: guessing one from the uid would
// hand the volume to whatever group happened to share the number.
func securityContext(user string) (*kubeapi.PodSecurityContext, error) {
	uidStr, gidStr, hasGID := strings.Cut(user, ":")
	uid, err := strconv.ParseInt(strings.TrimSpace(uidStr), 10, 64)
	if err != nil {
		return nil, fmt.Errorf(
			"k8sexec: user %q is not numeric, and a Kubernetes securityContext takes a uid rather "+
				"than a name: senro would have to read the image's /etc/passwd to translate it, "+
				"and it never talks to a registry. Declare k8s.User(\"1000:1000\")", user)
	}
	sc := &kubeapi.PodSecurityContext{RunAsUser: &uid}
	if hasGID {
		gid, err := strconv.ParseInt(strings.TrimSpace(gidStr), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("k8sexec: group in user %q is not numeric", user)
		}
		sc.RunAsGroup = &gid
		sc.FSGroup = &gid
	}
	return sc, nil
}

// resolve contacts the cluster once: it proves the connection works and reads
// the platform every schedulable node reports.
func (e *Executor) resolve(ctx context.Context) error {
	// A mutex and a resolved flag, not sync.Once, because only SUCCESS may
	// be memoized: the answer must stay stable across a run, but caching a
	// failure would let one dropped packet permanently fail every step on
	// this cluster, with retry.OnInfra receiving the cached error without a
	// request being made. See
	// TestAClusterThatWasBrieflyUnreachableIsNotUnreachableForTheRun.
	e.resolveMu.Lock()
	defer e.resolveMu.Unlock()
	if e.resolved {
		return nil
	}
	err := func() error {
		// Ping first, and separately from the node list below, so an
		// unreachable or unauthenticated apiserver says exactly that rather
		// than arriving as a failure to list nodes.
		if _, err := e.cli.Ping(ctx); err != nil {
			return fmt.Errorf("k8sexec: %w: cannot reach the apiserver at %s: %w",
				senroexec.ErrInfra, e.cli.Server(), err)
		}
		p, err := e.clusterPlatform(ctx)
		if err != nil {
			return err
		}
		e.platform = p
		return nil
	}()
	if err != nil {
		return err
	}
	e.resolved = true
	return nil
}

// clusterPlatform is the os and architecture the cluster's schedulable
// nodes report: the DECLARED platform of every step run here.
//
// containerexec reads this from the image manifest; this executor speaks to
// an apiserver, not a registry, and for a multi-architecture image the node
// is the more accurate answer anyway. A cluster whose nodes disagree is
// refused (declare k8s.Platform(...)) rather than put a platform in the
// cache key that half the fleet contradicts. Control-plane nodes are
// excluded: they carry a NoSchedule taint this executor sets no toleration
// for, so nothing runs there.
func (e *Executor) clusterPlatform(ctx context.Context) (senroexec.Platform, error) {
	if e.spec.OS != "" && e.spec.Arch != "" {
		return senroexec.Platform{OS: e.spec.OS, Arch: e.spec.Arch}, nil
	}
	nodes, err := e.cli.Nodes(ctx)
	if err != nil {
		return senroexec.Platform{}, fmt.Errorf(
			"k8sexec: %w: listing the cluster's nodes: %w", senroexec.ErrInfra, err)
	}
	seen := make(map[senroexec.Platform]bool)
	for _, n := range nodes {
		if n.Spec.Unschedulable || hasNoScheduleTaint(n) {
			continue
		}
		info := n.Status.NodeInfo
		if info.OperatingSystem == "" || info.Architecture == "" {
			continue
		}
		seen[senroexec.Platform{OS: info.OperatingSystem, Arch: info.Architecture}] = true
	}
	switch len(seen) {
	case 0:
		return senroexec.Platform{}, fmt.Errorf(
			"k8sexec: %w: the cluster at %s reports no schedulable node, so there is nothing to "+
				"run a step on and no platform to declare",
			senroexec.ErrInfra, e.cli.Server())
	case 1:
		for p := range seen {
			return p, nil
		}
	}
	names := make([]string, 0, len(seen))
	for p := range seen {
		names = append(names, p.String())
	}
	sort.Strings(names)
	return senroexec.Platform{}, fmt.Errorf(
		"k8sexec: the cluster's schedulable nodes report more than one platform (%s), so there is "+
			"no single one to put in this step's cache key. Declare the one you mean with "+
			"k8s.Platform(\"linux\", \"amd64\")", strings.Join(names, ", "))
}

func hasNoScheduleTaint(n kubeapi.Node) bool {
	for _, t := range n.Spec.Taints {
		if t.Effect == "NoSchedule" || t.Effect == "NoExecute" {
			return true
		}
	}
	return false
}

// Class is the cache equivalence class: the platform and the image, which
// is a digest because CheckSpec refuses anything else. WHICH cluster is
// deliberately absent: a cluster address is target identity, and a class
// built from it means a fleet never shares an entry. The namespace is
// absent because it is not a property of the computation. A DECLARED user
// is in it, as containerexec has it: a step that runs as root is not the
// same step.
func (e *Executor) Class(ctx context.Context) (string, error) {
	if err := e.resolve(ctx); err != nil {
		return "", err
	}
	class := "k8s/" + e.platform.OS + "/" + e.platform.Arch + "/" + e.spec.Image
	if e.spec.User != "" {
		class += "/user=" + e.spec.User
	}
	return class, nil
}

// DeclaredPlatform is the cluster's platform; see clusterPlatform.
func (e *Executor) DeclaredPlatform(ctx context.Context) (senroexec.Platform, error) {
	if err := e.resolve(ctx); err != nil {
		return senroexec.Platform{}, err
	}
	return e.platform, nil
}

// EffectiveEnv is the step's declared environment, unchanged: unlike
// containerexec, this executor cannot read the image manifest to merge the
// image's environment underneath. Bounded rather than open-ended: the image
// env is a property of the image, whose digest is already in Class, so two
// runs against the same pinned image share it by construction.
func (e *Executor) EffectiveEnv(ctx context.Context, declared []string) ([]string, error) {
	if err := e.resolve(ctx); err != nil {
		return nil, err
	}
	return declared, nil
}

// Sandbox prepares one attempt. The pod itself is created in Run, because a
// pod's command is fixed at creation and Run is the first moment it is known:
// the same reason containerexec creates its container there.
func (e *Executor) Sandbox(ctx context.Context, spec senroexec.SandboxSpec) (senroexec.Sandbox, error) {
	if err := e.resolve(ctx); err != nil {
		return nil, err
	}
	if err := checkWorkDir(spec.StepID, spec.WorkDir); err != nil {
		return nil, err
	}
	if err := e.checkSecretsAreReachable(spec); err != nil {
		return nil, err
	}
	s := &sandbox{
		ex: e, spec: spec,
		pod:     podName(e.runID, spec.StepID, spec.Attempt),
		secrets: make(map[string][]byte, len(spec.Secrets)),
	}
	for _, m := range spec.Mounts {
		if err := checkMount(spec.StepID, m); err != nil {
			return nil, err
		}
		s.mounts = append(s.mounts, m)
	}
	return s, nil
}

// checkSecretsAreReachable refuses, before anything is created, the one
// combination where senro could deliver a credential the step cannot open:
// a declared user with no group, plus a secret.
//
// kubelet owns a projected Secret as root, and FSGroup is the only lever
// Kubernetes offers for handing it to another account (see
// securityContext). Without a gid there is no FSGroup to set and no way to
// find one out: the group a uid belongs to lives in the image's /etc/group,
// which this executor never reads. Delivering the secret anyway would fail
// inside the step as "Permission denied" on its own token, which reads like
// a wrong credential rather than a missing declaration.
func (e *Executor) checkSecretsAreReachable(spec senroexec.SandboxSpec) error {
	if len(spec.Secrets) == 0 || e.spec.DelegateSecrets || e.user == nil {
		return nil
	}
	if e.user.FSGroup != nil {
		return nil
	}
	return fmt.Errorf(
		"k8sexec: %w: step %q runs as k8s.User(%q) and needs %d secret(s), and a user declared "+
			"without a group cannot read them: Kubernetes owns a projected Secret as root and "+
			"hands it to another account only through the pod's fsGroup, which senro sets from "+
			"the group you declare. Declare k8s.User(\"%d:<gid>\")",
		senroexec.ErrInfra, spec.StepID, e.spec.User, len(spec.Secrets), *e.user.RunAsUser)
}

// checkMount refuses a mount this executor cannot realize. A workspace is
// carried in both directions (see transfer.go), and both directions need
// the coordinator-side directory, so a mount without one is refused here
// rather than failing later as a stat of the empty string. The engine
// always sets it; a plan assembled by hand might not.
func checkMount(stepID string, m senroexec.Mount) error {
	if !strings.HasPrefix(m.At, "/") {
		return fmt.Errorf(
			"k8sexec: %w: step %q mounts %q at %q, and a mount path inside a pod must be "+
				"absolute: declare it as senro.Workspace(...).At(\"/repo\", senro.RW)",
			senroexec.ErrInfra, stepID, m.Name, m.At)
	}
	if m.Path == "" {
		return fmt.Errorf(
			"k8sexec: %w: step %q mounts %q with no coordinator-side directory, and this executor "+
				"carries a workspace in both directions: there would be nothing to send and "+
				"nowhere to put what came back",
			senroexec.ErrInfra, stepID, m.Name)
	}
	return nil
}

// checkWorkDir refuses a working directory that is not absolute, for exactly
// the reason containerexec.checkWorkDir does: a relative path inside a pod
// has nothing to be relative to that this executor could name, and the
// apiserver's own refusal would name neither the step nor the senro call.
func checkWorkDir(stepID, dir string) error {
	if dir == "" || strings.HasPrefix(dir, "/") {
		return nil
	}
	return fmt.Errorf(
		"k8sexec: %w: step %q sets its working directory to %q, and a working directory inside a "+
			"pod must be absolute: declare it as WorkDir(\"/repo\")", senroexec.ErrInfra, stepID, dir)
}

type sandbox struct {
	ex     *Executor
	spec   senroexec.SandboxSpec
	mounts []senroexec.Mount
	pod    string

	mu sync.Mutex
	// secrets are buffered in memory between PutSecret and Run, because the
	// Secret object cannot be created before its contents are known and the
	// pod cannot be created before the Secret it mounts exists.
	secrets map[string][]byte
	// secretObj is the created Secret's name, or "" when the step declared
	// none. Close deletes it.
	secretObj string
	// created records that a pod exists to delete, and node records where it
	// was scheduled so ObservedPlatform has something to read.
	created bool
	node    string
}

// PodName is the object this sandbox creates, for a test that has to look at
// the pod itself. Nothing in production reads it.
func (s *sandbox) PodName() string { return s.pod }

// SecretName is the Secret object this sandbox created, or "" for a step
// with no secrets. Test-facing, alongside PodName.
func (s *sandbox) SecretName() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.secretObj
}

// ObservedPlatform reads the architecture of the node the pod was actually
// scheduled onto: the second, independent observation containerexec does
// not have. Before Run has scheduled anything there is no fact to report,
// so the declaration is returned.
func (s *sandbox) ObservedPlatform(ctx context.Context) (senroexec.Platform, error) {
	s.mu.Lock()
	node := s.node
	s.mu.Unlock()
	if node == "" {
		return s.ex.platform, nil
	}
	n, err := s.ex.cli.GetNode(ctx, node)
	if err != nil {
		return senroexec.Platform{}, fmt.Errorf(
			"k8sexec: %w: reading node %q: %w", senroexec.ErrInfra, node, err)
	}
	return senroexec.Platform{
		OS: n.Status.NodeInfo.OperatingSystem, Arch: n.Status.NodeInfo.Architecture,
	}, nil
}

// findMount returns a mount by name, with the position its volume is numbered
// by inside the pod.
func (s *sandbox) findMount(name string) (int, senroexec.Mount, bool) {
	for i, m := range s.mounts {
		if m.Name == name {
			return i, m, true
		}
	}
	return 0, senroexec.Mount{}, false
}

// ran reports whether a pod exists to read a workspace out of.
func (s *sandbox) ran() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.created
}

// PutSecret buffers the value and returns the path the STEP will read it
// from, inside the projected volume. Buffered because of ordering: the
// Secret object carries every value at once and must exist before the pod
// that mounts it; Run creates both.
func (s *sandbox) PutSecret(_ context.Context, name string, v []byte) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.created {
		return "", fmt.Errorf(
			"k8sexec: %w: step %q delivered secret %q after its pod was created; a projected "+
				"secret is fixed when the pod is", senroexec.ErrInfra, s.spec.StepID, name)
	}
	// Copied: the engine owns the slice it passed and the buffer outlives
	// this call by the whole of Sandbox setup.
	s.secrets[secretdir.FileName(name)] = append([]byte(nil), v...)
	return SecretMountPath + "/" + secretdir.FileName(name), nil
}

// logDrainGrace bounds how long Run waits for the log stream to end after
// the container has exited: the same trade as containerexec's constant of
// the same name.
var logDrainGrace = 5 * time.Second

// Run creates the pod, streams its log and waits for the container to exit:
// create the Secret if there is one, create the pod, adopt the Secret,
// stage the workspaces, await start, follow the log, await exit, drain,
// classify. exit is the workload's verdict and err is infrastructure;
// classify is where that line is drawn.
func (s *sandbox) Run(ctx context.Context, c senroexec.Cmd, stdout, stderr io.Writer) (int, error) {
	if len(c.Args) == 0 {
		return 0, fmt.Errorf("k8sexec: %w: empty command", senroexec.ErrInfra)
	}
	workDir := s.spec.WorkDir
	if c.Dir != "" {
		workDir = c.Dir
	}
	// Checked again here, not instead of in Sandbox: Cmd.Dir arrives
	// separately and is the only working directory a step whose WorkDir a
	// mount already realized ever sends. Refusing before create also means no
	// pod is made for a command that could never run.
	if err := checkWorkDir(s.spec.StepID, workDir); err != nil {
		return 0, err
	}

	if err := s.createSecret(ctx); err != nil {
		return 0, err
	}

	pod, err := s.ex.cli.CreatePod(ctx, s.ex.spec.Namespace, s.podSpec(c, workDir, false))
	if err != nil {
		return 0, fmt.Errorf("k8sexec: %w: creating pod %s/%s: %w",
			senroexec.ErrInfra, s.ex.spec.Namespace, s.pod, err)
	}
	s.mu.Lock()
	s.created = true
	s.mu.Unlock()

	s.adoptSecret(ctx, pod)

	// Everything from here runs on a context that survives cancellation, so
	// a killed pod is still reaped and its output still lands in the step's
	// log.
	bg := context.WithoutCancel(ctx)

	// The workspaces, before anything of the step runs: the volumes do not
	// exist until the pod is scheduled, and the step's container does not
	// start until the staging init container has exited. See transfer.go.
	if err := s.stageWorkspaces(ctx); err != nil {
		return 0, err
	}

	if err := s.awaitStart(ctx, bg); err != nil {
		return 0, err
	}

	// The log stream gets a cancellable context of its own so the drain
	// below can STOP it rather than merely stop waiting for it: abandoning
	// the stream would leave a goroutine writing into a writer its caller is
	// closing (see containerexec's Run).
	logCtx, stopLogs := context.WithCancel(bg)
	defer stopLogs()
	logsDone := make(chan struct{})
	go func() {
		defer close(logsDone)
		// Kubernetes merges stdout and stderr into one log, so one writer
		// here where containerexec has two; stderr carries only senro's own
		// diagnostics.
		if err := s.ex.cli.PodLogs(
			logCtx, s.ex.spec.Namespace, s.pod, StepContainer, true, stdout,
		); err != nil && logCtx.Err() == nil {
			_, _ = fmt.Fprintf(stderr, "senro: the pod's log stream ended early: %v\n", err)
		}
	}()

	exit, waitErr, cancelled := s.awaitExit(ctx, bg)

	select {
	case <-logsDone:
	case <-time.After(logDrainGrace):
		stopLogs()
		<-logsDone
	}

	switch {
	case cancelled != nil:
		return exit, fmt.Errorf("k8sexec: %w: %w", senroexec.ErrInfra, cancelled)
	case waitErr != nil:
		return exit, waitErr
	default:
		return exit, nil
	}
}

// createSecret writes the step's credentials into a namespaced Secret.
func (s *sandbox) createSecret(ctx context.Context) error {
	s.mu.Lock()
	values := s.secrets
	s.mu.Unlock()
	if len(values) == 0 {
		return nil
	}
	name := secretName(s.pod)
	_, err := s.ex.cli.CreateSecret(ctx, s.ex.spec.Namespace, kubeapi.Secret{
		Metadata: kubeapi.ObjectMeta{
			Name: name, Namespace: s.ex.spec.Namespace, Labels: s.labels(),
			Annotations: s.annotations(),
		},
		Type: "Opaque", Data: values, Immutable: boolPtr(true),
	})
	if err != nil {
		return fmt.Errorf("k8sexec: %w: creating secret %s/%s: %w",
			senroexec.ErrInfra, s.ex.spec.Namespace, name, err)
	}
	s.mu.Lock()
	s.secretObj = name
	// The values are dropped the moment they are no longer needed: they are
	// in the apiserver now, and a second copy need not sit in the
	// coordinator's heap for the length of the step.
	s.secrets = nil
	s.mu.Unlock()
	return nil
}

// adoptSecret points the Secret at the pod, so the apiserver's garbage
// collector removes it even if this coordinator never runs Close. Failure
// is ignored on purpose: Close deletes the Secret explicitly, adoption is
// the backstop, and failing the step because the backstop could not be
// installed would trade a working run for a leak that has not happened.
func (s *sandbox) adoptSecret(ctx context.Context, pod kubeapi.Pod) {
	s.mu.Lock()
	name := s.secretObj
	s.mu.Unlock()
	if name == "" || pod.Metadata.UID == "" {
		return
	}
	_ = s.ex.cli.SetSecretOwner(ctx, s.ex.spec.Namespace, name, kubeapi.OwnerReference{
		APIVersion: "v1", Kind: "Pod", Name: pod.Metadata.Name, UID: pod.Metadata.UID,
	})
}

// awaitStart polls until the step's container is running or has run, or
// until something says it never will. The bounded timeout turns "this
// cluster has no capacity" into an infrastructure failure retry can act on,
// and the message carries the scheduler's own account of why (see
// pendingReason): "0/12 nodes are available" is the answer, "Pending" is
// not.
func (s *sandbox) awaitStart(ctx, bg context.Context) error {
	deadline := time.Now().Add(s.ex.startTimeout)
	var last kubeapi.Pod
	for {
		pod, st, err := s.poll(ctx)
		if err != nil {
			// Cancellation usually lands during the in-flight read, not the
			// sleep, and a read that failed because the caller cancelled is
			// not a report about the cluster. Handled here as well as in the
			// select so a cancelled run does not leave its starting pod
			// behind (same hole, same fix, as awaitExit).
			if ctx.Err() != nil {
				_ = s.ex.cli.DeletePod(bg, s.ex.spec.Namespace, s.pod, 0)
				return fmt.Errorf("k8sexec: %w: %w", senroexec.ErrInfra, ctx.Err())
			}
			return err
		}
		last = pod
		if pod.Spec.NodeName != "" {
			s.mu.Lock()
			s.node = pod.Spec.NodeName
			s.mu.Unlock()
		}
		if st.Started {
			return nil
		}
		if time.Now().After(deadline) {
			_ = s.ex.cli.DeletePod(bg, s.ex.spec.Namespace, s.pod, 0)
			return fmt.Errorf(
				"k8sexec: %w: pod %s/%s did not start within %s: %s",
				senroexec.ErrInfra, s.ex.spec.Namespace, s.pod, s.ex.startTimeout,
				pendingReason(last))
		}
		select {
		case <-ctx.Done():
			_ = s.ex.cli.DeletePod(bg, s.ex.spec.Namespace, s.pod, 0)
			return fmt.Errorf("k8sexec: %w: %w", senroexec.ErrInfra, ctx.Err())
		case <-time.After(kubeapi.PollInterval):
		}
	}
}

// awaitContainer polls until one named container of the pod is RUNNING, the
// state the exec subresource needs. Used for the staging container, whose
// whole life happens while the pod is still Pending, and for the STEP's own
// container when RunInteractive will exec into it; it shares classify, so an
// image that cannot be pulled stops this wait early. A container that
// already TERMINATED is a failure, not a state to keep waiting on: for
// either of those two it means the shell holding it exited before senro used
// it, which is what an image with no `sh` looks like.
func (s *sandbox) awaitContainer(ctx context.Context, name string) error {
	deadline := time.Now().Add(s.ex.startTimeout)
	var last kubeapi.Pod
	for {
		pod, err := s.readPod(ctx)
		if err != nil {
			return err
		}
		last = pod
		if pod.Spec.NodeName != "" {
			s.mu.Lock()
			s.node = pod.Spec.NodeName
			s.mu.Unlock()
		}
		st, err := classify(pod, name)
		if err != nil {
			return err
		}
		switch {
		case st.Exited:
			return fmt.Errorf(
				"k8sexec: %w: container %q of pod %s/%s exited (%d) before senro could use it; "+
					"this executor needs sh and tar in the step's image to carry a workspace and "+
					"to run a func step",
				senroexec.ErrInfra, name, s.ex.spec.Namespace, s.pod, st.ExitCode)
		case st.Started:
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf(
				"k8sexec: %w: container %q of pod %s/%s did not start within %s: %s",
				senroexec.ErrInfra, name, s.ex.spec.Namespace, s.pod, s.ex.startTimeout,
				pendingReason(last))
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("k8sexec: %w: %w", senroexec.ErrInfra, ctx.Err())
		case <-time.After(kubeapi.PollInterval):
		}
	}
}

// awaitExit polls until the container terminates, and deletes the pod if
// the run is cancelled first: the pod is deleted with a zero grace period
// and the wait continues on bg, so a killed step's exit code is still
// observable and a cancelled run leaves no pod running.
func (s *sandbox) awaitExit(ctx, bg context.Context) (exit int, waitErr, cancelled error) {
	poll := ctx
	var reapBy time.Time
	for {
		// Cancellation is checked BEFORE the read. The other order looks
		// equivalent and is not: once ctx is done, GetPod fails with the
		// context's own error, so the loop would leave through the
		// request-failed branch with cancelled still nil and never reach the
		// delete, leaving a pod running. See
		// TestCancellationIsInfrastructureAndTakesThePodWithIt.
		if cancelled == nil && ctx.Err() != nil {
			cancelled = ctx.Err()
			if err := s.ex.cli.DeletePod(bg, s.ex.spec.Namespace, s.pod, 0); err != nil {
				// Reported rather than swallowed: a cancelled run that could
				// not delete its pod has left a workload running in somebody
				// else's cluster, which nobody would otherwise find out
				// about.
				return 0, fmt.Errorf(
					"k8sexec: %w: the run was cancelled and pod %s/%s could not be deleted, so it "+
						"may still be running: %w",
					senroexec.ErrInfra, s.ex.spec.Namespace, s.pod, err), cancelled
			}
			// Keep waiting on a context that outlives the cancellation, so
			// the terminal state is still read, but bounded by reapBy: bg
			// never expires, and a stalled apiserver would otherwise hold
			// Run open forever on the one path already asked to stop.
			poll = bg
			reapBy = time.Now().Add(reapGrace)
		}
		_, st, err := s.poll(poll)
		if err != nil {
			// A read that failed BECAUSE the caller cancelled is not a
			// report about the cluster: cancellation usually lands during
			// the in-flight request, so this branch is the one that decides.
			// Going round once more lets the top of the loop delete the pod
			// and continue the wait on bg.
			if cancelled == nil && ctx.Err() != nil {
				continue
			}
			return 0, err, cancelled
		}
		if st.Exited {
			return st.ExitCode, nil, cancelled
		}
		if !reapBy.IsZero() && time.Now().After(reapBy) {
			return 0, fmt.Errorf(
					"k8sexec: %w: pod %s/%s had not terminated %s after the run was cancelled and it "+
						"was deleted", senroexec.ErrInfra, s.ex.spec.Namespace, s.pod, reapGrace),
				cancelled
		}
		// A cancellation during the sleep wakes this immediately and goes
		// round to the TOP of the loop, the only place that deletes the pod;
		// returning here instead would be a third way to leave without
		// deleting anything. bg is never done, so once cancellation has been
		// handled this case stops firing and reapBy bounds the wait.
		select {
		case <-ctx.Done():
		case <-time.After(kubeapi.PollInterval):
		}
	}
}

// reapGrace bounds how long Run keeps watching a pod it has already deleted
// after cancellation. Long enough for a zero-grace delete to take effect and
// for the terminal state to be read once, short enough that a cancelled run
// still ends. podReadTimeout bounds one status read.
var (
	reapGrace      = 30 * time.Second
	podReadTimeout = 30 * time.Second
)

// poll reads the pod once and classifies it. A pod that has vanished is an
// infrastructure failure and not an exit code: something outside this run
// deleted it, and the step's verdict went with it.
func (s *sandbox) poll(ctx context.Context) (kubeapi.Pod, podState, error) {
	pod, err := s.readPod(ctx)
	if err != nil {
		return pod, podState{}, err
	}
	st, err := classify(pod, StepContainer)
	return pod, st, err
}

// readPod reads the pod once, with a deadline of its own: after
// cancellation the waiting loop deliberately polls on a context that never
// expires, so without this a stalled apiserver would hold Run open
// indefinitely.
func (s *sandbox) readPod(ctx context.Context) (kubeapi.Pod, error) {
	ctx, cancel := context.WithTimeout(ctx, podReadTimeout)
	defer cancel()
	pod, err := s.ex.cli.GetPod(ctx, s.ex.spec.Namespace, s.pod)
	if err != nil {
		if kubeapi.IsNotFound(err) {
			return pod, fmt.Errorf(
				"k8sexec: %w: pod %s/%s no longer exists; something outside this run deleted it",
				senroexec.ErrInfra, s.ex.spec.Namespace, s.pod)
		}
		return pod, fmt.Errorf("k8sexec: %w: reading pod %s/%s: %w",
			senroexec.ErrInfra, s.ex.spec.Namespace, s.pod, err)
	}
	return pod, nil
}

// podSpec builds the object: the step's container, one emptyDir per
// workspace, one projected Secret when there are credentials, and, only for
// a step that carries a workspace, the two transfer containers (see
// transfer.go). bin adds the emptyDir a staged step binary lands in, for the
// one command that is one (see staging.go).
//
// Cmd.Env becomes plain container env, readable by anybody with read access
// to the namespace. That is why a step's secrets are NOT here: they arrive
// as files from the projected Secret volume, and only their PATHS appear in
// this list. The engine's trace context (TRACEPARENT, TRACESTATE) lands
// here in the open deliberately: a traceparent names no principal and
// grants no access (see TestTheTraceparentIsAnOrdinaryFieldOfThePod).
func (s *sandbox) podSpec(c senroexec.Cmd, workDir string, bin bool) kubeapi.Pod {
	env := make([]kubeapi.EnvVar, 0, len(c.Env))
	for _, kv := range c.Env {
		name, value, _ := strings.Cut(kv, "=")
		env = append(env, kubeapi.EnvVar{Name: name, Value: value})
	}

	volumes := make([]kubeapi.Volume, 0, len(s.mounts)+1)
	mounts := make([]kubeapi.VolumeMount, 0, len(s.mounts)+1)
	// The same volumes again, at a private root, for the two transfer
	// containers: a step may mount a workspace READ-ONLY and the staging
	// container has to write it, and the declared path belongs to the
	// pipeline.
	stageMounts := make([]kubeapi.VolumeMount, 0, len(s.mounts))
	readMounts := make([]kubeapi.VolumeMount, 0, len(s.mounts))
	for i, m := range s.mounts {
		// The volume name is positional: a workspace name is an arbitrary
		// string, a volume name is a DNS label, and nothing outside this
		// pod spec refers to it.
		name := "ws-" + strconv.Itoa(i)

		if m.Claim != "" {
			// A claim-backed workspace already IS in the cluster: the pod
			// mounts it, and it gets no stage and no read entry, which would
			// tar the tree onto itself and back out, spending exactly the
			// traffic the claim avoids and racing another run's pod while
			// doing it. Snapshot skips it too; see sandbox.Snapshot.
			volumes = append(volumes, kubeapi.Volume{
				Name:     name,
				ClaimRef: &kubeapi.PersistentVolumeClaimRef{ClaimName: m.Claim, ReadOnly: m.RO},
			})
			mounts = append(mounts, kubeapi.VolumeMount{Name: name, MountPath: m.At, ReadOnly: m.RO})
			continue
		}

		volumes = append(volumes, kubeapi.Volume{Name: name, EmptyDir: &kubeapi.EmptyDirVolume{}})
		mounts = append(mounts, kubeapi.VolumeMount{Name: name, MountPath: m.At, ReadOnly: m.RO})
		stageMounts = append(stageMounts, kubeapi.VolumeMount{
			Name: name, MountPath: stagePath(i),
		})
		// Read-only: a container that only ever hands a workspace back
		// cannot then be the thing that changed it.
		readMounts = append(readMounts, kubeapi.VolumeMount{
			Name: name, MountPath: stagePath(i), ReadOnly: true,
		})
	}
	if name := s.SecretName(); name != "" && !s.ex.spec.DelegateSecrets {
		volumes = append(volumes, kubeapi.Volume{
			Name: "senro-secrets",
			Secret: &kubeapi.SecretVolumeSource{
				SecretName: name,
				// 0400, which is what the file keeps when the pod runs as
				// root: readable by the one account in the container and by
				// nobody else, the closest equivalent to the 0600 inside
				// 0700 that secretdir gives a local or container step.
				//
				// With a declared user the pod carries an fsGroup (see
				// securityContext) and kubelet widens this to 0440 owned by
				// that group, which is the same promise one rung out: the
				// step's own account reads it, and no other account on the
				// node does.
				DefaultMode: int32Ptr(0o400),
			},
		})
		mounts = append(mounts, kubeapi.VolumeMount{
			Name: "senro-secrets", MountPath: SecretMountPath, ReadOnly: true,
		})
	}
	if bin {
		// Only the STEP's container gets it: senro's transfer containers have
		// no business holding senro's own executable, and the volume exists
		// only for the one command that IS it.
		volumes = append(volumes, kubeapi.Volume{
			Name: binVolume, EmptyDir: &kubeapi.EmptyDirVolume{},
		})
		mounts = append(mounts, kubeapi.VolumeMount{Name: binVolume, MountPath: BinDir})
	}

	var resources *kubeapi.ResourceRequirements
	if r := s.ex.spec.Resources; r != nil {
		resources = &kubeapi.ResourceRequirements{Requests: r.Requests, Limits: r.Limits}
	}
	containers := []kubeapi.Container{{
		Name: StepContainer, Image: s.ex.spec.Image,
		Command: c.Args, Env: env, WorkingDir: workDir,
		VolumeMounts: mounts,
		// The image is pinned to a digest (CheckSpec), so IfNotPresent is
		// both faster and exactly as correct as Always.
		ImagePullPolicy: "IfNotPresent",
		// Only the step's own container: senro's stage and reader containers
		// are its own plumbing, not part of what the pipeline asked to run,
		// and a limit sized for the step's workload would starve them.
		Resources: resources,
	}}
	var initContainers []kubeapi.Container
	// Only when there is a workspace to CARRY, not merely to mount: a pod
	// whose every mount is claim-backed needs no transfer, so this gates on
	// the stage list rather than on s.mounts.
	if len(stageMounts) > 0 {
		initContainers = append(initContainers, kubeapi.Container{
			Name: StageContainer, Image: s.ex.spec.Image,
			Command: stageCommand(), VolumeMounts: stageMounts,
			ImagePullPolicy: "IfNotPresent",
		})
		containers = append(containers, kubeapi.Container{
			Name: IOContainer, Image: s.ex.spec.Image,
			Command: ioCommand(), VolumeMounts: readMounts,
			ImagePullPolicy: "IfNotPresent",
		})
	}

	var imagePullSecrets []kubeapi.LocalObjectRefName
	for _, name := range s.ex.spec.ImagePullSecrets {
		imagePullSecrets = append(imagePullSecrets, kubeapi.LocalObjectRefName{Name: name})
	}
	var tolerations []kubeapi.Toleration
	for _, t := range s.ex.spec.Tolerations {
		tolerations = append(tolerations, kubeapi.Toleration{
			Key: t.Key, Operator: t.Operator, Value: t.Value, Effect: t.Effect,
		})
	}

	return kubeapi.Pod{
		Metadata: kubeapi.ObjectMeta{
			Name: s.pod, Namespace: s.ex.spec.Namespace,
			Labels: s.labels(), Annotations: s.annotations(),
		},
		Spec: kubeapi.PodSpec{
			// Never, so the kubelet does not restart a step's command behind
			// the engine's back (retry is the engine's, with its own attempt
			// accounting). Never also keeps the reader container alive after
			// the step exits: the pod stays Running while any container
			// does, which is the window Snapshot reads the workspace out in.
			RestartPolicy: "Never",
			// A step's command has no business holding a credential to the
			// cluster it runs in, unless delegation was asked for: that
			// token IS what AWS exchanges for an IRSA role. On only then;
			// DelegateSecrets' own doc states the cost.
			AutomountServiceAccountToken: boolPtr(s.ex.spec.DelegateSecrets),
			ServiceAccountName:           s.ex.spec.ServiceAccount,
			SecurityContext:              s.ex.user,
			Volumes:                      volumes,
			InitContainers:               initContainers,
			Containers:                   containers,
			NodeSelector:                 s.ex.spec.NodeSelector,
			Tolerations:                  tolerations,
			ImagePullSecrets:             imagePullSecrets,
		},
	}
}

// labels are what a leaked object is found by. Values are sanitized because a
// label value is a narrow character class and a senro step id is not; the
// unsanitized truth is in the annotations.
func (s *sandbox) labels() map[string]string {
	l := map[string]string{
		"senro.dev/managed": "true",
		"senro.dev/attempt": strconv.Itoa(s.spec.Attempt),
	}
	if v := labelValue(s.ex.runID); v != "" {
		l["senro.dev/run"] = v
	}
	if v := labelValue(s.spec.StepID); v != "" {
		l["senro.dev/step"] = v
	}
	return l
}

func (s *sandbox) annotations() map[string]string {
	return map[string]string{"senro.dev/step-id": s.spec.StepID}
}

// Close deletes the pod and the Secret, on EVERY path including keep.
//
// Not symmetric with localexec's Close, for containerexec's reason: by the
// time Close runs, Snapshot has already captured the workspace out of this
// very pod. A kept pod would buy nothing while holding a node's disk and
// memory, and deleting the pod is what triggers garbage collection of the
// Secret it owns. keep is accepted for interface symmetry and changes
// nothing.
func (s *sandbox) Close(ctx context.Context, _ bool) error {
	s.mu.Lock()
	created, secret := s.created, s.secretObj
	s.created, s.secretObj, s.secrets = false, "", nil
	s.mu.Unlock()

	var podErr, secretErr error
	if created {
		if err := s.ex.cli.DeletePod(ctx, s.ex.spec.Namespace, s.pod, 0); err != nil {
			podErr = fmt.Errorf("deleting pod %s/%s: %w", s.ex.spec.Namespace, s.pod, err)
		}
	}
	// Deleted explicitly rather than left to the ownerReference: garbage
	// collection is asynchronous, and a credential should not sit in etcd
	// for as long as a controller takes to notice. Both errors are reported.
	if secret != "" {
		if err := s.ex.cli.DeleteSecret(ctx, s.ex.spec.Namespace, secret); err != nil {
			secretErr = fmt.Errorf("deleting secret %s/%s: %w", s.ex.spec.Namespace, secret, err)
		}
	}
	if err := errors.Join(podErr, secretErr); err != nil {
		return fmt.Errorf("k8sexec: %w: %w", senroexec.ErrInfra, err)
	}
	return nil
}

func boolPtr(b bool) *bool    { return &b }
func int32Ptr(i int32) *int32 { return &i }

var _ senroexec.Executor = (*Executor)(nil)
var _ senroexec.Sandbox = (*sandbox)(nil)

// A pod's mounts live on a machine of its own, so plan.Node.RemoteMounts is
// true for every step here and the engine will ask this sandbox for a scratch
// cache back. Asserted rather than left to a runtime type check: a sandbox
// that stopped implementing it would silently stop saving one.
var _ senroexec.MountReader = (*sandbox)(nil)

// WorkspaceLocker returns a locker only for a workspace this target backs
// with a claim, nil otherwise, leaving ordinary persistent workspaces on
// the coordinator's own file lock; the engine uses the first non-nil answer
// (engine.lockerFor).
//
// A file lock is a complete exclusion only while the coordinator owns the
// tree; a claim is owned by the cluster and two coordinators can mount it,
// so the exclusion must live where the tree does. internal/persist/kubelock
// is that, including what an unfenced lease does not give.
func (e *Executor) WorkspaceLocker(workspace string) persist.Locker {
	if e.spec.Claims[workspace] == "" {
		return nil
	}
	return kubelock.New(e.cli, e.spec.Namespace)
}

// DelegatesSecrets reports that this sandbox's pod fetches its own secrets,
// so the engine must not resolve or push them. See k8s.DelegateSecrets:
// only the source URI crosses, and senro never holds the value so it cannot
// redact it out of the step's log.
func (s *sandbox) DelegatesSecrets() bool { return s.ex.spec.DelegateSecrets }
