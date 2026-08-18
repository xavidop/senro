// Package k8sexec runs steps as pods in a Kubernetes cluster.
//
// One step, one pod, one container. The step's command is the container's
// command, its output comes back through the pod log endpoint, and its exit
// code comes from the container's terminated status. Everything else follows
// from two facts: the coordinator does not share a filesystem with the node,
// and this package speaks nothing but request/response HTTP (see
// internal/kubeapi).
//
// # No ambient cluster, ever
//
// Nothing in this package or internal/kubeapi discovers a cluster:
// $KUBECONFIG and ~/.kube/config are never read and no current-context is
// consulted. A connection comes from SENRO_K8S_* variables an operator set on
// purpose, or from WithClient. A development kubeconfig commonly holds dozens
// of contexts, most of them customers' production clusters, and an executor
// that defaulted to the current one would run pods in the wrong company's
// cloud. A pipeline targeting Kubernetes with nothing configured fails at run
// start, by name, listing the variables it needs.
//
// # Workspaces are emptyDir, carried in both directions
//
// Each declared mount becomes an emptyDir volume at its declared path,
// read-only when senro.RO says so, deleted with the pod. It is FILLED from
// the coordinator's copy before the step's container starts and READ BACK OUT
// after it exits, as a tar over the apiserver's exec subresource in each
// direction. transfer.go is the mechanism; internal/executor/mountxfer is
// shared with sshexec so the two agree exactly about what crosses and what a
// digest is taken from.
//
// Two properties follow:
//
//   - A pod that mounts a workspace has two extra containers: content can
//     only land before the step's process by arriving in an INIT container,
//     and a container can only outlive the step's exit as an ordinary
//     container of a restartPolicy Never pod. No single container can be
//     both, and neither exists for a step that mounts nothing.
//   - The digest is computed from what came BACK, through the same
//     snapshotter every other executor uses. A digest of the coordinator's
//     own copy would describe bytes the step never touched, and it reaches
//     the ledger, ws.snapshot and the next step's cache key.
//
// The cost: every byte crosses the apiserver twice per attempt, and the
// apiserver is shared by the whole cluster (sshexec's tar goes point to
// point). A step whose inputs are genuinely large should fetch them in the
// pod and declare NoSnapshot. The step's image must carry sh and tar, exactly
// as sshexec requires tar on a remote host.
//
// # A scratch cache crosses the same way, and is read back before it is saved
//
// A scratch cache is an ordinary mount here: an emptyDir filled before the
// step and pulled back afterwards by ReadMount, which is Snapshot's own
// copyOut without the digest and without replacing the coordinator's copy.
// The engine keeps what came back aside and saves THAT (see
// engine.wsManager.readScratch), so the bytes stored under the key are the
// bytes the pod actually left. A read-back that fails stores nothing at all:
// an entry is written once under its key and never rewritten, so a stale
// copy there is the answer every later run gets.
//
// Two things differ from a workspace, both because a scratch cache is not
// evidence: nothing is excluded from it (internal/scratch saves the
// directory whole and node_modules is usually the POINT of one; see
// mountsnap.Excluder), and nothing is snapshotted, so it reaches no
// ws.snapshot, no ledger entry and no cache key.
//
// The cost is the one this package's transfer doc already names, paid again:
// every byte of the cache crosses the SHARED apiserver twice per attempt, on
// top of the workspace. A dependency tree large enough to be worth caching
// is often large enough that carrying it through the apiserver costs more
// than the download it saves, and the docs say so plainly rather than
// implying this is free.
//
// One shape stays refused at plan time: one scratch cache mounted both here
// and by a step on the coordinator's own filesystem. Such a step writes that
// directory while this executor is tarring it, and a half-written tree would
// be saved under an immutable key. See plan.validateScratchTargets.
//
// # A persistent workspace, with or without a claim
//
// senro.ScopePersistent works with no claim at all, and that is the default:
// a persistent workspace is a directory ON THE COORDINATOR that outlives a
// run (internal/persist), and this executor already carries a mount in both
// directions. What that costs is transfer, twice per attempt, on exactly the
// large tree where it hurts most. executor/k8s.Claim removes it by naming a
// PersistentVolumeClaim that already holds the workspace: the pod mounts the
// claim, no staging container and no reader are created for it, and nothing
// is carried at all.
//
// The trade, in three parts:
//
//   - Exclusion is one implementation with two backends:
//     internal/persist.Locker, with the file lock and
//     internal/persist/kubelock's coordination.k8s.io Lease as the
//     implementations. A Lease is unfenced, so a holder partitioned from the
//     apiserver can still write; kubelock's doc says so.
//   - The coordinator cannot measure a claim-backed tree, so it does not key
//     on it: plan.Validate refuses Pure() on a step that mounts one, and the
//     engine emits no ws.snapshot for it, because no digest would be true.
//   - senro owns no cluster object: it creates, binds, resizes and deletes no
//     PersistentVolumeClaim. The operator points senro at a claim they made.
//
// What is genuinely given up is the action cache on those steps, and
// scheduling freedom: a ReadWriteOnce claim ties every pod mounting it to one
// node, so a fan-out over that workspace serialises. The better answer to
// ordinary transfer cost remains the one transfer.go names: an init container
// pulling from the content store (internal/oci), which moves only what
// changed and needs no cluster object.
//
// # Secrets
//
// A value is delivered as a file under /run/senro/secrets, exactly as the
// local and container executors deliver it: a namespaced Secret projected as
// a volume with defaultMode 0400, mounted read-only, path told to the step
// through its environment.
//
// A Secret is weaker than the container executor's tmpfs directory: the value
// transits the apiserver, lands in etcd, and is readable by anyone holding
// `get secrets` in the namespace. What the choice buys:
//
//   - Never in the POD: not env, not envFrom, not an argument, not an
//     annotation. A pod spec is readable by anyone with `get pods`, a far
//     wider group, and envFrom survives in every pod spec dump forever. See
//     TestASecretIsAFileAndNeverAFieldOfThePod.
//   - Never in an image layer, never a build arg.
//   - Short-lived: created for one attempt, immutable, deleted by Close on
//     every path including keep, and owner-referenced to its pod so the
//     apiserver's garbage collector removes it if the coordinator dies before
//     Close runs.
//   - 0400 inside a read-only volume, the closest reachable equivalent of
//     secretdir's 0600-inside-0700.
//
// The genuinely better answer is not to push the value at all: see
// executor/k8s.DelegateSecrets, where only the source URI crosses the
// boundary and the pod resolves the credential in-cluster.
//
// # Func steps
//
// A function's body is compiled into the coordinator's binary, so a func step
// here means putting that binary in the pod and re-entering it as a step
// child (internal/stepchild). It travels as a one-entry tar over the exec
// subresource a workspace already crosses on, into an emptyDir at BinDir, and
// then runs as an EXEC into the step's container rather than as the
// container's command: an exec is the only channel with a stdin to hand the
// child its state on, streams kept apart to carry its frames, and an exit
// code of its own. That is why the container is started holding
// (transfer.go's holdCommand). staging.go is the mechanism.
//
// The image must carry sh and tar, exactly as carrying a workspace requires,
// and must be able to execute a static linux binary for the node's
// architecture: internal/binprov cross-compiles with CGO_ENABLED=0, -tags
// netgo,osusergo and a static link, so no dynamic loader or libc is needed
// and glibc/musl skew is not a category of bug here.
//
// The transfer is paid once per POD, which is once per attempt: a pod's
// filesystem does not outlive it and senro owns no cluster object to keep a
// copy in, so Staging.Reused is always false, unlike sshexec's per-host
// directory. The cross-build itself is paid once per platform per release.
//
// plan.Validate refuses one shape: a func step on a target that DELEGATES
// secrets. Delegation hands the pod a source URI for the step's own command
// to resolve, and a function reads ctx.Secret(name), a path to a file senro
// wrote, which there would be none of.
//
// # The exit code versus infrastructure split
//
// See classify, where the argument for each case lives. Summary: a container
// that RAN reports its exit code and no error, whatever killed it, OOMKilled
// and a missing command included; a container that never ran is
// executor.ErrInfra, as are eviction, a lost node, a pod deleted out from
// under the run, an unresponsive apiserver, cancellation, and a pod that did
// not start inside WithStartTimeout.
//
// # Divergences from the container executor
//
// Stdout and stderr arrive MERGED: Kubernetes keeps one log per container, so
// everything the step writes lands in the stdout writer and the stderr writer
// receives only senro's own diagnostics.
//
// The image must be pinned to a digest: this executor talks to an apiserver,
// not a registry, so a tag would enter the cache key as written and a moved
// tag would reuse an entry computed from different bytes. plan.Validate
// refuses an unpinned reference.
//
// EffectiveEnv does not merge the image's own environment, because the
// manifest is unreadable from here. Bounded, since the image's digest is
// already in Class.
//
// DeclaredPlatform comes from the cluster's schedulable nodes rather than an
// image manifest; for a multi-architecture image that is the more accurate
// answer, since what runs is whichever architecture the node has. A cluster
// whose nodes disagree is refused until the pipeline declares one.
// ObservedPlatform reads the node the pod was actually scheduled onto, the
// second independent observation containerexec does not have.
//
// The step's command is the container's `command`, which OVERRIDES the
// image's ENTRYPOINT: the opposite of containerexec, and what makes a k8s
// step run the process the pipeline named rather than whatever an image's
// wrapper script decides.
//
// # What it is not
//
// No Job. The only init container and the only second
// container are senro's own, run the step's image, and exist only for a step
// that carries a mount; a pipeline cannot add a container of its own. No
// registry credential: this executor talks to
// an apiserver rather than a registry, so the NODE pulls the image and a
// private one needs an imagePullSecret in the namespace (the container
// executor pulls its own image and does take one; see
// executor/container.RegistryAuth). No resource
// requests or limits, node selectors, or tolerations: the fields exist in
// internal/kubeapi's types because a pod spec has them, and nothing here sets
// them.
package k8sexec
