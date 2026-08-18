// Package sshexec runs steps on a remote host over SSH.
//
// One step, one SESSION per phase: prepare, one per workspace, one per
// secret, run, read back, clean up. Every session of a run rides one
// connection per host (see Multiplexing). The step's command is a process on
// the far side, its output comes back on the session's own two streams, and
// its exit code comes from a file the wrapper wrote before exiting with it.
//
// # The library that is not here
//
// senro shells out to the `ssh` binary rather than linking
// golang.org/x/crypto/ssh: the largest decision in this package.
//
// Shelling out buys everything an organisation has already configured:
// ~/.ssh/config with its Host, Match and Include blocks, known_hosts with
// hashed entries and certificates, ProxyJump and ProxyCommand, the agent,
// PKCS11 and FIDO keys. A pipeline names a destination and it works exactly
// as typing it works. The security half decided it: with a library, senro
// would be writing the host key callback, and getting that subtly wrong
// means a machine in the middle receiving a step's credentials. Host key
// policy here is OpenSSH's, applied by the configuration the operator
// already trusts, and senro's job is to not weaken it (see run). It also
// matches internal/kubeapi and internal/dockerd speaking HTTP rather than
// linking clients.
//
// The costs: a process per command; behaviour depends on a binary senro does
// not control; and error classification is coarser, which the next section
// solves.
//
// # Multiplexing
//
// A run pays connection setup once per host, not once per command: the first
// invocation opens an OpenSSH control master (`ssh -M -N -f` with a
// ControlPersist) and every later one rides it with ControlMaster=no. Only
// that dedicated invocation may ever become a master, because a master
// backgrounds itself holding whatever descriptors it was handed, and a
// command allowed to become one would daemonize onto the pipes a step's
// output is being read from.
//
// The socket is credential-adjacent: opening it IS the authenticated session.
// It lives in attachsrv.Dir's 0700 directory, the attach socket's own, under
// a random nonce rather than the run id, so no other run can name it and no
// run id is long enough to break a path measured against a 104-byte socket
// address. Close removes it; ssh's own ControlPersist removes it when Close
// never runs, on a process nothing about this coordinator's health can stop,
// exactly as the remote reaper does for a credential.
//
// The operator's configuration wins, as it does for host keys: senro asks
// `ssh -G` and, when the destination already resolves a ControlPath, adds no
// multiplexing option at all. Where a master cannot be opened the run carries
// on unmultiplexed and says so once (see muxer.disable), because that run is
// slower and correct, and refusing it would end a build over an optimisation.
// ssh.NoMultiplexing turns the whole of it off.
//
// # Exit 255, and the command that really exits 255
//
// ssh(1) exits with the remote command's status, or with 255 for its own
// failures, and nothing at the process level distinguishes "the connection
// broke" from "your command exited 255". retry.OnInfra keys off exactly
// this line.
//
// The wrapper writes the command's real status to a file in the attempt's
// directory BEFORE exiting with it. On any exit code other than 255 senro
// trusts the code. On 255 it opens one more connection and reads the file:
// present means the command ran and its contents are the verdict; absent
// means nothing ran and this is infrastructure. Any failure in the wrapper
// itself also exits 255, deliberately, so "the command never ran" is one
// case with one answer rather than a family of sentinel codes a real
// command could collide with. The extra round trip is paid only in the
// ambiguous case. See classify and
// TestARealExit255IsNotConfusedWithSSHsOwn255.
//
// Otherwise k8sexec.classify's rule holds: a command that RAN reports its
// exit code and no error, whatever killed it, 127 and 137 included.
//
// # Secrets
//
// The value crosses as stdin bytes on the connection that writes it, into a
// file created under `umask 077` in a directory made at 0700. Never a
// command argument: argv is in /proc, in `ps`, in auditd's execve records
// and in shell history. Never an environment variable: /proc/<pid>/environ.
// Never SendEnv. umask rather than a chmod after the write, because a chmod
// leaves a window in which the file is readable.
//
// Where: the host's own runtime directory, chosen ON THE HOST in
// secretdir.Root's order ($XDG_RUNTIME_DIR, /dev/shm, $TMPDIR). Deliberately
// not inside the attempt directory: that is what Snapshot reads, and a
// credential must never be a snapshot candidate.
//
// How it goes away: Close removes it on every path including keep, shredded
// first where the host has shred. A REAPER removes it when Close never runs
// at all: setupScript detaches a shell that sleeps for the TTL and then
// removes both directories, armed before anything is written, so nothing
// about this coordinator's health can stop it. See DefaultSecretTTL.
//
// # Workspaces cross the connection, in both directions
//
// A mount is filled before the step runs and read back when Snapshot is
// called, with tar over the connection. What this executor and k8sexec must
// agree about exactly lives in internal/executor/mountxfer; what is here is
// only how the far-side process is started. The tar out is
// workspace.WriteTar's normalized form, so the coordinator needs no tar of
// its own; the tar back is the remote tar's, extracted through
// workspace.ReadTar after a rewrite of its entry names, keeping ReadTar's
// path-traversal and symlink-target refusals exactly as they are: the
// tarball arrives from a machine a step just ran arbitrary commands on.
//
// A snapshot is taken of the STAGING copy, through the same
// mountsnap.Snapshot every other executor uses, and then staging replaces
// the coordinator's directory. Consequences:
//
//   - A mount carries exactly what a SNAPSHOT carries: excluded paths (.git
//     and node_modules by default) are not sent, not in what comes back,
//     and removed by the replacement. A step needing repository history on
//     the far side fetches it there.
//   - After an ssh step the coordinator's directory is EXACTLY what its
//     recorded digest describes, a stronger property than the local and
//     container executors have.
//   - The bytes cross twice per step, and there is no incremental transfer.
//
// # A scratch cache crosses the same way, and is read back before it is saved
//
// A scratch cache is an ordinary mount here: filled from the coordinator's
// directory before the step runs, and pulled back afterwards by ReadMount,
// which is Snapshot's own copyOut without the digest and without replacing
// the coordinator's copy. The engine keeps what came back aside and saves
// THAT (see engine.wsManager.readScratch), so the bytes stored under the key
// are the bytes this host actually left. A read-back that fails stores
// nothing at all: an entry is written once under its key and never
// rewritten, so a stale copy there is the answer every later run gets.
//
// Two things differ from a workspace, both because a scratch cache is not
// evidence:
//
//   - Nothing is excluded from it. internal/scratch saves the directory
//     whole and node_modules is usually the POINT of one, so the workspace
//     defaults would send a cache with its contents missing (see
//     mountsnap.Excluder).
//   - Nothing is snapshotted, and no digest exists. It never reaches a
//     ws.snapshot, a ledger entry or a cache key.
//
// The cost is a second full transfer per step, on top of the one that sent
// the cache out, with no incremental transfer either way. A dependency tree
// large enough to be worth caching can be large enough that carrying it
// twice costs more than the download it saves; the docs say so plainly.
//
// One shape stays refused at plan time: one scratch cache mounted both here
// and by a step on the coordinator's own filesystem. Such a step writes that
// directory while this executor is tarring it, and a half-written tree would
// be saved under an immutable key. See plan.validateScratchTargets.
//
// # Where things land on the host
//
// A mount's At is realized UNDER the attempt's own directory, leading
// separator stripped: "/src" becomes <attempt>/ws/src. localexec's rule,
// for localexec's constraint: senro owns nothing on the far side except one
// directory in an account's own space, and creating /src on a build host
// means being root on it.
//
// A working directory a mount realized is that mount's remote directory;
// anything else is used verbatim, so WorkDir("/opt/app") means /opt/app. A
// working directory that does not exist is an infrastructure failure,
// reached through the ambiguous-255 path above.
//
// # The cache class is not the hostname
//
// Class reports "ssh/<os>/<arch>" by default, read with uname, and reports
// ssh.CacheClass verbatim when declared. A class built from host identity
// would mean a fleet of identical build machines never shared a cache
// entry, and nothing would report it: the cache would simply never hit.
//
// # Divergences worth knowing
//
// Stdout and stderr arrive SEPARATE, unlike k8sexec's merged pod log.
//
// A step's environment is `env -i` plus what the pipeline declared plus the
// host's PATH, so nothing of the remote account's login environment reaches
// it. That matters more here than for localexec: without it a step would
// receive SSH_AUTH_SOCK on a connection with agent forwarding, handing a
// build step the operator's own keys.
//
// A login shell that PRINTS for a non-interactive session puts those bytes
// in the step's own output. senro's own scripts mark their output and parse
// only marked lines; a step's output cannot be protected. A property of
// ssh, recorded rather than fixed.
//
// A read-only mount is a request this executor cannot enforce, exactly as
// localexec cannot: it is read back and snapshotted without replacing the
// coordinator's copy, which is what lets engine.snapshotMounts' read-only
// breach check see the write.
//
// Cancellation closes the session, and Close then signals the wrapper's
// recorded pid. Neither guarantees the remote command is dead: a command
// that detached from its session can outlive both, and there is no
// equivalent here of deleting a pod and knowing the workload is gone.
//
// # What it is not
//
// No second control master per host, so sshd's MaxSessions caps how many of
// a run's steps share one connection; anything over maxMuxSessions opens its
// own. No bastion support beyond the operator's own ProxyJump, which is the
// point of shelling out. No func HANDLERS: plan.Validate refuses a func OnFailure
// or Always handler whose parent runs anywhere but the coordinator, because
// a handler inherits its parent's executor and there is nothing to key its
// staging to; func STEPS do run here, over staging.go's StageBinary and
// internal/binprov. No host-facts cache across runs. No
// `senro ssh gc`: the reaper removes what a coordinator's death leaves. No
// incremental or resumable workspace transfer, and no disk-space
// precondition before one.
package sshexec
