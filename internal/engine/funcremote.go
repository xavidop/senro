package engine

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/internal/binprov"
	"github.com/xavidop/senro/internal/executor"
	"github.com/xavidop/senro/internal/funcs"
	"github.com/xavidop/senro/internal/plan"
	"github.com/xavidop/senro/internal/stepwire"
)

// remoteFuncStep reports whether n is a func step that runs somewhere other
// than the coordinator's own process. It reads the NODE's executor, which
// makes it always false for a handler node (handlers declare no executor),
// correct today because plan.validateHandlers refuses a func handler whose
// parent is remote; if that refusal is ever lifted, this function is what
// has to learn it, not the four call sites.
func remoteFuncStep(n *plan.Node) bool {
	return n.Kind == "func" && n.Func != nil && n.ExecutorKey() != plan.ExecutorLocal
}

// invokeRemote runs one attempt of a func step on an executor that is not
// the coordinator: get a binary that runs over there, put it there, run it
// as a step child, read the frames it sends back. Everything a local func
// step inherits from runAttempt (retries, timeouts, snapshots, cache, logs,
// handlers, redaction) this inherits identically: same seam.
func (rc *runCore) invokeRemote(
	ctx context.Context, n *plan.Node, sb executor.Sandbox, c executor.Cmd,
	mounts []executor.Mount, secretPaths map[string]string, attempt int,
	stdout, stderr io.Writer,
) (int, error) {
	runner, ok := sb.(executor.Interactive)
	if !ok {
		return 0, fmt.Errorf(
			"engine: %w: step %q runs a func on the %q executor, whose sandbox cannot be given a "+
				"stdin; a step child reads its whole state from stdin, because nothing about it may "+
				"appear on a command line", executor.ErrInfra, n.ID, n.ExecutorKey())
	}
	loc, ok := sb.(executor.MountLocator)
	if !ok {
		return 0, fmt.Errorf(
			"engine: %w: step %q runs a func on the %q executor, which cannot say where it put "+
				"this step's workspaces; a function reaches a file through ctx.Workspace(name), and "+
				"that has to be a path on the target", executor.ErrInfra, n.ID, n.ExecutorKey())
	}

	bin, stager, err := rc.remoteBinary(ctx, n)
	if err != nil {
		return 0, err
	}
	path, err := rc.stage(ctx, n, bin, stager, attempt)
	if err != nil {
		return 0, err
	}

	state, err := json.Marshal(remoteState(ctx, rc.runID, n, mounts, loc, secretPaths, attempt))
	if err != nil {
		return 0, fmt.Errorf("engine: step %q: encoding the step state: %w", n.ID, err)
	}

	// The child's stdout is the frame channel, read on its own goroutine;
	// its stderr is unframed and goes straight into the step's stderr log.
	// That asymmetry is the protocol's (see internal/stepwire): stderr is
	// the last-resort diagnostic channel for a child that dies before it
	// can frame anything, so reading it must need nothing to have gone
	// right. Both writers are the same redacting, offset-recording writers
	// a local step's output goes through; redact.Writer's mutex matters
	// here, since the pump goroutine and the executor's stderr copy write
	// concurrently by construction.
	pr, pw := io.Pipe()
	var oc childOutcome
	pumped := make(chan struct{})
	go func() {
		defer close(pumped)
		oc = pump(pr, stdout, stderr)
	}()

	exit, runErr := runner.RunInteractive(ctx, executor.Cmd{
		Args: append([]string{path}, childArgs...),
		Env:  c.Env,
		Dir:  c.Dir,
	}, bytes.NewReader(state), pw, stderr)
	_ = pw.Close()
	<-pumped

	return settleChild(n, bin, oc, exit, runErr)
}

// childArgs is the re-entry the coordinator invokes a staged binary with. See
// internal/stepchild.
var childArgs = []string{"__step", "--state-fd", "0"}

// remoteBinary resolves the binary this step's target needs and the
// executor that can put it there. Both halves are asked first at run start
// (checkRemoteFunc); asking again here is a map lookup and a memoized
// provisioner hit, and keeps invokeRemote correct for any caller.
func (rc *runCore) remoteBinary(
	ctx context.Context, n *plan.Node,
) (binprov.Binary, executor.BinaryStager, error) {
	ex, err := rc.executorFor(n)
	if err != nil {
		return binprov.Binary{}, nil, err
	}
	stager, ok := ex.(executor.BinaryStager)
	if !ok {
		return binprov.Binary{}, nil, fmt.Errorf(
			"engine: step %q runs a func on the %q executor, which cannot stage a binary on its "+
				"target. A func's body is compiled into this binary and no plan can describe it, "+
				"so running one anywhere but the coordinator means putting this binary over there "+
				"first. This build can do that over ssh, in a container and in a pod; run this step "+
				"on the coordinator instead",
			n.ID, n.ExecutorKey())
	}
	plat, err := ex.DeclaredPlatform(ctx)
	if err != nil {
		return binprov.Binary{}, nil, fmt.Errorf(
			"engine: step %q runs a func on %q, and senro could not find out what platform that "+
				"is, so it cannot know what binary to send: %w", n.ID, n.ExecutorKey(), err)
	}
	bin, err := rc.binaries.For(ctx, plat)
	if err != nil {
		return binprov.Binary{}, nil, fmt.Errorf("engine: step %q: %w", n.ID, err)
	}
	return bin, stager, nil
}

// stage puts the binary on the target and records that it did.
// binary.staged is emitted on every staging, reused or not: a run whose
// second func step reports reused=false is paying a large transfer per
// step, and an event that only appeared on a transfer could not say so.
func (rc *runCore) stage(
	ctx context.Context, n *plan.Node, bin binprov.Binary,
	stager executor.BinaryStager, attempt int,
) (string, error) {
	started := time.Now()
	staged, err := stager.StageBinary(ctx, executor.StagedBinary{
		Digest: bin.Digest, Path: bin.Path, Size: bin.Size,
	})
	if err != nil {
		return "", fmt.Errorf("engine: step %q: %w", n.ID, err)
	}
	rc.emit(api.Event{
		Type: api.BinaryStaged, Step: n.ID, Attempt: attempt,
		Payload: mustMarshal(api.BinaryStagedBody{
			Digest:     bin.Digest,
			Platform:   bin.Platform.String(),
			Strategy:   string(bin.Strategy),
			Target:     n.ExecutorKey(),
			Path:       staged.Path,
			Bytes:      bin.Size,
			Reused:     staged.Reused,
			DurationNS: time.Since(started).Nanoseconds(),
		}),
	})
	return staged.Path, nil
}

// remoteState is the document the child reads off its stdin. Every path in
// it describes the TARGET: mounts are translated through the sandbox that
// realized them, and a secret's path already is the target's (PutSecret
// wrote the file there). No secret VALUE and no room for one; no
// environment either (a func step is refused Env at build time), and the
// one variable the child does receive, TRACEPARENT, travels in the process
// environment the executor launches it with.
func remoteState(
	ctx context.Context, runID string, n *plan.Node,
	mounts []executor.Mount, loc executor.MountLocator,
	secretPaths map[string]string, attempt int,
) stepwire.State {
	st := stepwire.State{
		Protocol: stepwire.Protocol,
		RunID:    runID, StepID: n.ID, Attempt: attempt,
		Func: n.Func.Name, Params: n.Func.Params,
		Secrets:   secretPaths,
		TimeoutMS: remainingMS(ctx),
	}
	for _, m := range mounts {
		p, ok := loc.MountPath(m.Name)
		if !ok {
			continue
		}
		if st.Workspaces == nil {
			st.Workspaces = make(map[string]string, len(mounts))
		}
		st.Workspaces[m.Name] = p
	}
	return st
}

// remainingMS is how long the child may run, as a DURATION rather than a
// deadline: the two machines' clocks agreeing is not something a build tool
// gets to assume, and a duration measured from the moment the child starts
// needs no agreement at all. Zero means the step declared no timeout, and the
// child then arms nothing.
func remainingMS(ctx context.Context) int64 {
	deadline, ok := ctx.Deadline()
	if !ok {
		return 0
	}
	left := time.Until(deadline).Milliseconds()
	if left < 1 {
		// Already past it. One millisecond rather than zero, because zero
		// means "no deadline at all" on the wire and this is the opposite of
		// that: the child must stop itself immediately.
		return 1
	}
	return left
}

// childOutcome is everything the frame channel said.
type childOutcome struct {
	hello  *stepwire.Hello
	result *stepwire.Result
	// err is a PROTOCOL failure: a frame this build cannot read, a stream
	// that stopped mid-frame. Never the step's own verdict, which is in
	// result.
	err error
}

// pump reads the child's frames and routes them.
//
// It drains to EOF even after a protocol error, because the other end of that
// pipe is the executor copying the child's stdout: a reader that stopped
// early would block it, and the step would hang instead of failing.
func pump(r io.Reader, stdout, stderr io.Writer) childOutcome {
	var oc childOutcome
	fr := stepwire.NewReader(r)
	for {
		stream, payload, err := fr.ReadFrame()
		if err != nil {
			if !errors.Is(err, io.EOF) {
				oc.err = err
			}
			break
		}
		switch stream {
		case stepwire.StreamHello:
			var h stepwire.Hello
			if err := json.Unmarshal(payload, &h); err != nil {
				oc.err = fmt.Errorf("decoding the step child's handshake: %w", err)
				continue
			}
			oc.hello = &h
		case stepwire.StreamStdout:
			_, _ = stdout.Write(payload)
		case stepwire.StreamStderr:
			_, _ = stderr.Write(payload)
		case stepwire.StreamResult:
			var res stepwire.Result
			if err := json.Unmarshal(payload, &res); err != nil {
				oc.err = fmt.Errorf("decoding the step child's result: %w", err)
				continue
			}
			oc.result = &res
		}
	}
	_, _ = io.Copy(io.Discard, r)
	return oc
}

// settleChild turns what came back into the (exit, err) pair Sandbox.Run
// means: exit is the workload's verdict, err is a failure of the substrate.
func settleChild(
	n *plan.Node, bin binprov.Binary, oc childOutcome, exit int, runErr error,
) (int, error) {
	if oc.hello == nil {
		if runErr != nil {
			// The transport is the better story: senro never got far
			// enough for "did not re-enter" to be a claim it can make.
			return exit, runErr
		}
		return 0, fmt.Errorf(
			"engine: %w: step %q: the binary staged on %q did not re-enter as a step child "+
				"(it exited %d and sent no handshake). A pipeline whose main parses its own "+
				"flags before calling senro.Run never reaches the re-entry check; call "+
				"senro.StepChild first if so%s",
			executor.ErrInfra, n.ID, n.ExecutorKey(), exit, protocolDetail(oc.err))
	}

	// Skew is fatal, not a warning: a digest disagreement means the file
	// over there is not the file senro staged, and everything after that
	// is a guess. Deliberately NOT wrapped in ErrInfra: retry.OnInfra
	// would re-run the same wrong binary forever, and this is a deployment
	// fact somebody has to look at.
	if oc.hello.BinaryDigest != bin.Digest {
		return 0, fmt.Errorf(
			"engine: step %q: the binary on %q reports digest %s, and senro staged %s there. "+
				"Something replaced the staged file, or two coordinators of different builds "+
				"are sharing this host's staging directory. senro refuses to run a step on a "+
				"binary it cannot identify",
			n.ID, n.ExecutorKey(), oc.hello.BinaryDigest, bin.Digest)
	}

	if oc.result == nil {
		if runErr != nil {
			return exit, runErr
		}
		return 0, fmt.Errorf(
			"engine: %w: step %q: the step child on %q exited %d without reporting a result; "+
				"its stderr is in this step's log%s",
			executor.ErrInfra, n.ID, n.ExecutorKey(), exit, protocolDetail(oc.err))
	}

	res := *oc.result
	if res.Exit == 0 && res.Error == "" {
		return 0, nil
	}
	code := res.Exit
	if code == 0 {
		code = 1
	}
	return code, remoteFuncError(res)
}

// remoteFuncError rebuilds, on this side, the error the function produced
// on the far side, in the form the engine's classification reads: a panic
// becomes a *funcs.PanicError again (what isPanic matches, settling the
// step StatePanicked; Stack nil since the child already wrote it to
// stderr), and a wrapped executor.ErrInfra is reconstructed rather than
// flattened, so retry.OnInfra still matches.
func remoteFuncError(res stepwire.Result) error {
	if res.Panicked {
		return &funcs.PanicError{Value: res.Error}
	}
	if res.Infra {
		return fmt.Errorf("%s: %w", res.Error, executor.ErrInfra)
	}
	return errors.New(res.Error)
}

func protocolDetail(err error) string {
	if err == nil {
		return ""
	}
	return ": " + err.Error()
}

// checkRemoteFunc provisions every remote func step's binary before the run
// emits its first event: a cgo-tainted dependency graph cannot be
// cross-compiled at all, and internal/cgocheck's report (the offending
// import and the chain that pulled it in) comes back here, before anything
// has run. It provisions rather than merely checking, because provisioning
// IS the check: the cgo analysis, toolchain, package, platform and compile
// are one question, and a guard verifying four of the five would pass and
// then fail. A plan with no remote func step reaches nothing here.
func checkRemoteFunc(ctx context.Context, rc *runCore, p *plan.Plan) error {
	for i := range p.Nodes {
		if !remoteFuncStep(&p.Nodes[i]) {
			continue
		}
		if _, _, err := rc.remoteBinary(ctx, &p.Nodes[i]); err != nil {
			return err
		}
	}
	return nil
}
