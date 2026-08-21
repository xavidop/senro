package engine

// The routing decision behind "a func handler runs wherever its parent
// ran". White-box because the whole point is that the answer CANNOT be read
// off the node: a handler declares no executor, so anything derived from
// the node alone is wrong for every handler ever written, and the
// end-to-end proof (funcremote_test.go's TestAFuncHandlerRunsOnItsParentsHost)
// needs a real host to demonstrate it.

import (
	"context"
	"testing"

	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/internal/executor"
	"github.com/xavidop/senro/internal/funcs"
	"github.com/xavidop/senro/internal/plan"
)

// handlerNode is a handler as the plan actually holds one: a func node with
// no executor of its own, because plan.Validate refuses one that declares
// any.
func handlerNode() *plan.Node {
	return &plan.Node{ID: "notify", Kind: "func", Func: &plan.FuncSpec{Name: "notify/slack"}}
}

// The trap this whole change exists to close: the handler node says
// "local", and believing it runs the function on the coordinator with the
// parent's container-only paths.
func TestAHandlerNodeAloneCannotSayWhereItRuns(t *testing.T) {
	h := handlerNode()
	if h.ExecutorKey() != plan.ExecutorLocal {
		t.Fatalf("test bug: a handler node's own key is %q, want the local default", h.ExecutorKey())
	}

	onParentsHost := invocation{key: "ssh:build-07.internal"}
	if !onParentsHost.remote(h) {
		t.Error("a func handler whose parent runs over ssh must be invoked remotely")
	}

	onCoordinator := invocation{key: plan.ExecutorLocal}
	if onCoordinator.remote(h) {
		t.Error("a func handler whose parent runs locally must be invoked in this process")
	}
}

// An exec handler on a remote parent is not a remote FUNC invocation: it is
// an ordinary command in the parent's sandbox, and staging a binary for it
// would be a transfer nobody asked for.
func TestOnlyAFuncNodeIsInvokedRemotely(t *testing.T) {
	execHandler := &plan.Node{ID: "notify", Kind: "exec", Cmd: []string{"curl", "-fsS", "..."}}
	if (invocation{key: "ssh:build-07.internal"}).remote(execHandler) {
		t.Error("an exec handler must not take the remote func path")
	}

	// A func node with no spec is a malformed plan, not a remote func step;
	// nodeShape refuses it, and this path must not dereference it first.
	noSpec := &plan.Node{ID: "notify", Kind: "func"}
	if (invocation{key: "ssh:build-07.internal"}).remote(noSpec) {
		t.Error("a func node carrying no spec must not take the remote func path")
	}
}

// The other half: what a remote handler is TOLD. ctx.Failure() has to work
// the same on the far side, and the only channel to it is the state
// document, so the failure has to be in there.
func TestRemoteStateCarriesAHandlersFailure(t *testing.T) {
	fail := &funcs.Failure{
		Run: "01RUN", Step: "boom", Attempt: 2, State: api.StateFailed,
		ExitCode: 7, Error: "the function said no", LogTail: "broke here\n",
	}
	st := remoteState(context.Background(), "01RUN", handlerNode(),
		invocation{key: "ssh:build-07.internal", failure: fail},
		nil, nopLocator{}, nil, 1)

	if st.Failure == nil {
		t.Fatal("a handler's state document carries no failure, so ctx.Failure() is false over there")
	}
	if st.Failure.Step != "boom" {
		t.Errorf("Step = %q, want the PARENT's id, not the handler's", st.Failure.Step)
	}
	if st.Failure.Attempt != 2 {
		t.Errorf("Attempt = %d, want the attempt the step actually reached", st.Failure.Attempt)
	}
	if st.Failure.ExitCode != 7 || st.Failure.State != string(api.StateFailed) {
		t.Errorf("exit/state = %d/%q, want 7/%q", st.Failure.ExitCode, st.Failure.State, api.StateFailed)
	}
	if st.Failure.LogTail != "broke here\n" {
		t.Errorf("LogTail = %q; a func handler classifies from this rather than opening the log file",
			st.Failure.LogTail)
	}
}

// A STEP is not cleaning up after anything, and the wire must say so by
// omission rather than by an empty object that decodes to ok=true.
func TestRemoteStateOmitsTheFailureForAStep(t *testing.T) {
	n := &plan.Node{
		ID: "deploy", Kind: "func", Func: &plan.FuncSpec{Name: "deploy/helm"},
		Executor: &plan.ExecutorSpec{Kind: plan.ExecutorSSH, Host: "build-07.internal"},
	}
	st := remoteState(context.Background(), "01RUN", n,
		invocation{key: n.ExecutorKey()}, nil, nopLocator{}, nil, 1)
	if st.Failure != nil {
		t.Errorf("a func step's state document carries a failure: %+v", st.Failure)
	}
}

type nopLocator struct{}

func (nopLocator) MountPath(string) (string, bool) { return "", false }

var _ executor.MountLocator = nopLocator{}
