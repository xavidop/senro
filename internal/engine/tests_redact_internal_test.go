package engine

import (
	"context"
	"io"
	"strings"
	"testing"

	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/internal/eventlog"
	"github.com/xavidop/senro/internal/executor"
	"github.com/xavidop/senro/internal/plan"
	"github.com/xavidop/senro/internal/redact"
	"github.com/xavidop/senro/internal/sink"
)

// printingSandbox writes out to the step's stdout and exits with code.
type printingSandbox struct {
	recordingSandbox
	out  string
	code int
}

func (s *printingSandbox) Run(_ context.Context, _ executor.Cmd, stdout, _ io.Writer) (int, error) {
	_, err := io.WriteString(stdout, s.out)
	return s.code, err
}

type printingExecutor struct {
	recordingExecutor
	sb *printingSandbox
}

func (e *printingExecutor) Sandbox(context.Context, executor.SandboxSpec) (executor.Sandbox, error) {
	return e.sb, nil
}

// TestTheTestParserReadsRedactedStdout pins WHERE the parser sits, which the
// end-to-end TestTestOutputIsRedacted cannot: every event payload is redacted
// again on append, so a parser wired upstream of the stdout redactor would
// still put the marker in the ledger. Here the payload pass must find nothing
// to replace (redactedPayloads stays 0), which holds only if the bytes the
// parser read were already redacted.
func TestTheTestParserReadsRedactedStdout(t *testing.T) {
	const value = "token-aaaaaaaaaa"
	ledger, err := eventlog.Open(t.TempDir())
	if err != nil {
		t.Fatalf("eventlog.Open: %v", err)
	}
	defer func() { _ = ledger.Close() }()
	logs := eventlog.NewLogSet(t.TempDir())
	rec := sink.Recording()
	sb := &printingSandbox{code: 1, out: `{"Action":"run","Test":"TestS"}` + "\n" +
		`{"Action":"output","Test":"TestS","Output":"leaked ` + value + `\n"}` + "\n" +
		`{"Action":"fail","Test":"TestS"}` + "\n"}
	rc := &runCore{
		ledger: ledger, sink: rec, runID: "r1",
		ws:          newTestWSManager(t, &plan.Plan{Version: 1}, nil),
		defaultExec: &printingExecutor{sb: sb},
		redact:      redact.New(redact.Value{Label: "tok", Value: []byte(value)}),
	}
	n := &plan.Node{ID: "t", Kind: "exec", Cmd: []string{"true"}, Tests: &plan.TestsSpec{Format: "gotest"}}
	rc.runAttempt(context.Background(), n, Options{}, logs, 1)

	var outputs []string
	for _, e := range rec.Events() {
		if e.Type != api.TestFinished {
			continue
		}
		var b api.TestFinishedBody
		if err := e.Decode(&b); err != nil {
			t.Fatal(err)
		}
		outputs = append(outputs, b.Output)
	}
	if len(outputs) != 1 || !strings.Contains(outputs[0], redact.Placeholder) || strings.Contains(outputs[0], value) {
		t.Fatalf("test.finished outputs = %q, want one carrying the marker and not the value", outputs)
	}
	if got := rc.redactedPayloads.Load(); got != 0 {
		t.Errorf("redactedPayloads = %d, want 0: the parser read the secret unredacted and only the "+
			"ledger's payload pass caught it", got)
	}
}
