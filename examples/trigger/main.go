// Command trigger is a pipeline that decides for itself whether an event is
// its business, which is the whole of what a trigger is for.
//
// Build it first, because the exit code is the whole point and `go run` does
// not pass one through: it reports "exit status 78" on stderr and then exits
// 1 itself, so `go run ... ; echo $?` prints 1 no matter what this program
// decided. Anything reading the code, a dispatcher included, must run the
// binary.
//
//	go build -o /tmp/trigger ./examples/trigger
//
//	/tmp/trigger --trigger-event ./examples/trigger/events/push-main.json
//	echo $?   # 0: a push to main is this pipeline's business, so it ran
//
//	/tmp/trigger --trigger-event ./examples/trigger/events/push-topic.json
//	echo $?   # 78: a push to somebody's branch is not, so nothing ran at all
//
//	/tmp/trigger
//	echo $?   # 0: no event, so no gating; this is the local loop
//
// This is an ordinary senro pipeline with one extra option; the decision is
// visible in the exit code, the only thing a dispatcher has to read, which
// is what keeps a dispatcher stateless. Three things to notice: main parses
// the flag, not senro (the library never reads os.Args); main exits, not
// senro (Run returns trigger.ErrNoMatch and this maps it to 78); and a
// no-match leaves nothing behind, not even a runs/<id> directory. After a
// run that did happen, runs/<id>/run.json says what triggered it.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"

	"github.com/xavidop/senro"
	"github.com/xavidop/senro/exec"
	"github.com/xavidop/senro/trigger"
)

// exitNoTriggerMatch is EX_CONFIG, the conventional "this event was not my
// business" code. A convention of the embedding program, not the library,
// which is why it is spelled out here rather than hidden inside Run.
const exitNoTriggerMatch = 78

func main() { os.Exit(run()) }

func run() int {
	eventPath := flag.String("trigger-event", "",
		"path to the event that started this invocation, or \"-\" for stdin; "+
			"empty means no event, which gates nothing")
	flag.Parse()

	// The library does not read this flag: main found it, main loads it.
	ev, err := trigger.LoadEvent(*eventPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}

	err = senro.Run(context.Background(), pipeline(),
		senro.WithTrigger(ev,
			// A push to main is a release candidate: build everything.
			trigger.OnPush(trigger.Branches("main")),
			// A pull request is worth building when it opens and every time
			// it gets new commits, and not when somebody labels it.
			trigger.OnPullRequest(trigger.Actions("opened", "synchronize")),
			// Only tags that are releases, so a "docs-2024" tag does not
			// deploy version 0.0.0.
			trigger.OnTag(trigger.Semver(">=1.0.0")),
			// The nightly, which says what kind of run it is through a
			// parameter the pipeline's own conditions can read.
			trigger.OnSchedule("0 3 * * *", trigger.Params{"suite": "full"}),
		))

	switch {
	case errors.Is(err, trigger.ErrNoMatch):
		// Not a failure. Nothing ran, nothing was written, and this is the
		// answer the dispatcher asked for.
		fmt.Fprintln(os.Stderr, err)
		return exitNoTriggerMatch
	case err != nil:
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	return 0
}

// pipeline is an ordinary pipeline. Nothing in it knows a trigger exists,
// except the one step that reads the "suite" parameter a matched trigger
// contributed, the same way it would read one from senro.WithParams.
func pipeline() *senro.Pipeline {
	pipe := senro.New("triggered")
	w := pipe.Workflow("build")
	w.Step("compile", exec.Command("sh", "-c", "echo compiling"))
	w.Step("test", exec.Command("sh", "-c", "echo testing")).Needs("compile")
	w.Step("soak", exec.Command("sh", "-c", "echo soaking, which only the nightly does")).
		Needs("test").
		When(senro.ParamIs("suite", "full"))
	return pipe
}
