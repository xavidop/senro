//go:build js && wasm

package main

import (
	"strings"
	"syscall/js"
	"time"

	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/internal/webui/present"
)

// view owns the page. Everything it draws comes from
// internal/webui/present, a pure function of the folded state tested on
// the host; this file is the DOM plumbing and holds no opinion about what
// anything means.
//
// Every piece of text from the run reaches the page through textContent,
// never innerHTML: step ids, error strings and log output are values a
// pipeline author controls, and interpolating them into markup would
// execute whatever a build's stderr happened to contain, on a page holding
// a session credential.
type view struct {
	doc js.Value

	runName   js.Value
	runSub    js.Value
	runStatus js.Value
	linkState js.Value

	counts      js.Value
	steps       js.Value
	runActions  js.Value
	stepActions js.Value
	detailTitle js.Value
	detail      js.Value
	logTabs     js.Value
	logLabel    js.Value
	logPre      js.Value
	notice      js.Value

	// drawn is the signature of what each section last put in the DOM, so
	// an unchanged section is not rebuilt. Not only an optimisation: the
	// paint loop runs every animation frame while anything runs, and
	// rebuilding nodes sixty times a second makes an element under the
	// pointer unclickable and text unselectable. The Node smoke harness
	// cannot see that (its stubbed DOM has no notion of detachment); it
	// took a real Chromium to find. Durations are deliberately NOT part of
	// a signature: they change every frame and are updated in place.
	drawn map[string]string
	// durs maps a step id to the duration element already in the list, which
	// is what makes updating one in place possible without a query.
	durs map[string]js.Value
}

// changed reports whether section's content differs from what is on the page,
// and records the new signature.
func (v *view) changed(section, sig string) bool {
	if v.drawn == nil {
		v.drawn = map[string]string{}
	}
	if v.drawn[section] == sig {
		return false
	}
	v.drawn[section] = sig
	return true
}

func newView() *view {
	doc := js.Global().Get("document")
	byID := func(id string) js.Value { return doc.Call("getElementById", id) }
	return &view{
		doc:         doc,
		runName:     byID("run-name"),
		runSub:      byID("run-sub"),
		runStatus:   byID("run-status"),
		linkState:   byID("link-state"),
		counts:      byID("counts"),
		steps:       byID("steps"),
		runActions:  byID("run-actions"),
		stepActions: byID("step-actions"),
		detailTitle: byID("detail-title"),
		detail:      byID("detail"),
		logTabs:     byID("log-tabs"),
		logLabel:    byID("log-label"),
		logPre:      byID("log"),
		notice:      byID("notice"),
	}
}

// frame is everything the page draws, gathered under the app's lock and
// rendered without it: a renderer holding the state lock while touching
// the DOM would apply backpressure through the subscriber to the engine.
type frame struct {
	state    *api.RunState
	now      time.Time
	selected string
	stream   string
	streams  []string
	log      string
	status   string
	link     string
	// notice is the outcome of the last control request, if there is one to
	// report. Empty draws nothing.
	notice string
	// noticeBad marks a notice that reports a refusal rather than a result.
	noticeBad bool
	// sending suppresses the buttons while a control request is in flight,
	// so a second click cannot queue a second op against a state the first
	// one is about to change.
	sending bool
}

func (v *view) render(f frame) {
	v.renderHeader(f)
	v.renderCounts(f)
	v.renderActions(f)
	v.renderSteps(f)
	v.renderDetail(f)
	v.renderLog(f)
	v.renderNotice(f)
}

func (v *view) renderHeader(f frame) {
	name := "senro"
	if f.state != nil && f.state.Run.ID != "" {
		name = f.state.Run.ID
	}
	setText(v.runName, name)

	sub := present.Subtitle(f.state)
	if f.status != "" {
		sub = f.status
	}
	setText(v.runSub, sub)
	setClass(v.runSub, "sub")
	if f.link == "broken" {
		setClass(v.runSub, "sub sub-error")
	}

	badge := present.RunBadge(f.state)
	setText(v.runStatus, badge.Label)
	setClass(v.runStatus, "pill pill-"+string(badge.Tone))

	setClass(v.linkState, "link-state "+f.link)
	v.linkState.Call("setAttribute", "title", linkTitle(f.link))
}

func linkTitle(link string) string {
	switch link {
	case "live":
		return "attached to the run's event stream"
	case "broken":
		return "not attached"
	default:
		return "connecting"
	}
}

func (v *view) renderCounts(f frame) {
	counts := present.Counts(f.state)
	var sig strings.Builder
	for _, c := range counts {
		sig.WriteString(c.Label)
		sig.WriteByte('=')
		sig.WriteString(itoa(c.N))
		sig.WriteByte(';')
	}
	if !v.changed("counts", sig.String()) {
		return
	}
	clear(v.counts)
	for _, c := range counts {
		li := v.el("li")
		n := v.el("span")
		setClass(n, "n")
		setText(n, itoa(c.N))
		k := v.el("span")
		setClass(k, "k")
		setText(k, c.Label)
		li.Call("appendChild", n)
		li.Call("appendChild", k)
		v.counts.Call("appendChild", li)
	}
}

// renderActions draws the run- and step-scoped control buttons. What to
// offer is decided by present; this draws whatever it returns. The op and
// step ride in data attributes rather than being parsed back out of the
// label, the same arrangement the step rows use.
func (v *view) renderActions(f frame) {
	draw := func(section string, into js.Value, actions []present.Action) {
		var sig strings.Builder
		if f.sending {
			sig.WriteString("sending|")
		}
		for _, a := range actions {
			sig.WriteString(a.Op)
			sig.WriteByte(':')
			sig.WriteString(a.Step)
			sig.WriteByte(':')
			sig.WriteString(a.Label)
			sig.WriteByte(';')
		}
		if !v.changed(section, sig.String()) {
			return
		}
		clear(into)
		for _, a := range actions {
			b := v.el("button")
			setClass(b, "act act-"+string(a.Tone))
			b.Call("setAttribute", "data-op", a.Op)
			if a.Step != "" {
				b.Call("setAttribute", "data-op-step", a.Step)
			}
			if a.Confirm {
				b.Call("setAttribute", "data-op-confirm", a.Label)
			}
			if f.sending {
				b.Set("disabled", true)
			}
			setText(b, a.Label)
			into.Call("appendChild", b)
		}
	}
	draw("run-actions", v.runActions, present.RunActions(f.state))
	draw("step-actions", v.stepActions, present.StepActions(f.state, f.selected))
}

func (v *view) renderNotice(f frame) {
	if !v.changed("notice", f.notice) {
		return
	}
	setText(v.notice, f.notice)
	class := "notice"
	if f.notice == "" {
		class = "notice notice-empty"
	} else if f.noticeBad {
		class = "notice notice-bad"
	}
	setClass(v.notice, class)
}

func (v *view) renderSteps(f frame) {
	rows := present.Rows(f.state, f.now)

	// The signature covers everything except the duration: a frame that
	// changed only elapsed time updates the duration nodes in place and
	// touches nothing else, which is what keeps a row clickable while its
	// step runs.
	var sig strings.Builder
	sig.WriteString(f.selected)
	sig.WriteByte('|')
	for _, r := range rows {
		sig.WriteString(r.ID)
		sig.WriteByte(':')
		sig.WriteString(r.Badge.Label)
		sig.WriteByte(':')
		sig.WriteString(string(r.Badge.Tone))
		sig.WriteByte(':')
		sig.WriteString(r.Needs)
		if r.Child {
			sig.WriteByte('*')
		}
		sig.WriteByte(';')
	}
	if !v.changed("steps", sig.String()) {
		for _, r := range rows {
			if d, ok := v.durs[r.ID]; ok {
				setText(d, r.Duration)
			}
		}
		return
	}

	clear(v.steps)
	v.durs = make(map[string]js.Value, len(rows))
	if len(rows) == 0 {
		p := v.el("li")
		setClass(p, "empty")
		setText(p, "no steps yet")
		v.steps.Call("appendChild", p)
		return
	}
	for _, r := range rows {
		li := v.el("li")
		class := ""
		if r.Child {
			class = "child"
		}
		if r.ID == f.selected {
			class += " selected"
		}
		setClass(li, class)
		// The id lives in a data attribute, so the click handler reads a
		// value rather than parsing the text it drew.
		li.Call("setAttribute", "data-step", r.ID)

		pill := v.el("span")
		setClass(pill, "pill pill-"+string(r.Badge.Tone))
		setText(pill, r.Badge.Label)

		id := v.el("span")
		setClass(id, "id")
		setText(id, r.ID)

		li.Call("appendChild", pill)
		li.Call("appendChild", id)

		if r.Needs != "" {
			needs := v.el("span")
			setClass(needs, "needs")
			setText(needs, r.Needs)
			li.Call("appendChild", needs)
		}
		// Always created, even when empty: a duration that appeared only
		// once a step had one would be a shape change, which would rebuild
		// the whole list on the first tick of every step.
		d := v.el("span")
		setClass(d, "dur")
		setText(d, r.Duration)
		li.Call("appendChild", d)
		v.durs[r.ID] = d
		v.steps.Call("appendChild", li)
	}
}

func (v *view) renderDetail(f frame) {
	if f.selected == "" {
		if v.changed("detail", "") {
			clear(v.detail)
			setText(v.detailTitle, "Nothing selected")
		}
		return
	}
	fields := present.Detail(f.state, f.selected, f.now)
	var sig strings.Builder
	sig.WriteString(f.selected)
	sig.WriteByte('|')
	for _, field := range fields {
		sig.WriteString(field.Label)
		sig.WriteByte('=')
		sig.WriteString(field.Value)
		sig.WriteByte(';')
	}
	if !v.changed("detail", sig.String()) {
		return
	}
	clear(v.detail)
	setText(v.detailTitle, f.selected)
	for _, field := range fields {
		dt := v.el("dt")
		setText(dt, field.Label)
		dd := v.el("dd")
		setText(dd, field.Value)
		v.detail.Call("appendChild", dt)
		v.detail.Call("appendChild", dd)
	}
}

func (v *view) renderLog(f frame) {
	tabSig := f.stream + "|" + strings.Join(f.streams, ",")
	if v.changed("log-tabs", tabSig) {
		v.renderLogTabs(f)
	}
	v.renderLogBody(f)
}

func (v *view) renderLogTabs(f frame) {
	clear(v.logTabs)
	for _, name := range f.streams {
		b := v.el("button")
		class := ""
		if name == f.stream {
			class = "on"
		}
		setClass(b, class)
		b.Call("setAttribute", "data-stream", name)
		setText(b, name)
		v.logTabs.Call("appendChild", b)
	}
}

func (v *view) renderLogBody(f frame) {
	setText(v.logLabel, "Output")

	// Guarded because assigning textContent replaces the text node even
	// when the string is identical, cancelling the operator's selection;
	// on a running step that happened every frame.
	if !v.changed("log", f.stream+"|"+f.log) {
		return
	}
	if f.log == "" {
		setText(v.logPre, "")
		return
	}
	// A single textContent assignment rather than appending nodes: the log
	// is one string and the browser's own text node is a better place to
	// keep it than a tree this code would have to reconcile.
	setText(v.logPre, f.log)
}

// onStepClick installs a single delegated click handler on the list: the
// list is rebuilt on every frame, and per-row handlers would allocate and
// release a js.Func per step per paint, a leak in all but name.
func (v *view) onStepClick(f func(step string)) {
	v.steps.Call("addEventListener", "click", js.FuncOf(func(_ js.Value, args []js.Value) any {
		if len(args) == 0 {
			return nil
		}
		target := args[0].Get("target")
		for !target.IsNull() && !target.IsUndefined() {
			if target.Equal(v.steps) {
				return nil
			}
			if id := target.Call("getAttribute", "data-step"); !id.IsNull() && !id.IsUndefined() {
				f(id.String())
				return nil
			}
			target = target.Get("parentElement")
		}
		return nil
	}))
}

// onStreamClick is the same delegated arrangement for the log's own tabs.
func (v *view) onStreamClick(f func(stream string)) {
	v.logTabs.Call("addEventListener", "click", js.FuncOf(func(_ js.Value, args []js.Value) any {
		if len(args) == 0 {
			return nil
		}
		target := args[0].Get("target")
		if target.IsNull() || target.IsUndefined() {
			return nil
		}
		if name := target.Call("getAttribute", "data-stream"); !name.IsNull() && !name.IsUndefined() {
			f(name.String())
		}
		return nil
	}))
}

// onActionClick installs the delegated handler for both action bars
// (delegated for onStepClick's reason). The confirmation happens here
// rather than in the app: window.confirm blocks the calling goroutine
// until the person answers, and the one acceptable place for that is a DOM
// event handler not holding the state lock.
func (v *view) onActionClick(f func(op, step string)) {
	handler := js.FuncOf(func(_ js.Value, args []js.Value) any {
		if len(args) == 0 {
			return nil
		}
		target := args[0].Get("target")
		if target.IsNull() || target.IsUndefined() {
			return nil
		}
		op := target.Call("getAttribute", "data-op")
		if op.IsNull() || op.IsUndefined() {
			return nil
		}
		step := ""
		if s := target.Call("getAttribute", "data-op-step"); !s.IsNull() && !s.IsUndefined() {
			step = s.String()
		}
		if c := target.Call("getAttribute", "data-op-confirm"); !c.IsNull() && !c.IsUndefined() {
			what := c.String()
			if step != "" {
				what += " " + step
			}
			if !js.Global().Call("confirm", what+"?").Bool() {
				return nil
			}
		}
		f(op.String(), step)
		return nil
	})
	v.runActions.Call("addEventListener", "click", handler)
	v.stepActions.Call("addEventListener", "click", handler)
}

func (v *view) el(tag string) js.Value { return v.doc.Call("createElement", tag) }

func setText(node js.Value, s string) {
	if node.IsNull() || node.IsUndefined() {
		return
	}
	node.Set("textContent", s)
}

func setClass(node js.Value, s string) {
	if node.IsNull() || node.IsUndefined() {
		return
	}
	node.Set("className", s)
}

func clear(node js.Value) {
	if node.IsNull() || node.IsUndefined() {
		return
	}
	node.Set("textContent", "")
}

// itoa formats a small non-negative count without importing strconv, which
// this binary otherwise links only through packages that already need it.
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}
