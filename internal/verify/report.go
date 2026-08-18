package verify

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
)

// explanations is what each verdict means to someone acting on it. A table
// rather than a switch so adding a verdict cannot leave one sentence-less.
// The mismatch text deliberately does not say "impure": this command sees
// bytes, not reasons, so it names what the key does not cover instead.
var explanations = map[Verdict]string{
	Verified: "the re-run reproduced this entry exactly",
	Mismatch: "both re-runs agreed with each other and neither reproduced the entry, so this step " +
		"depends on something its key does not cover: the network, a file outside its workspace, an " +
		"environment variable it never declared in CacheEnv, or the clock",
	NonDeterministic: "the two re-runs disagreed with each other as well as with the entry, so this step " +
		"does not produce the same bytes twice from the same input; that is a reproducibility problem " +
		"(an archive embedding a build timestamp looks exactly like this) and it is NOT evidence about " +
		"purity either way",
	Planned: "not executed: pass --rerun to actually run it",
}

// Format renders a report the way `senro cache explain` renders a record: a
// verdict line per step, indented detail, and a summary. Written into one
// buffer and out in a single write, so a partial report can never reach
// something parsing it, the same as `ws diff`.
func Format(w io.Writer, rep Report, dir string) error {
	var b bytes.Buffer

	if len(rep.Steps) == 0 {
		fmt.Fprintf(&b, "no cached Pure() step to verify in %s: a step is only verifiable once it has "+
			"declared Pure() and actually been attempted\n", dir)
		_, err := w.Write(b.Bytes())
		return err
	}

	for i, s := range rep.Steps {
		if i > 0 {
			b.WriteByte('\n')
		}
		formatStep(&b, s)
	}

	b.WriteByte('\n')
	formatSummary(&b, rep, dir)
	_, err := w.Write(b.Bytes())
	return err
}

func formatStep(b *bytes.Buffer, s Step) {
	line := fmt.Sprintf("%-16s %s", strings.ToUpper(string(s.Verdict)), s.Step)
	if s.Key != "" {
		line += "  key " + short(s.Key)
	}
	if s.FromRun != "" {
		line += fmt.Sprintf("  entry from run %s", s.FromRun)
	}
	if s.Hermeticity != "" {
		// On every entry, not only findings: the day entries produced under
		// real enforcement exist, the two are told apart at a glance.
		line += fmt.Sprintf("  (hermeticity: %s)", s.Hermeticity)
	}
	fmt.Fprintln(b, line)

	for _, d := range s.Differences {
		name := d.Name
		if name == "" {
			name = "-"
		}
		detail := fmt.Sprintf("  ✗ %-10s %-24s cached %s  re-run %s", d.Kind, name, d.Cached, d.Rerun)
		if d.RerunAgain != "" {
			detail += fmt.Sprintf("  again %s", d.RerunAgain)
		}
		fmt.Fprintln(b, detail)
	}

	// The declaration goes next to the finding: a mismatch is only
	// actionable beside what the step says it depends on.
	if s.Verdict == Mismatch || s.Verdict == NonDeterministic {
		if len(s.Inputs) > 0 {
			fmt.Fprintf(b, "  declared inputs   %s\n", strings.Join(s.Inputs, ", "))
		} else {
			fmt.Fprintln(b, "  declared inputs   none")
		}
		if len(s.Outputs) > 0 {
			fmt.Fprintf(b, "  declared outputs  %s\n", strings.Join(s.Outputs, ", "))
		}
	}

	// Printed for every verdict including a clean one: a reader must be able
	// to tell "this reproduced exactly" from "the part that was compared
	// reproduced exactly".
	for _, note := range s.NotCompared {
		fmt.Fprintf(b, "  ~ not compared    %s\n", wrapIndent(note, "                    ", 76))
	}

	if s.Reason != "" {
		fmt.Fprintf(b, "  %s\n", wrapIndent(s.Reason, "  ", 92))
	}
	if e := explanations[s.Verdict]; e != "" && s.Verdict != Verified {
		fmt.Fprintf(b, "  %s\n", wrapIndent(e, "  ", 92))
	}
	if s.WorkDir != "" {
		fmt.Fprintf(b, "  re-run tree       %s\n", s.WorkDir)
	}
}

func formatSummary(b *bytes.Buffer, rep Report, dir string) {
	counts := map[Verdict]int{}
	for _, s := range rep.Steps {
		counts[s.Verdict]++
	}
	order := []Verdict{Verified, Mismatch, NonDeterministic, Planned, Skipped, Errored}
	var parts []string
	for _, v := range order {
		if counts[v] > 0 {
			parts = append(parts, fmt.Sprintf("%d %s", counts[v], v))
		}
	}
	fmt.Fprintf(b, "%s of %d cached Pure() step(s) in %s\n", strings.Join(parts, ", "), rep.Checked, dir)

	if rep.Truncated > 0 {
		fmt.Fprintf(b, "%d further step(s) were not checked because of --limit; pass --limit 0 for all of them\n",
			rep.Truncated)
	}
	if !rep.Executed {
		fmt.Fprintln(b, "nothing was executed. Re-running a step that turns out not to be Pure() runs its "+
			"side effects again, which is the whole premise here, so execution is opt-in: pass --rerun.")
		return
	}
	if rep.Findings() == 0 {
		return
	}
	// The two findings mean different things and get different sentences.
	if counts[Mismatch] > 0 {
		fmt.Fprintf(b, "%d step(s) did not reproduce their cached result and are deterministic about it: "+
			"the entry a future run would be served is not what the step produces today.\n", counts[Mismatch])
	}
	if counts[NonDeterministic] > 0 {
		fmt.Fprintf(b, "%d step(s) cannot reproduce their own output twice, so their entries hold bytes a "+
			"re-run would not produce. That is worth fixing and it is not evidence about purity.\n",
			counts[NonDeterministic])
	}
}

// FormatJSON writes the report as one JSON document. Field names are the
// wire contract a script reads, so additive-only, as with `ws diff --json`.
func FormatJSON(w io.Writer, rep Report) error {
	var b bytes.Buffer
	enc := json.NewEncoder(&b)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "  ")
	if rep.Steps == nil {
		rep.Steps = []Step{}
	}
	if err := enc.Encode(rep); err != nil {
		return err
	}
	_, err := w.Write(b.Bytes())
	return err
}

// short trims a digest for display, matching `cache explain`: diagnostic,
// not an address anybody pastes.
func short(s string) string {
	const prefix = "sha256:"
	if strings.HasPrefix(s, prefix) && len(s) > len(prefix)+12 {
		return s[len(prefix) : len(prefix)+12]
	}
	return s
}

// wrapIndent breaks a long sentence at word boundaries so an explanation
// reads in an 80-column terminal.
func wrapIndent(s, indent string, width int) string {
	words := strings.Fields(s)
	if len(words) == 0 {
		return ""
	}
	var b strings.Builder
	line := words[0]
	for _, w := range words[1:] {
		if len(line)+1+len(w) > width {
			b.WriteString(line)
			b.WriteByte('\n')
			b.WriteString(indent)
			line = w
			continue
		}
		line += " " + w
	}
	b.WriteString(line)
	return b.String()
}

// SortSteps orders a report for display: findings first, then everything
// else in the order the pass produced it (plan order).
func SortSteps(rep *Report) {
	rank := map[Verdict]int{Mismatch: 0, NonDeterministic: 1, Errored: 2, Verified: 3, Planned: 4, Skipped: 5}
	sort.SliceStable(rep.Steps, func(i, j int) bool {
		return rank[rep.Steps[i].Verdict] < rank[rep.Steps[j].Verdict]
	})
}
