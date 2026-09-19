package candidateevidence

import (
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/vedantadhobley/legal-tender/internal/calculation/fec/fundingbasis"
)

// Markdown is a bounded reading view; complete populations and assertions stay
// in the JSON. It never adds distinct receipt, summary or upstream scopes.
func Markdown(r Report) string {
	var b strings.Builder
	e := r.Evidence
	names := map[string]EntityName{}
	for _, n := range r.Names {
		names[n.EntityID] = n
	}
	name := func(id string) string {
		n, ok := names[id]
		if !ok {
			n = EntityName{EntityID: id}
		}
		return display(n)
	}
	fmt.Fprintf(&b, "# Candidate evidence: %s — %s\n\n", name(e.CandidateID), safe(e.Cycle))
	fmt.Fprintln(&b, "This report shows disclosed receipt evidence and committee connections. It is not a complete candidate-funding total or a terminal-donor allocation. All amounts are USD. Independent spending is separate and is not included here.")
	fmt.Fprintf(&b, "\n%d authorized committees; %d unresolved linked committees; %d reached committees; %d graph-cycle groups (loops, not election periods).\n", e.Overview.AuthorizedCommittees, e.Overview.UnresolvedLinkedCommittees, e.Overview.ReachedCommittees, e.Overview.CyclicComponents)
	fmt.Fprintln(&b, "\n## Candidate-boundary committee receipts\n\nSelected receiver-reported Schedule A committee observations only. Individual-only and other receipt populations are outside this table. Internal transfers are not additional external funding.\n\n| Boundary | Rows | Known signed amount |\n|---|---:|---:|")
	for _, v := range []struct {
		name, amount string
		rows         uint64
	}{
		{"External to candidate-authorized scope", e.Trace.Accounting.External.Signed, e.Trace.Accounting.External.Records},
		{"Within candidate-authorized scope", e.Trace.Accounting.Internal.Signed, e.Trace.Accounting.Internal.Records},
		{"Authorization unresolved", e.Trace.Accounting.UnresolvedScope.Signed, e.Trace.Accounting.UnresolvedScope.Records},
	} {
		fmt.Fprintf(&b, "| %s | %d | %s |\n", v.name, v.rows, money(v.amount))
	}
	fmt.Fprintln(&b, "\n## Candidate-linked receipt populations\n\nDo not add this table to the boundary table: they overlap. These are reporting populations, not a qualified cash ledger. Signed sums preserve negative adjustments; unknown amounts are not zero.\n\n“Not classified by the committee-flow role map” describes a role label only. It does not mean that the amount or donor is missing; ordinary individual receipts can have this label.")
	for _, c := range e.Committees {
		if c.Authorization != "authorized" && c.Authorization != "unresolved" {
			continue
		}
		authorization := "Candidate-authorized committee"
		if c.Authorization == "unresolved" {
			authorization = "Candidate authorization unresolved"
		}
		fmt.Fprintf(&b, "\n### %s\n\n%s.\n", name(c.CommitteeID), authorization)
		populationTable(&b, c.Receipts.Components, false)
		populationTable(&b, c.Receipts.Components, true)
		fmt.Fprintln(&b, "\nCoverage limits:")
		for _, reason := range c.Coverage {
			fmt.Fprintf(&b, "\n- %s.\n", safe(label(reason)))
		}
		if c.Summary == nil {
			fmt.Fprintln(&b, "\nSummary context was not requested.")
			continue
		}
		s := c.Summary
		fmt.Fprintf(&b, "\n#### Separately reported summary context\n\n%s. These values do not replace receipt detail and are not added to it.\n\nComparison remains blocked:\n", safe(label(s.SummaryState)))
		for _, reason := range s.ComparisonBlocks {
			fmt.Fprintf(&b, "\n- %s.\n", safe(label(reason)))
		}
		for _, a := range s.Assertions {
			start, end := "unknown", "unknown"
			if a.CoverageStart.Value != nil {
				start = *a.CoverageStart.Value
			}
			if a.CoverageEnd.Value != nil {
				end = *a.CoverageEnd.Value
			}
			fmt.Fprintf(&b, "\nReported coverage: %s through %s. Assertion `%s`.\n\n| Summary field | Reported value |\n|---|---:|\n", safe(start), safe(end), a.AssertionID)
			for _, f := range a.Fields {
				value := safe(label(f.Value.State))
				if f.Value.MinorUnits != nil {
					value = money(*f.Value.MinorUnits)
				}
				fmt.Fprintf(&b, "| %s | %s |\n", safe(label(f.Field)), value)
			}
		}
	}
	fmt.Fprintln(&b, "\n## Concrete upstream connection examples\n\nExamples are selected by hop distance, then committee ID: one example at each of the first three available distances. They are not ranked donors and are not an exhaustive list of paths. Every hop below is an existing source observation. Amounts belong to separate hops: do not add them, take their minimum, or attribute them to the first committee.\n\nConnected hops do not prove that the same dollars moved through them. Reported dates are not a cash-availability test, even when dates increase.")
	if len(r.Paths) == 0 {
		fmt.Fprintln(&b, "\nNo upstream witness paths in this selected trace; this does not establish a terminal donor.")
	}
	for i, p := range r.Paths {
		fmt.Fprintf(&b, "\n### Example %d: %d reported hops\n\n%s → %s.\n\n| From | To | Reported date | Signed amount | Reported role | Source row / SUB_ID |\n|---|---|---|---:|---|---|\n", i+1, len(p.Hops), name(p.From), name(p.To))
		for _, h := range p.Hops {
			date := "Unknown in source"
			if h.Date != nil {
				date = time.Unix(int64(*h.Date)*86400, 0).UTC().Format("2006-01-02")
			}
			fmt.Fprintf(&b, "| %s | %s | %s | %s | %s | %d / %s |\n", name(h.Sender), name(h.Recipient), date, money(fmt.Sprint(h.Amount)), safe(label(h.Role)), h.Ordinal, safe(h.SubID))
		}
		if p.DateReversal {
			fmt.Fprintln(&b, "\nDate warning: a later hop has an earlier reported date. This is a connectivity example, not a chronological money trail.")
		}
		if p.MissingDates {
			fmt.Fprintln(&b, "\nDate warning: at least one reported date is missing.")
		}
		if p.SameDay {
			fmt.Fprintln(&b, "\nDate warning: consecutive known dates include the same day; within-day order is unknown.")
		}
		if p.Nonpositive {
			fmt.Fprintln(&b, "\nAmount warning: this path includes a zero or negative observation; it is not a positive-cash chain.")
		}
		fmt.Fprintln(&b, "\nAllocated path amount: unknown, not zero.")
	}
	fmt.Fprintf(&b, "\n## What remains unresolved\n\n%d reached committees lack a same-cycle master record; %d have no receipt inventory rows in this snapshot. Neither condition makes a committee a terminal source. Names do not resolve missing registration history or donor/corporate identity.\n\nThe JSON retains all %d shortest-hop witnesses and %d upstream source-row references, including cyclic membership. Terminal-source definition and dollar-allocation policy are not selected. Terminal amount: unknown, not zero.\n", e.Overview.ReachedWithoutMaster, e.Overview.ReachedWithoutReceipts, len(e.Witnesses), len(e.Trace.UpstreamOrdinals))
	fmt.Fprintln(&b, "\n## Sources and reproduction\n\nNames are exact reported labels, not new identity matches. Committee names use the trace's exact committee-master facts. The optional candidate-name snapshot is display context only; it may be from a different release and never changes authorization or amounts. Blank, conflicting and missing names remain explicit in JSON.")
	fmt.Fprintf(&b, "\nReport `%s`; core evidence `%s`; executable SHA-256 `%s`.\n\nReceipt inventory `%s`; Schedule A facts `%s`; observation bundle `%s`.\n\nCommittee-name facts `%s`, manifest SHA-256 `%s`.\n", r.ReportID, e.ResultID, e.ExecutableSHA256, e.InventoryID, e.ReceiptInput.FactSetID, e.Trace.Inputs.BundleID, r.CommitteeNames.FactSetID, r.CommitteeNames.ManifestSHA256)
	if r.CandidateNames != nil {
		fmt.Fprintf(&b, "\nCandidate-name facts `%s`, manifest SHA-256 `%s`.\n", r.CandidateNames.FactSetID, r.CandidateNames.ManifestSHA256)
	}
	fmt.Fprintln(&b, "\nThe JSON pins source releases, name fact/occurrence IDs, calculation policies, dates, sign populations and scope blockers. Source row ordinals refer to that exact Schedule A fact set. Use `inspect-funding-receipts` with the pinned inventory and committee ID to page contributor, employer, conduit, memo and filing evidence. Retain these inputs and this executable to reproduce the report.")
	return b.String()
}

func populationTable(b *strings.Builder, rows []fundingbasis.ReceiptRoleCoverage, memo bool) {
	selected := []fundingbasis.ReceiptRoleCoverage{}
	for _, p := range rows {
		if (p.Component == "memo_subtotal") == memo {
			selected = append(selected, p)
		}
	}
	if len(selected) == 0 {
		return
	}
	if memo {
		fmt.Fprintln(b, "\n#### Memo evidence — separate, not additional funding")
	}
	fmt.Fprintln(b, "\n| Population | Reported role | Rows | Known signed amount | Unknown amount rows |\n|---|---|---:|---:|---:|")
	for _, p := range selected {
		fmt.Fprintf(b, "| %s | %s | %d | %s | %d |\n", safe(label(p.Component)), safe(label(p.Role)), p.Measures.Rows, money(fmt.Sprint(p.Measures.Signed)), p.Measures.Unknown)
	}
}

func WriteMarkdown(w io.Writer, r Report) error {
	text := Markdown(r)
	n, err := io.WriteString(w, text)
	if err == nil && n != len(text) {
		return io.ErrShortWrite
	}
	return err
}
