package fundingbasis

import (
	"fmt"
	"io"
	"strings"
)

// CandidateEvidenceMarkdown presents the same pinned result without pretending
// that reporting-population subtotals are complete candidate funding.
func CandidateEvidenceMarkdown(r CandidateEvidence) string {
	var b strings.Builder
	fmt.Fprintf(&b, "# Candidate evidence: %s — %s\n\n", r.CandidateID, r.Cycle)
	fmt.Fprintf(&b, "Result `%s`\n\n", r.ResultID)
	fmt.Fprintln(&b, "Reported observations, not complete candidate funding or terminal-source attribution. All amounts below are USD; positive, negative, unknown and memo populations remain separate in the JSON result.")
	fmt.Fprintf(&b, "\n%d authorized committees; %d unresolved linked committees; %d reached committees; %d cyclic groups.\n\n",
		r.Overview.AuthorizedCommittees, r.Overview.UnresolvedLinkedCommittees, r.Overview.ReachedCommittees, r.Overview.CyclicComponents)
	fmt.Fprintln(&b, "## Candidate-boundary committee receipts\n\nThese are the selected Schedule A committee observations only. They exclude individual-only and other receipt populations. Internal transfers are not additional external funding.\n\n| Boundary | Rows | Known signed amount |\n|---|---:|---:|")
	for _, v := range []struct {
		name, amount string
		rows         uint64
	}{
		{"External to authorized scope", r.Trace.Accounting.External.Signed, r.Trace.Accounting.External.Records},
		{"Within authorized scope", r.Trace.Accounting.Internal.Signed, r.Trace.Accounting.Internal.Records},
		{"Unresolved authorization boundary", r.Trace.Accounting.UnresolvedScope.Signed, r.Trace.Accounting.UnresolvedScope.Records},
	} {
		fmt.Fprintf(&b, "| %s | %d | %s |\n", v.name, v.rows, decimalCents(v.amount))
	}
	fmt.Fprintln(&b, "\n## Candidate-linked receipt evidence\n\nThese disjoint reporting components include memo evidence and other unqualified populations. Do not add them to the boundary table or treat their sum as candidate cash. An empty population is not a reported zero.")
	for _, c := range r.Committees {
		if c.Authorization != "authorized" && c.Authorization != "unresolved" {
			continue
		}
		fmt.Fprintf(&b, "\n### %s (%s)\n\n", c.CommitteeID, c.Authorization)
		fmt.Fprintln(&b, "| Component | Reported role | Rows | Known signed amount | Unknown amount rows |\n|---|---|---:|---:|---:|")
		for _, p := range c.Receipts.Components {
			fmt.Fprintf(&b, "| %s | %s | %d | %s | %d |\n", p.Component, p.Role, p.Measures.Rows, decimalCents(fmt.Sprint(p.Measures.Signed)), p.Measures.Unknown)
		}
		if len(c.Receipts.Components) == 0 {
			fmt.Fprintln(&b, "\nNo receipt rows in this exact snapshot; activity is unknown.")
		}
		fmt.Fprintf(&b, "\nCoverage limits: %s.\n", strings.Join(c.Coverage, "; "))
		if c.Summary == nil {
			fmt.Fprintln(&b, "\nSummary context was not requested.")
			continue
		}
		s := c.Summary
		fmt.Fprintf(&b, "\nSummary context: `%s`; `%s`. Comparison blockers: %s.\n", s.SummaryState, s.SourceAlignment, strings.Join(s.ComparisonBlocks, "; "))
		for _, a := range s.Assertions {
			start, end := "unknown", "unknown"
			if a.CoverageStart.Value != nil {
				start = *a.CoverageStart.Value
			}
			if a.CoverageEnd.Value != nil {
				end = *a.CoverageEnd.Value
			}
			fmt.Fprintf(&b, "\nAssertion `%s`, reported coverage %s through %s.\n\n| Reported summary field | Amount or source state |\n|---|---:|\n", a.AssertionID, start, end)
			for _, f := range a.Fields {
				value := f.Value.State
				if f.Value.MinorUnits != nil {
					value = decimalCents(*f.Value.MinorUnits)
				}
				fmt.Fprintf(&b, "| %s | %s |\n", f.Field, value)
			}
		}
	}
	fmt.Fprintln(&b, "\n## Upstream connections and gaps")
	fmt.Fprintf(&b, "\nThe full JSON contains %d candidate-boundary observations, %d upstream source-row references, and %d source-backed shortest-hop witnesses. Each witness connects a committee to a node one hop closer to authorized scope; following these witnesses reaches a root without an arbitrary depth cutoff. Other paths and cycles remain in the complete pinned membership. These paths establish connectivity, not whose dollars funded a payment.\n",
		len(r.Trace.CandidateObservations), len(r.Trace.UpstreamOrdinals), len(r.Witnesses))
	fmt.Fprintf(&b, "\n%d reached committees lack a same-cycle master; %d lack receipt inventory rows. Neither condition establishes a terminal source. Upstream receipt populations are available per committee in the JSON and are never summed into candidate funding.\n",
		r.Overview.ReachedWithoutMaster, r.Overview.ReachedWithoutReceipts)
	fmt.Fprintln(&b, "\n## Source drilldown and reproduction")
	fmt.Fprintf(&b, "\nReceipt inventory `%s`; Schedule A fact set `%s`; observation bundle `%s`; executable SHA-256 `%s`. The JSON pins all component policies, manifests and source-release identities. Retain those inputs and the executable/build evidence to reproduce this answer.\n",
		r.InventoryID, r.ReceiptInput.FactSetID, r.Trace.Inputs.BundleID, r.ExecutableSHA256)
	fmt.Fprintln(&b, "\nUse `inspect-funding-receipts` with this exact inventory and a committee ID to page full contributor, conduit, employer, date, memo and filing evidence. Connection witness/source ordinals refer to the pinned Schedule A fact set, not to an unversioned latest graph.\n\nTerminal policy: not selected. Allocation policy: not selected. Terminal amount: unknown, not zero. Independent spending is not included in these receipt amounts.")
	return b.String()
}

// Decimal formatting uses strings only, including sums larger than int64.
func decimalCents(v string) string {
	sign := ""
	if strings.HasPrefix(v, "-") {
		sign, v = "-", v[1:]
	}
	for len(v) < 3 {
		v = "0" + v
	}
	return sign + "$" + v[:len(v)-2] + "." + v[len(v)-2:]
}

// WriteCandidateEvidenceReport propagates output failures instead of reporting
// a successful readable artifact after a short write.
func WriteCandidateEvidenceReport(w io.Writer, r CandidateEvidence) error {
	text := CandidateEvidenceMarkdown(r)
	n, err := io.WriteString(w, text)
	if err == nil && n != len(text) {
		return io.ErrShortWrite
	}
	return err
}
