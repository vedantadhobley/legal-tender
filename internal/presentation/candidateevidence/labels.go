package candidateevidence

import (
	"html"
	"strings"
	"unicode"
)

// Labels describe existing states. They never classify a source record.
func label(code string) string {
	labels := map[string]string{
		"contribution":                                       "Reported contribution",
		"in_kind":                                            "Reported in-kind contribution",
		"affiliated_transfer":                                "Reported affiliated transfer",
		"refund_or_repayment":                                "Reported refund or repayment",
		"itemized_individual_only":                           "Individual-receipt population only",
		"committee_flow_only":                                "Committee-flow population only",
		"overlapping_individual_and_committee":               "Shared individual/committee population (count once)",
		"memo_subtotal":                                      "Memo evidence",
		"other_reported_receipt":                             "Other reported receipts",
		"unresolved":                                         "Not classified by the committee-flow role map",
		"affiliated_transfer_in":                             "Reported affiliated transfer received",
		"registered_filer_contribution":                      "Reported contribution from a registered filer",
		"registered_filer_in_kind_contribution":              "Reported in-kind contribution from a registered filer",
		"refund_or_repayment_received":                       "Reported refund or repayment received",
		"earmarked":                                          "Reported earmarked receipt",
		"outbound":                                           "Outbound transaction code (not an incoming committee flow)",
		"unknown_amount":                                     "Amount unavailable",
		"unresolved_individual_class":                        "Individual-receipt classification unavailable",
		"unresolved_recipient":                               "Recipient identity unresolved",
		"semantic_memo":                                      "Transaction-code memo evidence",
		"noncommittee_receipt":                               "Other receipt transaction code",
		"authorized":                                         "Candidate-authorized committee",
		"reported_rows_not_complete_funding":                 "Reported rows do not establish complete funding",
		"resolved_donor_identity_not_established":            "Donor identities have not been resolved",
		"no_receipt_rows_is_not_reported_zero":               "No receipt rows in this snapshot does not mean zero activity",
		"unknown_signed_amounts":                             "Some amounts are unknown; the known signed sum is not a lower bound",
		"individual_committee_predicates_overlap_do_not_add": "Individual and committee selection rules overlap; do not add their totals",
		"source_release_mismatch":                            "Summary and receipt snapshots come from different source releases",
		"receipt_reporting_period_coverage_unverified":       "Receipt reporting-period coverage has not been verified",
		"summary_account_and_report_scope_unverified":        "Summary account and report scope has not been verified",
		"no_recipient_rows_in_snapshot":                      "No receipt rows for this committee in the selected snapshot",
		"no_indexed_summary_in_snapshot":                     "No summary assertion for this committee in the selected snapshot",
		"single_assertion":                                   "One reported summary assertion",
		"equivalent_assertions":                              "Equivalent reported summary assertions",
		"conflicting_assertions":                             "Conflicting reported summary assertions; no preferred value selected",
		"source_blank":                                       "Blank in source (not zero)",
		"invalid":                                            "Invalid source value (not zero)",
		"TTL_RECEIPTS":                                       "Total reported receipts",
		"TTL_DISB":                                           "Total reported disbursements",
		"COH_BOP":                                            "Cash on hand at beginning of period",
		"COH_COP":                                            "Cash on hand at close of period",
		"INDV_ITEM_CONTB":                                    "Itemized individual contributions",
		"INDV_UNITEM_CONTB":                                  "Unitemized individual contributions",
		"INDV_CONTB":                                         "Total individual contributions",
		"TTL_FED_RECEIPTS":                                   "Total federal receipts",
		"TTL_FED_DISB":                                       "Total federal disbursements",
	}
	if text, ok := labels[code]; ok {
		return text
	}
	return "Untranslated source state: " + code
}

// Escape untrusted labels for Markdown tables/headings and HTML renderers.
// The exact original string remains in the JSON assertion.
func safe(text string) string {
	text = strings.Map(func(r rune) rune {
		if unicode.IsControl(r) || unicode.Is(unicode.Cf, r) {
			return ' '
		}
		return r
	}, text)
	text = strings.NewReplacer("\\", "\\\\", "|", "\\|", "`", "\\`", "*", "\\*", "_", "\\_", "[", "\\[", "]", "\\]", "#", "\\#", "!", "\\!").Replace(text)
	// Escape HTML last so Markdown escaping cannot split numeric entities.
	return html.EscapeString(text)
}

func money(cents string) string {
	sign := ""
	if strings.HasPrefix(cents, "-") {
		sign, cents = "-", cents[1:]
	}
	for len(cents) < 3 {
		cents = "0" + cents
	}
	dollars, fraction := cents[:len(cents)-2], cents[len(cents)-2:]
	var grouped strings.Builder
	for i, r := range dollars {
		if i > 0 && (len(dollars)-i)%3 == 0 {
			grouped.WriteByte(',')
		}
		grouped.WriteRune(r)
	}
	return sign + "$" + grouped.String() + "." + fraction
}

func display(n EntityName) string {
	switch n.State {
	case "reported_name":
		if len(n.Assertions) > 0 {
			return safe(n.Assertions[0].RawName) + " (" + safe(n.EntityID) + ")"
		}
	case "source_name_blank":
		return safe(n.EntityID) + " (name blank in source)"
	case "conflicting_reported_names":
		return safe(n.EntityID) + " (conflicting source names; see JSON)"
	case "name_source_not_requested":
		return safe(n.EntityID) + " (candidate name source not requested)"
	}
	return safe(n.EntityID) + " (no name in the pinned reference source)"
}
