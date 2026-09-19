// Package disbursements calculates processed Schedule B reporting subtotals.
// Reporting roles never establish cash movement, beneficial ownership, or
// candidate-controlled receipts.
package disbursements

import "fmt"

const PolicyVersion = "legal-tender.fec.processed-disbursement-reporting-policy.v1"

const (
	Included   = "included_non_memo_itemized_disbursement"
	Memo       = "excluded_memo_subtotal"
	Separate   = "separate_reporting_scope"
	Unresolved = "unresolved_reporting"
)

// Cell preserves source NULL separately from empty text.
type Cell struct {
	Present bool   `json:"present"`
	Value   string `json:"value"`
}

func cell(value *string) Cell {
	if value == nil {
		return Cell{}
	}
	return Cell{true, *value}
}
func present(value *string) bool { return value != nil && *value != "" }

type LineRule struct {
	Form  string `json:"form"`
	Line  string `json:"line"`
	Scope string `json:"scope"`
	Role  string `json:"role"`
}

// LineRules returns a defensive, deterministic copy of the reviewed map.
func LineRules() []LineRule { return append([]LineRule(nil), lineRules...) }

var lineRules = []LineRule{
	{"F3", "17", "regular_committee", "operating_expenditure"},
	{"F3", "18", "regular_committee", "authorized_committee_transfer"},
	{"F3", "19A", "regular_committee", "candidate_loan_repayment"},
	{"F3", "19B", "regular_committee", "other_loan_repayment"},
	{"F3", "20A", "regular_committee", "individual_contribution_refund"},
	{"F3", "20B", "regular_committee", "party_contribution_refund"},
	{"F3", "20C", "regular_committee", "committee_contribution_refund"},
	{"F3", "21", "regular_committee", "other_disbursement"},
	{"F3P", "23", "regular_committee", "operating_expenditure"},
	{"F3P", "24", "regular_committee", "authorized_committee_transfer"},
	{"F3P", "25", "regular_committee", "fundraising_disbursement"},
	{"F3P", "26", "regular_committee", "legal_accounting_disbursement"},
	{"F3P", "27A", "regular_committee", "candidate_loan_repayment"},
	{"F3P", "27B", "regular_committee", "other_loan_repayment"},
	{"F3P", "28A", "regular_committee", "individual_contribution_refund"},
	{"F3P", "28B", "regular_committee", "party_contribution_refund"},
	{"F3P", "28C", "regular_committee", "committee_contribution_refund"},
	{"F3P", "29", "regular_committee", "other_disbursement"},
	{"F3X", "21B", "regular_committee", "operating_expenditure"},
	{"F3X", "22", "regular_committee", "affiliated_or_party_transfer"},
	{"F3X", "23", "regular_committee", "federal_contribution"},
	{"F3X", "26", "regular_committee", "loan_repayment"},
	{"F3X", "27", "regular_committee", "loan_made"},
	{"F3X", "28A", "regular_committee", "individual_contribution_refund"},
	{"F3X", "28B", "regular_committee", "party_contribution_refund"},
	{"F3X", "28C", "regular_committee", "committee_contribution_refund"},
	{"F3X", "29", "regular_committee", "other_disbursement"},
	{"F3X", "30B", "regular_committee", "federal_election_activity"},
	{"F4", "21A", "convention", "convention_expenditure"},
	{"F4", "22", "convention", "affiliated_transfer"},
	{"F4", "23A", "convention", "loan_made"},
	{"F4", "23B", "convention", "loan_repayment"},
	{"F4", "24A", "convention", "other_disbursement"},
	{"F9", "F93", "electioneering_notice", "electioneering_disbursement"},
	{"F3X", "SL4A", "levin", "levin_transfer"},
	{"F3X", "SL4C", "levin", "levin_transfer"},
	{"F3X", "SL4D", "levin", "levin_transfer"},
	{"F3X", "SL5", "levin", "levin_other_disbursement"},
	{"F3X", "24", "independent_expenditure_reported_on_sb", "independent_expenditure"},
}

var lineIndex = func() map[[2]string]LineRule {
	index := make(map[[2]string]LineRule, len(lineRules))
	for _, rule := range lineRules {
		index[[2]string{rule.Form, rule.Line}] = rule
	}
	return index
}()

type Input struct {
	Sender, RawRecipient, CleanRecipient       *string
	Form                                       string
	Line, Schedule, MemoCode, DisbursementType *string
	BeneficiaryName, ConduitName               *string
	Memoed                                     bool
	AmountState                                string
	Amount                                     *int64
}

// Key is the reproducible per-fact membership predicate and result grouping.
// Recipient names and IDs stay in source facts; this is not a transfer edge.
type Key struct {
	Sender             Cell   `json:"sender"`
	Form               string `json:"form"`
	Line               Cell   `json:"line"`
	Schedule           Cell   `json:"schedule"`
	Scope              string `json:"scope"`
	Role               string `json:"role"`
	Decision           string `json:"decision"`
	Reason             string `json:"reason"`
	RecipientIdentity  string `json:"recipient_identity"`
	SelfRecipient      bool   `json:"self_recipient"`
	DisbursementType   Cell   `json:"disbursement_type"`
	BeneficiaryPresent bool   `json:"beneficiary_present"`
	ConduitPresent     bool   `json:"conduit_present"`
}

func Evaluate(in Input) (Key, error) {
	if in.Memoed != (in.MemoCode != nil && *in.MemoCode == "X") {
		return Key{}, fmt.Errorf("memo normalization mismatch")
	}
	if (in.AmountState == "reported_value") != (in.Amount != nil) || (in.AmountState != "reported_value" && in.AmountState != "source_null") {
		return Key{}, fmt.Errorf("amount normalization mismatch")
	}
	k := Key{Sender: cell(in.Sender), Form: in.Form, Line: cell(in.Line), Schedule: cell(in.Schedule), Scope: "unresolved", Role: "unresolved", Decision: Unresolved, RecipientIdentity: recipientIdentity(in.RawRecipient, in.CleanRecipient), DisbursementType: cell(in.DisbursementType), BeneficiaryPresent: present(in.BeneficiaryName), ConduitPresent: present(in.ConduitName)}
	k.SelfRecipient = validCommittee(in.Sender) && ((validCommittee(in.RawRecipient) && *in.Sender == *in.RawRecipient) || (validCommittee(in.CleanRecipient) && *in.Sender == *in.CleanRecipient))
	rule, found := lineIndex[[2]string{in.Form, k.Line.Value}]
	if in.Line != nil && found && k.Schedule == (Cell{true, "SB"}) {
		k.Scope, k.Role = rule.Scope, rule.Role
	}
	switch {
	case in.Memoed:
		k.Decision, k.Reason = Memo, "memo_code_x"
	case in.Amount == nil:
		k.Reason = "missing_amount"
	case k.Schedule != (Cell{true, "SB"}):
		k.Reason = "unreviewed_schedule"
	case !found || in.Line == nil:
		k.Reason = "unreviewed_form_line"
	case k.Scope != "regular_committee":
		k.Decision, k.Reason = Separate, "distinct_reporting_scope"
	case !validCommittee(in.Sender):
		k.Reason = "invalid_filer_id"
	default:
		k.Decision, k.Reason = Included, "reviewed_regular_form_line"
	}
	return k, nil
}

func validCommittee(v *string) bool {
	if v == nil || len(*v) != 9 || (*v)[0] != 'C' {
		return false
	}
	for i := 1; i < 9; i++ {
		if (*v)[i] < '0' || (*v)[i] > '9' {
			return false
		}
	}
	return true
}
func recipientIdentity(raw, clean *string) string {
	r, c := validCommittee(raw), validCommittee(clean)
	switch {
	case r && c && *raw == *clean:
		return "exact_matching_committee_id"
	case r && c:
		return "conflicting_committee_ids"
	case r:
		return "raw_committee_id_only"
	case c:
		return "clean_committee_id_only"
	default:
		return "no_valid_committee_id"
	}
}
