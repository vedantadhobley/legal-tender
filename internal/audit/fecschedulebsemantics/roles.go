package fecschedulebsemantics

// reportingRole describes the form line, not the recipient or an economic
// transfer. In particular, a federal contribution can be in kind or earmarked.
// Exact line mappings follow the FEC Form 3, 3P, and 3X instructions. Unknown
// spellings and other schedules are deliberately visible for corpus review.
func reportingRole(form string, line, schedule Cell) string {
	if schedule != (Cell{Present: true, Value: "SB"}) {
		return "unresolved_schedule"
	}
	roles := formRoles[form]
	if role, ok := roles[line.Value]; line.Present && ok {
		return role
	}
	return "unresolved_form_line"
}

var formRoles = map[string]map[string]string{
	"F3": {
		"17": "operating_expenditure", "18": "authorized_committee_transfer",
		"19A": "candidate_loan_repayment", "19B": "other_loan_repayment",
		"20A": "individual_contribution_refund", "20B": "party_contribution_refund",
		"20C": "committee_contribution_refund", "21": "other_disbursement",
	},
	"F3P": {
		"23": "operating_expenditure", "24": "authorized_committee_transfer",
		"25": "fundraising_disbursement", "26": "legal_accounting_disbursement",
		"27A": "candidate_loan_repayment", "27B": "other_loan_repayment",
		"28A": "individual_contribution_refund", "28B": "party_contribution_refund",
		"28C": "committee_contribution_refund", "29": "other_disbursement",
	},
	"F3X": {
		"21B": "operating_expenditure", "22": "affiliated_or_party_transfer",
		"23": "federal_contribution", "26": "loan_repayment", "27": "loan_made",
		"28A": "individual_contribution_refund", "28B": "party_contribution_refund",
		"28C": "committee_contribution_refund", "29": "other_disbursement",
		"30B": "federal_election_activity",
	},
}

func cell(value *string) Cell {
	if value == nil {
		return Cell{}
	}
	return Cell{Present: true, Value: *value}
}

func present(value *string) bool { return value != nil && *value != "" }

func committeeID(value *string) bool {
	if value == nil || len(*value) != 9 || (*value)[0] != 'C' {
		return false
	}
	for i := 1; i < 9; i++ {
		if (*value)[i] < '0' || (*value)[i] > '9' {
			return false
		}
	}
	return true
}

func recipientIdentity(raw, clean *string) string {
	r, c := committeeID(raw), committeeID(clean)
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
