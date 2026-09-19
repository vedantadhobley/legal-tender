// Package flowreconciliation compares conservatively selected sender and
// receiver observations. It never combines their amounts or infers ownership.
package flowreconciliation

import (
	"github.com/vedantadhobley/legal-tender/internal/calculation/fec/disbursements"
)

const SenderPolicy = "legal-tender.fec.sender-reported-committee-flow-policy.v1"
const MatchPolicy = "legal-tender.fec.committee-flow-candidate-components.v1"
const Included = "included_sender_reported_flow"

type SenderRule struct {
	ReportingRole string `json:"reporting_role"`
	Code          string `json:"code"`
	FlowRole      string `json:"flow_role"`
}

var senderRules = []SenderRule{
	{"federal_contribution", "24K", "contribution"},
	{"federal_contribution", "24Z", "in_kind"},
	{"authorized_committee_transfer", "24G", "affiliated_transfer"},
	{"affiliated_or_party_transfer", "24G", "affiliated_transfer"},
	{"party_contribution_refund", "22Z", "refund_or_repayment"},
	{"committee_contribution_refund", "22Z", "refund_or_repayment"},
	{"loan_repayment", "20R", "refund_or_repayment"},
	{"loan_repayment", "22K", "refund_or_repayment"},
	{"other_loan_repayment", "20R", "refund_or_repayment"},
	{"other_loan_repayment", "22K", "refund_or_repayment"},
	{"loan_made", "22H", "loan"},
}

func SenderRules() []SenderRule { return append([]SenderRule(nil), senderRules...) }

type DecisionKey struct {
	State         string             `json:"state"`
	ReportingRole string             `json:"reporting_role"`
	Type          disbursements.Cell `json:"type"`
}

// EvaluateSender requires independent form-line and exact-code agreement.
// Included endpoints are reported committee/beneficiary identities, not a
// claim about the cash payee or the committee's beneficial ownership.
func EvaluateSender(in disbursements.Input) (DecisionKey, string, error) {
	report, err := disbursements.Evaluate(in)
	if err != nil {
		return DecisionKey{}, "", err
	}
	d := DecisionKey{ReportingRole: report.Role, Type: report.DisbursementType}
	switch {
	case report.Decision != disbursements.Included:
		d.State = report.Decision
	case report.SelfRecipient:
		d.State = "unresolved_self_recipient"
	case report.RecipientIdentity == "no_valid_committee_id":
		d.State = "excluded_no_committee_recipient"
	case report.RecipientIdentity != "exact_matching_committee_id":
		d.State = "unresolved_recipient_identity"
	case report.DisbursementType.Value == "24I" || report.DisbursementType.Value == "24T":
		d.State = "held_earmarked_forwarding"
	case !flowReportingRole(report.Role):
		d.State = "excluded_non_flow_reporting_role"
	case report.ConduitPresent:
		d.State = "unresolved_intermediary_evidence"
	case !report.DisbursementType.Present || report.DisbursementType.Value == "":
		d.State = "unresolved_flow_type"
	default:
		for _, rule := range senderRules {
			if rule.ReportingRole == report.Role && rule.Code == report.DisbursementType.Value {
				d.State = Included
				return d, rule.FlowRole, nil
			}
		}
		d.State = "unresolved_role_type_combination"
	}
	return d, "", nil
}

func flowReportingRole(role string) bool {
	for _, rule := range senderRules {
		if rule.ReportingRole == role {
			return true
		}
	}
	return false
}
