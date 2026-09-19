// Package committeeflows defines the accepted Schedule A membership and
// routing policy for receiver-reported committee-to-committee money flows.
package committeeflows

import "regexp"

const (
	ContractID      = "fec/receiver-reported-committee-flows"
	ContractVersion = "1.0.0"
	PolicyVersion   = "legal-tender.fec.receiver-reported-committee-flow-policy.v1"

	IdentityExactMatching = "exact_matching_committee_id"
	IdentityNoValid       = "no_valid_committee_id"
	IdentityRawOnly       = "raw_committee_id_only"
	IdentityCleanOnly     = "clean_committee_id_only"
	IdentityConflicting   = "conflicting_committee_ids"

	DecisionInvalidNormalization = "invalid_normalization"
	DecisionUnresolvedRecipient  = "unresolved_recipient_committee_id"
	DecisionExcludedNoSource     = "excluded_no_source_committee_id"
	DecisionUnresolvedOneSided   = "unresolved_one_sided_source_committee_id"
	DecisionUnresolvedConflict   = "unresolved_conflicting_source_committee_ids"
	DecisionExcludedMemo         = "excluded_memo_subtotal"
	DecisionUnresolvedAmount     = "unresolved_amount"
	DecisionExcludedOutbound     = "excluded_outbound_receipt_role"
	DecisionExcludedSemanticMemo = "excluded_semantic_memo_receipt_role"
	DecisionExcludedEarmarked    = "excluded_earmarked_receipt_role"
	DecisionExcludedNoncommittee = "excluded_noncommittee_receipt_role"
	DecisionUnresolvedRole       = "unresolved_receipt_role"
	DecisionIncluded             = "included_receiver_reported_committee_flow"

	RoleRegisteredFilerContribution = "registered_filer_contribution"
	RoleRegisteredFilerInKind       = "registered_filer_in_kind_contribution"
	RoleAffiliatedTransferIn        = "affiliated_transfer_in"
	RoleRefundOrRepaymentReceived   = "refund_or_repayment_received"
	RoleOutbound                    = "outbound"
	RoleSemanticMemo                = "semantic_memo"
	RoleEarmarked                   = "earmarked"
	RoleNoncommitteeReceipt         = "noncommittee_receipt"
	RoleUnresolved                  = "unresolved"
)

var committeeIDPattern = regexp.MustCompile(`^C[0-9]{8}$`)

// ReceiptTypeRule is one immutable exact-code routing rule.
type ReceiptTypeRule struct {
	Role     string   `json:"role"`
	Decision string   `json:"decision"`
	Codes    []string `json:"codes"`
}

var receiptTypeRules = []ReceiptTypeRule{
	{Role: RoleRegisteredFilerContribution, Decision: DecisionIncluded, Codes: []string{"15K", "18K", "30K", "31K", "32K"}},
	{Role: RoleRegisteredFilerInKind, Decision: DecisionIncluded, Codes: []string{"15Z"}},
	{Role: RoleAffiliatedTransferIn, Decision: DecisionIncluded, Codes: []string{"18G", "30G", "31G", "32G"}},
	{Role: RoleRefundOrRepaymentReceived, Decision: DecisionIncluded, Codes: []string{"20R", "20Y", "22Z"}},
	{Role: RoleOutbound, Decision: DecisionExcludedOutbound, Codes: []string{"24G", "24I", "24K", "24T", "24Z"}},
	{Role: RoleSemanticMemo, Decision: DecisionExcludedSemanticMemo, Codes: []string{"10J", "11J", "15J", "18J", "30F", "30J", "31F", "31J", "32F", "32J"}},
	{Role: RoleEarmarked, Decision: DecisionExcludedEarmarked, Codes: []string{"15E", "30E", "31E", "32E"}},
	{Role: RoleNoncommitteeReceipt, Decision: DecisionExcludedNoncommittee, Codes: []string{"10", "11", "12", "16C", "30", "31", "32"}},
}

var receiptTypeIndex = func() map[string]ReceiptTypeRule {
	result := make(map[string]ReceiptTypeRule)
	for _, rule := range receiptTypeRules {
		for _, code := range rule.Codes {
			result[code] = rule
		}
	}
	return result
}()

// EvaluationInput is the minimal Schedule A projection needed to decide
// receiver-reported committee-flow membership.
type EvaluationInput struct {
	NormalizationState     string
	RecipientCommitteeID   *string
	ContributorID          *string
	CleanContributorID     *string
	MemoedSubtotal         bool
	AmountObservationState string
	AmountMinorUnits       *int64
	ReceiptTypeCode        *string
}

// Evaluation is one terminal membership decision and, when included, the
// exact source identity and receipt role used to form a result key.
type Evaluation struct {
	Decision          string
	SourceCommitteeID string
	ReceiptRole       string
}

// ValidCommitteeID reports whether value is an exact FEC committee ID.
func ValidCommitteeID(value *string) bool {
	return value != nil && committeeIDPattern.MatchString(*value)
}

// ClassifySourceIdentity requires the raw and publisher-cleaned contributor
// IDs to agree. One-sided or conflicting IDs remain explicit exceptions.
func ClassifySourceIdentity(raw, clean *string) (state, committeeID string) {
	rawValid := ValidCommitteeID(raw)
	cleanValid := ValidCommitteeID(clean)
	switch {
	case rawValid && cleanValid && *raw == *clean:
		return IdentityExactMatching, *raw
	case rawValid && cleanValid:
		return IdentityConflicting, ""
	case rawValid:
		return IdentityRawOnly, *raw
	case cleanValid:
		return IdentityCleanOnly, *clean
	default:
		return IdentityNoValid, ""
	}
}

// ClassifyReceiptRole maps the FEC's exact Schedule A receipt-type code to a
// money-flow role and terminal membership decision. Unknown codes remain
// unresolved; names, entity types, and is_individual never override this map.
func ClassifyReceiptRole(code *string) (role, decision string) {
	if code == nil || *code == "" {
		return RoleUnresolved, DecisionUnresolvedRole
	}
	if rule, exists := receiptTypeIndex[*code]; exists {
		return rule.Role, rule.Decision
	}
	return RoleUnresolved, DecisionUnresolvedRole
}

// ReceiptTypeRules returns a defensive copy for calculation manifests.
func ReceiptTypeRules() []ReceiptTypeRule {
	result := make([]ReceiptTypeRule, len(receiptTypeRules))
	for index, rule := range receiptTypeRules {
		result[index] = rule
		result[index].Codes = append([]string(nil), rule.Codes...)
	}
	return result
}

// DecisionDisposition maps every terminal decision to its conservation bucket.
func DecisionDisposition(decision string) string {
	switch decision {
	case DecisionIncluded:
		return "included"
	case DecisionExcludedNoSource, DecisionExcludedMemo, DecisionExcludedOutbound,
		DecisionExcludedSemanticMemo, DecisionExcludedEarmarked, DecisionExcludedNoncommittee:
		return "excluded"
	case DecisionInvalidNormalization, DecisionUnresolvedRecipient, DecisionUnresolvedOneSided,
		DecisionUnresolvedConflict, DecisionUnresolvedAmount, DecisionUnresolvedRole:
		return "unresolved"
	default:
		return "unknown"
	}
}

// Evaluate applies the contract's ordered terminal-decision policy.
func Evaluate(input EvaluationInput) Evaluation {
	identity, sourceID := ClassifySourceIdentity(input.ContributorID, input.CleanContributorID)
	decision := DecisionIncluded
	switch {
	case input.NormalizationState != "valid":
		decision = DecisionInvalidNormalization
	case !ValidCommitteeID(input.RecipientCommitteeID):
		decision = DecisionUnresolvedRecipient
	case identity == IdentityNoValid:
		decision = DecisionExcludedNoSource
	case identity == IdentityRawOnly || identity == IdentityCleanOnly:
		decision = DecisionUnresolvedOneSided
	case identity == IdentityConflicting:
		decision = DecisionUnresolvedConflict
	case input.MemoedSubtotal:
		decision = DecisionExcludedMemo
	case input.AmountObservationState != "reported_value" || input.AmountMinorUnits == nil:
		decision = DecisionUnresolvedAmount
	default:
		role, roleDecision := ClassifyReceiptRole(input.ReceiptTypeCode)
		return Evaluation{Decision: roleDecision, SourceCommitteeID: sourceID, ReceiptRole: role}
	}
	return Evaluation{Decision: decision, SourceCommitteeID: sourceID}
}
