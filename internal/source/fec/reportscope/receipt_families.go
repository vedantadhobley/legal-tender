package reportscope

import "context"

const ReceiptFamilyCoverVersion = "legal-tender.fec.receipt-family-cover.v1"
const ReceiptFamilyMapVersion = "legal-tender.fec.receipt-family-map.v1"
const ReceiptFamilyCoverVersionV2 = "legal-tender.fec.receipt-family-cover.v2"
const RequiredItemized = "all_required_itemized"
const ThresholdedDetail = "thresholded_component_of_total"

// ReceiptFamilyField is the reviewed period-column subset, not a cash policy.
// Tests bind every position to the source map and its pinned 8.4 workbook.
type ReceiptFamilyField struct {
	ID                    string `json:"id"`
	Form                  string `json:"form"`
	Line                  string `json:"line"`
	Sequence              int    `json:"sequence"`
	MetadataField         string `json:"metadata_field"`
	DetailRelation        string `json:"detail_relation,omitempty"`
	CorroboratingSequence int    `json:"corroborating_sequence,omitempty"`
}

// ReceiptFamilyFields returns a fresh slice. Exact form names only; no aliases
// or general prefix matching that could accidentally accept F3P.
func ReceiptFamilyFields(form string) []ReceiptFamilyField {
	switch form {
	case "F3":
		return []ReceiptFamilyField{
			{"party_contributions", form, "11B", 36, "political_party_committee_contributions_period", "", 0},
			{"other_committee_contributions", form, "11C", 37, "other_political_committee_contributions_period", "", 0},
			{"authorized_transfers", form, "12", 40, "transfers_from_other_authorized_committee_period", "", 0},
			{"candidate_made_or_guaranteed_loans", form, "13A", 41, "loans_made_by_candidate_period", "", 0},
			{"other_loans", form, "13B", 42, "all_other_loans_period", "", 0},
		}
	case "F3X":
		return []ReceiptFamilyField{
			{"party_contributions", form, "11B", 33, "political_party_committee_contributions_period", "", 0},
			{"other_committee_contributions", form, "11C", 34, "other_political_committee_contributions_period", "", 0},
			{"affiliated_or_party_transfers", form, "12", 36, "transfers_from_affiliated_party_period", "", 0},
			{"loans_received", form, "13", 37, "all_loans_received_period", "", 0},
		}
	default:
		return nil
	}
}

// ReceiptFamilyFieldsV2 adds the remaining reviewed non-individual SA leaves.
// The relationship is source meaning, never inferred from observed amounts.
// Cover-only, aggregate and other-schedule leaves are deliberately not aliases.
func ReceiptFamilyFieldsV2(form string) []ReceiptFamilyField {
	fields := ReceiptFamilyFields(form)
	for i := range fields {
		fields[i].DetailRelation = RequiredItemized
	}
	switch form {
	case "F3":
		fields = append(fields,
			ReceiptFamilyField{"candidate_contributions", form, "11D", 38, "candidate_contribution_period", ThresholdedDetail, 0},
			ReceiptFamilyField{"operating_offsets", form, "14", 44, "total_offsets_to_operating_expenditures_period", ThresholdedDetail, 28},
			ReceiptFamilyField{"other_receipts", form, "15", 45, "other_receipts_period", ThresholdedDetail, 0},
		)
	case "F3X":
		fields = append(fields,
			ReceiptFamilyField{"loan_repayments_received", form, "14", 38, "loan_repayments_received_period", RequiredItemized, 0},
			ReceiptFamilyField{"operating_offsets", form, "15", 39, "offsets_to_operating_expenditures_period", ThresholdedDetail, 0},
			ReceiptFamilyField{"contribution_refunds_received", form, "16", 40, "fed_candidate_contribution_refunds_period", RequiredItemized, 0},
			ReceiptFamilyField{"other_federal_receipts", form, "17", 41, "other_fed_receipts_period", ThresholdedDetail, 0},
		)
	}
	return fields
}

func ReceiptFamilyCoverForm(form string) string {
	switch form {
	case "F3N", "F3A", "F3T":
		return "F3"
	case "F3XN", "F3XA", "F3XT":
		return "F3X"
	default:
		return ""
	}
}

// AssessReceiptFamilies preserves the complete verified original and changes
// only the named period-field projection. AssessElectronic's output stays v1.
func AssessReceiptFamilies(ctx context.Context, request Request) (ElectronicAssessment, error) {
	return assessReceiptFamilies(ctx, request, ReceiptFamilyCoverVersion, ReceiptFamilyFields)
}

func AssessReceiptFamiliesV2(ctx context.Context, request Request) (ElectronicAssessment, error) {
	return assessReceiptFamilies(ctx, request, ReceiptFamilyCoverVersionV2, ReceiptFamilyFieldsV2)
}

func assessReceiptFamilies(ctx context.Context, request Request, version string, fields func(string) []ReceiptFamilyField) (ElectronicAssessment, error) {
	a, err := AssessElectronic(ctx, request)
	if err != nil {
		return ElectronicAssessment{}, err
	}
	a.Version = version
	a.PeriodFields = []ElectronicField{}
	if a.Cover == nil {
		return a, nil
	}
	for _, spec := range fields(ReceiptFamilyCoverForm(a.Cover.Form)) {
		f := ElectronicField{Name: spec.MetadataField, Amounts: []AmountField{}}
		for _, amount := range a.Cover.Amounts {
			if amount.Sequence == spec.Sequence || amount.Sequence == spec.CorroboratingSequence {
				f.Amounts = append(f.Amounts, amount)
			}
		}
		a.PeriodFields = append(a.PeriodFields, f)
	}
	return a, nil
}
