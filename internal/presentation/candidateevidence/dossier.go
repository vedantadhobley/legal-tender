package candidateevidence

import (
	"context"
	"fmt"
	"math/big"
	"sort"

	"github.com/vedantadhobley/legal-tender/internal/calculation/fec/candidateresolution"
	"github.com/vedantadhobley/legal-tender/internal/calculation/fec/fundingbasis"
)

const DossierVersion = "legal-tender.fec.candidate-dossier.v1"
const DossierPolicy = "fec/candidate-dossier@1.0.0"

const (
	OutsideReportedEndpoint               = "reported_endpoint"
	OutsideSafeDefaultEndpoint            = "safe_default_endpoint"
	OutsideInferredAlternativeEndpoint    = "inferred_alternative_endpoint"
	OutsideConflictingAlternativeEndpoint = "conflicting_alternative_endpoint"
)

type DossierInput struct {
	ParentReportID                       string      `json:"parent_report_id"`
	ParentReportSHA256                   string      `json:"parent_report_sha256"`
	ParentReportVerification             string      `json:"parent_report_verification"`
	ParentEvidenceID                     string      `json:"parent_evidence_id"`
	CandidateNameSource                  *NameSource `json:"candidate_name_source"`
	ReceiptFactSetID                     string      `json:"receipt_fact_set_id"`
	ReceiptManifestSHA256                string      `json:"receipt_manifest_sha256"`
	ReceiptFECSourceReleaseID            string      `json:"receipt_fec_source_release_id"`
	CommitteeFlowCalculationSetID        string      `json:"committee_flow_calculation_set_id"`
	CommitteeFlowReconciliationReleaseID string      `json:"committee_flow_reconciliation_release_id"`
	CandidateInterpretationSetID         string      `json:"candidate_interpretation_set_id"`
	CandidateInterpretationManifestSHA   string      `json:"candidate_interpretation_manifest_sha256"`
	CandidateInterpretationArtifactSHA   string      `json:"candidate_interpretation_artifact_sha256"`
	CandidateInterpretationVerification  string      `json:"candidate_interpretation_verification"`
	IndependentSpendingFECReleaseID      string      `json:"independent_spending_fec_source_release_id"`
	SourceReleaseAlignment               string      `json:"source_release_alignment"`
}

type ReceiptEvidence struct {
	State                     string                           `json:"state"`
	SummaryContextState       string                           `json:"summary_context_state"`
	Overview                  fundingbasis.EvidenceOverview    `json:"overview"`
	CandidateLinkedCommittees []fundingbasis.EvidenceCommittee `json:"candidate_linked_committees"`
	Limitations               []string                         `json:"limitations"`
}

type AmountMeasures struct {
	Rows               uint64 `json:"rows"`
	PositiveRows       uint64 `json:"positive_rows"`
	NegativeRows       uint64 `json:"negative_rows"`
	ZeroRows           uint64 `json:"zero_rows"`
	SignedMinorUnits   string `json:"signed_minor_units"`
	PositiveMinorUnits string `json:"positive_minor_units"`
	NegativeMinorUnits string `json:"negative_minor_units"`
}

type StanceMeasures struct {
	Total      AmountMeasures `json:"total"`
	Supporting AmountMeasures `json:"supporting"`
	Opposing   AmountMeasures `json:"opposing"`
}

type OutsideSpendingView struct {
	Role         string         `json:"candidate_role"`
	FinancialUse string         `json:"financial_use"`
	Additive     bool           `json:"additive_with_other_views"`
	Measures     StanceMeasures `json:"measures"`
}

type OutsideSpendingEvidence struct {
	CandidateRoles []string                                    `json:"candidate_roles"`
	Interpretation candidateresolution.CandidateInterpretation `json:"interpretation"`
}

type OutsideSpendingEvidenceSet struct {
	State               string                                    `json:"state"`
	ViewsAreNonAdditive bool                                      `json:"views_are_non_additive"`
	RelevantRows        uint64                                    `json:"relevant_rows"`
	Views               []OutsideSpendingView                     `json:"views"`
	Evidence            []OutsideSpendingEvidence                 `json:"evidence"`
	GlobalCounts        candidateresolution.InterpretationCounts  `json:"global_interpretation_counts"`
	GlobalAmounts       candidateresolution.InterpretationAmounts `json:"global_interpretation_amounts"`
}

type Dossier struct {
	SchemaVersion      string                     `json:"schema_version"`
	DossierID          string                     `json:"dossier_id"`
	Policy             string                     `json:"policy"`
	ExecutableSHA256   string                     `json:"executable_sha256"`
	CandidateID        string                     `json:"candidate_id"`
	Cycle              string                     `json:"cycle"`
	State              string                     `json:"state"`
	CandidateName      EntityName                 `json:"candidate_name"`
	Inputs             DossierInput               `json:"inputs"`
	Receipts           ReceiptEvidence            `json:"candidate_linked_receipt_evidence"`
	OutsideSpending    OutsideSpendingEvidenceSet `json:"independent_expenditure_evidence"`
	ConnectionExamples []PathExample              `json:"committee_connection_examples"`
	TerminalPolicy     *string                    `json:"terminal_policy"`
	AllocationPolicy   *string                    `json:"allocation_policy"`
	TerminalAmount     *string                    `json:"terminal_amount_minor_units"`
	TerminalEligible   bool                       `json:"terminal_attribution_eligible"`
	Limitations        []string                   `json:"limitations"`
}

type amountAccumulator struct {
	rows, positiveRows, negativeRows, zeroRows uint64
	signed, positive, negative                 big.Int
}

type stanceAccumulator struct {
	total, supporting, opposing amountAccumulator
}

// BuildDossier creates a compact, read-only candidate view from two exact
// retained publications. It does not recalculate receipts, alter candidate
// identity, combine independent expenditures with receipts, or allocate money.
func BuildDossier(ctx context.Context, storageRoot, reportPath, expectedReportID, interpretationManifestPath, executable string) (Dossier, error) {
	if storageRoot == "" || interpretationManifestPath == "" || !digest(executable) {
		return Dossier{}, fmt.Errorf("storage root, candidate interpretations and executable digest required")
	}
	report, reportSHA, err := openReport(ctx, reportPath, expectedReportID)
	if err != nil {
		return Dossier{}, err
	}
	candidateName, err := dossierCandidateName(report)
	if err != nil {
		return Dossier{}, err
	}

	views := map[string]*stanceAccumulator{
		OutsideReportedEndpoint:               {},
		OutsideSafeDefaultEndpoint:            {},
		OutsideInferredAlternativeEndpoint:    {},
		OutsideConflictingAlternativeEndpoint: {},
	}
	evidence := []OutsideSpendingEvidence{}
	manifest, manifestSHA, err := candidateresolution.WalkPublishedInterpretations(
		ctx, storageRoot, interpretationManifestPath,
		func(value candidateresolution.CandidateInterpretation) error {
			roles := dossierCandidateRoles(report.Evidence.CandidateID, value)
			if len(roles) == 0 {
				return nil
			}
			amount, ok := new(big.Int).SetString(value.AmountMinorUnits, 10)
			if !ok {
				return fmt.Errorf("interpretation amount is not canonical integer minor units")
			}
			for _, role := range roles {
				views[role].add(value.SupportOppose, amount)
			}
			evidence = append(evidence, OutsideSpendingEvidence{CandidateRoles: roles, Interpretation: value})
			return nil
		},
	)
	if err != nil {
		return Dossier{}, err
	}
	e := report.Evidence
	if manifest.Cycle != e.Cycle {
		return Dossier{}, fmt.Errorf("candidate report and independent-expenditure interpretations do not share one cycle")
	}
	releaseAlignment := "different_source_releases"
	if manifest.SourceReleaseID == e.Trace.Inputs.Sources.A.SourceReleaseID {
		releaseAlignment = "same_source_release"
	}
	sort.Slice(evidence, func(i, j int) bool {
		return evidence[i].Interpretation.InterpretationID < evidence[j].Interpretation.InterpretationID
	})

	committees := []fundingbasis.EvidenceCommittee{}
	for _, committee := range e.Committees {
		if committee.Authorization == "authorized" || committee.Authorization == "unresolved" {
			committees = append(committees, committee)
		}
	}
	if len(committees) == 0 {
		return Dossier{}, fmt.Errorf("candidate report has no candidate-linked committee evidence")
	}

	out := Dossier{
		SchemaVersion: DossierVersion, Policy: DossierPolicy, ExecutableSHA256: executable,
		CandidateID: e.CandidateID, Cycle: e.Cycle, State: "candidate_evidence_not_complete_funding",
		CandidateName: candidateName,
		Inputs: DossierInput{
			ParentReportID: report.ReportID, ParentReportSHA256: reportSHA,
			ParentReportVerification: "content_identity_checked_not_full_recalculation", ParentEvidenceID: e.ResultID,
			CandidateNameSource: report.CandidateNames,
			ReceiptFactSetID:    e.ReceiptInput.FactSetID, ReceiptManifestSHA256: e.ReceiptInput.ManifestSHA256,
			ReceiptFECSourceReleaseID:            e.Trace.Inputs.Sources.A.SourceReleaseID,
			CommitteeFlowCalculationSetID:        e.Trace.Inputs.Reconciliation.CalculationSetID,
			CommitteeFlowReconciliationReleaseID: e.Trace.Inputs.Sources.ReleaseID,
			CandidateInterpretationSetID:         manifest.CalculationSetID, CandidateInterpretationManifestSHA: manifestSHA,
			CandidateInterpretationArtifactSHA:  manifest.Interpretations.UncompressedSHA256,
			CandidateInterpretationVerification: "immutable_manifest_and_complete_artifact_verified",
			IndependentSpendingFECReleaseID:     manifest.SourceReleaseID, SourceReleaseAlignment: releaseAlignment,
		},
		Receipts: ReceiptEvidence{
			State: "candidate_linked_reported_receipt_populations", SummaryContextState: e.SummaryState,
			Overview: e.Overview, CandidateLinkedCommittees: committees, Limitations: append([]string{}, e.Limitations...),
		},
		OutsideSpending: OutsideSpendingEvidenceSet{
			State: "reported_and_alternative_candidate_endpoints_kept_separate", ViewsAreNonAdditive: true,
			RelevantRows: uint64(len(evidence)), Views: dossierOutsideViews(views), Evidence: evidence,
			GlobalCounts: manifest.Counts, GlobalAmounts: manifest.Amounts,
		},
		ConnectionExamples: append([]PathExample{}, report.Paths...),
		Limitations: []string{
			"candidate_linked_receipts_and_independent_expenditures_are_separate_non_additive_domains",
			"receipt_and_independent_expenditure_source_release_alignment_is_disclosed_not_assumed",
			"reported_safe_default_and_alternative_candidate_views_overlap_and_are_not_additive",
			"independent_expenditures_are_not_candidate_receipts",
			"committee_connectivity_does_not_establish_chronological_dollar_provenance",
			"person_corporation_and_terminal_source_identity_remain_unresolved",
			"terminal_and_allocation_policies_not_selected",
			"lobbying_legislation_votes_and_official_actions_not_included",
		},
	}
	out.DossierID, err = contentID(out)
	if err != nil {
		return Dossier{}, err
	}
	return out, ctx.Err()
}

func dossierCandidateName(report Report) (EntityName, error) {
	var result *EntityName
	for i := range report.Names {
		name := report.Names[i]
		if name.Kind != "candidate" || name.EntityID != report.Evidence.CandidateID {
			continue
		}
		if result != nil {
			return EntityName{}, fmt.Errorf("candidate report has duplicate selected-candidate name evidence")
		}
		copy := name
		result = &copy
	}
	if result == nil {
		return EntityName{}, fmt.Errorf("candidate report lacks selected-candidate name evidence")
	}
	return *result, nil
}

func dossierCandidateRoles(candidateID string, value candidateresolution.CandidateInterpretation) []string {
	roles := []string{}
	if value.ReportedCandidate.CandidateID == candidateID {
		roles = append(roles, OutsideReportedEndpoint)
	}
	if value.SafeDefaultCandidateID != nil && *value.SafeDefaultCandidateID == candidateID {
		roles = append(roles, OutsideSafeDefaultEndpoint)
	}
	if value.AlternativeCandidateID != nil && *value.AlternativeCandidateID == candidateID {
		switch value.State {
		case candidateresolution.InterpretationInferred:
			roles = append(roles, OutsideInferredAlternativeEndpoint)
		case candidateresolution.InterpretationConflicting:
			roles = append(roles, OutsideConflictingAlternativeEndpoint)
		}
	}
	return roles
}

func dossierOutsideViews(values map[string]*stanceAccumulator) []OutsideSpendingView {
	return []OutsideSpendingView{
		{Role: OutsideReportedEndpoint, FinancialUse: "source_reported_endpoint_only", Additive: false, Measures: values[OutsideReportedEndpoint].measures()},
		{Role: OutsideSafeDefaultEndpoint, FinancialUse: "corroborated_default_endpoint", Additive: false, Measures: values[OutsideSafeDefaultEndpoint].measures()},
		{Role: OutsideInferredAlternativeEndpoint, FinancialUse: "alternative_endpoint_evidence_not_default", Additive: false, Measures: values[OutsideInferredAlternativeEndpoint].measures()},
		{Role: OutsideConflictingAlternativeEndpoint, FinancialUse: "alternative_endpoint_evidence_not_default", Additive: false, Measures: values[OutsideConflictingAlternativeEndpoint].measures()},
	}
}

func (value *stanceAccumulator) add(stance string, amount *big.Int) {
	value.total.add(amount)
	if stance == "S" {
		value.supporting.add(amount)
	} else {
		value.opposing.add(amount)
	}
}

func (value *amountAccumulator) add(amount *big.Int) {
	value.rows++
	value.signed.Add(&value.signed, amount)
	switch amount.Sign() {
	case 1:
		value.positiveRows++
		value.positive.Add(&value.positive, amount)
	case -1:
		value.negativeRows++
		value.negative.Add(&value.negative, amount)
	default:
		value.zeroRows++
	}
}

func (value amountAccumulator) measures() AmountMeasures {
	return AmountMeasures{
		Rows: value.rows, PositiveRows: value.positiveRows, NegativeRows: value.negativeRows, ZeroRows: value.zeroRows,
		SignedMinorUnits: value.signed.String(), PositiveMinorUnits: value.positive.String(), NegativeMinorUnits: value.negative.String(),
	}
}

func (value stanceAccumulator) measures() StanceMeasures {
	return StanceMeasures{Total: value.total.measures(), Supporting: value.supporting.measures(), Opposing: value.opposing.measures()}
}
