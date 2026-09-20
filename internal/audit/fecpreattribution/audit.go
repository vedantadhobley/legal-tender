// Package fecpreattribution validates the interpretation boundaries that must
// remain explicit before terminal-source attribution can carry money.
package fecpreattribution

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"os"
	"sort"
	"strings"

	"github.com/vedantadhobley/legal-tender/internal/audit/fecschedulebsemantics"
	"github.com/vedantadhobley/legal-tender/internal/calculation/fec/candidateresolution"
	"github.com/vedantadhobley/legal-tender/internal/calculation/fec/committeeflows"
	"github.com/vedantadhobley/legal-tender/internal/calculation/fec/flowreconciliation"
	"github.com/vedantadhobley/legal-tender/internal/storage/artifact"
)

const SchemaVersion = "legal-tender.audit.fec.pre-attribution-interpretations.v1"

type Options struct {
	StorageRoot               string
	CandidateResolutionPath   string
	ReceiverFlowPath          string
	ScheduleBSemanticsPath    string
	CommitteeFlowResultPath   string
	CommitteeFlowEvidenceRoot string
	Progress                  func(string)
}

type Input struct {
	Role     string `json:"role"`
	Path     string `json:"path"`
	SHA256   string `json:"sha256"`
	Identity string `json:"identity"`
}

type CandidateCohort struct {
	State                 string             `json:"state"`
	Rows                  uint64             `json:"rows"`
	AmountMinorUnits      string             `json:"amount_minor_units"`
	BlankReportedIDRows   uint64             `json:"blank_reported_id_rows"`
	DistinctReportedIDs   uint64             `json:"distinct_reported_ids"`
	DistinctResolvedIDs   uint64             `json:"distinct_resolved_ids"`
	DistinctEndpointPairs uint64             `json:"distinct_endpoint_pairs"`
	Examples              []CandidateExample `json:"examples"`
}

type CandidateExample struct {
	SelectionReason     string   `json:"selection_reason"`
	FactID              string   `json:"fact_id"`
	ReportedCandidateID string   `json:"reported_candidate_id"`
	ResolvedCandidateID string   `json:"resolved_candidate_id"`
	ReportedName        *string  `json:"reported_name"`
	ReportedOffice      *string  `json:"reported_office"`
	ReportedState       *string  `json:"reported_state"`
	ReportedDistrict    *string  `json:"reported_district"`
	SupportOppose       string   `json:"support_oppose"`
	AmountMinorUnits    string   `json:"amount_minor_units"`
	EvidenceCodes       []string `json:"evidence_codes"`
}

type CandidateValidation struct {
	ResolvedRows             uint64            `json:"resolved_rows"`
	ResolvedAmountMinorUnits string            `json:"resolved_amount_minor_units"`
	SameEndpointRows         uint64            `json:"same_endpoint_rows"`
	Cohorts                  []CandidateCohort `json:"cohorts"`
}

type IdentityCohort struct {
	Side                    string       `json:"side"`
	State                   string       `json:"state"`
	Rows                    uint64       `json:"rows"`
	AmountMinorUnits        string       `json:"amount_minor_units"`
	NonMemoRows             *uint64      `json:"non_memo_rows,omitempty"`
	NonMemoAmountMinorUnits *string      `json:"non_memo_amount_minor_units,omitempty"`
	SelfRecipientRows       uint64       `json:"self_recipient_rows"`
	DistinctCommitteeIDs    *uint64      `json:"distinct_committee_ids,omitempty"`
	ReceiptTypes            []NamedCount `json:"receipt_types,omitempty"`
	Example                 any          `json:"example,omitempty"`
}

type NamedCount struct {
	Name string `json:"name"`
	Rows uint64 `json:"rows"`
}

type CommitteeIdentityValidation struct {
	ScheduleAOneSidedRows uint64           `json:"schedule_a_one_sided_rows"`
	ScheduleAConflictRows uint64           `json:"schedule_a_conflict_rows"`
	ScheduleBOneSidedRows uint64           `json:"schedule_b_one_sided_rows"`
	ScheduleBConflictRows uint64           `json:"schedule_b_conflict_rows"`
	Cohorts               []IdentityCohort `json:"cohorts"`
}

type GapBand struct {
	Name       string `json:"name"`
	Components uint64 `json:"components"`
}

type ABShape struct {
	ScheduleATypes string `json:"schedule_a_types"`
	ScheduleBTypes string `json:"schedule_b_types"`
	Components     uint64 `json:"components"`
}

type ABValidation struct {
	Components                 uint64                            `json:"components"`
	AmbiguousComponents        uint64                            `json:"ambiguous_components"`
	DatedOneToOneComponents    uint64                            `json:"dated_one_to_one_components"`
	DateDisagreementComponents uint64                            `json:"date_disagreement_components"`
	AMinusBNegativeComponents  uint64                            `json:"a_minus_b_negative_components"`
	AMinusBPositiveComponents  uint64                            `json:"a_minus_b_positive_components"`
	MaximumAbsoluteGapDays     uint64                            `json:"maximum_absolute_gap_days"`
	GapBands                   []GapBand                         `json:"absolute_date_gap_bands"`
	DateDisagreementShapes     []ABShape                         `json:"date_disagreement_shapes"`
	LargestGapExample          *flowreconciliation.ReviewExample `json:"largest_gap_example,omitempty"`
}

type Check struct {
	ID       string `json:"id"`
	Passed   bool   `json:"passed"`
	Severity string `json:"severity"`
	Detail   string `json:"detail"`
}

type Result struct {
	SchemaVersion       string                      `json:"schema_version"`
	Status              string                      `json:"status"`
	Cycle               string                      `json:"cycle"`
	Inputs              []Input                     `json:"inputs"`
	CandidateResolution CandidateValidation         `json:"candidate_resolution"`
	CommitteeIdentities CommitteeIdentityValidation `json:"committee_identities"`
	ScheduleAB          ABValidation                `json:"schedule_a_b_comparison"`
	PolicyChanged       bool                        `json:"policy_changed"`
	GraphEligible       bool                        `json:"graph_eligible"`
	Checks              []Check                     `json:"checks"`
}

func Audit(ctx context.Context, options Options) (Result, error) {
	result := Result{
		SchemaVersion: SchemaVersion, Status: "incomplete", Inputs: []Input{},
		PolicyChanged: false, GraphEligible: false, Checks: []Check{},
	}
	if options.StorageRoot == "" || options.CandidateResolutionPath == "" || options.ReceiverFlowPath == "" ||
		options.ScheduleBSemanticsPath == "" || options.CommitteeFlowResultPath == "" || options.CommitteeFlowEvidenceRoot == "" {
		return result, fmt.Errorf("all pre-attribution audit inputs are required")
	}
	progress := func(message string) {
		if options.Progress != nil {
			options.Progress(message)
		}
	}

	progress("verifying and profiling candidate-resolution decisions")
	candidateManifest, candidateDigest, err := candidateresolution.LoadPublishedManifest(ctx, options.StorageRoot, options.CandidateResolutionPath)
	if err != nil {
		return result, fmt.Errorf("load candidate resolution: %w", err)
	}
	candidateProfile, err := scanCandidateDecisions(ctx, options.StorageRoot, candidateManifest)
	if err != nil {
		return result, err
	}

	progress("verifying and profiling Schedule A committee-ID exceptions")
	receiverManifest, receiverDigest, err := committeeflows.LoadPublishedManifest(ctx, options.StorageRoot, options.ReceiverFlowPath)
	if err != nil {
		return result, fmt.Errorf("load receiver flow: %w", err)
	}
	receiverProfile, err := scanReceiverExceptions(ctx, options.StorageRoot, receiverManifest)
	if err != nil {
		return result, err
	}

	progress("loading the complete Schedule B semantics profile")
	var scheduleB fecschedulebsemantics.Result
	scheduleBDigest, err := readStrictJSON(options.ScheduleBSemanticsPath, &scheduleB)
	if err != nil {
		return result, fmt.Errorf("load Schedule B semantics: %w", err)
	}
	scheduleBProfile, scheduleBRows, err := profileScheduleB(scheduleB)
	if err != nil {
		return result, err
	}

	progress("replaying the retained Schedule A/B component review")
	flowResult, flowDigest, err := readFlowResult(options.CommitteeFlowResultPath)
	if err != nil {
		return result, err
	}
	flowReview, err := flowreconciliation.Review(ctx, flowreconciliation.ReviewOptions{
		ResultPath: options.CommitteeFlowResultPath, EvidenceRoot: options.CommitteeFlowEvidenceRoot,
		StorageRoot: options.StorageRoot,
	})
	if err != nil {
		return result, fmt.Errorf("review committee flows: %w", err)
	}
	abProfile, dateDisagreementShapeCount, err := profileAB(flowReview)
	if err != nil {
		return result, err
	}

	result.Cycle = candidateManifest.Cycle
	result.Inputs = []Input{
		{Role: "candidate_resolution", Path: options.CandidateResolutionPath, SHA256: candidateDigest, Identity: candidateManifest.CalculationSetID},
		{Role: "receiver_reported_committee_flows", Path: options.ReceiverFlowPath, SHA256: receiverDigest, Identity: receiverManifest.CalculationSetID},
		{Role: "schedule_b_semantics", Path: options.ScheduleBSemanticsPath, SHA256: scheduleBDigest, Identity: scheduleB.ProfileSHA256},
		{Role: "committee_flow_reconciliation", Path: options.CommitteeFlowResultPath, SHA256: flowDigest, Identity: flowResult.CalculationSetID},
	}
	result.CandidateResolution = candidateProfile
	result.CommitteeIdentities = CommitteeIdentityValidation{
		ScheduleAOneSidedRows: receiverProfile.oneSided, ScheduleAConflictRows: receiverProfile.conflicts,
		ScheduleBOneSidedRows: scheduleBRows.oneSided, ScheduleBConflictRows: scheduleBRows.conflicts,
		Cohorts: append(receiverProfile.cohorts, scheduleBProfile...),
	}
	result.ScheduleAB = abProfile

	sameCycle := candidateManifest.Cycle == receiverManifest.Cycle && candidateManifest.Cycle == scheduleB.Cycle && candidateManifest.Cycle == flowResult.Cycle
	lineage := receiverManifest.InputFactSet.FactSetID == flowResult.Input.A.FactSetID && scheduleB.Input.FactSetID == flowResult.Input.B.FactSetID
	checks := []Check{
		{"cycle_coherence", sameCycle, "block", fmt.Sprintf("candidate=%s receiver=%s schedule_b=%s reconciliation=%s", candidateManifest.Cycle, receiverManifest.Cycle, scheduleB.Cycle, flowResult.Cycle)},
		{"flow_fact_lineage", lineage, "block", "Schedule A and B profiles match the exact reconciliation fact sets"},
		{"candidate_resolution_conservation", candidateProfile.ResolvedRows == candidateManifest.Counts.Resolved && candidateProfile.ResolvedAmountMinorUnits == candidateManifest.Amounts.ResolvedMinorUnits, "block", fmt.Sprintf("rows=%d amount=%s", candidateProfile.ResolvedRows, candidateProfile.ResolvedAmountMinorUnits)},
		{"candidate_endpoint_separation", candidateProfile.SameEndpointRows == 0, "block", fmt.Sprintf("resolved decisions retaining the reported endpoint=%d", candidateProfile.SameEndpointRows)},
		{"schedule_a_one_sided_conservation", receiverProfile.oneSided == receiverManifest.DecisionCounts.UnresolvedOneSidedSourceCommitteeID && receiverProfile.conflicts == receiverManifest.DecisionCounts.UnresolvedConflictingSourceCommitteeIDs, "block", fmt.Sprintf("one_sided=%d conflicts=%d", receiverProfile.oneSided, receiverProfile.conflicts)},
		{"schedule_b_profile_complete", scheduleB.Status == "complete_diagnostic" && scheduleB.Measures.Rows == scheduleB.Input.Facts, "block", fmt.Sprintf("rows=%d facts=%d", scheduleB.Measures.Rows, scheduleB.Input.Facts)},
		{"date_gap_conservation", abProfile.DateDisagreementComponents == dateDisagreementShapeCount, "block", fmt.Sprintf("nonzero_gaps=%d candidate_date_disagreement_shapes=%d", abProfile.DateDisagreementComponents, dateDisagreementShapeCount)},
		{"diagnostic_only", !result.PolicyChanged && !result.GraphEligible, "block", "the audit changes no policy, facts, graph edges, or money"},
	}
	for _, check := range checks {
		if !check.Passed && check.Severity == "block" {
			result.Checks = checks
			return result, fmt.Errorf("pre-attribution audit check %s failed", check.ID)
		}
	}
	result.Checks = checks
	result.Status = "complete_diagnostic"
	return result, nil
}

type candidateAccumulator struct {
	rows, blank uint64
	amount      big.Int
	reported    map[string]struct{}
	resolved    map[string]struct{}
	pairs       map[string]struct{}
	first       *candidateresolution.Decision
	largest     *candidateresolution.Decision
}

func scanCandidateDecisions(ctx context.Context, root string, manifest candidateresolution.Manifest) (CandidateValidation, error) {
	reader, err := artifact.Open[candidateresolution.Decision](ctx, root, manifest.Decisions)
	if err != nil {
		return CandidateValidation{}, err
	}
	defer reader.Abort()
	cohorts := map[string]*candidateAccumulator{}
	var total big.Int
	var rows, same uint64
	for {
		decision, ok, err := reader.Next()
		if err != nil {
			return CandidateValidation{}, err
		}
		if !ok {
			break
		}
		if decision.State != candidateresolution.StateResolved {
			continue
		}
		if decision.ResolvedCandidateID == nil {
			return CandidateValidation{}, fmt.Errorf("resolved candidate decision %s has no endpoint", decision.DecisionID)
		}
		state, err := resolvedCohort(decision.EvidenceCodes)
		if err != nil {
			return CandidateValidation{}, fmt.Errorf("candidate decision %s: %w", decision.DecisionID, err)
		}
		value, ok := new(big.Int).SetString(decision.AmountMinorUnits, 10)
		if !ok {
			return CandidateValidation{}, fmt.Errorf("candidate decision %s has invalid amount", decision.DecisionID)
		}
		total.Add(&total, value)
		rows++
		if decision.ReportedCandidate.CandidateID == *decision.ResolvedCandidateID {
			same++
		}
		acc := cohorts[state]
		if acc == nil {
			acc = &candidateAccumulator{reported: map[string]struct{}{}, resolved: map[string]struct{}{}, pairs: map[string]struct{}{}}
			cohorts[state] = acc
		}
		acc.rows++
		acc.amount.Add(&acc.amount, value)
		if decision.ReportedCandidate.CandidateID == "" {
			acc.blank++
		} else {
			acc.reported[decision.ReportedCandidate.CandidateID] = struct{}{}
		}
		acc.resolved[*decision.ResolvedCandidateID] = struct{}{}
		acc.pairs[decision.ReportedCandidate.CandidateID+"\x00"+*decision.ResolvedCandidateID] = struct{}{}
		copy := decision
		if acc.first == nil || decision.FactID < acc.first.FactID {
			acc.first = &copy
		}
		if acc.largest == nil || absoluteAmount(decision.AmountMinorUnits).Cmp(absoluteAmount(acc.largest.AmountMinorUnits)) > 0 ||
			(absoluteAmount(decision.AmountMinorUnits).Cmp(absoluteAmount(acc.largest.AmountMinorUnits)) == 0 && decision.FactID < acc.largest.FactID) {
			acc.largest = &copy
		}
	}
	if err := reader.Close(); err != nil {
		return CandidateValidation{}, err
	}
	result := CandidateValidation{ResolvedRows: rows, ResolvedAmountMinorUnits: total.String(), SameEndpointRows: same, Cohorts: []CandidateCohort{}}
	for _, state := range []string{"reported_id_absent_from_candidate_master", "reported_id_context_conflict"} {
		acc := cohorts[state]
		if acc == nil {
			continue
		}
		result.Cohorts = append(result.Cohorts, CandidateCohort{
			State: state, Rows: acc.rows, AmountMinorUnits: acc.amount.String(), BlankReportedIDRows: acc.blank,
			DistinctReportedIDs: uint64(len(acc.reported)), DistinctResolvedIDs: uint64(len(acc.resolved)), DistinctEndpointPairs: uint64(len(acc.pairs)),
			Examples: candidateExamples(acc),
		})
	}
	return result, nil
}

func resolvedCohort(codes []string) (string, error) {
	absent, conflict := false, false
	for _, code := range codes {
		absent = absent || code == "reported_id_absent_from_candidate_master"
		conflict = conflict || code == "reported_id_context_conflict"
	}
	if absent == conflict {
		return "", fmt.Errorf("resolved decision must have exactly one reported-ID cohort")
	}
	if conflict {
		return "reported_id_context_conflict", nil
	}
	return "reported_id_absent_from_candidate_master", nil
}

func candidateExamples(acc *candidateAccumulator) []CandidateExample {
	selected := []struct {
		reason string
		value  *candidateresolution.Decision
	}{{"lexicographically_first_fact", acc.first}, {"largest_absolute_amount", acc.largest}}
	result := []CandidateExample{}
	seen := map[string]bool{}
	for _, item := range selected {
		if item.value == nil || seen[item.value.FactID] {
			continue
		}
		seen[item.value.FactID] = true
		decision := item.value
		result = append(result, CandidateExample{
			SelectionReason: item.reason, FactID: decision.FactID, ReportedCandidateID: decision.ReportedCandidate.CandidateID,
			ResolvedCandidateID: *decision.ResolvedCandidateID, ReportedName: decision.ReportedCandidate.Name,
			ReportedOffice: decision.ReportedCandidate.Office, ReportedState: decision.ReportedCandidate.OfficeState,
			ReportedDistrict: decision.ReportedCandidate.OfficeDistrict, SupportOppose: decision.SupportOppose,
			AmountMinorUnits: decision.AmountMinorUnits, EvidenceCodes: append([]string(nil), decision.EvidenceCodes...),
		})
	}
	return result
}

type receiverProfile struct {
	oneSided, conflicts uint64
	cohorts             []IdentityCohort
}

func scanReceiverExceptions(ctx context.Context, root string, manifest committeeflows.Manifest) (receiverProfile, error) {
	reader, err := artifact.Open[committeeflows.Exception](ctx, root, manifest.Exceptions)
	if err != nil {
		return receiverProfile{}, err
	}
	defer reader.Abort()
	type acc struct {
		rows, self uint64
		amount     big.Int
		ids        map[string]struct{}
		types      map[string]uint64
		example    *committeeflows.Exception
	}
	cohorts := map[string]*acc{}
	var conflicts uint64
	for {
		exception, ok, err := reader.Next()
		if err != nil {
			return receiverProfile{}, err
		}
		if !ok {
			break
		}
		if exception.State == committeeflows.DecisionUnresolvedConflict {
			conflicts++
		}
		if exception.State != committeeflows.DecisionUnresolvedOneSided {
			continue
		}
		state, id, err := oneSidedIdentity(exception.ContributorID, exception.CleanContributorID)
		if err != nil {
			return receiverProfile{}, fmt.Errorf("Schedule A exception %s: %w", exception.ExceptionID, err)
		}
		value := new(big.Int)
		if exception.AmountMinorUnits != nil {
			if _, ok := value.SetString(*exception.AmountMinorUnits, 10); !ok {
				return receiverProfile{}, fmt.Errorf("Schedule A exception %s has invalid amount", exception.ExceptionID)
			}
		}
		group := cohorts[state]
		if group == nil {
			group = &acc{ids: map[string]struct{}{}, types: map[string]uint64{}}
			cohorts[state] = group
		}
		group.rows++
		group.amount.Add(&group.amount, value)
		group.ids[id] = struct{}{}
		if exception.RecipientCommitteeID != nil && *exception.RecipientCommitteeID == id {
			group.self++
		}
		if exception.ReceiptTypeCode != nil {
			group.types[*exception.ReceiptTypeCode]++
		} else {
			group.types["<null>"]++
		}
		if group.example == nil || exception.SourceRowOrdinal < group.example.SourceRowOrdinal {
			copy := exception
			group.example = &copy
		}
	}
	if err := reader.Close(); err != nil {
		return receiverProfile{}, err
	}
	result := receiverProfile{conflicts: conflicts, cohorts: []IdentityCohort{}}
	for _, state := range []string{"raw_committee_id_only", "clean_committee_id_only"} {
		group := cohorts[state]
		if group == nil {
			continue
		}
		result.oneSided += group.rows
		distinct := uint64(len(group.ids))
		result.cohorts = append(result.cohorts, IdentityCohort{
			Side: "schedule_a", State: state, Rows: group.rows, AmountMinorUnits: group.amount.String(),
			SelfRecipientRows: group.self, DistinctCommitteeIDs: &distinct,
			ReceiptTypes: namedCounts(group.types), Example: group.example,
		})
	}
	return result, nil
}

func oneSidedIdentity(raw, clean *string) (string, string, error) {
	rawValid, cleanValid := validCommittee(raw), validCommittee(clean)
	switch {
	case rawValid && !cleanValid:
		return "raw_committee_id_only", *raw, nil
	case !rawValid && cleanValid:
		return "clean_committee_id_only", *clean, nil
	default:
		return "", "", fmt.Errorf("not a one-sided committee identity")
	}
}

type scheduleBRows struct{ oneSided, conflicts uint64 }

func profileScheduleB(result fecschedulebsemantics.Result) ([]IdentityCohort, scheduleBRows, error) {
	if result.SchemaVersion != fecschedulebsemantics.Version || result.Status != "complete_diagnostic" {
		return nil, scheduleBRows{}, fmt.Errorf("unsupported Schedule B semantics result")
	}
	type acc struct {
		rows, nonMemo, self   uint64
		amount, nonMemoAmount big.Int
		example               *fecschedulebsemantics.Example
	}
	groups := map[string]*acc{}
	var totalRows uint64
	var totalAmount big.Int
	for _, group := range result.Groups {
		totalRows += group.Measures.Rows
		totalAmount.Add(&totalAmount, big.NewInt(group.Measures.AmountMinorUnits))
		state := group.Shape.RecipientIdentity
		if state != "raw_committee_id_only" && state != "clean_committee_id_only" && state != "conflicting_committee_ids" {
			continue
		}
		value := groups[state]
		if value == nil {
			value = &acc{}
			groups[state] = value
		}
		value.rows += group.Measures.Rows
		value.nonMemo += group.Measures.NonMemoRows
		value.amount.Add(&value.amount, big.NewInt(group.Measures.AmountMinorUnits))
		value.nonMemoAmount.Add(&value.nonMemoAmount, big.NewInt(group.Measures.NonMemoAmountMinorUnits))
		if group.Shape.SelfRecipient {
			value.self += group.Measures.Rows
		}
		if value.example == nil || group.Example.Ordinal < value.example.Ordinal {
			copy := group.Example
			value.example = &copy
		}
	}
	if totalRows != result.Measures.Rows || totalAmount.String() != fmt.Sprint(result.Measures.AmountMinorUnits) {
		return nil, scheduleBRows{}, fmt.Errorf("Schedule B semantics groups do not conserve rows or amount")
	}
	cohorts := []IdentityCohort{}
	rows := scheduleBRows{}
	for _, state := range []string{"raw_committee_id_only", "clean_committee_id_only", "conflicting_committee_ids"} {
		value := groups[state]
		if value == nil {
			continue
		}
		if state == "conflicting_committee_ids" {
			rows.conflicts = value.rows
		} else {
			rows.oneSided += value.rows
		}
		nonMemoRows, nonMemoAmount := value.nonMemo, value.nonMemoAmount.String()
		cohorts = append(cohorts, IdentityCohort{
			Side: "schedule_b", State: state, Rows: value.rows, AmountMinorUnits: value.amount.String(),
			NonMemoRows: &nonMemoRows, NonMemoAmountMinorUnits: &nonMemoAmount, SelfRecipientRows: value.self,
			Example: value.example,
		})
	}
	return cohorts, rows, nil
}

func profileAB(review flowreconciliation.ReviewResult) (ABValidation, uint64, error) {
	result := ABValidation{GapBands: []GapBand{
		{Name: "same_day"}, {Name: "1_to_3_days"}, {Name: "4_to_10_days"}, {Name: "11_to_30_days"},
		{Name: "31_to_90_days"}, {Name: "91_to_365_days"}, {Name: "over_365_days"},
	}, DateDisagreementShapes: []ABShape{}}
	shapeCounts := map[string]uint64{}
	var dateShapeCount uint64
	for _, shape := range review.Shapes {
		result.Components += shape.Components
		if shape.Key.State == "ambiguous_candidates" {
			result.AmbiguousComponents += shape.Components
		}
		if shape.Key.State == "candidate_date_disagreement" {
			dateShapeCount += shape.Components
			shapeCounts[shape.Key.ATypes+"\x00"+shape.Key.BTypes] += shape.Components
		}
	}
	for key, components := range shapeCounts {
		parts := strings.SplitN(key, "\x00", 2)
		result.DateDisagreementShapes = append(result.DateDisagreementShapes, ABShape{ScheduleATypes: parts[0], ScheduleBTypes: parts[1], Components: components})
	}
	sort.Slice(result.DateDisagreementShapes, func(i, j int) bool {
		left, right := result.DateDisagreementShapes[i], result.DateDisagreementShapes[j]
		if left.Components != right.Components {
			return left.Components > right.Components
		}
		return left.ScheduleATypes+"\x00"+left.ScheduleBTypes < right.ScheduleATypes+"\x00"+right.ScheduleBTypes
	})
	for i := range review.Examples {
		if contains(review.Examples[i].Reasons, "largest_one_to_one_date_gap") {
			example := review.Examples[i]
			result.LargestGapExample = &example
			break
		}
	}
	for _, date := range review.Dates {
		result.DatedOneToOneComponents += date.Components
		absolute := abs64(date.AMinusBDays)
		if absolute > result.MaximumAbsoluteGapDays {
			result.MaximumAbsoluteGapDays = absolute
		}
		if date.AMinusBDays < 0 {
			result.AMinusBNegativeComponents += date.Components
		} else if date.AMinusBDays > 0 {
			result.AMinusBPositiveComponents += date.Components
		}
		if date.AMinusBDays != 0 {
			result.DateDisagreementComponents += date.Components
		}
		index := gapBandIndex(absolute)
		result.GapBands[index].Components += date.Components
	}
	return result, dateShapeCount, nil
}

func gapBandIndex(days uint64) int {
	switch {
	case days == 0:
		return 0
	case days <= 3:
		return 1
	case days <= 10:
		return 2
	case days <= 30:
		return 3
	case days <= 90:
		return 4
	case days <= 365:
		return 5
	default:
		return 6
	}
}

func readFlowResult(path string) (flowreconciliation.Result, string, error) {
	var result flowreconciliation.Result
	digest, err := readStrictJSON(path, &result)
	if err != nil {
		return result, "", err
	}
	return result, digest, nil
}

func readStrictJSON(path string, target any) (string, error) {
	content, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	decoder := json.NewDecoder(bytes.NewReader(content))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return "", err
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return "", fmt.Errorf("extra JSON content")
		}
		return "", fmt.Errorf("decode trailing JSON content: %w", err)
	}
	digest := sha256.Sum256(content)
	return hex.EncodeToString(digest[:]), nil
}

func validCommittee(value *string) bool {
	if value == nil || len(*value) != 9 || (*value)[0] != 'C' {
		return false
	}
	for _, character := range (*value)[1:] {
		if character < '0' || character > '9' {
			return false
		}
	}
	return true
}

func namedCounts(values map[string]uint64) []NamedCount {
	result := make([]NamedCount, 0, len(values))
	for name, rows := range values {
		result = append(result, NamedCount{Name: name, Rows: rows})
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Name < result[j].Name })
	return result
}

func absoluteAmount(value string) *big.Int {
	parsed, ok := new(big.Int).SetString(value, 10)
	if !ok {
		return new(big.Int)
	}
	return parsed.Abs(parsed)
}

func abs64(value int64) uint64 {
	if value < 0 {
		return uint64(-(value + 1)) + 1
	}
	return uint64(value)
}

func contains(values []string, target string) bool {
	for _, value := range values {
		if strings.EqualFold(value, target) {
			return true
		}
	}
	return false
}
