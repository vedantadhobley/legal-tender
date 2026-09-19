// Package receipts calculates the first candidate-scoped component from
// preserved FEC facts. It never mutates or fills source facts.
package receipts

import (
	"fmt"
	"sort"
	"strconv"
	"time"

	fecoccurrence "github.com/vedantadhobley/legal-tender/internal/source/fec/occurrence"
)

const (
	ContractID            = "fec/candidate-itemized-individual-receipts"
	ContractVersion       = "1.0.0"
	SchemaVersion         = "legal-tender.fec.candidate-itemized-individual-receipts.v1"
	DecisionSchemaVersion = "legal-tender.fec.itemized-individual-receipt-decision.v1"
)

type Input struct {
	CandidateID string                        `json:"candidate_id"`
	Cycle       string                        `json:"cycle"`
	Receipts    []fecoccurrence.ScheduleAFact `json:"receipts"`
	Linkages    []LinkageFact                 `json:"linkages"`
	Summaries   []SummaryFact                 `json:"summaries"`
}

type LinkageFact struct {
	FactID          string `json:"fact_id"`
	State           string `json:"state"`
	CandidateID     string `json:"candidate_id"`
	CommitteeID     string `json:"committee_id"`
	DesignationCode string `json:"designation_code"`
}

type SummaryFact struct {
	FactID                       string        `json:"fact_id"`
	FactType                     string        `json:"fact_type"`
	Dataset                      string        `json:"dataset"`
	CandidateID                  string        `json:"candidate_id"`
	CoverageThrough              *string       `json:"coverage_through"`
	TotalIndividualContributions SummaryAmount `json:"total_individual_contributions"`
	TotalReceipts                SummaryAmount `json:"total_receipts"`
}

type SummaryAmount struct {
	RawValue           string  `json:"raw_value"`
	ReportedMinorUnits *string `json:"reported_minor_units"`
	ObservationState   string  `json:"observation_state"`
}

type Result struct {
	SchemaVersion          string                  `json:"schema_version"`
	CalculationContract    string                  `json:"calculation_contract"`
	CalculationVersion     string                  `json:"calculation_version"`
	CandidateID            string                  `json:"candidate_id"`
	Cycle                  string                  `json:"cycle"`
	State                  string                  `json:"state"`
	Component              string                  `json:"component"`
	MoneyMeasure           MoneyMeasure            `json:"money_measure"`
	IncludedRecords        IncludedCounts          `json:"included_records"`
	ExcludedRecords        map[string]uint64       `json:"excluded_records"`
	UnresolvedRecords      map[string]uint64       `json:"unresolved_records"`
	CommitteeRelationships []CommitteeRelationship `json:"committee_relationships"`
	CommitteeSubtotals     []CommitteeSubtotal     `json:"committee_subtotals"`
	SourceSummaries        []SourceSummary         `json:"source_summaries"`
	Reconciliations        []SummaryReconciliation `json:"reconciliations"`
	Issues                 []string                `json:"issues"`
}

type IncludedCounts struct {
	Records  uint64 `json:"records"`
	Positive uint64 `json:"positive"`
	Negative uint64 `json:"negative"`
	Zero     uint64 `json:"zero"`
}

type CommitteeRelationship struct {
	CommitteeID       string   `json:"committee_id"`
	State             string   `json:"state"`
	DesignationCodes  []string `json:"designation_codes"`
	SupportingFactIDs []string `json:"supporting_fact_ids"`
}

type CommitteeSubtotal struct {
	CommitteeID      string         `json:"committee_id"`
	AmountMinorUnits string         `json:"amount_minor_units"`
	IncludedRecords  IncludedCounts `json:"included_records"`
}

type SourceSummary struct {
	FactID                       string        `json:"fact_id"`
	FactType                     string        `json:"fact_type"`
	Dataset                      string        `json:"dataset"`
	CoverageThrough              *string       `json:"coverage_through"`
	TotalIndividualContributions SummaryAmount `json:"total_individual_contributions"`
	TotalReceipts                SummaryAmount `json:"total_receipts"`
}

type SummaryReconciliation struct {
	SummaryFactID                  string   `json:"summary_fact_id"`
	SummaryFactType                string   `json:"summary_fact_type"`
	ComparisonField                string   `json:"comparison_field"`
	ComparisonClass                string   `json:"comparison_class"`
	CoverageState                  string   `json:"coverage_state"`
	CoverageThrough                *string  `json:"coverage_through"`
	SummaryMinorUnits              *string  `json:"summary_minor_units"`
	ResolvedDetailMinorUnits       *string  `json:"resolved_detail_minor_units"`
	DifferenceMinorUnits           *string  `json:"difference_minor_units"`
	IncludedDetailRecords          uint64   `json:"included_detail_records"`
	UnresolvedDetailRecords        uint64   `json:"unresolved_detail_records"`
	ExcludedOutsideCoverageRecords uint64   `json:"excluded_outside_coverage_records"`
	Issues                         []string `json:"issues"`
}

type MoneyMeasure struct {
	Schema        string           `json:"$schema"`
	SchemaVersion string           `json:"schema_version"`
	SemanticRole  string           `json:"semantic_role"`
	Currency      string           `json:"currency"`
	Amount        MoneyAmount      `json:"amount"`
	Measurement   MoneyMeasurement `json:"measurement"`
	Uncertainty   MoneyUncertainty `json:"uncertainty"`
	Issues        []string         `json:"issues"`
}

type MoneyAmount struct {
	State           string  `json:"state"`
	LowerMinorUnits *string `json:"lower_minor_units"`
	UpperMinorUnits *string `json:"upper_minor_units"`
	LowerInclusive  *bool   `json:"lower_inclusive"`
	UpperInclusive  *bool   `json:"upper_inclusive"`
}

type MoneyMeasurement struct {
	Basis                        string  `json:"basis"`
	Kind                         string  `json:"kind"`
	ObservationState             *string `json:"observation_state"`
	ReportedMinorUnits           *string `json:"reported_minor_units"`
	PrecisionIncrementMinorUnits *string `json:"precision_increment_minor_units"`
	AccountingMethod             *string `json:"accounting_method"`
	SourceRuleVersion            *string `json:"source_rule_version"`
}

type MoneyUncertainty struct {
	Coverage    string `json:"coverage"`
	Attribution string `json:"attribution"`
	Revision    string `json:"revision"`
	Identity    string `json:"identity"`
	Evidence    string `json:"evidence"`
}

// ReceiptDecision records the calculation's disposition of one immutable
// Schedule A fact. The source fact remains authoritative for every preserved
// field; this record makes calculation membership independently auditable.
type ReceiptDecision struct {
	SchemaVersion        string  `json:"schema_version"`
	CalculationContract  string  `json:"calculation_contract"`
	CalculationVersion   string  `json:"calculation_version"`
	FactID               string  `json:"fact_id"`
	NaturalKey           string  `json:"natural_key"`
	Cycle                string  `json:"cycle"`
	RecipientCommitteeID *string `json:"recipient_committee_id"`
	ReceivedOn           *string `json:"received_on"`
	State                string  `json:"state"`
	AmountMinorUnits     *string `json:"amount_minor_units"`
}

type receiptDecision struct {
	State       string
	CommitteeID string
	Amount      int64
	ReceivedOn  *string
}

// receiptInput is the narrow calculation boundary shared by normalized facts
// and direct source probes. It contains only fields used by the accepted
// receipt-decision contract.
type receiptInput struct {
	FactID                     string
	NaturalKey                 string
	Cycle                      string
	CommitteeID                string
	ReceivedOn                 *string
	PublisherClassedIndividual *bool
	MemoedSubtotal             bool
	AmountObservationState     string
	AmountMinorUnits           *string
}

type candidateAccumulator struct {
	total           int64
	included        IncludedCounts
	excluded        map[string]uint64
	unresolved      map[string]uint64
	committees      map[string]*committeeAccumulator
	daily           map[string]int64
	dailyCounts     map[string]uint64
	undatedIncluded uint64
}

type committeeAccumulator struct {
	total    int64
	included IncludedCounts
}

// Calculate evaluates one candidate against one cycle's preserved facts.
func Calculate(input Input) (Result, error) {
	calculator, err := NewCycleCalculator(input.Cycle, []string{input.CandidateID}, input.Linkages, input.Summaries)
	if err != nil {
		return Result{}, err
	}
	for _, fact := range input.Receipts {
		if err := calculator.AddReceipt(fact); err != nil {
			return Result{}, err
		}
	}
	results, err := calculator.Results()
	if err != nil {
		return Result{}, err
	}
	for _, result := range results {
		if result.CandidateID == input.CandidateID {
			return result, nil
		}
	}
	return Result{}, fmt.Errorf("candidate %s was not calculated", input.CandidateID)
}

func receiptInputFromFact(fact fecoccurrence.ScheduleAFact) receiptInput {
	input := receiptInput{
		FactID: fact.FactID, NaturalKey: fact.NaturalKey, Cycle: fact.Cycle,
		ReceivedOn:                 fact.TypedFields.Receipt.ReceivedOn,
		PublisherClassedIndividual: fact.TypedFields.Contributor.PublisherClassedIndividual,
		MemoedSubtotal:             fact.TypedFields.Receipt.MemoedSubtotal,
		AmountObservationState:     fact.TypedFields.Receipt.Amount.ObservationState,
		AmountMinorUnits:           fact.TypedFields.Receipt.Amount.ReportedMinorUnits,
	}
	if fact.TypedFields.Recipient.CommitteeID != nil {
		input.CommitteeID = *fact.TypedFields.Recipient.CommitteeID
	}
	return input
}

func decideReceipt(input receiptInput) receiptDecision {
	decision := receiptDecision{State: individualReceiptState(input.PublisherClassedIndividual, input.MemoedSubtotal,
		input.AmountObservationState == "reported_value" && input.AmountMinorUnits != nil), CommitteeID: input.CommitteeID, ReceivedOn: input.ReceivedOn}
	if decision.State != "included" {
		return decision
	}
	parsed, err := strconv.ParseInt(*input.AmountMinorUnits, 10, 64)
	if err != nil {
		decision.State = "unresolved_amount"
		return decision
	}
	decision.Amount = parsed
	return decision
}

// ItemizedIndividualDecision shares the accepted receipt membership rule with
// typed-column consumers. Publisher classification is not resolved identity.
func ItemizedIndividualDecision(individual *bool, memo bool, amountState string, amount *int64) string {
	return individualReceiptState(individual, memo, amountState == "reported_value" && amount != nil)
}

func individualReceiptState(individual *bool, memo, amountKnown bool) string {
	if individual == nil {
		return "unresolved_individual_class"
	}
	if !*individual {
		return "excluded_non_individual"
	}
	if memo {
		return "excluded_memo_subtotal"
	}
	if !amountKnown {
		return "unresolved_amount"
	}
	return "included"
}

func publishedReceiptDecision(input receiptInput, decision receiptDecision) ReceiptDecision {
	result := ReceiptDecision{
		SchemaVersion: DecisionSchemaVersion, CalculationContract: ContractID, CalculationVersion: ContractVersion,
		FactID: input.FactID, NaturalKey: input.NaturalKey, Cycle: input.Cycle,
		ReceivedOn: decision.ReceivedOn, State: decision.State,
	}
	if decision.CommitteeID != "" {
		committeeID := decision.CommitteeID
		result.RecipientCommitteeID = &committeeID
	}
	if decision.State == "included" {
		amount := strconv.FormatInt(decision.Amount, 10)
		result.AmountMinorUnits = &amount
	}
	return result
}

// AuthorizedCommitteeRelationships is the shared accepted same-cycle A/P
// policy. It retains all supporting facts and makes conflicting or shared
// authorization unresolved; names and other-cycle relationships are not inputs.
func AuthorizedCommitteeRelationships(candidateID string, facts []LinkageFact) []CommitteeRelationship {
	result := calculateRelationships(candidateID, facts)
	shared := sharedAuthorizedCommittees(candidateID, facts)
	for i := range result {
		if _, exists := shared[result[i].CommitteeID]; exists && result[i].State == "authorized" {
			result[i].State = "unresolved"
		}
	}
	return result
}

func calculateRelationships(candidateID string, facts []LinkageFact) []CommitteeRelationship {
	type aggregate struct {
		designations map[string]struct{}
		factIDs      map[string]struct{}
		invalid      bool
	}
	byCommittee := make(map[string]*aggregate)
	for _, fact := range facts {
		if fact.CandidateID != candidateID || fact.CommitteeID == "" {
			continue
		}
		entry := byCommittee[fact.CommitteeID]
		if entry == nil {
			entry = &aggregate{designations: make(map[string]struct{}), factIDs: make(map[string]struct{})}
			byCommittee[fact.CommitteeID] = entry
		}
		entry.designations[fact.DesignationCode] = struct{}{}
		if fact.FactID != "" {
			entry.factIDs[fact.FactID] = struct{}{}
		}
		if fact.State != "valid" {
			entry.invalid = true
		}
	}
	result := make([]CommitteeRelationship, 0, len(byCommittee))
	for committeeID, entry := range byCommittee {
		designations := sortedKeys(entry.designations)
		factIDs := sortedKeys(entry.factIDs)
		authorized, other := false, false
		for _, designation := range designations {
			if designation == "A" || designation == "P" {
				authorized = true
			} else {
				other = true
			}
		}
		state := "unauthorized"
		if entry.invalid || authorized && other {
			state = "unresolved"
		} else if authorized {
			state = "authorized"
		}
		result = append(result, CommitteeRelationship{CommitteeID: committeeID, State: state, DesignationCodes: designations, SupportingFactIDs: factIDs})
	}
	sort.Slice(result, func(left, right int) bool { return result[left].CommitteeID < result[right].CommitteeID })
	return result
}

func sharedAuthorizedCommittees(candidateID string, facts []LinkageFact) map[string]struct{} {
	owners := make(map[string]map[string]struct{})
	for _, fact := range facts {
		if fact.State != "valid" || fact.CommitteeID == "" || fact.CandidateID == "" || fact.DesignationCode != "A" && fact.DesignationCode != "P" {
			continue
		}
		if owners[fact.CommitteeID] == nil {
			owners[fact.CommitteeID] = make(map[string]struct{})
		}
		owners[fact.CommitteeID][fact.CandidateID] = struct{}{}
	}
	shared := make(map[string]struct{})
	for committeeID, candidates := range owners {
		if len(candidates) > 1 {
			if _, selected := candidates[candidateID]; selected {
				shared[committeeID] = struct{}{}
			}
		}
	}
	return shared
}

func addIncluded(accumulator *candidateAccumulator, decision receiptDecision) error {
	var err error
	accumulator.total, err = checkedAdd(accumulator.total, decision.Amount)
	if err != nil {
		return fmt.Errorf("candidate receipt sum: %w", err)
	}
	addIncludedCount(&accumulator.included, decision.Amount)
	committee := accumulator.committees[decision.CommitteeID]
	if committee == nil {
		committee = &committeeAccumulator{}
		accumulator.committees[decision.CommitteeID] = committee
	}
	committee.total, err = checkedAdd(committee.total, decision.Amount)
	if err != nil {
		return fmt.Errorf("committee %s receipt sum: %w", decision.CommitteeID, err)
	}
	addIncludedCount(&committee.included, decision.Amount)
	if decision.ReceivedOn == nil {
		accumulator.undatedIncluded++
		return nil
	}
	accumulator.daily[*decision.ReceivedOn], err = checkedAdd(accumulator.daily[*decision.ReceivedOn], decision.Amount)
	if err != nil {
		return fmt.Errorf("daily receipt sum for %s: %w", *decision.ReceivedOn, err)
	}
	accumulator.dailyCounts[*decision.ReceivedOn]++
	return nil
}

func addIncludedCount(counts *IncludedCounts, amount int64) {
	counts.Records++
	switch {
	case amount > 0:
		counts.Positive++
	case amount < 0:
		counts.Negative++
	default:
		counts.Zero++
	}
}

func reconcileSummary(summary SummaryFact, cycle int, hasAuthorized bool, accumulator candidateAccumulator) (SummaryReconciliation, error) {
	result := SummaryReconciliation{
		SummaryFactID: summary.FactID, SummaryFactType: summary.FactType,
		ComparisonField: "TTL_INDIV_CONTRIB", ComparisonClass: "individual_detail_gap",
		CoverageState: "not_comparable", CoverageThrough: summary.CoverageThrough,
		Issues: []string{},
	}
	if !hasAuthorized {
		result.Issues = append(result.Issues, "no_authorized_committee")
		return result, nil
	}
	if summary.TotalIndividualContributions.ObservationState != "reported_value" || summary.TotalIndividualContributions.ReportedMinorUnits == nil {
		result.Issues = append(result.Issues, "summary_total_individual_unavailable")
		return result, nil
	}
	summaryAmount, err := strconv.ParseInt(*summary.TotalIndividualContributions.ReportedMinorUnits, 10, 64)
	if err != nil {
		result.Issues = append(result.Issues, "summary_total_individual_invalid")
		return result, nil
	}
	if summary.CoverageThrough == nil {
		result.Issues = append(result.Issues, "summary_coverage_date_unavailable")
		return result, nil
	}
	coverage, err := time.Parse("2006-01-02", *summary.CoverageThrough)
	if err != nil {
		result.Issues = append(result.Issues, "summary_coverage_date_invalid")
		return result, nil
	}
	start := time.Date(cycle-1, time.January, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(cycle, time.December, 31, 0, 0, 0, 0, time.UTC)
	if coverage.Before(start) || coverage.After(end) {
		result.Issues = append(result.Issues, "summary_coverage_outside_cycle")
		return result, nil
	}
	var detail int64
	var included, excluded uint64
	for date, amount := range accumulator.daily {
		parsed, parseErr := time.Parse("2006-01-02", date)
		if parseErr != nil {
			return result, fmt.Errorf("normalized receipt date %q is invalid", date)
		}
		count := accumulator.dailyCounts[date]
		if parsed.Before(start) || parsed.After(coverage) {
			excluded += count
			continue
		}
		detail, err = checkedAdd(detail, amount)
		if err != nil {
			return result, fmt.Errorf("date-bounded receipt sum: %w", err)
		}
		included += count
	}
	difference, err := checkedSubtract(summaryAmount, detail)
	if err != nil {
		return result, fmt.Errorf("summary reconciliation difference: %w", err)
	}
	summaryText, detailText, differenceText := strconv.FormatInt(summaryAmount, 10), strconv.FormatInt(detail, 10), strconv.FormatInt(difference, 10)
	result.CoverageState = "date_bounded"
	result.SummaryMinorUnits = &summaryText
	result.ResolvedDetailMinorUnits = &detailText
	result.DifferenceMinorUnits = &differenceText
	result.IncludedDetailRecords = included
	result.UnresolvedDetailRecords = accumulator.undatedIncluded + sumCounts(accumulator.unresolved)
	result.ExcludedOutsideCoverageRecords = excluded
	if result.UnresolvedDetailRecords != 0 {
		result.Issues = append(result.Issues, "detail_coverage_partial")
	}
	return result, nil
}

func calculatedMoneyMeasure(amount int64, state string, issues []string) MoneyMeasure {
	method, version := ContractID+"@"+ContractVersion, ContractVersion
	measure := MoneyMeasure{
		Schema: "../../../../common/money-measure/v1/schema.json", SchemaVersion: "1.0.0", SemanticRole: "itemized_individual_receipts", Currency: "USD",
		Measurement: MoneyMeasurement{Basis: "calculated", Kind: "calculated_sum", AccountingMethod: &method, SourceRuleVersion: &version},
		Uncertainty: MoneyUncertainty{Coverage: "complete", Attribution: "direct", Revision: "publisher_processed_snapshot", Identity: "resolved", Evidence: "calculated"},
		Issues:      append([]string{}, issues...),
	}
	if state == "not_comparable" {
		measure.Amount = MoneyAmount{State: "not_applicable"}
		measure.Uncertainty.Coverage = "incompatible"
		measure.Uncertainty.Identity = "unresolved"
		return measure
	}
	value, inclusive := strconv.FormatInt(amount, 10), true
	measure.Amount = MoneyAmount{State: "point", LowerMinorUnits: &value, UpperMinorUnits: &value, LowerInclusive: &inclusive, UpperInclusive: &inclusive}
	if state == "partial" {
		measure.Uncertainty.Coverage = "partial"
		measure.Uncertainty.Identity = "partial"
	}
	return measure
}

func committeeSubtotals(input map[string]*committeeAccumulator) []CommitteeSubtotal {
	result := make([]CommitteeSubtotal, 0, len(input))
	for committeeID, value := range input {
		result = append(result, CommitteeSubtotal{CommitteeID: committeeID, AmountMinorUnits: strconv.FormatInt(value.total, 10), IncludedRecords: value.included})
	}
	sort.Slice(result, func(left, right int) bool { return result[left].CommitteeID < result[right].CommitteeID })
	return result
}

func checkedAdd(left, right int64) (int64, error) {
	if right > 0 && left > (1<<63-1)-right || right < 0 && left < (-1<<63)-right {
		return 0, fmt.Errorf("signed 64-bit minor-unit overflow")
	}
	return left + right, nil
}

func checkedSubtract(left, right int64) (int64, error) {
	if right == -1<<63 {
		if left >= 0 {
			return 0, fmt.Errorf("signed 64-bit minor-unit overflow")
		}
		return left - right, nil
	}
	return checkedAdd(left, -right)
}

func sortedKeys(input map[string]struct{}) []string {
	result := make([]string, 0, len(input))
	for value := range input {
		result = append(result, value)
	}
	sort.Strings(result)
	return result
}

func sumCounts(input map[string]uint64) uint64 {
	var result uint64
	for _, count := range input {
		result += count
	}
	return result
}

func nonNilCounts(input map[string]uint64) map[string]uint64 {
	if input == nil {
		return map[string]uint64{}
	}
	return input
}
