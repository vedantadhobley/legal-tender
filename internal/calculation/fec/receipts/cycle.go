package receipts

import (
	"fmt"
	"sort"
	"strconv"

	fecoccurrence "github.com/vedantadhobley/legal-tender/internal/source/fec/occurrence"
)

type CycleCalculator struct {
	cycle      string
	cycleYear  int
	candidates map[string]*candidateCalculation
	routes     map[string]committeeRoute
}

type candidateCalculation struct {
	relationships           []CommitteeRelationship
	authorizedCommittees    int
	unresolvedRelationships int
	accumulator             candidateAccumulator
	summaries               []SummaryFact
}

type committeeRoute struct {
	authorized []string
	unresolved []string
}

// NewCycleCalculator prepares a cycle-scoped calculation. Receipts can then be
// streamed exactly once through AddReceipt before deterministic results are
// finalized.
func NewCycleCalculator(cycle string, requestedCandidates []string, linkages []LinkageFact, summaries []SummaryFact) (*CycleCalculator, error) {
	cycleYear, err := strconv.Atoi(cycle)
	if err != nil || cycleYear < 1976 || cycleYear%2 != 0 {
		return nil, fmt.Errorf("cycle must be an even year at or after 1976")
	}
	candidateIDs := make(map[string]struct{})
	for _, candidateID := range requestedCandidates {
		if candidateID == "" {
			return nil, fmt.Errorf("candidate ID is required")
		}
		candidateIDs[candidateID] = struct{}{}
	}
	for _, linkage := range linkages {
		if linkage.CandidateID != "" {
			candidateIDs[linkage.CandidateID] = struct{}{}
		}
	}
	for _, summary := range summaries {
		if summary.CandidateID != "" {
			candidateIDs[summary.CandidateID] = struct{}{}
		}
	}
	calculator := &CycleCalculator{
		cycle: cycle, cycleYear: cycleYear,
		candidates: make(map[string]*candidateCalculation, len(candidateIDs)),
		routes:     make(map[string]committeeRoute),
	}
	for candidateID := range candidateIDs {
		relationships := AuthorizedCommitteeRelationships(candidateID, linkages)
		candidate := &candidateCalculation{
			relationships: relationships,
			accumulator: candidateAccumulator{
				excluded: make(map[string]uint64), unresolved: make(map[string]uint64),
				committees: make(map[string]*committeeAccumulator), daily: make(map[string]int64), dailyCounts: make(map[string]uint64),
			},
		}
		for index := range candidate.relationships {
			relationship := &candidate.relationships[index]
			route := calculator.routes[relationship.CommitteeID]
			switch relationship.State {
			case "authorized":
				candidate.authorizedCommittees++
				route.authorized = append(route.authorized, candidateID)
			case "unresolved":
				candidate.unresolvedRelationships++
				route.unresolved = append(route.unresolved, candidateID)
			}
			calculator.routes[relationship.CommitteeID] = route
		}
		calculator.candidates[candidateID] = candidate
	}
	for _, summary := range summaries {
		candidate := calculator.candidates[summary.CandidateID]
		if candidate != nil {
			candidate.summaries = append(candidate.summaries, summary)
		}
	}
	for committeeID, route := range calculator.routes {
		sort.Strings(route.authorized)
		sort.Strings(route.unresolved)
		calculator.routes[committeeID] = route
	}
	return calculator, nil
}

// AddReceipt evaluates one fact and applies it only to candidates with an
// explicit authorized or unresolved relationship to its recipient committee.
func (calculator *CycleCalculator) AddReceipt(fact fecoccurrence.ScheduleAFact) error {
	_, err := calculator.AddReceiptWithDecision(fact)
	return err
}

// AddReceiptWithDecision applies one fact and returns its source-level
// calculation decision for immutable publication and drill-down.
func (calculator *CycleCalculator) AddReceiptWithDecision(fact fecoccurrence.ScheduleAFact) (ReceiptDecision, error) {
	return calculator.addReceiptInput(receiptInputFromFact(fact))
}

func (calculator *CycleCalculator) addReceiptInput(input receiptInput) (ReceiptDecision, error) {
	if input.Cycle != calculator.cycle {
		return ReceiptDecision{}, fmt.Errorf("receipt fact %s belongs to cycle %s", input.FactID, input.Cycle)
	}
	decision := decideReceipt(input)
	published := publishedReceiptDecision(input, decision)
	route := calculator.routes[decision.CommitteeID]
	if decision.State != "excluded_non_individual" && decision.State != "excluded_memo_subtotal" {
		for _, candidateID := range route.unresolved {
			calculator.candidates[candidateID].accumulator.unresolved["authorization_unresolved"]++
		}
	}
	for _, candidateID := range route.authorized {
		accumulator := &calculator.candidates[candidateID].accumulator
		switch decision.State {
		case "included":
			if err := addIncluded(accumulator, decision); err != nil {
				return ReceiptDecision{}, fmt.Errorf("candidate %s: %w", candidateID, err)
			}
		case "excluded_non_individual", "excluded_memo_subtotal":
			accumulator.excluded[decision.State]++
		case "unresolved_individual_class", "unresolved_amount":
			accumulator.unresolved[decision.State]++
		default:
			return ReceiptDecision{}, fmt.Errorf("unsupported receipt decision %q", decision.State)
		}
	}
	return published, nil
}

func (calculator *CycleCalculator) hasCommitteeRouteBytes(committeeID []byte) bool {
	route, ok := calculator.routes[string(committeeID)]
	return ok && (len(route.authorized) != 0 || len(route.unresolved) != 0)
}

func (calculator *CycleCalculator) Results() ([]Result, error) {
	candidateIDs := make([]string, 0, len(calculator.candidates))
	for candidateID := range calculator.candidates {
		candidateIDs = append(candidateIDs, candidateID)
	}
	sort.Strings(candidateIDs)
	results := make([]Result, 0, len(candidateIDs))
	for _, candidateID := range candidateIDs {
		result, err := calculator.finalize(candidateID, calculator.candidates[candidateID])
		if err != nil {
			return nil, err
		}
		results = append(results, result)
	}
	return results, nil
}

func (calculator *CycleCalculator) finalize(candidateID string, candidate *candidateCalculation) (Result, error) {
	state := "complete"
	issues := make([]string, 0, 2)
	if candidate.authorizedCommittees == 0 {
		state = "not_comparable"
		issues = append(issues, "no_authorized_committee")
	} else if candidate.unresolvedRelationships != 0 || sumCounts(candidate.accumulator.unresolved) != 0 {
		state = "partial"
		issues = append(issues, "unresolved_receipt_or_authorization")
	}
	result := Result{
		SchemaVersion: SchemaVersion, CalculationContract: ContractID, CalculationVersion: ContractVersion,
		CandidateID: candidateID, Cycle: calculator.cycle, State: state,
		Component: "fec_itemized_individual_receipts", IncludedRecords: candidate.accumulator.included,
		ExcludedRecords: nonNilCounts(candidate.accumulator.excluded), UnresolvedRecords: nonNilCounts(candidate.accumulator.unresolved),
		CommitteeRelationships: candidate.relationships, CommitteeSubtotals: committeeSubtotals(candidate.accumulator.committees),
		Issues: issues,
	}
	result.MoneyMeasure = calculatedMoneyMeasure(candidate.accumulator.total, state, issues)
	summaries := append([]SummaryFact(nil), candidate.summaries...)
	sort.Slice(summaries, func(left, right int) bool {
		if summaries[left].Dataset == summaries[right].Dataset {
			return summaries[left].FactID < summaries[right].FactID
		}
		return summaries[left].Dataset < summaries[right].Dataset
	})
	for _, summary := range summaries {
		result.SourceSummaries = append(result.SourceSummaries, SourceSummary{
			FactID: summary.FactID, FactType: summary.FactType, Dataset: summary.Dataset,
			CoverageThrough:              summary.CoverageThrough,
			TotalIndividualContributions: summary.TotalIndividualContributions,
			TotalReceipts:                summary.TotalReceipts,
		})
		reconciliation, err := reconcileSummary(summary, calculator.cycleYear, candidate.authorizedCommittees != 0, candidate.accumulator)
		if err != nil {
			return Result{}, fmt.Errorf("candidate %s: %w", candidateID, err)
		}
		result.Reconciliations = append(result.Reconciliations, reconciliation)
	}
	if result.SourceSummaries == nil {
		result.SourceSummaries = []SourceSummary{}
	}
	if result.Reconciliations == nil {
		result.Reconciliations = []SummaryReconciliation{}
	}
	return result, nil
}
