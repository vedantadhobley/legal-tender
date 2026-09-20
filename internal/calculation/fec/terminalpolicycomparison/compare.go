package terminalpolicycomparison

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"sort"

	"github.com/vedantadhobley/legal-tender/internal/calculation/fec/committeeflows"
	"github.com/vedantadhobley/legal-tender/internal/calculation/fec/fundingbasis"
)

type accumulator struct {
	rows, known, unknown, positiveRows, negativeRows, zeroRows uint64
	signed, positive, negative                                 big.Int
}

func digest(value string) bool {
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == sha256.Size && hex.EncodeToString(decoded) == value
}

func (a *accumulator) add(value fundingbasis.Measures) {
	a.rows += value.Rows
	a.known += value.Known
	a.unknown += value.Unknown
	a.positiveRows += value.PositiveRows
	a.negativeRows += value.NegativeRows
	a.zeroRows += value.ZeroRows
	a.signed.Add(&a.signed, big.NewInt(value.Signed))
	a.positive.Add(&a.positive, big.NewInt(value.Positive))
	a.negative.Add(&a.negative, big.NewInt(value.Negative))
}

func (a *accumulator) addAccumulator(value accumulator) {
	a.rows += value.rows
	a.known += value.known
	a.unknown += value.unknown
	a.positiveRows += value.positiveRows
	a.negativeRows += value.negativeRows
	a.zeroRows += value.zeroRows
	a.signed.Add(&a.signed, &value.signed)
	a.positive.Add(&a.positive, &value.positive)
	a.negative.Add(&a.negative, &value.negative)
}

func known(value fundingbasis.Measures) accumulator {
	var out accumulator
	value.Rows = value.Known
	value.Unknown = 0
	out.add(value)
	return out
}

func unknown(value fundingbasis.Measures) accumulator {
	return accumulator{rows: value.Unknown, unknown: value.Unknown}
}

func (a accumulator) measures() Measures {
	return Measures{
		Rows: a.rows, KnownAmountRows: a.known, UnknownAmountRows: a.unknown,
		PositiveRows: a.positiveRows, NegativeRows: a.negativeRows, ZeroRows: a.zeroRows,
		SignedMinorUnits: a.signed.String(), PositiveMinorUnits: a.positive.String(), NegativeMinorUnits: a.negative.String(),
	}
}

func (a accumulator) equal(b accumulator) bool {
	return a.rows == b.rows && a.known == b.known && a.unknown == b.unknown &&
		a.positiveRows == b.positiveRows && a.negativeRows == b.negativeRows && a.zeroRows == b.zeroRows &&
		a.signed.Cmp(&b.signed) == 0 && a.positive.Cmp(&b.positive) == 0 && a.negative.Cmp(&b.negative) == 0
}

func terminalDefinitions() []TerminalDefinition {
	return []TerminalDefinition{
		{ID: "explicit_earmark_origin@1", Boundary: "the original contributor appearance on a reported earmarked candidate receipt", Status: "supported_at_source_occurrence_grain", Meaning: "the contributor designated the candidate; the conduit is retained as a separate zero-money association"},
		{ID: "reported_noncommittee_appearance@1", Boundary: "a nonmemo itemized-individual appearance reported directly to an authorized candidate committee", Status: "supported_at_source_occurrence_grain", Meaning: "a direct reported source appearance, not a deduplicated or resolved person"},
		{ID: "reported_committee_counterparty@1", Boundary: "the committee reported as the immediate source of a candidate-committee receipt", Status: "supported_as_immediate_counterparty_only", Meaning: "the direct committee relationship is evidenced; upstream economic origin is not"},
		{ID: "selected_topology_frontier@1", Boundary: "a committee with no incoming edge in a selected A or B ledger", Status: "rejected_as_financial_origin", Meaning: "selected adjacency is useful topology evidence", Blocker: "the selected ledgers are incomplete funding denominators and disagree on frontier membership"},
		{ID: "resolved_person_or_organization@1", Boundary: "a time-scoped resolved person or organization supported by source appearances", Status: "blocked", Meaning: "the desired semantic entity boundary", Blocker: "live person and organization resolution is not accepted"},
	}
}

func allocationMethods() []AllocationMethod {
	return []AllocationMethod{
		{ID: "no_allocation@1", Status: "supported_baseline", Rule: "retain the complete scoped population as unresolved", Conservation: "exact"},
		{ID: "explicit_earmark@1", Status: "supported_at_source_occurrence_grain", Rule: "allocate only reported earmarked receipts to their original contributor appearance", Conservation: "exact"},
		{ID: "reported_direct@1", Status: "supported_at_source_occurrence_grain", Rule: "allocate nonmemo itemized-individual receipts directly and keep earmarked receipts in a separate exclusive bucket", Conservation: "exact"},
		{ID: "reported_committee_stop@1", Status: "supported_but_not_upstream_origin", Rule: "also stop at the immediate reported committee counterparty", Conservation: "exact"},
		{ID: "pooled_pro_rata@1", Status: "blocked", Rule: "allocate committee outflow across eligible inbound funds in proportion to an accepted funding denominator", Conservation: "not_executable", Blocker: "complete denominators, cash continuity, opening balances and cross-cycle availability are not established"},
		{ID: "chronological_fifo@1", Status: "blocked", Rule: "consume eligible inbound funds in reported chronological order", Conservation: "not_executable", Blocker: "reported dates do not establish cash availability or payment identity"},
		{ID: "path_replication@1", Status: "rejected", Rule: "copy the full downstream amount onto every reachable upstream path", Conservation: "fails_by_design", Blocker: "branching paths duplicate money"},
	}
}

// Compare builds a diagnostic over the dossier's disjoint component
// populations. It neither selects a policy nor promotes terminal eligibility.
func Compare(ctx context.Context, input Input) (Result, error) {
	out := Result{SchemaVersion: SchemaVersion, Policy: Policy, ExecutableSHA256: input.ExecutableSHA256, CandidateID: input.CandidateID, Cycle: input.Cycle, Input: input.Reference,
		TerminalDefinitions: terminalDefinitions(), AllocationMethods: allocationMethods(), Scenarios: []Scenario{}}
	if err := ctx.Err(); err != nil {
		return out, err
	}
	if input.CandidateID == "" || input.Cycle == "" || len(input.Committees) == 0 || !digest(input.ExecutableSHA256) ||
		!digest(input.Reference.DossierID) || !digest(input.Reference.DossierSHA256) ||
		!digest(input.Reference.ReceiptFactSetID) || !digest(input.Reference.ReceiptManifestSHA) || input.Reference.ReceiptSourceRelease == "" {
		return out, fmt.Errorf("candidate scope and exact dossier/receipt identities required")
	}
	sort.Slice(input.Committees, func(i, j int) bool { return input.Committees[i].CommitteeID < input.Committees[j].CommitteeID })
	seen := map[string]bool{}
	var included, memo, direct, earmarked, committee, residual accumulator
	for _, candidateCommittee := range input.Committees {
		if err := ctx.Err(); err != nil {
			return out, err
		}
		if !committeeflows.ValidCommitteeID(&candidateCommittee.CommitteeID) || seen[candidateCommittee.CommitteeID] ||
			(candidateCommittee.Authorization != "authorized" && candidateCommittee.Authorization != "unresolved") {
			return out, fmt.Errorf("invalid, duplicate or unsupported candidate-linked committee")
		}
		seen[candidateCommittee.CommitteeID] = true
		var conserved accumulator
		for _, component := range candidateCommittee.Receipts.Components {
			conserved.add(component.Measures)
			if component.Component == "memo_subtotal" {
				memo.add(component.Measures)
				continue
			}
			included.add(component.Measures)
			if candidateCommittee.Authorization != "authorized" {
				residual.add(component.Measures)
				continue
			}
			residual.addAccumulator(unknown(component.Measures))
			switch {
			case component.Component == "itemized_individual_only" && component.Role == "earmarked":
				earmarked.addAccumulator(known(component.Measures))
			case component.Component == "itemized_individual_only":
				direct.addAccumulator(known(component.Measures))
			case component.Component == "committee_flow_only":
				committee.addAccumulator(known(component.Measures))
			default:
				residual.addAccumulator(known(component.Measures))
			}
		}
		var total accumulator
		total.add(candidateCommittee.Receipts.Total)
		if !conserved.equal(total) {
			return out, fmt.Errorf("committee receipt component conservation failed")
		}
	}
	var partition accumulator
	for _, value := range []accumulator{direct, earmarked, committee, residual} {
		partition.addAccumulator(value)
	}
	if !partition.equal(included) {
		return out, fmt.Errorf("comparison population conservation failed")
	}
	out.Scope = Scope{
		State: "candidate_linked_known_and_unknown_nonmemo_schedule_a_appearances", Included: included.measures(), ExcludedMemo: memo.measures(),
		CompleteFunding: false, IndependentSpend: false,
		Limitations: []string{"not_complete_candidate_receipts", "unitemized_receipts_and_opening_cash_not_allocated", "source_appearances_are_not_resolved_people_or_organizations", "signed_observation_amounts_are_not_cash_availability", "independent_expenditures_are_separate"},
	}
	makeScenario := func(id, status, method string, definitions []string, d, e, p, u accumulator, notes []string) Scenario {
		var total accumulator
		for _, value := range []accumulator{d, e, p, u} {
			total.addAccumulator(value)
		}
		return Scenario{ID: id, Status: status, TerminalDefinitionIDs: definitions, AllocationMethodID: method,
			Direct: d.measures(), Earmarked: e.measures(), Proportional: p.measures(), Unresolved: u.measures(),
			Conserved: total.equal(included), Selected: false, Notes: notes}
	}
	zero := accumulator{}
	out.Scenarios = append(out.Scenarios,
		makeScenario("unallocated_evidence_baseline@1", "supported_baseline", "no_allocation@1", []string{}, zero, zero, zero, included,
			[]string{"no terminal claim; every included appearance remains unresolved"}),
	)
	var withoutEarmark accumulator
	for _, value := range []accumulator{direct, committee, residual} {
		withoutEarmark.addAccumulator(value)
	}
	out.Scenarios = append(out.Scenarios,
		makeScenario("explicit_earmark_only@1", "supported_partial_not_adopted", "explicit_earmark@1", []string{"explicit_earmark_origin@1"}, zero, earmarked, zero, withoutEarmark,
			[]string{"the conduit association carries no additional money", "the endpoint remains a reported appearance rather than a resolved person"}),
	)
	var directUnresolved accumulator
	directUnresolved.addAccumulator(committee)
	directUnresolved.addAccumulator(residual)
	out.Scenarios = append(out.Scenarios,
		makeScenario("reported_direct_appearances@1", "supported_partial_not_adopted", "reported_direct@1", []string{"reported_noncommittee_appearance@1", "explicit_earmark_origin@1"}, direct, earmarked, zero, directUnresolved,
			[]string{"direct and earmarked buckets are exclusive", "committee receipts remain unresolved upstream"}),
	)
	var stoppedDirect accumulator
	stoppedDirect.addAccumulator(direct)
	stoppedDirect.addAccumulator(committee)
	out.Scenarios = append(out.Scenarios,
		makeScenario("reported_committee_stop@1", "supported_counterparty_not_upstream_origin", "reported_committee_stop@1", []string{"reported_noncommittee_appearance@1", "explicit_earmark_origin@1", "reported_committee_counterparty@1"}, stoppedDirect, earmarked, zero, residual,
			[]string{"a committee is only the immediate reported source under this scenario", "this scenario does not meet the product goal of tracing committee chains"}),
		makeScenario("pooled_pro_rata@1", "blocked_not_calculated", "pooled_pro_rata@1", []string{"resolved_person_or_organization@1"}, zero, zero, zero, included,
			[]string{"no invented percentages; the entire scoped amount remains unresolved"}),
		makeScenario("chronological_fifo@1", "blocked_not_calculated", "chronological_fifo@1", []string{"resolved_person_or_organization@1"}, zero, zero, zero, included,
			[]string{"reported dates do not establish spendable-fund order; the entire scoped amount remains unresolved"}),
	)
	for _, scenario := range out.Scenarios {
		if !scenario.Conserved {
			return out, fmt.Errorf("scenario %s failed exact conservation", scenario.ID)
		}
	}
	body, err := json.Marshal(out)
	if err != nil {
		return out, err
	}
	h := sha256.Sum256(body)
	out.ComparisonID = hex.EncodeToString(h[:])
	return out, ctx.Err()
}
