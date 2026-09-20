package directattribution

import (
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/vedantadhobley/legal-tender/internal/calculation/fec/committeeflows"
	participants "github.com/vedantadhobley/legal-tender/internal/calculation/fec/receiptparticipants"
	"github.com/vedantadhobley/legal-tender/internal/calculation/fec/receipts"
)

var candidatePattern = regexp.MustCompile(`^[HSP][0-9A-Z]{8}$`)

type route struct {
	candidate string
	committee string
}

type candidateAccumulator struct {
	authorized, direct, earmarked, unresolved, memo accumulator
	reasons                                         map[string]accumulator
}

type worker struct {
	complete, outside accumulator
	candidates        map[string]*candidateAccumulator
}

type Collector struct {
	routes        map[string]route
	committees    map[string][]string
	workers       []worker
	authorization AuthorizationCensus
}

func NewCollector(linkages []receipts.LinkageFact, workers int) (*Collector, error) {
	if len(linkages) == 0 || workers < 1 || workers > 8 {
		return nil, fmt.Errorf("candidate-committee linkages and 1..8 workers required")
	}
	candidates := map[string]bool{}
	for _, fact := range linkages {
		if !candidatePattern.MatchString(fact.CandidateID) || !committeeflows.ValidCommitteeID(&fact.CommitteeID) {
			return nil, fmt.Errorf("invalid candidate-committee linkage identity")
		}
		candidates[fact.CandidateID] = true
	}
	ids := make([]string, 0, len(candidates))
	for id := range candidates {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	c := &Collector{routes: map[string]route{}, committees: map[string][]string{}, workers: make([]worker, workers)}
	c.authorization.CandidatesWithLinkageFacts = uint64(len(ids))
	for _, candidateID := range ids {
		for _, relationship := range receipts.AuthorizedCommitteeRelationships(candidateID, linkages) {
			switch relationship.State {
			case "authorized":
				c.authorization.AuthorizedRelationships++
				if prior, exists := c.routes[relationship.CommitteeID]; exists && prior.candidate != candidateID {
					return nil, fmt.Errorf("committee authorized to more than one candidate after shared-authorization policy")
				}
				c.routes[relationship.CommitteeID] = route{candidate: candidateID, committee: relationship.CommitteeID}
				c.committees[candidateID] = append(c.committees[candidateID], relationship.CommitteeID)
			case "unresolved":
				c.authorization.UnresolvedRelationships++
			case "unauthorized":
				c.authorization.UnauthorizedRelationships++
			default:
				return nil, fmt.Errorf("unsupported candidate-committee relationship state")
			}
		}
	}
	c.authorization.CandidatesWithAuthorization = uint64(len(c.committees))
	c.authorization.AuthorizedCommittees = uint64(len(c.routes))
	for candidateID := range c.committees {
		sort.Strings(c.committees[candidateID])
	}
	for index := range c.workers {
		c.workers[index] = worker{candidates: map[string]*candidateAccumulator{}}
	}
	return c, nil
}

func reason(row participants.Row) (string, error) {
	switch row.Component {
	case "committee_flow_only":
		return "committee_chain_unresolved", nil
	case "overlapping_individual_and_committee":
		return "individual_committee_role_conflict", nil
	case "unresolved_individual_class":
		return "individual_class_unresolved", nil
	case "other_reported_receipt":
		return "other_reported_source_role_unresolved", nil
	case "unknown_amount":
		return "amount_unresolved", nil
	case "unresolved_recipient":
		return "", fmt.Errorf("authorized scope contains unresolved recipient")
	case "memo_subtotal":
		return "", fmt.Errorf("memo row reached nonmemo attribution branch")
	case "itemized_individual_only":
		return "", fmt.Errorf("allocatable itemized row reached unresolved branch")
	default:
		return "", fmt.Errorf("unsupported participant component %q", row.Component)
	}
}

func (c *Collector) Observe(workerID int, row participants.Row) error {
	if workerID < 0 || workerID >= len(c.workers) || row.Ordinal <= 0 {
		return fmt.Errorf("invalid attribution worker or participant occurrence")
	}
	w := &c.workers[workerID]
	w.complete.observe(row.Amount)
	if !committeeflows.ValidCommitteeID(row.Recipient) {
		w.outside.observe(row.Amount)
		return nil
	}
	selected, ok := c.routes[*row.Recipient]
	if !ok {
		w.outside.observe(row.Amount)
		return nil
	}
	a := w.candidates[selected.candidate]
	if a == nil {
		a = &candidateAccumulator{reasons: map[string]accumulator{}}
		w.candidates[selected.candidate] = a
	}
	a.authorized.observe(row.Amount)
	if row.Memo {
		if row.Component != "memo_subtotal" {
			return fmt.Errorf("memo flag and participant component disagree")
		}
		a.memo.observe(row.Amount)
		return nil
	}
	if row.Amount == nil {
		a.unresolved.observe(nil)
		v := a.reasons["amount_unresolved"]
		v.observe(nil)
		a.reasons["amount_unresolved"] = v
		return nil
	}
	if row.Component == "itemized_individual_only" {
		if row.IndividualDecision != "included" || row.IndividualOverlap {
			return fmt.Errorf("itemized participant membership disagrees")
		}
		if row.ReceiptRole == committeeflows.RoleEarmarked {
			a.earmarked.observe(row.Amount)
		} else {
			a.direct.observe(row.Amount)
		}
		return nil
	}
	why, err := reason(row)
	if err != nil {
		return err
	}
	a.unresolved.observe(row.Amount)
	v := a.reasons[why]
	v.observe(row.Amount)
	a.reasons[strings.Clone(why)] = v
	return nil
}

func (c *Collector) Finish(expected uint64) (Census, []CandidateResult, error) {
	all := map[string]*candidateAccumulator{}
	var complete, outside accumulator
	for _, worker := range c.workers {
		complete.add(worker.complete)
		outside.add(worker.outside)
		for candidateID, value := range worker.candidates {
			target := all[candidateID]
			if target == nil {
				target = &candidateAccumulator{reasons: map[string]accumulator{}}
				all[candidateID] = target
			}
			target.authorized.add(value.authorized)
			target.direct.add(value.direct)
			target.earmarked.add(value.earmarked)
			target.unresolved.add(value.unresolved)
			target.memo.add(value.memo)
			for reason, measures := range value.reasons {
				merged := target.reasons[reason]
				merged.add(measures)
				target.reasons[reason] = merged
			}
		}
	}
	if complete.rows != expected {
		return Census{}, nil, fmt.Errorf("participant population differs from expected complete scan")
	}
	ids := make([]string, 0, len(c.committees))
	for candidateID := range c.committees {
		ids = append(ids, candidateID)
	}
	sort.Strings(ids)
	results := make([]CandidateResult, 0, len(ids))
	var authorized, direct, earmarked, unresolved, memo accumulator
	for _, candidateID := range ids {
		value := all[candidateID]
		if value == nil {
			value = &candidateAccumulator{reasons: map[string]accumulator{}}
		}
		var partition accumulator
		partition.add(value.direct)
		partition.add(value.earmarked)
		partition.add(value.unresolved)
		partition.add(value.memo)
		if !partition.equal(value.authorized) {
			return Census{}, nil, fmt.Errorf("candidate attribution population does not conserve")
		}
		var reasonTotal accumulator
		for _, measures := range value.reasons {
			reasonTotal.add(measures)
		}
		if !reasonTotal.equal(value.unresolved) {
			return Census{}, nil, fmt.Errorf("candidate unresolved reasons do not conserve")
		}
		var nonmemo accumulator
		nonmemo.add(value.direct)
		nonmemo.add(value.earmarked)
		nonmemo.add(value.unresolved)
		results = append(results, CandidateResult{CandidateID: candidateID, State: "partial_source_appearance_attribution",
			AuthorizedCommittees: append([]string{}, c.committees[candidateID]...), AuthorizedScope: value.authorized.measures(), IncludedNonmemo: nonmemo.measures(),
			Direct: value.direct.measures(), Earmarked: value.earmarked.measures(), Unresolved: value.unresolved.measures(), ExcludedMemo: value.memo.measures(),
			UnresolvedReasons: sortedReasonMeasures(value.reasons)})
		authorized.add(value.authorized)
		direct.add(value.direct)
		earmarked.add(value.earmarked)
		unresolved.add(value.unresolved)
		memo.add(value.memo)
	}
	var completePartition accumulator
	completePartition.add(outside)
	completePartition.add(authorized)
	if !completePartition.equal(complete) {
		return Census{}, nil, fmt.Errorf("complete attribution scope does not conserve")
	}
	var nonmemo accumulator
	nonmemo.add(direct)
	nonmemo.add(earmarked)
	nonmemo.add(unresolved)
	var authorizedPartition accumulator
	authorizedPartition.add(nonmemo)
	authorizedPartition.add(memo)
	if !authorizedPartition.equal(authorized) {
		return Census{}, nil, fmt.Errorf("authorized attribution census does not conserve")
	}
	return Census{CompleteParticipants: complete.measures(), OutsideAuthorizationScope: outside.measures(), AuthorizedScope: authorized.measures(), IncludedNonmemo: nonmemo.measures(), Direct: direct.measures(), Earmarked: earmarked.measures(), Unresolved: unresolved.measures(), ExcludedMemo: memo.measures()}, results, nil
}

func (c *Collector) Authorization() AuthorizationCensus { return c.authorization }
