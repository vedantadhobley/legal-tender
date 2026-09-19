package fundinggeneration

import (
	"context"
	"fmt"

	assessment "github.com/vedantadhobley/legal-tender/internal/calculation/fec/terminalassessment"
	flow "github.com/vedantadhobley/legal-tender/internal/projection/arango/flowevidence"
	"github.com/vedantadhobley/legal-tender/internal/projection/arango/graphread"
)

const TerminalAssessmentVersion = "legal-tender.terminal-source-assessment.v1"

type TerminalWitness struct {
	Kind        string           `json:"kind"`
	Ledger      flow.Ledger      `json:"ledger"`
	State       string           `json:"state"`
	CommitteeID string           `json:"committee_id"`
	Identity    *graphread.Facet `json:"identity_evidence"`
	Link        *graphread.Link  `json:"incident_observation"`
	Evidence    *graphread.Item  `json:"source_evidence"`
}

type TerminalAssessment struct {
	SchemaVersion      string            `json:"schema_version"`
	ResultID           string            `json:"result_id"`
	GenerationID       string            `json:"generation_id"`
	GenerationSHA256   string            `json:"generation_sha256"`
	BuildSHA256        string            `json:"consumer_executable_sha256"`
	Cycle              string            `json:"cycle"`
	Scope              string            `json:"scope"`
	Families           []Family          `json:"assessed_families"`
	UnassessedFamilies []Family          `json:"not_assessed_families"`
	Assessment         assessment.Result `json:"assessment"`
	Witnesses          []TerminalWitness `json:"automatic_source_witnesses"`
	Checks             []string          `json:"checks"`
	Limitations        []string          `json:"limitations"`
	TerminalEligible   bool              `json:"terminal_attribution_eligible"`
}

func (r *Reader) AssessTerminalSources(ctx context.Context, expected string, progress func(string)) (TerminalAssessment, error) {
	var out TerminalAssessment
	if expected != "" && !validDigest(expected) {
		return out, fmt.Errorf("invalid expected assessment identity")
	}
	if progress == nil {
		progress = func(string) {}
	}
	progress("assessing complete selected committee topology; ledgers remain separate")
	committees := []assessment.Committee{}
	for _, c := range r.flow.CommitteeIdentities() {
		v := assessment.Committee{ID: c.ID, IdentityState: c.State, MasterFactSetID: c.FactSetID}
		if c.FactID != "" {
			id := c.FactID
			v.MasterFactID = &id
		}
		committees = append(committees, v)
	}
	ledgers := map[flow.Ledger][]assessment.Observation{}
	for _, side := range []flow.Ledger{flow.ScheduleA, flow.ScheduleB} {
		ledgers[side] = []assessment.Observation{}
		if err := r.flow.VisitLinks(ctx, side, func(e graphread.Link) error {
			ledgers[side] = append(ledgers[side], assessment.Observation{Key: e.Key, From: e.From, To: e.To})
			return nil
		}); err != nil {
			return out, err
		}
	}
	value, err := assessment.Analyze(ctx, committees, ledgers[flow.ScheduleA], ledgers[flow.ScheduleB])
	if err != nil {
		return out, err
	}
	if len(value.Committees) != r.generation.CommitteeFlow.Entities {
		return out, fmt.Errorf("assessment endpoint census differs from generation")
	}
	out = TerminalAssessment{SchemaVersion: TerminalAssessmentVersion, GenerationID: r.generation.GenerationID, GenerationSHA256: r.manifestSHA, BuildSHA256: r.consumerBuild, Cycle: r.generation.Cycle,
		Scope:    "complete_selected_a_and_b_committee_endpoint_topologies_not_all_receipt_origins",
		Families: []Family{}, UnassessedFamilies: []Family{}, Assessment: value, Witnesses: []TerminalWitness{},
		Checks:      []string{"fresh_generation_source_and_graph_verification", "complete_selected_observation_and_endpoint_conservation", "complete_iterative_scc_no_hop_or_path_cutoff", "separate_ledgers_and_explicit_absent_membership", "data_selected_identity_and_full_source_witnesses", "completion_boundaries_rechecked"},
		Limitations: []string{"hypothesis_matches_are_not_terminal_source_classifications", "master_presence_is_not_financial_origin_or_person_corporation_resolution", "same_cycle_master_gaps_do_not_prove_absent_historical_registration", "ledger_disagreement_is_not_proof_either_reporter_is_wrong", "source_witness_is_illustrative_not_the_proof_of_zero_incoming_edges", "source_roles_of_all_receipt_appearances_not_assessed", "no_amounts_allocated_no_path_money_sum", "immutable_publications_required_not_cross_database_transaction"},
	}
	for _, family := range r.generation.Families {
		var count uint64
		switch family.Kind {
		case "receiver_reported_committee_observation":
			count = value.A.Observations
		case "sender_reported_committee_observation":
			count = value.B.Observations
		default:
			out.UnassessedFamilies = append(out.UnassessedFamilies, family)
			continue
		}
		if count != family.Membership {
			return out, fmt.Errorf("assessment family census differs")
		}
		out.Families = append(out.Families, family)
	}
	if len(out.Families) != 2 {
		return out, fmt.Errorf("both exact committee families required")
	}
	for _, side := range []flow.Ledger{flow.ScheduleA, flow.ScheduleB} {
		for _, selected := range terminalSelections(value, side) {
			w := TerminalWitness{Kind: selected.kind, Ledger: side, State: "no_matching_selection_population", CommitteeID: selected.id}
			if selected.id != "" {
				progress("verifying assessment witness " + string(side) + ":" + selected.kind)
				link, err := selectTerminalIncident(side, selected.id, ledgers[side])
				if err != nil {
					return out, err
				}
				facet, err := r.flow.Facet(ctx, selected.id)
				if err != nil {
					return out, err
				}
				if facet.State != "present" {
					return out, fmt.Errorf("selected identity witness absent")
				}
				item, err := r.flow.PathEvidence(ctx, side, link.Key)
				if err != nil {
					return out, err
				}
				w.State, w.Identity, w.Link, w.Evidence = "verified", &facet, &link, &item
			}
			out.Witnesses = append(out.Witnesses, w)
		}
	}
	if err := r.VerifyCompletion(ctx); err != nil {
		return out, err
	}
	out.ResultID = valueID(out)
	if expected != "" && out.ResultID != expected {
		return out, fmt.Errorf("terminal assessment replay identity differs")
	}
	return out, nil
}

type terminalSelection struct{ kind, id string }

func terminalSelections(v assessment.Result, side flow.Ledger) []terminalSelection {
	ledger, other := v.A, v.B
	if side == flow.ScheduleB {
		ledger, other = v.B, v.A
	}
	out := []terminalSelection{{kind: "identified_frontier"}, {kind: "missing_master_frontier"}, {kind: "cyclic_root"}, {kind: "nonroot_cycle"}, {kind: "cross_ledger_frontier_disagreement"}}
	components := map[string]assessment.Component{}
	for _, c := range ledger.Components {
		components[c.ID] = c
	}
	for i, n := range ledger.Nodes {
		if n.State == assessment.Absent {
			continue
		}
		c := components[n.ComponentID]
		predicates := []bool{
			*n.IdentityFrontier,
			*n.Frontier && v.Committees[i].IdentityState == assessment.MissingMaster,
			c.Cyclic && c.Incoming == 0,
			c.Cyclic && c.Incoming > 0,
			other.Nodes[i].State != assessment.Absent && n.State != other.Nodes[i].State,
		}
		for j, match := range predicates {
			if match && out[j].id == "" {
				out[j].id = n.CommitteeID
			}
		}
	}
	return out
}

func selectTerminalIncident(side flow.Ledger, id string, observations []assessment.Observation) (graphread.Link, error) {
	family := "receiver_reported_committee_observation"
	if side == flow.ScheduleB {
		family = "sender_reported_committee_observation"
	}
	var out graphread.Link
	for _, e := range observations {
		if (e.From == id || e.To == id) && (out.Key == "" || e.Key < out.Key) {
			out = graphread.Link{Family: family, Key: e.Key, From: e.From, To: e.To}
		}
	}
	if out.Key == "" {
		return out, fmt.Errorf("selected topology witness has no incident observation")
	}
	return out, nil
}
