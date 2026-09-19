package fundinggeneration

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"

	p "github.com/vedantadhobley/legal-tender/internal/calculation/fec/receiptparticipants"
	roles "github.com/vedantadhobley/legal-tender/internal/calculation/fec/receiptroles"
	r "github.com/vedantadhobley/legal-tender/internal/projection/arango/receiptgraph"
)

const ReceiptRolesVersion = "legal-tender.terminal-receipt-role-profile.v1"

type RoleWitness struct {
	Kinds     []string    `json:"selection_kinds"`
	Recipient string      `json:"reported_recipient_committee_id"`
	Role      roles.Key   `json:"role"`
	Entry     r.PathEntry `json:"receipt_source_and_graph_evidence"`
}
type ReceiptRoles struct {
	SchemaVersion     string             `json:"schema_version"`
	ResultID          string             `json:"result_id"`
	GenerationID      string             `json:"generation_id"`
	GenerationSHA256  string             `json:"generation_sha256"`
	BuildSHA256       string             `json:"consumer_executable_sha256"`
	Boundary          TerminalAssessment `json:"boundary_assessment"`
	ParticipantInput  r.Reference        `json:"participant_input"`
	MasterInput       r.Reference        `json:"reported_source_master_input"`
	Census            p.Census           `json:"verified_participant_census"`
	Profiles          roles.Result       `json:"reported_receipt_roles"`
	CommitteeEvidence []json.RawMessage  `json:"reported_committee_master_evidence"`
	Witnesses         []RoleWitness      `json:"automatic_source_witnesses"`
	Limitations       []string           `json:"limitations"`
	TerminalEligible  bool               `json:"terminal_attribution_eligible"`
}

func (reader *Reader) ProfileReceiptRoles(ctx context.Context, workers int, expected string, progress func(string)) (ReceiptRoles, error) {
	var out ReceiptRoles
	if workers < 1 || workers > 8 || expected != "" && !validDigest(expected) {
		return out, fmt.Errorf("1..8 workers and optional exact profile replay identity required")
	}
	if progress == nil {
		progress = func(string) {}
	}
	boundary, err := reader.AssessTerminalSources(ctx, "", progress)
	if err != nil {
		return out, err
	}
	ids := make([]string, 0, len(boundary.Assessment.Committees))
	for _, c := range boundary.Assessment.Committees {
		ids = append(ids, c.ID)
	}
	masters := reader.receipts.CommitteeMasterFacts()
	collector, err := roles.New(ids, masters, workers)
	if err != nil {
		return out, err
	}
	progress("profiling all compact participant occurrences with bounded parallel readers")
	census, err := reader.receipts.ScanParticipants(ctx, workers, collector.Observe, progress)
	if err != nil {
		return out, err
	}
	if census.Rows != reader.generation.Receipts.SourceRows {
		return out, fmt.Errorf("profile participant population differs from generation")
	}
	profiles, err := collector.Finish(census.Rows)
	if err != nil {
		return out, err
	}
	out = ReceiptRoles{SchemaVersion: ReceiptRolesVersion, GenerationID: reader.generation.GenerationID, GenerationSHA256: reader.manifestSHA, BuildSHA256: reader.consumerBuild, Boundary: boundary, ParticipantInput: reader.generation.Receipts.Inputs.Participants, MasterInput: reader.generation.Receipts.Inputs.Committees, Census: census, Profiles: profiles, CommitteeEvidence: []json.RawMessage{}, Witnesses: []RoleWitness{}, Limitations: []string{
		"source_occurrence_counts_not_distinct_people_effective_payments_or_dollars",
		"schedule_a_role_profiles_shared_by_both_topologies_not_schedule_b_receipt_data",
		"zero_profile_means_no_occurrences_in_exact_fact_set_not_no_funding",
		"role_groups_are_joint_partitions_annotation_maps_are_separate_marginals",
		"reported_entity_labels_employer_and_connected_organization_text_are_not_resolved_identities",
		"reported_master_attributes_are_assertions_not_evidence_of_corporate_payment",
		"all_index_rows_verified_full_source_and_live_receipt_edges_only_for_selected_witnesses",
		"structured_conduit_annotation_not_qualified_conduit_join_or_additional_money",
		"historical_registration_and_name_based_identity_resolution_not_evaluated",
		"employer_occupation_and_name_text_retained_in_full_facts_not_profiled_corpus_wide",
		"unchanged_terminal_hypotheses_no_terminal_policy_or_allocation_selected",
	}}
	if err := reader.flow.VisitCommitteeDocuments(ctx, func(id string, doc json.RawMessage) error {
		out.CommitteeEvidence = append(out.CommitteeEvidence, doc)
		return nil
	}); err != nil {
		return out, err
	}
	if len(out.CommitteeEvidence) != len(profiles.Profiles) {
		return out, fmt.Errorf("profile master evidence population differs")
	}
	for _, pick := range roleSelections(profiles) {
		progress(fmt.Sprintf("verifying reported-role witness at source ordinal %d", pick.ordinal))
		entry, err := reader.receipts.PathEntry(ctx, "reported_receipt", pick.ordinal)
		if err != nil {
			return out, err
		}
		if err := checkRoleWitness(pick, entry, masters); err != nil {
			return out, err
		}
		out.Witnesses = append(out.Witnesses, RoleWitness{Kinds: pick.kinds, Recipient: pick.recipient, Role: pick.key, Entry: entry})
	}
	if err := reader.VerifyCompletion(ctx); err != nil {
		return out, err
	}
	out.ResultID = valueID(out)
	if expected != "" && out.ResultID != expected {
		return out, fmt.Errorf("receipt role profile replay identity differs")
	}
	return out, nil
}

type rolePick struct {
	ordinal   uint64
	recipient string
	key       roles.Key
	kinds     []string
}

func roleSelections(v roles.Result) []rolePick {
	byKind := map[string]rolePick{}
	for _, profile := range v.Profiles {
		for _, g := range profile.Groups {
			kinds := []string{"source_route:" + g.Key.Route, "source_identity:" + g.Key.SourceIdentity}
			if g.Key.Conflict {
				kinds = append(kinds, "entity_conflict")
			}
			if g.Key.Overlap {
				kinds = append(kinds, "publisher_individual_overlap")
			}
			for _, kind := range kinds {
				prior, ok := byKind[kind]
				if !ok || g.Counts.First < prior.ordinal {
					byKind[kind] = rolePick{ordinal: g.Counts.First, recipient: profile.CommitteeID, key: g.Key}
				}
			}
		}
	}
	byOrdinal := map[uint64]rolePick{}
	for kind, pick := range byKind {
		v := byOrdinal[pick.ordinal]
		if v.ordinal == 0 {
			v = pick
		}
		v.kinds = append(v.kinds, kind)
		byOrdinal[pick.ordinal] = v
	}
	out := make([]rolePick, 0, len(byOrdinal))
	for _, v := range byOrdinal {
		sort.Strings(v.kinds)
		out = append(out, v)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ordinal < out[j].ordinal })
	return out
}
func checkRoleWitness(pick rolePick, entry r.PathEntry, masters map[string]string) error {
	var source p.Inspection
	if err := json.Unmarshal(entry.Source, &source); err != nil {
		return err
	}
	k, err := roles.Classify(source.Participant, masters)
	if err != nil {
		return err
	}
	if source.IdentityResolved || source.FinancialEligibility || source.Participant.Recipient == nil || *source.Participant.Recipient != pick.recipient || uint64(source.Participant.Ordinal) != pick.ordinal || !reflect.DeepEqual(k, pick.key) || entry.State != "available" || entry.Link == nil || entry.Item == nil || entry.Link.To != pick.recipient || entry.Link.From != source.AppearanceID || entry.Item.Key != source.AppearanceID {
		return fmt.Errorf("reported-role witness differs from verified profile or graph")
	}
	return nil
}
