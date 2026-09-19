package fundinggeneration

import (
	flow "github.com/vedantadhobley/legal-tender/internal/projection/arango/flowevidence"
	ie "github.com/vedantadhobley/legal-tender/internal/projection/arango/independentexpenditures"
	receipts "github.com/vedantadhobley/legal-tender/internal/projection/arango/receiptgraph"
	occ "github.com/vedantadhobley/legal-tender/internal/source/fec/occurrence"
)

// All families are required. Counts describe their declared grains, not totals
// to sum across families. In particular both Schedule A projections overlap.
func assemble(build string, r receipts.CycleView, f flow.View, flowDB string, components uint64, e ie.ResolvedView, proofs []occ.ClassicReferenceProof, em occ.ScheduleEReleaseMembership) Result {
	a := r.Inputs.Facts.ID
	out := Result{
		SchemaVersion: Version, Policy: Policy, BuildSHA256: build, State: "verified_declared_evidence_families", Cycle: r.Inputs.Cycle,
		Release: occ.ReferenceIdentity{ID: f.Inputs.ReleaseID, SHA256: f.Inputs.ReleaseSHA256}, Receipts: r, CommitteeFlow: f, OutsideSpending: e, ReferenceProofs: proofs, OutsideMembership: em,
		Families: []Family{
			{"reported_receipt", r.Database, "reported_receipts", r.Projection.ID, "schedule_a", a, "all", "source_occurrence", "reported_signed_amount_not_effective_payment", r.Counts["reported_receipts"], "schedule_a_occurrences"},
			{"conduit_association", r.Database, "reported_conduit_associations", r.Projection.ID, "evidence_only", a, "all", "qualified_source_association", "no_additional_money", r.Counts["reported_conduit_associations"], "schedule_a_occurrences"},
			{"candidate_authorization_context", r.Database, "candidate_authorization_context", r.Projection.ID, "reference", r.Inputs.Linkages.ID, "all_preserve_authorization_state", "committee_candidate_context", "not_a_payment", r.Counts["candidate_authorization_context"], "none"},
			{"receiver_reported_committee_observation", flowDB, "receiver_reported_observations", f.ProjectionID, "schedule_a", a, "selected_receiver_policy", "source_occurrence", "reported_signed_amount_not_effective_payment", f.A.Rows, "schedule_a_occurrences"},
			{"sender_reported_committee_observation", flowDB, "sender_reported_observations", f.ProjectionID, "schedule_b", f.Inputs.B.FactSetID, "selected_sender_policy", "source_occurrence", "reported_signed_amount_not_reconciled_payment", f.B.Rows, "schedule_b_occurrences"},
			{"reconciliation_candidate", flowDB, "reconciliation_components", f.ProjectionID, "evidence_only", "", "all", "candidate_component", "not_an_additional_payment", components, "schedule_a_and_b_observations"},
			{"independent_support", e.Database, "independent_expenditure_edges", e.ProjectionID, "schedule_e", e.Inputs.ScheduleEFactSetID, "support_oppose=S", "resolved_spender_candidate_stance_group", "effective_independent_expenditure_not_candidate_receipt", e.Counts.SupportEdges, "schedule_e_effective_decisions"},
			{"independent_opposition", e.Database, "independent_expenditure_edges", e.ProjectionID, "schedule_e", e.Inputs.ScheduleEFactSetID, "support_oppose=O", "resolved_spender_candidate_stance_group", "effective_independent_expenditure_not_candidate_receipt", e.Counts.OppositionEdges, "schedule_e_effective_decisions"},
		},
		Checks:      []string{"exact_completed_receipt_ancestry_schema_and_counts", "exact_shared_schedule_a_fact_set", "complete_reference_content_and_separate_provenance", "exact_schedule_e_release_membership", "full_selected_sender_receiver_graph_readback", "full_resolved_outside_graph_readback", "separate_ledger_and_stance_coverage", "typed_endpoint_namespaces", "all_completion_boundaries_rechecked"},
		Limitations: []string{"declared_relationship_families_not_all_source_records_as_payment_edges", "receipt_readiness_uses_published_full_readback_not_a_new_full_field_scan", "receiver_and_sender_are_selected_cohorts_with_explicit_source_population", "schedule_a_receipt_and_receiver_views_overlap_do_not_sum", "reconciliation_candidates_do_not_establish_one_economic_transfer", "authorization_state_must_be_checked_before_candidate_path_use", "shared_fec_identifier_is_not_person_or_corporation_resolution", "missing_masters_and_unprojectable_outside_decisions_remain_unresolved", "namespace_mapping_does_not_assert_endpoint_presence_in_every_projection", "no_all_candidate_path_or_chronological_allocation_gate", "no_combined_money_total_or_terminal_policy", "other_cycles_and_weekly_activation_not_accepted"},
	}
	for _, p := range []struct {
		id, db, committeePrefix, candidatePrefix string
		candidates                               bool
	}{
		{r.Projection.ID, r.Database, "", "", true},
		{f.ProjectionID, flowDB, "", "", false},
		{e.ProjectionID, e.Database, "committee_", "candidate_", true},
	} {
		out.EndpointNamespaces = append(out.EndpointNamespaces, EndpointNamespace{p.id, p.db, "entities", "committee", p.committeePrefix, `^C[0-9]{8}$`, "same_cycle_exact_reported_fec_id_preserve_projection_context"})
		if p.candidates {
			out.EndpointNamespaces = append(out.EndpointNamespaces, EndpointNamespace{p.id, p.db, "entities", "candidate", p.candidatePrefix, `^[HSP][A-Z0-9]{8}$`, "same_cycle_exact_reported_fec_id_preserve_projection_context"})
		}
	}
	return out
}
