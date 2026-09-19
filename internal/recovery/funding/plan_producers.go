package funding

import (
	"encoding/json"
	"strconv"

	cr "github.com/vedantadhobley/legal-tender/internal/calculation/fec/candidateresolution"
	flow "github.com/vedantadhobley/legal-tender/internal/calculation/fec/flowreconciliation"
	ie "github.com/vedantadhobley/legal-tender/internal/calculation/fec/independentexpenditures"
	c "github.com/vedantadhobley/legal-tender/internal/calculation/fec/receiptconduits"
	p "github.com/vedantadhobley/legal-tender/internal/calculation/fec/receiptparticipants"
	r "github.com/vedantadhobley/legal-tender/internal/calculation/fec/receiptreferences"
	fg "github.com/vedantadhobley/legal-tender/internal/projection/arango/fundinggeneration"
	rg "github.com/vedantadhobley/legal-tender/internal/projection/arango/receiptgraph"
)

type dependencyRule struct {
	kind     string
	min, max int
	output   bool
}
type producerSpec struct {
	operation, implementation string
	dependencies              map[string]dependencyRule
}

// This is a closed mapping of actual publisher parameters and output artifacts.
// It is deliberately not a rule that treats every non-file reference as input.
func producer(kind, schema string) (producerSpec, bool) {
	one := func(kind string) dependencyRule { return dependencyRule{kind, 1, 1, false} }
	many := func(kind string) dependencyRule { return dependencyRule{kind, 1, 0, false} }
	out := dependencyRule{"file", 1, 1, true}
	var s producerSpec
	want := ""
	switch kind {
	case "participants":
		want = p.Version
		s = producerSpec{"receipt_participants", "receiptparticipants.Run", map[string]dependencyRule{"receipt_facts": one("a_facts"), "participant_shard": {"file", 1, 0, true}}}
	case "references":
		want = r.Version
		s = producerSpec{"receipt_references", "receiptreferences.Run", map[string]dependencyRule{"receipt_facts": one("a_facts"), "reference_decisions": out, "lookup_evidence": out, "exact_incidences": out, "neighbors": out}}
	case "topology":
		want = r.TopologyVersion
		s = producerSpec{"receipt_topology", "receiptreferences.RunTopology", map[string]dependencyRule{"reference_join": one("references"), "receipt_facts": one("a_facts"), "endpoints": out}}
	case "conduits":
		want = c.Version
		s = producerSpec{"receipt_conduits", "receiptconduits.Run", map[string]dependencyRule{"participants": one("participants"), "topology": one("topology"), "receipt_facts": one("a_facts"), "conduit_decisions": out}}
		if schema == c.GroupVersion {
			want, s.operation = c.GroupVersion, "shared_conduit_groups"
			s.dependencies["one_to_one_baseline"] = one("conduits")
			s.dependencies["group_decisions"] = out
		}
	case "flow_calculation":
		want = flow.Version
		s = producerSpec{"committee_flow_reconciliation", "flowreconciliation.Publish", map[string]dependencyRule{"coordinated_release": one("release"), "receiver_facts": one("a_facts"), "sender_facts": one("b_facts"), "receiver_observations": out, "sender_observations": out, "reconciliation_assertions": out}}
	case "flow_bundle":
		want = flow.BundleVersion
		s = producerSpec{"committee_flow_readiness", "flowreconciliation.PublishBundle", map[string]dependencyRule{"flow_calculation": one("flow_calculation"), "coordinated_release": one("release"), "receiver_facts": one("a_facts"), "sender_facts": one("b_facts"), "committee_master": one("classic_facts")}}
	case "effective":
		want = ie.ManifestSchemaVersion
		s = producerSpec{"effective_expenditures", "independentexpenditures.Publish", map[string]dependencyRule{"schedule_e_facts": one("e_facts"), "effective_results": out, "effective_exceptions": out}}
	case "resolution":
		want = cr.ManifestSchemaVersion
		s = producerSpec{"candidate_resolution", "candidateresolution.Publish", map[string]dependencyRule{"effective_expenditures": one("effective"), "schedule_e_facts": one("e_facts"), "candidate_master": one("classic_facts"), "resolution_decisions": out}}
	case "resolved_aggregate":
		want = cr.AggregateManifestSchemaVersion
		s = producerSpec{"resolved_expenditure_groups", "candidateresolution.PublishAggregate", map[string]dependencyRule{"candidate_resolution": one("resolution"), "aggregate_results": out, "aggregate_exceptions": out}}
	case "resolved_bundle":
		want = cr.ResolvedProjectionBundleSchemaVersion
		s = producerSpec{"outside_spending_readiness", "candidateresolution.PublishResolvedProjectionBundle", map[string]dependencyRule{"resolved_aggregate": one("resolved_aggregate"), "candidate_resolution": one("resolution"), "candidate_master": one("classic_facts"), "committee_master": one("classic_facts")}}
	case "receipt_projection":
		want = rg.CycleVersion
		s = producerSpec{"receipt_graph", "receiptgraph.Run", receiptDependencies()}
	case "shared_projection":
		want = rg.SharedVersion
		s = producerSpec{"shared_conduit_graph", "receiptgraph.CycleReader.PublishShared", map[string]dependencyRule{"base_receipt_projection": one("receipt_projection"), "conduits": one("conduits")}}
	case "generation":
		want = fg.Version
		s = producerSpec{"funding_generation", "fundinggeneration.Verify", receiptDependencies()}
		for role, rule := range map[string]dependencyRule{
			"receipt_projection": one("receipt_projection"), "committee_flow_bundle": one("flow_bundle"), "outside_spending_bundle": one("resolved_bundle"), "coordinated_release": one("release"),
			"reference_proof_fact": many("classic_facts"), "reference_proof_occurrences": many("classic_occurrences"), "reference_proof_origin": many("release"), "reference_proof_target": many("release"),
			"outside_membership_facts": one("e_facts"), "outside_membership_origin": one("release"), "outside_membership_target": one("release"),
		} {
			s.dependencies[role] = rule
		}
	case "shared_generation":
		want = "legal-tender.funding-evidence-generation.shared-conduits.v1"
		s = producerSpec{"shared_funding_generation", "fundinggeneration.Reader.PublishShared", map[string]dependencyRule{"base_generation": one("generation"), "extension_projection": one("shared_projection"), "extension_calculation": one("conduits")}}
	default:
		return s, false
	}
	return s, schema == want
}

func receiptDependencies() map[string]dependencyRule {
	m := map[string]dependencyRule{}
	for role, kind := range map[string]string{"participants": "participants", "conduits": "conduits", "receipt_facts": "a_facts", "committee_facts": "classic_facts", "candidate_facts": "classic_facts", "linkage_facts": "classic_facts"} {
		m[role] = dependencyRule{kind, 1, 1, false}
	}
	return m
}

func (p *planner) cycle(key string) string {
	// These bytes already passed expand's closed typed decoding. This common
	// field extraction is not dependency discovery or acceptance of new fields.
	var header struct {
		Cycle string `json:"cycle"`
	}
	json.Unmarshal(p.metadata[key], &header)
	n := p.nodes[key]
	switch n.Reference.Kind {
	case "receipt_projection":
		var v rg.CycleManifest
		json.Unmarshal(p.metadata[key], &v)
		header.Cycle = v.Inputs.Cycle
	case "shared_projection":
		var v rg.SharedManifest
		json.Unmarshal(p.metadata[key], &v)
		header.Cycle = v.Cycle
	case "shared_generation":
		var v fg.SharedGeneration
		json.Unmarshal(p.metadata[key], &v)
		header.Cycle = v.Base.Cycle
	}
	value, err := strconv.Atoi(header.Cycle)
	if err != nil || len(header.Cycle) != 4 || strconv.Itoa(value) != header.Cycle || value%2 != 0 {
		p.fail(key, "unsupported_cycle_scope", "exact even-year source partition required")
	}
	return header.Cycle
}

// Pin supported publisher/method versions independently of the wire schema.
// Normal publisher validation remains required for all policy fields/bodies.
func (p *planner) checkProducerVersions(key string) {
	b := p.metadata[key]
	check := func(got, want string) {
		if got != want {
			p.fail(key, "unsupported_producer_contract", got)
		}
	}
	switch p.nodes[key].Reference.Kind {
	case "effective":
		var v ie.Manifest
		json.Unmarshal(b, &v)
		check(v.PublisherVersion, ie.PublisherVersion)
		check(v.CalculationVersion, ie.ContractVersion)
	case "resolution":
		var v cr.Manifest
		json.Unmarshal(b, &v)
		check(v.PublisherVersion, cr.PublisherVersion)
		check(v.CalculationVersion, cr.ContractVersion)
		check(v.Method.Version, cr.MethodVersion)
	case "resolved_aggregate":
		var v cr.AggregateManifest
		json.Unmarshal(b, &v)
		check(v.PublisherVersion, cr.AggregatePublisherVersion)
		check(v.GroupingPolicy.Version, cr.AggregateGroupingPolicyVersion)
	case "resolved_bundle":
		var v cr.ResolvedProjectionBundleManifest
		json.Unmarshal(b, &v)
		check(v.PublisherVersion, cr.ResolvedProjectionBundlePublisherVersion)
	case "topology":
		var v r.TopologyResult
		json.Unmarshal(b, &v)
		check(v.Policy, r.TopologyPolicy)
	case "flow_calculation":
		var v flow.Result
		json.Unmarshal(b, &v)
		check(v.Policy.Matcher, flow.MatchPolicy)
	}
}
