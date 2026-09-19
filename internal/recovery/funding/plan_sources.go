package funding

import (
	"encoding/json"
	"fmt"

	flowgraph "github.com/vedantadhobley/legal-tender/internal/projection/arango/flowevidence"
	fg "github.com/vedantadhobley/legal-tender/internal/projection/arango/fundinggeneration"
	iegraph "github.com/vedantadhobley/legal-tender/internal/projection/arango/independentexpenditures"
	"github.com/vedantadhobley/legal-tender/internal/source/fec/classic"
	occ "github.com/vedantadhobley/legal-tender/internal/source/fec/occurrence"
	rel "github.com/vedantadhobley/legal-tender/internal/source/fec/release"
)

func (p *planner) retain(key string) {
	if p.hasRole(key, executionRole) {
		return
	}
	if !p.mark(key, executionRole) {
		return
	}
	n := p.nodes[key]
	// Read contracts: occurrence/load.go and schedule_b_load.go load fact
	// metadata and fact bodies, not raw archives. A/B source compatibility also
	// opens release manifests. Classic/E generation proofs need occurrence
	// metadata, not the occurrence data/index/change bodies.
	allowed := map[string]string{}
	historical := map[string]bool{}
	switch n.Reference.Kind {
	case "a_facts":
		allowed = map[string]string{"parquet_shard": "file", "source_release": "release"}
		historical["occurrences"] = true
	case "b_facts":
		allowed = map[string]string{"parquet_shard": "file", "source_release": "release"}
		historical["raw_source"] = true
	case "e_facts":
		allowed = map[string]string{"facts": "file", "source_release": "release", "occurrences": "e_occurrences"}
	case "classic_facts":
		allowed = map[string]string{"facts": "file", "source_release": "release", "occurrences": "classic_occurrences"}
	case "e_occurrences", "classic_occurrences":
		allowed = map[string]string{"source_release": "release"}
		for _, role := range []string{"prior_publication", "occurrences", "issues", "natural_key_index", "changes"} {
			historical[role] = true
		}
	case "release":
		for _, role := range []string{"prior_publication", "plan", "acquisition", "stage", "raw_source", "staged_source"} {
			historical[role] = true
		}
	case "file":
		return
	default:
		p.fail(key, "unsupported_execution_input", n.Reference.Kind)
		return
	}
	for _, e := range p.edges[key] {
		kind, used := allowed[e.Role]
		if !used {
			if !historical[e.Role] {
				p.fail(key, "unclassified_source_dependency", e.Role)
			}
			continue
		} // explicitly named other edges remain historical provenance
		if p.nodes[e.To].Reference.Kind != kind {
			p.fail(key, "ambiguous_input_role", e.Role)
			continue
		}
		p.retain(e.To)
	}
}

// Metadata selection mirrors ProveClassicReference's exact source/member keys.
// Body validation is deliberately not performed by the planner.
func (p *planner) proofInputs(owner string, proof occ.ClassicReferenceProof) []Binding {
	bindings := []Binding{}
	spec, err := classic.Lookup(proof.Dataset)
	if err != nil || (spec.Code != "cn" && spec.Code != "cm" && spec.Code != "ccl") {
		p.fail(owner, "unsupported_reference_proof", proof.Dataset)
		return bindings
	}
	policy := occ.ClassicReferencePolicy
	if spec.Code == "cn" {
		policy = occ.CandidateReferencePolicy
	}
	if proof.Policy != policy || proof.Cycle != p.cycle(owner) {
		p.fail(owner, "unsupported_reference_proof", "policy or cycle differs")
		return bindings
	}
	for _, selected := range []struct {
		role    string
		release occ.ReferenceIdentity
		archive occ.ReferenceArchive
	}{
		{"origin", proof.SourceRelease, proof.SourceArchive}, {"target", proof.TargetRelease, proof.TargetArchive},
	} {
		ref := Reference{Kind: "release", ID: selected.release.ID, SHA256: selected.release.SHA256}
		key := digest([]byte(refKey(ref)))
		if !p.mark(key, executionRole) {
			continue
		}
		if p.nodes[key].Reference.SHA256 != ref.SHA256 {
			p.fail(owner, "reference_release_pin_differs", selected.role)
			continue
		}
		var release rel.ReleaseManifest
		if json.Unmarshal(p.metadata[key], &release) != nil {
			p.fail(key, "invalid_release_metadata", selected.role)
			continue
		}
		source := fmt.Sprintf("fec:%s:%s", spec.Code, proof.Cycle)
		archives, outputs := 0, 0
		for _, a := range release.Artifacts {
			if a.SourceID != source {
				continue
			}
			archives++
			if a.SHA256 != selected.archive.SHA256 || a.ByteCount != selected.archive.Bytes || a.ByteCount < 0 {
				p.fail(owner, "reference_archive_pin_differs", source)
				continue
			}
			bindings = append(bindings, p.proofFile(owner, "reference_"+selected.role+"_archive", a.StorageKey, a.SHA256, uint64(a.ByteCount)))
		}
		for _, s := range release.StagedOutputs {
			if s.SourceID != source || s.Period != proof.Cycle || s.SelectionKind != "member" {
				continue
			}
			outputs++
			if s.Selection != proof.Member || s.UncompressedSHA256 != proof.MemberSHA256 || s.UncompressedByteCount != proof.MemberBytes || s.SourceArtifactSHA256 != selected.archive.SHA256 {
				p.fail(owner, "reference_member_pin_differs", source)
				continue
			}
			bindings = append(bindings, p.proofFile(owner, "reference_"+selected.role+"_member", s.StorageKey, s.CompressedSHA256, s.CompressedByteCount))
		}
		if archives != 1 || outputs != 1 {
			p.fail(owner, "ambiguous_reference_selection", source)
		}
	}
	return bindings
}

func (p *planner) proofFile(owner, role, path, sha string, size uint64) Binding {
	key := digest([]byte("file:" + path))
	n, ok := p.nodes[key]
	if !ok || n.Reference.Kind != "file" || n.Reference.Path != path || n.Reference.SHA256 != sha || n.Reference.Bytes == nil || *n.Reference.Bytes != size {
		p.fail(owner, "unlocated_reference_body", role)
		return Binding{Role: role, NodeID: key, Mode: "unresolved"}
	}
	p.retain(key)
	return Binding{Role: role, NodeID: key, Mode: "retained_input"}
}

func (p *planner) generationExtras(key string, s *RecoveryStep) {
	var v fg.Result
	if json.Unmarshal(p.metadata[key], &v) != nil {
		p.fail(key, "invalid_generation", "typed generation required")
		return
	}
	if v.Policy != fg.Policy {
		p.fail(key, "unsupported_producer_policy", v.Policy)
	}
	for _, proof := range v.ReferenceProofs {
		s.Inputs = append(s.Inputs, p.proofInputs(key, proof)...)
	}
	// Graph results for A/B and E are embedded in the generation, not separate
	// file nodes. They must still be explicit fresh construction steps.
	for _, g := range []struct{ role, kind, bundleID, bundleSHA, projection, operation, implementation, schema string }{
		{"committee_flow_graph", "flow_bundle", v.CommitteeFlow.BundleID, v.CommitteeFlow.BundleSHA, v.CommitteeFlow.ProjectionID, "committee_flow_graph", "flowevidence.Run", flowgraph.Version},
		{"outside_spending_graph", "resolved_bundle", v.OutsideSpending.BundleID, v.OutsideSpending.BundleSHA256, v.OutsideSpending.ProjectionID, "outside_spending_graph", "independentexpenditures.RunResolved", iegraph.ResolvedProjectionVersion},
	} {
		if !validDigest(g.projection) {
			p.fail(key, "invalid_projection_identity", g.role)
			continue
		}
		bundleKey := digest([]byte(refKey(Reference{Kind: g.kind, ID: g.bundleID})))
		bundle, ok := p.steps[bundleKey]
		if !ok || p.nodes[bundleKey].Reference.SHA256 != g.bundleSHA {
			p.fail(key, "missing_graph_input", g.role)
			continue
		}
		id := digest([]byte(key + ":" + g.role))
		step := RecoveryStep{ID: id, Operation: g.operation, Implementation: g.implementation, OutputSchema: g.schema, Cycle: s.Cycle, ExpectedNode: key, ExpectedProjection: g.projection,
			Inputs: []Binding{{Role: "bundle", NodeID: bundleKey, Mode: "rebuilt_output", StepID: bundle.ID}}, ExpectedFiles: []string{}, PolicyEvidence: p.nodes[key].Reference.SHA256,
			Comparison: "not_implemented", Completion: "empty_server_import_complete_field_readback_and_exact_source_drilldown"}
		p.steps[id] = step
		p.out.Steps = append(p.out.Steps, step)
		s.Inputs = append(s.Inputs, Binding{Role: g.role, NodeID: key, Mode: "rebuilt_output", StepID: id})
	}
}
