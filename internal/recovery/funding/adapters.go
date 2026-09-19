package funding

import (
	"encoding/json"
	"fmt"
	"path"
	"sort"

	cr "github.com/vedantadhobley/legal-tender/internal/calculation/fec/candidateresolution"
	flow "github.com/vedantadhobley/legal-tender/internal/calculation/fec/flowreconciliation"
	ie "github.com/vedantadhobley/legal-tender/internal/calculation/fec/independentexpenditures"
	c "github.com/vedantadhobley/legal-tender/internal/calculation/fec/receiptconduits"
	p "github.com/vedantadhobley/legal-tender/internal/calculation/fec/receiptparticipants"
	r "github.com/vedantadhobley/legal-tender/internal/calculation/fec/receiptreferences"
	fg "github.com/vedantadhobley/legal-tender/internal/projection/arango/fundinggeneration"
	rg "github.com/vedantadhobley/legal-tender/internal/projection/arango/receiptgraph"
	occ "github.com/vedantadhobley/legal-tender/internal/source/fec/occurrence"
	art "github.com/vedantadhobley/legal-tender/internal/storage/artifact"
	xsort "github.com/vedantadhobley/legal-tender/internal/storage/externalsort"
	"github.com/vedantadhobley/legal-tender/internal/strictjson"
)

type dependency struct {
	Role string
	Ref  Reference
}
type adapter struct {
	r         Reference
	b         []byte
	schema    string
	deps      []dependency
	reqs      []Requirement
	checks    []string
	err       error
	decodeErr error
}

func (a *adapter) decode(target any, schema string) {
	if a.schema != schema {
		a.decodeErr = fmt.Errorf("unsupported_manifest_version")
		return
	}
	if e := strictjson.Decode(a.b, target); e != nil {
		a.decodeErr = fmt.Errorf("invalid_closed_manifest")
	}
}
func (a *adapter) id(id string) {
	if id != a.r.ID {
		a.err = fmt.Errorf("manifest_identity_differs")
	}
}
func (a *adapter) ref(role, kind, id, sha, dataset string) {
	if !validID(kind, id) || (sha != "" && !validDigest(sha)) {
		a.err = fmt.Errorf("invalid_dependency_identity")
		return
	}
	a.deps = append(a.deps, dependency{role, Reference{Kind: kind, ID: id, SHA256: sha, Dataset: dataset}})
}
func (a *adapter) blob(role, file, sha string, size uint64) {
	if !relative(file) || !validDigest(sha) {
		a.err = fmt.Errorf("invalid_artifact_descriptor")
		return
	}
	a.deps = append(a.deps, dependency{role, Reference{Kind: "file", ID: sha, SHA256: sha, Path: file, Bytes: &size}})
}
func (a *adapter) local(role, name, sha string, size uint64) {
	if path.Base(name) != name || name == "." || name == ".." {
		a.err = fmt.Errorf("invalid_local_artifact_name")
		return
	}
	a.blob(role, path.Join(path.Dir(a.r.Path), "data", name), sha, size)
}
func (a *adapter) artifact(role string, d art.Descriptor) {
	a.blob(role, d.StorageKey, d.CompressedSHA256, d.CompressedBytes)
}
func (a *adapter) flowArtifact(role string, d art.Descriptor) {
	if !relative(d.StorageKey) {
		a.err = fmt.Errorf("invalid_artifact_descriptor")
		return
	}
	a.blob(role, path.Join(flow.PublicationBase, d.StorageKey), d.CompressedSHA256, d.CompressedBytes)
}
func (a *adapter) occurrenceArtifact(role string, d occ.Artifact) {
	a.blob(role, d.StorageKey, d.CompressedSHA256, d.CompressedBytes)
}
func (a *adapter) sorted(role string, d xsort.File) { a.local(role, d.Name, d.SHA256, d.Bytes) }
func (a *adapter) require(kind, id string) {
	if id != "" {
		a.reqs = append(a.reqs, Requirement{Kind: kind, Identity: id, State: "declared_not_located_or_verified"})
	}
}
func (a *adapter) prior(kind, id, dataset string) {
	if id != "" {
		a.ref("prior_publication", kind, id, "", dataset)
	}
}
func (a *adapter) receiptInputs(i rg.Inputs) {
	a.ref("participants", "participants", i.Participants.ID, i.Participants.SHA256, "")
	a.ref("conduits", "conduits", i.Conduits.ID, i.Conduits.SHA256, "")
	a.ref("receipt_facts", "a_facts", i.Facts.ID, i.Facts.SHA256, "")
	a.ref("committee_facts", "classic_facts", i.Committees.ID, i.Committees.SHA256, "committee-master")
	a.ref("candidate_facts", "classic_facts", i.Candidates.ID, i.Candidates.SHA256, "candidate-master")
	a.ref("linkage_facts", "classic_facts", i.Linkages.ID, i.Linkages.SHA256, "candidate-committee-linkage")
}
func (a *adapter) flowInputs(i flow.Inputs) {
	a.ref("coordinated_release", "release", i.ReleaseID, i.ReleaseSHA256, "")
	a.ref("receiver_facts", "a_facts", i.A.FactSetID, i.A.ManifestSHA256, "")
	a.ref("sender_facts", "b_facts", i.B.FactSetID, i.B.ManifestSHA256, "")
}
func (a *adapter) generation(v fg.Result) {
	a.require("executable_sha256", v.BuildSHA256)
	a.require("live_graph_projection", v.Receipts.Projection.ID)
	a.require("live_graph_projection", v.CommitteeFlow.ProjectionID)
	a.require("live_graph_projection", v.OutsideSpending.ProjectionID)
	a.ref("receipt_projection", "receipt_projection", v.Receipts.Projection.ID, v.Receipts.Projection.SHA256, "")
	a.receiptInputs(v.Receipts.Inputs)
	a.ref("committee_flow_bundle", "flow_bundle", v.CommitteeFlow.BundleID, v.CommitteeFlow.BundleSHA, "")
	a.ref("outside_spending_bundle", "resolved_bundle", v.OutsideSpending.BundleID, v.OutsideSpending.BundleSHA256, "")
	a.ref("coordinated_release", "release", v.Release.ID, v.Release.SHA256, "")
	for _, p := range v.ReferenceProofs {
		a.ref("reference_proof_fact", "classic_facts", p.FactSet.ID, p.FactSet.SHA256, p.Dataset)
		a.ref("reference_proof_occurrences", "classic_occurrences", p.Occurrences.ID, p.Occurrences.SHA256, p.Dataset)
		a.ref("reference_proof_origin", "release", p.SourceRelease.ID, p.SourceRelease.SHA256, "")
		a.ref("reference_proof_target", "release", p.TargetRelease.ID, p.TargetRelease.SHA256, "")
	}
	p := v.OutsideMembership
	a.ref("outside_membership_facts", "e_facts", p.FactSet.ID, p.FactSet.SHA256, "")
	a.ref("outside_membership_origin", "release", p.SourceRelease.ID, p.SourceRelease.SHA256, "")
	a.ref("outside_membership_target", "release", p.TargetRelease.ID, p.TargetRelease.SHA256, "")
	a.checks = append(a.checks, v.Checks...)
}

func expand(ref Reference, b []byte) ([]dependency, []Requirement, []string, string, error) {
	a := adapter{r: ref, b: b}
	var header struct {
		Schema string `json:"schema_version"`
	}
	if json.Unmarshal(b, &header) != nil {
		return nil, nil, nil, "", fmt.Errorf("invalid_manifest_json")
	}
	a.schema = header.Schema
	switch ref.Kind {
	case "generation":
		var v fg.Result
		a.decode(&v, fg.Version)
		a.id(v.GenerationID)
		v.GenerationID = ""
		if identity(v) != ref.ID {
			a.err = fmt.Errorf("generation_identity_differs")
		}
		a.generation(v)
	case "shared_generation":
		var v fg.SharedGeneration
		a.decode(&v, "legal-tender.funding-evidence-generation.shared-conduits.v1")
		a.id(v.GenerationID)
		v.GenerationID = ""
		if identity(v) != ref.ID {
			a.err = fmt.Errorf("generation_identity_differs")
		}
		a.ref("base_generation", "generation", v.Base.GenerationID, v.BaseSHA256, "")
		a.ref("extension_projection", "shared_projection", v.Extension.Projection.ID, v.Extension.Projection.SHA256, "")
		a.ref("extension_calculation", "conduits", v.Extension.Calculation.ID, v.Extension.Calculation.SHA256, "")
		a.require("executable_sha256", v.BuildSHA256)
		a.require("live_graph_projection", v.Extension.Projection.ID)
	case "receipt_projection":
		var v rg.CycleManifest
		a.decode(&v, rg.CycleVersion)
		a.id(v.Key)
		a.receiptInputs(v.Inputs)
		a.require("executable_sha256", v.Build)
	case "shared_projection":
		var v rg.SharedManifest
		a.decode(&v, rg.SharedVersion)
		a.id(v.Key)
		a.ref("base_receipt_projection", "receipt_projection", v.Base.ID, v.Base.SHA256, "")
		a.ref("conduits", "conduits", v.Calculation.ID, v.Calculation.SHA256, "")
		a.require("executable_sha256", v.Build)
	case "participants":
		var v p.Result
		a.decode(&v, p.Version)
		a.id(v.CalculationID)
		if _, err := p.DecodeManifest(b, ref.ID); err != nil {
			a.err = fmt.Errorf("invalid_participant_metadata")
		}
		a.ref("receipt_facts", "a_facts", v.FactSetID, v.ManifestSHA256, "")
		a.require("executable_sha256", v.BuildSHA256)
		for _, f := range v.Files {
			a.local("participant_shard", f.Name, f.SHA256, f.Bytes)
		}
	case "conduits":
		var v c.Result
		if a.schema != c.Version && a.schema != c.GroupVersion {
			a.err = fmt.Errorf("unsupported_manifest_version")
		} else {
			a.decode(&v, a.schema)
		}
		if _, err := c.DecodeManifest(b, ref.ID); err != nil {
			a.err = fmt.Errorf("invalid_conduit_metadata")
		}
		a.id(v.CalculationID)
		a.require("executable_sha256", v.BuildSHA256)
		a.ref("participants", "participants", v.ParticipantID, v.ParticipantSHA256, "")
		a.ref("topology", "topology", v.TopologyID, v.TopologySHA256, "")
		a.ref("receipt_facts", "a_facts", v.FactSetID, v.FactManifestSHA256, "")
		a.sorted("conduit_decisions", v.Decisions)
		if v.Groups != nil {
			a.ref("one_to_one_baseline", "conduits", v.Groups.BaselineID, v.Groups.BaselineSHA256, "")
			a.sorted("group_decisions", v.Groups.Decisions)
		}
	case "topology":
		var v r.TopologyResult
		a.decode(&v, r.TopologyVersion)
		a.id(v.CalculationID)
		a.ref("reference_join", "references", v.ReferenceCalculationID, v.ReferenceManifestSHA256, "")
		a.ref("receipt_facts", "a_facts", v.FactSetID, v.FactManifestSHA256, "")
		a.sorted("endpoints", v.Endpoints)
		a.require("executable_sha256", v.BuildSHA256)
	case "references":
		var v r.Result
		a.decode(&v, r.Version)
		a.id(v.CalculationID)
		a.ref("receipt_facts", "a_facts", v.FactSetID, v.ManifestSHA256, "")
		a.require("executable_sha256", v.BuildSHA256)
		a.sorted("reference_decisions", v.Decisions)
		a.sorted("lookup_evidence", v.LookupEvidence)
		a.sorted("exact_incidences", v.ExactIncidences)
		a.sorted("neighbors", v.Neighbors)
	case "flow_bundle":
		var v flow.Bundle
		a.decode(&v, flow.BundleVersion)
		a.id(v.BundleID)
		a.ref("flow_calculation", "flow_calculation", v.Calculation.CalculationSetID, v.Calculation.ManifestSHA256, "")
		a.flowInputs(v.Input)
		a.ref("committee_master", "classic_facts", v.Committee.FactSetID, v.Committee.ManifestSHA256, "committee-master")
		a.checks = append(a.checks, v.Checks...)
	case "flow_calculation":
		var v flow.Result
		a.decode(&v, flow.Version)
		a.id(v.CalculationSetID)
		a.flowInputs(v.Input)
		a.flowArtifact("receiver_observations", v.A.Observations)
		a.flowArtifact("sender_observations", v.B.Observations)
		a.flowArtifact("reconciliation_assertions", v.Assertions)
		a.require("calculation_contract", v.Policy.Matcher)
	case "resolved_bundle":
		var v cr.ResolvedProjectionBundleManifest
		a.decode(&v, cr.ResolvedProjectionBundleSchemaVersion)
		a.id(v.BundleID)
		a.ref("resolved_aggregate", "resolved_aggregate", v.InputCalculation.CalculationSetID, v.InputCalculation.ManifestSHA256, "")
		a.ref("candidate_resolution", "resolution", v.InputCalculation.CandidateResolutionCalculationSetID, v.InputCalculation.CandidateResolutionManifestSHA256, "")
		for _, f := range v.InputFactSets {
			a.ref(f.Role, "classic_facts", f.FactSetID, f.ManifestSHA256, f.Dataset)
		}
		for _, q := range v.Checks {
			if q.Passed {
				a.checks = append(a.checks, q.ID)
			}
		}
		a.require("producer_contract", v.PublisherVersion)
	case "resolved_aggregate":
		var v cr.AggregateManifest
		a.decode(&v, cr.AggregateManifestSchemaVersion)
		a.id(v.CalculationSetID)
		a.ref("candidate_resolution", "resolution", v.InputResolution.CalculationSetID, v.InputResolution.ManifestSHA256, "")
		a.artifact("aggregate_results", v.Results)
		a.artifact("aggregate_exceptions", v.Exceptions)
		a.require("producer_contract", v.PublisherVersion)
		for _, q := range v.Checks {
			if q.Passed {
				a.checks = append(a.checks, q.ID)
			}
		}
	case "resolution":
		var v cr.Manifest
		a.decode(&v, cr.ManifestSchemaVersion)
		a.id(v.CalculationSetID)
		a.ref("effective_expenditures", "effective", v.InputCalculation.CalculationSetID, v.InputCalculation.ManifestSHA256, "")
		a.ref("schedule_e_facts", "e_facts", v.InputCalculation.ScheduleEFactSetID, v.InputCalculation.ScheduleEManifestSHA256, "")
		a.ref("candidate_master", "classic_facts", v.InputCandidateFactSet.FactSetID, v.InputCandidateFactSet.ManifestSHA256, v.InputCandidateFactSet.Dataset)
		a.artifact("resolution_decisions", v.Decisions)
		a.require("producer_contract", v.PublisherVersion)
		for _, q := range v.Checks {
			if q.Passed {
				a.checks = append(a.checks, q.ID)
			}
		}
	case "effective":
		var v ie.Manifest
		a.decode(&v, ie.ManifestSchemaVersion)
		a.id(v.CalculationSetID)
		a.ref("schedule_e_facts", "e_facts", v.InputFactSet.FactSetID, v.InputFactSet.ManifestSHA256, "")
		a.artifact("effective_results", v.Results)
		a.artifact("effective_exceptions", v.Exceptions)
		a.require("producer_contract", v.PublisherVersion)
		for _, q := range v.Checks {
			if q.Passed {
				a.checks = append(a.checks, q.ID)
			}
		}
	default:
		if !a.source() {
			a.err = fmt.Errorf("unsupported_manifest_kind")
		}
	}
	sort.Strings(a.checks)
	if a.decodeErr != nil {
		return nil, nil, nil, a.schema, a.decodeErr
	}
	if a.err != nil {
		return nil, nil, nil, a.schema, a.err
	}
	return a.deps, a.reqs, a.checks, a.schema, nil
}

// These are publisher-owned immutable layout contracts, not discovered paths.
// Audit publications and representation-polymorphic A occurrences require an
// explicit locator instead of trying directories until one happens to exist.
func defaultPath(r Reference) string {
	if category := map[string]string{"plan": "plans", "acquisition": "acquisitions", "stage": "stages"}[r.Kind]; category != "" {
		return path.Join("control/fec/release", category, r.ID+".json")
	}
	bases := map[string]string{
		"a_facts": "facts/fec/schedule-a/columnar", "b_facts": "facts/fec/schedule-b/columnar", "e_facts": "facts/fec/schedule-e",
		"e_occurrences": "evidence/fec/schedule-e", "release": "releases/fec",
		"flow_bundle": flow.BundleBase, "flow_calculation": flow.PublicationBase,
		"resolved_bundle":    "bundles/fec/resolved-independent-expenditure-projection",
		"resolved_aggregate": "calculations/fec/resolved-independent-expenditures",
		"resolution":         "calculations/fec/independent-expenditure-candidate-resolution",
		"effective":          "calculations/fec/effective-independent-expenditures",
	}
	if r.Kind == "classic_facts" || r.Kind == "classic_occurrences" {
		if !classicDataset(r.Dataset) {
			return ""
		}
		base := "facts"
		if r.Kind == "classic_occurrences" {
			base = "evidence"
		}
		return path.Join(base, "fec/classic", r.Dataset, "manifests", r.ID+".json")
	}
	if base := bases[r.Kind]; base != "" {
		return path.Join(base, "manifests", r.ID+".json")
	}
	return ""
}
func classicDataset(s string) bool {
	switch s {
	case "candidate-master", "committee-master", "candidate-committee-linkage", "all-candidates-summary", "current-campaigns-summary":
		return true
	}
	return false
}
