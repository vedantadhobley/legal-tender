// Package candidateevidence presents preserved candidate evidence. Names and
// path examples do not change identity resolution, receipt selection or money.
package candidateevidence

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"path/filepath"

	"github.com/vedantadhobley/legal-tender/internal/calculation/fec/fundingbasis"
)

const Version = "legal-tender.fec.candidate-evidence.v2"
const Policy = "fec/candidate-evidence-presentation@1.0.0"
const PathPolicy = "first_committee_id_at_each_of_first_three_hop_distances"

type Report struct {
	SchemaVersion  string                         `json:"schema_version"`
	ReportID       string                         `json:"report_id"`
	Policy         string                         `json:"policy"`
	Evidence       fundingbasis.CandidateEvidence `json:"evidence"`
	CandidateNames *NameSource                    `json:"candidate_name_source"`
	CommitteeNames NameSource                     `json:"committee_name_source"`
	Names          []EntityName                   `json:"names"`
	PathPolicy     string                         `json:"path_example_policy"`
	Paths          []PathExample                  `json:"path_examples"`
}

// Build only consumes an already computed v1 result and small reference facts.
// The candidate name source is optional context; committee names must match the
// exact master input used by the trace. No network access or transaction scan.
func Build(ctx context.Context, root, candidateManifest string, evidence fundingbasis.CandidateEvidence) (Report, error) {
	out := Report{SchemaVersion: Version, Policy: Policy, Evidence: evidence,
		Names: []EntityName{}, PathPolicy: PathPolicy, Paths: []PathExample{}}
	ref := evidence.Trace.Inputs.CommitteeMaster
	path := filepath.Join(root, "facts/fec/classic/committee-master/manifests", ref.FactSetID+".json")
	committees, source, err := loadNames(ctx, root, path, evidence.Cycle, "committee-master")
	if err != nil {
		return Report{}, err
	}
	if source.FactSetID != ref.FactSetID || source.ManifestSHA256 != ref.ManifestSHA256 || source.SourceReleaseID != ref.SourceReleaseID || source.Facts != ref.Facts {
		return Report{}, fmt.Errorf("committee names do not match the trace's exact master source")
	}
	out.CommitteeNames = source
	candidate := EntityName{EntityID: evidence.CandidateID, Kind: "candidate", State: "name_source_not_requested", Assertions: []NameAssertion{}}
	if candidateManifest != "" {
		byID, source, err := loadNames(ctx, root, candidateManifest, evidence.Cycle, "candidate-master")
		if err != nil {
			return Report{}, err
		}
		out.CandidateNames = &source
		candidate = nameFor(byID, evidence.CandidateID, "candidate")
	}
	out.Names = append(out.Names, candidate)
	for _, c := range evidence.Committees {
		out.Names = append(out.Names, nameFor(committees, c.CommitteeID, "committee"))
	}
	// A display name cannot quietly come from another master occurrence.
	for _, n := range evidence.Trace.Nodes {
		name := nameFor(committees, n.CommitteeID, "committee")
		if n.MasterFactID == nil && len(name.Assertions) != 0 || n.MasterFactID != nil && (len(name.Assertions) != 1 || name.Assertions[0].FactID != *n.MasterFactID) {
			return Report{}, fmt.Errorf("committee name membership disagrees with trace master identity")
		}
	}
	out.Paths, err = pathExamples(ctx, evidence)
	if err != nil {
		return Report{}, err
	}
	body, err := json.Marshal(out)
	if err != nil {
		return Report{}, err
	}
	h := sha256.Sum256(body)
	out.ReportID = hex.EncodeToString(h[:])
	return out, ctx.Err()
}
