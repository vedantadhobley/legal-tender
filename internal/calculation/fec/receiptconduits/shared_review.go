package receiptconduits

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"slices"

	policy "github.com/vedantadhobley/legal-tender/internal/calculation/fec/earmarkassociation"
	"github.com/vedantadhobley/legal-tender/internal/calculation/fec/fundingbasis"
	refs "github.com/vedantadhobley/legal-tender/internal/calculation/fec/receiptreferences"
	occ "github.com/vedantadhobley/legal-tender/internal/source/fec/occurrence"
)

const SharedReviewPolicy = "fec/shared-reference-original-source-review@1.0.0"

type SharedReviewOptions struct {
	StorageRoot, Facts, Profile, ProfileID, Topology, References, BuildSHA256 string
	Progress                                                                  func(string)
}
type ReviewedPeer struct {
	Ordinal          uint64        `json:"source_row_ordinal"`
	Direction        string        `json:"reference_direction_relative_to_shared_record"`
	Topology         refs.Endpoint `json:"topology"`
	AssociationState string        `json:"unchanged_association_state"`
	RoleState        string        `json:"role_inspection"`
}
type ReviewedGroup struct {
	ProfileClass  SharedGroupClass     `json:"profile_class"`
	Topology      refs.Endpoint        `json:"shared_record_topology"`
	Peers         []ReviewedPeer       `json:"all_exact_peers"`
	AllRoles      map[string]uint64    `json:"all_peer_role_rows"`
	GroupDecision policy.GroupDecision `json:"additive_group_rule_decision"`
}
type SharedReview struct {
	SchemaVersion      string                 `json:"schema_version"`
	Policy             string                 `json:"policy"`
	State              string                 `json:"state"`
	BuildSHA256        string                 `json:"executable_sha256"`
	ProfileID          string                 `json:"profile_id"`
	ProfileSHA256      string                 `json:"profile_sha256"`
	FactSetID          string                 `json:"fact_set_id"`
	FactManifestSHA256 string                 `json:"fact_manifest_sha256"`
	TopologyID         string                 `json:"topology_id"`
	ReferenceID        string                 `json:"reference_id"`
	Groups             []ReviewedGroup        `json:"groups"`
	Sources            []fundingbasis.Receipt `json:"complete_source_rows"`
	AdditionalAmount   string                 `json:"additional_amount_minor_units"`
	TerminalEligible   bool                   `json:"terminal_eligible"`
}

func LoadSharedProfile(path, expected string) (SharedProfile, string, error) {
	if filepath.Base(path) != "shared-reference-profile.json" || !digest(expected) {
		return SharedProfile{}, "", fmt.Errorf("exact profile filename and identity required")
	}
	f, err := os.Open(path)
	if err != nil {
		return SharedProfile{}, "", err
	}
	defer f.Close()
	b, err := io.ReadAll(io.LimitReader(f, (1<<20)+1))
	if err != nil || len(b) > 1<<20 {
		return SharedProfile{}, "", fmt.Errorf("profile read/size failure")
	}
	var p SharedProfile
	d := json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	if err := d.Decode(&p); err != nil {
		return p, "", err
	}
	if d.Decode(new(any)) != io.EOF {
		return p, "", fmt.Errorf("trailing profile data")
	}
	id, err := sharedProfileID(p)
	if err != nil || id != expected || p.ProfileID != expected || p.SchemaVersion != "legal-tender.fec.shared-reference-profile.v1" || p.Policy != SharedProfilePolicy || p.State != "complete_diagnostic_no_association_changes" || p.AssociationChanges != 0 || p.TerminalEligible || p.AdditionalAmount != "0" || len(p.Classes) > 18 {
		return p, "", fmt.Errorf("profile identity/boundary mismatch")
	}
	baseline, err := json.Marshal(p.Baseline)
	if err != nil {
		return p, "", err
	}
	if _, err = DecodeManifest(baseline, p.Baseline.CalculationID); err != nil {
		return p, "", err
	}
	h := sha256.Sum256(b)
	return p, hex.EncodeToString(h[:]), nil
}

// ReviewSharedReferences selects whole groups from the immutable profile, not
// handpicked IDs. It reads complete adjacency and original facts for those
// groups only; neither sample completeness nor role agreement adds graph links.
func ReviewSharedReferences(ctx context.Context, o SharedReviewOptions) (SharedReview, error) {
	if !digest(o.BuildSHA256) {
		return SharedReview{}, fmt.Errorf("executable digest required")
	}
	log := func(s string) {
		if o.Progress != nil {
			o.Progress(s)
		}
	}
	p, psha, err := LoadSharedProfile(o.Profile, o.ProfileID)
	if err != nil {
		return SharedReview{}, err
	}
	t, tsha, err := refs.LoadTopology(o.Topology, p.Baseline.TopologyID)
	if err != nil {
		return SharedReview{}, err
	}
	r, rsha, err := refs.Load(o.References, t.ReferenceCalculationID)
	if err != nil {
		return SharedReview{}, err
	}
	if tsha != p.Baseline.TopologySHA256 || rsha != t.ReferenceManifestSHA256 || r.FactSetID != p.Baseline.FactSetID || r.ManifestSHA256 != p.Baseline.FactManifestSHA256 || r.SourceRows != p.Baseline.SourceRows || r.Cycle != p.Baseline.Cycle {
		return SharedReview{}, fmt.Errorf("profile/reference/topology ancestry mismatch")
	}
	log("verifying full incidence stream and selecting all peers of automatic witness groups")
	groups, ids, err := reviewIncidences(ctx, filepath.Join(filepath.Dir(o.References), "data"), r, p.Classes)
	if err != nil {
		return SharedReview{}, err
	}
	log("verifying full endpoint stream for selected groups and all peers")
	tops, err := reviewEndpoints(ctx, filepath.Join(filepath.Dir(o.Topology), "data"), t, ids)
	if err != nil {
		return SharedReview{}, err
	}
	log("verifying original fact ancestry and backing")
	m, msha, err := occ.LoadPublishedScheduleAColumnarManifest(ctx, o.StorageRoot, o.Facts)
	if err != nil {
		return SharedReview{}, err
	}
	if filepath.Base(o.Facts) != m.FactSetID+".json" || m.FactSetID != p.Baseline.FactSetID || msha != p.Baseline.FactManifestSHA256 || m.Cycle != p.Baseline.Cycle || m.Counts.Facts != p.Baseline.SourceRows {
		return SharedReview{}, fmt.Errorf("review source ancestry mismatch")
	}
	log("batch-reading full original facts for every selected peer")
	sources, err := fundingbasis.ReadSourceOccurrences(ctx, o.StorageRoot, m, ids)
	if err != nil {
		return SharedReview{}, err
	}
	out := SharedReview{SchemaVersion: "legal-tender.fec.shared-reference-source-review.v1", Policy: SharedReviewPolicy, State: "complete_selected_groups_not_complete_cycle", BuildSHA256: o.BuildSHA256, ProfileID: p.ProfileID, ProfileSHA256: psha, FactSetID: m.FactSetID, FactManifestSHA256: msha, TopologyID: t.CalculationID, ReferenceID: r.CalculationID, Sources: sources, AdditionalAmount: "0"}
	byID := map[uint64]fundingbasis.Receipt{}
	for _, row := range sources {
		byID[row.Ordinal] = row
	}
	for _, c := range p.Classes {
		g, err := reviewGroup(c, groups[c.Witness.Related.Ordinal], tops, byID)
		if err != nil {
			return SharedReview{}, err
		}
		out.Groups = append(out.Groups, g)
	}
	return out, ctx.Err()
}

func reviewGroup(c SharedGroupClass, peers map[uint64]byte, tops map[uint64]refs.Endpoint, rows map[uint64]fundingbasis.Receipt) (ReviewedGroup, error) {
	root := c.Witness.Related.Ordinal
	top := tops[root]
	shared, err := fundingbasis.SourceEvidenceFromReceipt(rows[root])
	if err != nil {
		return ReviewedGroup{}, err
	}
	g := ReviewedGroup{ProfileClass: c, Topology: top, AllRoles: map[string]uint64{}}
	if top.Peers != uint64(len(peers)) || !sameProfileSource(c.Witness.Related, shared, top) {
		return g, fmt.Errorf("profile/source group mismatch")
	}
	groupRule, err := policy.NewGroup(root, shared.AssociationEvidence(), topology(top))
	if err != nil {
		return g, err
	}
	var observed sharedGroup
	var requests, incoming, outgoing uint64
	ids := make([]uint64, 0, len(peers))
	for id := range peers {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	for _, id := range ids {
		row, err := fundingbasis.SourceEvidenceFromReceipt(rows[id])
		if err != nil {
			return g, err
		}
		e := tops[id]
		if err := verifyReviewReference(rows[root], rows[id], peers[id]); err != nil {
			return g, err
		}
		if peers[id]&1 != 0 {
			outgoing++
		}
		if peers[id]&2 != 0 {
			incoming++
		}
		if err := groupRule.Observe(id, row.AssociationEvidence(), topology(e), e.OnlyPeer); err != nil {
			return g, err
		}
		role := policy.InspectRoles(row.AssociationEvidence(), shared.AssociationEvidence()).State
		state := "not_non_memo_reviewed_earmark"
		if policy.Applies(row.AssociationEvidence()) {
			switch {
			case e.Peers == 1:
				if e.OnlyPeer != root {
					return g, fmt.Errorf("peer endpoint mismatch")
				}
				requests++
				d, err := policy.Decide(row.AssociationEvidence(), topology(e), &policy.Related{Evidence: shared.AssociationEvidence(), Topology: topology(top)})
				if err != nil {
					return g, err
				}
				state = d.State
				if state == sharedRejection {
					observed.observe(participantEvidence(row), e, participantEvidence(shared))
				}
			default:
				state = "multiple_related_records_unresolved"
				if e.UnsafeReasons != 0 {
					state = "ambiguous_or_incomplete_reference_evidence"
				}
			}
		}
		g.AllRoles[role]++
		directions := map[byte]string{1: "shared_to_peer", 2: "peer_to_shared", 3: "reciprocal"}
		g.Peers = append(g.Peers, ReviewedPeer{id, directions[peers[id]], e, state, role})
	}
	if incoming != top.Incoming || outgoing != top.Outgoing {
		return g, fmt.Errorf("source/reference directional census mismatch")
	}
	g.GroupDecision = groupRule.Decide()
	if requests != c.Witness.SolePeerRequests || observed.rows != c.Witness.SharedRejectedRows || !reflect.DeepEqual(observed.roles, c.Witness.Roles) || !reflect.DeepEqual(observed.amounts.result(), c.Witness.OriginalAmounts) {
		return g, fmt.Errorf("original-source group profile differs")
	}
	for _, ex := range c.Witness.Examples {
		row, err := fundingbasis.SourceEvidenceFromReceipt(rows[ex.Occurrence.Ordinal])
		if err != nil || !sameProfileSource(ex.Occurrence, row, tops[ex.Occurrence.Ordinal]) {
			return g, fmt.Errorf("profile example/source mismatch")
		}
	}
	return g, nil
}
