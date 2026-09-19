package fundinggeneration

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	flow "github.com/vedantadhobley/legal-tender/internal/projection/arango/flowevidence"
	receipt "github.com/vedantadhobley/legal-tender/internal/projection/arango/receiptgraph"
	"sort"
)

const NeighborhoodGateVersion = "legal-tender.funding-neighborhood-gate.v1"

type NeighborhoodCase struct {
	Kind         string        `json:"kind"`
	State        string        `json:"state"`
	Result       *Neighborhood `json:"result"`
	Continuation *Neighborhood `json:"continuation"`
}
type NeighborhoodGate struct {
	SchemaVersion       string             `json:"schema_version"`
	GateID              string             `json:"gate_id"`
	GenerationID        string             `json:"generation_id"`
	ConsumerBuildSHA256 string             `json:"consumer_executable_sha256"`
	Scope               string             `json:"scope"`
	Cases               []NeighborhoodCase `json:"cases"`
	Checks              []string           `json:"checks"`
}

func firstID(ids map[string]bool) string {
	keys := []string{}
	for id := range ids {
		keys = append(keys, id)
	}
	sort.Strings(keys)
	if len(keys) == 0 {
		return ""
	}
	return keys[0]
}
func firstShared(ordered []string, members map[string]bool) string {
	for _, id := range ordered {
		if members[id] {
			return id
		}
	}
	return ""
}

// ValidateNeighborhoods selects witnesses from verified publications, not
// operator-supplied politicians or source rows. Missing populations stay explicit.
func (r *Reader) ValidateNeighborhoods(ctx context.Context, expected string, progress func(string)) (NeighborhoodGate, error) {
	if expected != "" && !validDigest(expected) {
		return NeighborhoodGate{}, fmt.Errorf("invalid expected gate identity")
	}
	if progress == nil {
		progress = func(string) {}
	}
	a, err := r.flow.Recipients(flow.ScheduleA)
	if err != nil {
		return NeighborhoodGate{}, err
	}
	b, err := r.flow.Recipients(flow.ScheduleB)
	if err != nil {
		return NeighborhoodGate{}, err
	}
	conduit, err := r.receipts.ConduitWitnessID(ctx)
	if err != nil {
		return NeighborhoodGate{}, err
	}
	authorized := r.receipts.AuthorizedCandidateIDs()
	auth := ""
	if len(authorized) > 0 {
		auth = authorized[0]
	}
	outsideCandidates := map[string]bool{}
	for _, id := range r.outside.EntityIDs("candidate") {
		outsideCandidates[id] = true
	}
	crossCandidate := firstShared(authorized, outsideCandidates)
	crossCommittee := firstShared(r.outside.EntityIDs("committee"), a)
	missing := ""
	if ids := r.flow.MissingMasterIDs(); len(ids) > 0 {
		missing = ids[0]
	}
	selections := []struct{ kind, entity, family string }{
		{"reported_receipt", firstID(a), "reported_receipt"},
		{"conduit_association", conduit, "conduit_association"},
		{"candidate_authorization_context", auth, "candidate_authorization_context"},
		{"receiver_reported_committee_observation", firstID(a), "receiver_reported_committee_observation"},
		{"sender_reported_committee_observation", firstID(b), "sender_reported_committee_observation"},
		{"reconciliation_candidate", firstID(a), "reconciliation_candidate"},
		{"independent_support", r.outside.StanceCandidateID("S"), "independent_support"},
		{"independent_opposition", r.outside.StanceCandidateID("O"), "independent_opposition"},
		{"cross_family_candidate", crossCandidate, ""},
		{"cross_family_committee", crossCommittee, ""},
		{"missing_master_committee", missing, ""},
	}
	if r.shared != nil {
		selections = append(selections, struct{ kind, entity, family string }{receipt.SharedFamily, r.shared.WitnessID(), receipt.SharedFamily})
	}
	out := NeighborhoodGate{SchemaVersion: NeighborhoodGateVersion, GenerationID: r.queryGenerationID(), ConsumerBuildSHA256: r.consumerBuild, Scope: "data_selected_one_hop_witnesses_not_all_entities_or_paths", Cases: []NeighborhoodCase{}, Checks: []string{"exact_generation_reverified", "source_backed_typed_family_witnesses", "scope_bound_continuations", "no_cross_ledger_sum", "all_completion_boundaries_rechecked"}}
	for _, s := range selections {
		c := NeighborhoodCase{Kind: s.kind, State: "no_matching_witness_population"}
		if s.entity != "" {
			progress("reading neighborhood case " + s.kind)
			result, err := r.Neighborhood(ctx, Query{Entity: s.entity, Family: s.family, Limit: 1})
			if err != nil {
				return NeighborhoodGate{}, fmt.Errorf("%s: %w", s.kind, err)
			}
			if s.family != "" {
				found := false
				for _, p := range result.Pages {
					if p.Family.Kind == s.family {
						if len(p.Items) != 1 {
							return NeighborhoodGate{}, fmt.Errorf("selected family witness is missing: %s", s.kind)
						}
						found = true
						if p.HasMore {
							next, err := r.Neighborhood(ctx, Query{Entity: s.entity, Family: s.family, Limit: 1, Cursor: p.NextCursor})
							if err != nil {
								return NeighborhoodGate{}, err
							}
							for _, np := range next.Pages {
								if np.Family.Kind == s.family && (len(np.Items) != 1 || np.Items[0].Key <= p.Items[0].Key) {
									return NeighborhoodGate{}, fmt.Errorf("continuation did not advance")
								}
							}
							c.Continuation = &next
						}
					}
				}
				if !found {
					return NeighborhoodGate{}, fmt.Errorf("missing declared family")
				}
			}
			c.State = "verified"
			c.Result = &result
		}
		out.Cases = append(out.Cases, c)
	}
	if err := r.VerifyCompletion(ctx); err != nil {
		return NeighborhoodGate{}, err
	}
	raw, _ := json.Marshal(out)
	sum := sha256.Sum256(raw)
	out.GateID = hex.EncodeToString(sum[:])
	if expected != "" && expected != out.GateID {
		return NeighborhoodGate{}, fmt.Errorf("neighborhood gate replay differs")
	}
	return out, nil
}
