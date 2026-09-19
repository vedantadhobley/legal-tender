package fundinggeneration

import (
	"context"
	"encoding/json"
	"fmt"

	flow "github.com/vedantadhobley/legal-tender/internal/projection/arango/flowevidence"
	receipt "github.com/vedantadhobley/legal-tender/internal/projection/arango/receiptgraph"
)

type SharedQueryGate struct {
	SchemaVersion       string        `json:"schema_version"`
	GateID              string        `json:"gate_id"`
	GenerationID        string        `json:"generation_id"`
	ConsumerBuildSHA256 string        `json:"consumer_executable_sha256"`
	State               string        `json:"state"`
	Neighborhood        *Neighborhood `json:"neighborhood"`
	Continuation        *Neighborhood `json:"continuation"`
	SharedPath          *PathsResult  `json:"shared_path"`
	OriginalPath        *PathsResult  `json:"original_conduit_path"`
	CommitteePath       *PathsResult  `json:"committee_path"`
	CommitteePathState  string        `json:"committee_path_state"`
	Checks              []string      `json:"checks"`
}

// ValidateSharedQueries picks the lexicographically first source-backed conduit
// and first graph key. This is a deterministic test witness, not a ranking.
func (r *Reader) ValidateSharedQueries(ctx context.Context, expected string, progress func(string)) (SharedQueryGate, error) {
	out := SharedQueryGate{SchemaVersion: "legal-tender.shared-conduit-query-gate.v1", GenerationID: r.queryGenerationID(), ConsumerBuildSHA256: r.consumerBuild, State: "no_shared_associations", CommitteePathState: "no_matching_selection_population"}
	if r.shared == nil || expected != "" && !validDigest(expected) {
		return out, fmt.Errorf("extended generation and valid replay identity required")
	}
	if progress == nil {
		progress = func(string) {}
	}
	id := r.shared.WitnessID()
	if id != "" {
		q := Query{Entity: id, Family: receipt.SharedFamily, Limit: 1}
		progress("verifying shared neighborhood and source-backed lookahead")
		n, err := r.Neighborhood(ctx, q)
		if err != nil {
			return out, err
		}
		out.Neighborhood = &n
		p := n.Pages[len(n.Pages)-1]
		if p.Family.Kind != receipt.SharedFamily || len(p.Items) != 1 {
			return out, fmt.Errorf("selected shared neighborhood absent")
		}
		if p.HasMore {
			progress("verifying shared continuation")
			q.Cursor = p.NextCursor
			next, err := r.Neighborhood(ctx, q)
			if err != nil {
				return out, err
			}
			np := next.Pages[len(next.Pages)-1]
			if len(np.Items) != 1 || np.Items[0].Key <= p.Items[0].Key {
				return out, fmt.Errorf("shared continuation did not advance")
			}
			out.Continuation = &next
		}
		var evidence receipt.SharedEvidence
		if json.Unmarshal(p.Items[0].Evidence, &evidence) != nil {
			return out, fmt.Errorf("invalid shared witness evidence")
		}
		pq := PathQuery{ReceiptOrdinal: evidence.Decision.Ordinal, EntryFamily: receipt.SharedFamily, Ledger: flow.ScheduleA, Target: id, MaxHops: 0, Limit: 1, Budget: 100000}
		progress("verifying shared entry and retained original decision")
		v, err := r.Paths(ctx, pq)
		if err != nil {
			return out, err
		}
		if len(v.Paths) != 1 || len(v.Links) != 1 || v.Entry == nil || v.Entry.State != "available" {
			return out, fmt.Errorf("shared entry witness missing")
		}
		out.SharedPath = &v
		pq.EntryFamily = "conduit_association"
		old, err := r.Paths(ctx, pq)
		if err != nil {
			return out, err
		}
		if len(old.Paths) != 0 || old.Entry == nil || old.Entry.State != "no_qualified_conduit_association" {
			return out, fmt.Errorf("original association decision changed")
		}
		out.OriginalPath = &old
		chain, err := r.chainTopology(ctx, flow.ScheduleA)
		if err != nil {
			return out, err
		}
		for _, edge := range chain[id] {
			if edge.To == id {
				continue
			}
			progress("verifying shared entry followed by a committee observation")
			pq.EntryFamily, pq.Target, pq.MaxHops = receipt.SharedFamily, edge.To, 1
			connected, err := r.Paths(ctx, pq)
			if err != nil {
				return out, err
			}
			if len(connected.Paths) != 1 || len(connected.Links) != 2 {
				return out, fmt.Errorf("shared entry did not compose with committee observation")
			}
			out.CommitteePath, out.CommitteePathState = &connected, "verified"
			break
		}
		out.State = "verified_selected_shared_queries"
	}
	if err := r.VerifyCompletion(ctx); err != nil {
		return out, err
	}
	out.Checks = []string{"complete_calculation_membership_and_published_payload_digests", "selected_live_documents_and_lookahead_source_verified", "exact_original_and_related_occurrences", "prior_disposition_preserved", "no_additional_money_or_terminal_policy", "completion_boundaries_rechecked"}
	out.GateID = valueID(out)
	if expected != "" && expected != out.GateID {
		return out, fmt.Errorf("shared query replay differs")
	}
	return out, nil
}
