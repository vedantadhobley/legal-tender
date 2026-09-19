package fundinggeneration

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"

	"github.com/vedantadhobley/legal-tender/internal/projection/arango/graphread"
	receipt "github.com/vedantadhobley/legal-tender/internal/projection/arango/receiptgraph"
	"github.com/vedantadhobley/legal-tender/internal/strictjson"
)

const NeighborhoodVersion = "legal-tender.funding-neighborhood.v1"
const NeighborhoodPolicy = "fec/typed-funding-neighborhood@1.0.0"

type Query struct {
	Entity string `json:"entity_id"`
	Family string `json:"family"`
	Limit  int    `json:"limit_per_family"`
	Cursor string `json:"cursor"`
}
type FamilyPage struct {
	Family Family `json:"family"`
	graphread.Page
	NextCursor string `json:"next_cursor"`
}
type Neighborhood struct {
	SchemaVersion        string                     `json:"schema_version"`
	Policy               string                     `json:"policy"`
	QueryID              string                     `json:"query_id"`
	GenerationID         string                     `json:"generation_id"`
	GenerationSHA256     string                     `json:"generation_sha256"`
	ConsumerBuildSHA256  string                     `json:"consumer_executable_sha256"`
	Cycle                string                     `json:"cycle"`
	Query                Query                      `json:"query"`
	Facets               map[string]graphread.Facet `json:"entity_facets"`
	Pages                []FamilyPage               `json:"relationship_pages"`
	Limitations          []string                   `json:"limitations"`
	FinancialEligibility bool                       `json:"financial_eligibility"`
	TerminalEligible     bool                       `json:"terminal_attribution_eligible"`
}
type continuation struct {
	Version    string `json:"version"`
	Generation string `json:"generation"`
	Entity     string `json:"entity"`
	Family     string `json:"family"`
	After      string `json:"after"`
}

func familyProjection(kind string) string {
	switch kind {
	case "reported_receipt", "conduit_association", "candidate_authorization_context":
		return "receipts"
	case receipt.SharedFamily:
		return "shared_conduits"
	case "receiver_reported_committee_observation", "sender_reported_committee_observation", "reconciliation_candidate":
		return "committee_flow"
	case "independent_support", "independent_opposition":
		return "outside_spending"
	}
	return ""
}
func (q Query) Validate() error {
	if err := graphread.ValidPage(q.Entity, "", q.Limit); err != nil {
		return err
	}
	if q.Family != "" && familyProjection(q.Family) == "" || len(q.Cursor) > 2048 || q.Cursor != "" && q.Family == "" {
		return fmt.Errorf("invalid family or continuation scope")
	}
	return nil
}
func decodeCursor(q Query, generation string) (string, error) {
	if err := q.Validate(); err != nil {
		return "", err
	}
	if q.Cursor == "" {
		return "", nil
	}
	raw, err := base64.RawURLEncoding.DecodeString(q.Cursor)
	if err != nil {
		return "", fmt.Errorf("invalid continuation encoding")
	}
	var c continuation
	if strictjson.Decode(raw, &c) != nil || c.Version != NeighborhoodVersion || c.Generation != generation || c.Entity != q.Entity || c.Family != q.Family || !validDigest(c.After) {
		return "", fmt.Errorf("continuation differs from generation/entity/family scope")
	}
	return c.After, nil
}
func encodeCursor(generation, entity, family, after string) string {
	raw, _ := json.Marshal(continuation{NeighborhoodVersion, generation, entity, family, after})
	return base64.RawURLEncoding.EncodeToString(raw)
}
func neighborhoodID(v Neighborhood) string {
	v.QueryID = ""
	raw, _ := json.Marshal(v)
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

func (r *Reader) Neighborhood(ctx context.Context, q Query) (Neighborhood, error) {
	if q.Family == receipt.SharedFamily && r.shared == nil {
		return Neighborhood{}, fmt.Errorf("shared family requires an extended generation")
	}
	after, err := decodeCursor(q, r.queryGenerationID())
	if err != nil {
		return Neighborhood{}, err
	}
	out := Neighborhood{SchemaVersion: NeighborhoodVersion, Policy: NeighborhoodPolicy, GenerationID: r.queryGenerationID(), GenerationSHA256: r.manifestSHA, ConsumerBuildSHA256: r.consumerBuild, Cycle: r.generation.Cycle, Query: q, Facets: map[string]graphread.Facet{}, Pages: []FamilyPage{}, Limitations: []string{
		"one_hop_not_all_paths_or_terminal_attribution",
		"schedule_a_receipt_and_receiver_views_overlap_do_not_sum",
		"sender_receiver_and_outside_ledgers_remain_separate",
		"authorization_context_is_not_a_payment_and_preserves_authorization_state",
		"outside_evidence_is_calculation_group_not_enumerated_source_membership",
		"reconciliation_component_is_not_an_additional_payment",
		"page_limit_bounds_output_not_server_scan_or_source_verification_work",
		"graph_absence_is_not_evidence_of_zero_funding",
		"immutable_publications_required_not_a_cross_database_transaction",
	}}
	out.Facets["receipts"], err = r.receipts.Entity(ctx, q.Entity)
	if err != nil {
		return Neighborhood{}, err
	}
	out.Facets["committee_flow"], err = r.flow.Facet(ctx, q.Entity)
	if err != nil {
		return Neighborhood{}, err
	}
	out.Facets["outside_spending"], err = r.outside.Entity(ctx, q.Entity)
	if err != nil {
		return Neighborhood{}, err
	}
	if r.shared != nil {
		out.Facets["shared_conduits"], err = r.shared.Entity(ctx, q.Entity)
		if err != nil {
			return Neighborhood{}, err
		}
		out.Limitations = append(out.Limitations, "shared_associations_preserve_original_dispositions_and_add_zero_money", "shared_open_verifies_complete_source_membership_not_all_live_document_fields")
	}
	for _, family := range r.queryFamilies() {
		projection := familyProjection(family.Kind)
		if projection == "" {
			return Neighborhood{}, fmt.Errorf("unsupported pinned relationship family")
		}
		page := graphread.Empty("not_requested")
		if q.Family == "" || q.Family == family.Kind {
			facet := out.Facets[projection]
			if facet.State != "present" {
				page = graphread.Empty(facet.State)
			} else {
				switch projection {
				case "shared_conduits":
					page, err = r.shared.Page(ctx, q.Entity, after, q.Limit)
				case "receipts":
					page, err = r.receipts.Page(ctx, family.Kind, q.Entity, after, q.Limit)
				case "committee_flow":
					page, err = r.flow.NeighborhoodPage(ctx, family.Kind, q.Entity, after, q.Limit)
				case "outside_spending":
					page, err = r.outside.Page(ctx, family.Kind, q.Entity, after, q.Limit)
				}
				if err != nil {
					return Neighborhood{}, err
				}
			}
		}
		p := FamilyPage{Family: family, Page: page}
		if page.HasMore {
			if len(page.Items) != q.Limit || !validDigest(page.NextAfter) || page.NextAfter <= after {
				return Neighborhood{}, fmt.Errorf("invalid family continuation boundary")
			}
			p.NextCursor = encodeCursor(out.GenerationID, q.Entity, family.Kind, page.NextAfter)
		}
		out.Pages = append(out.Pages, p)
	}
	if err := r.VerifyCompletion(ctx); err != nil {
		return Neighborhood{}, err
	}
	out.QueryID = neighborhoodID(out)
	return out, nil
}
