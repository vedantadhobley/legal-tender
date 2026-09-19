package fundinggeneration

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"

	receipts "github.com/vedantadhobley/legal-tender/internal/projection/arango/receiptgraph"
)

// SharedGeneration is an additive binding, not a replacement for the base's
// receipt dispositions or amounts. Legacy v1 readers reject this schema.
type SharedGeneration struct {
	SchemaVersion            string              `json:"schema_version"`
	GenerationID             string              `json:"generation_id"`
	BuildSHA256              string              `json:"executable_sha256"`
	State                    string              `json:"state"`
	BaseSHA256               string              `json:"base_generation_sha256"`
	Base                     Result              `json:"base_generation"`
	Extension                receipts.SharedView `json:"shared_conduit_extension"`
	TotalConduitAssociations uint64              `json:"combined_conduit_associations"`
	AdditionalAmount         string              `json:"additional_amount_minor_units"`
	FinancialEligibility     bool                `json:"financial_eligibility"`
	TerminalEligible         bool                `json:"terminal_attribution_eligible"`
}

type SharedGenerationResult struct {
	Generation  SharedGeneration           `json:"generation"`
	Publication receipts.SharedPublication `json:"publication"`
}

func (r *Reader) PublishShared(ctx context.Context, o receipts.SharedOptions) (SharedGenerationResult, error) {
	var out SharedGenerationResult
	if r.shared != nil {
		return out, fmt.Errorf("nested shared extensions are not supported")
	}
	if o.BuildSHA256 != r.consumerBuild {
		return out, fmt.Errorf("shared publisher build differs from consumer")
	}
	if err := r.VerifyCompletion(ctx); err != nil {
		return out, err
	}
	p, err := r.receipts.PublishShared(ctx, o)
	if err != nil {
		return out, err
	}
	if err = r.VerifyCompletion(ctx); err != nil {
		return out, err
	}
	g := SharedGeneration{SchemaVersion: "legal-tender.funding-evidence-generation.shared-conduits.v1", BuildSHA256: r.consumerBuild, State: "verified_base_plus_shared_conduit_extension", BaseSHA256: r.manifestSHA, Base: r.generation, Extension: p.View, TotalConduitAssociations: r.generation.Receipts.Counts["reported_conduit_associations"] + p.View.Added, AdditionalAmount: "0"}
	b, _ := json.Marshal(g)
	h := sha256.Sum256(b)
	g.GenerationID = hex.EncodeToString(h[:])
	out.Generation, out.Publication = g, p
	return out, nil
}
