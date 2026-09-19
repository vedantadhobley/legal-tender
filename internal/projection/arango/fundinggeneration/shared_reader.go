package fundinggeneration

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"

	receipt "github.com/vedantadhobley/legal-tender/internal/projection/arango/receiptgraph"
	"github.com/vedantadhobley/legal-tender/internal/strictjson"
)

type SharedReadOptions struct {
	BaseGeneration string `json:"base_generation"`
	GraphManifest  string `json:"graph_manifest"`
	Conduits       string `json:"conduits"`
}

// OpenQueryReader admits the additive schema for explicitly wired consumers.
// Metadata preflight is shared with window admission; it grants no graph access.
func OpenQueryReader(ctx context.Context, o ReadOptions, shared SharedReadOptions) (*Reader, error) {
	v, g, err := readQueryGeneration(o.Generation, o.GenerationSHA256, shared)
	if err != nil {
		return nil, err
	}
	baseOptions := o
	if g != nil {
		baseOptions.Generation, baseOptions.GenerationSHA256 = shared.BaseGeneration, g.BaseSHA256
	}
	r, err := OpenReader(ctx, baseOptions)
	if err != nil {
		return nil, err
	}
	if !reflect.DeepEqual(r.generation, v) {
		return nil, fmt.Errorf("generation changed during query opening")
	}
	if g == nil {
		return r, nil
	}
	if o.Progress != nil {
		o.Progress("verifying complete shared-conduit ancestry and expected payloads; live selected fields checked on query")
	}
	r.shared, err = r.receipts.OpenShared(ctx, shared.GraphManifest, shared.Conduits, g.Extension)
	if err != nil {
		return nil, err
	}
	r.sharedGeneration, r.manifestSHA = g, o.GenerationSHA256
	return r, nil
}

func readQueryGeneration(path, sha string, shared SharedReadOptions) (Result, *SharedGeneration, error) {
	b, err := readGenerationBytes(path, sha)
	if err != nil {
		return Result{}, nil, err
	}
	var schema struct {
		Version string `json:"schema_version"`
	}
	if err = json.Unmarshal(b, &schema); err != nil {
		return Result{}, nil, err
	}
	if schema.Version == Version {
		if shared != (SharedReadOptions{}) {
			return Result{}, nil, fmt.Errorf("base generation cannot silently ignore shared locators")
		}
		v, err := readGeneration(path, sha)
		return v, nil, err
	}
	var g SharedGeneration
	if strictjson.Decode(b, &g) != nil || g.SchemaVersion != "legal-tender.funding-evidence-generation.shared-conduits.v1" || g.State != "verified_base_plus_shared_conduit_extension" || !validDigest(g.BuildSHA256) || !validDigest(g.BaseSHA256) || g.AdditionalAmount != "0" || g.FinancialEligibility || g.TerminalEligible {
		return Result{}, nil, fmt.Errorf("unsupported shared generation contract")
	}
	id := g.GenerationID
	g.GenerationID = ""
	if !validDigest(id) || valueID(g) != id {
		return Result{}, nil, fmt.Errorf("shared generation identity differs")
	}
	g.GenerationID = id
	if shared.BaseGeneration == "" || shared.GraphManifest == "" || shared.Conduits == "" {
		return Result{}, nil, fmt.Errorf("exact base generation, shared graph and shared calculation locators required")
	}
	v, err := readGeneration(shared.BaseGeneration, g.BaseSHA256)
	if err != nil {
		return Result{}, nil, err
	}
	if !reflect.DeepEqual(v, g.Base) || g.Extension.Added > ^uint64(0)-g.Base.Receipts.Counts["reported_conduit_associations"] || g.TotalConduitAssociations != g.Base.Receipts.Counts["reported_conduit_associations"]+g.Extension.Added {
		return Result{}, nil, fmt.Errorf("shared generation base or combined membership differs")
	}
	return v, &g, nil
}

func (r *Reader) queryGenerationID() string {
	if r.sharedGeneration != nil {
		return r.sharedGeneration.GenerationID
	}
	return r.generation.GenerationID
}

func (r *Reader) queryFamilies() []Family {
	if r.sharedGeneration == nil {
		return r.generation.Families
	}
	v := r.sharedGeneration.Extension
	out := append([]Family(nil), r.generation.Families...)
	return append(out, Family{Kind: receipt.SharedFamily, Database: v.Database, Collection: "reported_conduit_associations", ProjectionID: v.Projection.ID, Ledger: "evidence_only", FactSetID: v.FactSet, Predicate: v.Policy, Grain: "qualified_source_association", AmountMeaning: "no_additional_money", Membership: v.Added, OverlapGroup: "schedule_a_occurrences"})
}
