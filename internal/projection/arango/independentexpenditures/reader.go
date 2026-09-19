package independentexpenditures

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"reflect"

	resolution "github.com/vedantadhobley/legal-tender/internal/calculation/fec/candidateresolution"
)

const ResolvedReadVersion = "legal-tender.resolved-independent-expenditure-reader.v1"

// ResolvedReader has no repair/import path. Full field readback is bounded in
// batches and compared to the existing deterministic resolved projection model.
type ResolvedReader struct {
	c                   *arangoClient
	m                   resolvedProjection
	bundleID, bundleSHA string
	storageRoot         string
}

type ResolvedView struct {
	SchemaVersion string                      `json:"schema_version"`
	ProjectionID  string                      `json:"projection_id"`
	Database      string                      `json:"database"`
	Cycle         string                      `json:"cycle"`
	BundleID      string                      `json:"bundle_id"`
	BundleSHA256  string                      `json:"bundle_sha256"`
	Inputs        InputReferences             `json:"inputs"`
	Counts        ProjectionCounts            `json:"counts"`
	Amounts       ProjectionAmounts           `json:"amounts"`
	Missing       MissingMasterFacts          `json:"missing_master_facts"`
	Coverage      CandidateResolutionCoverage `json:"candidate_resolution_coverage"`
	Verification  string                      `json:"verification_scope"`
}

func OpenResolvedReader(ctx context.Context, input ResolvedInput, expectedBundleSHA string) (*ResolvedReader, error) {
	if input.StorageRoot == "" || input.ReadinessBundlePath == "" || input.Cycle == "" || !validDigest(expectedBundleSHA) {
		return nil, fmt.Errorf("exact resolved bundle, digest, storage root and cycle required")
	}
	b, bd, paths, err := resolution.LoadResolvedProjectionBundle(ctx, input.StorageRoot, input.ReadinessBundlePath)
	if err != nil {
		return nil, err
	}
	if bd != expectedBundleSHA || b.Cycle != input.Cycle {
		return nil, fmt.Errorf("resolved bundle digest or cycle differs")
	}
	// Load the immutable paths returned by the verified bundle, never its pointer again.
	input.ReadinessBundlePath = ""
	input.AggregateManifestPath, input.ResolutionManifestPath = paths.AggregateManifestPath, paths.ResolutionManifestPath
	input.CandidateManifestPath, input.CommitteeManifestPath = paths.CandidateManifestPath, paths.CommitteeManifestPath
	l, err := loadResolvedInputs(ctx, input)
	if err != nil {
		return nil, err
	}
	if l.aggregate.CalculationSetID != b.InputCalculation.CalculationSetID || l.aggregateDigest != b.InputCalculation.ManifestSHA256 || l.resolution.CalculationSetID != b.InputCalculation.CandidateResolutionCalculationSetID || l.resolutionDigest != b.InputCalculation.CandidateResolutionManifestSHA256 {
		return nil, fmt.Errorf("resolved calculation changed after bundle verification")
	}
	for _, ref := range b.InputFactSets {
		switch ref.Role {
		case "candidate_master":
			if l.candidates.FactSetID != ref.FactSetID || l.candidatesDigest != ref.ManifestSHA256 {
				return nil, fmt.Errorf("candidate facts changed after bundle verification")
			}
		case "committee_master":
			if l.committees.FactSetID != ref.FactSetID || l.committeesDigest != ref.ManifestSHA256 {
				return nil, fmt.Errorf("committee facts changed after bundle verification")
			}
		}
	}
	m, err := buildResolvedProjection(ctx, input.StorageRoot, l)
	if err != nil {
		return nil, err
	}
	c, err := newArangoClient(input.Endpoint, input.Username, input.Password)
	if err != nil {
		return nil, err
	}
	if err = verifyResolvedModel(ctx, c, m); err != nil {
		return nil, err
	}
	return &ResolvedReader{c: c, m: m, bundleID: b.BundleID, bundleSHA: bd, storageRoot: input.StorageRoot}, nil
}

func (r *ResolvedReader) View() ResolvedView {
	m := r.m
	return ResolvedView{ResolvedReadVersion, m.ID, m.Database, m.Cycle, r.bundleID, r.bundleSHA, m.Inputs, m.Counts, m.Amounts, m.Missing, m.Coverage, "all_entities_edges_metadata_fields_counts_and_signed_amounts"}
}

func (r *ResolvedReader) VerifyCompletion(ctx context.Context) error {
	return readResolvedDocuments(ctx, r.c, r.m.Database, metadataCollection, []projectionMetadata{r.m.Metadata}, func(v projectionMetadata) string { return v.Key })
}

func verifyResolvedModel(ctx context.Context, c *arangoClient, m resolvedProjection) error {
	var graph struct {
		Graph struct {
			Name    string           `json:"name"`
			Edges   []edgeDefinition `json:"edgeDefinitions"`
			Orphans []string         `json:"orphanCollections"`
		} `json:"graph"`
	}
	if err := c.doJSON(ctx, m.Database, "GET", "/_api/gharial/"+GraphName, nil, nil, &graph); err != nil {
		return err
	}
	if graph.Graph.Name != GraphName || len(graph.Graph.Orphans) != 0 || !sameEdgeDefinitions(graph.Graph.Edges, []edgeDefinition{{edgesCollection, []string{entitiesCollection}, []string{entitiesCollection}}}) {
		return fmt.Errorf("resolved read graph differs")
	}
	if err := readResolvedDocuments(ctx, c, m.Database, metadataCollection, []projectionMetadata{m.Metadata}, func(v projectionMetadata) string { return v.Key }); err != nil {
		return err
	}
	if err := readResolvedDocuments(ctx, c, m.Database, entitiesCollection, m.Entities, func(v entityDocument) string { return v.Key }); err != nil {
		return err
	}
	if err := readResolvedDocuments(ctx, c, m.Database, edgesCollection, m.Edges, func(v resolvedExpenditureEdge) string { return v.Key }); err != nil {
		return err
	}
	counts, err := readCounts(ctx, c, m.Database)
	if err != nil {
		return err
	}
	amounts, err := readAmounts(ctx, c, m.Database)
	if err != nil {
		return err
	}
	if counts != m.Counts || amounts != m.Amounts {
		return fmt.Errorf("resolved count or amount readback differs")
	}
	return nil
}

func readResolvedDocuments[T any](ctx context.Context, c *arangoClient, database, collection string, want []T, key func(T) string) error {
	for first := 0; first < len(want); first += 1000 {
		last := min(first+1000, len(want))
		keys := make([]string, 0, last-first)
		expected := make(map[string]T, last-first)
		for _, v := range want[first:last] {
			k := key(v)
			if _, ok := expected[k]; ok {
				return fmt.Errorf("duplicate expected document")
			}
			expected[k], keys = v, append(keys, k)
		}
		rows, err := c.query(ctx, database, "FOR d IN @@collection FILTER d._key IN @keys RETURN UNSET(d, '_id', '_rev')", map[string]any{"@collection": collection, "keys": keys})
		if err != nil {
			return err
		}
		for _, raw := range rows {
			var got T
			d := json.NewDecoder(bytes.NewReader(raw))
			d.DisallowUnknownFields()
			if d.Decode(&got) != nil || d.Decode(new(any)) != io.EOF {
				return fmt.Errorf("invalid resolved %s document", collection)
			}
			k := key(got)
			v, ok := expected[k]
			if !ok || !reflect.DeepEqual(v, got) {
				return fmt.Errorf("resolved %s document differs", collection)
			}
			delete(expected, k)
		}
		if len(expected) != 0 {
			return fmt.Errorf("missing resolved %s documents", collection)
		}
	}
	counts, err := c.query(ctx, database, "RETURN LENGTH(@@collection)", map[string]any{"@collection": collection})
	if err != nil {
		return err
	}
	var count uint64
	if len(counts) != 1 || json.Unmarshal(counts[0], &count) != nil || count != uint64(len(want)) {
		return fmt.Errorf("resolved %s membership differs", collection)
	}
	return nil
}
