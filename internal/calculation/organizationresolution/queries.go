// Package organizationresolution proposes organization candidates without
// asserting canonical identities or creating financial graph connections.
package organizationresolution

import (
	"context"
	"fmt"
	"path/filepath"
	"sort"
	"strings"

	"github.com/vedantadhobley/legal-tender/internal/calculation/fec/fundingbasis"
	occ "github.com/vedantadhobley/legal-tender/internal/source/fec/occurrence"
	"github.com/vedantadhobley/legal-tender/internal/source/wikimedia"
	"github.com/vedantadhobley/legal-tender/internal/storage/artifact"
)

// CommitteeQueries selects the lexicographically first N distinct nonblank
// reported names. This is a reproducible test population, not a representative
// sample or a financial materiality rule. All references for those exact strings
// in this one manifest are retained. No normalization is used for deduplication.
func CommitteeQueries(ctx context.Context, root, path, build string, limit int) (wikimedia.Queries, error) {
	q := wikimedia.Queries{Version: "organization-queries.v1", Selection: "first_distinct_nonblank_connected_org_text_lexical.v1", BuildSHA256: build}
	if limit < 1 || limit > wikimedia.MaxQueries {
		return q, fmt.Errorf("query limit must be 1..20")
	}
	m, sha, err := occ.LoadPublishedClassicFactManifest(root, path, "committee-master")
	if err != nil {
		return q, err
	}
	if filepath.Base(path) != m.FactSetID+".json" {
		return q, fmt.Errorf("immutable committee fact manifest required")
	}
	return committeeQueries(ctx, root, m, sha, build, limit)
}

func committeeQueries(ctx context.Context, root string, m occ.ClassicFactManifest, sha, build string, limit int) (wikimedia.Queries, error) {
	q := wikimedia.Queries{Version: "organization-queries.v1", Selection: "first_distinct_nonblank_connected_org_text_lexical.v1", BuildSHA256: build}
	a := m.Facts
	if a.RecordCount != m.Counts.Facts || a.RecordCount > 1_000_000 || a.UncompressedBytes > 512<<20 {
		return q, fmt.Errorf("committee query source budget/count")
	}
	d := artifact.Descriptor{RecordCount: a.RecordCount, UncompressedBytes: a.UncompressedBytes, UncompressedSHA256: a.UncompressedSHA256, CompressedBytes: a.CompressedBytes, CompressedSHA256: a.CompressedSHA256, Compression: a.Compression, StorageKey: a.StorageKey}
	r, err := artifact.Open[occ.ClassicFact](ctx, root, d)
	if err != nil {
		return q, err
	}
	defer r.Abort()
	groups := map[string][]wikimedia.Reference{}
	seen := map[string]bool{}
	for {
		f, ok, err := r.Next()
		if err != nil {
			return q, err
		}
		if !ok {
			break
		}
		if f.FactID == "" || f.OccurrenceID == "" || seen[f.FactID] || f.State != "valid" || f.Dataset != m.Dataset || f.Cycle != m.Cycle || f.OccurrenceSetID != m.OccurrenceSetID || f.SourceReleaseID != m.SourceReleaseID || f.SourceContract != m.SourceContract || f.FactType != m.FactType || f.SchemaVersion != m.FactSchemaVersion {
			return q, fmt.Errorf("committee fact identity mismatch")
		}
		seen[f.FactID] = true
		name, ok := f.SourceFields["CONNECTED_ORG_NM"]
		if !ok {
			return q, fmt.Errorf("connected organization field missing")
		}
		if strings.TrimSpace(name) == "" {
			continue
		}
		groups[name] = append(groups[name], wikimedia.Reference{FactSetID: m.FactSetID, ManifestSHA256: sha, FactID: f.FactID, Field: "CONNECTED_ORG_NM"})
	}
	if err = r.Close(); err != nil {
		return q, err
	}
	names := make([]string, 0, len(groups))
	for s := range groups {
		names = append(names, s)
	}
	sort.Strings(names)
	for _, name := range names[:min(limit, len(names))] {
		refs := groups[name]
		sort.Slice(refs, func(i, j int) bool { return refs[i].FactID < refs[j].FactID })
		q.Queries = append(q.Queries, wikimedia.Query{Text: name, References: refs})
	}
	return q, q.Validate()
}

// EmployerQueries uses explicit source ordinals, not a donor merge or dollar
// threshold. The existing strict loader verifies the entire fact set; the row
// reader reconstructs only the requested occurrences. Do not run per receipt.
func EmployerQueries(ctx context.Context, root, path, build string, ordinals []uint64) (wikimedia.Queries, error) {
	q := wikimedia.Queries{Version: "organization-queries.v1", Selection: "explicit_schedule_a_source_ordinals.v1", BuildSHA256: build}
	if len(ordinals) == 0 || len(ordinals) > wikimedia.MaxQueries {
		return q, fmt.Errorf("1..20 receipt ordinals required")
	}
	m, sha, err := occ.LoadPublishedScheduleAColumnarManifest(ctx, root, path)
	if err != nil {
		return q, err
	}
	if filepath.Base(path) != m.FactSetID+".json" {
		return q, fmt.Errorf("immutable receipt fact manifest required")
	}
	rows, err := fundingbasis.ReadSourceOccurrences(ctx, root, m, ordinals)
	if err != nil {
		return q, err
	}
	return employerQueries(rows, m.FactSetID, sha, build)
}

func employerQueries(rows []fundingbasis.Receipt, factSetID, sha, build string) (wikimedia.Queries, error) {
	q := wikimedia.Queries{Version: "organization-queries.v1", Selection: "explicit_schedule_a_source_ordinals.v1", BuildSHA256: build}
	groups := map[string][]wikimedia.Reference{}
	for _, r := range rows {
		name, ok := r.Fields["contbr_employer"].(string)
		if !ok || strings.TrimSpace(name) == "" {
			return q, fmt.Errorf("selected receipt %d has no nonblank reported employer", r.Ordinal)
		}
		groups[name] = append(groups[name], wikimedia.Reference{FactSetID: factSetID, ManifestSHA256: sha, Ordinal: r.Ordinal, Field: "contbr_employer"})
	}
	names := make([]string, 0, len(groups))
	for s := range groups {
		names = append(names, s)
	}
	sort.Strings(names)
	for _, s := range names {
		refs := groups[s]
		sort.Slice(refs, func(i, j int) bool { return refs[i].Ordinal < refs[j].Ordinal })
		q.Queries = append(q.Queries, wikimedia.Query{Text: s, References: refs})
	}
	return q, q.Validate()
}
