package candidateevidence

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"

	occ "github.com/vedantadhobley/legal-tender/internal/source/fec/occurrence"
	"github.com/vedantadhobley/legal-tender/internal/storage/artifact"
)

type NameSource struct {
	Dataset             string `json:"dataset"`
	Cycle               string `json:"cycle"`
	FactSetID           string `json:"fact_set_id"`
	ManifestSHA256      string `json:"manifest_sha256"`
	SourceReleaseID     string `json:"source_release_id"`
	SourceReleaseSHA256 string `json:"source_release_manifest_sha256"`
	Facts               uint64 `json:"facts"`
}

type NameAssertion struct {
	FactID       string `json:"fact_id"`
	OccurrenceID string `json:"occurrence_id"`
	RawName      string `json:"raw_name"`
}

type EntityName struct {
	EntityID   string          `json:"entity_id"`
	Kind       string          `json:"entity_kind"`
	State      string          `json:"state"`
	Assertions []NameAssertion `json:"assertions"`
}

func loadNames(ctx context.Context, root, path, cycle, dataset string) (map[string][]NameAssertion, NameSource, error) {
	m, digest, err := occ.LoadPublishedClassicFactManifest(root, path, dataset)
	if err != nil {
		return nil, NameSource{}, err
	}
	if m.Cycle != cycle {
		return nil, NameSource{}, fmt.Errorf("name reference cycle mismatch")
	}
	s := NameSource{dataset, cycle, m.FactSetID, digest, m.SourceReleaseID, m.SourceReleaseManifestSHA256, m.Counts.Facts}
	a := m.Facts
	d := artifact.Descriptor{RecordCount: a.RecordCount, UncompressedBytes: a.UncompressedBytes, UncompressedSHA256: a.UncompressedSHA256,
		CompressedBytes: a.CompressedBytes, CompressedSHA256: a.CompressedSHA256, Compression: a.Compression, StorageKey: a.StorageKey}
	r, err := artifact.Open[occ.ClassicFact](ctx, root, d)
	if err != nil {
		return nil, s, err
	}
	defer r.Abort()
	names := map[string][]NameAssertion{}
	seen := map[string]bool{}
	for {
		if err := ctx.Err(); err != nil {
			return nil, s, err
		}
		f, ok, err := r.Next()
		if err != nil {
			return nil, s, err
		}
		if !ok {
			break
		}
		if f.FactID == "" || f.OccurrenceID == "" || seen[f.FactID] || f.SchemaVersion != m.FactSchemaVersion || f.FactType != m.FactType || f.Dataset != dataset || f.Cycle != cycle || f.OccurrenceSetID != m.OccurrenceSetID || f.SourceReleaseID != m.SourceReleaseID || f.SourceContract != m.SourceContract || f.State != "valid" {
			return nil, s, fmt.Errorf("name fact differs from pinned reference source")
		}
		seen[f.FactID] = true
		id, raw, err := factName(f, cycle)
		if err != nil {
			return nil, s, err
		}
		names[id] = append(names[id], NameAssertion{f.FactID, f.OccurrenceID, raw})
	}
	if err := r.Close(); err != nil {
		return nil, s, err
	}
	if uint64(len(seen)) != m.Counts.Facts {
		return nil, s, fmt.Errorf("name fact count mismatch")
	}
	for _, assertions := range names {
		sort.Slice(assertions, func(i, j int) bool { return assertions[i].FactID < assertions[j].FactID })
	}
	return names, s, nil
}

func factName(f occ.ClassicFact, cycle string) (string, string, error) {
	body, err := json.Marshal(f.TypedFields)
	if err != nil {
		return "", "", err
	}
	var id, name, idField, nameField string
	var sourceCycle int
	switch f.Dataset {
	case "committee-master":
		var t occ.CommitteeTypedFields
		if err := json.Unmarshal(body, &t); err != nil {
			return "", "", err
		}
		id, name, sourceCycle, idField, nameField = t.CommitteeID, t.Name, t.SourceCycle, "CMTE_ID", "CMTE_NM"
	case "candidate-master":
		var t occ.CandidateTypedFields
		if err := json.Unmarshal(body, &t); err != nil {
			return "", "", err
		}
		id, name, sourceCycle, idField, nameField = t.CandidateID, t.Name, t.SourceCycle, "CAND_ID", "CAND_NAME"
	default:
		return "", "", fmt.Errorf("unsupported name source")
	}
	raw, present := f.SourceFields[nameField]
	if id == "" || f.SourceFields[idField] != id || !present || raw != name || strconv.Itoa(sourceCycle) != cycle {
		return "", "", fmt.Errorf("name source and typed fields disagree")
	}
	return id, raw, nil
}

func nameFor(byID map[string][]NameAssertion, id, kind string) EntityName {
	name := EntityName{EntityID: id, Kind: kind, State: "no_reference_record", Assertions: append([]NameAssertion{}, byID[id]...)}
	if len(name.Assertions) == 0 {
		return name
	}
	name.State = "reported_name"
	first := name.Assertions[0].RawName
	if first == "" {
		name.State = "source_name_blank"
	}
	for _, a := range name.Assertions[1:] {
		if a.RawName != first {
			name.State = "conflicting_reported_names"
		}
	}
	return name
}
