package receiptgraph

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"

	c "github.com/vedantadhobley/legal-tender/internal/calculation/fec/receiptconduits"
	p "github.com/vedantadhobley/legal-tender/internal/calculation/fec/receiptparticipants"
	receiptcalc "github.com/vedantadhobley/legal-tender/internal/calculation/fec/receipts"
	occ "github.com/vedantadhobley/legal-tender/internal/source/fec/occurrence"
	"github.com/vedantadhobley/legal-tender/internal/storage/artifact"
)

type loaded struct {
	definition definition
	p          p.Result
	c          c.Result
	inspector  *p.Inspector
	masters    map[string]entity
	links      []authorization
}

func manifestBytes(path string) ([]byte, error) {
	f, e := os.Open(path)
	if e != nil {
		return nil, e
	}
	defer f.Close()
	b, e := io.ReadAll(io.LimitReader(f, (8<<20)+1))
	if e == nil && len(b) > 8<<20 {
		e = fmt.Errorf("oversized input manifest")
	}
	return b, e
}
func load(ctx context.Context, o Options) (loaded, error) {
	l := loaded{masters: map[string]entity{}}
	b, e := manifestBytes(o.Participants)
	if e != nil {
		return l, e
	}
	l.p, e = p.DecodeManifest(b, o.ParticipantID)
	if e != nil {
		return l, e
	}
	ph := digest(b)
	if o.FullCycle {
		o.First, o.Rows = 1, l.p.SourceRows
	}
	if filepath.Base(o.Conduits) != "manifest.json" {
		return l, fmt.Errorf("exact conduit manifest.json required")
	}
	b, e = manifestBytes(o.Conduits)
	if e != nil {
		return l, e
	}
	l.c, e = c.DecodeManifest(b, o.ConduitID)
	if e != nil {
		return l, e
	}
	if l.p.State != "complete_cycle_participant_index" || l.c.ParticipantID != l.p.CalculationID || l.c.ParticipantSHA256 != ph || l.c.FactSetID != l.p.FactSetID || l.c.FactManifestSHA256 != l.p.ManifestSHA256 || l.c.SourceRows != l.p.SourceRows || l.c.Cycle != l.p.Cycle {
		return l, fmt.Errorf("complete exact participant/conduit ancestry required")
	}
	if o.First > l.p.SourceRows || o.Rows > l.p.SourceRows-o.First+1 {
		return l, fmt.Errorf("selected range outside source")
	}
	l.inspector, e = p.OpenInspector(ctx, o.StorageRoot, o.Facts, o.Participants, o.ParticipantID)
	if e != nil {
		return l, e
	}
	facts := l.inspector.Scope()
	fh := facts.ManifestSHA256
	if facts.FactSetID != l.p.FactSetID || fh != l.p.ManifestSHA256 || facts.Cycle != l.p.Cycle || facts.Rows != l.p.SourceRows || facts.ParticipantSHA256 != ph {
		return l, fmt.Errorf("participant fact ancestry mismatch")
	}
	l.definition = definition{Version: versionFor(o.Layout), State: "verified_bounded_sample_not_complete_cycle", Build: o.BuildSHA256, First: o.First, Rows: o.Rows, SourceRows: l.p.SourceRows,
		Inputs: Inputs{Participants: Reference{l.p.CalculationID, ph}, Conduits: Reference{l.c.CalculationID, digest(b)}, Facts: Reference{facts.FactSetID, fh}, SourceRelease: facts.SourceReleaseID, Cycle: facts.Cycle}}
	if o.FullCycle {
		l.definition.Version, l.definition.State = CycleVersion, CycleState
	}
	var linkageFacts []receiptcalc.LinkageFact
	for _, spec := range []struct {
		path, dataset string
		ref           *Reference
	}{{o.Committees, "committee-master", &l.definition.Inputs.Committees}, {o.Candidates, "candidate-master", &l.definition.Inputs.Candidates}, {o.Linkages, "candidate-committee-linkage", &l.definition.Inputs.Linkages}} {
		m, h, e := occ.LoadPublishedClassicFactManifest(o.StorageRoot, spec.path, spec.dataset)
		if e != nil {
			return l, e
		}
		// A stricter initial sample boundary: no implicit latest or cross-release reuse.
		if filepath.Base(spec.path) != m.FactSetID+".json" || m.Cycle != facts.Cycle || m.SourceReleaseID != facts.SourceReleaseID || m.SourceReleaseManifestSHA256 != facts.SourceReleaseManifestSHA256 {
			return l, fmt.Errorf("masters/linkages must share exact Schedule A cycle and source release")
		}
		*spec.ref = Reference{m.FactSetID, h}
		e = readClassic(ctx, o.StorageRoot, m, func(f occ.ClassicFact) error {
			b, e := json.Marshal(f.TypedFields)
			if e != nil {
				return e
			}
			switch spec.dataset {
			case "candidate-committee-linkage":
				var v occ.LinkageTypedFields
				if e = json.Unmarshal(b, &v); e != nil {
					return e
				}
				if strconv.Itoa(v.SourceCycle) != facts.Cycle || !committeePattern.MatchString(v.CommitteeID) || !candidatePattern.MatchString(v.CandidateID) {
					return fmt.Errorf("invalid linkage cycle/IDs")
				}
				linkageFacts = append(linkageFacts, receiptcalc.LinkageFact{FactID: f.FactID, State: f.State, CandidateID: v.CandidateID, CommitteeID: v.CommitteeID, DesignationCode: v.DesignationCode})
			default:
				var id, name, kind string
				var cycle int
				if spec.dataset == "committee-master" {
					var v occ.CommitteeTypedFields
					if e = json.Unmarshal(b, &v); e != nil {
						return e
					}
					id, name, kind, cycle = v.CommitteeID, v.Name, "committee", v.SourceCycle
					if !committeePattern.MatchString(id) {
						return fmt.Errorf("invalid committee ID")
					}
				} else {
					var v occ.CandidateTypedFields
					if e = json.Unmarshal(b, &v); e != nil {
						return e
					}
					id, name, kind, cycle = v.CandidateID, v.Name, "candidate", v.SourceCycle
					if !candidatePattern.MatchString(id) {
						return fmt.Errorf("invalid candidate ID")
					}
				}
				if strconv.Itoa(cycle) != facts.Cycle {
					return fmt.Errorf("invalid master cycle")
				}
				if _, ok := l.masters[id]; ok {
					return fmt.Errorf("duplicate master identity")
				}
				l.masters[id] = entity{Key: id, Kind: kind, State: "same_cycle_master", FactSet: m.FactSetID, FactID: &f.FactID, Name: &name}
			}
			return nil
		})
		if e != nil {
			return l, e
		}
	}
	candidates := map[string]bool{}
	for _, v := range linkageFacts {
		candidates[v.CandidateID] = true
	}
	ids := make([]string, 0, len(candidates))
	for id := range candidates {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		for _, r := range receiptcalc.AuthorizedCommitteeRelationships(id, linkageFacts) {
			l.links = append(l.links, authorization{Key: digest([]byte(id + "\x00" + r.CommitteeID)), From: entities + "/" + r.CommitteeID, To: entities + "/" + id, State: r.State, FactSet: l.definition.Inputs.Linkages.ID, SupportingFacts: r.SupportingFactIDs, Designations: r.DesignationCodes})
		}
	}
	b, e = json.Marshal(l.definition)
	if e != nil {
		return l, e
	}
	l.definition.Key = digest(b)
	return l, nil
}
func readClassic(ctx context.Context, root string, m occ.ClassicFactManifest, visit func(occ.ClassicFact) error) error {
	if m.Counts.Facts > 100000 {
		return fmt.Errorf("master/linkage input exceeds bounded context cap")
	}
	a := m.Facts
	d := artifact.Descriptor{RecordCount: a.RecordCount, UncompressedBytes: a.UncompressedBytes, UncompressedSHA256: a.UncompressedSHA256, CompressedBytes: a.CompressedBytes, CompressedSHA256: a.CompressedSHA256, Compression: a.Compression, StorageKey: a.StorageKey}
	r, e := artifact.Open[occ.ClassicFact](ctx, root, d)
	if e != nil {
		return e
	}
	defer r.Abort()
	seen := map[string]bool{}
	for {
		f, ok, e := r.Next()
		if e != nil {
			return e
		}
		if !ok {
			break
		}
		if seen[f.FactID] || !validDigest(f.FactID) || f.State != "valid" || f.Cycle != m.Cycle || f.Dataset != m.Dataset || f.FactType != m.FactType || f.SchemaVersion != m.FactSchemaVersion || f.SourceReleaseID != m.SourceReleaseID || f.SourceContract != m.SourceContract || f.OccurrenceSetID != m.OccurrenceSetID {
			return fmt.Errorf("classic context fact mismatch")
		}
		seen[f.FactID] = true
		if e = visit(f); e != nil {
			return e
		}
	}
	return r.Close()
}
func (l *loaded) entity(id string) entity {
	if v, ok := l.masters[id]; ok {
		return v
	}
	kind := "committee"
	fact := l.definition.Inputs.Committees.ID
	if candidatePattern.MatchString(id) {
		kind = "candidate"
		fact = l.definition.Inputs.Candidates.ID
	}
	return entity{Key: id, Kind: kind, State: "missing_same_cycle_master", FactSet: fact}
}
func participantDir(o Options) string { return filepath.Join(filepath.Dir(o.Participants), "data") }
