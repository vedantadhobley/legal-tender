package summaryassertion

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"reflect"
	"sort"

	"github.com/vedantadhobley/legal-tender/internal/source/fec/committeesummary"
	"github.com/vedantadhobley/legal-tender/internal/source/fec/summarypublication"
	"github.com/vedantadhobley/legal-tender/internal/storage/artifact"
)

// Run verifies the complete published input before deriving any usable result.
// The extra stored-fact scan stays bounded by the accepted 100,000-row summary.
func Run(ctx context.Context, root, manifestPath, cycle string) (Result, error) {
	if root == "" || manifestPath == "" || cycle == "" {
		return Result{}, fmt.Errorf("storage root, summary manifest, and cycle required")
	}
	m, err := summarypublication.Load(ctx, root, manifestPath)
	if err != nil {
		return Result{}, err
	}
	if m.Cycle != cycle {
		return Result{}, fmt.Errorf("summary cycle differs from requested cycle")
	}
	f, err := os.Open(summarypublication.ManifestPath(root, m.FactSetID))
	if err != nil {
		return Result{}, err
	}
	raw, err := io.ReadAll(io.LimitReader(f, (4<<20)+1))
	closeErr := f.Close()
	if err != nil {
		return Result{}, err
	}
	if closeErr != nil {
		return Result{}, closeErr
	}
	var backing summarypublication.Manifest
	if len(raw) > 4<<20 || json.Unmarshal(raw, &backing) != nil || !reflect.DeepEqual(backing, m) {
		return Result{}, fmt.Errorf("summary manifest changed after validation")
	}
	input := Input{m.FactSetID, digest(raw), m.SourceReleaseID, m.SourceReleaseManifestSHA256, m.SourceArtifact.SHA256}
	b := newBuilder(cycle, input)
	r, err := artifact.Open[summarypublication.Fact](ctx, root, m.Facts)
	if err != nil {
		return Result{}, err
	}
	defer r.Abort()
	for {
		f, ok, err := r.Next()
		if err != nil {
			return Result{}, err
		}
		if !ok {
			break
		}
		if err := ctx.Err(); err != nil {
			return Result{}, err
		}
		if err := b.add(f); err != nil {
			return Result{}, err
		}
	}
	if err := r.Close(); err != nil {
		return Result{}, err
	}
	if b.out.Counts.SourceRows != m.Facts.RecordCount {
		return Result{}, fmt.Errorf("source row conservation failed")
	}
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	return b.finish()
}

type variant struct {
	assertion Assertion
	fields    []string
}
type builder struct {
	out    Result
	fields []string
	groups map[string]map[string]*variant
	seen   map[string]bool
}

func newBuilder(cycle string, input Input) *builder {
	fields := []string{}
	for _, name := range committeesummary.Fields() {
		if name != "CAND_ID" {
			fields = append(fields, name)
		}
	}
	return &builder{
		out:    Result{SchemaVersion: SchemaVersion, Policy: Policy, State: "complete_evidence_grouping", Cycle: cycle, Input: input, ExcludedGroupingFields: []string{"CAND_ID"}, Committees: []Committee{}, Unindexed: []Unindexed{}},
		fields: fields, groups: map[string]map[string]*variant{}, seen: map[string]bool{},
	}
}

func (b *builder) add(f summarypublication.Fact) error {
	r := f.Record
	if b.out.Counts.SourceRows >= committeesummary.MaxRows || r.Ordinal != b.out.Counts.SourceRows+1 || f.Cycle != b.out.Cycle || f.OriginSnapshotID != b.out.Input.SourceArtifactSHA256 || f.FactType != summarypublication.FactType || f.FactID == "" || b.seen[f.FactID] {
		return fmt.Errorf("summary fact identity, ordering, or row bound mismatch")
	}
	b.seen[f.FactID] = true
	b.out.Counts.SourceRows++
	cmte, year := r.Identifiers["CMTE_ID"], r.Identifiers["FEC_ELECTION_YR"]
	if cmte.State != committeesummary.Valid || year.State != committeesummary.Valid || r.SourceFields["FEC_ELECTION_YR"] != b.out.Cycle {
		b.out.Unindexed = append(b.out.Unindexed, Unindexed{r.SourceFields["CMTE_ID"], "invalid_committee_or_cycle_identity", member(f)})
		b.out.Counts.UnindexedRows++
		return nil
	}
	values := make([]string, len(b.fields))
	for i, name := range b.fields {
		v, ok := r.SourceFields[name]
		if !ok {
			return fmt.Errorf("missing source field %s", name)
		}
		values[i] = v
	}
	// Use full encoded values for equality, not digest equality alone. No trim,
	// money reformatting, date clipping, or ignored financial/scope field.
	encoded, err := json.Marshal(values)
	if err != nil {
		return err
	}
	key := string(encoded)
	id := r.SourceFields["CMTE_ID"]
	if b.groups[id] == nil {
		b.groups[id] = map[string]*variant{}
	}
	v := b.groups[id][key]
	if v == nil {
		eq, err := diagnostics(r)
		if err != nil {
			return err
		}
		identity, _ := json.Marshal([]string{Policy, b.out.Input.FactSetID, key})
		v = &variant{fields: values, assertion: Assertion{ID: digest(identity), EvidenceSHA256: digest(encoded), RepresentativeFactID: f.FactID, CommitteeType: r.SourceFields["CMTE_TP"], Designation: r.SourceFields["CMTE_DSGN"], CoverageStart: r.Dates["CVG_START_DT"], CoverageEnd: r.Dates["CVG_END_DT"], Issues: r.Issues, Members: []Member{}, Equations: eq}}
		b.groups[id][key] = v
	}
	v.assertion.Members = append(v.assertion.Members, member(f))
	b.out.Counts.IndexedRows++
	return nil
}

func (b *builder) finish() (Result, error) {
	var members uint64
	for id, variants := range b.groups {
		g := Committee{CommitteeID: id, State: "single_assertion", ConflictFields: []string{}, Assertions: []Assertion{}}
		var first []string
		conflicts := map[string]bool{}
		for _, v := range variants {
			if first == nil {
				first = v.fields
			}
			for i, field := range b.fields {
				if first[i] != v.fields[i] {
					conflicts[field] = true
				}
			}
			g.Assertions = append(g.Assertions, v.assertion)
			members += uint64(len(v.assertion.Members))
			b.out.Counts.RepeatedEvidenceRows += uint64(len(v.assertion.Members) - 1)
		}
		sort.Slice(g.Assertions, func(i, j int) bool { return g.Assertions[i].ID < g.Assertions[j].ID })
		if len(g.Assertions) > 1 {
			g.State = "conflicting_assertions"
			b.out.Counts.ConflictingCommittees++
		}
		for _, field := range b.fields {
			if conflicts[field] {
				g.ConflictFields = append(g.ConflictFields, field)
			}
		}
		b.out.Counts.Assertions += uint64(len(g.Assertions))
		b.out.Committees = append(b.out.Committees, g)
	}
	b.out.Counts.Committees = uint64(len(b.out.Committees))
	sort.Slice(b.out.Committees, func(i, j int) bool { return b.out.Committees[i].CommitteeID < b.out.Committees[j].CommitteeID })
	c := b.out.Counts
	if c.SourceRows != c.IndexedRows+c.UnindexedRows || members != c.IndexedRows || c.Assertions+c.RepeatedEvidenceRows != c.IndexedRows {
		return Result{}, fmt.Errorf("assertion membership does not conserve source rows")
	}
	encoded, err := json.Marshal(b.out)
	if err != nil {
		return Result{}, err
	}
	b.out.CalculationID = digest(encoded)
	return b.out, nil
}

func digest(raw []byte) string { h := sha256.Sum256(raw); return hex.EncodeToString(h[:]) }
