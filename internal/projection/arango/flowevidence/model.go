package flowevidence

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"

	flow "github.com/vedantadhobley/legal-tender/internal/calculation/fec/flowreconciliation"
	"github.com/vedantadhobley/legal-tender/internal/source/fec/occurrence"
	"github.com/vedantadhobley/legal-tender/internal/storage/artifact"
)

func hash(parts ...string) string {
	h := sha256.New()
	for _, s := range parts {
		_, _ = h.Write([]byte(s))
		_, _ = h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))
}
func entityID(id string) string { return entities + "/" + id }
func databaseName(cycle, id string) string {
	return "lt_flow_evidence_" + cycle + "_" + id[:32]
}
func edgeKey(side Ledger, fact string, ordinal uint64) string {
	return hash(string(side), fact, strconv.FormatUint(ordinal, 10))
}

func readArtifact[T any](ctx context.Context, root string, d artifact.Descriptor, visit func(T) error) error {
	r, err := artifact.Open[T](ctx, root, d)
	if err != nil {
		return err
	}
	defer r.Abort()
	for {
		v, ok, err := r.Next()
		if err != nil {
			return err
		}
		if !ok {
			break
		}
		if err := visit(v); err != nil {
			return err
		}
	}
	return r.Close()
}

func loadModel(ctx context.Context, o Options) (model, error) {
	b, digest, r, err := flow.LoadBundle(ctx, o.StorageRoot, o.Bundle)
	if err != nil {
		return model{}, err
	}
	if b.Cycle != o.Cycle {
		return model{}, fmt.Errorf("bundle cycle mismatch")
	}
	m := model{bundle: b, bundleSHA: digest, calculation: r}
	m.id = hash(Version, b.BundleID, digest)
	// Arango traditional database names are capped at 64 bytes. The full
	// identity stays in completion metadata and is checked before any replay.
	m.database = databaseName(b.Cycle, m.id)
	root := filepath.Join(o.StorageRoot, flow.PublicationBase)
	for _, side := range []Ledger{ScheduleA, ScheduleB} {
		d, f, policy := r.A.Observations, r.Input.A.FactSetID, r.Policy.Receiver
		if side == ScheduleB {
			d, f, policy = r.B.Observations, r.Input.B.FactSetID, r.Policy.Sender
		}
		edges := []observation{}
		err := readArtifact(ctx, root, d, func(v flow.Observation) error {
			edges = append(edges, observation{Key: edgeKey(side, f, v.Ordinal), From: entityID(v.Sender), To: entityID(v.Recipient), SchemaVersion: Version, Cycle: r.Cycle, Ledger: side, FactSetID: f, CalculationID: r.CalculationSetID, SelectionPolicy: policy, ReportingPolicy: r.Policy.Reporting, MatchingPolicy: r.Policy.Matcher, EconomicFlowStatus: "not_established", Observation: v})
			return nil
		})
		if err != nil {
			return m, err
		}
		if side == ScheduleA {
			m.a = edges
		} else {
			m.b = edges
		}
	}
	if err := readArtifact(ctx, root, r.Assertions, func(a flow.Assertion) error {
		m.components = append(m.components, component{Key: a.ID, Cycle: r.Cycle, CalculationID: r.CalculationSetID, MatchingPolicy: r.Policy.Matcher, AFactSetID: r.Input.A.FactSetID, BFactSetID: r.Input.B.FactSetID, EconomicFlowStatus: "not_established", Assertion: a})
		return nil
	}); err != nil {
		return m, err
	}
	if err := bindComponents(&m); err != nil {
		return m, err
	}
	masters := map[string]entity{}
	manifest, _, err := occurrence.LoadPublishedClassicFactManifest(o.StorageRoot, filepath.Join(o.StorageRoot, "facts/fec/classic/committee-master/manifests", b.Committee.FactSetID+".json"), "committee-master")
	if err != nil {
		return m, err
	}
	d := artifact.Descriptor{RecordCount: manifest.Facts.RecordCount, UncompressedBytes: manifest.Facts.UncompressedBytes, UncompressedSHA256: manifest.Facts.UncompressedSHA256, CompressedBytes: manifest.Facts.CompressedBytes, CompressedSHA256: manifest.Facts.CompressedSHA256, Compression: manifest.Facts.Compression, StorageKey: manifest.Facts.StorageKey}
	if err := readArtifact(ctx, o.StorageRoot, d, func(f occurrence.ClassicFact) error {
		if f.State != "valid" || f.Cycle != b.Cycle {
			return fmt.Errorf("invalid committee master fact")
		}
		var fields occurrence.CommitteeTypedFields
		bytes, err := json.Marshal(f.TypedFields)
		if err != nil {
			return err
		}
		if err := json.Unmarshal(bytes, &fields); err != nil {
			return err
		}
		v := entity{Key: fields.CommitteeID, Cycle: b.Cycle, CommitteeID: fields.CommitteeID, IdentityState: "same_cycle_master", MasterFactSetID: b.Committee.FactSetID, MasterFactID: &f.FactID, Master: &fields}
		if old, ok := masters[v.Key]; ok && !reflect.DeepEqual(old, v) {
			return fmt.Errorf("conflicting committee master")
		}
		masters[v.Key] = v
		return nil
	}); err != nil {
		return m, err
	}
	ids := map[string]bool{}
	for _, side := range []Ledger{ScheduleA, ScheduleB} {
		for _, e := range m.edges(side) {
			ids[e.Sender] = true
			ids[e.Recipient] = true
		}
	}
	for id := range ids {
		if len(id) != 9 || id[0] != 'C' || strings.Trim(id[1:], "0123456789") != "" {
			return m, fmt.Errorf("invalid reported committee ID")
		}
		e, ok := masters[id]
		if !ok {
			e = entity{Key: id, Cycle: b.Cycle, CommitteeID: id, IdentityState: "unresolved_same_cycle_master", MasterFactSetID: b.Committee.FactSetID}
			m.unresolved++
		}
		m.entities = append(m.entities, e)
	}
	sort.Slice(m.entities, func(i, j int) bool { return m.entities[i].Key < m.entities[j].Key })
	for _, edges := range [][]observation{m.a, m.b} {
		sort.Slice(edges, func(i, j int) bool { return edges[i].Key < edges[j].Key })
	}
	sort.Slice(m.components, func(i, j int) bool { return m.components[i].Key < m.components[j].Key })
	return m, nil
}

// Bind each occurrence exactly once. No matching or attribution rule lives here.
func bindComponents(m *model) error {
	bySide := map[Ledger]map[uint64]*observation{}
	for _, side := range []Ledger{ScheduleA, ScheduleB} {
		index := map[uint64]*observation{}
		edges := m.edges(side)
		for i := range edges {
			e := &edges[i]
			if _, ok := index[e.Ordinal]; ok {
				return fmt.Errorf("duplicate %s ordinal", side)
			}
			index[e.Ordinal] = e
		}
		bySide[side] = index
	}
	seen := map[string]bool{}
	for i := range m.components {
		c := &m.components[i]
		if seen[c.ID] || c.ID == "" {
			return fmt.Errorf("duplicate or empty component")
		}
		seen[c.ID] = true
		for _, side := range []Ledger{ScheduleA, ScheduleB} {
			ordinals, expected := c.A, c.AAmount
			if side == ScheduleB {
				ordinals, expected = c.B, c.BAmount
			}
			sum := new(big.Int)
			for _, ordinal := range ordinals {
				e, ok := bySide[side][ordinal]
				if !ok || e.ComponentID != "" {
					return fmt.Errorf("missing or repeated component member")
				}
				if c.Sender == "" {
					c.Sender, c.Recipient = e.Sender, e.Recipient
				}
				if c.Sender != e.Sender || c.Recipient != e.Recipient {
					return fmt.Errorf("component endpoints differ")
				}
				e.ComponentID = c.ID
				sum.Add(sum, big.NewInt(e.Amount))
			}
			if sum.Cmp(big.NewInt(expected)) != 0 {
				return fmt.Errorf("component %s amount mismatch", side)
			}
		}
		if c.Sender == "" {
			return fmt.Errorf("empty component")
		}
	}
	for _, side := range []Ledger{ScheduleA, ScheduleB} {
		sum := new(big.Int)
		for _, e := range m.edges(side) {
			if e.ComponentID == "" {
				return fmt.Errorf("unassigned occurrence")
			}
			sum.Add(sum, big.NewInt(e.Amount))
		}
		expected := m.calculation.A.Selected
		if side == ScheduleB {
			expected = m.calculation.B.Selected
		}
		if uint64(len(m.edges(side))) != expected.Rows || expected.Known != expected.Rows || sum.Cmp(big.NewInt(expected.Amount)) != 0 {
			return fmt.Errorf("%s selection conservation failed", side)
		}
	}
	return nil
}
