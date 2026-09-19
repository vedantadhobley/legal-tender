package candidateupstream

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strconv"

	flows "github.com/vedantadhobley/legal-tender/internal/calculation/fec/committeeflows"
	flow "github.com/vedantadhobley/legal-tender/internal/calculation/fec/flowreconciliation"
	"github.com/vedantadhobley/legal-tender/internal/calculation/fec/receipts"
	"github.com/vedantadhobley/legal-tender/internal/source/fec/occurrence"
	"github.com/vedantadhobley/legal-tender/internal/storage/artifact"
)

type loaded struct {
	inputs       Inputs
	linkages     []receipts.LinkageFact
	masters      map[string]string
	observations []flow.Observation
}

// Run reads exact immutable publications. It writes no source artifact,
// current pointer, graph document, or terminal classification.
func Run(ctx context.Context, o Options) (Result, error) {
	if err := validateSelection(o.Cycle, o.Candidate); err != nil {
		return Result{}, err
	}
	if o.StorageRoot == "" || o.Bundle == "" || o.Linkages == "" {
		return Result{}, fmt.Errorf("storage root, observation bundle, and linkage manifest required")
	}
	if o.Progress == nil {
		o.Progress = func(string) {}
	}
	o.Progress("verifying pinned observation evidence, source ancestry, and candidate linkage bytes")
	l, err := load(ctx, o)
	if err != nil {
		return Result{}, err
	}
	o.Progress("tracing complete selected receiver cohort; candidate amounts counted only at the scope boundary")
	return analyze(ctx, o.Cycle, o.Candidate, l.inputs, l.linkages, l.masters, l.observations)
}

func load(ctx context.Context, o Options) (loaded, error) {
	return loadContext(ctx, o, nil)
}

func loadContext(ctx context.Context, o Options, references *flow.ReferenceContext) (loaded, error) {
	l := loaded{masters: map[string]string{}}
	b, bd, r, err := flow.LoadBundle(ctx, o.StorageRoot, o.Bundle)
	if err != nil {
		return l, err
	}
	if b.Cycle != o.Cycle {
		return l, fmt.Errorf("upstream bundle cycle mismatch")
	}
	master := b.Committee
	var ref flow.FactReference
	var proofs []occurrence.ClassicReferenceProof
	if references == nil {
		ref, err = flow.LoadLinkageReference(ctx, o.StorageRoot, o.Linkages, r)
	} else {
		master, ref, proofs, err = references.References(b, bd)
	}
	if err != nil {
		return l, fmt.Errorf("candidate reference source gate: %w", err)
	}
	l.inputs = Inputs{BundleID: b.BundleID, BundleSHA256: bd, Reconciliation: b.Calculation, Sources: b.Input, CommitteeMaster: master, Linkages: ref, ReferenceEquivalence: proofs}
	m, md, err := occurrence.LoadPublishedClassicFactManifest(o.StorageRoot, filepath.Join(o.StorageRoot, "facts/fec/classic/candidate-committee-linkage/manifests", ref.FactSetID+".json"), "candidate-committee-linkage")
	if err != nil {
		return l, err
	}
	if md != ref.ManifestSHA256 {
		return l, fmt.Errorf("linkage manifest bytes changed")
	}
	if err := readClassic(ctx, o.StorageRoot, m, func(f occurrence.ClassicFact) error {
		var t occurrence.LinkageTypedFields
		if err := typed(f.TypedFields, &t); err != nil {
			return err
		}
		if strconv.Itoa(t.SourceCycle) != o.Cycle || !flows.ValidCommitteeID(&t.CommitteeID) || !candidatePattern.MatchString(t.CandidateID) {
			return fmt.Errorf("invalid linkage typed identity or cycle")
		}
		l.linkages = append(l.linkages, receipts.LinkageFact{FactID: f.FactID, State: f.State, CandidateID: t.CandidateID, CommitteeID: t.CommitteeID, DesignationCode: t.DesignationCode})
		return nil
	}); err != nil {
		return l, err
	}
	m, md, err = occurrence.LoadPublishedClassicFactManifest(o.StorageRoot, filepath.Join(o.StorageRoot, "facts/fec/classic/committee-master/manifests", master.FactSetID+".json"), "committee-master")
	if err != nil {
		return l, err
	}
	if md != master.ManifestSHA256 {
		return l, fmt.Errorf("committee manifest bytes changed")
	}
	if err := readClassic(ctx, o.StorageRoot, m, func(f occurrence.ClassicFact) error {
		var t occurrence.CommitteeTypedFields
		if err := typed(f.TypedFields, &t); err != nil {
			return err
		}
		if strconv.Itoa(t.SourceCycle) != o.Cycle || !flows.ValidCommitteeID(&t.CommitteeID) {
			return fmt.Errorf("invalid committee typed identity or cycle")
		}
		if _, exists := l.masters[t.CommitteeID]; exists {
			return fmt.Errorf("repeated committee master identity")
		}
		l.masters[t.CommitteeID] = f.FactID
		return nil
	}); err != nil {
		return l, err
	}
	var total amountSum
	if err := readArtifact(ctx, filepath.Join(o.StorageRoot, flow.PublicationBase), r.A.Observations, func(v flow.Observation) error {
		l.observations = append(l.observations, v)
		total.add(v.Amount)
		return nil
	}); err != nil {
		return l, err
	}
	if total.rows != r.A.Selected.Rows || total.rows != r.A.Selected.Known || total.signed.String() != strconv.FormatInt(r.A.Selected.Amount, 10) {
		return l, fmt.Errorf("upstream receiver input does not conserve selected ledger")
	}
	return l, nil
}

func typed(value, target any) error {
	b, err := json.Marshal(value)
	if err != nil {
		return err
	}
	d := json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	return d.Decode(target)
}

func readClassic(ctx context.Context, root string, m occurrence.ClassicFactManifest, consume func(occurrence.ClassicFact) error) error {
	a := m.Facts
	d := artifact.Descriptor{RecordCount: a.RecordCount, UncompressedBytes: a.UncompressedBytes, UncompressedSHA256: a.UncompressedSHA256, CompressedBytes: a.CompressedBytes, CompressedSHA256: a.CompressedSHA256, Compression: a.Compression, StorageKey: a.StorageKey}
	seen := map[string]bool{}
	return readArtifact(ctx, root, d, func(f occurrence.ClassicFact) error {
		if f.FactID == "" || seen[f.FactID] || f.SchemaVersion != m.FactSchemaVersion || f.FactType != m.FactType || f.Dataset != m.Dataset || f.Cycle != m.Cycle || f.OccurrenceSetID != m.OccurrenceSetID || f.SourceReleaseID != m.SourceReleaseID || f.SourceContract != m.SourceContract || f.State != "valid" {
			return fmt.Errorf("classic row differs from pinned upstream input")
		}
		seen[f.FactID] = true
		return consume(f)
	})
}

func readArtifact[T any](ctx context.Context, root string, d artifact.Descriptor, consume func(T) error) error {
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
		if err := consume(v); err != nil {
			return err
		}
	}
	return r.Close()
}
