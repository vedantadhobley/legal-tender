package flowreconciliation

import (
	"context"
	"fmt"
	"path/filepath"

	"github.com/vedantadhobley/legal-tender/internal/source/fec/occurrence"
)

// ReferenceContext is an in-process capability created only by full reference
// verification. A deserialized "passed" proof cannot authorize a consumer.
type ReferenceContext struct {
	bundleID, bundleSHA string
	committee, linkages FactReference
	proofs              []occurrence.ClassicReferenceProof
}

func LoadReferenceContext(ctx context.Context, root, bundlePath, committeePath, linkagePath string) (*ReferenceContext, error) {
	b, bd, err := LoadBundleMetadata(root, bundlePath)
	if err != nil {
		return nil, err
	}
	paths := []struct{ path, dataset string }{
		{committeePath, "committee-master"},
		{filepath.Join(root, committeeFactBase, "manifests", b.Committee.FactSetID+".json"), "committee-master"},
		{linkagePath, "candidate-committee-linkage"},
	}
	proofs := make([]occurrence.ClassicReferenceProof, 0, len(paths))
	for _, p := range paths {
		proof, err := occurrence.ProveClassicReference(ctx, root, p.path, p.dataset, b.Input.ReleaseID, b.Input.ReleaseSHA256)
		if err != nil {
			return nil, fmt.Errorf("%s reference content gate: %w", p.dataset, err)
		}
		if proof.Cycle != b.Cycle {
			return nil, fmt.Errorf("reference context cycle differs from bundle")
		}
		proofs = append(proofs, proof)
	}
	ref := func(p occurrence.ClassicReferenceProof) FactReference {
		return FactReference{p.FactSet.ID, p.FactSet.SHA256, p.SourceRelease.ID, p.SourceArchive.SHA256, p.Rows}
	}
	if ref(proofs[1]) != b.Committee || !occurrence.SameReferenceContent(proofs[0], proofs[1]) {
		return nil, fmt.Errorf("receipt and flow committee content/schema differ")
	}
	return &ReferenceContext{b.BundleID, bd, ref(proofs[0]), ref(proofs[2]), proofs}, nil
}

// References binds the capability to the full bundle verification performed by
// the consumer. Copies prevent callers mutating the capability's proof list.
func (c *ReferenceContext) References(b Bundle, sha string) (FactReference, FactReference, []occurrence.ClassicReferenceProof, error) {
	if c == nil || c.bundleID == "" || c.bundleID != b.BundleID || c.bundleSHA != sha || len(c.proofs) != 3 {
		return FactReference{}, FactReference{}, nil, fmt.Errorf("verified reference context for exact bundle required")
	}
	return c.committee, c.linkages, append([]occurrence.ClassicReferenceProof(nil), c.proofs...), nil
}
