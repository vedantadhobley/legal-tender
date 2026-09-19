package fundinggeneration

import (
	"context"
	"fmt"
	"path/filepath"

	flowcalc "github.com/vedantadhobley/legal-tender/internal/calculation/fec/flowreconciliation"
	flow "github.com/vedantadhobley/legal-tender/internal/projection/arango/flowevidence"
	ie "github.com/vedantadhobley/legal-tender/internal/projection/arango/independentexpenditures"
	receipts "github.com/vedantadhobley/legal-tender/internal/projection/arango/receiptgraph"
	occ "github.com/vedantadhobley/legal-tender/internal/source/fec/occurrence"
)

// Verify returns a reproducible generation binding only after all required graph
// families pass. It reads no latest pointer, writes no database or publication,
// and never accepts a serialized validation result as an authorization token.
func Verify(ctx context.Context, o Options) (Result, error) {
	r, err := openVerified(ctx, o)
	if err != nil {
		return Result{}, err
	}
	return r.generation, nil
}

func openVerified(ctx context.Context, o Options) (*Reader, error) {
	var out Result
	if !validDigest(o.BuildSHA256) || !validDigest(o.ReceiptSHA256) || !validDigest(o.FlowBundleSHA256) || !validDigest(o.OutsideBundleSHA256) || o.ExpectedGenerationID != "" && !validDigest(o.ExpectedGenerationID) {
		return nil, fmt.Errorf("exact graph/bundle/build digests required")
	}
	root := o.Receipt.StorageRoot
	progress := o.Receipt.Progress
	if progress == nil {
		progress = func(string) {}
	}
	b, bd, err := flowcalc.LoadBundleMetadata(root, o.FlowBundle)
	if err != nil {
		return nil, err
	}
	if bd != o.FlowBundleSHA256 || filepath.Base(o.FlowBundle) != b.BundleID+".json" {
		return nil, fmt.Errorf("exact immutable flow bundle required")
	}
	progress("verifying receipt/committee reference context")
	refs, err := flowcalc.LoadReferenceContext(ctx, root, o.FlowBundle, o.Receipt.Committees, o.Receipt.Linkages)
	if err != nil {
		return nil, err
	}
	_, _, proofs, err := refs.References(b, bd)
	if err != nil {
		return nil, err
	}
	progress("opening completed receipt graph without import or full receipt rescan")
	r, err := receipts.OpenCycleReader(ctx, o.Receipt, o.ReceiptManifest, o.ReceiptSHA256)
	if err != nil {
		return nil, err
	}
	rv := r.View()
	if err := compatibleReceipt(rv, b); err != nil {
		return nil, err
	}
	if !sameRef(rv.Inputs.Committees, proofs[0].FactSet) || !sameRef(rv.Inputs.Linkages, proofs[2].FactSet) {
		return nil, fmt.Errorf("receipt reference proof ancestry differs")
	}
	progress("opening resolved outside-spending graph with complete field readback")
	e, err := ie.OpenResolvedReader(ctx, ie.ResolvedInput{StorageRoot: root, Cycle: b.Cycle, ReadinessBundlePath: o.OutsideBundle, Endpoint: o.Receipt.Endpoint, Username: o.Receipt.Username, Password: o.Receipt.Password}, o.OutsideBundleSHA256)
	if err != nil {
		return nil, err
	}
	ev := e.View()
	if filepath.Base(o.OutsideBundle) != ev.BundleID+".json" {
		return nil, fmt.Errorf("immutable outside-spending bundle required")
	}
	progress("verifying outside source membership and common candidate/committee reference content")
	em, err := occ.ProveScheduleEReleaseMembership(ctx, root, filepath.Join(root, "facts/fec/schedule-e/manifests", ev.Inputs.ScheduleEFactSetID+".json"), b.Input.ReleaseID, b.Input.ReleaseSHA256)
	if err != nil {
		return nil, err
	}
	if em.Cycle != b.Cycle || em.FactSet.ID != ev.Inputs.ScheduleEFactSetID || em.FactSet.SHA256 != ev.Inputs.ScheduleEManifestSHA256 || em.SourceRelease.ID != ev.Inputs.SourceReleaseID {
		return nil, fmt.Errorf("outside graph source membership differs")
	}
	for _, item := range []struct {
		path, dataset string
		ref           receipts.Reference
	}{
		{o.Receipt.Candidates, "candidate-master", rv.Inputs.Candidates},
		{filepath.Join(root, "facts/fec/classic/candidate-master/manifests", ev.Inputs.CandidateFactSetID+".json"), "candidate-master", receipts.Reference{ID: ev.Inputs.CandidateFactSetID, SHA256: ev.Inputs.CandidateManifestSHA256}},
		{filepath.Join(root, "facts/fec/classic/committee-master/manifests", ev.Inputs.CommitteeFactSetID+".json"), "committee-master", receipts.Reference{ID: ev.Inputs.CommitteeFactSetID, SHA256: ev.Inputs.CommitteeManifestSHA256}},
	} {
		p, err := occ.ProveClassicReference(ctx, root, item.path, item.dataset, b.Input.ReleaseID, b.Input.ReleaseSHA256)
		if err != nil {
			return nil, err
		}
		if p.Cycle != b.Cycle || !sameRef(item.ref, p.FactSet) {
			return nil, fmt.Errorf("generation reference identity differs")
		}
		proofs = append(proofs, p)
	}
	if !occ.SameReferenceContent(proofs[3], proofs[4]) || !occ.SameReferenceContent(proofs[0], proofs[5]) {
		return nil, fmt.Errorf("cross-family master content/schema differs")
	}
	progress("opening receiver and sender ledgers with complete field readback")
	f, err := flow.OpenReader(ctx, flow.Options{StorageRoot: root, Bundle: o.FlowBundle, Cycle: b.Cycle, Endpoint: o.Receipt.Endpoint, Username: o.Receipt.Username, Password: o.Receipt.Password})
	if err != nil {
		return nil, err
	}
	fv := f.View()
	if fv.BundleID != b.BundleID || fv.BundleSHA != bd || fv.Inputs != b.Input {
		return nil, fmt.Errorf("flow generation ancestry changed")
	}
	out = assemble(o.BuildSHA256, rv, fv, f.Database(), f.ComponentCount(), ev, proofs, em)
	progress("rechecking all three graph completion boundaries")
	if err = r.VerifyCompletion(ctx); err != nil {
		return nil, err
	}
	if err = f.VerifyCompletion(ctx); err != nil {
		return nil, err
	}
	if err = e.VerifyCompletion(ctx); err != nil {
		return nil, err
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	out.GenerationID = identity(out)
	if o.ExpectedGenerationID != "" && out.GenerationID != o.ExpectedGenerationID {
		return nil, fmt.Errorf("generation replay identity differs")
	}
	return &Reader{generation: out, receipts: r, flow: f, outside: e}, nil
}

func sameRef(a receipts.Reference, b occ.ReferenceIdentity) bool {
	return a.ID == b.ID && a.SHA256 == b.SHA256
}

func compatibleReceipt(r receipts.CycleView, b flowcalc.Bundle) error {
	if r.Inputs.Cycle != b.Cycle || r.Inputs.Facts.ID != b.Input.A.FactSetID || r.Inputs.Facts.SHA256 != b.Input.A.ManifestSHA256 || r.Inputs.SourceRelease != b.Input.A.SourceReleaseID || r.SourceRows != b.Input.A.Facts {
		return fmt.Errorf("generation needs exact shared Schedule A ancestry and cycle")
	}
	return nil
}
