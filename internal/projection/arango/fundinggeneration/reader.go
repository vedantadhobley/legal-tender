package fundinggeneration

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"

	flow "github.com/vedantadhobley/legal-tender/internal/projection/arango/flowevidence"
	ie "github.com/vedantadhobley/legal-tender/internal/projection/arango/independentexpenditures"
	r "github.com/vedantadhobley/legal-tender/internal/projection/arango/receiptgraph"
	"github.com/vedantadhobley/legal-tender/internal/strictjson"
)

// Reader is a freshly verified in-process capability, never a deserialized
// approval. Keep it for a bounded query session to avoid reopening all sources.
type Reader struct {
	generation       Result
	receipts         *r.CycleReader
	flow             *flow.Reader
	outside          *ie.ResolvedReader
	consumerBuild    string
	manifestSHA      string
	shared           *r.SharedReader
	sharedGeneration *SharedGeneration
}

type ReadOptions struct {
	Generation, GenerationSHA256                       string
	StorageRoot, GraphManifest, Participants, Conduits string
	Endpoint, Username, Password, BuildSHA256          string
	Progress                                           func(string)
}

func readGeneration(path, sha string) (Result, error) {
	var v Result
	b, err := readGenerationBytes(path, sha)
	if err != nil {
		return v, err
	}
	if err := strictjson.Decode(b, &v); err != nil {
		return v, err
	}
	if v.SchemaVersion != Version || v.Policy != Policy || v.State != "verified_declared_evidence_families" || !validDigest(v.BuildSHA256) || v.GenerationID != identity(v) || v.FinancialEligibility || v.TerminalEligible {
		return v, fmt.Errorf("unsupported generation contract or identity")
	}
	return v, nil
}

func readGenerationBytes(path, sha string) ([]byte, error) {
	if !validDigest(sha) {
		return nil, fmt.Errorf("exact generation byte checksum required")
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	b, err := io.ReadAll(io.LimitReader(f, (1<<20)+1))
	if err != nil {
		return nil, err
	}
	h := sha256.Sum256(b)
	if len(b) > 1<<20 || hex.EncodeToString(h[:]) != sha {
		return nil, fmt.Errorf("generation bytes differ")
	}
	return b, nil
}

func OpenReader(ctx context.Context, o ReadOptions) (*Reader, error) {
	if !validDigest(o.BuildSHA256) || o.StorageRoot == "" || o.GraphManifest == "" || o.Participants == "" || o.Conduits == "" {
		return nil, fmt.Errorf("consumer build, storage and retained receipt locators required")
	}
	v, err := readGeneration(o.Generation, o.GenerationSHA256)
	if err != nil {
		return nil, err
	}
	opt, err := readerInputs(o, v)
	if err != nil {
		return nil, err
	}
	reader, err := openVerified(ctx, opt)
	if err != nil {
		return nil, err
	}
	if !reflect.DeepEqual(reader.generation, v) {
		return nil, fmt.Errorf("generation differs from freshly verified backing")
	}
	reader.consumerBuild, reader.manifestSHA = o.BuildSHA256, o.GenerationSHA256
	return reader, nil
}

func readerInputs(o ReadOptions, v Result) (Options, error) {
	i := v.Receipts.Inputs
	for _, id := range []string{i.Facts.ID, i.Candidates.ID, i.Committees.ID, i.Linkages.ID, v.CommitteeFlow.BundleID, v.OutsideSpending.BundleID} {
		if !validDigest(id) {
			return Options{}, fmt.Errorf("invalid generation input identity")
		}
	}
	classic := func(dataset, id string) string {
		return filepath.Join(o.StorageRoot, "facts/fec/classic", dataset, "manifests", id+".json")
	}
	return Options{
		Receipt:         r.Options{StorageRoot: o.StorageRoot, Participants: o.Participants, ParticipantID: i.Participants.ID, Conduits: o.Conduits, ConduitID: i.Conduits.ID, Facts: filepath.Join(o.StorageRoot, "facts/fec/schedule-a/columnar/manifests", i.Facts.ID+".json"), Committees: classic("committee-master", i.Committees.ID), Candidates: classic("candidate-master", i.Candidates.ID), Linkages: classic("candidate-committee-linkage", i.Linkages.ID), Endpoint: o.Endpoint, Username: o.Username, Password: o.Password, Progress: o.Progress},
		ReceiptManifest: o.GraphManifest, ReceiptSHA256: v.Receipts.Projection.SHA256,
		FlowBundle: filepath.Join(o.StorageRoot, "bundles/fec/committee-flow-evidence/v1/manifests", v.CommitteeFlow.BundleID+".json"), FlowBundleSHA256: v.CommitteeFlow.BundleSHA,
		OutsideBundle: filepath.Join(o.StorageRoot, "bundles/fec/resolved-independent-expenditure-projection/manifests", v.OutsideSpending.BundleID+".json"), OutsideBundleSHA256: v.OutsideSpending.BundleSHA256,
		// Preserve the pinned producer's identity. The new consumer build is
		// recorded separately in each query result; it does not impersonate it.
		BuildSHA256: v.BuildSHA256, ExpectedGenerationID: v.GenerationID,
	}, nil
}

func (r *Reader) VerifyCompletion(ctx context.Context) error {
	if err := r.receipts.VerifyCompletion(ctx); err != nil {
		return err
	}
	if err := r.flow.VerifyCompletion(ctx); err != nil {
		return err
	}
	if err := r.outside.VerifyCompletion(ctx); err != nil {
		return err
	}
	if r.shared != nil {
		return r.shared.VerifyCompletion(ctx)
	}
	return nil
}
