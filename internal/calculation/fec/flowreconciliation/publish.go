package flowreconciliation

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"

	"github.com/vedantadhobley/legal-tender/internal/source/fec/occurrence"
	artifact "github.com/vedantadhobley/legal-tender/internal/storage/artifact"
)

const PublicationBase = "calculations/fec/committee-flow-reconciliation/v1"

// Publish preserves the existing result contract and calculation identity.
// Evidence keys are relative to PublicationBase, not the source storage root.
// Every invocation verifies source backing; only new identities decode facts.
func Publish(ctx context.Context, o Options) (Result, error) {
	if o.StorageRoot == "" || o.OutputRoot != "" {
		return Result{}, fmt.Errorf("storage root required; publication owns its output root")
	}
	o.OutputRoot = filepath.Join(o.StorageRoot, PublicationBase)
	if err := prepareOptions(&o); err != nil {
		return Result{}, err
	}
	o.Progress("verifying exact source backing for reconciliation publication")
	inputs, a, b, err := loadInputs(ctx, o)
	if err != nil {
		return Result{}, err
	}
	return publishLoaded(ctx, o, inputs, a, b)
}

func publishLoaded(ctx context.Context, o Options, inputs Inputs, a occurrence.ScheduleAColumnarManifest, b occurrence.ScheduleBColumnarManifest) (Result, error) {
	unlock, err := publicationLock(ctx, o.StorageRoot, PublicationBase, o.Cycle)
	if err != nil {
		return Result{}, err
	}
	defer unlock()
	current := filepath.Join(o.OutputRoot, "current", o.Cycle+".json")
	if _, err := os.Stat(current); err == nil {
		r, _, err := readResultHeader(o.StorageRoot, current)
		if err != nil {
			return Result{}, err
		}
		if r.Cycle != o.Cycle {
			return Result{}, fmt.Errorf("current calculation belongs to another cycle")
		}
	} else if !os.IsNotExist(err) {
		return Result{}, err
	}
	id := calculationIdentity(o.Cycle, inputs, currentPolicy())
	path := filepath.Join(o.OutputRoot, "manifests", id+".json")
	var r Result
	if _, err := os.Stat(path); err == nil {
		r, _, err = readPublication(ctx, o.StorageRoot, path)
		if err != nil {
			return Result{}, err
		}
		if r.Input != inputs || r.Cycle != o.Cycle {
			return Result{}, fmt.Errorf("published calculation input collision")
		}
		o.Progress("reused verified reconciliation; no source-row scan")
	} else if !os.IsNotExist(err) {
		return Result{}, err
	} else {
		r, err = calculateLoaded(ctx, o, inputs, a, b)
		if err != nil {
			return Result{}, err
		}
		if err := validateResultHeader(r); err != nil {
			return Result{}, err
		}
		content, err := jsonBytes(r)
		if err != nil {
			return Result{}, err
		}
		if err := writePublicationJSON(ctx, path, content, true); err != nil {
			return Result{}, err
		}
		// Replay the persisted result and all compact evidence before readiness.
		if _, _, err := readPublication(ctx, o.StorageRoot, path); err != nil {
			return Result{}, err
		}
	}
	content, err := jsonBytes(r)
	if err != nil {
		return Result{}, err
	}
	if err := writePublicationJSON(ctx, current, content, false); err != nil {
		return Result{}, err
	}
	return r, nil
}

func validateResultHeader(r Result) error {
	if r.SchemaVersion != Version || r.State != "complete_candidate_reconciliation" || r.GraphEligible || !validCycle(r.Cycle) || !reflect.DeepEqual(r.Policy, currentPolicy()) || r.CalculationSetID != calculationIdentity(r.Cycle, r.Input, r.Policy) {
		return fmt.Errorf("unsupported or inconsistent published reconciliation")
	}
	if !strings.HasPrefix(r.Input.ReleaseID, "fec-") || !validDigest(strings.TrimPrefix(r.Input.ReleaseID, "fec-")) || !validDigest(r.Input.ReleaseSHA256) {
		return fmt.Errorf("invalid coordinated release identity")
	}
	for _, f := range []FactReference{r.Input.A, r.Input.B} {
		if !validDigest(f.FactSetID) || !validDigest(f.ManifestSHA256) || !validDigest(f.SourceArtifactSHA256) || !strings.HasPrefix(f.SourceReleaseID, "fec-") || !validDigest(strings.TrimPrefix(f.SourceReleaseID, "fec-")) {
			return fmt.Errorf("invalid published fact identity")
		}
	}
	for kind, d := range map[string]artifact.Descriptor{"schedule-a": r.A.Observations, "schedule-b": r.B.Observations, "assertions": r.Assertions} {
		if !validDigest(d.CompressedSHA256) || !validDigest(d.UncompressedSHA256) || d.Compression != "zstd" || d.CompressedBytes == 0 {
			return fmt.Errorf("invalid published evidence descriptor")
		}
		key := filepath.ToSlash(filepath.Join("evidence", r.CalculationSetID, kind, "sha256", d.CompressedSHA256[:2], d.CompressedSHA256+".jsonl.zst"))
		if d.StorageKey != key {
			return fmt.Errorf("noncanonical published evidence key")
		}
	}
	return nil
}

func readResultHeader(root, path string) (Result, []byte, error) {
	var r Result
	path, err := inside(root, path)
	if err != nil {
		return r, nil, err
	}
	content, err := os.ReadFile(path)
	if err != nil {
		return r, nil, err
	}
	if err := strictJSON(content, &r); err != nil {
		return r, nil, err
	}
	if err := validateResultHeader(r); err != nil {
		return r, nil, err
	}
	if err := verifyPinnedBytes(root, PublicationBase, r.CalculationSetID, content); err != nil {
		return r, nil, err
	}
	return r, content, nil
}

func readPublication(ctx context.Context, root, path string) (Result, string, error) {
	r, content, err := readResultHeader(root, path)
	if err != nil {
		return r, "", err
	}
	r, _, _, _, digest, err := readReviewEvidence(ctx, ReviewOptions{
		ResultPath:   filepath.Join(root, PublicationBase, "manifests", r.CalculationSetID+".json"),
		EvidenceRoot: filepath.Join(root, PublicationBase),
	})
	if err != nil {
		return r, "", err
	}
	if digest != hashBytes(content) {
		return r, "", fmt.Errorf("reconciliation changed while loading")
	}
	return r, digest, nil
}

// LoadPublished verifies immutable result/evidence bytes, full candidate replay,
// conservation, and exact A/B fact and coordinated-release backing.
func LoadPublished(ctx context.Context, root, path string) (Result, string, error) {
	r, digest, err := readPublication(ctx, root, path)
	if err != nil {
		return r, "", err
	}
	inputs, _, _, err := loadInputs(ctx, sourceOptions(root, r))
	if err != nil {
		return r, "", err
	}
	if inputs != r.Input {
		return r, "", fmt.Errorf("published reconciliation source ancestry differs")
	}
	return r, digest, nil
}

func sourceOptions(root string, r Result) Options {
	return Options{StorageRoot: root, Cycle: r.Cycle,
		ScheduleA: filepath.Join(root, "facts/fec/schedule-a/columnar/manifests", r.Input.A.FactSetID+".json"),
		ScheduleB: filepath.Join(root, "facts/fec/schedule-b/columnar/manifests", r.Input.B.FactSetID+".json"),
		Release:   filepath.Join(root, "releases/fec/manifests", r.Input.ReleaseID+".json"),
	}
}
