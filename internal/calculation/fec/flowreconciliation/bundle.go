package flowreconciliation

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
)

const BundleVersion = "legal-tender.fec.committee-flow-evidence-bundle.v1"
const BundleBase = "bundles/fec/committee-flow-evidence/v1"

type BundleOptions struct{ StorageRoot, Calculation, Committee, Cycle string }
type CalculationReference struct {
	CalculationSetID string `json:"calculation_set_id"`
	ManifestSHA256   string `json:"manifest_sha256"`
}
type Bundle struct {
	SchemaVersion        string               `json:"schema_version"`
	BundleID             string               `json:"bundle_id"`
	Cycle                string               `json:"cycle"`
	State                string               `json:"state"`
	Consumer             string               `json:"consumer"`
	Calculation          CalculationReference `json:"calculation"`
	Input                Inputs               `json:"input"`
	Committee            FactReference        `json:"committee_master"`
	IdentityScope        string               `json:"identity_scope"`
	EconomicFlowEligible bool                 `json:"economic_flow_eligible"`
	Checks               []string             `json:"checks"`
}

func bundleChecks() []string {
	return []string{"immutable_calculation", "candidate_evidence_replay", "separate_ledger_conservation", "exact_source_ancestry", "same_cycle_master_source_bytes", "master_fact_integrity", "observation_only_consumer"}
}

func newBundle(r Result, digest string, master FactReference) Bundle {
	b := Bundle{SchemaVersion: BundleVersion, Cycle: r.Cycle, State: "ready", Consumer: "committee_flow_evidence", Calculation: CalculationReference{r.CalculationSetID, digest}, Input: r.Input, Committee: master, IdentityScope: "same_cycle_master_only", Checks: bundleChecks()}
	b.BundleID = bundleIdentity(b)
	return b
}
func bundleIdentity(b Bundle) string { b.BundleID = ""; return hashJSON(b) }

// PublishBundle freezes exact Go-verified inputs, not graph completeness or
// economic-flow eligibility. Historical identity automation is not accepted.
func PublishBundle(ctx context.Context, o BundleOptions) (Bundle, error) {
	if !validCycle(o.Cycle) || o.StorageRoot == "" || o.Calculation == "" || o.Committee == "" {
		return Bundle{}, fmt.Errorf("storage root, calculation, committee master, and cycle required")
	}
	r, digest, err := LoadPublished(ctx, o.StorageRoot, o.Calculation)
	if err != nil {
		return Bundle{}, err
	}
	if r.Cycle != o.Cycle {
		return Bundle{}, fmt.Errorf("reconciliation cycle mismatch")
	}
	master, err := loadCommittee(ctx, o.StorageRoot, o.Committee, r)
	if err != nil {
		return Bundle{}, err
	}
	return commitBundle(ctx, o.StorageRoot, newBundle(r, digest, master))
}

func commitBundle(ctx context.Context, root string, b Bundle) (Bundle, error) {
	unlock, err := publicationLock(ctx, root, BundleBase, b.Cycle)
	if err != nil {
		return Bundle{}, err
	}
	defer unlock()
	current := filepath.Join(root, BundleBase, "current", b.Cycle+".json")
	if _, err := os.Stat(current); err == nil {
		prior, _, err := readBundle(root, current)
		if err != nil {
			return Bundle{}, err
		}
		if prior.Cycle != b.Cycle {
			return Bundle{}, fmt.Errorf("current bundle cycle mismatch")
		}
	} else if !os.IsNotExist(err) {
		return Bundle{}, err
	}
	content, err := jsonBytes(b)
	if err != nil {
		return Bundle{}, err
	}
	if err := writePublicationJSON(ctx, filepath.Join(root, BundleBase, "manifests", b.BundleID+".json"), content, true); err != nil {
		return Bundle{}, err
	}
	if err := writePublicationJSON(ctx, current, content, false); err != nil {
		return Bundle{}, err
	}
	return b, nil
}

func readBundle(root, path string) (Bundle, string, error) {
	var b Bundle
	path, err := inside(root, path)
	if err != nil {
		return b, "", err
	}
	content, err := os.ReadFile(path)
	if err != nil {
		return b, "", err
	}
	if err := strictJSON(content, &b); err != nil {
		return b, "", err
	}
	if b.SchemaVersion != BundleVersion || !validCycle(b.Cycle) || b.BundleID != bundleIdentity(b) || b.State != "ready" || b.Consumer != "committee_flow_evidence" || b.IdentityScope != "same_cycle_master_only" || b.EconomicFlowEligible || !reflect.DeepEqual(b.Checks, bundleChecks()) || !validDigest(b.Calculation.CalculationSetID) || !validDigest(b.Calculation.ManifestSHA256) || !validDigest(b.Committee.FactSetID) || !validDigest(b.Committee.ManifestSHA256) {
		return b, "", fmt.Errorf("unsupported or inconsistent observation bundle")
	}
	if err := verifyPinnedBytes(root, BundleBase, b.BundleID, content); err != nil {
		return b, "", err
	}
	return b, hashBytes(content), nil
}

// LoadBundleMetadata validates the immutable bundle bytes and identity only.
// Use it to reject incompatible consumers before expensive backing scans. It is
// never sufficient to accept a graph or calculation; LoadBundle must still pass.
func LoadBundleMetadata(root, path string) (Bundle, string, error) {
	return readBundle(root, path)
}

// LoadBundle revalidates all backing and returns the exact calculation. It
// resolves immutable paths only; later current-pointer changes are irrelevant.
func LoadBundle(ctx context.Context, root, path string) (Bundle, string, Result, error) {
	b, digest, err := readBundle(root, path)
	if err != nil {
		return b, "", Result{}, err
	}
	r, rd, err := LoadPublished(ctx, root, filepath.Join(root, PublicationBase, "manifests", b.Calculation.CalculationSetID+".json"))
	if err != nil {
		return b, "", r, err
	}
	master, err := loadCommittee(ctx, root, filepath.Join(root, committeeFactBase, "manifests", b.Committee.FactSetID+".json"), r)
	if err != nil {
		return b, "", r, err
	}
	if !reflect.DeepEqual(b, newBundle(r, rd, master)) {
		return b, "", r, fmt.Errorf("bundle differs from exact backing")
	}
	return b, digest, r, nil
}
