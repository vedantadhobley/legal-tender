package funding

import (
	"context"
	"fmt"
	"os"
	"slices"
	"sort"
	"sync"
	"sync/atomic"
)

const VerificationVersion = "legal-tender.funding-recovery-file-verification.v1"

type VerifyOptions struct {
	Options
	MaxBytes uint64 // Explicit admission ceiling, not a workspace estimate.
	Workers  int    // 1..8, each with a bounded hashing buffer.
}

type VerifiedFile struct {
	Path           string   `json:"path"`
	NodeIDs        []string `json:"node_ids"`
	Roles          []string `json:"roles"`
	ExpectedSHA256 string   `json:"expected_sha256"`
	ExpectedBytes  uint64   `json:"expected_bytes"`
	State          string   `json:"state"`
	ObservedSHA256 string   `json:"observed_sha256,omitempty"`
	ObservedBytes  uint64   `json:"observed_bytes"`
}

type FileVerification struct {
	Version         string            `json:"schema_version"`
	ID              string            `json:"verification_id"`
	PlanID          string            `json:"plan_id"`
	PlanSHA256      string            `json:"plan_canonical_json_sha256"`
	InputSHA256     string            `json:"input_sha256"`
	BuildSHA256     string            `json:"executable_sha256"`
	InventoryID     string            `json:"historical_inventory_id"`
	HistoryComplete bool              `json:"historical_inventory_complete"`
	Complete        bool              `json:"selected_file_bytes_verified"`
	RecoveryReady   bool              `json:"recovery_ready"`
	State           string            `json:"state"`
	TotalBytes      uint64            `json:"selected_logical_bytes"`
	VerifiedBytes   uint64            `json:"verified_logical_bytes"`
	Counts          map[string]uint64 `json:"verification_counts"`
	Files           []VerifiedFile    `json:"files"`
	Limitations     []string          `json:"limitations"`
}

// VerifyFiles derives a fresh typed plan from exact input pins. A saved plan,
// historical success flag or current pointer cannot authorize a body check.
// Success covers selected execution AND comparison bytes, not reconstruction.
func VerifyFiles(ctx context.Context, in Inputs, inputSHA string, o VerifyOptions) (FileVerification, error) {
	if o.HashBlobs || o.Workers < 1 || o.Workers > 8 || o.MaxBytes == 0 {
		return FileVerification{}, fmt.Errorf("explicit positive byte ceiling and 1..8 workers required; full-history hashing forbidden")
	}
	p, err := Plan(ctx, in, inputSHA, o.Options)
	if err != nil {
		return FileVerification{}, err
	}
	return verifyPlannedFiles(ctx, p, o)
}

// Internal: no externally supplied plan is trusted. Kept separate to exercise
// admission, file replacement and corruption after metadata planning in tests.
func verifyPlannedFiles(ctx context.Context, p RecoveryPlan, o VerifyOptions) (FileVerification, error) {
	out := FileVerification{Version: VerificationVersion, PlanID: p.ID, PlanSHA256: identity(p), InputSHA256: p.InputSHA256, BuildSHA256: p.BuildSHA256, InventoryID: p.Inventory.ID, HistoryComplete: p.Inventory.Complete, State: "dependency_plan_incomplete", Files: []VerifiedFile{}, Counts: map[string]uint64{}, Limitations: []string{
		"byte_integrity_only_normal_source_decoders_and_domain_validators_not_rerun",
		"historical_provenance_inventory_and_missing_records_unchanged",
		"point_in_time_checks_not_a_snapshot_backup_or_retention_enforcement",
		"runtime_availability_isolation_reconstruction_and_comparison_still_required",
	}}
	if !p.DependencyPlanComplete {
		return finishVerification(out), nil
	}
	var err error
	out.Files, out.TotalBytes, err = selectedFiles(p)
	if err != nil {
		return FileVerification{}, err
	}
	if out.TotalBytes > o.MaxBytes {
		out.State = "byte_ceiling_exceeded"
		return finishVerification(out), nil
	}
	root, err := os.OpenRoot(o.StorageRoot)
	if err != nil {
		return FileVerification{}, err
	}
	defer root.Close()
	var next atomic.Int64
	var wg sync.WaitGroup
	for range min(o.Workers, len(out.Files)) {
		wg.Go(func() {
			buffer := make([]byte, 128<<10)
			for ctx.Err() == nil {
				i := int(next.Add(1) - 1)
				if i >= len(out.Files) {
					return
				}
				out.Files[i] = verifyFile(ctx, root, out.Files[i], buffer)
			}
		})
	}
	wg.Wait()
	out.State = "selected_file_verification_failed"
	if ctx.Err() != nil {
		out.State = "cancelled"
	}
	out = finishVerification(out)
	if ctx.Err() != nil {
		return out, ctx.Err()
	}
	return out, nil
}

func finishVerification(out FileVerification) FileVerification {
	out.Complete = len(out.Files) > 0 && out.State == "selected_file_verification_failed"
	for _, f := range out.Files {
		out.Counts[f.State]++
		if f.State == "sha256_verified" {
			out.VerifiedBytes += f.ExpectedBytes
		} else {
			out.Complete = false
		}
	}
	if out.Complete {
		out.State = "selected_file_bytes_verified"
	}
	out.ID = identity(out)
	return out
}

// Physical paths are deduplicated, not digests: two copies with equal hashes
// both need checking. Metadata sizes come from the planner's verified bytes.
func selectedFiles(p RecoveryPlan) ([]VerifiedFile, uint64, error) {
	nodes := map[string]Node{}
	for _, n := range p.Inventory.Nodes {
		nodes[n.Key] = n
	}
	byPath := map[string]VerifiedFile{}
	for _, pn := range p.Nodes {
		roles := []string{}
		for _, r := range []string{comparisonRole, executionRole} {
			if slices.Contains(pn.Roles, r) {
				roles = append(roles, r)
			}
		}
		if len(roles) == 0 {
			continue
		}
		n, ok := nodes[pn.NodeID]
		if !ok || !relative(n.Reference.Path) || !validDigest(n.Reference.SHA256) {
			return nil, 0, fmt.Errorf("selected file lacks an exact immutable pin")
		}
		size := n.Reference.Bytes
		if size == nil && n.Reference.Kind != "file" && n.Verification == "sha256_verified" {
			size = n.ObservedBytes
		}
		if size == nil || *size >= 1<<63-1 {
			return nil, 0, fmt.Errorf("selected file lacks a bounded exact byte size")
		}
		f, exists := byPath[n.Reference.Path]
		if exists && (f.ExpectedSHA256 != n.Reference.SHA256 || f.ExpectedBytes != *size) {
			return nil, 0, fmt.Errorf("conflicting selected file pins")
		}
		if !exists {
			f = VerifiedFile{Path: n.Reference.Path, ExpectedSHA256: n.Reference.SHA256, ExpectedBytes: *size, State: "not_checked", NodeIDs: []string{}, Roles: []string{}}
		}
		f.NodeIDs = append(f.NodeIDs, pn.NodeID)
		f.Roles = append(f.Roles, roles...)
		sort.Strings(f.NodeIDs)
		sort.Strings(f.Roles)
		f.NodeIDs = slices.Compact(f.NodeIDs)
		f.Roles = slices.Compact(f.Roles)
		byPath[f.Path] = f
	}
	files := make([]VerifiedFile, 0, len(byPath))
	var total uint64
	for _, f := range byPath {
		if f.ExpectedBytes > ^uint64(0)-total {
			return nil, 0, fmt.Errorf("selected byte count overflow")
		}
		total += f.ExpectedBytes
		files = append(files, f)
	}
	sort.Slice(files, func(i, j int) bool { return files[i].Path < files[j].Path })
	if len(files) == 0 {
		return nil, 0, fmt.Errorf("empty selected recovery input set")
	}
	return files, total, nil
}
