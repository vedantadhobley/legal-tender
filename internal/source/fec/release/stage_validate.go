package release

import (
	"context"
	"fmt"
	"os"
)

// ValidateStageResult validates the durable staging control artifact. It does
// not touch storage; Stage and Publish validate physical objects separately.
func ValidateStageResult(inventory Inventory, result StageResult) []Issue {
	issues := make([]Issue, 0)
	add := func(code, message string) { issues = append(issues, Issue{Code: code, Message: message}) }
	if result.SchemaVersion != StageSchemaVersion {
		add("stage_schema_version", "unexpected staged-release schema version")
	}
	if result.InventoryVersion != inventory.InventoryVersion {
		add("stage_inventory_version", "staged release inventory version does not match")
	}
	if result.CandidateReleaseID == "" || !validSHA256(result.PlanSHA256) || !validSHA256(result.AcquisitionSHA256) || !ValidAcquisitionRunID(result.RunID) {
		add("stage_identity", "staged release input identity is incomplete")
	}
	if result.StartedAt.IsZero() || result.CompletedAt.IsZero() || result.CompletedAt.Before(result.StartedAt) {
		add("stage_timestamps", "staged release requires an ordered time window")
	}
	expected := expectedSelections(inventory)
	seen := make(map[string]struct{}, len(result.Outputs))
	for _, output := range result.Outputs {
		identity := outputIdentity(output)
		selection, exists := expected[identity]
		if !exists {
			issues = append(issues, Issue{SourceID: output.SourceID, Code: "stage_unknown_output", Message: "staged output is not selected by the inventory"})
			continue
		}
		if _, duplicate := seen[identity]; duplicate {
			issues = append(issues, Issue{SourceID: output.SourceID, Code: "stage_duplicate_output", Message: "selected output occurs more than once"})
			continue
		}
		seen[identity] = struct{}{}
		if output.Period != selection.period || !validSHA256(output.SourceArtifactSHA256) || !validSHA256(output.UncompressedSHA256) || !validSHA256(output.CompressedSHA256) || output.StorageKey == "" || output.CompressedByteCount == 0 || !output.DecompressionValidated || output.StagedAt.IsZero() {
			issues = append(issues, Issue{SourceID: output.SourceID, Code: "stage_output_shape", Message: "staged output metadata is incomplete or inconsistent"})
		}
		if validSHA256(output.CompressedSHA256) && output.StorageKey != stagedStorageKey(output.SourceID, output.CompressedSHA256) {
			issues = append(issues, Issue{SourceID: output.SourceID, Code: "stage_output_storage_key", Message: "staged output storage key is not its canonical content-addressed path"})
		}
		if output.Compression != "zstd" || output.CompressionLevel != zstdCompressionLevel || (output.Disposition != "staged" && output.Disposition != "reused") {
			issues = append(issues, Issue{SourceID: output.SourceID, Code: "stage_output_encoding", Message: "staged output encoding or disposition is invalid"})
		}
		if output.SelectionKind == "relation" {
			if output.Representation != "postgresql_copy_text_data_rows_zstd" || output.RowCount == nil || output.ContractedFieldCount == nil || *output.ContractedFieldCount != selection.fieldCount {
				issues = append(issues, Issue{SourceID: output.SourceID, Code: "stage_relation_shape", Message: "processed schedule relation output lacks its exact row and field evidence"})
			}
		} else if output.Representation != "selected_member_zstd" || output.RowCount != nil || output.ContractedFieldCount != nil {
			issues = append(issues, Issue{SourceID: output.SourceID, Code: "stage_member_shape", Message: "selected ZIP member output has invalid relation metadata"})
		}
	}
	for identity, selection := range expected {
		if _, exists := seen[identity]; !exists && result.Status == StageStaged {
			issues = append(issues, Issue{SourceID: selection.sourceID, Code: "stage_missing_output", Message: "required selected output is missing"})
		}
	}
	for _, check := range result.Checks {
		if check.ID == "" || check.Detail == "" || (check.Severity != "block" && check.Severity != "warn") {
			add("stage_check_shape", "staged release check is malformed")
		}
	}
	switch result.Status {
	case StageStaged:
		if len(result.Outputs) != len(expected) || len(result.Issues) != 0 || !result.Storage.Passed || len(result.Checks) < 4 {
			add("stage_success_shape", "staged result must contain all outputs, passing storage, checks, and no issues")
		}
		for _, check := range result.Checks {
			if check.Severity == "block" && !check.Passed {
				add("stage_blocking_check", "staged result contains a failed blocking check")
			}
		}
	case StageBlocked, StageFailed:
		if len(result.Issues) == 0 {
			add("stage_failure_issues", "blocked or failed stage requires an issue")
		}
	default:
		add("stage_status", "unknown staged-release status")
	}
	return issues
}

type expectedSelection struct {
	sourceID   string
	period     string
	fieldCount int
}

func expectedSelections(inventory Inventory) map[string]expectedSelection {
	result := make(map[string]expectedSelection)
	for _, source := range inventory.Sources {
		for _, member := range source.SelectedMembers {
			output := StagedOutput{SourceID: source.SourceID, SelectionKind: "member", Selection: member}
			result[outputIdentity(output)] = expectedSelection{sourceID: source.SourceID, period: source.Periods[0]}
		}
		for _, relation := range normalizedRelationSelections(source) {
			if !relationRequiresStage(relation) {
				continue
			}
			output := StagedOutput{SourceID: source.SourceID, SelectionKind: "relation", Selection: relation.Name}
			result[outputIdentity(output)] = expectedSelection{sourceID: source.SourceID, period: relation.Scope, fieldCount: relation.FieldCount}
		}
	}
	return result
}

func validateStagedOutputFile(ctx context.Context, storageRoot string, output StagedOutput) error {
	if err := validateStagedOutputIdentity(ctx, storageRoot, output); err != nil {
		return err
	}
	path, err := resolveStorageKey(storageRoot, output.StorageKey)
	if err != nil {
		return err
	}
	return verifyZstdStream(ctx, path, streamMetrics{
		UncompressedBytes:  output.UncompressedByteCount,
		UncompressedSHA256: output.UncompressedSHA256,
		CompressedBytes:    output.CompressedByteCount,
		CompressedSHA256:   output.CompressedSHA256,
	})
}

// validateStagedOutputIdentity verifies the exact immutable compressed object
// without decompressing it. A prior release or completed checkpoint already
// carries the successful decompression evidence; repeating hundreds of GiB of
// decompression cannot strengthen the identity check.
func validateStagedOutputIdentity(ctx context.Context, storageRoot string, output StagedOutput) error {
	path, err := resolveStorageKey(storageRoot, output.StorageKey)
	if err != nil {
		return err
	}
	info, err := os.Stat(path)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() || info.Size() < 0 || uint64(info.Size()) != output.CompressedByteCount || filepathDigest(path) != output.CompressedSHA256 {
		return fmt.Errorf("staged object metadata does not match immutable file")
	}
	digest, err := fileSHA256(ctx, path)
	if err != nil {
		return err
	}
	if digest != output.CompressedSHA256 {
		return fmt.Errorf("staged object does not match its compressed SHA-256")
	}
	return nil
}
