package release

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

// Stage extracts only inventory-selected members and relations into immutable
// zstd streams. It checkpoints each completed stream and never publishes the
// active release pointer.
func Stage(
	ctx context.Context,
	inventory Inventory,
	plan ReleasePlan,
	planSHA256 string,
	acquisition AcquisitionResult,
	acquisitionSHA256 string,
	current *ReleaseManifest,
	runID string,
	options StageOptions,
) (StageResult, error) {
	clock := options.Clock
	if clock == nil {
		clock = time.Now
	}
	result := StageResult{
		SchemaVersion:      StageSchemaVersion,
		InventoryVersion:   inventory.InventoryVersion,
		CandidateReleaseID: plan.CandidateReleaseID,
		PriorReleaseID:     plan.PriorReleaseID,
		PlanSHA256:         planSHA256,
		AcquisitionSHA256:  acquisitionSHA256,
		RunID:              runID,
		Status:             StageFailed,
		StartedAt:          clock().UTC(),
		Outputs:            []StagedOutput{},
		Checks:             []ReleaseCheck{},
		Issues:             []Issue{},
	}
	configureStageStorage(&result.Storage, options)
	fail := func(status, code, message, sourceID string) (StageResult, error) {
		result.Status = status
		if status == StageBlocked {
			result.Storage.Passed = false
		}
		result.CompletedAt = clock().UTC()
		result.Issues = append(result.Issues, Issue{SourceID: sourceID, Code: code, Message: message})
		return result, fmt.Errorf("%s: %s", code, message)
	}

	if options.StorageRoot == "" {
		return fail(StageFailed, "storage_root", "storage root is required", "")
	}
	if !ValidAcquisitionRunID(runID) {
		return fail(StageFailed, "run_id", "run ID contains unsupported characters", "")
	}
	if !validSHA256(planSHA256) || !validSHA256(acquisitionSHA256) {
		return fail(StageFailed, "stage_input_sha256", "plan or acquisition SHA-256 is invalid", "")
	}
	if issues := ValidatePlan(inventory, plan); len(issues) != 0 || plan.Status != PlanUpdateAvailable {
		return fail(StageFailed, "stage_plan", "only a valid update_available plan can be staged", "")
	}
	if issues := ValidateAcquisitionResult(inventory, acquisition); len(issues) != 0 || acquisition.Status != AcquisitionAcquired {
		return fail(StageFailed, "stage_acquisition", "only a valid acquired result can be staged", "")
	}
	if acquisition.CandidateReleaseID != plan.CandidateReleaseID || acquisition.PriorReleaseID != plan.PriorReleaseID || acquisition.PlanSHA256 != planSHA256 {
		return fail(StageFailed, "stage_input_identity", "plan and acquisition identities do not match", "")
	}
	if current != nil {
		if issues := validateKnownManifest(*current); len(issues) != 0 {
			return fail(StageFailed, "stage_current_manifest", issues[0].Message, "")
		}
		if current.ReleaseID != plan.PriorReleaseID {
			return fail(StageFailed, "stage_prior_release", "active release does not match the plan baseline", "")
		}
	} else if plan.PriorReleaseID != "" {
		return fail(StageFailed, "stage_prior_release", "plan has a prior release but no current manifest was supplied", "")
	}
	storageLock, err := lockSourceStorage(options.StorageRoot)
	if err != nil {
		if errors.Is(err, errStorageBusy) {
			return fail(StageBlocked, "storage_busy", err.Error(), "")
		}
		return fail(StageFailed, "storage_lock", err.Error(), "")
	}
	defer func() { _ = storageLock.Close() }()
	if err := validateAcquisitionArtifactFiles(options.StorageRoot, acquisition); err != nil {
		return fail(StageFailed, "stage_source_artifacts", err.Error(), "")
	}

	statePath := stageStatePath(options.StorageRoot, plan.CandidateReleaseID, runID)
	completed, err := readCompletedStage(statePath, plan.CandidateReleaseID, planSHA256, acquisitionSHA256, runID)
	if err != nil {
		return fail(StageFailed, "stage_state", err.Error(), "")
	}
	if completed != nil {
		if issues := ValidateStageResult(inventory, *completed); len(issues) != 0 {
			return fail(StageFailed, "stage_state", issues[0].Message, "")
		}
		for _, output := range completed.Outputs {
			artifact, exists := acquisitionArtifactBySource(acquisition, output.SourceID)
			if !exists || output.SourceArtifactSHA256 != artifact.SHA256 {
				return fail(StageFailed, "stage_state_source", "completed output does not reference its acquired source artifact", output.SourceID)
			}
			if err := validateStagedOutputIdentity(ctx, options.StorageRoot, output); err != nil {
				return fail(StageFailed, "stage_state_output", fmt.Sprintf("%s: %v", output.Selection, err), output.SourceID)
			}
		}
		return *completed, nil
	}

	if err := os.MkdirAll(options.StorageRoot, 0o750); err != nil {
		return fail(StageFailed, "stage_storage", err.Error(), "")
	}
	if err := os.MkdirAll(scheduleAStorageRoot(options.StorageRoot), 0o750); err != nil {
		return fail(StageFailed, "stage_storage", err.Error(), "")
	}
	storage, storageIssues, err := inspectStageStorage(options.StorageRoot, result.Storage, options, true)
	if err != nil {
		return fail(StageFailed, "stage_storage", err.Error(), "")
	}
	result.Storage = storage
	if len(storageIssues) != 0 {
		result.Checks = append(result.Checks, ReleaseCheck{ID: "storage_budget", Passed: false, Severity: "block", Detail: storageIssues[0].Message})
		return fail(StageBlocked, storageIssues[0].Code, storageIssues[0].Message, "")
	}

	artifactBySource := make(map[string]AcquisitionArtifact, len(acquisition.Artifacts))
	for _, artifact := range acquisition.Artifacts {
		artifactBySource[artifact.SourceID] = artifact
	}
	checkpointPath := stageCheckpointPath(options.StorageRoot, plan.CandidateReleaseID, runID)
	completedOutputs, err := readStageCheckpoint(checkpointPath, plan.CandidateReleaseID, planSHA256, acquisitionSHA256, runID)
	if err != nil {
		return fail(StageFailed, "stage_checkpoint", err.Error(), "")
	}
	priorOutputs := make(map[string]StagedOutput)
	if current != nil {
		for _, output := range current.StagedOutputs {
			priorOutputs[outputIdentity(output)] = output
		}
	}

	memberExtractor := options.MemberExtractor
	if memberExtractor == nil {
		memberExtractor = extractSelectedMember
	}
	relationExtractor := options.RelationExtractor
	if relationExtractor == nil {
		relationExtractor = func(ctx context.Context, artifactPath, relation, period string, destination io.Writer) (uint64, error) {
			return extractSelectedRelation(ctx, options.PGRestorePath, artifactPath, relation, period, destination)
		}
	}

	for _, desired := range desiredStageOutputs(inventory) {
		artifact := artifactBySource[desired.SourceID]
		desired.SourceArtifactSHA256 = artifact.SHA256
		identity := outputIdentity(desired)
		if checkpoint, exists := completedOutputs[identity]; exists {
			if checkpoint.SourceArtifactSHA256 != artifact.SHA256 {
				return fail(StageFailed, "stage_checkpoint_source", "checkpoint output belongs to another source artifact", desired.SourceID)
			}
			if err := validateStagedOutputIdentity(ctx, options.StorageRoot, checkpoint); err != nil {
				return fail(StageFailed, "stage_checkpoint_output", err.Error(), desired.SourceID)
			}
			result.Outputs = append(result.Outputs, checkpoint)
			continue
		}
		if prior, exists := priorOutputs[identity]; exists && prior.SourceArtifactSHA256 == artifact.SHA256 {
			if err := validateStagedOutputIdentity(ctx, options.StorageRoot, prior); err != nil {
				return fail(StageFailed, "stage_reused_output", err.Error(), desired.SourceID)
			}
			prior.Disposition = "reused"
			result.Outputs = append(result.Outputs, prior)
			completedOutputs[identity] = prior
			if err := writeStageCheckpoint(checkpointPath, plan.CandidateReleaseID, planSHA256, acquisitionSHA256, runID, result.Outputs); err != nil {
				return fail(StageFailed, "stage_checkpoint_write", err.Error(), desired.SourceID)
			}
			continue
		}
		if options.Progress != nil {
			options.Progress("staging " + desired.SourceID + " " + desired.Selection)
		}
		storage, storageIssues, err = inspectStageStorage(options.StorageRoot, result.Storage, options, true)
		if err != nil {
			return fail(StageFailed, "stage_storage", err.Error(), desired.SourceID)
		}
		result.Storage = storage
		if len(storageIssues) != 0 {
			return fail(StageBlocked, storageIssues[0].Code, storageIssues[0].Message, desired.SourceID)
		}
		artifactPath, err := openAcquisitionArtifact(options.StorageRoot, artifact)
		if err != nil {
			return fail(StageFailed, "stage_source_artifact", err.Error(), desired.SourceID)
		}
		var rowCount *uint64
		budget, err := newWriteBudget(ctx, options.StorageRoot, result.Storage, options.DiskUsage)
		if err != nil {
			status := StageFailed
			if errors.Is(err, errStorageBudget) {
				status = StageBlocked
			}
			return fail(status, "stage_write_budget", err.Error(), desired.SourceID)
		}
		temporaryPath, metrics, err := streamToZstd(ctx, stageTemporaryDirectory(options.StorageRoot, plan.CandidateReleaseID, runID), func(destination io.Writer) error {
			if desired.SelectionKind == "member" {
				return memberExtractor(ctx, artifactPath, desired.Selection, destination)
			}
			rows, extractErr := relationExtractor(ctx, artifactPath, desired.Selection, desired.Period, destination)
			rowCount = &rows
			return extractErr
		}, budget)
		if err != nil {
			if errors.Is(err, errStorageBudget) {
				return fail(StageBlocked, "stage_write_budget", err.Error(), desired.SourceID)
			}
			return fail(StageFailed, "stage_extract", err.Error(), desired.SourceID)
		}
		storageKey, err := finalizeStagedStream(ctx, options.StorageRoot, temporaryPath, desired.SourceID, metrics)
		// Only this attempt's private temporary file is disposable. A CAS object
		// linked before a later failure remains retained for a verified retry.
		cleanupErr := os.Remove(temporaryPath)
		if cleanupErr != nil && !os.IsNotExist(cleanupErr) {
			err = errors.Join(err, cleanupErr)
		}
		if err != nil {
			return fail(StageFailed, "stage_finalize", err.Error(), desired.SourceID)
		}
		desired.Disposition = "staged"
		desired.RowCount = rowCount
		desired.UncompressedByteCount = metrics.UncompressedBytes
		desired.UncompressedSHA256 = metrics.UncompressedSHA256
		desired.Compression = "zstd"
		desired.CompressionLevel = zstdCompressionLevel
		desired.CompressedByteCount = metrics.CompressedBytes
		desired.CompressedSHA256 = metrics.CompressedSHA256
		desired.StorageKey = storageKey
		desired.DecompressionValidated = true
		desired.StagedAt = clock().UTC()
		result.Outputs = append(result.Outputs, desired)
		completedOutputs[identity] = desired
		if err := writeStageCheckpoint(checkpointPath, plan.CandidateReleaseID, planSHA256, acquisitionSHA256, runID, result.Outputs); err != nil {
			return fail(StageFailed, "stage_checkpoint_write", err.Error(), desired.SourceID)
		}
	}

	storage, storageIssues, err = inspectStageStorage(options.StorageRoot, result.Storage, options, false)
	if err != nil {
		return fail(StageFailed, "stage_storage", err.Error(), "")
	}
	result.Storage = storage
	result.Checks = []ReleaseCheck{
		{ID: "input_identity", Passed: true, Severity: "block", Detail: "plan and acquisition identities match"},
		{ID: "source_artifact_membership", Passed: true, Severity: "block", Detail: fmt.Sprintf("all %d acquired artifacts are immutable and present", len(acquisition.Artifacts))},
		{ID: "selected_output_membership", Passed: len(result.Outputs) == len(expectedSelections(inventory)), Severity: "block", Detail: fmt.Sprintf("staged %d exact selected outputs", len(result.Outputs))},
		{ID: "output_integrity", Passed: true, Severity: "block", Detail: "new zstd outputs round-tripped; reused immutable outputs matched their compressed SHA-256"},
		{ID: "storage_budget", Passed: len(storageIssues) == 0, Severity: "block", Detail: "final hot-storage cap and free-space floor passed"},
	}
	if len(storageIssues) != 0 {
		return fail(StageBlocked, storageIssues[0].Code, storageIssues[0].Message, "")
	}
	result.Status = StageStaged
	result.CompletedAt = clock().UTC()
	if issues := ValidateStageResult(inventory, result); len(issues) != 0 {
		return fail(StageFailed, "stage_result", issues[0].Message, issues[0].SourceID)
	}
	if err := writeAtomicJSON(statePath, result); err != nil {
		return fail(StageFailed, "stage_state_write", err.Error(), "")
	}
	return result, nil
}

func acquisitionArtifactBySource(acquisition AcquisitionResult, sourceID string) (AcquisitionArtifact, bool) {
	for _, artifact := range acquisition.Artifacts {
		if artifact.SourceID == sourceID {
			return artifact, true
		}
	}
	return AcquisitionArtifact{}, false
}

func desiredStageOutputs(inventory Inventory) []StagedOutput {
	result := make([]StagedOutput, 0, len(expectedSelections(inventory)))
	for _, source := range inventory.Sources {
		for _, member := range source.SelectedMembers {
			result = append(result, StagedOutput{SourceID: source.SourceID, SelectionKind: "member", Selection: member, Period: source.Periods[0], Representation: "selected_member_zstd"})
		}
		for _, relation := range normalizedRelationSelections(source) {
			if !relationRequiresStage(relation) {
				continue
			}
			fieldCount := relation.FieldCount
			result = append(result, StagedOutput{SourceID: source.SourceID, SelectionKind: "relation", Selection: relation.Name, Period: relation.Scope, Representation: "postgresql_copy_text_data_rows_zstd", ContractedFieldCount: &fieldCount})
		}
	}
	return result
}

func configureStageStorage(storage *StageStorage, options StageOptions) {
	storage.ScheduleAHotCapBytes = options.ScheduleAHotCapBytes
	if storage.ScheduleAHotCapBytes == 0 {
		storage.ScheduleAHotCapBytes = ScheduleAHotCapBytes
	}
	storage.FreeFloorBytes = options.FreeFloorBytes
	if storage.FreeFloorBytes == 0 {
		storage.FreeFloorBytes = AcquisitionFreeFloorBytes
	}
	storage.WorkingMarginBytes = options.WorkingMarginBytes
	if storage.WorkingMarginBytes == 0 {
		storage.WorkingMarginBytes = AcquisitionWorkingMarginBytes
	}
	storage.LargestExtractWorkingBytes = options.LargestExtractWorkBytes
}

func scheduleAStorageRoot(storageRoot string) string {
	return filepath.Join(storageRoot, "raw", "fec", "schedule-a")
}

func inspectStageStorage(storageRoot string, baseline StageStorage, options StageOptions, reserveWorking bool) (StageStorage, []Issue, error) {
	if err := checkLegacyWorkspace(storageRoot); err != nil {
		return baseline, nil, err
	}
	diskUsage := options.DiskUsage
	if diskUsage == nil {
		diskUsage = defaultDiskUsage
	}
	hot, err := directoryBytes(scheduleAStorageRoot(storageRoot))
	if err != nil {
		return baseline, nil, err
	}
	disk, err := diskUsage(storageRoot)
	if err != nil {
		return baseline, nil, err
	}
	if baseline.FreeBytesBefore == 0 {
		baseline.FreeBytesBefore = disk.AvailableBytes
		baseline.ScheduleAHotBytesBefore = hot
	}
	baseline.FreeBytesAfter = disk.AvailableBytes
	baseline.ScheduleAHotBytesAfter = hot
	working := uint64(0)
	if reserveWorking {
		if baseline.LargestExtractWorkingBytes > ^uint64(0)-baseline.WorkingMarginBytes {
			return baseline, nil, fmt.Errorf("stage working byte count overflow")
		}
		working = baseline.LargestExtractWorkingBytes + baseline.WorkingMarginBytes
	}
	issues := make([]Issue, 0, 2)
	if hot > baseline.ScheduleAHotCapBytes || working > baseline.ScheduleAHotCapBytes-hot {
		issues = append(issues, Issue{Code: "stage_schedule_a_hot_cap", Message: "Schedule A hot storage plus extraction workspace exceeds the configured cap"})
	}
	if disk.AvailableBytes < baseline.FreeFloorBytes || working > disk.AvailableBytes-baseline.FreeFloorBytes {
		issues = append(issues, Issue{Code: "stage_free_floor", Message: "staging would leave less than the required free-space floor"})
	}
	baseline.Passed = len(issues) == 0
	return baseline, issues, nil
}
