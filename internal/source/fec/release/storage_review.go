package release

import (
	"fmt"
	"os"
	"slices"
)

// StorageReview is a read-only planning diagnostic, never an acquisition or
// publication authorization. Prior output sizes are a scenario, not bounds on
// unseen source bytes. The existing acquisition/stage result schemas stay fixed.
type StorageReview struct {
	SchemaVersion      string                 `json:"schema_version"`
	CandidateReleaseID string                 `json:"candidate_release_id"`
	PriorReleaseID     string                 `json:"prior_release_id,omitempty"`
	Acquisition        StoragePreflight       `json:"acquisition"`
	AcquisitionIssues  []Issue                `json:"acquisition_issues"`
	Scenario           StagingStorageScenario `json:"staging_scenario"`
}

type StagingStorageScenario struct {
	Basis                   string                  `json:"basis"`
	Complete                bool                    `json:"complete"`
	FitsBudget              bool                    `json:"fits_budget"`
	NewOutputBytes          uint64                  `json:"new_output_bytes"`
	NewScheduleAOutputBytes uint64                  `json:"new_schedule_a_output_bytes"`
	HotBytesWithReserve     uint64                  `json:"hot_bytes_with_reserve"`
	RequiredFreeBytes       uint64                  `json:"required_free_bytes"`
	HotCapExcessBytes       uint64                  `json:"hot_cap_excess_bytes"`
	FreeSpaceShortfallBytes uint64                  `json:"free_space_shortfall_bytes"`
	Outputs                 []StorageOutputEstimate `json:"outputs"`
}

type StorageOutputEstimate struct {
	SourceID           string  `json:"source_id"`
	Selection          string  `json:"selection"`
	Period             string  `json:"period"`
	Basis              string  `json:"basis"`
	NewCompressedBytes *uint64 `json:"new_compressed_bytes,omitempty"`
}

// ReviewStorage uses the real acquisition counter and inventory-selected stage
// outputs. It never creates directories, hashes large bodies, captures sources,
// or grants new-content/checkpoint deduplication credit. Storage must be quiescent
// on one filesystem, as required by staging's atomic hard-link finalization.
func ReviewStorage(inventory Inventory, plan ReleasePlan, current *ReleaseManifest, storageRoot string, diskUsage func(string) (DiskSpace, error)) (StorageReview, error) {
	result := StorageReview{
		SchemaVersion:      "legal-tender.fec.storage-review.v2",
		CandidateReleaseID: plan.CandidateReleaseID,
		PriorReleaseID:     plan.PriorReleaseID,
		AcquisitionIssues:  []Issue{},
	}
	if storageRoot == "" {
		return result, fmt.Errorf("storage root is required")
	}
	if issues := ValidatePlan(inventory, plan); len(issues) != 0 || plan.Status != PlanUpdateAvailable {
		return result, fmt.Errorf("storage review requires a valid update_available plan")
	}
	if current == nil && plan.PriorReleaseID != "" {
		return result, fmt.Errorf("storage review requires the plan's prior manifest")
	}
	if current != nil {
		if issues := validateKnownManifest(*current); len(issues) != 0 {
			return result, fmt.Errorf("invalid prior manifest: %s", issues[0].Message)
		}
		if current.ReleaseID != plan.PriorReleaseID {
			return result, fmt.Errorf("prior manifest does not match the plan baseline")
		}
	}
	if _, err := validateReusableArtifacts(storageRoot, inventory, plan, current); err != nil {
		return result, err
	}
	selectedByID := make(map[string]SelectedSource, len(plan.SelectedSources))
	for _, source := range plan.SelectedSources {
		selectedByID[source.SourceID] = source
	}
	if diskUsage == nil {
		diskUsage = defaultDiskUsage
	}
	preflight, issues, err := preflightStorage(storageRoot, scheduleAStorageRoot(storageRoot), plan, selectedByID, diskUsage)
	if err != nil {
		return result, err
	}
	result.Acquisition = preflight
	result.AcquisitionIssues = append(result.AcquisitionIssues, issues...)
	// Missing lengths leave the acquisition model incomplete, not zero-cost.
	if hasStorageIssue(issues, "content_length_required") {
		return result, fmt.Errorf("positive changed-source lengths are required for storage review")
	}
	outputs := estimateStageOutputs(inventory, plan, current)
	// Even reused outputs must exist. This is a size/name check, not the stage
	// operation's full hash verification, and does not read the output body.
	if current != nil {
		priorByID := make(map[string]StagedOutput, len(current.StagedOutputs))
		for _, output := range current.StagedOutputs {
			priorByID[output.SourceID+"\x00"+output.Selection] = output
		}
		for _, output := range outputs {
			if output.Basis != "unchanged_source" {
				continue
			}
			prior := priorByID[output.SourceID+"\x00"+output.Selection]
			path, err := resolveStorageKey(storageRoot, prior.StorageKey)
			if err != nil {
				return result, err
			}
			info, err := os.Stat(path)
			if err != nil {
				return result, err
			}
			if !info.Mode().IsRegular() || info.Size() < 0 || uint64(info.Size()) != prior.CompressedByteCount {
				return result, fmt.Errorf("reused staged output metadata mismatch: %s", output.Selection)
			}
		}
	}
	result.Scenario, err = stagingStorageScenario(preflight, slices.Contains(plan.ChangedSourceIDs, ScheduleASourceID), outputs)
	return result, err
}

func hasStorageIssue(issues []Issue, code string) bool {
	return slices.ContainsFunc(issues, func(issue Issue) bool { return issue.Code == code })
}

func estimateStageOutputs(inventory Inventory, plan ReleasePlan, current *ReleaseManifest) []StorageOutputEstimate {
	priorOutputs := make(map[string]StagedOutput)
	if current != nil {
		for _, output := range current.StagedOutputs {
			priorOutputs[outputIdentity(output)] = output
		}
	}
	outputs := make([]StorageOutputEstimate, 0)
	for _, desired := range desiredStageOutputs(inventory) {
		output := StorageOutputEstimate{SourceID: desired.SourceID, Selection: desired.Selection, Period: desired.Period, Basis: "unknown"}
		if prior, exists := priorOutputs[outputIdentity(desired)]; exists && prior.Representation == desired.Representation && prior.Period == desired.Period {
			bytes := prior.CompressedByteCount
			output.Basis = "prior_size_new_content"
			if slices.Contains(plan.ReusedSourceIDs, desired.SourceID) {
				bytes = 0
				output.Basis = "unchanged_source"
			}
			output.NewCompressedBytes = &bytes
		}
		outputs = append(outputs, output)
	}
	return outputs
}

func stagingStorageScenario(preflight StoragePreflight, scheduleAChanged bool, outputs []StorageOutputEstimate) (StagingStorageScenario, error) {
	result := StagingStorageScenario{Basis: "prior_sizes_new_content_full_reserve", Complete: true, Outputs: outputs}
	if preflight.LargestExtractWorkingBytes == 0 {
		result.Basis = "prior_sizes_new_content_streaming_margin"
	}
	var newHotPeak uint64
	for _, output := range outputs {
		if output.NewCompressedBytes == nil {
			result.Complete = false
			continue
		}
		bytes := *output.NewCompressedBytes
		var err error
		result.NewOutputBytes, err = sumStorageBytes(result.NewOutputBytes, bytes)
		if err != nil {
			return result, err
		}
		if output.SourceID == ScheduleASourceID {
			result.NewScheduleAOutputBytes, err = sumStorageBytes(result.NewScheduleAOutputBytes, bytes)
			if err != nil {
				return result, err
			}
			newHotPeak = max(newHotPeak, result.NewScheduleAOutputBytes)
		} else {
			// All selected temporary files use the hot lane. Non-A outputs
			// leave it on finalization; Stage processes outputs in this order.
			peak, err := sumStorageBytes(result.NewScheduleAOutputBytes, bytes)
			if err != nil {
				return result, err
			}
			newHotPeak = max(newHotPeak, peak)
		}
	}
	working, err := sumStorageBytes(preflight.LargestExtractWorkingBytes, preflight.WorkingMarginBytes)
	if err != nil {
		return result, err
	}
	// Streaming preflight always includes the margin. Historical non-A
	// preflight omitted workspace; retain that interpretation for old scenarios.
	hot := preflight.ProjectedScheduleAHotBytes
	if !scheduleAChanged && preflight.LargestExtractWorkingBytes != 0 {
		hot, err = sumStorageBytes(hot, working)
		if err != nil {
			return result, err
		}
	}
	if preflight.LargestExtractWorkingBytes != 0 {
		newHotPeak = result.NewScheduleAOutputBytes // historical workspace layout
	}
	result.HotBytesWithReserve, err = sumStorageBytes(hot, newHotPeak)
	if err != nil {
		return result, err
	}
	// Reserve every new retained output plus the existing workspace/floor. The
	// temporary zstd file becomes its final inode; it is not a second retained
	// copy. A matching future CAS hash receives no speculative savings here.
	result.RequiredFreeBytes, err = sumStorageBytes(preflight.RemainingDownloadBytes, result.NewOutputBytes, working, preflight.FreeFloorBytes)
	if err != nil {
		return result, err
	}
	if result.HotBytesWithReserve > preflight.ScheduleAHotCapBytes {
		result.HotCapExcessBytes = result.HotBytesWithReserve - preflight.ScheduleAHotCapBytes
	}
	if result.RequiredFreeBytes > preflight.FreeBytesBefore {
		result.FreeSpaceShortfallBytes = result.RequiredFreeBytes - preflight.FreeBytesBefore
	}
	result.FitsBudget = result.Complete && result.HotCapExcessBytes == 0 && result.FreeSpaceShortfallBytes == 0
	return result, nil
}

func sumStorageBytes(values ...uint64) (uint64, error) {
	var total uint64
	for _, value := range values {
		if value > ^uint64(0)-total {
			return 0, fmt.Errorf("storage projection byte count overflow")
		}
		total += value
	}
	return total, nil
}
