package release

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"sync"
	"time"
)

const defaultAcquisitionConcurrency = 3

var safeRunID = regexp.MustCompile(`^[A-Za-z0-9._-]{1,128}$`)

// ValidAcquisitionRunID reports whether a run ID is safe for both the wire
// contract and candidate-state path.
func ValidAcquisitionRunID(value string) bool {
	return safeRunID.MatchString(value)
}

// Acquire captures every changed body authorized by one update_available plan,
// rechecks all publisher versions, validates containers, and moves the bytes
// into immutable content-addressed storage. It does not publish the release.
func Acquire(
	ctx context.Context,
	client HTTPDoer,
	inventory Inventory,
	plan ReleasePlan,
	current *ReleaseManifest,
	planSHA256 string,
	runID string,
	options AcquisitionOptions,
) (AcquisitionResult, error) {
	clock := options.Clock
	if clock == nil {
		clock = time.Now
	}
	result := AcquisitionResult{
		SchemaVersion:      AcquisitionSchemaVersion,
		InventoryVersion:   inventory.InventoryVersion,
		CandidateReleaseID: plan.CandidateReleaseID,
		PriorReleaseID:     plan.PriorReleaseID,
		PlanSHA256:         planSHA256,
		RunID:              runID,
		Status:             AcquisitionFailed,
		StartedAt:          clock().UTC(),
		Storage: StoragePreflight{
			ScheduleAHotCapBytes:       ScheduleAHotCapBytes,
			FreeFloorBytes:             AcquisitionFreeFloorBytes,
			WorkingMarginBytes:         AcquisitionWorkingMarginBytes,
			LargestExtractWorkingBytes: 0, // extraction streams directly into zstd
		},
		Artifacts:   []AcquisitionArtifact{},
		PostCapture: PostCaptureCheck{Versions: []PostCaptureVersion{}},
		Issues:      []Issue{},
	}
	fail := func(status, code, message, sourceID string) (AcquisitionResult, error) {
		result.Status = status
		result.CompletedAt = clock().UTC()
		result.Issues = append(result.Issues, Issue{SourceID: sourceID, Code: code, Message: message})
		return result, fmt.Errorf("%s: %s", code, message)
	}

	if client == nil {
		return fail(AcquisitionFailed, "acquisition_client", "HTTP client is required", "")
	}
	if options.StorageRoot == "" {
		return fail(AcquisitionFailed, "storage_root", "storage root is required", "")
	}
	if !ValidAcquisitionRunID(runID) {
		return fail(AcquisitionFailed, "run_id", "run ID must contain only letters, digits, dot, underscore, or hyphen", "")
	}
	if !validSHA256(planSHA256) {
		return fail(AcquisitionFailed, "plan_sha256", "plan SHA-256 must be 64 lowercase hexadecimal characters", "")
	}
	if issues := ValidatePlan(inventory, plan); len(issues) != 0 {
		result.Issues = append(result.Issues, issues...)
		result.CompletedAt = clock().UTC()
		return result, fmt.Errorf("invalid release plan: %s", issues[0].Message)
	}
	if plan.Status != PlanUpdateAvailable {
		return fail(AcquisitionFailed, "plan_not_update_available", "only update_available authorizes body acquisition", "")
	}
	if result.StartedAt.IsZero() {
		return fail(AcquisitionFailed, "acquisition_started_at", "acquisition start time is required", "")
	}

	storageLock, err := lockSourceStorage(options.StorageRoot)
	if err != nil {
		if errors.Is(err, errStorageBusy) {
			return fail(AcquisitionBlocked, "storage_busy", err.Error(), "")
		}
		return fail(AcquisitionFailed, "storage_lock", err.Error(), "")
	}
	defer func() { _ = storageLock.Close() }()
	statePath := acquisitionStatePath(options.StorageRoot, plan.CandidateReleaseID, runID)
	completed, err := readCompletedAcquisition(statePath, planSHA256)
	if err != nil {
		return fail(AcquisitionFailed, "acquisition_state", err.Error(), "")
	}
	if completed != nil {
		if completed.CandidateReleaseID != plan.CandidateReleaseID || completed.RunID != runID {
			return fail(AcquisitionFailed, "acquisition_state", "completed state identity does not match the requested acquisition", "")
		}
		if issues := ValidateAcquisitionResult(inventory, *completed); len(issues) != 0 {
			return fail(AcquisitionFailed, "acquisition_state", issues[0].Message, "")
		}
		if err := validateAcquisitionArtifactFiles(options.StorageRoot, *completed); err != nil {
			return fail(AcquisitionFailed, "acquisition_state", err.Error(), "")
		}
		return *completed, nil
	}

	selectedByID := make(map[string]SelectedSource, len(plan.SelectedSources))
	for _, selected := range plan.SelectedSources {
		selectedByID[selected.SourceID] = selected
	}
	currentByID, err := validateReusableArtifacts(options.StorageRoot, inventory, plan, current)
	if err != nil {
		return fail(AcquisitionFailed, "reused_artifact_invalid", err.Error(), "")
	}

	scheduleRoot := filepath.Join(options.StorageRoot, "raw", "fec", "schedule-a")
	if err := os.MkdirAll(scheduleRoot, 0o750); err != nil {
		return fail(AcquisitionFailed, "storage_prepare", err.Error(), "")
	}
	diskUsage := options.DiskUsage
	if diskUsage == nil {
		diskUsage = defaultDiskUsage
	}
	storage, storageIssues, err := preflightStorage(options.StorageRoot, scheduleRoot, plan, selectedByID, diskUsage)
	result.Storage = storage
	if err != nil {
		return fail(AcquisitionFailed, "storage_preflight", err.Error(), "")
	}
	if len(storageIssues) != 0 {
		result.Status = AcquisitionBlocked
		result.CompletedAt = clock().UTC()
		result.Issues = append(result.Issues, storageIssues...)
		return result, fmt.Errorf("storage preflight blocked acquisition: %s", storageIssues[0].Message)
	}

	// Reject a known over-budget complete prior-size scenario before GETs. It
	// remains an estimate: runtime writers enforce actual growth, including seeds
	// with no prior output sizes and outputs larger than the previous snapshot.
	scenario, err := stagingStorageScenario(storage, slices.Contains(plan.ChangedSourceIDs, ScheduleASourceID), estimateStageOutputs(inventory, plan, current))
	if err != nil {
		return fail(AcquisitionFailed, "staging_projection", err.Error(), "")
	}
	if scenario.Complete && !scenario.FitsBudget {
		return fail(AcquisitionBlocked, "staging_projection", "prior-size staged output scenario exceeds storage budget", "")
	}
	var limits StageStorage
	configureStageStorage(&limits, StageOptions{})
	budget, err := newWriteBudget(ctx, options.StorageRoot, limits, diskUsage)
	if err != nil {
		status := AcquisitionFailed
		if errors.Is(err, errStorageBudget) {
			status = AcquisitionBlocked
		}
		return fail(status, "acquisition_write_budget", err.Error(), "")
	}
	captured, err := captureChangedSources(ctx, client, plan, selectedByID, options, clock, budget)
	if err != nil {
		if errors.Is(err, errStorageBudget) {
			return fail(AcquisitionBlocked, "acquisition_write_budget", err.Error(), "")
		}
		return fail(AcquisitionFailed, "source_capture", err.Error(), "")
	}

	postStartedAt := clock().UTC()
	result.PostCapture.StartedAt = &postStartedAt
	postDiscovery, err := Discover(ctx, client, inventory, clock, DiscoverOptions{MaxConcurrent: acquisitionConcurrency(options.MaxConcurrent, len(inventory.Sources))})
	if err != nil {
		return fail(AcquisitionFailed, "post_capture_discovery", err.Error(), "")
	}
	postCompletedAt := postDiscovery.CompletedAt
	result.PostCapture.CompletedAt = &postCompletedAt
	postIssues := comparePostCapture(plan, postDiscovery, &result.PostCapture)
	if len(postIssues) != 0 {
		result.Issues = append(result.Issues, postIssues...)
		result.CompletedAt = clock().UTC()
		return result, fmt.Errorf("publisher metadata changed during capture")
	}

	containerValidator := options.ContainerValidator
	if containerValidator == nil {
		containerValidator = validateContainer
	}
	containerChecks := make(map[string]string, len(captured))
	for _, source := range inventory.Sources {
		download, changed := captured[source.SourceID]
		if !changed {
			continue
		}
		if options.Progress != nil {
			options.Progress("validating container " + source.SourceID)
		}
		containerCheck, err := containerValidator(ctx, source, download.path, options.PGRestorePath)
		if err != nil {
			return fail(AcquisitionFailed, "container_validation", err.Error(), source.SourceID)
		}
		containerChecks[source.SourceID] = containerCheck
	}

	newArtifacts := make(map[string]AcquisitionArtifact, len(captured))
	for _, source := range inventory.Sources {
		download, changed := captured[source.SourceID]
		if !changed {
			continue
		}
		storageKey := contentStorageKey(source.SourceID, download.digest)
		destination, err := resolveStorageKey(options.StorageRoot, storageKey)
		if err != nil {
			return fail(AcquisitionFailed, "artifact_storage_key", err.Error(), source.SourceID)
		}
		if err := finalizeArtifact(ctx, download.path, destination, download.byteCount, download.digest); err != nil {
			return fail(AcquisitionFailed, "artifact_finalize", err.Error(), source.SourceID)
		}
		newArtifacts[source.SourceID] = AcquisitionArtifact{
			SourceID:        source.SourceID,
			Disposition:     "acquired",
			VersionIdentity: selectedByID[source.SourceID].VersionIdentity,
			ByteCount:       download.byteCount,
			SHA256:          download.digest,
			StorageKey:      storageKey,
			AcquiredAt:      download.acquiredAt,
			ContainerCheck:  containerChecks[source.SourceID],
		}
	}

	for _, source := range inventory.Sources {
		if artifact, exists := newArtifacts[source.SourceID]; exists {
			result.Artifacts = append(result.Artifacts, artifact)
			continue
		}
		prior := currentByID[source.SourceID]
		result.Artifacts = append(result.Artifacts, AcquisitionArtifact{
			SourceID:        source.SourceID,
			Disposition:     "reused",
			VersionIdentity: prior.VersionIdentity,
			ByteCount:       prior.ByteCount,
			SHA256:          prior.SHA256,
			StorageKey:      prior.StorageKey,
			AcquiredAt:      prior.AcquiredAt,
			ContainerCheck:  "reused_published_artifact",
		})
	}
	result.Status = AcquisitionAcquired
	result.CompletedAt = clock().UTC()
	if issues := ValidateAcquisitionResult(inventory, result); len(issues) != 0 {
		result.Status = AcquisitionFailed
		result.Issues = append(result.Issues, issues...)
		return result, fmt.Errorf("invalid acquisition result: %s", issues[0].Message)
	}
	if err := writeAcquisitionState(statePath, result); err != nil {
		return fail(AcquisitionFailed, "acquisition_state_write", err.Error(), "")
	}
	for _, download := range captured {
		if err := os.Remove(download.path); err != nil && !os.IsNotExist(err) && options.Progress != nil {
			options.Progress("warning: remove completed partial: " + err.Error())
		}
	}
	return result, nil
}

func acquisitionConcurrency(configured, sourceCount int) int {
	if configured <= 0 {
		configured = defaultAcquisitionConcurrency
	}
	if configured > sourceCount {
		return sourceCount
	}
	return configured
}

func preflightStorage(
	storageRoot string,
	scheduleRoot string,
	plan ReleasePlan,
	selectedByID map[string]SelectedSource,
	diskUsage func(string) (DiskSpace, error),
) (StoragePreflight, []Issue, error) {
	preflight := StoragePreflight{
		ScheduleAHotCapBytes:       ScheduleAHotCapBytes,
		FreeFloorBytes:             AcquisitionFreeFloorBytes,
		WorkingMarginBytes:         AcquisitionWorkingMarginBytes,
		LargestExtractWorkingBytes: 0,
	}
	if err := checkLegacyWorkspace(storageRoot); err != nil {
		return preflight, nil, err
	}
	hotBytes, err := directoryBytes(scheduleRoot)
	if err != nil {
		return preflight, nil, err
	}
	preflight.ScheduleAHotBytesBefore = hotBytes
	var remainingSchedule uint64
	for _, sourceID := range plan.ChangedSourceIDs {
		selected := selectedByID[sourceID]
		if selected.ContentLength == nil || *selected.ContentLength <= 0 {
			return preflight, []Issue{{SourceID: sourceID, Code: "content_length_required", Message: "changed source requires a positive selected content length"}}, nil
		}
		length := uint64(*selected.ContentLength)
		if ^uint64(0)-preflight.CandidateDownloadBytes < length {
			return preflight, nil, fmt.Errorf("candidate download byte count overflow")
		}
		preflight.CandidateDownloadBytes += length
		partialPath := stagingPath(storageRoot, plan.CandidateReleaseID, sourceID)
		info, statErr := os.Stat(partialPath)
		var partialBytes uint64
		if statErr == nil {
			if !info.Mode().IsRegular() || info.Size() < 0 {
				return preflight, nil, fmt.Errorf("partial for %s is not a regular file", sourceID)
			}
			partialBytes = uint64(info.Size())
			if partialBytes > length {
				partialBytes = 0
			}
		} else if !os.IsNotExist(statErr) {
			return preflight, nil, statErr
		}
		remaining := length - partialBytes
		if ^uint64(0)-preflight.RemainingDownloadBytes < remaining {
			return preflight, nil, fmt.Errorf("remaining download byte count overflow")
		}
		preflight.RemainingDownloadBytes += remaining
		if sourceID == ScheduleASourceID {
			remainingSchedule = remaining
		}
	}
	preflight.ProjectedScheduleAHotBytes, err = sumStorageBytes(hotBytes, remainingSchedule, AcquisitionWorkingMarginBytes)
	if err != nil {
		return preflight, nil, err
	}
	disk, err := diskUsage(storageRoot)
	if err != nil {
		return preflight, nil, err
	}
	preflight.FreeBytesBefore = disk.AvailableBytes
	issues := make([]Issue, 0, 2)
	if preflight.ProjectedScheduleAHotBytes > ScheduleAHotCapBytes {
		issues = append(issues, Issue{Code: "schedule_a_hot_cap", Message: "acquisition would exceed the Schedule A hot-lane cap"})
	}
	remainingRequired, err := sumStorageBytes(preflight.RemainingDownloadBytes, AcquisitionWorkingMarginBytes, AcquisitionFreeFloorBytes)
	if err != nil {
		return preflight, nil, err
	}
	if disk.AvailableBytes < remainingRequired {
		issues = append(issues, Issue{Code: "storage_free_floor", Message: "acquisition would leave less than the required free-space floor and working margin"})
	}
	preflight.Passed = len(issues) == 0
	return preflight, issues, nil
}

func validateReusableArtifacts(storageRoot string, inventory Inventory, plan ReleasePlan, current *ReleaseManifest) (map[string]PublishedArtifact, error) {
	result := make(map[string]PublishedArtifact)
	if current == nil {
		if plan.PriorReleaseID != "" || len(plan.ReusedSourceIDs) != 0 {
			return nil, fmt.Errorf("plan requires a prior published manifest")
		}
		return result, nil
	}
	if issues := validateKnownManifest(*current); len(issues) != 0 {
		return nil, fmt.Errorf("invalid prior manifest: %s", issues[0].Message)
	}
	if current.ReleaseID != plan.PriorReleaseID {
		return nil, fmt.Errorf("prior manifest release ID does not match the plan")
	}
	selectedByID := make(map[string]SelectedSource, len(plan.SelectedSources))
	for _, selected := range plan.SelectedSources {
		selectedByID[selected.SourceID] = selected
	}
	for _, artifact := range current.Artifacts {
		result[artifact.SourceID] = artifact
	}
	priorInventory, _ := InventoryForVersion(current.InventoryVersion)
	priorSpecByID := sourceSpecByID(priorInventory)
	targetSpecByID := sourceSpecByID(inventory)
	for _, sourceID := range plan.ReusedSourceIDs {
		artifact, exists := result[sourceID]
		if !exists || artifact.VersionIdentity != selectedByID[sourceID].VersionIdentity {
			return nil, fmt.Errorf("reused source %s does not match the selected version", sourceID)
		}
		priorSpec, priorExists := priorSpecByID[sourceID]
		if !priorExists || !sourceSpecsArtifactCompatible(targetSpecByID[sourceID], priorSpec) {
			return nil, fmt.Errorf("reused source %s is not artifact-compatible across inventories", sourceID)
		}
		path, err := resolveStorageKey(storageRoot, artifact.StorageKey)
		if err != nil {
			return nil, fmt.Errorf("reused source %s: %w", sourceID, err)
		}
		info, err := os.Stat(path)
		if err != nil {
			return nil, fmt.Errorf("reused source %s: %w", sourceID, err)
		}
		if !info.Mode().IsRegular() || info.Size() != artifact.ByteCount || filepath.Base(path) != artifact.SHA256 {
			return nil, fmt.Errorf("reused source %s does not match its immutable artifact metadata", sourceID)
		}
	}
	return result, nil
}

func captureChangedSources(
	ctx context.Context,
	client HTTPDoer,
	plan ReleasePlan,
	selectedByID map[string]SelectedSource,
	options AcquisitionOptions,
	clock func() time.Time,
	budget *writeBudget,
) (map[string]capturedDownload, error) {
	results := make(map[string]capturedDownload, len(plan.ChangedSourceIDs))
	maxConcurrent := acquisitionConcurrency(options.MaxConcurrent, len(plan.ChangedSourceIDs))
	indexes := make(chan string)
	var workers sync.WaitGroup
	var mutex sync.Mutex
	var firstErr error
	captureContext, cancel := context.WithCancel(ctx)
	defer cancel()
	for range maxConcurrent {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for sourceID := range indexes {
				if options.Progress != nil {
					options.Progress("capturing " + sourceID)
				}
				download, err := captureSource(captureContext, client, selectedByID[sourceID], stagingPath(options.StorageRoot, plan.CandidateReleaseID, sourceID), clock, budget)
				mutex.Lock()
				if err != nil && firstErr == nil {
					firstErr = fmt.Errorf("%s: %w", sourceID, err)
					cancel()
				} else if err == nil {
					results[sourceID] = download
				}
				mutex.Unlock()
			}
		}()
	}
sendLoop:
	for _, sourceID := range plan.ChangedSourceIDs {
		select {
		case indexes <- sourceID:
		case <-captureContext.Done():
			break sendLoop
		}
	}
	close(indexes)
	workers.Wait()
	return results, firstErr
}

func comparePostCapture(plan ReleasePlan, discovery Discovery, output *PostCaptureCheck) []Issue {
	selectedByID := make(map[string]SelectedSource, len(plan.SelectedSources))
	for _, selected := range plan.SelectedSources {
		selectedByID[selected.SourceID] = selected
	}
	issues := make([]Issue, 0)
	for _, observation := range discovery.Observations {
		version := PostCaptureVersion{
			SourceID:        observation.SourceID,
			ObservedAt:      observation.ObservedAt,
			Status:          observation.Status,
			VersionIdentity: observation.VersionIdentity,
			ProblemCode:     observation.ProblemCode,
			Problem:         observation.Problem,
		}
		output.Versions = append(output.Versions, version)
		selected := selectedByID[observation.SourceID]
		lengthMatches := (selected.ContentLength == nil && observation.ContentLength == nil) ||
			(selected.ContentLength != nil && observation.ContentLength != nil && *selected.ContentLength == *observation.ContentLength)
		if observation.Status != ObservationAvailable || observation.VersionIdentity != selected.VersionIdentity || !lengthMatches {
			issues = append(issues, Issue{SourceID: observation.SourceID, Code: "publisher_changed_during_capture", Message: "publisher metadata no longer matches the selected source version"})
		}
	}
	return issues
}

func finalizeArtifact(ctx context.Context, stagingPath, destination string, expectedBytes int64, expectedSHA256 string) error {
	if err := os.MkdirAll(filepath.Dir(destination), 0o750); err != nil {
		return err
	}
	if info, err := os.Stat(destination); err == nil {
		if !info.Mode().IsRegular() || info.Size() != expectedBytes {
			return fmt.Errorf("existing content-addressed artifact has unexpected shape")
		}
		digest, err := fileSHA256(ctx, destination)
		if err != nil {
			return err
		}
		if digest != expectedSHA256 {
			return fmt.Errorf("existing content-addressed artifact has an invalid SHA-256")
		}
		return nil
	} else if !os.IsNotExist(err) {
		return err
	}
	return os.Link(stagingPath, destination)
}

func validateAcquisitionArtifactFiles(storageRoot string, result AcquisitionResult) error {
	for _, artifact := range result.Artifacts {
		path, err := resolveStorageKey(storageRoot, artifact.StorageKey)
		if err != nil {
			return fmt.Errorf("artifact %s: %w", artifact.SourceID, err)
		}
		info, err := os.Stat(path)
		if err != nil {
			return fmt.Errorf("artifact %s: %w", artifact.SourceID, err)
		}
		if !info.Mode().IsRegular() || info.Size() != artifact.ByteCount || filepath.Base(path) != artifact.SHA256 {
			return fmt.Errorf("artifact %s does not match completed acquisition state", artifact.SourceID)
		}
	}
	return nil
}

// ValidateAcquisitionResult checks the durable success shape beyond JSON
// syntax. Failed and blocked results intentionally retain partial information.
func ValidateAcquisitionResult(inventory Inventory, result AcquisitionResult) []Issue {
	issues := make([]Issue, 0)
	if result.SchemaVersion != AcquisitionSchemaVersion {
		issues = append(issues, Issue{Code: "acquisition_schema_version", Message: "unexpected acquisition schema version"})
	}
	if result.InventoryVersion != inventory.InventoryVersion || !validCandidateReleaseID(result.CandidateReleaseID) || !validSHA256(result.PlanSHA256) || !ValidAcquisitionRunID(result.RunID) {
		issues = append(issues, Issue{Code: "acquisition_identity", Message: "acquisition inventory, plan hash, or run identity is invalid"})
	}
	if result.StartedAt.IsZero() || result.CompletedAt.IsZero() || result.CompletedAt.Before(result.StartedAt) {
		issues = append(issues, Issue{Code: "acquisition_window", Message: "acquisition requires an ordered execution window"})
	}
	if result.Status == AcquisitionAcquired {
		if !result.Storage.Passed || len(result.Artifacts) != len(inventory.Sources) || len(result.PostCapture.Versions) != len(inventory.Sources) || len(result.Issues) != 0 || result.PostCapture.StartedAt == nil || result.PostCapture.CompletedAt == nil {
			issues = append(issues, Issue{Code: "acquisition_success_shape", Message: "acquired result requires passed storage, complete artifacts and post-capture versions, and no issues"})
		}
		seen := make(map[string]struct{}, len(result.Artifacts))
		known := make(map[string]struct{}, len(inventory.Sources))
		for _, source := range inventory.Sources {
			known[source.SourceID] = struct{}{}
		}
		for _, artifact := range result.Artifacts {
			_, sourceExists := known[artifact.SourceID]
			_, duplicate := seen[artifact.SourceID]
			if !sourceExists || duplicate || artifact.VersionIdentity == "" || artifact.ByteCount < 0 || !validSHA256(artifact.SHA256) || artifact.StorageKey == "" || artifact.AcquiredAt.IsZero() || artifact.ContainerCheck == "" || (artifact.Disposition != "acquired" && artifact.Disposition != "reused") {
				issues = append(issues, Issue{SourceID: artifact.SourceID, Code: "acquisition_artifact", Message: "acquisition artifact is invalid or duplicated"})
			}
			seen[artifact.SourceID] = struct{}{}
		}
		postSeen := make(map[string]struct{}, len(result.PostCapture.Versions))
		for _, version := range result.PostCapture.Versions {
			_, knownSource := known[version.SourceID]
			_, duplicate := postSeen[version.SourceID]
			if !knownSource || duplicate || version.ObservedAt.IsZero() || version.Status != ObservationAvailable || version.VersionIdentity == "" {
				issues = append(issues, Issue{SourceID: version.SourceID, Code: "acquisition_post_capture", Message: "post-capture version is invalid or duplicated"})
			}
			postSeen[version.SourceID] = struct{}{}
		}
	} else if result.Status != AcquisitionBlocked && result.Status != AcquisitionFailed {
		issues = append(issues, Issue{Code: "acquisition_status", Message: "unknown acquisition status"})
	}
	return issues
}

func validCandidateReleaseID(value string) bool {
	return ValidReleaseID(value)
}

// ValidReleaseID checks the coordinated FEC release identifier, which is a
// namespaced identity rather than the manifest's bare SHA256 checksum.
func ValidReleaseID(value string) bool {
	return len(value) == len("fec-")+64 && value[:len("fec-")] == "fec-" && validSHA256(value[len("fec-"):])
}
