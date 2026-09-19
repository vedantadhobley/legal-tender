package release

import (
	"fmt"
	"net/url"
	"slices"
	"strings"
)

// ValidatePlan checks both the common wire shape and the status-specific
// acquisition safety invariants of a planner result.
func ValidatePlan(inventory Inventory, plan ReleasePlan) []Issue {
	issues := make([]Issue, 0)
	if plan.SchemaVersion != PlanSchemaVersion {
		issues = append(issues, Issue{Code: "plan_schema_version", Message: "unexpected release-plan schema version"})
	}
	if plan.InventoryVersion != inventory.InventoryVersion {
		issues = append(issues, Issue{Code: "plan_inventory_version", Message: "release-plan inventory version does not match the inventory"})
	}
	if plan.PlannedAt.IsZero() {
		issues = append(issues, Issue{Code: "plan_planned_at", Message: "planner time is required"})
	}
	known := make(map[string]SourceSpec, len(inventory.Sources))
	for _, source := range inventory.Sources {
		known[source.SourceID] = source
	}
	selectedIDs := make(map[string]struct{}, len(plan.SelectedSources))
	for _, selected := range plan.SelectedSources {
		spec, exists := known[selected.SourceID]
		if !exists {
			issues = append(issues, Issue{SourceID: selected.SourceID, Code: "plan_unknown_source", Message: "selected source is not an inventory member"})
			continue
		}
		if _, duplicate := selectedIDs[selected.SourceID]; duplicate {
			issues = append(issues, Issue{SourceID: selected.SourceID, Code: "plan_duplicate_source", Message: "source is selected more than once"})
			continue
		}
		selectedIDs[selected.SourceID] = struct{}{}
		issues = append(issues, validateSelectedSource(spec, selected)...)
		if plan.DiscoveryStartedAt != nil && plan.DiscoveryCompletedAt != nil && !selected.ObservedAt.IsZero() &&
			(selected.ObservedAt.Before(*plan.DiscoveryStartedAt) || selected.ObservedAt.After(*plan.DiscoveryCompletedAt)) {
			issues = append(issues, Issue{SourceID: selected.SourceID, Code: "plan_source_time_outside_window", Message: "selected source observation time is outside the discovery window"})
		}
	}
	changed, changedIssues := validateSourceIDs("changed", plan.ChangedSourceIDs, known)
	issues = append(issues, changedIssues...)
	reused, reusedIssues := validateSourceIDs("reused", plan.ReusedSourceIDs, known)
	issues = append(issues, reusedIssues...)
	for sourceID := range changed {
		if _, exists := reused[sourceID]; exists {
			issues = append(issues, Issue{SourceID: sourceID, Code: "plan_source_overlap", Message: "source cannot be both changed and reused"})
		}
	}
	for _, planIssue := range plan.Issues {
		if planIssue.Code == "" || planIssue.Message == "" {
			issues = append(issues, Issue{SourceID: planIssue.SourceID, Code: "plan_issue_shape", Message: "planner issue code and message are required"})
		}
	}

	switch plan.Status {
	case PlanUpdateAvailable:
		if len(plan.SelectedSources) != len(inventory.Sources) || len(plan.ChangedSourceIDs) == 0 {
			issues = append(issues, Issue{Code: "plan_update_membership", Message: "update_available must select every source and identify at least one change"})
		}
		if len(plan.Issues) != 0 {
			issues = append(issues, Issue{Code: "plan_update_issues", Message: "update_available cannot contain blocking issues"})
		}
		if !validPlanDiscoveryWindow(plan) {
			issues = append(issues, Issue{Code: "plan_update_discovery_window", Message: "update_available requires an ordered discovery window"})
		}
		if len(changed)+len(reused) != len(inventory.Sources) {
			issues = append(issues, Issue{Code: "plan_update_classification", Message: "update_available must classify every selected source as changed or reused"})
		}
		if len(reused) != 0 && plan.PriorReleaseID == "" {
			issues = append(issues, Issue{Code: "plan_reused_without_prior", Message: "update_available cannot reuse sources without a prior release"})
		}
		for sourceID := range selectedIDs {
			_, isChanged := changed[sourceID]
			_, isReused := reused[sourceID]
			if !isChanged && !isReused {
				issues = append(issues, Issue{SourceID: sourceID, Code: "plan_unclassified_source", Message: "selected source is neither changed nor reused"})
			}
		}
		if plan.CandidateReleaseID != releaseID(inventory.InventoryVersion, plan.SelectedSources) {
			issues = append(issues, Issue{Code: "plan_candidate_release_id", Message: "candidate release ID does not match the canonical selected versions"})
		}
	case PlanNoChange:
		if len(plan.SelectedSources) != len(inventory.Sources) || len(plan.ReusedSourceIDs) != len(inventory.Sources) || len(plan.ChangedSourceIDs) != 0 {
			issues = append(issues, Issue{Code: "plan_no_change_membership", Message: "no_change must select and reuse every source with no changes"})
		}
		if plan.PriorReleaseID == "" || plan.CandidateReleaseID != "" || len(plan.Issues) != 0 {
			issues = append(issues, Issue{Code: "plan_no_change_state", Message: "no_change requires a prior release and cannot contain a candidate ID or issues"})
		}
		if !validPlanDiscoveryWindow(plan) {
			issues = append(issues, Issue{Code: "plan_no_change_discovery_window", Message: "no_change requires an ordered discovery window"})
		}
	case PlanSourceNotReady, PlanInvalid:
		if len(plan.Issues) == 0 {
			issues = append(issues, Issue{Code: "plan_blocking_issues", Message: "blocked plan must contain at least one issue"})
		}
		if len(plan.SelectedSources) != 0 || len(plan.ChangedSourceIDs) != 0 || len(plan.ReusedSourceIDs) != 0 || plan.CandidateReleaseID != "" {
			issues = append(issues, Issue{Code: "plan_blocked_selection", Message: "blocked plan cannot authorize or classify acquisition"})
		}
		if plan.Status == PlanSourceNotReady && !validPlanDiscoveryWindow(plan) {
			issues = append(issues, Issue{Code: "plan_not_ready_discovery_window", Message: "source_not_ready requires an ordered discovery window"})
		}
	default:
		issues = append(issues, Issue{Code: "plan_status", Message: "unknown release-plan status"})
	}
	return issues
}

func validPlanDiscoveryWindow(plan ReleasePlan) bool {
	return plan.DiscoveryStartedAt != nil &&
		plan.DiscoveryCompletedAt != nil &&
		!plan.DiscoveryStartedAt.IsZero() &&
		!plan.DiscoveryCompletedAt.IsZero() &&
		!plan.DiscoveryCompletedAt.Before(*plan.DiscoveryStartedAt)
}

func validateSelectedSource(spec SourceSpec, selected SelectedSource) []Issue {
	issues := make([]Issue, 0)
	add := func(code, message string) {
		issues = append(issues, Issue{SourceID: selected.SourceID, Code: code, Message: message})
	}
	if selected.RequestURL != spec.RequestURL {
		add("plan_request_url", "selected request URL does not match the inventory")
	}
	if !validHTTPSURL(selected.FinalURL) {
		add("plan_final_url", "selected source must have an HTTPS final URL")
	}
	if selected.ObservedAt.IsZero() {
		add("plan_source_observed_at", "selected source observation time is required")
	}
	if selected.ContentLength != nil && *selected.ContentLength < 0 {
		add("plan_content_length", "selected content length cannot be negative")
	}
	basis, identity := deriveVersionIdentity(
		selected.VersionID,
		selected.ETag,
		selected.Digest,
		selected.LastModified,
		selected.ContentLength,
	)
	if identity == "" || selected.VersionBasis != basis || selected.VersionIdentity != identity {
		add("plan_version_identity", "selected version identity does not match its publisher metadata")
	}
	return issues
}

func validateSourceIDs(label string, sourceIDs []string, known map[string]SourceSpec) (map[string]struct{}, []Issue) {
	seen := make(map[string]struct{}, len(sourceIDs))
	issues := make([]Issue, 0)
	for _, sourceID := range sourceIDs {
		if _, exists := known[sourceID]; !exists {
			issues = append(issues, Issue{SourceID: sourceID, Code: "plan_unknown_" + label + "_source", Message: label + " source is not an inventory member"})
		}
		if _, duplicate := seen[sourceID]; duplicate {
			issues = append(issues, Issue{SourceID: sourceID, Code: "plan_duplicate_" + label + "_source", Message: label + " source occurs more than once"})
		}
		seen[sourceID] = struct{}{}
	}
	return seen, issues
}

func validateDiscovery(inventory Inventory, discovery Discovery) []Issue {
	issues := make([]Issue, 0)
	if discovery.SchemaVersion != DiscoverySchemaVersion {
		issues = append(issues, Issue{Code: "discovery_schema_version", Message: "unexpected discovery schema version"})
	}
	if discovery.InventoryVersion != inventory.InventoryVersion {
		issues = append(issues, Issue{Code: "discovery_inventory_version", Message: "discovery inventory version does not match the planner inventory"})
	}
	if discovery.StartedAt.IsZero() || discovery.CompletedAt.IsZero() {
		issues = append(issues, Issue{Code: "discovery_window", Message: "discovery start and completion times are required"})
	} else if discovery.CompletedAt.Before(discovery.StartedAt) {
		issues = append(issues, Issue{Code: "discovery_window_order", Message: "discovery completion time precedes its start"})
	}
	if len(discovery.Observations) != len(inventory.Sources) {
		issues = append(issues, Issue{Code: "discovery_source_count", Message: fmt.Sprintf("discovery has %d observations; want %d", len(discovery.Observations), len(inventory.Sources))})
	}

	specByID := make(map[string]SourceSpec, len(inventory.Sources))
	for _, source := range inventory.Sources {
		specByID[source.SourceID] = source
	}
	seen := make(map[string]struct{}, len(discovery.Observations))
	for _, observation := range discovery.Observations {
		spec, known := specByID[observation.SourceID]
		if !known {
			issues = append(issues, Issue{SourceID: observation.SourceID, Code: "discovery_unknown_source", Message: "observation is not a member of the release inventory"})
			continue
		}
		if _, exists := seen[observation.SourceID]; exists {
			issues = append(issues, Issue{SourceID: observation.SourceID, Code: "discovery_duplicate_source", Message: "source was observed more than once"})
			continue
		}
		seen[observation.SourceID] = struct{}{}
		issues = append(issues, validateObservation(spec, observation)...)
		if !observation.ObservedAt.IsZero() &&
			(observation.ObservedAt.Before(discovery.StartedAt) || observation.ObservedAt.After(discovery.CompletedAt)) {
			issues = append(issues, Issue{SourceID: observation.SourceID, Code: "discovery_source_time_outside_window", Message: "source observation time is outside the discovery window"})
		}
	}
	for _, source := range inventory.Sources {
		if _, exists := seen[source.SourceID]; !exists {
			issues = append(issues, Issue{SourceID: source.SourceID, Code: "discovery_missing_source", Message: "required source has no observation"})
		}
	}
	return issues
}

func validateObservation(spec SourceSpec, observation Observation) []Issue {
	issues := make([]Issue, 0)
	add := func(code, message string) {
		issues = append(issues, Issue{SourceID: observation.SourceID, Code: code, Message: message})
	}
	if observation.RequestMethod != "HEAD" {
		add("discovery_request_method", "metadata discovery must use HEAD")
	}
	if observation.RequestURL != spec.RequestURL {
		add("discovery_request_url", "observation request URL does not match the inventory")
	}
	if observation.ObservedAt.IsZero() {
		add("discovery_source_observed_at", "source observation time is required")
	}
	if observation.ContentLength != nil && *observation.ContentLength < 0 {
		add("discovery_content_length", "content length cannot be negative")
	}
	switch observation.Status {
	case ObservationAvailable:
		if observation.HTTPStatus < 200 || observation.HTTPStatus >= 300 {
			add("discovery_http_status", "available source must have a successful HTTP status")
		}
		if !validHTTPSURL(observation.FinalURL) {
			add("discovery_final_url", "available source must have an HTTPS final URL")
		}
		basis, identity := deriveVersionIdentity(
			observation.VersionID,
			observation.ETag,
			observation.Digest,
			observation.LastModified,
			observation.ContentLength,
		)
		if identity == "" {
			add("discovery_version_identity_missing", "available source has no usable publisher version metadata")
		} else if observation.VersionBasis != basis || observation.VersionIdentity != identity {
			add("discovery_version_identity_mismatch", "stored version identity does not match the observed metadata")
		}
	case ObservationUnavailable:
		if observation.ProblemCode == "" || observation.Problem == "" {
			add("discovery_problem", "unavailable source must include a problem code and message")
		}
	default:
		add("discovery_status", "source status must be available or unavailable")
	}
	return issues
}

func validateManifest(inventory Inventory, manifest ReleaseManifest) []Issue {
	issues := make([]Issue, 0)
	if manifest.Schema != "release-manifest.schema.json" {
		issues = append(issues, Issue{Code: "manifest_schema", Message: "release manifest must reference release-manifest.schema.json"})
	}
	if manifest.SchemaVersion != ManifestSchemaVersion {
		issues = append(issues, Issue{Code: "manifest_schema_version", Message: "unexpected release manifest schema version"})
	}
	if manifest.InventoryVersion != inventory.InventoryVersion {
		issues = append(issues, Issue{Code: "manifest_inventory_version", Message: "release manifest inventory version does not match the planner inventory"})
	}
	if manifest.ReleaseID == "" {
		issues = append(issues, Issue{Code: "manifest_release_id", Message: "release ID is required"})
	}
	if !ValidAcquisitionRunID(manifest.RunID) {
		issues = append(issues, Issue{Code: "manifest_run_id", Message: "release run ID is invalid"})
	}
	if !validSHA256(manifest.PlanSHA256) || !validSHA256(manifest.AcquisitionSHA256) || !validSHA256(manifest.StageSHA256) {
		issues = append(issues, Issue{Code: "manifest_evidence_hashes", Message: "release evidence SHA-256 values are invalid"})
	}
	if manifest.State != "published" {
		issues = append(issues, Issue{Code: "manifest_state", Message: "planner baseline must be a published release"})
	}
	if manifest.SelectedAt.IsZero() || manifest.PublishedAt.IsZero() {
		issues = append(issues, Issue{Code: "manifest_timestamps", Message: "selected and published times are required"})
	}
	if !slices.Equal(manifest.Periods, inventory.Periods) {
		issues = append(issues, Issue{Code: "manifest_periods", Message: "release periods do not match the inventory"})
	}
	if len(manifest.Artifacts) != len(inventory.Sources) {
		issues = append(issues, Issue{Code: "manifest_source_count", Message: fmt.Sprintf("manifest has %d artifacts; want %d", len(manifest.Artifacts), len(inventory.Sources))})
	}

	specByID := make(map[string]SourceSpec, len(inventory.Sources))
	for _, source := range inventory.Sources {
		specByID[source.SourceID] = source
	}
	seen := make(map[string]struct{}, len(manifest.Artifacts))
	for _, artifact := range manifest.Artifacts {
		spec, known := specByID[artifact.SourceID]
		if !known {
			issues = append(issues, Issue{SourceID: artifact.SourceID, Code: "manifest_unknown_source", Message: "artifact is not a member of the release inventory"})
			continue
		}
		if _, exists := seen[artifact.SourceID]; exists {
			issues = append(issues, Issue{SourceID: artifact.SourceID, Code: "manifest_duplicate_source", Message: "source occurs more than once in the release manifest"})
			continue
		}
		seen[artifact.SourceID] = struct{}{}
		issues = append(issues, validatePublishedArtifact(spec, artifact)...)
	}
	for _, source := range inventory.Sources {
		if _, exists := seen[source.SourceID]; !exists {
			issues = append(issues, Issue{SourceID: source.SourceID, Code: "manifest_missing_source", Message: "required source has no published artifact"})
		}
	}
	stagedView := StageResult{
		SchemaVersion:      StageSchemaVersion,
		InventoryVersion:   manifest.InventoryVersion,
		CandidateReleaseID: manifest.ReleaseID,
		PriorReleaseID:     manifest.PriorReleaseID,
		PlanSHA256:         manifest.PlanSHA256,
		AcquisitionSHA256:  manifest.AcquisitionSHA256,
		RunID:              manifest.RunID,
		Status:             StageStaged,
		StartedAt:          manifest.SelectedAt,
		CompletedAt:        manifest.PublishedAt,
		Storage:            StageStorage{Passed: true},
		Outputs:            manifest.StagedOutputs,
		Checks:             manifest.Checks,
		Issues:             []Issue{},
	}
	for _, stageIssue := range ValidateStageResult(inventory, stagedView) {
		issues = append(issues, Issue{SourceID: stageIssue.SourceID, Code: "manifest_" + stageIssue.Code, Message: stageIssue.Message})
	}
	if len(manifest.Checks) < 5 {
		issues = append(issues, Issue{Code: "manifest_checks", Message: "published release requires at least five checks"})
	}
	return issues
}

// ValidateManifest checks a published coordinated release before a downstream
// evidence layer consumes its immutable artifacts.
func ValidateManifest(inventory Inventory, manifest ReleaseManifest) []Issue {
	return validateManifest(inventory, manifest)
}

// ValidateKnownManifest resolves and validates the manifest's committed
// inventory version. Downstream evidence publishers use it across migrations.
func ValidateKnownManifest(manifest ReleaseManifest) []Issue {
	return validateKnownManifest(manifest)
}

func validateKnownManifest(manifest ReleaseManifest) []Issue {
	inventory, known := InventoryForVersion(manifest.InventoryVersion)
	if !known {
		return []Issue{{Code: "manifest_inventory_version", Message: "release manifest uses an unknown inventory version"}}
	}
	return validateManifest(inventory, manifest)
}

func validatePublishedArtifact(spec SourceSpec, artifact PublishedArtifact) []Issue {
	issues := make([]Issue, 0)
	add := func(code, message string) {
		issues = append(issues, Issue{SourceID: artifact.SourceID, Code: code, Message: message})
	}
	if artifact.RequestURL != spec.RequestURL {
		add("manifest_request_url", "artifact request URL does not match the inventory")
	}
	if !validHTTPSURL(artifact.FinalURL) {
		add("manifest_final_url", "artifact must have an HTTPS final URL")
	}
	if artifact.ObservedAt.IsZero() || artifact.AcquiredAt.IsZero() {
		add("manifest_artifact_timestamps", "artifact observation and acquisition times are required")
	}
	if artifact.ContentLength != nil && *artifact.ContentLength < 0 {
		add("manifest_content_length", "artifact content length cannot be negative")
	}
	basis, identity := deriveVersionIdentity(
		artifact.VersionID,
		artifact.ETag,
		artifact.Digest,
		artifact.LastModified,
		artifact.ContentLength,
	)
	if identity == "" || artifact.VersionBasis != basis || artifact.VersionIdentity != identity {
		add("manifest_version_identity", "artifact version identity does not match its publisher metadata")
	}
	if artifact.ByteCount < 0 {
		add("manifest_byte_count", "artifact byte count cannot be negative")
	}
	if artifact.ContentLength != nil && artifact.ByteCount != *artifact.ContentLength {
		add("manifest_content_length_mismatch", "acquired byte count does not match selected content length")
	}
	if !validSHA256(artifact.SHA256) {
		add("manifest_sha256", "artifact SHA-256 must be 64 lowercase hexadecimal characters")
	}
	if artifact.StorageKey == "" {
		add("manifest_storage_key", "artifact storage key is required")
	}
	return issues
}

func validHTTPSURL(value string) bool {
	parsed, err := url.Parse(value)
	return err == nil && parsed.Scheme == "https" && parsed.Host != ""
}

func validSHA256(value string) bool {
	if len(value) != 64 {
		return false
	}
	for _, character := range value {
		if !strings.ContainsRune("0123456789abcdef", character) {
			return false
		}
	}
	return true
}
