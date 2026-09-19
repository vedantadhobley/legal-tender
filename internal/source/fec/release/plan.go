package release

import (
	"crypto/sha256"
	"encoding/hex"
	"hash"
	"slices"
	"sort"
	"time"
)

// Plan selects exact source versions without network or filesystem access.
func Plan(inventory Inventory, discovery Discovery, current *ReleaseManifest, plannedAt time.Time) ReleasePlan {
	plan := ReleasePlan{
		SchemaVersion:    PlanSchemaVersion,
		InventoryVersion: inventory.InventoryVersion,
		Status:           PlanInvalid,
		PlannedAt:        plannedAt.UTC(),
		SelectedSources:  []SelectedSource{},
		ChangedSourceIDs: []string{},
		ReusedSourceIDs:  []string{},
		Issues:           []Issue{},
	}
	if !discovery.StartedAt.IsZero() {
		startedAt := discovery.StartedAt.UTC()
		plan.DiscoveryStartedAt = &startedAt
	}
	if !discovery.CompletedAt.IsZero() {
		completedAt := discovery.CompletedAt.UTC()
		plan.DiscoveryCompletedAt = &completedAt
	}
	if current != nil {
		plan.PriorReleaseID = current.ReleaseID
	}

	plan.Issues = append(plan.Issues, ValidateInventory(inventory)...)
	plan.Issues = append(plan.Issues, validateDiscovery(inventory, discovery)...)
	if current != nil {
		plan.Issues = append(plan.Issues, validateKnownManifest(*current)...)
	}
	if plan.PlannedAt.IsZero() {
		plan.Issues = append(plan.Issues, Issue{Code: "planned_at", Message: "planner time is required"})
	}
	if len(plan.Issues) != 0 {
		return plan
	}

	observationByID := make(map[string]Observation, len(discovery.Observations))
	for _, observation := range discovery.Observations {
		observationByID[observation.SourceID] = observation
	}
	for _, source := range inventory.Sources {
		observation := observationByID[source.SourceID]
		if observation.Status != ObservationAvailable {
			plan.Issues = append(plan.Issues, Issue{
				SourceID: source.SourceID,
				Code:     "source_not_ready",
				Message:  observation.ProblemCode + ": " + observation.Problem,
			})
		}
	}
	if len(plan.Issues) != 0 {
		plan.Status = PlanSourceNotReady
		return plan
	}

	for _, source := range inventory.Sources {
		plan.SelectedSources = append(plan.SelectedSources, selectObservation(observationByID[source.SourceID]))
	}
	if current == nil {
		for _, source := range inventory.Sources {
			plan.ChangedSourceIDs = append(plan.ChangedSourceIDs, source.SourceID)
		}
		plan.Status = PlanUpdateAvailable
		plan.CandidateReleaseID = releaseID(inventory.InventoryVersion, plan.SelectedSources)
		return plan
	}

	currentByID := make(map[string]PublishedArtifact, len(current.Artifacts))
	for _, artifact := range current.Artifacts {
		currentByID[artifact.SourceID] = artifact
	}
	priorInventory, _ := InventoryForVersion(current.InventoryVersion)
	priorSpecByID := sourceSpecByID(priorInventory)
	targetSpecByID := sourceSpecByID(inventory)
	for _, selected := range plan.SelectedSources {
		priorArtifact, artifactExists := currentByID[selected.SourceID]
		priorSpec, specExists := priorSpecByID[selected.SourceID]
		if artifactExists && specExists &&
			priorArtifact.VersionIdentity == selected.VersionIdentity &&
			sourceSpecsArtifactCompatible(targetSpecByID[selected.SourceID], priorSpec) {
			plan.ReusedSourceIDs = append(plan.ReusedSourceIDs, selected.SourceID)
		} else {
			plan.ChangedSourceIDs = append(plan.ChangedSourceIDs, selected.SourceID)
		}
	}
	if len(plan.ChangedSourceIDs) == 0 {
		plan.Status = PlanNoChange
		return plan
	}
	plan.Status = PlanUpdateAvailable
	plan.CandidateReleaseID = releaseID(inventory.InventoryVersion, plan.SelectedSources)
	return plan
}

func selectObservation(observation Observation) SelectedSource {
	return SelectedSource{
		SourceID:        observation.SourceID,
		RequestURL:      observation.RequestURL,
		FinalURL:        observation.FinalURL,
		ObservedAt:      observation.ObservedAt,
		VersionIdentity: observation.VersionIdentity,
		VersionBasis:    observation.VersionBasis,
		VersionID:       observation.VersionID,
		ETag:            observation.ETag,
		LastModified:    observation.LastModified,
		ContentLength:   observation.ContentLength,
		Digest:          observation.Digest,
		AcceptRanges:    observation.AcceptRanges,
	}
}

func releaseID(inventoryVersion string, sources []SelectedSource) string {
	digest := sha256.New()
	writeIdentityPart(digest, inventoryVersion)
	ordered := slices.Clone(sources)
	sort.Slice(ordered, func(left, right int) bool {
		return ordered[left].SourceID < ordered[right].SourceID
	})
	for _, source := range ordered {
		writeIdentityPart(digest, source.SourceID)
		writeIdentityPart(digest, source.VersionIdentity)
	}
	return "fec-" + hex.EncodeToString(digest.Sum(nil))
}

func writeIdentityPart(digest hash.Hash, value string) {
	_, _ = digest.Write([]byte(value))
	_, _ = digest.Write([]byte{0})
}
