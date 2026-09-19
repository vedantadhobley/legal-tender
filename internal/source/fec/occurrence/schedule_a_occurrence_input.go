package occurrence

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// scheduleAOccurrenceInput is the representation-independent occurrence
// boundary consumed by Schedule A projections. The legacy JSON ledger remains
// readable during migration; new publications use the compact manifest.
type scheduleAOccurrenceInput struct {
	OccurrenceSetID             string
	SourceReleaseID             string
	SourceReleaseManifestSHA256 string
	SourceArtifactSHA256        string
	StagedOutputSHA256          string
	Relation                    string
	Cycle                       string
	Counts                      Counts
	ManifestSHA256              string
	legacy                      *Manifest
	compact                     *ScheduleACompactManifest
}

func readScheduleAOccurrenceInput(ctx context.Context, storageRoot, path string) (scheduleAOccurrenceInput, error) {
	content, err := os.ReadFile(path)
	if err != nil {
		return scheduleAOccurrenceInput{}, err
	}
	var header struct {
		SchemaVersion string `json:"schema_version"`
	}
	if err := json.Unmarshal(content, &header); err != nil {
		return scheduleAOccurrenceInput{}, err
	}
	switch header.SchemaVersion {
	case ManifestSchemaVersion:
		manifest, _, err := readScheduleAManifestWithSHA256(path)
		if err != nil {
			return scheduleAOccurrenceInput{}, err
		}
		if err := validateManifest(manifest); err != nil {
			return scheduleAOccurrenceInput{}, fmt.Errorf("invalid Schedule A occurrence input: %w", err)
		}
		if err := validateManifestBacking(storageRoot, manifest); err != nil {
			return scheduleAOccurrenceInput{}, fmt.Errorf("validate Schedule A occurrence input: %w", err)
		}
		immutablePath := filepath.Join(storageRoot, "evidence", "fec", "schedule-a", "manifests", manifest.OccurrenceSetID+".json")
		immutable, digest, err := readScheduleAManifestWithSHA256(immutablePath)
		if err != nil {
			return scheduleAOccurrenceInput{}, err
		}
		return scheduleAOccurrenceInput{
			OccurrenceSetID: manifest.OccurrenceSetID, SourceReleaseID: manifest.SourceReleaseID,
			SourceReleaseManifestSHA256: manifest.SourceReleaseManifestSHA256, SourceArtifactSHA256: manifest.SourceArtifactSHA256,
			StagedOutputSHA256: manifest.StagedOutputSHA256, Relation: manifest.Relation, Cycle: manifest.Cycle,
			Counts: manifest.Counts, ManifestSHA256: digest, legacy: &immutable,
		}, nil
	case ScheduleACompactManifestSchemaVersion:
		manifest, err := readScheduleACompactManifestIfPresent(path)
		if err != nil {
			return scheduleAOccurrenceInput{}, err
		}
		if manifest == nil {
			return scheduleAOccurrenceInput{}, fmt.Errorf("Schedule A compact occurrence manifest is absent")
		}
		if err := validateScheduleACompactManifest(*manifest); err != nil {
			return scheduleAOccurrenceInput{}, fmt.Errorf("invalid Schedule A compact occurrence input: %w", err)
		}
		if err := validateScheduleACompactManifestBacking(ctx, storageRoot, *manifest); err != nil {
			return scheduleAOccurrenceInput{}, fmt.Errorf("validate Schedule A compact occurrence input: %w", err)
		}
		immutablePath := filepath.Join(storageRoot, compactEvidenceBase, "manifests", manifest.OccurrenceSetID+".json")
		immutableContent, err := os.ReadFile(immutablePath)
		if err != nil {
			return scheduleAOccurrenceInput{}, err
		}
		immutable, err := readScheduleACompactManifestIfPresent(immutablePath)
		if err != nil || immutable == nil {
			if err == nil {
				err = fmt.Errorf("Schedule A compact immutable manifest is absent")
			}
			return scheduleAOccurrenceInput{}, err
		}
		digest := sha256.Sum256(immutableContent)
		return scheduleAOccurrenceInput{
			OccurrenceSetID: immutable.OccurrenceSetID, SourceReleaseID: immutable.SourceReleaseID,
			SourceReleaseManifestSHA256: immutable.SourceReleaseManifestSHA256, SourceArtifactSHA256: immutable.SourceArtifactSHA256,
			StagedOutputSHA256: immutable.StagedOutputSHA256, Relation: immutable.Relation, Cycle: immutable.Cycle,
			Counts: immutable.Counts, ManifestSHA256: hex.EncodeToString(digest[:]), compact: immutable,
		}, nil
	default:
		return scheduleAOccurrenceInput{}, fmt.Errorf("unsupported Schedule A occurrence manifest schema %q", header.SchemaVersion)
	}
}

func (input scheduleAOccurrenceInput) selectsAllRows() bool {
	return input.Counts.Total == input.Counts.UniqueKeys && input.Counts.Invalid == 0 &&
		input.Counts.InvalidKeys == 0 && input.Counts.DuplicateKeys == 0 && input.Counts.DuplicateOccurrences == 0
}

func loadScheduleASelectedRows(ctx context.Context, storageRoot string, input scheduleAOccurrenceInput) ([]byte, error) {
	if input.selectsAllRows() {
		return nil, nil
	}
	if input.legacy != nil {
		return loadScheduleAUniqueRows(ctx, storageRoot, *input.legacy)
	}
	if input.compact == nil {
		return nil, fmt.Errorf("Schedule A occurrence selection has no backing representation")
	}
	byteCount := (input.Counts.Total + 7) / 8
	if uint64(int(byteCount)) != byteCount {
		return nil, fmt.Errorf("Schedule A occurrence selection bitmap exceeds the platform range")
	}
	selected := make([]byte, int(byteCount))
	var count uint64
	for _, partition := range input.compact.IndexPartitions {
		states, err := readCompactIndexStates(ctx, storageRoot, partition.Index, partition.Partition, input.compact.Configuration.Partitions)
		if err != nil {
			return nil, err
		}
		for _, state := range states {
			if state.RowOrdinal == 0 || state.RowOrdinal > input.Counts.Total {
				return nil, fmt.Errorf("Schedule A compact index row ordinal %d is out of range", state.RowOrdinal)
			}
			index := state.RowOrdinal - 1
			mask := byte(1 << (index % 8))
			if selected[index/8]&mask != 0 {
				return nil, fmt.Errorf("Schedule A compact index selects row %d more than once", state.RowOrdinal)
			}
			selected[index/8] |= mask
			count++
		}
	}
	if count != input.Counts.UniqueKeys {
		return nil, fmt.Errorf("Schedule A compact index selected %d unique rows; want %d", count, input.Counts.UniqueKeys)
	}
	return selected, nil
}
