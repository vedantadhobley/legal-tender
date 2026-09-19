package occurrence

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"time"

	"github.com/klauspost/compress/zstd"
	fecrelease "github.com/vedantadhobley/legal-tender/internal/source/fec/release"
	"github.com/vedantadhobley/legal-tender/internal/source/fec/schedulee"
)

const (
	ScheduleEOccurrenceSetSchemaVersion = "legal-tender.fec.schedule-e-occurrence-set.v1"
	ScheduleEOccurrenceSchemaVersion    = "legal-tender.fec.schedule-e-occurrence.v1"
	ScheduleEOccurrenceParserVersion    = "legal-tender.fec.schedule-e-occurrence-parser.v1"
	ScheduleESourceContract             = "fec/schedule-e@1.0.0"
	scheduleEAllHistoryScope            = "all_history"
)

type ScheduleEOccurrenceManifest struct {
	Schema                      string                    `json:"$schema"`
	SchemaVersion               string                    `json:"schema_version"`
	OccurrenceSetID             string                    `json:"occurrence_set_id"`
	PriorOccurrenceSetID        string                    `json:"prior_occurrence_set_id,omitempty"`
	SourceReleaseID             string                    `json:"source_release_id"`
	SourceReleaseManifestSHA256 string                    `json:"source_release_manifest_sha256"`
	SourceArtifactSHA256        string                    `json:"source_artifact_sha256"`
	StagedOutputSHA256          string                    `json:"staged_output_sha256"`
	Relation                    string                    `json:"relation"`
	Cycle                       string                    `json:"cycle"`
	RunID                       string                    `json:"run_id"`
	State                       string                    `json:"state"`
	ParserVersion               string                    `json:"parser_version"`
	PublishedAt                 time.Time                 `json:"published_at"`
	Counts                      ScheduleEOccurrenceCounts `json:"counts"`
	Occurrences                 Artifact                  `json:"occurrences"`
	Checks                      []Check                   `json:"checks"`
}

type ScheduleEOccurrenceCounts struct {
	SourceRows     uint64 `json:"source_rows"`
	SelectedRows   uint64 `json:"selected_rows"`
	OtherCycleRows uint64 `json:"other_cycle_rows"`
	NullCycleRows  uint64 `json:"null_cycle_rows"`
}

type ScheduleEOccurrence struct {
	SchemaVersion            string `json:"schema_version"`
	OccurrenceID             string `json:"occurrence_id"`
	RowOrdinal               uint64 `json:"row_ordinal"`
	RawByteOffset            uint64 `json:"raw_byte_offset"`
	RawByteLength            uint64 `json:"raw_byte_length"`
	RawContentSHA256         string `json:"raw_content_sha256"`
	NaturalKey               string `json:"natural_key"`
	PublisherRecordReference string `json:"publisher_record_reference"`
	RecordVersionID          string `json:"record_version_id"`
	Cycle                    string `json:"cycle"`
	State                    string `json:"state"`
}

// PublishScheduleEOccurrences selects one two-year cycle from the exact
// all-history Schedule E relation. It preserves every selected physical row
// and applies no amendment, memo, support/oppose, or spending policy.
func PublishScheduleEOccurrences(
	ctx context.Context,
	releaseManifest fecrelease.ReleaseManifest,
	releaseManifestSHA256 string,
	cycle string,
	runID string,
	options Options,
) (ScheduleEOccurrenceManifest, error) {
	if options.Clock == nil {
		options.Clock = time.Now
	}
	if options.FreeFloorBytes == 0 {
		options.FreeFloorBytes = fecrelease.AcquisitionFreeFloorBytes
	}
	if options.WorkingMarginBytes == 0 {
		options.WorkingMarginBytes = fecrelease.AcquisitionWorkingMarginBytes
	}
	if options.DiskAvailable == nil {
		options.DiskAvailable = availableBytes
	}
	if options.StorageRoot == "" {
		return ScheduleEOccurrenceManifest{}, fmt.Errorf("storage root is required")
	}
	if !validCycle(cycle) {
		return ScheduleEOccurrenceManifest{}, fmt.Errorf("cycle must be a four-digit even year")
	}
	if !fecrelease.ValidAcquisitionRunID(runID) {
		return ScheduleEOccurrenceManifest{}, fmt.Errorf("run ID contains unsupported characters")
	}
	if !digestPattern.MatchString(releaseManifestSHA256) {
		return ScheduleEOccurrenceManifest{}, fmt.Errorf("source release manifest SHA-256 is invalid")
	}
	if issues := fecrelease.ValidateKnownManifest(releaseManifest); len(issues) != 0 {
		return ScheduleEOccurrenceManifest{}, fmt.Errorf("invalid source release: %s", issues[0].Message)
	}
	if releaseManifest.InventoryVersion != fecrelease.ScheduleEInventoryVersion &&
		releaseManifest.InventoryVersion != fecrelease.ActiveInventoryVersion &&
		releaseManifest.InventoryVersion != fecrelease.CommitteeSummaryInventoryVersion {
		return ScheduleEOccurrenceManifest{}, fmt.Errorf("source release does not include Schedule E")
	}
	output, sourceSHA, err := selectedScheduleEOutput(releaseManifest)
	if err != nil {
		return ScheduleEOccurrenceManifest{}, err
	}

	basePath := scheduleEEvidenceBase()
	currentPath := options.CurrentManifestPath
	if currentPath == "" {
		currentPath = filepath.Join(options.StorageRoot, basePath, "current", cycle+".json")
	}
	if err := requirePathInside(options.StorageRoot, currentPath); err != nil {
		return ScheduleEOccurrenceManifest{}, err
	}
	unlock, err := lockContext(ctx, filepath.Join(options.StorageRoot, basePath, ".publish-"+cycle+".lock"))
	if err != nil {
		return ScheduleEOccurrenceManifest{}, err
	}
	defer unlock()

	current, err := readScheduleEOccurrenceManifestIfPresent(currentPath)
	if err != nil {
		return ScheduleEOccurrenceManifest{}, err
	}
	if current != nil {
		if err := validateScheduleEOccurrenceManifest(*current); err != nil {
			return ScheduleEOccurrenceManifest{}, fmt.Errorf("invalid current Schedule E occurrence manifest: %w", err)
		}
		if err := validateScheduleEOccurrenceManifestBacking(ctx, options.StorageRoot, *current); err != nil {
			return ScheduleEOccurrenceManifest{}, err
		}
		if current.Cycle != cycle {
			return ScheduleEOccurrenceManifest{}, fmt.Errorf("current Schedule E occurrence manifest belongs to cycle %s", current.Cycle)
		}
		descends, err := releaseDescendsFrom(ctx, options.StorageRoot, releaseManifest, current.SourceReleaseID)
		if err != nil {
			return ScheduleEOccurrenceManifest{}, err
		}
		if !descends {
			return ScheduleEOccurrenceManifest{}, fmt.Errorf("source release does not descend from the Schedule E occurrence baseline")
		}
		if current.Cycle == cycle && current.SourceArtifactSHA256 == sourceSHA &&
			current.StagedOutputSHA256 == output.CompressedSHA256 && current.ParserVersion == ScheduleEOccurrenceParserVersion {
			return *current, nil
		}
	}

	priorID := ""
	if current != nil {
		priorID = current.OccurrenceSetID
	}
	setID := digestParts("fec.schedule-e.occurrence-set.v1", sourceSHA, output.CompressedSHA256, output.Selection, cycle, ScheduleEOccurrenceParserVersion)
	manifestPath := filepath.Join(options.StorageRoot, basePath, "manifests", setID+".json")
	if existing, err := readScheduleEOccurrenceManifestIfPresent(manifestPath); err != nil {
		return ScheduleEOccurrenceManifest{}, err
	} else if existing != nil {
		if err := validateScheduleEOccurrenceManifest(*existing); err != nil {
			return ScheduleEOccurrenceManifest{}, err
		}
		if existing.OccurrenceSetID != setID || existing.SourceArtifactSHA256 != sourceSHA || existing.StagedOutputSHA256 != output.CompressedSHA256 {
			return ScheduleEOccurrenceManifest{}, fmt.Errorf("immutable Schedule E occurrence manifest collision")
		}
		if err := validateScheduleEOccurrenceArtifact(ctx, options.StorageRoot, existing.Occurrences); err != nil {
			return ScheduleEOccurrenceManifest{}, err
		}
		if err := writeAtomicJSON(currentPath, *existing); err != nil {
			return ScheduleEOccurrenceManifest{}, err
		}
		return *existing, nil
	}
	if output.UncompressedByteCount > ^uint64(0)-options.WorkingMarginBytes {
		return ScheduleEOccurrenceManifest{}, fmt.Errorf("Schedule E occurrence working-space estimate overflows")
	}
	if err := requireOccurrenceStorage(options, output.UncompressedByteCount+options.WorkingMarginBytes); err != nil {
		return ScheduleEOccurrenceManifest{}, err
	}

	temporaryDirectory := filepath.Join(options.StorageRoot, basePath, "staging", setID, runID)
	writer, err := newArtifactWriterAt(ctx, options.StorageRoot, temporaryDirectory, basePath, "occurrences")
	if err != nil {
		return ScheduleEOccurrenceManifest{}, err
	}
	defer writer.Abort()
	counts, err := streamScheduleEOccurrences(ctx, options.StorageRoot, output, sourceSHA, cycle, writer, options.Progress)
	if err != nil {
		return ScheduleEOccurrenceManifest{}, err
	}
	artifact, err := writer.Finalize()
	if err != nil {
		return ScheduleEOccurrenceManifest{}, err
	}
	_ = os.RemoveAll(temporaryDirectory)

	manifest := ScheduleEOccurrenceManifest{
		Schema: "manifest.schema.json", SchemaVersion: ScheduleEOccurrenceSetSchemaVersion,
		OccurrenceSetID: setID, PriorOccurrenceSetID: priorID,
		SourceReleaseID: releaseManifest.ReleaseID, SourceReleaseManifestSHA256: releaseManifestSHA256,
		SourceArtifactSHA256: sourceSHA, StagedOutputSHA256: output.CompressedSHA256,
		Relation: output.Selection, Cycle: cycle, RunID: runID, State: "published",
		ParserVersion: ScheduleEOccurrenceParserVersion, PublishedAt: options.Clock().UTC(),
		Counts: counts, Occurrences: artifact,
	}
	manifest.Checks = scheduleEOccurrenceChecks(manifest, output)
	if err := validateScheduleEOccurrenceManifest(manifest); err != nil {
		return ScheduleEOccurrenceManifest{}, err
	}
	if err := requireOccurrenceStorage(options, 0); err != nil {
		return ScheduleEOccurrenceManifest{}, err
	}
	if err := writeAtomicJSON(manifestPath, manifest); err != nil {
		return ScheduleEOccurrenceManifest{}, err
	}
	if err := writeAtomicJSON(currentPath, manifest); err != nil {
		return ScheduleEOccurrenceManifest{}, err
	}
	return manifest, nil
}

func streamScheduleEOccurrences(ctx context.Context, storageRoot string, output fecrelease.StagedOutput, sourceSHA, cycle string, writer *artifactWriter, progress func(string)) (ScheduleEOccurrenceCounts, error) {
	path, err := resolveStorageKey(storageRoot, output.StorageKey)
	if err != nil {
		return ScheduleEOccurrenceCounts{}, err
	}
	file, err := os.Open(path)
	if err != nil {
		return ScheduleEOccurrenceCounts{}, err
	}
	defer func() { _ = file.Close() }()
	compressed := &countingHashReader{reader: &contextReader{ctx: ctx, reader: file}, hash: sha256.New()}
	decoder, err := zstd.NewReader(compressed, zstd.WithDecoderConcurrency(2), zstd.WithDecoderLowmem(true), zstd.WithDecoderMaxMemory(64<<20))
	if err != nil {
		return ScheduleEOccurrenceCounts{}, err
	}
	defer decoder.Close()
	uncompressed := &countingHashReader{reader: decoder, hash: sha256.New()}
	rows := schedulee.NewDecoder(uncompressed)
	counts := ScheduleEOccurrenceCounts{}
	seenSubIDs := make(map[string]struct{})
	var byteOffset uint64
	for rows.Scan() {
		row := rows.Row()
		counts.SourceRows++
		if err := schedulee.Validate(row, ""); err != nil {
			return counts, fmt.Errorf("Schedule E source row %d is invalid: %w", row.Number(), err)
		}
		subIDField, _ := row.Field(scheduleEColumnIndex("sub_id"))
		subID := subIDField.String()
		if _, duplicate := seenSubIDs[subID]; duplicate {
			return counts, fmt.Errorf("Schedule E source contains duplicate sub_id %s", subID)
		}
		seenSubIDs[subID] = struct{}{}
		cycleField, _ := row.Field(scheduleEColumnIndex("election_cycle"))
		if cycleField.IsNull() {
			counts.NullCycleRows++
			byteOffset += uint64(len(row.Raw()))
			continue
		}
		if cycleField.String() != cycle {
			counts.OtherCycleRows++
			byteOffset += uint64(len(row.Raw()))
			continue
		}
		rawDigestBytes := sha256.Sum256(row.Raw())
		rawDigest := hex.EncodeToString(rawDigestBytes[:])
		naturalKey := "fec:schedule-e:" + cycle + ":" + subID
		occurrenceID := digestParts("fec.schedule-e.occurrence.v1", sourceSHA, output.Selection, cycle, fmt.Sprintf("%d", row.Number()))
		recordVersionID := digestParts("fec.schedule-e.record-version.v1", cycle, subID, rawDigest)
		occurrence := ScheduleEOccurrence{
			SchemaVersion: ScheduleEOccurrenceSchemaVersion, OccurrenceID: occurrenceID,
			RowOrdinal: row.Number(), RawByteOffset: byteOffset, RawByteLength: uint64(len(row.Raw())), RawContentSHA256: rawDigest,
			NaturalKey: naturalKey, PublisherRecordReference: subID, RecordVersionID: recordVersionID,
			Cycle: cycle, State: "valid",
		}
		if err := writer.WriteJSON(occurrence); err != nil {
			return counts, err
		}
		counts.SelectedRows++
		byteOffset += uint64(len(row.Raw()))
		if progress != nil && counts.SourceRows%100_000 == 0 {
			progress(fmt.Sprintf("scanned %d Schedule E rows; selected %d for %s", counts.SourceRows, counts.SelectedRows, cycle))
		}
	}
	if err := rows.Err(); err != nil {
		return counts, err
	}
	if output.RowCount == nil || counts.SourceRows != *output.RowCount {
		return counts, fmt.Errorf("Schedule E occurrence replay row count mismatch")
	}
	if compressed.bytes != output.CompressedByteCount || hex.EncodeToString(compressed.hash.Sum(nil)) != output.CompressedSHA256 {
		return counts, fmt.Errorf("Schedule E occurrence replay compressed identity mismatch")
	}
	if uncompressed.bytes != output.UncompressedByteCount || hex.EncodeToString(uncompressed.hash.Sum(nil)) != output.UncompressedSHA256 {
		return counts, fmt.Errorf("Schedule E occurrence replay uncompressed identity mismatch")
	}
	return counts, nil
}

func selectedScheduleEOutput(manifest fecrelease.ReleaseManifest) (fecrelease.StagedOutput, string, error) {
	sourceSHA := ""
	for _, artifact := range manifest.Artifacts {
		if artifact.SourceID == fecrelease.ScheduleESourceID {
			sourceSHA = artifact.SHA256
			break
		}
	}
	if sourceSHA == "" {
		return fecrelease.StagedOutput{}, "", fmt.Errorf("published release has no Schedule E source artifact")
	}
	var selected *fecrelease.StagedOutput
	for index := range manifest.StagedOutputs {
		output := &manifest.StagedOutputs[index]
		if output.SourceID == fecrelease.ScheduleESourceID && output.SelectionKind == "relation" {
			if selected != nil {
				return fecrelease.StagedOutput{}, "", fmt.Errorf("published release has multiple Schedule E relation outputs")
			}
			selected = output
		}
	}
	if selected == nil || selected.Period != scheduleEAllHistoryScope || selected.ContractedFieldCount == nil || *selected.ContractedFieldCount != schedulee.FieldCount {
		return fecrelease.StagedOutput{}, "", fmt.Errorf("published release has no exact all-history Schedule E output")
	}
	if selected.SourceArtifactSHA256 != sourceSHA {
		return fecrelease.StagedOutput{}, "", fmt.Errorf("Schedule E staged output belongs to another source artifact")
	}
	return *selected, sourceSHA, nil
}

func scheduleEColumnIndex(name string) int {
	index, ok := schedulee.ColumnIndex(name)
	if !ok {
		panic("missing contracted Schedule E column: " + name)
	}
	return index
}

func scheduleEOccurrenceChecks(manifest ScheduleEOccurrenceManifest, output fecrelease.StagedOutput) []Check {
	return []Check{
		{ID: "source_release_lineage", Passed: true, Severity: "block", Detail: "occurrence set names the exact v2 source release and all-history Schedule E relation"},
		{ID: "staged_output_integrity", Passed: true, Severity: "block", Detail: "complete staged compressed and uncompressed identities matched"},
		{ID: "source_row_conservation", Passed: output.RowCount != nil && manifest.Counts.SourceRows == *output.RowCount, Severity: "block", Detail: fmt.Sprintf("scanned %d all-history source rows", manifest.Counts.SourceRows)},
		{ID: "cycle_partition_conservation", Passed: manifest.Counts.SourceRows == manifest.Counts.SelectedRows+manifest.Counts.OtherCycleRows+manifest.Counts.NullCycleRows, Severity: "block", Detail: "every source row is selected, assigned to another cycle, or has a null cycle"},
		{ID: "selected_occurrence_conservation", Passed: manifest.Occurrences.RecordCount == manifest.Counts.SelectedRows, Severity: "block", Detail: fmt.Sprintf("published %d exact Schedule E occurrences for %s", manifest.Counts.SelectedRows, manifest.Cycle)},
		{ID: "submission_identity_uniqueness", Passed: true, Severity: "block", Detail: "every source sub_id was non-null and globally unique"},
	}
}

func validateScheduleEOccurrenceManifest(manifest ScheduleEOccurrenceManifest) error {
	if manifest.Schema != "manifest.schema.json" || manifest.SchemaVersion != ScheduleEOccurrenceSetSchemaVersion || manifest.ParserVersion != ScheduleEOccurrenceParserVersion {
		return fmt.Errorf("unexpected Schedule E occurrence manifest schema")
	}
	for _, digest := range []string{manifest.OccurrenceSetID, manifest.SourceReleaseManifestSHA256, manifest.SourceArtifactSHA256, manifest.StagedOutputSHA256} {
		if !digestPattern.MatchString(digest) {
			return fmt.Errorf("Schedule E occurrence manifest contains an invalid digest")
		}
	}
	if manifest.PriorOccurrenceSetID != "" && !digestPattern.MatchString(manifest.PriorOccurrenceSetID) {
		return fmt.Errorf("Schedule E prior occurrence set ID is invalid")
	}
	if !releaseIDPattern.MatchString(manifest.SourceReleaseID) || !validCycle(manifest.Cycle) || !fecrelease.ValidAcquisitionRunID(manifest.RunID) || manifest.State != "published" || manifest.PublishedAt.IsZero() {
		return fmt.Errorf("Schedule E occurrence publication identity is incomplete")
	}
	expected := digestParts("fec.schedule-e.occurrence-set.v1", manifest.SourceArtifactSHA256, manifest.StagedOutputSHA256, manifest.Relation, manifest.Cycle, manifest.ParserVersion)
	if manifest.OccurrenceSetID != expected {
		return fmt.Errorf("Schedule E occurrence-set ID does not match canonical inputs")
	}
	if manifest.Counts.SourceRows != manifest.Counts.SelectedRows+manifest.Counts.OtherCycleRows+manifest.Counts.NullCycleRows || manifest.Occurrences.RecordCount != manifest.Counts.SelectedRows {
		return fmt.Errorf("Schedule E occurrence counts are not conserved")
	}
	if manifest.Occurrences.Compression != "zstd" || manifest.Occurrences.CompressedBytes == 0 || !digestPattern.MatchString(manifest.Occurrences.CompressedSHA256) || !digestPattern.MatchString(manifest.Occurrences.UncompressedSHA256) {
		return fmt.Errorf("Schedule E occurrence artifact identity is invalid")
	}
	prefix := filepath.ToSlash(filepath.Join(scheduleEEvidenceBase(), "occurrences", "sha256", manifest.Occurrences.CompressedSHA256[:2])) + "/"
	if !strings.HasPrefix(manifest.Occurrences.StorageKey, prefix) {
		return fmt.Errorf("Schedule E occurrence artifact storage key is not canonical")
	}
	if len(manifest.Checks) < 6 {
		return fmt.Errorf("Schedule E occurrence manifest is missing required checks")
	}
	for _, check := range manifest.Checks {
		if check.Severity == "block" && !check.Passed {
			return fmt.Errorf("blocking Schedule E occurrence check %s failed", check.ID)
		}
	}
	return nil
}

func readScheduleEOccurrenceManifestIfPresent(path string) (*ScheduleEOccurrenceManifest, error) {
	content, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var manifest ScheduleEOccurrenceManifest
	decoder := json.NewDecoder(strings.NewReader(string(content)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&manifest); err != nil {
		return nil, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return nil, fmt.Errorf("multiple JSON values in Schedule E occurrence manifest")
		}
		return nil, err
	}
	return &manifest, nil
}

func validateScheduleEOccurrenceManifestBacking(ctx context.Context, storageRoot string, current ScheduleEOccurrenceManifest) error {
	immutablePath := filepath.Join(storageRoot, scheduleEEvidenceBase(), "manifests", current.OccurrenceSetID+".json")
	immutable, err := readScheduleEOccurrenceManifestIfPresent(immutablePath)
	if err != nil {
		return err
	}
	if immutable == nil || !reflect.DeepEqual(current, *immutable) {
		return fmt.Errorf("active Schedule E occurrence pointer differs from its immutable manifest")
	}
	if err := validateScheduleEOccurrenceManifest(*immutable); err != nil {
		return err
	}
	return validateScheduleEOccurrenceArtifact(ctx, storageRoot, current.Occurrences)
}

func validateScheduleEOccurrenceArtifact(ctx context.Context, storageRoot string, artifact Artifact) error {
	path, err := resolveStorageKey(storageRoot, artifact.StorageKey)
	if err != nil {
		return err
	}
	info, err := os.Stat(path)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() || info.Size() < 0 || uint64(info.Size()) != artifact.CompressedBytes {
		return fmt.Errorf("Schedule E occurrence artifact does not match its published size")
	}
	return verifyArtifact(ctx, path, artifact)
}

func scheduleEEvidenceBase() string { return filepath.Join("evidence", "fec", "schedule-e") }
