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
	ScheduleEFactSetSchemaVersion = "legal-tender.fec.schedule-e-fact-set.v1"
	ScheduleEFactSchemaVersion    = "legal-tender.fec.schedule-e-independent-expenditure.v1"
	ScheduleENormalizerVersion    = "legal-tender.fec.schedule-e-normalizer.v1"
	ScheduleEFactType             = "fec.schedule_e_independent_expenditure.v1"
)

type ScheduleEFactManifest struct {
	Schema                      string              `json:"$schema"`
	SchemaVersion               string              `json:"schema_version"`
	FactSetID                   string              `json:"fact_set_id"`
	FactType                    string              `json:"fact_type"`
	Cycle                       string              `json:"cycle"`
	SourceContract              string              `json:"source_contract"`
	SourceReleaseID             string              `json:"source_release_id"`
	SourceReleaseManifestSHA256 string              `json:"source_release_manifest_sha256"`
	OccurrenceSetID             string              `json:"occurrence_set_id"`
	OccurrenceManifestSHA256    string              `json:"occurrence_manifest_sha256"`
	RunID                       string              `json:"run_id"`
	State                       string              `json:"state"`
	NormalizerVersion           string              `json:"normalizer_version"`
	FactSchemaVersion           string              `json:"fact_schema_version"`
	PublishedAt                 time.Time           `json:"published_at"`
	Counts                      ScheduleEFactCounts `json:"counts"`
	Facts                       Artifact            `json:"facts"`
	Checks                      []Check             `json:"checks"`
}

type ScheduleEFactCounts struct {
	SourceOccurrences uint64 `json:"source_occurrences"`
	Facts             uint64 `json:"facts"`
	ValidFacts        uint64 `json:"valid_facts"`
	InvalidFacts      uint64 `json:"invalid_facts"`
}

type ScheduleEFact struct {
	SchemaVersion   string                                     `json:"schema_version"`
	FactID          string                                     `json:"fact_id"`
	FactType        string                                     `json:"fact_type"`
	Cycle           string                                     `json:"cycle"`
	NaturalKey      string                                     `json:"natural_key"`
	OccurrenceSetID string                                     `json:"occurrence_set_id"`
	OccurrenceID    string                                     `json:"occurrence_id"`
	RecordVersionID string                                     `json:"record_version_id"`
	SourceReleaseID string                                     `json:"source_release_id"`
	SourceContract  string                                     `json:"source_contract"`
	State           string                                     `json:"state"`
	IssueCodes      []string                                   `json:"issue_codes"`
	SourceFields    json.RawMessage                            `json:"source_fields"`
	TypedFields     ScheduleEIndependentExpenditureTypedFields `json:"typed_fields"`
}

// PublishScheduleEFacts turns every selected physical Schedule E occurrence
// into one lossless typed fact. It does not select effective amendments,
// exclude memos, or interpret support/oppose and spending policy.
func PublishScheduleEFacts(
	ctx context.Context,
	releaseManifest fecrelease.ReleaseManifest,
	releaseManifestSHA256 string,
	occurrenceManifestPath string,
	runID string,
	options Options,
) (ScheduleEFactManifest, error) {
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
	if options.StorageRoot == "" || occurrenceManifestPath == "" {
		return ScheduleEFactManifest{}, fmt.Errorf("storage root and occurrence manifest path are required")
	}
	if !fecrelease.ValidAcquisitionRunID(runID) {
		return ScheduleEFactManifest{}, fmt.Errorf("run ID contains unsupported characters")
	}
	if !digestPattern.MatchString(releaseManifestSHA256) {
		return ScheduleEFactManifest{}, fmt.Errorf("source release manifest SHA-256 is invalid")
	}
	if issues := fecrelease.ValidateKnownManifest(releaseManifest); len(issues) != 0 {
		return ScheduleEFactManifest{}, fmt.Errorf("invalid source release: %s", issues[0].Message)
	}

	occurrenceManifest, _, err := readScheduleEOccurrenceManifestWithSHA256(occurrenceManifestPath)
	if err != nil {
		return ScheduleEFactManifest{}, err
	}
	if err := validateScheduleEOccurrenceManifest(occurrenceManifest); err != nil {
		return ScheduleEFactManifest{}, fmt.Errorf("invalid Schedule E occurrence input: %w", err)
	}
	if err := validateScheduleEOccurrenceManifestBacking(ctx, options.StorageRoot, occurrenceManifest); err != nil {
		return ScheduleEFactManifest{}, fmt.Errorf("validate Schedule E occurrence input: %w", err)
	}
	immutableOccurrencePath := filepath.Join(options.StorageRoot, scheduleEEvidenceBase(), "manifests", occurrenceManifest.OccurrenceSetID+".json")
	_, occurrenceManifestSHA256, err := readScheduleEOccurrenceManifestWithSHA256(immutableOccurrencePath)
	if err != nil {
		return ScheduleEFactManifest{}, err
	}
	descends, err := releaseDescendsFrom(ctx, options.StorageRoot, releaseManifest, occurrenceManifest.SourceReleaseID)
	if err != nil {
		return ScheduleEFactManifest{}, err
	}
	if !descends {
		return ScheduleEFactManifest{}, fmt.Errorf("source release %s does not descend from occurrence release %s", releaseManifest.ReleaseID, occurrenceManifest.SourceReleaseID)
	}
	output, sourceSHA, err := selectedScheduleEOutput(releaseManifest)
	if err != nil {
		return ScheduleEFactManifest{}, err
	}
	if sourceSHA != occurrenceManifest.SourceArtifactSHA256 || output.CompressedSHA256 != occurrenceManifest.StagedOutputSHA256 || output.Selection != occurrenceManifest.Relation {
		return ScheduleEFactManifest{}, fmt.Errorf("Schedule E occurrence input does not describe the selected source bytes")
	}

	basePath := scheduleEFactBase()
	currentPath := options.CurrentManifestPath
	if currentPath == "" {
		currentPath = filepath.Join(options.StorageRoot, basePath, "current", occurrenceManifest.Cycle+".json")
	}
	if err := requirePathInside(options.StorageRoot, currentPath); err != nil {
		return ScheduleEFactManifest{}, err
	}
	unlock, err := lockContext(ctx, filepath.Join(options.StorageRoot, basePath, ".publish-"+occurrenceManifest.Cycle+".lock"))
	if err != nil {
		return ScheduleEFactManifest{}, err
	}
	defer unlock()

	current, err := readScheduleEFactManifestIfPresent(currentPath)
	if err != nil {
		return ScheduleEFactManifest{}, err
	}
	if current != nil {
		if err := validateScheduleEFactManifest(*current); err != nil {
			return ScheduleEFactManifest{}, fmt.Errorf("invalid current Schedule E fact manifest: %w", err)
		}
		if err := validateScheduleEFactManifestBacking(ctx, options.StorageRoot, *current); err != nil {
			return ScheduleEFactManifest{}, err
		}
		if current.Cycle != occurrenceManifest.Cycle {
			return ScheduleEFactManifest{}, fmt.Errorf("current Schedule E fact manifest belongs to cycle %s", current.Cycle)
		}
		if current.OccurrenceSetID == occurrenceManifest.OccurrenceSetID && current.NormalizerVersion == ScheduleENormalizerVersion && current.FactSchemaVersion == ScheduleEFactSchemaVersion {
			return *current, nil
		}
	}

	factSetID := digestParts("fec.schedule-e.fact-set.v1", occurrenceManifest.OccurrenceSetID, ScheduleENormalizerVersion, ScheduleEFactSchemaVersion)
	manifestPath := filepath.Join(options.StorageRoot, basePath, "manifests", factSetID+".json")
	if existing, err := readScheduleEFactManifestIfPresent(manifestPath); err != nil {
		return ScheduleEFactManifest{}, err
	} else if existing != nil {
		if err := validateScheduleEFactManifest(*existing); err != nil {
			return ScheduleEFactManifest{}, err
		}
		if existing.FactSetID != factSetID || existing.OccurrenceSetID != occurrenceManifest.OccurrenceSetID || existing.OccurrenceManifestSHA256 != occurrenceManifestSHA256 {
			return ScheduleEFactManifest{}, fmt.Errorf("immutable Schedule E fact manifest collision")
		}
		if err := validateScheduleEFactArtifact(ctx, options.StorageRoot, existing.Facts); err != nil {
			return ScheduleEFactManifest{}, err
		}
		if err := writeAtomicJSON(currentPath, *existing); err != nil {
			return ScheduleEFactManifest{}, err
		}
		return *existing, nil
	}
	if output.UncompressedByteCount > ^uint64(0)-options.WorkingMarginBytes {
		return ScheduleEFactManifest{}, fmt.Errorf("Schedule E fact working-space estimate overflows")
	}
	if err := requireOccurrenceStorage(options, output.UncompressedByteCount+options.WorkingMarginBytes); err != nil {
		return ScheduleEFactManifest{}, err
	}
	if options.Progress != nil {
		options.Progress("normalizing Schedule E facts for " + occurrenceManifest.Cycle)
	}
	manifest, err := buildScheduleEFactSet(ctx, occurrenceManifest, occurrenceManifestSHA256, output, runID, factSetID, options)
	if err != nil {
		return ScheduleEFactManifest{}, err
	}
	if err := validateScheduleEFactManifest(manifest); err != nil {
		return ScheduleEFactManifest{}, err
	}
	if err := requireOccurrenceStorage(options, 0); err != nil {
		return ScheduleEFactManifest{}, err
	}
	if err := writeAtomicJSON(manifestPath, manifest); err != nil {
		return ScheduleEFactManifest{}, err
	}
	if err := writeAtomicJSON(currentPath, manifest); err != nil {
		return ScheduleEFactManifest{}, err
	}
	return manifest, nil
}

func buildScheduleEFactSet(ctx context.Context, occurrence ScheduleEOccurrenceManifest, occurrenceSHA string, output fecrelease.StagedOutput, runID, factSetID string, options Options) (ScheduleEFactManifest, error) {
	basePath := scheduleEFactBase()
	temporaryDirectory := filepath.Join(options.StorageRoot, basePath, "staging", factSetID, runID)
	if err := os.MkdirAll(temporaryDirectory, 0o750); err != nil {
		return ScheduleEFactManifest{}, err
	}
	defer func() { _ = os.RemoveAll(temporaryDirectory) }()
	writer, err := newArtifactWriterAt(ctx, options.StorageRoot, temporaryDirectory, basePath, "facts")
	if err != nil {
		return ScheduleEFactManifest{}, err
	}
	defer writer.Abort()
	counts, err := streamScheduleEFacts(ctx, occurrence, output, options.StorageRoot, writer, options.Progress)
	if err != nil {
		return ScheduleEFactManifest{}, err
	}
	artifact, err := writer.Finalize()
	if err != nil {
		return ScheduleEFactManifest{}, err
	}
	manifest := ScheduleEFactManifest{
		Schema: "manifest.schema.json", SchemaVersion: ScheduleEFactSetSchemaVersion,
		FactSetID: factSetID, FactType: ScheduleEFactType, Cycle: occurrence.Cycle, SourceContract: ScheduleESourceContract,
		SourceReleaseID: occurrence.SourceReleaseID, SourceReleaseManifestSHA256: occurrence.SourceReleaseManifestSHA256,
		OccurrenceSetID: occurrence.OccurrenceSetID, OccurrenceManifestSHA256: occurrenceSHA,
		RunID: runID, State: "published", NormalizerVersion: ScheduleENormalizerVersion,
		FactSchemaVersion: ScheduleEFactSchemaVersion, PublishedAt: options.Clock().UTC(), Counts: counts, Facts: artifact,
	}
	manifest.Checks = []Check{
		{ID: "occurrence_lineage", Passed: true, Severity: "block", Detail: "fact set names an immutable validated Schedule E occurrence set"},
		{ID: "selected_output_integrity", Passed: true, Severity: "block", Detail: "normalization replay matched the exact staged all-history source bytes"},
		{ID: "lossless_projection", Passed: counts.Facts == occurrence.Counts.SelectedRows, Severity: "block", Detail: "every selected physical Schedule E occurrence produced one fact"},
		{ID: "fact_state_conservation", Passed: counts.Facts == counts.ValidFacts+counts.InvalidFacts, Severity: "block", Detail: "every normalized fact has an explicit typed state"},
	}
	return manifest, nil
}

func streamScheduleEFacts(ctx context.Context, occurrence ScheduleEOccurrenceManifest, output fecrelease.StagedOutput, storageRoot string, writer *artifactWriter, progress func(string)) (ScheduleEFactCounts, error) {
	path, err := resolveStorageKey(storageRoot, output.StorageKey)
	if err != nil {
		return ScheduleEFactCounts{}, err
	}
	file, err := os.Open(path)
	if err != nil {
		return ScheduleEFactCounts{}, err
	}
	defer func() { _ = file.Close() }()
	compressed := &countingHashReader{reader: &contextReader{ctx: ctx, reader: file}, hash: sha256.New()}
	decoder, err := zstd.NewReader(compressed, zstd.WithDecoderConcurrency(2), zstd.WithDecoderLowmem(true), zstd.WithDecoderMaxMemory(64<<20))
	if err != nil {
		return ScheduleEFactCounts{}, err
	}
	defer decoder.Close()
	uncompressed := &countingHashReader{reader: decoder, hash: sha256.New()}
	rows := schedulee.NewDecoder(uncompressed)
	occurrences, err := openScheduleEOccurrenceReader(ctx, storageRoot, occurrence.Occurrences)
	if err != nil {
		return ScheduleEFactCounts{}, err
	}
	defer occurrences.Abort()
	counts := ScheduleEFactCounts{SourceOccurrences: occurrence.Counts.SelectedRows}
	var sourceRows uint64
	var byteOffset uint64
	for rows.Scan() {
		row := rows.Row()
		sourceRows++
		rowOffset := byteOffset
		byteOffset += uint64(len(row.Raw()))
		if err := schedulee.Validate(row, ""); err != nil {
			return counts, fmt.Errorf("Schedule E source row %d is invalid: %w", row.Number(), err)
		}
		cycleField, _ := row.Field(scheduleEColumnIndex("election_cycle"))
		if cycleField.IsNull() || cycleField.String() != occurrence.Cycle {
			continue
		}
		record, err := schedulee.Freeze(row, occurrence.Cycle)
		if err != nil {
			return counts, err
		}
		subID, _ := record.ValueByName("sub_id")
		natural := "fec:schedule-e:" + occurrence.Cycle + ":" + subID.Lexeme
		rawDigestBytes := sha256.Sum256(row.Raw())
		rawDigest := hex.EncodeToString(rawDigestBytes[:])
		occurrenceID := digestParts("fec.schedule-e.occurrence.v1", occurrence.SourceArtifactSHA256, occurrence.Relation, occurrence.Cycle, fmt.Sprintf("%d", row.Number()))
		recordVersionID := digestParts("fec.schedule-e.record-version.v1", occurrence.Cycle, subID.Lexeme, rawDigest)
		expectedOccurrence, ok, err := occurrences.Next()
		if err != nil {
			return counts, err
		}
		if !ok {
			return counts, fmt.Errorf("Schedule E occurrence artifact ended before selected source row %d", row.Number())
		}
		if expectedOccurrence.SchemaVersion != ScheduleEOccurrenceSchemaVersion ||
			expectedOccurrence.OccurrenceID != occurrenceID ||
			expectedOccurrence.RowOrdinal != row.Number() ||
			expectedOccurrence.RawByteOffset != rowOffset ||
			expectedOccurrence.RawByteLength != uint64(len(row.Raw())) ||
			expectedOccurrence.RawContentSHA256 != rawDigest ||
			expectedOccurrence.NaturalKey != natural ||
			expectedOccurrence.PublisherRecordReference != subID.Lexeme ||
			expectedOccurrence.RecordVersionID != recordVersionID ||
			expectedOccurrence.Cycle != occurrence.Cycle ||
			expectedOccurrence.State != "valid" {
			return counts, fmt.Errorf("Schedule E occurrence artifact does not match selected source row %d", row.Number())
		}
		typed, issueCodes, err := normalizeScheduleEIndependentExpenditure(record)
		if err != nil {
			return counts, err
		}
		state := "valid"
		if len(issueCodes) != 0 {
			state = "invalid"
			counts.InvalidFacts++
		} else {
			counts.ValidFacts++
		}
		fact := ScheduleEFact{
			SchemaVersion: ScheduleEFactSchemaVersion,
			FactID:        digestParts("fec.schedule-e.fact.v1", recordVersionID, ScheduleENormalizerVersion, ScheduleEFactSchemaVersion),
			FactType:      ScheduleEFactType, Cycle: occurrence.Cycle, NaturalKey: natural,
			OccurrenceSetID: occurrence.OccurrenceSetID, OccurrenceID: occurrenceID, RecordVersionID: recordVersionID,
			SourceReleaseID: occurrence.SourceReleaseID, SourceContract: ScheduleESourceContract,
			State: state, IssueCodes: issueCodes, SourceFields: record.CanonicalJSON(), TypedFields: typed,
		}
		if err := writer.WriteJSON(fact); err != nil {
			return counts, err
		}
		counts.Facts++
		if progress != nil && counts.Facts%100_000 == 0 {
			progress(fmt.Sprintf("normalized %d Schedule E facts for %s", counts.Facts, occurrence.Cycle))
		}
		if counts.Facts&0x3fff == 0 {
			if err := ctx.Err(); err != nil {
				return counts, err
			}
		}
	}
	if err := rows.Err(); err != nil {
		return counts, err
	}
	if extra, ok, err := occurrences.Next(); err != nil {
		return counts, err
	} else if ok {
		return counts, fmt.Errorf("Schedule E occurrence artifact contains extra row %d", extra.RowOrdinal)
	}
	if err := occurrences.Close(); err != nil {
		return counts, err
	}
	if output.RowCount == nil || sourceRows != *output.RowCount {
		return counts, fmt.Errorf("Schedule E fact replay row count mismatch")
	}
	if counts.Facts != occurrence.Counts.SelectedRows {
		return counts, fmt.Errorf("Schedule E fact projection selected %d rows; want %d", counts.Facts, occurrence.Counts.SelectedRows)
	}
	if compressed.bytes != output.CompressedByteCount || hex.EncodeToString(compressed.hash.Sum(nil)) != output.CompressedSHA256 {
		return counts, fmt.Errorf("Schedule E fact replay compressed identity mismatch")
	}
	if uncompressed.bytes != output.UncompressedByteCount || hex.EncodeToString(uncompressed.hash.Sum(nil)) != output.UncompressedSHA256 {
		return counts, fmt.Errorf("Schedule E fact replay uncompressed identity mismatch")
	}
	return counts, nil
}

type scheduleEOccurrenceReader struct {
	file         *os.File
	compressed   *countingHashReader
	decoder      *zstd.Decoder
	uncompressed *countingHashReader
	jsonDecoder  *json.Decoder
	expected     Artifact
	records      uint64
	closed       bool
}

func openScheduleEOccurrenceReader(ctx context.Context, storageRoot string, artifact Artifact) (*scheduleEOccurrenceReader, error) {
	path, err := resolveStorageKey(storageRoot, artifact.StorageKey)
	if err != nil {
		return nil, err
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	compressed := &countingHashReader{reader: &contextReader{ctx: ctx, reader: file}, hash: sha256.New()}
	decoder, err := zstd.NewReader(compressed, zstd.WithDecoderConcurrency(2), zstd.WithDecoderLowmem(true), zstd.WithDecoderMaxMemory(64<<20))
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	uncompressed := &countingHashReader{reader: &contextReader{ctx: ctx, reader: decoder}, hash: sha256.New()}
	return &scheduleEOccurrenceReader{
		file: file, compressed: compressed, decoder: decoder, uncompressed: uncompressed,
		jsonDecoder: json.NewDecoder(uncompressed), expected: artifact,
	}, nil
}

func (reader *scheduleEOccurrenceReader) Next() (ScheduleEOccurrence, bool, error) {
	if reader.closed {
		return ScheduleEOccurrence{}, false, fmt.Errorf("Schedule E occurrence reader is closed")
	}
	var occurrence ScheduleEOccurrence
	if err := reader.jsonDecoder.Decode(&occurrence); err != nil {
		if errors.Is(err, io.EOF) {
			return ScheduleEOccurrence{}, false, nil
		}
		return ScheduleEOccurrence{}, false, err
	}
	reader.records++
	return occurrence, true, nil
}

func (reader *scheduleEOccurrenceReader) Close() error {
	if reader.closed {
		return nil
	}
	reader.closed = true
	reader.decoder.Close()
	closeErr := reader.file.Close()
	if reader.records != reader.expected.RecordCount {
		return fmt.Errorf("Schedule E occurrence artifact record count mismatch")
	}
	if reader.compressed.bytes != reader.expected.CompressedBytes || hex.EncodeToString(reader.compressed.hash.Sum(nil)) != reader.expected.CompressedSHA256 {
		return fmt.Errorf("Schedule E occurrence artifact compressed identity mismatch")
	}
	if reader.uncompressed.bytes != reader.expected.UncompressedBytes || hex.EncodeToString(reader.uncompressed.hash.Sum(nil)) != reader.expected.UncompressedSHA256 {
		return fmt.Errorf("Schedule E occurrence artifact uncompressed identity mismatch")
	}
	return closeErr
}

func (reader *scheduleEOccurrenceReader) Abort() {
	if reader.closed {
		return
	}
	reader.closed = true
	reader.decoder.Close()
	_ = reader.file.Close()
}

func readScheduleEOccurrenceManifestWithSHA256(path string) (ScheduleEOccurrenceManifest, string, error) {
	content, err := os.ReadFile(path)
	if err != nil {
		return ScheduleEOccurrenceManifest{}, "", err
	}
	var manifest ScheduleEOccurrenceManifest
	decoder := json.NewDecoder(strings.NewReader(string(content)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&manifest); err != nil {
		return ScheduleEOccurrenceManifest{}, "", err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return ScheduleEOccurrenceManifest{}, "", fmt.Errorf("multiple JSON values in Schedule E occurrence manifest")
		}
		return ScheduleEOccurrenceManifest{}, "", err
	}
	digest := sha256.Sum256(content)
	return manifest, hex.EncodeToString(digest[:]), nil
}

func readScheduleEFactManifestIfPresent(path string) (*ScheduleEFactManifest, error) {
	content, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var manifest ScheduleEFactManifest
	decoder := json.NewDecoder(strings.NewReader(string(content)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&manifest); err != nil {
		return nil, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return nil, fmt.Errorf("multiple JSON values in Schedule E fact manifest")
		}
		return nil, err
	}
	return &manifest, nil
}

func validateScheduleEFactManifest(manifest ScheduleEFactManifest) error {
	if manifest.Schema != "manifest.schema.json" || manifest.SchemaVersion != ScheduleEFactSetSchemaVersion || manifest.FactType != ScheduleEFactType {
		return fmt.Errorf("unexpected Schedule E fact manifest schema")
	}
	if manifest.SourceContract != ScheduleESourceContract || !validCycle(manifest.Cycle) {
		return fmt.Errorf("Schedule E fact identity is inconsistent")
	}
	for _, digest := range []string{manifest.FactSetID, manifest.SourceReleaseManifestSHA256, manifest.OccurrenceSetID, manifest.OccurrenceManifestSHA256} {
		if !digestPattern.MatchString(digest) {
			return fmt.Errorf("Schedule E fact manifest contains an invalid digest")
		}
	}
	if !releaseIDPattern.MatchString(manifest.SourceReleaseID) || !fecrelease.ValidAcquisitionRunID(manifest.RunID) || manifest.State != "published" || manifest.PublishedAt.IsZero() {
		return fmt.Errorf("Schedule E fact publication identity is incomplete")
	}
	if manifest.NormalizerVersion != ScheduleENormalizerVersion || manifest.FactSchemaVersion != ScheduleEFactSchemaVersion {
		return fmt.Errorf("Schedule E fact parser contract is unsupported")
	}
	expected := digestParts("fec.schedule-e.fact-set.v1", manifest.OccurrenceSetID, manifest.NormalizerVersion, manifest.FactSchemaVersion)
	if manifest.FactSetID != expected {
		return fmt.Errorf("Schedule E fact-set ID does not match canonical inputs")
	}
	if manifest.Facts.Compression != "zstd" || manifest.Facts.CompressedBytes == 0 || !digestPattern.MatchString(manifest.Facts.CompressedSHA256) || !digestPattern.MatchString(manifest.Facts.UncompressedSHA256) || manifest.Facts.RecordCount != manifest.Counts.Facts {
		return fmt.Errorf("Schedule E fact artifact identity is invalid")
	}
	prefix := filepath.ToSlash(filepath.Join(scheduleEFactBase(), "facts", "sha256", manifest.Facts.CompressedSHA256[:2])) + "/"
	if !strings.HasPrefix(manifest.Facts.StorageKey, prefix) {
		return fmt.Errorf("Schedule E fact artifact storage key is not canonical")
	}
	if manifest.Counts.SourceOccurrences != manifest.Counts.Facts || manifest.Counts.Facts != manifest.Counts.ValidFacts+manifest.Counts.InvalidFacts {
		return fmt.Errorf("Schedule E fact counts are not conserved")
	}
	if len(manifest.Checks) < 4 {
		return fmt.Errorf("Schedule E fact manifest is missing required checks")
	}
	for _, check := range manifest.Checks {
		if check.Severity == "block" && !check.Passed {
			return fmt.Errorf("blocking Schedule E fact check %s failed", check.ID)
		}
	}
	return nil
}

func validateScheduleEFactManifestBacking(ctx context.Context, storageRoot string, current ScheduleEFactManifest) error {
	immutablePath := filepath.Join(storageRoot, scheduleEFactBase(), "manifests", current.FactSetID+".json")
	immutable, err := readScheduleEFactManifestIfPresent(immutablePath)
	if err != nil {
		return err
	}
	if immutable == nil || !reflect.DeepEqual(current, *immutable) {
		return fmt.Errorf("active Schedule E fact pointer differs from its immutable manifest")
	}
	if err := validateScheduleEFactManifest(*immutable); err != nil {
		return err
	}
	return validateScheduleEFactArtifact(ctx, storageRoot, current.Facts)
}

func validateScheduleEFactArtifact(ctx context.Context, storageRoot string, artifact Artifact) error {
	path, err := resolveStorageKey(storageRoot, artifact.StorageKey)
	if err != nil {
		return err
	}
	info, err := os.Stat(path)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() || info.Size() < 0 || uint64(info.Size()) != artifact.CompressedBytes {
		return fmt.Errorf("Schedule E fact artifact does not match its published size")
	}
	return verifyArtifact(ctx, path, artifact)
}

func scheduleEFactBase() string { return filepath.Join("facts", "fec", "schedule-e") }
