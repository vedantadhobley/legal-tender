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
	"github.com/vedantadhobley/legal-tender/internal/source/fec/schedulea"
)

const (
	ScheduleAFactSetSchemaVersion = "legal-tender.fec.schedule-a-fact-set.v1"
	ScheduleAFactSchemaVersion    = "legal-tender.fec.schedule-a-receipt.v1"
	ScheduleANormalizerVersion    = "legal-tender.fec.schedule-a-normalizer.v1"
	ScheduleASourceContract       = "fec/schedule-a@1.0.0"
	ScheduleAFactType             = "fec.schedule_a_receipt.v1"
)

type ScheduleAFactManifest struct {
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
	Counts                      ScheduleAFactCounts `json:"counts"`
	Facts                       Artifact            `json:"facts"`
	Checks                      []Check             `json:"checks"`
}

type ScheduleAFactCounts struct {
	SourceOccurrences   uint64 `json:"source_occurrences"`
	Facts               uint64 `json:"facts"`
	ValidFacts          uint64 `json:"valid_facts"`
	InvalidFacts        uint64 `json:"invalid_facts"`
	ExcludedOccurrences uint64 `json:"excluded_occurrences"`
	SourceInvalid       uint64 `json:"source_invalid_occurrences"`
	SourceDuplicates    uint64 `json:"source_duplicate_occurrences"`
}

type ScheduleAFact struct {
	SchemaVersion   string                      `json:"schema_version"`
	FactID          string                      `json:"fact_id"`
	FactType        string                      `json:"fact_type"`
	Cycle           string                      `json:"cycle"`
	NaturalKey      string                      `json:"natural_key"`
	OccurrenceSetID string                      `json:"occurrence_set_id"`
	OccurrenceID    string                      `json:"occurrence_id"`
	RecordVersionID string                      `json:"record_version_id"`
	SourceReleaseID string                      `json:"source_release_id"`
	SourceContract  string                      `json:"source_contract"`
	State           string                      `json:"state"`
	IssueCodes      []string                    `json:"issue_codes"`
	SourceFields    json.RawMessage             `json:"source_fields"`
	TypedFields     ScheduleAReceiptTypedFields `json:"typed_fields"`
}

// PublishScheduleAFacts turns every unique source-valid record in one exact
// Schedule A occurrence set into a lossless typed receipt fact. It applies no
// memo, amendment, entity-resolution, or receipt-counting policy.
func PublishScheduleAFacts(
	ctx context.Context,
	releaseManifest fecrelease.ReleaseManifest,
	releaseManifestSHA256 string,
	occurrenceManifestPath string,
	runID string,
	options Options,
) (ScheduleAFactManifest, error) {
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
		return ScheduleAFactManifest{}, fmt.Errorf("storage root and occurrence manifest path are required")
	}
	if !fecrelease.ValidAcquisitionRunID(runID) {
		return ScheduleAFactManifest{}, fmt.Errorf("run ID contains unsupported characters")
	}
	if !digestPattern.MatchString(releaseManifestSHA256) {
		return ScheduleAFactManifest{}, fmt.Errorf("source release manifest SHA-256 is invalid")
	}
	if issues := fecrelease.ValidateKnownManifest(releaseManifest); len(issues) != 0 {
		return ScheduleAFactManifest{}, fmt.Errorf("invalid source release: %s", issues[0].Message)
	}

	occurrenceManifest, _, err := readScheduleAManifestWithSHA256(occurrenceManifestPath)
	if err != nil {
		return ScheduleAFactManifest{}, err
	}
	if err := validateManifest(occurrenceManifest); err != nil {
		return ScheduleAFactManifest{}, fmt.Errorf("invalid Schedule A occurrence input: %w", err)
	}
	if err := validateManifestBacking(options.StorageRoot, occurrenceManifest); err != nil {
		return ScheduleAFactManifest{}, fmt.Errorf("validate Schedule A occurrence input: %w", err)
	}
	immutableOccurrencePath := filepath.Join(options.StorageRoot, "evidence", "fec", "schedule-a", "manifests", occurrenceManifest.OccurrenceSetID+".json")
	_, occurrenceManifestSHA256, err := readScheduleAManifestWithSHA256(immutableOccurrencePath)
	if err != nil {
		return ScheduleAFactManifest{}, err
	}
	descends, err := releaseDescendsFrom(ctx, options.StorageRoot, releaseManifest, occurrenceManifest.SourceReleaseID)
	if err != nil {
		return ScheduleAFactManifest{}, err
	}
	if !descends {
		return ScheduleAFactManifest{}, fmt.Errorf("source release %s does not descend from occurrence release %s", releaseManifest.ReleaseID, occurrenceManifest.SourceReleaseID)
	}
	output, sourceSHA, err := selectedScheduleAOutput(releaseManifest, occurrenceManifest.Cycle)
	if err != nil {
		return ScheduleAFactManifest{}, err
	}
	if sourceSHA != occurrenceManifest.SourceArtifactSHA256 || output.CompressedSHA256 != occurrenceManifest.StagedOutputSHA256 || output.Selection != occurrenceManifest.Relation {
		return ScheduleAFactManifest{}, fmt.Errorf("Schedule A occurrence input does not describe the selected source bytes")
	}

	basePath := scheduleAFactBase()
	currentPath := options.CurrentManifestPath
	if currentPath == "" {
		currentPath = filepath.Join(options.StorageRoot, basePath, "current", occurrenceManifest.Cycle+".json")
	}
	if err := requirePathInside(options.StorageRoot, currentPath); err != nil {
		return ScheduleAFactManifest{}, err
	}
	lockPath := filepath.Join(options.StorageRoot, basePath, ".publish-"+occurrenceManifest.Cycle+".lock")
	unlock, err := lockContext(ctx, lockPath)
	if err != nil {
		return ScheduleAFactManifest{}, err
	}
	defer unlock()

	current, err := readScheduleAFactManifestIfPresent(currentPath)
	if err != nil {
		return ScheduleAFactManifest{}, err
	}
	if current != nil {
		if err := validateScheduleAFactManifest(*current); err != nil {
			return ScheduleAFactManifest{}, fmt.Errorf("invalid current Schedule A fact manifest: %w", err)
		}
		if err := validateScheduleAFactManifestBacking(options.StorageRoot, *current); err != nil {
			return ScheduleAFactManifest{}, fmt.Errorf("validate current Schedule A fact publication: %w", err)
		}
		if current.Cycle != occurrenceManifest.Cycle {
			return ScheduleAFactManifest{}, fmt.Errorf("current Schedule A fact manifest belongs to cycle %s", current.Cycle)
		}
		if current.OccurrenceSetID == occurrenceManifest.OccurrenceSetID && current.NormalizerVersion == ScheduleANormalizerVersion && current.FactSchemaVersion == ScheduleAFactSchemaVersion {
			return *current, nil
		}
	}

	factSetID := digestParts("fec.schedule-a.fact-set.v1", occurrenceManifest.OccurrenceSetID, ScheduleANormalizerVersion, ScheduleAFactSchemaVersion)
	manifestPath := filepath.Join(options.StorageRoot, basePath, "manifests", factSetID+".json")
	if existing, err := readScheduleAFactManifestIfPresent(manifestPath); err != nil {
		return ScheduleAFactManifest{}, err
	} else if existing != nil {
		if err := validateScheduleAFactManifest(*existing); err != nil {
			return ScheduleAFactManifest{}, fmt.Errorf("invalid immutable Schedule A fact manifest: %w", err)
		}
		if existing.FactSetID != factSetID || existing.OccurrenceSetID != occurrenceManifest.OccurrenceSetID || existing.OccurrenceManifestSHA256 != occurrenceManifestSHA256 {
			return ScheduleAFactManifest{}, fmt.Errorf("immutable Schedule A fact manifest collision at %s", manifestPath)
		}
		if err := validateScheduleAFactArtifact(options.StorageRoot, existing.Facts); err != nil {
			return ScheduleAFactManifest{}, err
		}
		if err := writeAtomicJSON(currentPath, *existing); err != nil {
			return ScheduleAFactManifest{}, err
		}
		return *existing, nil
	}
	if output.UncompressedByteCount > ^uint64(0)-options.WorkingMarginBytes {
		return ScheduleAFactManifest{}, fmt.Errorf("Schedule A fact working-space estimate overflows")
	}
	if err := requireOccurrenceStorage(options, output.UncompressedByteCount+options.WorkingMarginBytes); err != nil {
		return ScheduleAFactManifest{}, err
	}
	if options.Progress != nil {
		options.Progress("normalizing Schedule A facts for " + occurrenceManifest.Cycle)
	}
	manifest, err := buildScheduleAFactSet(ctx, occurrenceManifest, occurrenceManifestSHA256, output, runID, factSetID, options)
	if err != nil {
		return ScheduleAFactManifest{}, err
	}
	if err := validateScheduleAFactManifest(manifest); err != nil {
		return ScheduleAFactManifest{}, err
	}
	if err := requireOccurrenceStorage(options, 0); err != nil {
		return ScheduleAFactManifest{}, err
	}
	if err := writeAtomicJSON(manifestPath, manifest); err != nil {
		return ScheduleAFactManifest{}, err
	}
	if err := writeAtomicJSON(currentPath, manifest); err != nil {
		return ScheduleAFactManifest{}, err
	}
	return manifest, nil
}

func buildScheduleAFactSet(ctx context.Context, occurrence Manifest, occurrenceSHA string, output fecrelease.StagedOutput, runID, factSetID string, options Options) (ScheduleAFactManifest, error) {
	basePath := scheduleAFactBase()
	temporaryDirectory := filepath.Join(options.StorageRoot, basePath, "staging", factSetID, runID)
	if err := os.MkdirAll(temporaryDirectory, 0o750); err != nil {
		return ScheduleAFactManifest{}, err
	}
	defer func() { _ = os.RemoveAll(temporaryDirectory) }()
	selectedRows, err := loadScheduleAUniqueRows(ctx, options.StorageRoot, occurrence)
	if err != nil {
		return ScheduleAFactManifest{}, err
	}
	factsWriter, err := newArtifactWriterAt(ctx, options.StorageRoot, temporaryDirectory, basePath, "facts")
	if err != nil {
		return ScheduleAFactManifest{}, err
	}
	defer factsWriter.Abort()
	counts, err := streamScheduleAFacts(ctx, occurrence, output, options.StorageRoot, selectedRows, factsWriter, options.Progress)
	if err != nil {
		return ScheduleAFactManifest{}, err
	}
	factsArtifact, err := factsWriter.Finalize()
	if err != nil {
		return ScheduleAFactManifest{}, err
	}
	manifest := ScheduleAFactManifest{
		Schema: "manifest.schema.json", SchemaVersion: ScheduleAFactSetSchemaVersion,
		FactSetID: factSetID, FactType: ScheduleAFactType, Cycle: occurrence.Cycle, SourceContract: ScheduleASourceContract,
		SourceReleaseID: occurrence.SourceReleaseID, SourceReleaseManifestSHA256: occurrence.SourceReleaseManifestSHA256,
		OccurrenceSetID: occurrence.OccurrenceSetID, OccurrenceManifestSHA256: occurrenceSHA,
		RunID: runID, State: "published", NormalizerVersion: ScheduleANormalizerVersion,
		FactSchemaVersion: ScheduleAFactSchemaVersion, PublishedAt: options.Clock().UTC(), Counts: counts, Facts: factsArtifact,
	}
	manifest.Checks = []Check{
		{ID: "occurrence_lineage", Passed: true, Severity: "block", Detail: "fact set names an immutable validated Schedule A occurrence set"},
		{ID: "selected_output_integrity", Passed: true, Severity: "block", Detail: "normalization replay matched the exact staged source bytes"},
		{ID: "unique_projection", Passed: counts.Facts == occurrence.Counts.UniqueKeys, Severity: "block", Detail: "each unique valid SUB_ID produced one fact"},
		{ID: "occurrence_conservation", Passed: counts.SourceOccurrences == counts.Facts+counts.ExcludedOccurrences, Severity: "block", Detail: "source occurrences are partitioned into facts and explicit exclusions"},
		{ID: "fact_state_conservation", Passed: counts.Facts == counts.ValidFacts+counts.InvalidFacts, Severity: "block", Detail: "every normalized fact has an explicit typed state"},
	}
	return manifest, nil
}

func loadScheduleAUniqueRows(ctx context.Context, storageRoot string, occurrence Manifest) ([]byte, error) {
	if occurrence.Counts.Total > ^uint64(0)-7 {
		return nil, fmt.Errorf("Schedule A occurrence count overflows row-selection size")
	}
	byteCount := (occurrence.Counts.Total + 7) / 8
	maxInt := uint64(^uint(0) >> 1)
	if byteCount > maxInt {
		return nil, fmt.Errorf("Schedule A row-selection bitmap exceeds platform capacity")
	}
	selected := make([]byte, int(byteCount))
	reader, err := openArtifactJSON(ctx, storageRoot, occurrence.Artifacts.NaturalIndex)
	if err != nil {
		return nil, err
	}
	var unique uint64
	for {
		entry, ok, err := reader.Next()
		if err != nil {
			_ = reader.Close()
			return nil, err
		}
		if !ok {
			break
		}
		if entry.State != "unique" {
			continue
		}
		if entry.RowOrdinal == 0 || entry.RowOrdinal > occurrence.Counts.Total {
			_ = reader.Close()
			return nil, fmt.Errorf("Schedule A natural index row ordinal %d is out of range", entry.RowOrdinal)
		}
		expectedOccurrenceID := occurrenceID(occurrence.SourceArtifactSHA256, occurrence.Relation, occurrence.Cycle, entry.RowOrdinal)
		if entry.OccurrenceID != expectedOccurrenceID {
			_ = reader.Close()
			return nil, fmt.Errorf("Schedule A natural index occurrence identity does not match row %d", entry.RowOrdinal)
		}
		index := entry.RowOrdinal - 1
		mask := byte(1 << (index % 8))
		if selected[index/8]&mask != 0 {
			_ = reader.Close()
			return nil, fmt.Errorf("Schedule A natural index selects row %d more than once", entry.RowOrdinal)
		}
		selected[index/8] |= mask
		unique++
	}
	if err := reader.Close(); err != nil {
		return nil, err
	}
	if unique != occurrence.Counts.UniqueKeys {
		return nil, fmt.Errorf("Schedule A natural index selected %d unique rows; want %d", unique, occurrence.Counts.UniqueKeys)
	}
	return selected, nil
}

func streamScheduleAFacts(ctx context.Context, occurrence Manifest, output fecrelease.StagedOutput, storageRoot string, selectedRows []byte, writer *artifactWriter, progress func(string)) (ScheduleAFactCounts, error) {
	stagedPath, err := resolveStorageKey(storageRoot, output.StorageKey)
	if err != nil {
		return ScheduleAFactCounts{}, err
	}
	file, err := os.Open(stagedPath)
	if err != nil {
		return ScheduleAFactCounts{}, err
	}
	defer func() { _ = file.Close() }()
	compressed := &countingHashReader{reader: &contextReader{ctx: ctx, reader: file}, hash: sha256.New()}
	zstdDecoder, err := zstd.NewReader(compressed, zstd.WithDecoderConcurrency(2), zstd.WithDecoderLowmem(true), zstd.WithDecoderMaxMemory(64<<20))
	if err != nil {
		return ScheduleAFactCounts{}, err
	}
	defer zstdDecoder.Close()
	uncompressed := &countingHashReader{reader: zstdDecoder, hash: sha256.New()}
	rowDecoder := schedulea.NewDecoder(uncompressed)
	counts := ScheduleAFactCounts{
		SourceOccurrences: occurrence.Counts.Total, ExcludedOccurrences: occurrence.Counts.Total - occurrence.Counts.UniqueKeys,
		SourceInvalid: occurrence.Counts.Invalid, SourceDuplicates: occurrence.Counts.DuplicateOccurrences,
	}
	for rowDecoder.Scan() {
		row := rowDecoder.Row()
		index := row.Number() - 1
		if index/8 >= uint64(len(selectedRows)) || selectedRows[index/8]&(1<<(index%8)) == 0 {
			continue
		}
		record, err := schedulea.Freeze(row, occurrence.Cycle)
		if err != nil {
			return counts, fmt.Errorf("selected Schedule A row %d is invalid: %w", row.Number(), err)
		}
		subID, _ := record.ValueByName("sub_id")
		natural := naturalKey(occurrence.Cycle, subID.Lexeme)
		rawDigestBytes := sha256.Sum256(row.Raw())
		rawDigest := hex.EncodeToString(rawDigestBytes[:])
		occurrenceIdentifier := occurrenceID(occurrence.SourceArtifactSHA256, occurrence.Relation, occurrence.Cycle, row.Number())
		recordVersion := recordVersionID(occurrence.Cycle, natural, rawDigest)
		typed, issueCodes, err := normalizeScheduleAReceipt(record)
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
		fact := ScheduleAFact{
			SchemaVersion: ScheduleAFactSchemaVersion,
			FactID:        digestParts("fec.schedule-a.fact.v1", recordVersion, ScheduleANormalizerVersion, ScheduleAFactSchemaVersion),
			FactType:      ScheduleAFactType, Cycle: occurrence.Cycle, NaturalKey: natural,
			OccurrenceSetID: occurrence.OccurrenceSetID, OccurrenceID: occurrenceIdentifier, RecordVersionID: recordVersion,
			SourceReleaseID: occurrence.SourceReleaseID, SourceContract: ScheduleASourceContract,
			State: state, IssueCodes: issueCodes, SourceFields: record.CanonicalJSON(), TypedFields: typed,
		}
		if err := writer.WriteJSON(fact); err != nil {
			return counts, err
		}
		counts.Facts++
		if progress != nil && counts.Facts%1_000_000 == 0 {
			progress(fmt.Sprintf("normalized %d Schedule A facts for %s", counts.Facts, occurrence.Cycle))
		}
		if counts.Facts&0x3fff == 0 {
			if err := ctx.Err(); err != nil {
				return counts, err
			}
		}
	}
	if err := rowDecoder.Err(); err != nil {
		return counts, err
	}
	if counts.Facts != occurrence.Counts.UniqueKeys {
		return counts, fmt.Errorf("Schedule A fact projection selected %d unique rows; want %d", counts.Facts, occurrence.Counts.UniqueKeys)
	}
	if compressed.bytes != output.CompressedByteCount || hex.EncodeToString(compressed.hash.Sum(nil)) != output.CompressedSHA256 {
		return counts, fmt.Errorf("Schedule A fact replay compressed identity mismatch")
	}
	if uncompressed.bytes != output.UncompressedByteCount || hex.EncodeToString(uncompressed.hash.Sum(nil)) != output.UncompressedSHA256 {
		return counts, fmt.Errorf("Schedule A fact replay uncompressed identity mismatch")
	}
	return counts, nil
}

func scheduleAFactBase() string {
	return filepath.Join("facts", "fec", "schedule-a")
}

func readScheduleAManifestWithSHA256(path string) (Manifest, string, error) {
	content, err := os.ReadFile(path)
	if err != nil {
		return Manifest{}, "", err
	}
	var manifest Manifest
	decoder := json.NewDecoder(strings.NewReader(string(content)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&manifest); err != nil {
		return Manifest{}, "", err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return Manifest{}, "", fmt.Errorf("multiple JSON values in Schedule A occurrence manifest")
		}
		return Manifest{}, "", err
	}
	digest := sha256.Sum256(content)
	return manifest, hex.EncodeToString(digest[:]), nil
}

func readScheduleAFactManifestIfPresent(path string) (*ScheduleAFactManifest, error) {
	content, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var manifest ScheduleAFactManifest
	decoder := json.NewDecoder(strings.NewReader(string(content)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&manifest); err != nil {
		return nil, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return nil, fmt.Errorf("multiple JSON values in Schedule A fact manifest")
		}
		return nil, err
	}
	return &manifest, nil
}

func validateScheduleAFactManifest(manifest ScheduleAFactManifest) error {
	if manifest.Schema != "manifest.schema.json" || manifest.SchemaVersion != ScheduleAFactSetSchemaVersion || manifest.FactType != ScheduleAFactType {
		return fmt.Errorf("unexpected Schedule A fact manifest schema")
	}
	if manifest.SourceContract != ScheduleASourceContract || !validCycle(manifest.Cycle) {
		return fmt.Errorf("Schedule A fact identity is inconsistent")
	}
	for _, digest := range []string{manifest.FactSetID, manifest.SourceReleaseManifestSHA256, manifest.OccurrenceSetID, manifest.OccurrenceManifestSHA256} {
		if !digestPattern.MatchString(digest) {
			return fmt.Errorf("Schedule A fact manifest contains an invalid digest")
		}
	}
	if !releaseIDPattern.MatchString(manifest.SourceReleaseID) || !fecrelease.ValidAcquisitionRunID(manifest.RunID) || manifest.State != "published" || manifest.PublishedAt.IsZero() {
		return fmt.Errorf("Schedule A fact publication identity is incomplete")
	}
	if manifest.NormalizerVersion != ScheduleANormalizerVersion || manifest.FactSchemaVersion != ScheduleAFactSchemaVersion {
		return fmt.Errorf("Schedule A fact parser contract is unsupported")
	}
	expected := digestParts("fec.schedule-a.fact-set.v1", manifest.OccurrenceSetID, manifest.NormalizerVersion, manifest.FactSchemaVersion)
	if manifest.FactSetID != expected {
		return fmt.Errorf("Schedule A fact-set ID does not match canonical inputs")
	}
	if manifest.Facts.Compression != "zstd" || !digestPattern.MatchString(manifest.Facts.CompressedSHA256) || !digestPattern.MatchString(manifest.Facts.UncompressedSHA256) || manifest.Facts.CompressedBytes == 0 || manifest.Facts.RecordCount != manifest.Counts.Facts {
		return fmt.Errorf("Schedule A fact artifact identity is invalid")
	}
	prefix := filepath.ToSlash(filepath.Join(scheduleAFactBase(), "facts", "sha256", manifest.Facts.CompressedSHA256[:2])) + "/"
	if !strings.HasPrefix(manifest.Facts.StorageKey, prefix) {
		return fmt.Errorf("Schedule A fact artifact storage key is not canonical")
	}
	if manifest.Counts.SourceOccurrences != manifest.Counts.Facts+manifest.Counts.ExcludedOccurrences || manifest.Counts.Facts != manifest.Counts.ValidFacts+manifest.Counts.InvalidFacts {
		return fmt.Errorf("Schedule A fact counts are not conserved")
	}
	if len(manifest.Checks) < 5 {
		return fmt.Errorf("Schedule A fact manifest is missing required checks")
	}
	for _, check := range manifest.Checks {
		if check.Severity == "block" && !check.Passed {
			return fmt.Errorf("blocking Schedule A fact check %s failed", check.ID)
		}
	}
	return nil
}

func validateScheduleAFactManifestBacking(storageRoot string, current ScheduleAFactManifest) error {
	immutablePath := filepath.Join(storageRoot, scheduleAFactBase(), "manifests", current.FactSetID+".json")
	immutable, err := readScheduleAFactManifestIfPresent(immutablePath)
	if err != nil {
		return err
	}
	if immutable == nil {
		return fmt.Errorf("immutable Schedule A fact manifest is missing")
	}
	if err := validateScheduleAFactManifest(*immutable); err != nil {
		return err
	}
	if !reflect.DeepEqual(current, *immutable) {
		return fmt.Errorf("active Schedule A fact pointer differs from its immutable manifest")
	}
	return validateScheduleAFactArtifact(storageRoot, current.Facts)
}

func validateScheduleAFactArtifact(storageRoot string, artifact Artifact) error {
	path, err := resolveStorageKey(storageRoot, artifact.StorageKey)
	if err != nil {
		return err
	}
	info, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("Schedule A facts artifact: %w", err)
	}
	if !info.Mode().IsRegular() || info.Size() < 0 || uint64(info.Size()) != artifact.CompressedBytes {
		return fmt.Errorf("Schedule A facts artifact does not match its published size")
	}
	return nil
}
