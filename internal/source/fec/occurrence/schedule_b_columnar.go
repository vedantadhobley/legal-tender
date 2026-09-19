package occurrence

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"time"

	fecrelease "github.com/vedantadhobley/legal-tender/internal/source/fec/release"
	"github.com/vedantadhobley/legal-tender/internal/source/fec/scheduleb"
	"github.com/vedantadhobley/legal-tender/internal/source/fec/schedulebparquet"
)

const (
	defaultScheduleBColumnarRowsPerShard    = 1_000_000
	defaultScheduleBColumnarRowsPerRowGroup = 128_000
)

// PublishScheduleBColumnarFacts streams one cycle relation directly from the
// release-owned PostgreSQL archive into deterministic Parquet shards. It
// publishes no pointer until source validation, SUB_ID uniqueness, source-byte
// replay, and Parquet semantic readback all pass.
func PublishScheduleBColumnarFacts(
	ctx context.Context,
	releaseManifest fecrelease.ReleaseManifest,
	releaseManifestSHA256 string,
	cycle string,
	runID string,
	options ScheduleBColumnarOptions,
) (ScheduleBColumnarManifest, error) {
	if options.Clock == nil {
		options.Clock = time.Now
	}
	if options.RowsPerShard == 0 {
		options.RowsPerShard = defaultScheduleBColumnarRowsPerShard
	}
	if options.RowsPerRowGroup == 0 {
		options.RowsPerRowGroup = defaultScheduleBColumnarRowsPerRowGroup
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
	if options.PGRestorePath == "" {
		options.PGRestorePath = "pg_restore"
	}
	if options.StorageRoot == "" {
		return ScheduleBColumnarManifest{}, fmt.Errorf("storage root is required")
	}
	if options.WorkDir == "" {
		options.WorkDir = filepath.Join(options.StorageRoot, "cache", "fec", "schedule-b")
	}
	if options.RowsPerRowGroup > options.RowsPerShard || options.RowsPerShard > math.MaxInt64 || options.RowsPerRowGroup > math.MaxInt64 {
		return ScheduleBColumnarManifest{}, fmt.Errorf("Schedule B columnar row boundaries are invalid")
	}
	if !validCycle(cycle) {
		return ScheduleBColumnarManifest{}, fmt.Errorf("cycle must be a four-digit even year")
	}
	if !fecrelease.ValidAcquisitionRunID(runID) {
		return ScheduleBColumnarManifest{}, fmt.Errorf("run ID contains unsupported characters")
	}
	if !digestPattern.MatchString(releaseManifestSHA256) {
		return ScheduleBColumnarManifest{}, fmt.Errorf("source release manifest SHA-256 is invalid")
	}
	if issues := fecrelease.ValidateKnownManifest(releaseManifest); len(issues) != 0 {
		return ScheduleBColumnarManifest{}, fmt.Errorf("invalid source release: %s", issues[0].Message)
	}

	artifact, relation, err := selectedScheduleBArtifact(releaseManifest, cycle)
	if err != nil {
		return ScheduleBColumnarManifest{}, err
	}
	physicalSchema, err := schedulebparquet.NewSchema()
	if err != nil {
		return ScheduleBColumnarManifest{}, err
	}
	configuration := ScheduleBColumnarConfiguration{
		RowsPerShard: options.RowsPerShard, RowsPerRowGroup: options.RowsPerRowGroup,
		ColumnCount: physicalSchema.ColumnCount(), Compression: "parquet-zstd",
		Locator: "source artifact plus selected relation, cycle, one-based row ordinal, raw byte offset, and raw byte length",
	}
	factSetID := scheduleBColumnarFactSetID(artifact.SHA256, relation, cycle, configuration)
	basePath := scheduleBColumnarBase()
	currentPath := options.CurrentManifestPath
	if currentPath == "" {
		currentPath = filepath.Join(options.StorageRoot, basePath, "current", cycle+".json")
	}
	if err := requirePathInside(options.StorageRoot, currentPath); err != nil {
		return ScheduleBColumnarManifest{}, err
	}
	lockPath := filepath.Join(options.StorageRoot, basePath, ".publish-"+cycle+".lock")
	unlock, err := lockContext(ctx, lockPath)
	if err != nil {
		return ScheduleBColumnarManifest{}, err
	}
	defer unlock()

	current, err := readScheduleBColumnarManifestIfPresent(currentPath)
	if err != nil {
		return ScheduleBColumnarManifest{}, err
	}
	if current != nil {
		if err := validateScheduleBColumnarManifest(*current); err != nil {
			return ScheduleBColumnarManifest{}, fmt.Errorf("invalid current Schedule B columnar manifest: %w", err)
		}
		if err := validateScheduleBColumnarManifestBacking(options.StorageRoot, *current, true); err != nil {
			return ScheduleBColumnarManifest{}, fmt.Errorf("validate current Schedule B columnar publication: %w", err)
		}
		if current.Cycle != cycle {
			return ScheduleBColumnarManifest{}, fmt.Errorf("current Schedule B columnar manifest belongs to cycle %s", current.Cycle)
		}
		if scheduleBColumnarManifestMatchesSource(*current, artifact, relation, cycle, configuration) {
			descends, err := releaseDescendsFrom(ctx, options.StorageRoot, releaseManifest, current.SourceReleaseID)
			if err != nil {
				return ScheduleBColumnarManifest{}, err
			}
			if !descends {
				return ScheduleBColumnarManifest{}, fmt.Errorf("source release %s does not descend from Schedule B fact release %s", releaseManifest.ReleaseID, current.SourceReleaseID)
			}
			if current.FactSetID == factSetID {
				return *current, nil
			}
			adopted := *current
			adopted.FactSetID = factSetID
			adopted.RunID = runID
			adopted.PublishedAt = options.Clock().UTC()
			adopted.Checks = scheduleBColumnarChecks(adopted)
			manifestPath := filepath.Join(options.StorageRoot, basePath, "manifests", factSetID+".json")
			if err := validateScheduleBColumnarManifest(adopted); err != nil {
				return ScheduleBColumnarManifest{}, fmt.Errorf("validate adopted Schedule B columnar publication: %w", err)
			}
			if err := writeAtomicJSON(manifestPath, adopted); err != nil {
				return ScheduleBColumnarManifest{}, err
			}
			if err := writeAtomicJSON(currentPath, adopted); err != nil {
				return ScheduleBColumnarManifest{}, err
			}
			if options.Progress != nil {
				options.Progress("adopted verified Schedule B columnar shards under source-stable identity for " + cycle)
			}
			return adopted, nil
		}
	}

	manifestPath := filepath.Join(options.StorageRoot, basePath, "manifests", factSetID+".json")
	if existing, err := readScheduleBColumnarManifestIfPresent(manifestPath); err != nil {
		return ScheduleBColumnarManifest{}, err
	} else if existing != nil {
		if err := validateScheduleBColumnarManifest(*existing); err != nil {
			return ScheduleBColumnarManifest{}, fmt.Errorf("invalid immutable Schedule B columnar manifest: %w", err)
		}
		if err := validateScheduleBColumnarManifestBacking(options.StorageRoot, *existing, true); err != nil {
			return ScheduleBColumnarManifest{}, err
		}
		if !scheduleBColumnarManifestMatchesSource(*existing, artifact, relation, cycle, configuration) {
			return ScheduleBColumnarManifest{}, fmt.Errorf("immutable Schedule B columnar manifest collision at %s", manifestPath)
		}
		descends, err := releaseDescendsFrom(ctx, options.StorageRoot, releaseManifest, existing.SourceReleaseID)
		if err != nil {
			return ScheduleBColumnarManifest{}, err
		}
		if !descends {
			return ScheduleBColumnarManifest{}, fmt.Errorf("source release %s does not descend from Schedule B fact release %s", releaseManifest.ReleaseID, existing.SourceReleaseID)
		}
		if err := writeAtomicJSON(currentPath, *existing); err != nil {
			return ScheduleBColumnarManifest{}, err
		}
		return *existing, nil
	}

	archivePath, err := resolveStorageKey(options.StorageRoot, artifact.StorageKey)
	if err != nil {
		return ScheduleBColumnarManifest{}, err
	}
	if err := validateScheduleBArchive(ctx, archivePath, artifact); err != nil {
		return ScheduleBColumnarManifest{}, err
	}
	if err := requireScheduleBColumnarStorage(options, uint64(artifact.ByteCount)+options.WorkingMarginBytes); err != nil {
		return ScheduleBColumnarManifest{}, err
	}
	if err := os.MkdirAll(options.WorkDir, 0o750); err != nil {
		return ScheduleBColumnarManifest{}, err
	}
	if options.Progress != nil {
		options.Progress("publishing Schedule B columnar facts for " + cycle)
	}
	manifest, err := buildScheduleBColumnarFactSet(ctx, releaseManifest, releaseManifestSHA256, artifact, archivePath, relation, cycle, runID, factSetID, configuration, physicalSchema, options)
	if err != nil {
		return ScheduleBColumnarManifest{}, err
	}
	if err := validateScheduleBColumnarManifest(manifest); err != nil {
		return ScheduleBColumnarManifest{}, err
	}
	if err := writeAtomicJSON(manifestPath, manifest); err != nil {
		return ScheduleBColumnarManifest{}, err
	}
	if err := writeAtomicJSON(currentPath, manifest); err != nil {
		return ScheduleBColumnarManifest{}, err
	}
	return manifest, nil
}

func buildScheduleBColumnarFactSet(
	ctx context.Context,
	releaseManifest fecrelease.ReleaseManifest,
	releaseManifestSHA string,
	artifact fecrelease.PublishedArtifact,
	archivePath, relation, cycle, runID, factSetID string,
	configuration ScheduleBColumnarConfiguration,
	physicalSchema *schedulebparquet.Schema,
	options ScheduleBColumnarOptions,
) (ScheduleBColumnarManifest, error) {
	stagingDirectory := filepath.Join(options.StorageRoot, scheduleBColumnarBase(), "staging", factSetID)
	if err := os.MkdirAll(stagingDirectory, 0o750); err != nil {
		return ScheduleBColumnarManifest{}, err
	}
	checkpointPath := filepath.Join(stagingDirectory, "checkpoint.json")
	checkpoint, err := readScheduleBColumnarCheckpoint(checkpointPath, factSetID, cycle, artifact.SHA256, configuration)
	if err != nil {
		return ScheduleBColumnarManifest{}, err
	}
	checkpoint.RunID = runID
	if err := validateScheduleBColumnarShardFiles(options.StorageRoot, checkpoint.Shards, true); err != nil {
		return ScheduleBColumnarManifest{}, fmt.Errorf("validate Schedule B columnar checkpoint: %w", err)
	}

	shards := append([]ScheduleBColumnarShard(nil), checkpoint.Shards...)
	globalSemantic := schedulebparquet.NewSemanticHasher()
	var activeWriter *scheduleBColumnarShardWriter
	var activeExpected *ScheduleBColumnarShard
	var activeSemantic *schedulebparquet.SemanticHasher
	var activeIndex, activeFirstOrdinal, activeFirstOffset, activeRows, activeFacts uint64
	var rawOffset, lastOrdinal uint64
	var haveActive bool

	finishActive := func(last uint64) error {
		if !haveActive {
			return nil
		}
		if activeExpected != nil {
			expected := *activeExpected
			if expected.Index != activeIndex || expected.FirstSourceRowOrdinal != activeFirstOrdinal || expected.LastSourceRowOrdinal != last ||
				expected.FirstRawByteOffset != activeFirstOffset || expected.LastRawByteEnd != rawOffset || expected.SourceRows != activeRows ||
				expected.Facts != activeFacts || expected.SemanticSHA256 != activeSemantic.Sum() {
				return fmt.Errorf("Schedule B columnar checkpoint shard %d does not match source replay", activeIndex)
			}
		} else {
			descriptor, err := activeWriter.Close(last, rawOffset, activeRows)
			if err != nil {
				return err
			}
			activeWriter = nil
			if descriptor.Facts != activeFacts || descriptor.SemanticSHA256 != activeSemantic.Sum() {
				return fmt.Errorf("Schedule B columnar shard %d does not match source replay", activeIndex)
			}
			shards = append(shards, descriptor)
			checkpoint.Shards = append([]ScheduleBColumnarShard(nil), shards...)
			checkpoint.UpdatedAt = options.Clock().UTC()
			if err := writeAtomicJSON(checkpointPath, checkpoint); err != nil {
				return err
			}
			if err := requireScheduleBColumnarStorage(options, options.WorkingMarginBytes); err != nil {
				return err
			}
			if options.Progress != nil {
				options.Progress(fmt.Sprintf("published Schedule B columnar shard %d for %s", descriptor.Index, cycle))
			}
		}
		haveActive = false
		activeExpected = nil
		return nil
	}

	extractionContext, cancelExtraction := context.WithCancel(ctx)
	defer cancelExtraction()
	reader, writer := io.Pipe()
	type extractionResult struct {
		rows uint64
		err  error
	}
	extractionDone := make(chan extractionResult, 1)
	go func() {
		rows, extractErr := fecrelease.ExtractRelation(extractionContext, options.PGRestorePath, archivePath, relation, cycle, writer)
		_ = writer.CloseWithError(extractErr)
		extractionDone <- extractionResult{rows: rows, err: extractErr}
		close(extractionDone)
	}()

	verification, verifyErr := scheduleb.Verify(ctx, reader, scheduleb.VerifyOptions{
		ExpectedPeriod: cycle,
		WorkDir:        options.WorkDir,
		Progress:       options.Progress,
		ObserveRow: func(row *scheduleb.Row, validationErr error) error {
			lastOrdinal = row.Number()
			shardIndex := (row.Number() - 1) / configuration.RowsPerShard
			if !haveActive || shardIndex != activeIndex {
				if haveActive {
					if err := finishActive(row.Number() - 1); err != nil {
						return err
					}
				}
				activeIndex, activeFirstOrdinal, activeFirstOffset = shardIndex, row.Number(), rawOffset
				activeRows, activeFacts = 0, 0
				activeSemantic = schedulebparquet.NewSemanticHasher()
				haveActive = true
				if shardIndex < uint64(len(checkpoint.Shards)) {
					activeExpected = &checkpoint.Shards[shardIndex]
				} else {
					activeWriter, err = newScheduleBColumnarShardWriter(ctx, options.StorageRoot, stagingDirectory, shardIndex, row.Number(), rawOffset, configuration.RowsPerRowGroup, physicalSchema)
					if err != nil {
						return err
					}
				}
			}
			activeRows++
			metadata := schedulebparquet.Metadata{SourceRowOrdinal: row.Number(), SourceRawByteOffset: rawOffset, SourceRawByteLength: uint64(len(row.Raw()))}
			if validationErr == nil {
				record, err := scheduleb.Freeze(row, cycle)
				if err != nil {
					return err
				}
				derived, err := scheduleBColumnarDerived(record)
				if err != nil {
					return err
				}
				if err := globalSemantic.AddSource(row, metadata); err != nil {
					return err
				}
				if err := activeSemantic.AddSource(row, metadata); err != nil {
					return err
				}
				if activeWriter != nil {
					if err := activeWriter.Write(row, metadata, derived); err != nil {
						return err
					}
				}
				activeFacts++
			}
			rawOffset += uint64(len(row.Raw()))
			return nil
		},
	})
	if verifyErr != nil {
		cancelExtraction()
		_ = reader.CloseWithError(verifyErr)
		if activeWriter != nil {
			activeWriter.Abort()
		}
		<-extractionDone
		return ScheduleBColumnarManifest{}, verifyErr
	}
	if err := finishActive(lastOrdinal); err != nil {
		cancelExtraction()
		_ = reader.CloseWithError(err)
		if activeWriter != nil {
			activeWriter.Abort()
		}
		<-extractionDone
		return ScheduleBColumnarManifest{}, err
	}
	extraction := <-extractionDone
	if extraction.err != nil {
		return ScheduleBColumnarManifest{}, extraction.err
	}
	if extraction.rows != lastOrdinal {
		return ScheduleBColumnarManifest{}, fmt.Errorf("pg_restore emitted %d rows but publisher observed %d", extraction.rows, lastOrdinal)
	}
	if !verification.Complete || verification.Rows == 0 || verification.Rows != lastOrdinal || verification.ValidRows != verification.Rows ||
		verification.InvalidRows != 0 || verification.UniqueSubIDs != verification.Rows || verification.DuplicateSubIDRows != 0 ||
		verification.Bytes != rawOffset || globalSemantic.Rows() != verification.Rows {
		return ScheduleBColumnarManifest{}, fmt.Errorf("Schedule B source verification did not conserve a complete unique fact set")
	}

	manifest := ScheduleBColumnarManifest{
		Schema: "manifest.schema.json", SchemaVersion: ScheduleBColumnarFactSetSchemaVersion,
		FactSetID: factSetID, FactType: ScheduleBFactType, Cycle: cycle, Relation: relation, SourceContract: ScheduleBSourceContract,
		SourceReleaseID: releaseManifest.ReleaseID, SourceReleaseManifestSHA256: releaseManifestSHA,
		SourceArtifactSHA256: artifact.SHA256, SourceArtifactByteCount: artifact.ByteCount, SourceArtifactStorageKey: artifact.StorageKey,
		RunID: runID, State: "published", PhysicalSchemaVersion: schedulebparquet.PhysicalSchemaVersion,
		PublisherVersion: ScheduleBColumnarPublisherVersion, ParquetLibrary: schedulebparquet.LibraryVersion,
		PublishedAt: options.Clock().UTC(), Configuration: configuration,
		Counts:       ScheduleBColumnarCounts{SourceRows: verification.Rows, Facts: verification.Rows, ValidFacts: verification.ValidRows, InvalidSourceRows: verification.InvalidRows, UniqueSubIDs: verification.UniqueSubIDs, DuplicateSubIDs: verification.DuplicateSubIDRows},
		SourceReplay: ScheduleBColumnarSourceReplay{ArchiveBytes: artifact.ByteCount, ArchiveSHA256: artifact.SHA256, COPYRows: verification.Rows, COPYBytes: verification.Bytes, COPYSHA256: verification.SHA256, SemanticSHA256: globalSemantic.Sum()},
		Shards:       shards,
	}
	manifest.Checks = scheduleBColumnarChecks(manifest)
	if err := os.Remove(checkpointPath); err != nil && !os.IsNotExist(err) {
		return ScheduleBColumnarManifest{}, err
	}
	_ = os.Remove(stagingDirectory)
	return manifest, nil
}

func selectedScheduleBArtifact(manifest fecrelease.ReleaseManifest, cycle string) (fecrelease.PublishedArtifact, string, error) {
	inventory, known := fecrelease.InventoryForVersion(manifest.InventoryVersion)
	if !known {
		return fecrelease.PublishedArtifact{}, "", fmt.Errorf("source release inventory is unknown")
	}
	var relation string
	for _, source := range inventory.Sources {
		if source.SourceID != fecrelease.ScheduleBSourceID {
			continue
		}
		for _, selection := range source.RelationSelections {
			if selection.Scope == cycle && selection.Materialization == fecrelease.RelationMaterializationArchiveDirect {
				relation = selection.Name
			}
		}
	}
	if relation == "" {
		return fecrelease.PublishedArtifact{}, "", fmt.Errorf("source release does not select an archive-direct Schedule B relation for %s", cycle)
	}
	for _, output := range manifest.StagedOutputs {
		if output.SourceID == fecrelease.ScheduleBSourceID {
			return fecrelease.PublishedArtifact{}, "", fmt.Errorf("Schedule B archive-direct source unexpectedly has a staged COPY output")
		}
	}
	for _, artifact := range manifest.Artifacts {
		if artifact.SourceID == fecrelease.ScheduleBSourceID {
			return artifact, relation, nil
		}
	}
	return fecrelease.PublishedArtifact{}, "", fmt.Errorf("source release has no Schedule B artifact")
}

func validateScheduleBArchive(ctx context.Context, path string, artifact fecrelease.PublishedArtifact) error {
	info, err := os.Stat(path)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() || info.Size() != artifact.ByteCount {
		return fmt.Errorf("Schedule B archive does not match its published size")
	}
	digest, err := scheduleBColumnarFileSHA256(ctx, path)
	if err != nil {
		return err
	}
	if digest != artifact.SHA256 {
		return fmt.Errorf("Schedule B archive does not match its published digest")
	}
	return nil
}

func scheduleBColumnarFactSetID(artifactSHA, relation, cycle string, configuration ScheduleBColumnarConfiguration) string {
	return digestParts("fec.schedule-b.columnar-fact-set.v1", artifactSHA, relation, cycle,
		schedulebparquet.PhysicalSchemaVersion, ScheduleBColumnarPublisherVersion,
		strconv.FormatUint(configuration.RowsPerShard, 10), strconv.FormatUint(configuration.RowsPerRowGroup, 10))
}

func legacyScheduleBColumnarFactSetID(releaseID, releaseSHA, artifactSHA, relation, cycle string, configuration ScheduleBColumnarConfiguration) string {
	return digestParts("fec.schedule-b.columnar-fact-set.v1", releaseID, releaseSHA, artifactSHA, relation, cycle,
		schedulebparquet.PhysicalSchemaVersion, ScheduleBColumnarPublisherVersion,
		strconv.FormatUint(configuration.RowsPerShard, 10), strconv.FormatUint(configuration.RowsPerRowGroup, 10))
}

func scheduleBColumnarManifestMatchesSource(
	manifest ScheduleBColumnarManifest,
	artifact fecrelease.PublishedArtifact,
	relation, cycle string,
	configuration ScheduleBColumnarConfiguration,
) bool {
	return manifest.Cycle == cycle && manifest.Relation == relation && manifest.Configuration == configuration &&
		manifest.SourceArtifactSHA256 == artifact.SHA256 && manifest.SourceArtifactByteCount == artifact.ByteCount &&
		manifest.SourceArtifactStorageKey == artifact.StorageKey && manifest.SourceReplay.ArchiveSHA256 == artifact.SHA256 &&
		manifest.SourceReplay.ArchiveBytes == artifact.ByteCount && manifest.PhysicalSchemaVersion == schedulebparquet.PhysicalSchemaVersion &&
		manifest.PublisherVersion == ScheduleBColumnarPublisherVersion
}

func scheduleBColumnarBase() string { return filepath.Join("facts", "fec", "schedule-b", "columnar") }

func requireScheduleBColumnarStorage(options ScheduleBColumnarOptions, reserve uint64) error {
	available, err := options.DiskAvailable(options.StorageRoot)
	if err != nil {
		return fmt.Errorf("inspect Schedule B columnar storage: %w", err)
	}
	if available < options.FreeFloorBytes || reserve > available-options.FreeFloorBytes {
		return fmt.Errorf("Schedule B columnar publication would leave less than the required free-space floor")
	}
	return nil
}

func scheduleBColumnarChecks(manifest ScheduleBColumnarManifest) []Check {
	var sourceRows, facts, bytes uint64
	for _, shard := range manifest.Shards {
		sourceRows += shard.SourceRows
		facts += shard.Facts
		bytes += shard.Bytes
	}
	return []Check{
		{ID: "release_lineage", Passed: true, Severity: "block", Detail: "fact set names an immutable coordinated release and Schedule B archive"},
		{ID: "archive_integrity", Passed: manifest.SourceArtifactSHA256 == manifest.SourceReplay.ArchiveSHA256, Severity: "block", Detail: "release-owned Schedule B archive size and SHA-256 matched before extraction"},
		{ID: "copy_replay", Passed: manifest.SourceReplay.COPYRows == manifest.Counts.SourceRows && manifest.SourceReplay.COPYBytes != 0, Severity: "block", Detail: "selected relation COPY bytes were fully hashed and counted"},
		{ID: "source_row_conservation", Passed: sourceRows == manifest.Counts.SourceRows && facts == manifest.Counts.Facts, Severity: "block", Detail: "deterministic shard ranges cover every source row exactly once"},
		{ID: "strict_source_validity", Passed: manifest.Counts.InvalidSourceRows == 0 && manifest.Counts.ValidFacts == manifest.Counts.SourceRows, Severity: "block", Detail: "every selected row passed the exact 81-column source contract"},
		{ID: "submission_identity", Passed: manifest.Counts.UniqueSubIDs == manifest.Counts.SourceRows && manifest.Counts.DuplicateSubIDs == 0, Severity: "block", Detail: "SUB_ID is present, positive, and unique across the selected relation"},
		{ID: "parquet_round_trip", Passed: true, Severity: "block", Detail: "every newly written shard matched its source-value semantic digest after Parquet readback"},
		{ID: "immutable_shards", Passed: len(manifest.Shards) != 0 && bytes != 0, Severity: "block", Detail: "all Parquet shards are content addressed and independently retryable"},
	}
}

func readScheduleBColumnarManifestIfPresent(path string) (*ScheduleBColumnarManifest, error) {
	content, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var manifest ScheduleBColumnarManifest
	decoder := json.NewDecoder(strings.NewReader(string(content)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&manifest); err != nil {
		return nil, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return nil, fmt.Errorf("multiple JSON values in Schedule B columnar manifest")
		}
		return nil, err
	}
	return &manifest, nil
}

func validateScheduleBColumnarManifest(manifest ScheduleBColumnarManifest) error {
	if manifest.Schema != "manifest.schema.json" || manifest.SchemaVersion != ScheduleBColumnarFactSetSchemaVersion || manifest.FactType != ScheduleBFactType {
		return fmt.Errorf("unexpected Schedule B columnar manifest schema")
	}
	if manifest.SourceContract != ScheduleBSourceContract || !validCycle(manifest.Cycle) || manifest.Relation == "" || manifest.State != "published" || manifest.PublishedAt.IsZero() {
		return fmt.Errorf("Schedule B columnar publication identity is incomplete")
	}
	for _, digest := range []string{manifest.FactSetID, manifest.SourceReleaseManifestSHA256, manifest.SourceArtifactSHA256, manifest.SourceReplay.ArchiveSHA256, manifest.SourceReplay.COPYSHA256, manifest.SourceReplay.SemanticSHA256} {
		if !digestPattern.MatchString(digest) {
			return fmt.Errorf("Schedule B columnar manifest contains an invalid digest")
		}
	}
	if !releaseIDPattern.MatchString(manifest.SourceReleaseID) || !fecrelease.ValidAcquisitionRunID(manifest.RunID) {
		return fmt.Errorf("Schedule B columnar source or run identity is invalid")
	}
	if manifest.PhysicalSchemaVersion != schedulebparquet.PhysicalSchemaVersion || manifest.PublisherVersion != ScheduleBColumnarPublisherVersion || manifest.ParquetLibrary != schedulebparquet.LibraryVersion {
		return fmt.Errorf("Schedule B columnar physical contract is unsupported")
	}
	if manifest.Configuration.RowsPerShard == 0 || manifest.Configuration.RowsPerRowGroup == 0 || manifest.Configuration.RowsPerRowGroup > manifest.Configuration.RowsPerShard ||
		manifest.Configuration.ColumnCount != scheduleb.FieldCount+17 || manifest.Configuration.Compression != "parquet-zstd" || manifest.Configuration.Locator == "" {
		return fmt.Errorf("Schedule B columnar configuration is invalid")
	}
	expectedID := scheduleBColumnarFactSetID(manifest.SourceArtifactSHA256, manifest.Relation, manifest.Cycle, manifest.Configuration)
	legacyID := legacyScheduleBColumnarFactSetID(manifest.SourceReleaseID, manifest.SourceReleaseManifestSHA256, manifest.SourceArtifactSHA256, manifest.Relation, manifest.Cycle, manifest.Configuration)
	if manifest.FactSetID != expectedID && manifest.FactSetID != legacyID {
		return fmt.Errorf("Schedule B columnar fact-set ID does not match canonical inputs")
	}
	if manifest.SourceArtifactByteCount <= 0 || manifest.SourceReplay.ArchiveBytes != manifest.SourceArtifactByteCount || manifest.SourceReplay.ArchiveSHA256 != manifest.SourceArtifactSHA256 ||
		manifest.Counts.SourceRows == 0 || manifest.Counts.SourceRows != manifest.Counts.Facts || manifest.Counts.ValidFacts != manifest.Counts.Facts || manifest.Counts.InvalidSourceRows != 0 ||
		manifest.Counts.UniqueSubIDs != manifest.Counts.SourceRows || manifest.Counts.DuplicateSubIDs != 0 || manifest.SourceReplay.COPYRows != manifest.Counts.SourceRows || manifest.SourceReplay.COPYBytes == 0 {
		return fmt.Errorf("Schedule B columnar counts are not conserved")
	}
	var sourceRows, facts, priorOrdinal, priorRawEnd uint64
	for index, shard := range manifest.Shards {
		if shard.Index != uint64(index) || shard.FirstSourceRowOrdinal != priorOrdinal+1 || shard.FirstRawByteOffset != priorRawEnd ||
			shard.LastSourceRowOrdinal < shard.FirstSourceRowOrdinal || shard.SourceRows != shard.LastSourceRowOrdinal-shard.FirstSourceRowOrdinal+1 ||
			shard.LastRawByteEnd <= shard.FirstRawByteOffset || shard.Facts != shard.SourceRows || shard.RowGroups == 0 || shard.Bytes == 0 ||
			!digestPattern.MatchString(shard.SHA256) || !digestPattern.MatchString(shard.SemanticSHA256) {
			return fmt.Errorf("Schedule B columnar shard %d is invalid", index)
		}
		expectedKey := filepath.ToSlash(filepath.Join(scheduleBColumnarBase(), "shards", "sha256", shard.SHA256[:2], shard.SHA256+".parquet"))
		if shard.StorageKey != expectedKey {
			return fmt.Errorf("Schedule B columnar shard %d storage key is not canonical", index)
		}
		priorOrdinal, priorRawEnd = shard.LastSourceRowOrdinal, shard.LastRawByteEnd
		sourceRows += shard.SourceRows
		facts += shard.Facts
	}
	if sourceRows != manifest.Counts.SourceRows || facts != manifest.Counts.Facts || priorRawEnd != manifest.SourceReplay.COPYBytes || len(manifest.Checks) < 8 {
		return fmt.Errorf("Schedule B columnar shard totals or checks are not conserved")
	}
	for _, check := range manifest.Checks {
		if check.Severity == "block" && !check.Passed {
			return fmt.Errorf("blocking Schedule B columnar check %s failed", check.ID)
		}
	}
	return nil
}

func validateScheduleBColumnarManifestBacking(storageRoot string, current ScheduleBColumnarManifest, verifyDigest bool) error {
	immutablePath := filepath.Join(storageRoot, scheduleBColumnarBase(), "manifests", current.FactSetID+".json")
	immutable, err := readScheduleBColumnarManifestIfPresent(immutablePath)
	if err != nil {
		return err
	}
	if immutable == nil || !reflect.DeepEqual(current, *immutable) {
		return fmt.Errorf("active Schedule B columnar pointer differs from its immutable manifest")
	}
	return validateScheduleBColumnarShardFiles(storageRoot, current.Shards, verifyDigest)
}

func validateScheduleBColumnarShardFiles(storageRoot string, shards []ScheduleBColumnarShard, verifyDigest bool) error {
	for _, shard := range shards {
		path, err := resolveStorageKey(storageRoot, shard.StorageKey)
		if err != nil {
			return err
		}
		info, err := os.Stat(path)
		if err != nil {
			return fmt.Errorf("Schedule B columnar shard %d: %w", shard.Index, err)
		}
		if !info.Mode().IsRegular() || info.Size() < 0 || uint64(info.Size()) != shard.Bytes {
			return fmt.Errorf("Schedule B columnar shard %d does not match its published size", shard.Index)
		}
		if verifyDigest {
			digest, err := scheduleBColumnarFileSHA256(context.Background(), path)
			if err != nil || digest != shard.SHA256 {
				return fmt.Errorf("Schedule B columnar shard %d does not match its published digest", shard.Index)
			}
		}
	}
	return nil
}

func readScheduleBColumnarCheckpoint(path, factSetID, cycle, artifactSHA string, configuration ScheduleBColumnarConfiguration) (scheduleBColumnarCheckpoint, error) {
	checkpoint := scheduleBColumnarCheckpoint{SchemaVersion: ScheduleBColumnarCheckpointVersion, FactSetID: factSetID, Cycle: cycle, SourceArtifactSHA256: artifactSHA, PhysicalSchemaVersion: schedulebparquet.PhysicalSchemaVersion, PublisherVersion: ScheduleBColumnarPublisherVersion, Configuration: configuration}
	content, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return checkpoint, nil
	}
	if err != nil {
		return checkpoint, err
	}
	decoder := json.NewDecoder(strings.NewReader(string(content)))
	decoder.DisallowUnknownFields()
	var prior scheduleBColumnarCheckpoint
	if err := decoder.Decode(&prior); err != nil {
		return checkpoint, err
	}
	if prior.SchemaVersion != checkpoint.SchemaVersion || prior.FactSetID != factSetID || prior.Cycle != cycle || prior.SourceArtifactSHA256 != artifactSHA || prior.PhysicalSchemaVersion != checkpoint.PhysicalSchemaVersion || prior.PublisherVersion != checkpoint.PublisherVersion || prior.Configuration != configuration {
		return checkpoint, fmt.Errorf("Schedule B columnar checkpoint does not match this publication")
	}
	probe := ScheduleBColumnarManifest{Shards: prior.Shards}
	var priorOrdinal, priorRawEnd uint64
	for index, shard := range probe.Shards {
		if shard.Index != uint64(index) || shard.FirstSourceRowOrdinal != priorOrdinal+1 || shard.FirstRawByteOffset != priorRawEnd || shard.LastSourceRowOrdinal < shard.FirstSourceRowOrdinal || shard.SourceRows != shard.Facts || shard.SemanticSHA256 == "" {
			return checkpoint, fmt.Errorf("Schedule B columnar checkpoint shard %d is invalid", index)
		}
		priorOrdinal, priorRawEnd = shard.LastSourceRowOrdinal, shard.LastRawByteEnd
	}
	return prior, nil
}
