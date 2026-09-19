package occurrence

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
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

	"github.com/klauspost/compress/zstd"
	fecrelease "github.com/vedantadhobley/legal-tender/internal/source/fec/release"
	"github.com/vedantadhobley/legal-tender/internal/source/fec/schedulea"
	"github.com/vedantadhobley/legal-tender/internal/source/fec/scheduleaparquet"
)

const (
	defaultScheduleAColumnarRowsPerShard    = 1_000_000
	defaultScheduleAColumnarRowsPerRowGroup = 128_000
)

// PublishScheduleAColumnarFacts replaces the rejected full-row JSON artifact
// with deterministic, immutable Parquet shards. It publishes no active pointer
// until every source byte, fact, shard, and semantic round trip is conserved.
func PublishScheduleAColumnarFacts(
	ctx context.Context,
	releaseManifest fecrelease.ReleaseManifest,
	releaseManifestSHA256 string,
	occurrenceManifestPath string,
	runID string,
	options Options,
) (ScheduleAColumnarManifest, error) {
	if options.Clock == nil {
		options.Clock = time.Now
	}
	if options.RowsPerColumnarShard == 0 {
		options.RowsPerColumnarShard = defaultScheduleAColumnarRowsPerShard
	}
	if options.RowsPerColumnarRowGroup == 0 {
		options.RowsPerColumnarRowGroup = defaultScheduleAColumnarRowsPerRowGroup
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
		return ScheduleAColumnarManifest{}, fmt.Errorf("storage root and occurrence manifest path are required")
	}
	if options.RowsPerColumnarRowGroup > options.RowsPerColumnarShard {
		return ScheduleAColumnarManifest{}, fmt.Errorf("rows per row group cannot exceed rows per shard")
	}
	if options.RowsPerColumnarShard > math.MaxInt64 || options.RowsPerColumnarRowGroup > math.MaxInt64 {
		return ScheduleAColumnarManifest{}, fmt.Errorf("Schedule A columnar row boundary exceeds the writer range")
	}
	if !fecrelease.ValidAcquisitionRunID(runID) {
		return ScheduleAColumnarManifest{}, fmt.Errorf("run ID contains unsupported characters")
	}
	if !digestPattern.MatchString(releaseManifestSHA256) {
		return ScheduleAColumnarManifest{}, fmt.Errorf("source release manifest SHA-256 is invalid")
	}
	if issues := fecrelease.ValidateKnownManifest(releaseManifest); len(issues) != 0 {
		return ScheduleAColumnarManifest{}, fmt.Errorf("invalid source release: %s", issues[0].Message)
	}

	occurrenceManifest, err := readScheduleAOccurrenceInput(ctx, options.StorageRoot, occurrenceManifestPath)
	if err != nil {
		return ScheduleAColumnarManifest{}, err
	}
	occurrenceManifestSHA256 := occurrenceManifest.ManifestSHA256
	descends, err := releaseDescendsFrom(ctx, options.StorageRoot, releaseManifest, occurrenceManifest.SourceReleaseID)
	if err != nil {
		return ScheduleAColumnarManifest{}, err
	}
	if !descends {
		return ScheduleAColumnarManifest{}, fmt.Errorf("source release %s does not descend from occurrence release %s", releaseManifest.ReleaseID, occurrenceManifest.SourceReleaseID)
	}
	output, sourceSHA, err := selectedScheduleAOutput(releaseManifest, occurrenceManifest.Cycle)
	if err != nil {
		return ScheduleAColumnarManifest{}, err
	}
	if sourceSHA != occurrenceManifest.SourceArtifactSHA256 || output.CompressedSHA256 != occurrenceManifest.StagedOutputSHA256 || output.Selection != occurrenceManifest.Relation {
		return ScheduleAColumnarManifest{}, fmt.Errorf("Schedule A occurrence input does not describe the selected source bytes")
	}
	physicalSchema, err := scheduleaparquet.NewSchema()
	if err != nil {
		return ScheduleAColumnarManifest{}, err
	}
	configuration := ScheduleAColumnarConfiguration{
		RowsPerShard: options.RowsPerColumnarShard, RowsPerRowGroup: options.RowsPerColumnarRowGroup,
		ColumnCount: physicalSchema.ColumnCount(), Compression: "parquet-zstd",
		Locator: "source artifact plus one-based row ordinal, raw byte offset, and raw byte length",
	}
	factSetID := scheduleAColumnarFactSetID(occurrenceManifest.OccurrenceSetID, occurrenceManifestSHA256, configuration)

	basePath := scheduleAColumnarBase()
	currentPath := options.CurrentManifestPath
	if currentPath == "" {
		currentPath = filepath.Join(options.StorageRoot, basePath, "current", occurrenceManifest.Cycle+".json")
	}
	if err := requirePathInside(options.StorageRoot, currentPath); err != nil {
		return ScheduleAColumnarManifest{}, err
	}
	lockPath := filepath.Join(options.StorageRoot, basePath, ".publish-"+occurrenceManifest.Cycle+".lock")
	unlock, err := lockContext(ctx, lockPath)
	if err != nil {
		return ScheduleAColumnarManifest{}, err
	}
	defer unlock()

	current, err := readScheduleAColumnarManifestIfPresent(currentPath)
	if err != nil {
		return ScheduleAColumnarManifest{}, err
	}
	if current != nil {
		if err := validateScheduleAColumnarManifest(*current); err != nil {
			return ScheduleAColumnarManifest{}, fmt.Errorf("invalid current Schedule A columnar manifest: %w", err)
		}
		if err := validateScheduleAColumnarManifestBacking(options.StorageRoot, *current); err != nil {
			return ScheduleAColumnarManifest{}, fmt.Errorf("validate current Schedule A columnar publication: %w", err)
		}
		if current.Cycle != occurrenceManifest.Cycle {
			return ScheduleAColumnarManifest{}, fmt.Errorf("current Schedule A columnar manifest belongs to cycle %s", current.Cycle)
		}
		if current.FactSetID == factSetID {
			return *current, nil
		}
	}

	manifestPath := filepath.Join(options.StorageRoot, basePath, "manifests", factSetID+".json")
	if existing, err := readScheduleAColumnarManifestIfPresent(manifestPath); err != nil {
		return ScheduleAColumnarManifest{}, err
	} else if existing != nil {
		if err := validateScheduleAColumnarManifest(*existing); err != nil {
			return ScheduleAColumnarManifest{}, fmt.Errorf("invalid immutable Schedule A columnar manifest: %w", err)
		}
		if existing.FactSetID != factSetID || existing.OccurrenceSetID != occurrenceManifest.OccurrenceSetID || existing.OccurrenceManifestSHA256 != occurrenceManifestSHA256 {
			return ScheduleAColumnarManifest{}, fmt.Errorf("immutable Schedule A columnar manifest collision at %s", manifestPath)
		}
		if err := validateScheduleAColumnarShardFiles(options.StorageRoot, existing.Shards, true); err != nil {
			return ScheduleAColumnarManifest{}, err
		}
		if err := writeAtomicJSON(currentPath, *existing); err != nil {
			return ScheduleAColumnarManifest{}, err
		}
		return *existing, nil
	}
	if current != nil {
		adopted, ok := adoptScheduleAColumnarShards(*current, occurrenceManifest, occurrenceManifestSHA256, output, runID, factSetID, configuration, options.Clock().UTC())
		if ok {
			if err := validateScheduleAColumnarManifest(adopted); err != nil {
				return ScheduleAColumnarManifest{}, fmt.Errorf("validate adopted Schedule A columnar publication: %w", err)
			}
			if err := writeAtomicJSON(manifestPath, adopted); err != nil {
				return ScheduleAColumnarManifest{}, err
			}
			if err := writeAtomicJSON(currentPath, adopted); err != nil {
				return ScheduleAColumnarManifest{}, err
			}
			if options.Progress != nil {
				options.Progress("adopted verified Schedule A columnar shards under compact occurrence ancestry for " + occurrenceManifest.Cycle)
			}
			return adopted, nil
		}
	}

	if err := requireScheduleAColumnarStorage(options, 0); err != nil {
		return ScheduleAColumnarManifest{}, err
	}
	if options.Progress != nil {
		options.Progress("publishing Schedule A columnar facts for " + occurrenceManifest.Cycle)
	}
	manifest, err := buildScheduleAColumnarFactSet(
		ctx, occurrenceManifest, occurrenceManifestSHA256, output, runID, factSetID,
		configuration, physicalSchema, options,
	)
	if err != nil {
		return ScheduleAColumnarManifest{}, err
	}
	if err := validateScheduleAColumnarManifest(manifest); err != nil {
		return ScheduleAColumnarManifest{}, err
	}
	if err := requireScheduleAColumnarStorage(options, 0); err != nil {
		return ScheduleAColumnarManifest{}, err
	}
	if err := writeAtomicJSON(manifestPath, manifest); err != nil {
		return ScheduleAColumnarManifest{}, err
	}
	if err := writeAtomicJSON(currentPath, manifest); err != nil {
		return ScheduleAColumnarManifest{}, err
	}
	return manifest, nil
}

func adoptScheduleAColumnarShards(
	current ScheduleAColumnarManifest,
	occurrence scheduleAOccurrenceInput,
	occurrenceSHA string,
	output fecrelease.StagedOutput,
	runID, factSetID string,
	configuration ScheduleAColumnarConfiguration,
	publishedAt time.Time,
) (ScheduleAColumnarManifest, bool) {
	if !occurrence.selectsAllRows() || current.Configuration != configuration || current.Cycle != occurrence.Cycle ||
		current.Counts.SourceOccurrences != occurrence.Counts.Total || current.Counts.Facts != occurrence.Counts.UniqueKeys || current.Counts.ExcludedOccurrences != 0 ||
		output.RowCount == nil || current.SourceReplay.Rows != *output.RowCount || current.SourceReplay.CompressedBytes != output.CompressedByteCount ||
		current.SourceReplay.CompressedSHA256 != output.CompressedSHA256 || current.SourceReplay.UncompressedBytes != output.UncompressedByteCount ||
		current.SourceReplay.UncompressedSHA256 != output.UncompressedSHA256 {
		return ScheduleAColumnarManifest{}, false
	}
	adopted := current
	adopted.FactSetID = factSetID
	adopted.SourceReleaseID = occurrence.SourceReleaseID
	adopted.SourceReleaseManifestSHA256 = occurrence.SourceReleaseManifestSHA256
	adopted.OccurrenceSetID = occurrence.OccurrenceSetID
	adopted.OccurrenceManifestSHA256 = occurrenceSHA
	adopted.RunID = runID
	adopted.PublishedAt = publishedAt
	adopted.Checks = scheduleAColumnarChecks(adopted)
	return adopted, true
}

func buildScheduleAColumnarFactSet(
	ctx context.Context,
	occurrence scheduleAOccurrenceInput,
	occurrenceSHA string,
	output fecrelease.StagedOutput,
	runID, factSetID string,
	configuration ScheduleAColumnarConfiguration,
	physicalSchema *scheduleaparquet.Schema,
	options Options,
) (ScheduleAColumnarManifest, error) {
	selectAllRows := occurrence.selectsAllRows()
	var selectedRows []byte
	var err error
	if !selectAllRows {
		selectedRows, err = loadScheduleASelectedRows(ctx, options.StorageRoot, occurrence)
		if err != nil {
			return ScheduleAColumnarManifest{}, err
		}
	}
	stagingDirectory := filepath.Join(options.StorageRoot, scheduleAColumnarBase(), "staging", factSetID)
	if err := os.MkdirAll(stagingDirectory, 0o750); err != nil {
		return ScheduleAColumnarManifest{}, err
	}
	checkpointPath := filepath.Join(stagingDirectory, "checkpoint.json")
	checkpoint, err := readScheduleAColumnarCheckpoint(checkpointPath, factSetID, occurrence, configuration)
	if err != nil {
		return ScheduleAColumnarManifest{}, err
	}
	checkpoint.RunID = runID
	if err := validateScheduleAColumnarShardFiles(options.StorageRoot, checkpoint.Shards, true); err != nil {
		return ScheduleAColumnarManifest{}, fmt.Errorf("validate Schedule A columnar checkpoint: %w", err)
	}
	var completedBytes uint64
	for _, shard := range checkpoint.Shards {
		if completedBytes > ^uint64(0)-shard.Bytes {
			return ScheduleAColumnarManifest{}, fmt.Errorf("Schedule A columnar checkpoint byte count overflows")
		}
		completedBytes += shard.Bytes
	}
	reserve, err := scheduleAColumnarReserve(output.CompressedByteCount, completedBytes, options.WorkingMarginBytes)
	if err != nil {
		return ScheduleAColumnarManifest{}, err
	}
	if err := requireScheduleAColumnarStorage(options, reserve); err != nil {
		return ScheduleAColumnarManifest{}, err
	}

	stagedPath, err := resolveStorageKey(options.StorageRoot, output.StorageKey)
	if err != nil {
		return ScheduleAColumnarManifest{}, err
	}
	file, err := os.Open(stagedPath)
	if err != nil {
		return ScheduleAColumnarManifest{}, err
	}
	defer func() { _ = file.Close() }()
	compressed := &countingHashReader{reader: &contextReader{ctx: ctx, reader: file}, hash: sha256.New()}
	zstdDecoder, err := zstd.NewReader(compressed, zstd.WithDecoderConcurrency(2), zstd.WithDecoderLowmem(true), zstd.WithDecoderMaxMemory(64<<20))
	if err != nil {
		return ScheduleAColumnarManifest{}, err
	}
	defer zstdDecoder.Close()
	uncompressed := &countingHashReader{reader: zstdDecoder, hash: sha256.New()}
	rowDecoder := schedulea.NewDecoder(uncompressed)

	counts := ScheduleAFactCounts{
		SourceOccurrences:   occurrence.Counts.Total,
		ExcludedOccurrences: occurrence.Counts.Total - occurrence.Counts.UniqueKeys,
		SourceInvalid:       occurrence.Counts.Invalid, SourceDuplicates: occurrence.Counts.DuplicateOccurrences,
	}
	globalSemantic := scheduleaparquet.NewSemanticHasher()
	shards := append([]ScheduleAColumnarShard(nil), checkpoint.Shards...)
	var activeIndex uint64
	var activeFirstOrdinal uint64
	var activeFirstOffset uint64
	var activeSourceRows uint64
	var activeFacts uint64
	var activeValid uint64
	var activeInvalid uint64
	var activeSemantic *scheduleaparquet.SemanticHasher
	var activeWriter *scheduleAColumnarShardWriter
	var activeExpected *ScheduleAColumnarShard
	var haveActive bool
	var rawOffset uint64
	var lastSourceOrdinal uint64

	finishActive := func(lastOrdinal uint64) error {
		if !haveActive {
			return nil
		}
		if activeExpected != nil {
			expected := *activeExpected
			if expected.Index != activeIndex || expected.FirstSourceRowOrdinal != activeFirstOrdinal || expected.LastSourceRowOrdinal != lastOrdinal ||
				expected.FirstRawByteOffset != activeFirstOffset || expected.LastRawByteEnd != rawOffset || expected.SourceRows != activeSourceRows ||
				expected.Facts != activeFacts || expected.ValidFacts != activeValid || expected.InvalidFacts != activeInvalid || expected.SemanticSHA256 != activeSemantic.Sum() {
				return fmt.Errorf("Schedule A columnar checkpoint shard %d does not match source replay", activeIndex)
			}
		} else {
			descriptor, err := activeWriter.Close(lastOrdinal, rawOffset, activeSourceRows)
			if err != nil {
				return err
			}
			activeWriter = nil
			if descriptor.Facts != activeFacts || descriptor.ValidFacts != activeValid || descriptor.InvalidFacts != activeInvalid || descriptor.SemanticSHA256 != activeSemantic.Sum() {
				return fmt.Errorf("Schedule A columnar shard %d does not match source replay", activeIndex)
			}
			shards = append(shards, descriptor)
			checkpoint.Shards = append([]ScheduleAColumnarShard(nil), shards...)
			checkpoint.UpdatedAt = options.Clock().UTC()
			if err := writeAtomicJSON(checkpointPath, checkpoint); err != nil {
				return err
			}
			if options.Progress != nil {
				options.Progress(fmt.Sprintf("published Schedule A columnar shard %d for %s", descriptor.Index, occurrence.Cycle))
			}
		}
		haveActive = false
		activeExpected = nil
		return nil
	}

	for rowDecoder.Scan() {
		row := rowDecoder.Row()
		lastSourceOrdinal = row.Number()
		shardIndex := (row.Number() - 1) / configuration.RowsPerShard
		if !haveActive || shardIndex != activeIndex {
			if haveActive {
				if err := finishActive(row.Number() - 1); err != nil {
					if activeWriter != nil {
						activeWriter.Abort()
					}
					return ScheduleAColumnarManifest{}, err
				}
			}
			activeIndex = shardIndex
			activeFirstOrdinal = row.Number()
			activeFirstOffset = rawOffset
			activeSourceRows, activeFacts, activeValid, activeInvalid = 0, 0, 0, 0
			activeSemantic = scheduleaparquet.NewSemanticHasher()
			haveActive = true
			if shardIndex < uint64(len(checkpoint.Shards)) {
				activeExpected = &checkpoint.Shards[shardIndex]
			} else {
				activeExpected = nil
				activeWriter, err = newScheduleAColumnarShardWriter(
					ctx, options.StorageRoot, stagingDirectory, shardIndex, row.Number(), rawOffset,
					configuration.RowsPerRowGroup, physicalSchema,
				)
				if err != nil {
					return ScheduleAColumnarManifest{}, err
				}
			}
		}

		activeSourceRows++
		metadata := scheduleaparquet.Metadata{
			SourceRowOrdinal: row.Number(), SourceRawByteOffset: rawOffset, SourceRawByteLength: uint64(len(row.Raw())),
		}
		index := row.Number() - 1
		selected := selectAllRows || index/8 < uint64(len(selectedRows)) && selectedRows[index/8]&(1<<(index%8)) != 0
		if selected {
			record, err := schedulea.Freeze(row, occurrence.Cycle)
			if err != nil {
				if activeWriter != nil {
					activeWriter.Abort()
				}
				return ScheduleAColumnarManifest{}, fmt.Errorf("selected Schedule A row %d is invalid: %w", row.Number(), err)
			}
			typed, issueCodes, err := normalizeScheduleAReceipt(record)
			if err != nil {
				if activeWriter != nil {
					activeWriter.Abort()
				}
				return ScheduleAColumnarManifest{}, err
			}
			derived, err := scheduleAColumnarDerived(typed, issueCodes)
			if err != nil {
				if activeWriter != nil {
					activeWriter.Abort()
				}
				return ScheduleAColumnarManifest{}, err
			}
			if err := globalSemantic.AddSource(row, metadata); err != nil {
				return ScheduleAColumnarManifest{}, err
			}
			if err := activeSemantic.AddSource(row, metadata); err != nil {
				return ScheduleAColumnarManifest{}, err
			}
			if activeWriter != nil {
				if err := activeWriter.Write(row, metadata, derived); err != nil {
					activeWriter.Abort()
					return ScheduleAColumnarManifest{}, err
				}
			}
			counts.Facts++
			activeFacts++
			if derived.NormalizationState == "valid" {
				counts.ValidFacts++
				activeValid++
			} else {
				counts.InvalidFacts++
				activeInvalid++
			}
		}
		rawOffset += uint64(len(row.Raw()))
		if options.Progress != nil && row.Number()%1_000_000 == 0 {
			options.Progress(fmt.Sprintf("processed %d Schedule A source rows into columnar facts for %s", row.Number(), occurrence.Cycle))
		}
		if row.Number()&0x3fff == 0 {
			if err := ctx.Err(); err != nil {
				if activeWriter != nil {
					activeWriter.Abort()
				}
				return ScheduleAColumnarManifest{}, err
			}
		}
	}
	if err := rowDecoder.Err(); err != nil {
		if activeWriter != nil {
			activeWriter.Abort()
		}
		return ScheduleAColumnarManifest{}, err
	}
	if haveActive {
		if err := finishActive(lastSourceOrdinal); err != nil {
			if activeWriter != nil {
				activeWriter.Abort()
			}
			return ScheduleAColumnarManifest{}, err
		}
	}
	if uint64(len(checkpoint.Shards)) > uint64(len(shards)) {
		return ScheduleAColumnarManifest{}, fmt.Errorf("Schedule A columnar checkpoint contains shards beyond source EOF")
	}
	if counts.Facts != occurrence.Counts.UniqueKeys || counts.SourceOccurrences != counts.Facts+counts.ExcludedOccurrences {
		return ScheduleAColumnarManifest{}, fmt.Errorf("Schedule A columnar fact counts do not conserve occurrence membership")
	}
	if output.RowCount == nil || counts.SourceOccurrences != *output.RowCount {
		return ScheduleAColumnarManifest{}, fmt.Errorf("Schedule A columnar source row count mismatch")
	}
	compressedDigest := hex.EncodeToString(compressed.hash.Sum(nil))
	uncompressedDigest := hex.EncodeToString(uncompressed.hash.Sum(nil))
	if compressed.bytes != output.CompressedByteCount || compressedDigest != output.CompressedSHA256 {
		return ScheduleAColumnarManifest{}, fmt.Errorf("Schedule A columnar replay compressed identity mismatch")
	}
	if uncompressed.bytes != output.UncompressedByteCount || uncompressedDigest != output.UncompressedSHA256 {
		return ScheduleAColumnarManifest{}, fmt.Errorf("Schedule A columnar replay uncompressed identity mismatch")
	}

	manifest := ScheduleAColumnarManifest{
		Schema: "manifest.schema.json", SchemaVersion: ScheduleAColumnarFactSetSchemaVersion,
		FactSetID: factSetID, FactType: ScheduleAFactType, Cycle: occurrence.Cycle, SourceContract: ScheduleASourceContract,
		SourceReleaseID: occurrence.SourceReleaseID, SourceReleaseManifestSHA256: occurrence.SourceReleaseManifestSHA256,
		OccurrenceSetID: occurrence.OccurrenceSetID, OccurrenceManifestSHA256: occurrenceSHA,
		RunID: runID, State: "published", NormalizerVersion: ScheduleANormalizerVersion,
		FactSchemaVersion: ScheduleAFactSchemaVersion, PhysicalSchemaVersion: scheduleaparquet.PhysicalSchemaVersion,
		PublisherVersion: ScheduleAColumnarPublisherVersion, ParquetLibrary: scheduleaparquet.LibraryVersion,
		PublishedAt: options.Clock().UTC(), Configuration: configuration, Counts: counts, Shards: shards,
		SourceReplay: ScheduleAColumnarSourceReplay{
			Rows: counts.SourceOccurrences, CompressedBytes: compressed.bytes, CompressedSHA256: compressedDigest,
			UncompressedBytes: uncompressed.bytes, UncompressedSHA256: uncompressedDigest,
			SemanticSHA256: globalSemantic.Sum(),
		},
	}
	manifest.Checks = scheduleAColumnarChecks(manifest)
	if err := os.Remove(checkpointPath); err != nil && !os.IsNotExist(err) {
		return ScheduleAColumnarManifest{}, err
	}
	_ = os.Remove(stagingDirectory)
	return manifest, nil
}

func scheduleAColumnarDerived(typed ScheduleAReceiptTypedFields, issueCodes []string) (scheduleaparquet.Derived, error) {
	derived := scheduleaparquet.Derived{
		NormalizationState: "valid", ReceiptAmountState: typed.Receipt.Amount.ObservationState,
		AggregateYTDState: typed.Receipt.ContributorAggregate.ObservationState,
		FECElectionYear:   typed.Election.FECElectionYear, ReportYear: typed.Filing.ReportYear,
		TwoYearTransactionPeriod: typed.Election.TransactionPeriod, MemoedSubtotal: typed.Receipt.MemoedSubtotal,
	}
	if len(issueCodes) != 0 {
		derived.NormalizationState = "invalid"
		encoded, err := json.Marshal(issueCodes)
		if err != nil {
			return scheduleaparquet.Derived{}, err
		}
		text := string(encoded)
		derived.NormalizationIssuesJSON = &text
	}
	parseMinor := func(value *string) (*int64, error) {
		if value == nil {
			return nil, nil
		}
		parsed, err := strconv.ParseInt(*value, 10, 64)
		if err != nil {
			return nil, err
		}
		return &parsed, nil
	}
	var err error
	derived.ReceiptAmountMinorUnits, err = parseMinor(typed.Receipt.Amount.ReportedMinorUnits)
	if err != nil {
		return scheduleaparquet.Derived{}, err
	}
	derived.AggregateYTDMinorUnits, err = parseMinor(typed.Receipt.ContributorAggregate.ReportedMinorUnits)
	if err != nil {
		return scheduleaparquet.Derived{}, err
	}
	scale := func(value *int) (*int32, error) {
		if value == nil {
			return nil, nil
		}
		if *value < 0 || *value > math.MaxInt32 {
			return nil, fmt.Errorf("Schedule A money source scale exceeds Parquet range")
		}
		converted := int32(*value)
		return &converted, nil
	}
	derived.ReceiptAmountSourceScale, err = scale(typed.Receipt.Amount.SourceScale)
	if err != nil {
		return scheduleaparquet.Derived{}, err
	}
	derived.AggregateYTDSourceScale, err = scale(typed.Receipt.ContributorAggregate.SourceScale)
	if err != nil {
		return scheduleaparquet.Derived{}, err
	}
	parseLocal := func(value *string) *int64 {
		if value == nil {
			return nil
		}
		parsed, err := time.Parse("2006-01-02 15:04:05.999999999", *value)
		if err != nil {
			return nil
		}
		nanos := parsed.UnixNano()
		return &nanos
	}
	derived.ReceiptAtLocalNanos = parseLocal(typed.Receipt.ReceivedAtLocal)
	derived.PublisherLoadedAtNanos = parseLocal(typed.Filing.PublisherLoadedAtLocal)
	if typed.Receipt.ReceivedOn != nil {
		parsed, err := time.Parse("2006-01-02", *typed.Receipt.ReceivedOn)
		if err != nil {
			return scheduleaparquet.Derived{}, err
		}
		days := int32(parsed.Unix() / 86400)
		derived.ReceiptDateDays = &days
	}
	return derived, nil
}

func scheduleAColumnarChecks(manifest ScheduleAColumnarManifest) []Check {
	var sourceRows, facts, valid, invalid, bytes uint64
	for _, shard := range manifest.Shards {
		sourceRows += shard.SourceRows
		facts += shard.Facts
		valid += shard.ValidFacts
		invalid += shard.InvalidFacts
		bytes += shard.Bytes
	}
	return []Check{
		{ID: "occurrence_lineage", Passed: true, Severity: "block", Detail: "columnar fact set names an immutable validated Schedule A occurrence set"},
		{ID: "selected_output_integrity", Passed: true, Severity: "block", Detail: "complete staged compressed and uncompressed identities matched"},
		{ID: "source_row_conservation", Passed: sourceRows == manifest.Counts.SourceOccurrences, Severity: "block", Detail: "deterministic shard ranges cover every source row exactly once"},
		{ID: "unique_fact_projection", Passed: facts == manifest.Counts.Facts && facts+manifest.Counts.ExcludedOccurrences == manifest.Counts.SourceOccurrences, Severity: "block", Detail: "each unique source-valid SUB_ID produced one fact and every exclusion remains explicit"},
		{ID: "fact_state_conservation", Passed: facts == valid+invalid && valid == manifest.Counts.ValidFacts && invalid == manifest.Counts.InvalidFacts, Severity: "block", Detail: "every columnar fact has an explicit normalization state"},
		{ID: "parquet_round_trip", Passed: true, Severity: "block", Detail: "every newly written shard matched its source-value semantic digest after Parquet readback"},
		{ID: "immutable_shards", Passed: len(manifest.Shards) != 0 && bytes != 0, Severity: "block", Detail: "all Parquet shards are content addressed and independently retryable"},
	}
}

func scheduleAColumnarFactSetID(occurrenceSetID, occurrenceSHA string, configuration ScheduleAColumnarConfiguration) string {
	return digestParts(
		"fec.schedule-a.columnar-fact-set.v1", occurrenceSetID, occurrenceSHA,
		ScheduleANormalizerVersion, ScheduleAFactSchemaVersion, scheduleaparquet.PhysicalSchemaVersion,
		ScheduleAColumnarPublisherVersion, strconv.FormatUint(configuration.RowsPerShard, 10),
		strconv.FormatUint(configuration.RowsPerRowGroup, 10),
	)
}

func scheduleAColumnarBase() string { return filepath.Join("facts", "fec", "schedule-a", "columnar") }

func scheduleAColumnarReserve(sourceCompressed, completed, margin uint64) (uint64, error) {
	half := sourceCompressed / 2
	if sourceCompressed > ^uint64(0)-half {
		return 0, fmt.Errorf("Schedule A columnar storage estimate overflows")
	}
	estimated := sourceCompressed + half
	remaining := uint64(0)
	if completed < estimated {
		remaining = estimated - completed
	}
	if remaining > ^uint64(0)-margin {
		return 0, fmt.Errorf("Schedule A columnar storage estimate overflows")
	}
	return remaining + margin, nil
}

func requireScheduleAColumnarStorage(options Options, reserve uint64) error {
	available, err := options.DiskAvailable(options.StorageRoot)
	if err != nil {
		return fmt.Errorf("inspect Schedule A columnar storage: %w", err)
	}
	if available < options.FreeFloorBytes || reserve > available-options.FreeFloorBytes {
		return fmt.Errorf("Schedule A columnar publication would leave less than the required free-space floor")
	}
	return nil
}

func readScheduleAColumnarManifestIfPresent(path string) (*ScheduleAColumnarManifest, error) {
	content, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var manifest ScheduleAColumnarManifest
	decoder := json.NewDecoder(strings.NewReader(string(content)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&manifest); err != nil {
		return nil, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return nil, fmt.Errorf("multiple JSON values in Schedule A columnar manifest")
		}
		return nil, err
	}
	return &manifest, nil
}

func validateScheduleAColumnarManifest(manifest ScheduleAColumnarManifest) error {
	if manifest.Schema != "manifest.schema.json" || manifest.SchemaVersion != ScheduleAColumnarFactSetSchemaVersion || manifest.FactType != ScheduleAFactType {
		return fmt.Errorf("unexpected Schedule A columnar manifest schema")
	}
	if manifest.SourceContract != ScheduleASourceContract || !validCycle(manifest.Cycle) || manifest.State != "published" || manifest.PublishedAt.IsZero() {
		return fmt.Errorf("Schedule A columnar publication identity is incomplete")
	}
	for _, digest := range []string{manifest.FactSetID, manifest.SourceReleaseManifestSHA256, manifest.OccurrenceSetID, manifest.OccurrenceManifestSHA256, manifest.SourceReplay.CompressedSHA256, manifest.SourceReplay.UncompressedSHA256, manifest.SourceReplay.SemanticSHA256} {
		if !digestPattern.MatchString(digest) {
			return fmt.Errorf("Schedule A columnar manifest contains an invalid digest")
		}
	}
	if !releaseIDPattern.MatchString(manifest.SourceReleaseID) || !fecrelease.ValidAcquisitionRunID(manifest.RunID) {
		return fmt.Errorf("Schedule A columnar source or run identity is invalid")
	}
	if manifest.NormalizerVersion != ScheduleANormalizerVersion || manifest.FactSchemaVersion != ScheduleAFactSchemaVersion ||
		manifest.PhysicalSchemaVersion != scheduleaparquet.PhysicalSchemaVersion || manifest.PublisherVersion != ScheduleAColumnarPublisherVersion ||
		manifest.ParquetLibrary != scheduleaparquet.LibraryVersion {
		return fmt.Errorf("Schedule A columnar parser or physical contract is unsupported")
	}
	if manifest.Configuration.RowsPerShard == 0 || manifest.Configuration.RowsPerRowGroup == 0 ||
		manifest.Configuration.RowsPerRowGroup > manifest.Configuration.RowsPerShard || manifest.Configuration.ColumnCount != schedulea.FieldCount+18 ||
		manifest.Configuration.Compression != "parquet-zstd" || manifest.Configuration.Locator == "" {
		return fmt.Errorf("Schedule A columnar configuration is invalid")
	}
	expectedID := scheduleAColumnarFactSetID(manifest.OccurrenceSetID, manifest.OccurrenceManifestSHA256, manifest.Configuration)
	if manifest.FactSetID != expectedID {
		return fmt.Errorf("Schedule A columnar fact-set ID does not match canonical inputs")
	}
	if manifest.Counts.SourceOccurrences != manifest.Counts.Facts+manifest.Counts.ExcludedOccurrences || manifest.Counts.Facts != manifest.Counts.ValidFacts+manifest.Counts.InvalidFacts ||
		manifest.SourceReplay.Rows != manifest.Counts.SourceOccurrences || manifest.SourceReplay.CompressedBytes == 0 || manifest.SourceReplay.UncompressedBytes == 0 {
		return fmt.Errorf("Schedule A columnar counts are not conserved")
	}
	var sourceRows, facts, valid, invalid uint64
	var priorOrdinal, priorRawEnd uint64
	for index, shard := range manifest.Shards {
		if shard.Index != uint64(index) || shard.FirstSourceRowOrdinal != priorOrdinal+1 || shard.FirstRawByteOffset != priorRawEnd ||
			shard.LastSourceRowOrdinal < shard.FirstSourceRowOrdinal || shard.SourceRows != shard.LastSourceRowOrdinal-shard.FirstSourceRowOrdinal+1 ||
			shard.LastRawByteEnd < shard.FirstRawByteOffset || shard.Facts != shard.ValidFacts+shard.InvalidFacts || shard.Bytes == 0 ||
			!digestPattern.MatchString(shard.SHA256) || !digestPattern.MatchString(shard.SemanticSHA256) {
			return fmt.Errorf("Schedule A columnar shard %d is invalid", index)
		}
		expectedStorageKey := filepath.ToSlash(filepath.Join(
			scheduleAColumnarBase(), "shards", "sha256", shard.SHA256[:2], shard.SHA256+".parquet",
		))
		if shard.StorageKey != expectedStorageKey {
			return fmt.Errorf("Schedule A columnar shard %d storage key is not canonical", index)
		}
		priorOrdinal, priorRawEnd = shard.LastSourceRowOrdinal, shard.LastRawByteEnd
		sourceRows += shard.SourceRows
		facts += shard.Facts
		valid += shard.ValidFacts
		invalid += shard.InvalidFacts
	}
	if sourceRows != manifest.Counts.SourceOccurrences || facts != manifest.Counts.Facts || valid != manifest.Counts.ValidFacts || invalid != manifest.Counts.InvalidFacts ||
		priorRawEnd != manifest.SourceReplay.UncompressedBytes {
		return fmt.Errorf("Schedule A columnar shard totals are not conserved")
	}
	if len(manifest.Checks) < 7 {
		return fmt.Errorf("Schedule A columnar manifest is missing required checks")
	}
	for _, check := range manifest.Checks {
		if check.Severity == "block" && !check.Passed {
			return fmt.Errorf("blocking Schedule A columnar check %s failed", check.ID)
		}
	}
	return nil
}

func validateScheduleAColumnarManifestBacking(storageRoot string, current ScheduleAColumnarManifest) error {
	immutablePath := filepath.Join(storageRoot, scheduleAColumnarBase(), "manifests", current.FactSetID+".json")
	immutable, err := readScheduleAColumnarManifestIfPresent(immutablePath)
	if err != nil {
		return err
	}
	if immutable == nil {
		return fmt.Errorf("immutable Schedule A columnar manifest is missing")
	}
	if err := validateScheduleAColumnarManifest(*immutable); err != nil {
		return err
	}
	if !reflect.DeepEqual(current, *immutable) {
		return fmt.Errorf("active Schedule A columnar pointer differs from its immutable manifest")
	}
	return validateScheduleAColumnarShardFiles(storageRoot, current.Shards, true)
}

func validateScheduleAColumnarShardFiles(storageRoot string, shards []ScheduleAColumnarShard, verifyDigest bool) error {
	for _, shard := range shards {
		path, err := resolveStorageKey(storageRoot, shard.StorageKey)
		if err != nil {
			return err
		}
		info, err := os.Stat(path)
		if err != nil {
			return fmt.Errorf("Schedule A columnar shard %d: %w", shard.Index, err)
		}
		if !info.Mode().IsRegular() || info.Size() < 0 || uint64(info.Size()) != shard.Bytes {
			return fmt.Errorf("Schedule A columnar shard %d does not match its published size", shard.Index)
		}
		if verifyDigest {
			digest, err := scheduleAColumnarFileSHA256(context.Background(), path)
			if err != nil || digest != shard.SHA256 {
				return fmt.Errorf("Schedule A columnar shard %d does not match its published digest", shard.Index)
			}
		}
	}
	return nil
}

func readScheduleAColumnarCheckpoint(path, factSetID string, occurrence scheduleAOccurrenceInput, configuration ScheduleAColumnarConfiguration) (scheduleAColumnarCheckpoint, error) {
	checkpoint := scheduleAColumnarCheckpoint{
		SchemaVersion: ScheduleAColumnarCheckpointVersion, FactSetID: factSetID, Cycle: occurrence.Cycle,
		OccurrenceSetID: occurrence.OccurrenceSetID, PhysicalSchemaVersion: scheduleaparquet.PhysicalSchemaVersion,
		PublisherVersion: ScheduleAColumnarPublisherVersion, Configuration: configuration,
	}
	content, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return checkpoint, nil
	}
	if err != nil {
		return checkpoint, err
	}
	decoder := json.NewDecoder(strings.NewReader(string(content)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&checkpoint); err != nil {
		return scheduleAColumnarCheckpoint{}, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return scheduleAColumnarCheckpoint{}, fmt.Errorf("Schedule A columnar checkpoint contains trailing JSON")
	}
	if checkpoint.SchemaVersion != ScheduleAColumnarCheckpointVersion || checkpoint.FactSetID != factSetID || checkpoint.Cycle != occurrence.Cycle ||
		checkpoint.OccurrenceSetID != occurrence.OccurrenceSetID || checkpoint.PhysicalSchemaVersion != scheduleaparquet.PhysicalSchemaVersion ||
		checkpoint.PublisherVersion != ScheduleAColumnarPublisherVersion || checkpoint.Configuration != configuration {
		return scheduleAColumnarCheckpoint{}, fmt.Errorf("Schedule A columnar checkpoint belongs to another publication")
	}
	for index, shard := range checkpoint.Shards {
		if shard.Index != uint64(index) {
			return scheduleAColumnarCheckpoint{}, fmt.Errorf("Schedule A columnar checkpoint shards are not a contiguous prefix")
		}
	}
	return checkpoint, nil
}
