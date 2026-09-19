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
	"strconv"
	"strings"
	"time"

	"github.com/klauspost/compress/zstd"
	fecrelease "github.com/vedantadhobley/legal-tender/internal/source/fec/release"
	"github.com/vedantadhobley/legal-tender/internal/source/fec/schedulea"
)

const compactEvidenceBase = "evidence/fec/schedule-a/compact"

// PublishScheduleACompactOccurrences publishes derivable dense occurrences,
// partitioned fixed-width natural-key indexes, sparse row/key exceptions, and
// actual inter-release deltas.
func PublishScheduleACompactOccurrences(
	ctx context.Context,
	releaseManifest fecrelease.ReleaseManifest,
	releaseManifestSHA256, cycle, runID string,
	options Options,
) (ScheduleACompactManifest, error) {
	if options.Clock == nil {
		options.Clock = time.Now
	}
	if options.ShardCount <= 0 {
		options.ShardCount = defaultShardCount
	}
	if options.ShardCount > 4096 {
		return ScheduleACompactManifest{}, fmt.Errorf("compact index partition count must not exceed 4096")
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
		return ScheduleACompactManifest{}, fmt.Errorf("storage root is required")
	}
	if !validCycle(cycle) {
		return ScheduleACompactManifest{}, fmt.Errorf("cycle must be a four-digit even year")
	}
	if !fecrelease.ValidAcquisitionRunID(runID) {
		return ScheduleACompactManifest{}, fmt.Errorf("run ID contains unsupported characters")
	}
	if !digestPattern.MatchString(releaseManifestSHA256) {
		return ScheduleACompactManifest{}, fmt.Errorf("source release manifest SHA-256 is invalid")
	}
	if issues := fecrelease.ValidateKnownManifest(releaseManifest); len(issues) != 0 {
		return ScheduleACompactManifest{}, fmt.Errorf("invalid source release: %s", issues[0].Message)
	}
	output, sourceArtifactSHA256, err := selectedScheduleAOutput(releaseManifest, cycle)
	if err != nil {
		return ScheduleACompactManifest{}, err
	}
	if output.RowCount == nil {
		return ScheduleACompactManifest{}, fmt.Errorf("selected Schedule A output has no row count")
	}
	configuration := ScheduleACompactConfiguration{
		Partitions: options.ShardCount, PartitionHash: ScheduleACompactIndexHash,
		IndexRecordBytes: ScheduleACompactIndexRecordBytes, IndexEncoding: "uint64-be-sub-id,uint64-be-row-ordinal,sha256-semantic-digest",
	}
	currentPath := options.CurrentManifestPath
	if currentPath == "" {
		currentPath = filepath.Join(options.StorageRoot, compactEvidenceBase, "current", cycle+".json")
	}
	if err := requirePathInside(options.StorageRoot, currentPath); err != nil {
		return ScheduleACompactManifest{}, err
	}

	lockPath := filepath.Join(options.StorageRoot, compactEvidenceBase, ".publish-"+cycle+".lock")
	unlock, err := lockContext(ctx, lockPath)
	if err != nil {
		return ScheduleACompactManifest{}, err
	}
	defer unlock()

	current, err := readScheduleACompactManifestIfPresent(currentPath)
	if err != nil {
		return ScheduleACompactManifest{}, err
	}
	if current != nil {
		if err := validateScheduleACompactManifest(*current); err != nil {
			return ScheduleACompactManifest{}, fmt.Errorf("invalid current compact occurrence manifest: %w", err)
		}
		if err := validateScheduleACompactManifestBacking(ctx, options.StorageRoot, *current); err != nil {
			return ScheduleACompactManifest{}, fmt.Errorf("validate current compact occurrence publication: %w", err)
		}
		if current.Cycle != cycle || !reflect.DeepEqual(current.Configuration, configuration) {
			return ScheduleACompactManifest{}, fmt.Errorf("current compact occurrence manifest has incompatible cycle or configuration")
		}
		descends, err := releaseDescendsFrom(ctx, options.StorageRoot, releaseManifest, current.SourceReleaseID)
		if err != nil {
			return ScheduleACompactManifest{}, err
		}
		if !descends {
			return ScheduleACompactManifest{}, fmt.Errorf("source release does not descend from the compact occurrence baseline")
		}
		if current.SourceArtifactSHA256 == sourceArtifactSHA256 && current.StagedOutputSHA256 == output.CompressedSHA256 &&
			current.ParserVersion == ParserVersion && current.SemanticSchemaVersion == SemanticSchemaVersion {
			return *current, nil
		}
	}

	priorID := ""
	if current != nil {
		priorID = current.OccurrenceSetID
	}
	setID := scheduleACompactOccurrenceSetID(sourceArtifactSHA256, output, cycle, priorID, configuration)
	manifestPath := filepath.Join(options.StorageRoot, compactEvidenceBase, "manifests", setID+".json")
	if existing, err := readScheduleACompactManifestIfPresent(manifestPath); err != nil {
		return ScheduleACompactManifest{}, err
	} else if existing != nil {
		if err := validateScheduleACompactManifest(*existing); err != nil {
			return ScheduleACompactManifest{}, fmt.Errorf("invalid immutable compact occurrence manifest: %w", err)
		}
		if existing.OccurrenceSetID != setID || existing.PriorOccurrenceSetID != priorID {
			return ScheduleACompactManifest{}, fmt.Errorf("immutable compact occurrence manifest collision")
		}
		if err := validateScheduleACompactManifestBacking(ctx, options.StorageRoot, *existing); err != nil {
			return ScheduleACompactManifest{}, err
		}
		if err := writeAtomicJSON(currentPath, *existing); err != nil {
			return ScheduleACompactManifest{}, err
		}
		return *existing, nil
	}

	reserve, err := scheduleACompactReserve(*output.RowCount, options.WorkingMarginBytes)
	if err != nil {
		return ScheduleACompactManifest{}, err
	}
	if err := requireOccurrenceStorage(options, reserve); err != nil {
		return ScheduleACompactManifest{}, err
	}
	if options.Progress != nil {
		options.Progress("publishing compact Schedule A occurrences for " + cycle)
	}
	manifest, err := buildScheduleACompactOccurrenceSet(
		ctx, releaseManifest, releaseManifestSHA256, output, sourceArtifactSHA256,
		cycle, runID, setID, current, configuration, options,
	)
	if err != nil {
		return ScheduleACompactManifest{}, err
	}
	if err := validateScheduleACompactManifest(manifest); err != nil {
		return ScheduleACompactManifest{}, err
	}
	if err := requireOccurrenceStorage(options, 0); err != nil {
		return ScheduleACompactManifest{}, err
	}
	if err := writeAtomicJSON(manifestPath, manifest); err != nil {
		return ScheduleACompactManifest{}, err
	}
	if err := writeAtomicJSON(currentPath, manifest); err != nil {
		return ScheduleACompactManifest{}, err
	}
	return manifest, nil
}

func scheduleACompactReserve(rows, workingMargin uint64) (uint64, error) {
	const bytesPerRow = uint64(ScheduleACompactStageRecordBytes + ScheduleACompactIndexRecordBytes)
	if rows > (^uint64(0)-workingMargin)/bytesPerRow {
		return 0, fmt.Errorf("compact occurrence working-space estimate overflows")
	}
	return rows*bytesPerRow + workingMargin, nil
}

func scheduleACompactOccurrenceSetID(sourceArtifactSHA256 string, output fecrelease.StagedOutput, cycle, priorID string, configuration ScheduleACompactConfiguration) string {
	return digestParts(
		"fec.schedule-a.compact-occurrence-set.v1", sourceArtifactSHA256, output.CompressedSHA256,
		output.Selection, cycle, ParserVersion, SemanticSchemaVersion, ScheduleACompactPublisherVersion,
		ScheduleACompactIndexSchemaVersion, strconv.Itoa(configuration.Partitions), configuration.PartitionHash,
		strconv.Itoa(configuration.IndexRecordBytes), configuration.IndexEncoding, priorID,
	)
}

func buildScheduleACompactOccurrenceSet(
	ctx context.Context,
	releaseManifest fecrelease.ReleaseManifest,
	releaseManifestSHA256 string,
	output fecrelease.StagedOutput,
	sourceArtifactSHA256, cycle, runID, setID string,
	prior *ScheduleACompactManifest,
	configuration ScheduleACompactConfiguration,
	options Options,
) (ScheduleACompactManifest, error) {
	temporaryDirectory := filepath.Join(options.StorageRoot, compactEvidenceBase, "staging", setID, runID)
	if err := os.MkdirAll(temporaryDirectory, 0o750); err != nil {
		return ScheduleACompactManifest{}, err
	}
	defer func() { _ = os.RemoveAll(temporaryDirectory) }()

	rowExceptions, err := newArtifactWriterAt(ctx, options.StorageRoot, temporaryDirectory, compactEvidenceBase, "row-exceptions")
	if err != nil {
		return ScheduleACompactManifest{}, err
	}
	defer rowExceptions.Abort()
	stage, err := newCompactStageSet(ctx, filepath.Join(temporaryDirectory, "index-stage"), configuration.Partitions)
	if err != nil {
		return ScheduleACompactManifest{}, err
	}
	defer stage.Remove()

	counts, replay, err := streamScheduleACompactOccurrences(
		ctx, options.StorageRoot, output, sourceArtifactSHA256, cycle, stage, rowExceptions, options.Progress,
	)
	if err != nil {
		return ScheduleACompactManifest{}, err
	}
	if err := stage.Close(); err != nil {
		return ScheduleACompactManifest{}, err
	}

	deltas, err := newArtifactWriterAt(ctx, options.StorageRoot, temporaryDirectory, compactEvidenceBase, "deltas")
	if err != nil {
		return ScheduleACompactManifest{}, err
	}
	defer deltas.Abort()
	changeMode := "bootstrap-dense-membership"
	if prior != nil {
		changeMode = "explicit-inter-release-delta"
	}
	changes := ChangeCounts{}
	partitions := make([]ScheduleACompactPartition, 0, configuration.Partitions)
	for partition := 0; partition < configuration.Partitions; partition++ {
		entries, err := readCompactStageEntries(stage.shards[partition].path)
		if err != nil {
			return ScheduleACompactManifest{}, fmt.Errorf("read compact partition %d: %w", partition, err)
		}
		descriptor, currentStates, err := buildScheduleACompactPartition(
			ctx, options.StorageRoot, temporaryDirectory, partition, configuration.Partitions,
			entries, sourceArtifactSHA256, output.Selection, cycle, &counts, rowExceptions,
		)
		if err != nil {
			return ScheduleACompactManifest{}, err
		}
		var priorStates []compactKeyState
		if prior != nil {
			priorStates, err = readScheduleACompactPartitionStates(ctx, options.StorageRoot, prior.IndexPartitions[partition], configuration.Partitions)
			if err != nil {
				return ScheduleACompactManifest{}, fmt.Errorf("read prior compact partition %d: %w", partition, err)
			}
		}
		if err := compareScheduleACompactStates(currentStates, priorStates, prior == nil, deltas, &changes); err != nil {
			return ScheduleACompactManifest{}, err
		}
		partitions = append(partitions, descriptor)
		_ = os.Remove(stage.shards[partition].path)
		if options.Progress != nil && (partition+1)%32 == 0 {
			options.Progress(fmt.Sprintf("published %d of %d compact Schedule A index partitions for %s", partition+1, configuration.Partitions, cycle))
		}
	}

	rowExceptionArtifact, err := rowExceptions.Finalize()
	if err != nil {
		return ScheduleACompactManifest{}, err
	}
	deltaArtifact, err := deltas.Finalize()
	if err != nil {
		return ScheduleACompactManifest{}, err
	}
	for kind, artifact := range map[string]Artifact{"row-exceptions": rowExceptionArtifact, "deltas": deltaArtifact} {
		path, err := resolveStorageKey(options.StorageRoot, artifact.StorageKey)
		if err != nil {
			return ScheduleACompactManifest{}, err
		}
		if err := verifyArtifact(ctx, path, artifact); err != nil {
			return ScheduleACompactManifest{}, fmt.Errorf("verify compact %s: %w", kind, err)
		}
	}
	manifest := ScheduleACompactManifest{
		Schema: "manifest.schema.json", SchemaVersion: ScheduleACompactManifestSchemaVersion,
		OccurrenceSetID: setID, SourceReleaseID: releaseManifest.ReleaseID,
		SourceReleaseManifestSHA256: releaseManifestSHA256, SourceArtifactSHA256: sourceArtifactSHA256,
		StagedOutputSHA256: output.CompressedSHA256, Relation: output.Selection, Cycle: cycle, RunID: runID,
		State: "published", ParserVersion: ParserVersion, SemanticSchemaVersion: SemanticSchemaVersion,
		PublisherVersion: ScheduleACompactPublisherVersion, IndexSchemaVersion: ScheduleACompactIndexSchemaVersion,
		OccurrenceEncoding: ScheduleACompactOccurrenceEncoding, ChangeMode: changeMode, PublishedAt: options.Clock().UTC(),
		Configuration: configuration, Counts: counts, Changes: changes, SourceReplay: replay,
		IndexPartitions: partitions, RowExceptions: rowExceptionArtifact, Deltas: deltaArtifact,
	}
	if prior != nil {
		manifest.PriorOccurrenceSetID = prior.OccurrenceSetID
	}
	manifest.Checks = scheduleACompactChecks(manifest, output)
	return manifest, nil
}

func streamScheduleACompactOccurrences(
	ctx context.Context,
	storageRoot string,
	output fecrelease.StagedOutput,
	sourceArtifactSHA256, cycle string,
	stage *compactStageSet,
	rowExceptions *artifactWriter,
	progress func(string),
) (Counts, ScheduleACompactSourceReplay, error) {
	path, err := resolveStorageKey(storageRoot, output.StorageKey)
	if err != nil {
		return Counts{}, ScheduleACompactSourceReplay{}, err
	}
	file, err := os.Open(path)
	if err != nil {
		return Counts{}, ScheduleACompactSourceReplay{}, err
	}
	defer func() { _ = file.Close() }()
	compressed := &countingHashReader{reader: &contextReader{ctx: ctx, reader: file}, hash: sha256.New()}
	decoder, err := zstd.NewReader(compressed, zstd.WithDecoderConcurrency(2), zstd.WithDecoderLowmem(true), zstd.WithDecoderMaxMemory(64<<20))
	if err != nil {
		return Counts{}, ScheduleACompactSourceReplay{}, err
	}
	defer decoder.Close()
	uncompressed := &countingHashReader{reader: decoder, hash: sha256.New()}
	rows := schedulea.NewDecoder(uncompressed)
	var counts Counts
	var rawOffset uint64
	for rows.Scan() {
		row := rows.Row()
		raw := row.Raw()
		counts.Total++
		subID, hasKey := compactSubID(row)
		if hasKey {
			counts.Keyed++
		}
		validationErr := schedulea.Validate(row, cycle)
		if validationErr == nil && !hasKey {
			return counts, ScheduleACompactSourceReplay{}, fmt.Errorf("valid Schedule A row %d has no encodable SUB_ID", row.Number())
		}
		entry := compactStageEntry{
			SubID: subID, RowOrdinal: row.Number(), RawByteOffset: rawOffset,
			RawByteLength: uint64(len(raw)), Valid: validationErr == nil,
		}
		if validationErr == nil {
			semantic, err := semanticDigestBytes(row)
			if err != nil {
				return counts, ScheduleACompactSourceReplay{}, err
			}
			entry.Semantic = semantic
			counts.Valid++
		} else {
			counts.Invalid++
			rawDigest := sha256.Sum256(raw)
			occurrenceIdentifier := occurrenceID(sourceArtifactSHA256, output.Selection, cycle, row.Number())
			codes := validationCodes(validationErr)
			for _, code := range codes {
				exception := ScheduleACompactRowException{
					ExceptionID: issueID(occurrenceIdentifier, code), OccurrenceID: occurrenceIdentifier,
					RowOrdinal: row.Number(), RawByteOffset: rawOffset, RawByteLength: uint64(len(raw)),
					RawContentSHA256: hex.EncodeToString(rawDigest[:]), Code: code, Severity: "block",
					Message: validationErr.Error(),
				}
				if hasKey {
					exception.SubID = strconv.FormatUint(subID, 10)
				}
				if err := rowExceptions.WriteJSON(exception); err != nil {
					return counts, ScheduleACompactSourceReplay{}, err
				}
			}
		}
		if hasKey {
			if err := stage.Add(entry); err != nil {
				return counts, ScheduleACompactSourceReplay{}, err
			}
		}
		rawOffset += uint64(len(raw))
		if progress != nil && counts.Total%1_000_000 == 0 {
			progress(fmt.Sprintf("indexed %d compact Schedule A occurrences for %s", counts.Total, cycle))
		}
	}
	if err := rows.Err(); err != nil {
		return counts, ScheduleACompactSourceReplay{}, err
	}
	if err := ctx.Err(); err != nil {
		return counts, ScheduleACompactSourceReplay{}, err
	}
	if output.RowCount == nil || counts.Total != *output.RowCount || compressed.bytes != output.CompressedByteCount ||
		hex.EncodeToString(compressed.hash.Sum(nil)) != output.CompressedSHA256 || uncompressed.bytes != output.UncompressedByteCount ||
		hex.EncodeToString(uncompressed.hash.Sum(nil)) != output.UncompressedSHA256 {
		return counts, ScheduleACompactSourceReplay{}, fmt.Errorf("compact occurrence source replay identity mismatch")
	}
	replay := ScheduleACompactSourceReplay{
		Rows: counts.Total, CompressedBytes: compressed.bytes, CompressedSHA256: hex.EncodeToString(compressed.hash.Sum(nil)),
		UncompressedBytes: uncompressed.bytes, UncompressedSHA256: hex.EncodeToString(uncompressed.hash.Sum(nil)),
	}
	return counts, replay, nil
}

func compactSubID(row *schedulea.Row) (uint64, bool) {
	index, exists := schedulea.ColumnIndex("sub_id")
	field, ok := row.Field(index)
	if !exists || !ok || field.IsNull() || len(field.Bytes()) == 0 || len(field.Bytes()) > 1 && field.Bytes()[0] == '0' {
		return 0, false
	}
	value, err := strconv.ParseUint(string(field.Bytes()), 10, 64)
	return value, err == nil && value != 0
}

func buildScheduleACompactPartition(
	ctx context.Context,
	storageRoot, temporaryDirectory string,
	partition, partitions int,
	entries []compactStageEntry,
	sourceArtifactSHA256, relation, cycle string,
	counts *Counts,
	rowExceptions *artifactWriter,
) (ScheduleACompactPartition, []compactKeyState, error) {
	indexWriter, err := newCompactIndexWriter(ctx, storageRoot, temporaryDirectory)
	if err != nil {
		return ScheduleACompactPartition{}, nil, err
	}
	defer indexWriter.Abort()
	keyExceptions, err := newArtifactWriterAt(ctx, storageRoot, temporaryDirectory, compactEvidenceBase, "key-exceptions")
	if err != nil {
		return ScheduleACompactPartition{}, nil, err
	}
	defer keyExceptions.Abort()
	states := make([]compactKeyState, 0, len(entries))
	firstSubID, lastSubID := "", ""
	for start := 0; start < len(entries); {
		end := start + 1
		for end < len(entries) && entries[end].SubID == entries[start].SubID {
			end++
		}
		group := entries[start:end]
		var state compactKeyState
		subID := strconv.FormatUint(group[0].SubID, 10)
		switch {
		case len(group) == 1 && group[0].Valid:
			state = compactKeyState{
				SubID: group[0].SubID, State: "unique", OccurrenceCount: 1,
				RowOrdinal: group[0].RowOrdinal, ComparisonDigest: group[0].Semantic,
			}
			if err := indexWriter.Write(state); err != nil {
				return ScheduleACompactPartition{}, nil, err
			}
			counts.UniqueKeys++
			if firstSubID == "" {
				firstSubID = subID
			}
			lastSubID = subID
		case len(group) == 1:
			state, err = compactExceptionalKeyState(group, "invalid")
			if err != nil {
				return ScheduleACompactPartition{}, nil, err
			}
			counts.InvalidKeys++
			if err := keyExceptions.WriteJSON(compactKeyException(partition, state)); err != nil {
				return ScheduleACompactPartition{}, nil, err
			}
		default:
			state, err = compactExceptionalKeyState(group, "duplicate")
			if err != nil {
				return ScheduleACompactPartition{}, nil, err
			}
			counts.DuplicateKeys++
			counts.DuplicateOccurrences += uint64(len(group))
			if err := keyExceptions.WriteJSON(compactKeyException(partition, state)); err != nil {
				return ScheduleACompactPartition{}, nil, err
			}
			for _, entry := range group {
				occurrenceIdentifier := occurrenceID(sourceArtifactSHA256, relation, cycle, entry.RowOrdinal)
				exception := ScheduleACompactRowException{
					ExceptionID: issueID(occurrenceIdentifier, "duplicate_natural_key"), OccurrenceID: occurrenceIdentifier,
					RowOrdinal: entry.RowOrdinal, RawByteOffset: entry.RawByteOffset, RawByteLength: entry.RawByteLength,
					SubID: subID, Code: "duplicate_natural_key", Severity: "block",
					Message:                "publisher natural key occurs more than once in this snapshot partition",
					RelatedOccurrenceCount: uint64(len(group)),
				}
				if err := rowExceptions.WriteJSON(exception); err != nil {
					return ScheduleACompactPartition{}, nil, err
				}
			}
		}
		states = append(states, state)
		start = end
	}
	indexArtifact, err := indexWriter.Finalize(partition, partitions)
	if err != nil {
		return ScheduleACompactPartition{}, nil, err
	}
	keyExceptionArtifact, err := keyExceptions.Finalize()
	if err != nil {
		return ScheduleACompactPartition{}, nil, err
	}
	verifiedExceptions, err := readCompactKeyExceptionStates(ctx, storageRoot, keyExceptionArtifact, partition, partitions)
	if err != nil {
		return ScheduleACompactPartition{}, nil, err
	}
	uniqueStates := make([]compactKeyState, 0, indexArtifact.RecordCount)
	for _, state := range states {
		if state.State == "unique" {
			uniqueStates = append(uniqueStates, state)
		}
	}
	verifiedStates, err := mergeCompactStates(uniqueStates, verifiedExceptions)
	if err != nil || !reflect.DeepEqual(states, verifiedStates) {
		return ScheduleACompactPartition{}, nil, fmt.Errorf("compact partition %d readback differs from staged key states", partition)
	}
	return ScheduleACompactPartition{
		Partition: partition, FirstSubID: firstSubID, LastSubID: lastSubID,
		Index: indexArtifact, KeyExceptions: keyExceptionArtifact,
	}, states, nil
}

func compactExceptionalKeyState(group []compactStageEntry, state string) (compactKeyState, error) {
	if len(group) == 0 || state != "invalid" && state != "duplicate" {
		return compactKeyState{}, fmt.Errorf("compact exceptional key group is invalid")
	}
	parts := []string{"fec.schedule-a.compact-key-state.v1", strconv.FormatUint(group[0].SubID, 10), state}
	for _, entry := range group {
		parts = append(parts, strconv.FormatUint(entry.RowOrdinal, 10), strconv.FormatBool(entry.Valid), hex.EncodeToString(entry.Semantic[:]))
	}
	digest, err := decodeCompactDigest(digestParts(parts...))
	if err != nil {
		return compactKeyState{}, err
	}
	result := compactKeyState{
		SubID: group[0].SubID, State: state, OccurrenceCount: uint64(len(group)), ComparisonDigest: digest,
	}
	if state == "invalid" {
		result.RowOrdinal = group[0].RowOrdinal
	}
	return result, nil
}

func compactKeyException(partition int, state compactKeyState) ScheduleACompactKeyException {
	return ScheduleACompactKeyException{
		Partition: partition, SubID: strconv.FormatUint(state.SubID, 10), State: state.State,
		OccurrenceCount: state.OccurrenceCount, RowOrdinal: state.RowOrdinal,
		ComparisonDigest: hex.EncodeToString(state.ComparisonDigest[:]),
	}
}

func readScheduleACompactPartitionStates(ctx context.Context, storageRoot string, descriptor ScheduleACompactPartition, partitions int) ([]compactKeyState, error) {
	unique, err := readCompactIndexStates(ctx, storageRoot, descriptor.Index, descriptor.Partition, partitions)
	if err != nil {
		return nil, err
	}
	exceptions, err := readCompactKeyExceptionStates(ctx, storageRoot, descriptor.KeyExceptions, descriptor.Partition, partitions)
	if err != nil {
		return nil, err
	}
	return mergeCompactStates(unique, exceptions)
}

func compareScheduleACompactStates(current, prior []compactKeyState, bootstrap bool, writer *artifactWriter, counts *ChangeCounts) error {
	left, right := 0, 0
	for left < len(current) || right < len(prior) {
		var currentState, priorState *compactKeyState
		switch {
		case right >= len(prior) || left < len(current) && current[left].SubID < prior[right].SubID:
			currentState = &current[left]
			left++
		case left >= len(current) || prior[right].SubID < current[left].SubID:
			priorState = &prior[right]
			right++
		default:
			currentState = &current[left]
			priorState = &prior[right]
			left++
			right++
		}
		delta := scheduleACompactDeltaFor(priorState, currentState, counts)
		if delta == nil {
			continue
		}
		if bootstrap && delta.Change == "added" {
			continue
		}
		if err := writer.WriteJSON(*delta); err != nil {
			return err
		}
	}
	return nil
}

func scheduleACompactDeltaFor(prior, current *compactKeyState, counts *ChangeCounts) *ScheduleACompactDelta {
	delta := &ScheduleACompactDelta{PriorState: "missing", CurrentState: "missing"}
	if prior != nil {
		delta.SubID = strconv.FormatUint(prior.SubID, 10)
		delta.PriorState = prior.State
		delta.PriorRowOrdinal = prior.RowOrdinal
		delta.PriorComparisonDigest = hex.EncodeToString(prior.ComparisonDigest[:])
	}
	if current != nil {
		delta.SubID = strconv.FormatUint(current.SubID, 10)
		delta.CurrentState = current.State
		delta.CurrentRowOrdinal = current.RowOrdinal
		delta.CurrentComparisonDigest = hex.EncodeToString(current.ComparisonDigest[:])
	}
	switch {
	case current == nil:
		delta.Change = "absent"
		counts.Absent++
	case current.State != "unique":
		delta.Change = "invalid"
		counts.Invalid++
	case prior == nil:
		delta.Change = "added"
		counts.Added++
	case prior.State != "unique" || prior.ComparisonDigest != current.ComparisonDigest:
		delta.Change = "changed"
		counts.Changed++
	default:
		counts.Unchanged++
		return nil
	}
	return delta
}

func scheduleACompactChecks(manifest ScheduleACompactManifest, output fecrelease.StagedOutput) []Check {
	var unique, keyExceptions uint64
	for _, partition := range manifest.IndexPartitions {
		unique += partition.Index.RecordCount
		keyExceptions += partition.KeyExceptions.RecordCount
	}
	expectedDeltaRecords := manifest.Changes.Added + manifest.Changes.Changed + manifest.Changes.Absent + manifest.Changes.Invalid
	if manifest.ChangeMode == "bootstrap-dense-membership" {
		expectedDeltaRecords -= manifest.Changes.Added
	}
	return []Check{
		{ID: "source_release_lineage", Passed: true, Severity: "block", Detail: "compact occurrence set names the exact published source release and staged relation"},
		{ID: "staged_output_integrity", Passed: output.RowCount != nil && manifest.SourceReplay.Rows == *output.RowCount, Severity: "block", Detail: "complete staged compressed and uncompressed identities matched"},
		{ID: "dense_occurrence_conservation", Passed: manifest.Counts.Total == manifest.SourceReplay.Rows && manifest.Counts.Total == manifest.Counts.Valid+manifest.Counts.Invalid, Severity: "block", Detail: "every physical source row is represented by the dense ordinal occurrence encoding"},
		{ID: "unique_key_index", Passed: unique == manifest.Counts.UniqueKeys, Severity: "block", Detail: "every valid unique SUB_ID appears once in a fixed-width partitioned index"},
		{ID: "exception_conservation", Passed: keyExceptions == manifest.Counts.InvalidKeys+manifest.Counts.DuplicateKeys && manifest.RowExceptions.RecordCount >= manifest.Counts.Invalid+manifest.Counts.DuplicateOccurrences, Severity: "block", Detail: "invalid and duplicate source/key states remain explicit sparse evidence"},
		{ID: "semantic_change_conservation", Passed: manifest.Deltas.RecordCount == expectedDeltaRecords && manifest.Counts.UniqueKeys+manifest.Counts.InvalidKeys+manifest.Counts.DuplicateKeys == manifest.Changes.Added+manifest.Changes.Changed+manifest.Changes.Unchanged+manifest.Changes.Invalid, Severity: "block", Detail: "bootstrap membership is implicit and every later non-unchanged transition is materialized"},
		{ID: "projection_readiness", Passed: manifest.Counts.Invalid == 0 && manifest.Counts.DuplicateKeys == 0, Severity: "warning", Detail: fmt.Sprintf("%d invalid rows and %d duplicate natural keys require downstream isolation", manifest.Counts.Invalid, manifest.Counts.DuplicateKeys)},
	}
}

func validateScheduleACompactManifest(manifest ScheduleACompactManifest) error {
	if manifest.Schema != "manifest.schema.json" || manifest.SchemaVersion != ScheduleACompactManifestSchemaVersion ||
		manifest.PublisherVersion != ScheduleACompactPublisherVersion || manifest.IndexSchemaVersion != ScheduleACompactIndexSchemaVersion ||
		manifest.OccurrenceEncoding != ScheduleACompactOccurrenceEncoding {
		return fmt.Errorf("unexpected compact occurrence manifest contract")
	}
	for _, digest := range []string{manifest.OccurrenceSetID, manifest.SourceReleaseManifestSHA256, manifest.SourceArtifactSHA256, manifest.StagedOutputSHA256, manifest.SourceReplay.CompressedSHA256, manifest.SourceReplay.UncompressedSHA256} {
		if !digestPattern.MatchString(digest) {
			return fmt.Errorf("compact occurrence manifest contains an invalid digest")
		}
	}
	if manifest.PriorOccurrenceSetID != "" && !digestPattern.MatchString(manifest.PriorOccurrenceSetID) {
		return fmt.Errorf("compact prior occurrence-set ID is invalid")
	}
	if !releaseIDPattern.MatchString(manifest.SourceReleaseID) || !validCycle(manifest.Cycle) || manifest.Relation == "" ||
		manifest.State != "published" || manifest.PublishedAt.IsZero() || !fecrelease.ValidAcquisitionRunID(manifest.RunID) {
		return fmt.Errorf("compact occurrence manifest identity or state is incomplete")
	}
	if manifest.ParserVersion != ParserVersion || manifest.SemanticSchemaVersion != SemanticSchemaVersion ||
		manifest.Configuration.Partitions <= 0 || manifest.Configuration.PartitionHash != ScheduleACompactIndexHash ||
		manifest.Configuration.IndexRecordBytes != ScheduleACompactIndexRecordBytes || manifest.Configuration.IndexEncoding == "" ||
		len(manifest.IndexPartitions) != manifest.Configuration.Partitions {
		return fmt.Errorf("compact occurrence configuration is unsupported")
	}
	if manifest.ChangeMode != "bootstrap-dense-membership" && manifest.ChangeMode != "explicit-inter-release-delta" ||
		manifest.ChangeMode == "bootstrap-dense-membership" && manifest.PriorOccurrenceSetID != "" ||
		manifest.ChangeMode == "explicit-inter-release-delta" && manifest.PriorOccurrenceSetID == "" {
		return fmt.Errorf("compact occurrence change mode is invalid")
	}
	output := fecrelease.StagedOutput{CompressedSHA256: manifest.StagedOutputSHA256, Selection: manifest.Relation}
	expectedID := scheduleACompactOccurrenceSetID(manifest.SourceArtifactSHA256, output, manifest.Cycle, manifest.PriorOccurrenceSetID, manifest.Configuration)
	if manifest.OccurrenceSetID != expectedID {
		return fmt.Errorf("compact occurrence-set ID does not match canonical inputs")
	}
	if manifest.Counts.Total != manifest.Counts.Valid+manifest.Counts.Invalid || manifest.SourceReplay.Rows != manifest.Counts.Total ||
		manifest.Counts.Keyed != manifest.Counts.UniqueKeys+manifest.Counts.InvalidKeys+manifest.Counts.DuplicateOccurrences ||
		manifest.SourceReplay.CompressedBytes == 0 || manifest.SourceReplay.UncompressedBytes == 0 {
		return fmt.Errorf("compact occurrence counts are not conserved")
	}
	var unique, keyExceptions uint64
	for index, partition := range manifest.IndexPartitions {
		if partition.Partition != index {
			return fmt.Errorf("compact index partitions are not canonical")
		}
		if err := validateScheduleACompactArtifact(partition.Index); err != nil {
			return fmt.Errorf("compact index partition %d: %w", index, err)
		}
		if err := validateCompactJSONArtifact(partition.KeyExceptions, "key-exceptions"); err != nil {
			return fmt.Errorf("compact key exceptions partition %d: %w", index, err)
		}
		if partition.Index.RecordCount == 0 && (partition.FirstSubID != "" || partition.LastSubID != "") ||
			partition.Index.RecordCount > 0 && (partition.FirstSubID == "" || partition.LastSubID == "") {
			return fmt.Errorf("compact partition key bounds are invalid")
		}
		unique += partition.Index.RecordCount
		keyExceptions += partition.KeyExceptions.RecordCount
	}
	if err := validateCompactJSONArtifact(manifest.RowExceptions, "row-exceptions"); err != nil {
		return err
	}
	if err := validateCompactJSONArtifact(manifest.Deltas, "deltas"); err != nil {
		return err
	}
	expectedDeltaRecords := manifest.Changes.Added + manifest.Changes.Changed + manifest.Changes.Absent + manifest.Changes.Invalid
	if manifest.ChangeMode == "bootstrap-dense-membership" {
		if manifest.Changes.Changed != 0 || manifest.Changes.Absent != 0 || manifest.Changes.Unchanged != 0 {
			return fmt.Errorf("compact bootstrap change counts are invalid")
		}
		expectedDeltaRecords -= manifest.Changes.Added
	}
	if unique != manifest.Counts.UniqueKeys || keyExceptions != manifest.Counts.InvalidKeys+manifest.Counts.DuplicateKeys ||
		manifest.RowExceptions.RecordCount < manifest.Counts.Invalid+manifest.Counts.DuplicateOccurrences ||
		manifest.Deltas.RecordCount != expectedDeltaRecords ||
		manifest.Counts.UniqueKeys+manifest.Counts.InvalidKeys+manifest.Counts.DuplicateKeys != manifest.Changes.Added+manifest.Changes.Changed+manifest.Changes.Unchanged+manifest.Changes.Invalid {
		return fmt.Errorf("compact index, exception, or change counts are not conserved")
	}
	if len(manifest.Checks) < 6 {
		return fmt.Errorf("compact occurrence manifest is missing checks")
	}
	for _, check := range manifest.Checks {
		if check.Severity == "block" && !check.Passed {
			return fmt.Errorf("blocking compact occurrence check %s failed", check.ID)
		}
	}
	return nil
}

func validateScheduleACompactArtifact(artifact ScheduleACompactArtifact) error {
	if artifact.RecordBytes != ScheduleACompactIndexRecordBytes || artifact.UncompressedBytes != artifact.RecordCount*ScheduleACompactIndexRecordBytes ||
		artifact.Compression != "zstd" || artifact.Encoding != "uint64-be-sub-id,uint64-be-row-ordinal,sha256-semantic-digest" || artifact.CompressedBytes == 0 ||
		!digestPattern.MatchString(artifact.CompressedSHA256) || !digestPattern.MatchString(artifact.UncompressedSHA256) {
		return fmt.Errorf("compact binary artifact identity is invalid")
	}
	expected := filepath.ToSlash(filepath.Join(compactEvidenceBase, "key-index", "sha256", artifact.CompressedSHA256[:2], artifact.CompressedSHA256+".bin.zst"))
	if artifact.StorageKey != expected {
		return fmt.Errorf("compact binary artifact storage key is not canonical")
	}
	return nil
}

func validateCompactJSONArtifact(artifact Artifact, kind string) error {
	if artifact.Compression != "zstd" || artifact.CompressedBytes == 0 || !digestPattern.MatchString(artifact.CompressedSHA256) || !digestPattern.MatchString(artifact.UncompressedSHA256) {
		return fmt.Errorf("compact %s artifact identity is invalid", kind)
	}
	expected := filepath.ToSlash(filepath.Join(compactEvidenceBase, kind, "sha256", artifact.CompressedSHA256[:2], artifact.CompressedSHA256+".jsonl.zst"))
	if artifact.StorageKey != expected {
		return fmt.Errorf("compact %s artifact storage key is not canonical", kind)
	}
	return nil
}

func validateScheduleACompactManifestBacking(ctx context.Context, storageRoot string, current ScheduleACompactManifest) error {
	immutablePath := filepath.Join(storageRoot, compactEvidenceBase, "manifests", current.OccurrenceSetID+".json")
	immutable, err := readScheduleACompactManifestIfPresent(immutablePath)
	if err != nil {
		return err
	}
	if immutable == nil || !reflect.DeepEqual(current, *immutable) {
		return fmt.Errorf("active compact occurrence pointer differs from its immutable manifest")
	}
	for _, partition := range current.IndexPartitions {
		unique, err := readCompactIndexStates(ctx, storageRoot, partition.Index, partition.Partition, current.Configuration.Partitions)
		if err != nil {
			return err
		}
		if len(unique) == 0 && (partition.FirstSubID != "" || partition.LastSubID != "") ||
			len(unique) != 0 && (partition.FirstSubID != strconv.FormatUint(unique[0].SubID, 10) || partition.LastSubID != strconv.FormatUint(unique[len(unique)-1].SubID, 10)) {
			return fmt.Errorf("compact index partition %d bounds differ from its records", partition.Partition)
		}
		if _, err := readCompactKeyExceptionStates(ctx, storageRoot, partition.KeyExceptions, partition.Partition, current.Configuration.Partitions); err != nil {
			return err
		}
	}
	for kind, artifact := range map[string]Artifact{"row-exceptions": current.RowExceptions, "deltas": current.Deltas} {
		path, err := resolveStorageKey(storageRoot, artifact.StorageKey)
		if err != nil {
			return err
		}
		if err := verifyArtifact(ctx, path, artifact); err != nil {
			return fmt.Errorf("compact %s artifact: %w", kind, err)
		}
	}
	return nil
}

func readScheduleACompactManifestIfPresent(path string) (*ScheduleACompactManifest, error) {
	content, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var manifest ScheduleACompactManifest
	decoder := json.NewDecoder(strings.NewReader(string(content)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&manifest); err != nil {
		return nil, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return nil, fmt.Errorf("multiple JSON values in compact occurrence manifest")
		}
		return nil, err
	}
	return &manifest, nil
}
