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
	"regexp"
	"strings"
	"syscall"
	"time"

	"github.com/klauspost/compress/zstd"
	fecrelease "github.com/vedantadhobley/legal-tender/internal/source/fec/release"
	"github.com/vedantadhobley/legal-tender/internal/source/fec/schedulea"
)

var digestPattern = regexp.MustCompile(`^[a-f0-9]{64}$`)
var releaseIDPattern = regexp.MustCompile(`^fec-[a-f0-9]{64}$`)

// Publish streams one selected Schedule A relation into immutable occurrence,
// issue, natural-key, and semantic-change artifacts, then advances only that
// cycle's occurrence pointer.
func Publish(
	ctx context.Context,
	releaseManifest fecrelease.ReleaseManifest,
	releaseManifestSHA256 string,
	cycle string,
	runID string,
	options Options,
) (Manifest, error) {
	if options.Clock == nil {
		options.Clock = time.Now
	}
	if options.ShardCount <= 0 {
		options.ShardCount = defaultShardCount
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
		return Manifest{}, fmt.Errorf("storage root is required")
	}
	if !validCycle(cycle) {
		return Manifest{}, fmt.Errorf("cycle must be a four-digit even year")
	}
	if !fecrelease.ValidAcquisitionRunID(runID) {
		return Manifest{}, fmt.Errorf("run ID contains unsupported characters")
	}
	if !digestPattern.MatchString(releaseManifestSHA256) {
		return Manifest{}, fmt.Errorf("source release manifest SHA-256 is invalid")
	}
	if issues := fecrelease.ValidateKnownManifest(releaseManifest); len(issues) != 0 {
		return Manifest{}, fmt.Errorf("invalid source release: %s", issues[0].Message)
	}
	if !releaseIDPattern.MatchString(releaseManifest.ReleaseID) {
		return Manifest{}, fmt.Errorf("source release ID is invalid")
	}
	output, sourceArtifactSHA256, err := selectedScheduleAOutput(releaseManifest, cycle)
	if err != nil {
		return Manifest{}, err
	}
	currentPath := options.CurrentManifestPath
	if currentPath == "" {
		currentPath = filepath.Join(options.StorageRoot, "evidence", "fec", "schedule-a", "current", cycle+".json")
	}
	if err := requirePathInside(options.StorageRoot, currentPath); err != nil {
		return Manifest{}, err
	}

	lockPath := filepath.Join(options.StorageRoot, "evidence", "fec", "schedule-a", ".publish-"+cycle+".lock")
	unlock, err := lockContext(ctx, lockPath)
	if err != nil {
		return Manifest{}, err
	}
	defer unlock()

	current, err := readManifestIfPresent(currentPath)
	if err != nil {
		return Manifest{}, err
	}
	if current != nil {
		if err := validateManifest(*current); err != nil {
			return Manifest{}, fmt.Errorf("invalid current occurrence manifest: %w", err)
		}
		if err := validateManifestBacking(options.StorageRoot, *current); err != nil {
			return Manifest{}, fmt.Errorf("validate current occurrence publication: %w", err)
		}
		if current.Cycle != cycle {
			return Manifest{}, fmt.Errorf("current occurrence manifest belongs to cycle %s", current.Cycle)
		}
		descends, err := releaseDescendsFrom(ctx, options.StorageRoot, releaseManifest, current.SourceReleaseID)
		if err != nil {
			return Manifest{}, err
		}
		if !descends {
			return Manifest{}, fmt.Errorf("source release %s does not descend from occurrence baseline release %s", releaseManifest.ReleaseID, current.SourceReleaseID)
		}
		if current.SourceArtifactSHA256 == sourceArtifactSHA256 &&
			current.StagedOutputSHA256 == output.CompressedSHA256 &&
			current.ParserVersion == ParserVersion &&
			current.SemanticSchemaVersion == SemanticSchemaVersion {
			return *current, nil
		}
	}

	priorID := ""
	if current != nil {
		priorID = current.OccurrenceSetID
	}
	setID := digestParts(
		"fec.schedule-a.occurrence-set.v1",
		sourceArtifactSHA256,
		output.CompressedSHA256,
		output.Selection,
		cycle,
		ParserVersion,
		SemanticSchemaVersion,
		priorID,
	)
	manifestPath := filepath.Join(options.StorageRoot, "evidence", "fec", "schedule-a", "manifests", setID+".json")
	if existing, err := readManifestIfPresent(manifestPath); err != nil {
		return Manifest{}, err
	} else if existing != nil {
		if err := validateManifest(*existing); err != nil {
			return Manifest{}, fmt.Errorf("invalid immutable occurrence manifest: %w", err)
		}
		if existing.OccurrenceSetID != setID || existing.PriorOccurrenceSetID != priorID ||
			existing.SourceArtifactSHA256 != sourceArtifactSHA256 || existing.StagedOutputSHA256 != output.CompressedSHA256 {
			return Manifest{}, fmt.Errorf("immutable occurrence manifest collision at %s", manifestPath)
		}
		descends, err := releaseDescendsFrom(ctx, options.StorageRoot, releaseManifest, existing.SourceReleaseID)
		if err != nil {
			return Manifest{}, err
		}
		if !descends {
			return Manifest{}, fmt.Errorf("immutable occurrence manifest belongs to an unrelated source release")
		}
		if err := validateArtifactFiles(options.StorageRoot, existing.Artifacts); err != nil {
			return Manifest{}, fmt.Errorf("validate immutable occurrence artifacts: %w", err)
		}
		if err := writeAtomicJSON(currentPath, *existing); err != nil {
			return Manifest{}, err
		}
		return *existing, nil
	}
	if output.UncompressedByteCount > ^uint64(0)-options.WorkingMarginBytes {
		return Manifest{}, fmt.Errorf("occurrence working-space estimate overflows")
	}
	if err := requireOccurrenceStorage(options, output.UncompressedByteCount+options.WorkingMarginBytes); err != nil {
		return Manifest{}, err
	}

	if options.Progress != nil {
		options.Progress("publishing Schedule A occurrences for " + cycle)
	}
	manifest, err := buildOccurrenceSet(ctx, releaseManifest, releaseManifestSHA256, output, sourceArtifactSHA256, cycle, runID, priorID, setID, current, options)
	if err != nil {
		return Manifest{}, err
	}
	if err := validateManifest(manifest); err != nil {
		return Manifest{}, err
	}
	if err := requireOccurrenceStorage(options, 0); err != nil {
		return Manifest{}, err
	}
	if err := writeAtomicJSON(manifestPath, manifest); err != nil {
		return Manifest{}, err
	}
	if err := writeAtomicJSON(currentPath, manifest); err != nil {
		return Manifest{}, err
	}
	return manifest, nil
}

func buildOccurrenceSet(
	ctx context.Context,
	releaseManifest fecrelease.ReleaseManifest,
	releaseManifestSHA256 string,
	output fecrelease.StagedOutput,
	sourceArtifactSHA256 string,
	cycle string,
	runID string,
	priorID string,
	setID string,
	prior *Manifest,
	options Options,
) (Manifest, error) {
	stagedPath, err := resolveStorageKey(options.StorageRoot, output.StorageKey)
	if err != nil {
		return Manifest{}, err
	}
	temporaryDirectory := filepath.Join(options.StorageRoot, "evidence", "fec", "schedule-a", "staging", setID, runID)
	if err := os.MkdirAll(temporaryDirectory, 0o750); err != nil {
		return Manifest{}, err
	}
	defer func() { _ = os.RemoveAll(temporaryDirectory) }()

	occurrences, err := newArtifactWriter(ctx, options.StorageRoot, temporaryDirectory, "occurrences")
	if err != nil {
		return Manifest{}, err
	}
	defer occurrences.Abort()
	issues, err := newArtifactWriter(ctx, options.StorageRoot, temporaryDirectory, "issues")
	if err != nil {
		return Manifest{}, err
	}
	defer issues.Abort()
	naturalIndex, err := newArtifactWriter(ctx, options.StorageRoot, temporaryDirectory, "natural-index")
	if err != nil {
		return Manifest{}, err
	}
	defer naturalIndex.Abort()
	shards, err := newShardSet(ctx, temporaryDirectory, options.ShardCount)
	if err != nil {
		return Manifest{}, err
	}
	defer shards.Remove()

	counts, err := streamOccurrences(ctx, stagedPath, output, sourceArtifactSHA256, cycle, occurrences, issues, shards, options.Progress)
	if err != nil {
		return Manifest{}, err
	}
	if err := shards.BuildIndex(naturalIndex, issues, &counts); err != nil {
		return Manifest{}, err
	}
	occurrenceArtifact, err := occurrences.Finalize()
	if err != nil {
		return Manifest{}, err
	}
	issueArtifact, err := issues.Finalize()
	if err != nil {
		return Manifest{}, err
	}
	indexArtifact, err := naturalIndex.Finalize()
	if err != nil {
		return Manifest{}, err
	}

	changesWriter, err := newArtifactWriter(ctx, options.StorageRoot, temporaryDirectory, "changes")
	if err != nil {
		return Manifest{}, err
	}
	defer changesWriter.Abort()
	var priorIndex *Artifact
	if prior != nil {
		priorIndex = &prior.Artifacts.NaturalIndex
	}
	changes, err := compareNaturalIndexes(ctx, options.StorageRoot, indexArtifact, priorIndex, changesWriter)
	if err != nil {
		return Manifest{}, err
	}
	changeArtifact, err := changesWriter.Finalize()
	if err != nil {
		return Manifest{}, err
	}

	manifest := Manifest{
		Schema:                      "manifest.schema.json",
		SchemaVersion:               ManifestSchemaVersion,
		OccurrenceSetID:             setID,
		PriorOccurrenceSetID:        priorID,
		SourceReleaseID:             releaseManifest.ReleaseID,
		SourceReleaseManifestSHA256: releaseManifestSHA256,
		SourceArtifactSHA256:        sourceArtifactSHA256,
		StagedOutputSHA256:          output.CompressedSHA256,
		Relation:                    output.Selection,
		Cycle:                       cycle,
		RunID:                       runID,
		State:                       "published",
		ParserVersion:               ParserVersion,
		SemanticSchemaVersion:       SemanticSchemaVersion,
		PublishedAt:                 options.Clock().UTC(),
		Counts:                      counts,
		Changes:                     changes,
		Artifacts: ArtifactSet{
			Occurrences:  occurrenceArtifact,
			Issues:       issueArtifact,
			NaturalIndex: indexArtifact,
			Changes:      changeArtifact,
		},
	}
	manifest.Checks = occurrenceChecks(manifest, output)
	return manifest, nil
}

func streamOccurrences(
	ctx context.Context,
	stagedPath string,
	output fecrelease.StagedOutput,
	sourceArtifactSHA256 string,
	cycle string,
	occurrenceWriter *artifactWriter,
	issueWriter *artifactWriter,
	shards *shardSet,
	progress func(string),
) (Counts, error) {
	file, err := os.Open(stagedPath)
	if err != nil {
		return Counts{}, err
	}
	defer func() { _ = file.Close() }()
	compressed := &countingHashReader{reader: &contextReader{ctx: ctx, reader: file}, hash: sha256.New()}
	decoder, err := zstd.NewReader(compressed, zstd.WithDecoderConcurrency(2), zstd.WithDecoderLowmem(true), zstd.WithDecoderMaxMemory(64<<20))
	if err != nil {
		return Counts{}, err
	}
	defer decoder.Close()
	uncompressed := &countingHashReader{reader: decoder, hash: sha256.New()}
	copyDecoder := schedulea.NewDecoder(uncompressed)
	var counts Counts
	var offset uint64
	for copyDecoder.Scan() {
		row := copyDecoder.Row()
		counts.Total++
		raw := row.Raw()
		rawDigestBytes := sha256.Sum256(raw)
		rawDigest := hex.EncodeToString(rawDigestBytes[:])
		occurrenceIdentifier := occurrenceID(sourceArtifactSHA256, output.Selection, cycle, row.Number())
		key, hasKey := rowNaturalKey(row, cycle)
		publisherReference := "unkeyed:" + occurrenceIdentifier
		var keyPointer *string
		if hasKey {
			publisherReference = key
			keyPointer = &key
			counts.Keyed++
		}
		recordVersion := recordVersionID(cycle, publisherReference, rawDigest)
		validationErr := schedulea.Validate(row, cycle)
		occurrence := Occurrence{
			OccurrenceID:             occurrenceIdentifier,
			RowOrdinal:               row.Number(),
			RawByteOffset:            offset,
			RawByteLength:            uint64(len(raw)),
			RawContentSHA256:         rawDigest,
			NaturalKey:               keyPointer,
			PublisherRecordReference: publisherReference,
			RecordVersionID:          recordVersion,
			State:                    "invalid",
			IssueCodes:               []string{},
		}
		if validationErr == nil {
			semantic, digestErr := semanticDigest(row)
			if digestErr != nil {
				return counts, digestErr
			}
			occurrence.State = "valid"
			occurrence.SemanticDigest = &semantic
			counts.Valid++
			if err := shards.Add(shardEntry{
				NaturalKey: key, OccurrenceID: occurrenceIdentifier, RecordVersionID: recordVersion,
				SemanticDigest: semantic, RowOrdinal: row.Number(), Valid: true,
			}); err != nil {
				return counts, err
			}
		} else {
			codes := validationCodes(validationErr)
			occurrence.IssueCodes = codes
			counts.Invalid++
			if hasKey {
				if err := shards.Add(shardEntry{
					NaturalKey: key, OccurrenceID: occurrenceIdentifier, RecordVersionID: recordVersion,
					RowOrdinal: row.Number(), Valid: false,
				}); err != nil {
					return counts, err
				}
			}
			for _, code := range codes {
				issue := Issue{
					IssueID:      issueID(occurrenceIdentifier, code),
					OccurrenceID: occurrenceIdentifier,
					RowOrdinal:   row.Number(),
					NaturalKey:   keyPointer,
					Code:         code,
					Severity:     "block",
					Message:      validationErr.Error(),
				}
				if err := issueWriter.WriteJSON(issue); err != nil {
					return counts, err
				}
			}
		}
		if err := occurrenceWriter.WriteJSON(occurrence); err != nil {
			return counts, err
		}
		offset += uint64(len(raw))
		if progress != nil && counts.Total%1_000_000 == 0 {
			progress(fmt.Sprintf("indexed %d Schedule A occurrences for %s", counts.Total, cycle))
		}
		if counts.Total&0x3fff == 0 {
			if err := ctx.Err(); err != nil {
				return counts, err
			}
		}
	}
	if err := copyDecoder.Err(); err != nil {
		return counts, err
	}
	if err := ctx.Err(); err != nil {
		return counts, err
	}
	if output.RowCount == nil || counts.Total != *output.RowCount {
		return counts, fmt.Errorf("occurrence count %d does not match staged row count", counts.Total)
	}
	if compressed.bytes != output.CompressedByteCount || hex.EncodeToString(compressed.hash.Sum(nil)) != output.CompressedSHA256 {
		return counts, fmt.Errorf("staged Schedule A compressed identity mismatch")
	}
	if uncompressed.bytes != output.UncompressedByteCount || hex.EncodeToString(uncompressed.hash.Sum(nil)) != output.UncompressedSHA256 {
		return counts, fmt.Errorf("staged Schedule A uncompressed identity mismatch")
	}
	return counts, nil
}

func rowNaturalKey(row *schedulea.Row, cycle string) (string, bool) {
	index, exists := schedulea.ColumnIndex("sub_id")
	field, ok := row.Field(index)
	if !exists || !ok || field.IsNull() || len(field.Bytes()) == 0 {
		return "", false
	}
	for _, character := range field.Bytes() {
		if character < '0' || character > '9' {
			return "", false
		}
	}
	return naturalKey(cycle, string(field.Bytes())), true
}

func selectedScheduleAOutput(manifest fecrelease.ReleaseManifest, cycle string) (fecrelease.StagedOutput, string, error) {
	sourceSHA := ""
	for _, artifact := range manifest.Artifacts {
		if artifact.SourceID == fecrelease.ScheduleASourceID {
			sourceSHA = artifact.SHA256
			break
		}
	}
	if sourceSHA == "" {
		return fecrelease.StagedOutput{}, "", fmt.Errorf("published release has no Schedule A source artifact")
	}
	var selected *fecrelease.StagedOutput
	for index := range manifest.StagedOutputs {
		output := &manifest.StagedOutputs[index]
		if output.SourceID == fecrelease.ScheduleASourceID && output.SelectionKind == "relation" && output.Period == cycle {
			if selected != nil {
				return fecrelease.StagedOutput{}, "", fmt.Errorf("published release has multiple Schedule A outputs for %s", cycle)
			}
			selected = output
		}
	}
	if selected == nil {
		return fecrelease.StagedOutput{}, "", fmt.Errorf("published release has no Schedule A output for %s", cycle)
	}
	if selected.SourceArtifactSHA256 != sourceSHA {
		return fecrelease.StagedOutput{}, "", fmt.Errorf("Schedule A staged output belongs to another source artifact")
	}
	return *selected, sourceSHA, nil
}

func occurrenceChecks(manifest Manifest, output fecrelease.StagedOutput) []Check {
	return []Check{
		{ID: "source_release_lineage", Passed: true, Severity: "block", Detail: "occurrence set names the exact published source release and staged relation"},
		{ID: "staged_output_integrity", Passed: true, Severity: "block", Detail: "complete staged compressed and uncompressed identities matched"},
		{ID: "occurrence_conservation", Passed: output.RowCount != nil && manifest.Counts.Total == *output.RowCount && manifest.Artifacts.Occurrences.RecordCount == manifest.Counts.Total, Severity: "block", Detail: fmt.Sprintf("published %d occurrences from %d staged rows", manifest.Counts.Total, manifest.Counts.Total)},
		{ID: "row_state_conservation", Passed: manifest.Counts.Total == manifest.Counts.Valid+manifest.Counts.Invalid, Severity: "block", Detail: "every occurrence is valid or explicitly invalid"},
		{ID: "natural_key_index", Passed: manifest.Artifacts.NaturalIndex.RecordCount == manifest.Counts.UniqueKeys+manifest.Counts.InvalidKeys+manifest.Counts.DuplicateKeys && manifest.Counts.Keyed == manifest.Counts.UniqueKeys+manifest.Counts.InvalidKeys+manifest.Counts.DuplicateOccurrences, Severity: "block", Detail: "natural-key states were externally sharded, sorted, and conserved"},
		{ID: "semantic_change_set", Passed: manifest.Artifacts.Changes.RecordCount == manifest.Changes.Added+manifest.Changes.Changed+manifest.Changes.Absent+manifest.Changes.Invalid, Severity: "block", Detail: "all non-unchanged natural-key transitions are materialized"},
		{ID: "projection_readiness", Passed: manifest.Counts.Invalid == 0 && manifest.Counts.DuplicateKeys == 0, Severity: "warning", Detail: fmt.Sprintf("%d invalid rows and %d duplicate natural keys require downstream isolation", manifest.Counts.Invalid, manifest.Counts.DuplicateKeys)},
	}
}

func validateManifest(manifest Manifest) error {
	if manifest.Schema != "manifest.schema.json" || manifest.SchemaVersion != ManifestSchemaVersion {
		return fmt.Errorf("unexpected occurrence manifest schema")
	}
	if !releaseIDPattern.MatchString(manifest.SourceReleaseID) {
		return fmt.Errorf("occurrence manifest source release ID is invalid")
	}
	for _, digest := range []string{manifest.OccurrenceSetID, manifest.SourceReleaseManifestSHA256, manifest.SourceArtifactSHA256, manifest.StagedOutputSHA256} {
		if !digestPattern.MatchString(digest) {
			return fmt.Errorf("occurrence manifest contains an invalid digest")
		}
	}
	if manifest.PriorOccurrenceSetID != "" && !digestPattern.MatchString(manifest.PriorOccurrenceSetID) {
		return fmt.Errorf("prior occurrence-set ID is invalid")
	}
	if !validCycle(manifest.Cycle) || manifest.Relation == "" || manifest.State != "published" || manifest.PublishedAt.IsZero() {
		return fmt.Errorf("occurrence manifest identity or state is incomplete")
	}
	if manifest.ParserVersion != ParserVersion || manifest.SemanticSchemaVersion != SemanticSchemaVersion {
		return fmt.Errorf("occurrence manifest parser contract is unsupported")
	}
	expectedSetID := digestParts(
		"fec.schedule-a.occurrence-set.v1",
		manifest.SourceArtifactSHA256,
		manifest.StagedOutputSHA256,
		manifest.Relation,
		manifest.Cycle,
		manifest.ParserVersion,
		manifest.SemanticSchemaVersion,
		manifest.PriorOccurrenceSetID,
	)
	if manifest.OccurrenceSetID != expectedSetID {
		return fmt.Errorf("occurrence-set ID does not match its canonical inputs")
	}
	if !fecrelease.ValidAcquisitionRunID(manifest.RunID) {
		return fmt.Errorf("occurrence manifest run ID is invalid")
	}
	for kind, artifact := range map[string]Artifact{
		"occurrences":   manifest.Artifacts.Occurrences,
		"issues":        manifest.Artifacts.Issues,
		"natural-index": manifest.Artifacts.NaturalIndex,
		"changes":       manifest.Artifacts.Changes,
	} {
		if artifact.Compression != "zstd" || !digestPattern.MatchString(artifact.CompressedSHA256) || !digestPattern.MatchString(artifact.UncompressedSHA256) || artifact.CompressedBytes == 0 {
			return fmt.Errorf("%s artifact identity is invalid", kind)
		}
		prefix := filepath.ToSlash(filepath.Join("evidence", "fec", "schedule-a", kind, "sha256", artifact.CompressedSHA256[:2])) + "/"
		if !strings.HasPrefix(artifact.StorageKey, prefix) {
			return fmt.Errorf("%s artifact storage key is not canonical", kind)
		}
	}
	if manifest.Counts.Total != manifest.Counts.Valid+manifest.Counts.Invalid ||
		manifest.Artifacts.Occurrences.RecordCount != manifest.Counts.Total {
		return fmt.Errorf("occurrence counts are not conserved")
	}
	if manifest.Artifacts.NaturalIndex.RecordCount != manifest.Counts.UniqueKeys+manifest.Counts.InvalidKeys+manifest.Counts.DuplicateKeys ||
		manifest.Counts.Keyed != manifest.Counts.UniqueKeys+manifest.Counts.InvalidKeys+manifest.Counts.DuplicateOccurrences {
		return fmt.Errorf("natural-key counts are not conserved")
	}
	if manifest.Artifacts.Issues.RecordCount < manifest.Counts.Invalid+manifest.Counts.DuplicateOccurrences {
		return fmt.Errorf("issue counts do not cover invalid and duplicate occurrences")
	}
	if manifest.Artifacts.Changes.RecordCount != manifest.Changes.Added+manifest.Changes.Changed+manifest.Changes.Absent+manifest.Changes.Invalid ||
		manifest.Artifacts.NaturalIndex.RecordCount != manifest.Changes.Added+manifest.Changes.Changed+manifest.Changes.Unchanged+manifest.Changes.Invalid {
		return fmt.Errorf("semantic change counts are not conserved")
	}
	if len(manifest.Checks) < 6 {
		return fmt.Errorf("occurrence manifest is missing required checks")
	}
	for _, check := range manifest.Checks {
		if check.Severity == "block" && !check.Passed {
			return fmt.Errorf("blocking occurrence check %s failed", check.ID)
		}
	}
	return nil
}

func validateManifestBacking(storageRoot string, current Manifest) error {
	immutablePath := filepath.Join(storageRoot, "evidence", "fec", "schedule-a", "manifests", current.OccurrenceSetID+".json")
	immutable, err := readManifestIfPresent(immutablePath)
	if err != nil {
		return err
	}
	if immutable == nil {
		return fmt.Errorf("immutable occurrence manifest is missing")
	}
	if err := validateManifest(*immutable); err != nil {
		return fmt.Errorf("immutable occurrence manifest is invalid: %w", err)
	}
	if !reflect.DeepEqual(current, *immutable) {
		return fmt.Errorf("active occurrence pointer differs from its immutable manifest")
	}
	return validateArtifactFiles(storageRoot, current.Artifacts)
}

func validateArtifactFiles(storageRoot string, artifacts ArtifactSet) error {
	for kind, artifact := range map[string]Artifact{
		"occurrences":   artifacts.Occurrences,
		"issues":        artifacts.Issues,
		"natural-index": artifacts.NaturalIndex,
		"changes":       artifacts.Changes,
	} {
		path, err := resolveStorageKey(storageRoot, artifact.StorageKey)
		if err != nil {
			return err
		}
		info, err := os.Stat(path)
		if err != nil {
			return fmt.Errorf("%s artifact: %w", kind, err)
		}
		if !info.Mode().IsRegular() || info.Size() < 0 || uint64(info.Size()) != artifact.CompressedBytes {
			return fmt.Errorf("%s artifact does not match its published size", kind)
		}
	}
	return nil
}

func readManifestIfPresent(path string) (*Manifest, error) {
	content, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var manifest Manifest
	decoder := json.NewDecoder(strings.NewReader(string(content)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&manifest); err != nil {
		return nil, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return nil, fmt.Errorf("multiple JSON values in occurrence manifest")
		}
		return nil, err
	}
	return &manifest, nil
}

func releaseDescendsFrom(ctx context.Context, storageRoot string, candidate fecrelease.ReleaseManifest, ancestorID string) (bool, error) {
	current := candidate
	seen := map[string]struct{}{}
	for {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		if current.ReleaseID == ancestorID {
			return true, nil
		}
		if current.PriorReleaseID == "" {
			return false, nil
		}
		if !releaseIDPattern.MatchString(current.PriorReleaseID) {
			return false, fmt.Errorf("source release prior ID is invalid")
		}
		if _, duplicate := seen[current.ReleaseID]; duplicate {
			return false, fmt.Errorf("cycle in coordinated source release history")
		}
		seen[current.ReleaseID] = struct{}{}
		path := filepath.Join(storageRoot, "releases", "fec", "manifests", current.PriorReleaseID+".json")
		content, err := os.ReadFile(path)
		if err != nil {
			return false, fmt.Errorf("read source release ancestor %s: %w", current.PriorReleaseID, err)
		}
		var ancestor fecrelease.ReleaseManifest
		decoder := json.NewDecoder(strings.NewReader(string(content)))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&ancestor); err != nil {
			return false, err
		}
		var trailing any
		if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
			if err == nil {
				return false, fmt.Errorf("multiple JSON values in source release ancestor")
			}
			return false, err
		}
		if ancestor.ReleaseID != current.PriorReleaseID {
			return false, fmt.Errorf("source release ancestor path contains release %s; want %s", ancestor.ReleaseID, current.PriorReleaseID)
		}
		if issues := fecrelease.ValidateKnownManifest(ancestor); len(issues) != 0 {
			return false, fmt.Errorf("invalid source release ancestor %s: %s", ancestor.ReleaseID, issues[0].Message)
		}
		current = ancestor
	}
}

func lockContext(ctx context.Context, path string) (func(), error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return nil, err
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o640)
	if err != nil {
		return nil, err
	}
	for {
		err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
		if err == nil {
			return func() {
				_ = syscall.Flock(int(file.Fd()), syscall.LOCK_UN)
				_ = file.Close()
			}, nil
		}
		if err != syscall.EWOULDBLOCK && err != syscall.EAGAIN {
			_ = file.Close()
			return nil, err
		}
		select {
		case <-ctx.Done():
			_ = file.Close()
			return nil, ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func writeAtomicJSON(path string, value any) error {
	content, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	content = append(content, '\n')
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return err
	}
	temporary, err := os.CreateTemp(filepath.Dir(path), ".pending-*.json")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	defer func() { _ = os.Remove(temporaryPath) }()
	if err := temporary.Chmod(0o640); err != nil {
		_ = temporary.Close()
		return err
	}
	if _, err := temporary.Write(content); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	if err := os.Rename(temporaryPath, path); err != nil {
		return err
	}
	return syncDirectory(filepath.Dir(path))
}

func requirePathInside(root, path string) error {
	rootAbsolute, err := filepath.Abs(root)
	if err != nil {
		return err
	}
	pathAbsolute, err := filepath.Abs(path)
	if err != nil {
		return err
	}
	relative, err := filepath.Rel(rootAbsolute, pathAbsolute)
	if err != nil {
		return err
	}
	if relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return fmt.Errorf("occurrence current-manifest path escapes storage root")
	}
	return nil
}

func validCycle(cycle string) bool {
	if len(cycle) != 4 {
		return false
	}
	for _, character := range cycle {
		if character < '0' || character > '9' {
			return false
		}
	}
	return (cycle[3]-'0')%2 == 0
}

func requireOccurrenceStorage(options Options, reserve uint64) error {
	available, err := options.DiskAvailable(options.StorageRoot)
	if err != nil {
		return fmt.Errorf("inspect occurrence storage: %w", err)
	}
	if available < options.FreeFloorBytes || reserve > available-options.FreeFloorBytes {
		return fmt.Errorf("occurrence publication would leave less than the required free-space floor")
	}
	return nil
}

func availableBytes(path string) (uint64, error) {
	var statistics syscall.Statfs_t
	if err := syscall.Statfs(path, &statistics); err != nil {
		return 0, err
	}
	return statistics.Bavail * uint64(statistics.Bsize), nil
}
