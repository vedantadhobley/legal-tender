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
	"sort"
	"strings"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/vedantadhobley/legal-tender/internal/source/fec/classic"
	fecrelease "github.com/vedantadhobley/legal-tender/internal/source/fec/release"
)

const (
	ClassicManifestSchemaVersion = "legal-tender.fec.classic-occurrence-set.v1"
	ClassicSemanticSchemaVersion = "legal-tender.fec.classic-semantic-digest.v1"
)

type ClassicManifest struct {
	Schema                      string       `json:"$schema"`
	SchemaVersion               string       `json:"schema_version"`
	OccurrenceSetID             string       `json:"occurrence_set_id"`
	PriorOccurrenceSetID        string       `json:"prior_occurrence_set_id,omitempty"`
	Dataset                     string       `json:"dataset"`
	SourceContract              string       `json:"source_contract"`
	SourceID                    string       `json:"source_id"`
	SourceReleaseID             string       `json:"source_release_id"`
	SourceReleaseManifestSHA256 string       `json:"source_release_manifest_sha256"`
	SourceArtifactSHA256        string       `json:"source_artifact_sha256"`
	StagedOutputSHA256          string       `json:"staged_output_sha256"`
	Member                      string       `json:"member"`
	Cycle                       string       `json:"cycle"`
	RunID                       string       `json:"run_id"`
	State                       string       `json:"state"`
	ParserVersion               string       `json:"parser_version"`
	SemanticSchemaVersion       string       `json:"semantic_schema_version"`
	PublishedAt                 time.Time    `json:"published_at"`
	Counts                      Counts       `json:"counts"`
	Changes                     ChangeCounts `json:"changes"`
	Artifacts                   ArtifactSet  `json:"artifacts"`
	Checks                      []Check      `json:"checks"`
}

// PublishClassic publishes one exact cycle/member stream. It does no entity
// resolution, relationship inference, or monetary aggregation.
func PublishClassic(
	ctx context.Context,
	releaseManifest fecrelease.ReleaseManifest,
	releaseManifestSHA256 string,
	dataset string,
	cycle string,
	runID string,
	options Options,
) (ClassicManifest, error) {
	spec, err := classic.Lookup(dataset)
	if err != nil {
		return ClassicManifest{}, err
	}
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
		return ClassicManifest{}, fmt.Errorf("storage root is required")
	}
	if !validCycle(cycle) {
		return ClassicManifest{}, fmt.Errorf("cycle must be a four-digit even year")
	}
	if !fecrelease.ValidAcquisitionRunID(runID) {
		return ClassicManifest{}, fmt.Errorf("run ID contains unsupported characters")
	}
	if !digestPattern.MatchString(releaseManifestSHA256) {
		return ClassicManifest{}, fmt.Errorf("source release manifest SHA-256 is invalid")
	}
	if issues := fecrelease.ValidateKnownManifest(releaseManifest); len(issues) != 0 {
		return ClassicManifest{}, fmt.Errorf("invalid source release: %s", issues[0].Message)
	}
	if !releaseIDPattern.MatchString(releaseManifest.ReleaseID) {
		return ClassicManifest{}, fmt.Errorf("source release ID is invalid")
	}
	output, sourceArtifactSHA256, err := selectedClassicOutput(releaseManifest, spec, cycle)
	if err != nil {
		return ClassicManifest{}, err
	}
	basePath := classicEvidenceBase(spec)
	currentPath := options.CurrentManifestPath
	if currentPath == "" {
		currentPath = filepath.Join(options.StorageRoot, basePath, "current", cycle+".json")
	}
	if err := requirePathInside(options.StorageRoot, currentPath); err != nil {
		return ClassicManifest{}, err
	}

	lockPath := filepath.Join(options.StorageRoot, basePath, ".publish-"+cycle+".lock")
	unlock, err := lockContext(ctx, lockPath)
	if err != nil {
		return ClassicManifest{}, err
	}
	defer unlock()

	current, err := readClassicManifestIfPresent(currentPath)
	if err != nil {
		return ClassicManifest{}, err
	}
	if current != nil {
		if err := validateClassicManifest(*current); err != nil {
			return ClassicManifest{}, fmt.Errorf("invalid current classic occurrence manifest: %w", err)
		}
		if err := validateClassicManifestBacking(options.StorageRoot, *current); err != nil {
			return ClassicManifest{}, fmt.Errorf("validate current classic occurrence publication: %w", err)
		}
		if current.Dataset != dataset || current.Cycle != cycle {
			return ClassicManifest{}, fmt.Errorf("current classic occurrence manifest belongs to %s/%s", current.Dataset, current.Cycle)
		}
		descends, err := releaseDescendsFrom(ctx, options.StorageRoot, releaseManifest, current.SourceReleaseID)
		if err != nil {
			return ClassicManifest{}, err
		}
		if !descends {
			return ClassicManifest{}, fmt.Errorf("source release %s does not descend from occurrence baseline release %s", releaseManifest.ReleaseID, current.SourceReleaseID)
		}
		if current.SourceArtifactSHA256 == sourceArtifactSHA256 &&
			current.StagedOutputSHA256 == output.CompressedSHA256 &&
			current.ParserVersion == classic.ParserVersion &&
			current.SemanticSchemaVersion == ClassicSemanticSchemaVersion {
			return *current, nil
		}
	}

	priorID := ""
	if current != nil {
		priorID = current.OccurrenceSetID
	}
	setID := digestParts(
		"fec.classic.occurrence-set.v1", string(spec.Dataset), sourceArtifactSHA256,
		output.CompressedSHA256, output.Selection, cycle, classic.ParserVersion,
		ClassicSemanticSchemaVersion, priorID,
	)
	manifestPath := filepath.Join(options.StorageRoot, basePath, "manifests", setID+".json")
	if existing, err := readClassicManifestIfPresent(manifestPath); err != nil {
		return ClassicManifest{}, err
	} else if existing != nil {
		if err := validateClassicManifest(*existing); err != nil {
			return ClassicManifest{}, fmt.Errorf("invalid immutable classic occurrence manifest: %w", err)
		}
		if existing.OccurrenceSetID != setID || existing.PriorOccurrenceSetID != priorID ||
			existing.SourceArtifactSHA256 != sourceArtifactSHA256 || existing.StagedOutputSHA256 != output.CompressedSHA256 {
			return ClassicManifest{}, fmt.Errorf("immutable classic occurrence manifest collision at %s", manifestPath)
		}
		descends, err := releaseDescendsFrom(ctx, options.StorageRoot, releaseManifest, existing.SourceReleaseID)
		if err != nil {
			return ClassicManifest{}, err
		}
		if !descends {
			return ClassicManifest{}, fmt.Errorf("immutable classic occurrence manifest belongs to an unrelated source release")
		}
		if err := validateArtifactFilesAt(options.StorageRoot, classicEvidenceBase(spec), existing.Artifacts); err != nil {
			return ClassicManifest{}, fmt.Errorf("validate immutable classic occurrence artifacts: %w", err)
		}
		if err := writeAtomicJSON(currentPath, *existing); err != nil {
			return ClassicManifest{}, err
		}
		return *existing, nil
	}
	if output.UncompressedByteCount > ^uint64(0)-options.WorkingMarginBytes {
		return ClassicManifest{}, fmt.Errorf("classic occurrence working-space estimate overflows")
	}
	if err := requireOccurrenceStorage(options, output.UncompressedByteCount+options.WorkingMarginBytes); err != nil {
		return ClassicManifest{}, err
	}
	if options.Progress != nil {
		options.Progress(fmt.Sprintf("publishing %s occurrences for %s", dataset, cycle))
	}
	manifest, err := buildClassicOccurrenceSet(ctx, releaseManifest, releaseManifestSHA256, output, sourceArtifactSHA256, spec, cycle, runID, priorID, setID, current, options)
	if err != nil {
		return ClassicManifest{}, err
	}
	if err := validateClassicManifest(manifest); err != nil {
		return ClassicManifest{}, err
	}
	if err := requireOccurrenceStorage(options, 0); err != nil {
		return ClassicManifest{}, err
	}
	if err := writeAtomicJSON(manifestPath, manifest); err != nil {
		return ClassicManifest{}, err
	}
	if err := writeAtomicJSON(currentPath, manifest); err != nil {
		return ClassicManifest{}, err
	}
	return manifest, nil
}

func buildClassicOccurrenceSet(
	ctx context.Context,
	releaseManifest fecrelease.ReleaseManifest,
	releaseManifestSHA256 string,
	output fecrelease.StagedOutput,
	sourceArtifactSHA256 string,
	spec classic.Spec,
	cycle string,
	runID string,
	priorID string,
	setID string,
	prior *ClassicManifest,
	options Options,
) (ClassicManifest, error) {
	stagedPath, err := resolveStorageKey(options.StorageRoot, output.StorageKey)
	if err != nil {
		return ClassicManifest{}, err
	}
	basePath := classicEvidenceBase(spec)
	temporaryDirectory := filepath.Join(options.StorageRoot, basePath, "staging", setID, runID)
	if err := os.MkdirAll(temporaryDirectory, 0o750); err != nil {
		return ClassicManifest{}, err
	}
	defer func() { _ = os.RemoveAll(temporaryDirectory) }()

	occurrences, err := newArtifactWriterAt(ctx, options.StorageRoot, temporaryDirectory, basePath, "occurrences")
	if err != nil {
		return ClassicManifest{}, err
	}
	defer occurrences.Abort()
	issues, err := newArtifactWriterAt(ctx, options.StorageRoot, temporaryDirectory, basePath, "issues")
	if err != nil {
		return ClassicManifest{}, err
	}
	defer issues.Abort()
	naturalIndex, err := newArtifactWriterAt(ctx, options.StorageRoot, temporaryDirectory, basePath, "natural-index")
	if err != nil {
		return ClassicManifest{}, err
	}
	defer naturalIndex.Abort()
	shards, err := newConfiguredShardSet(
		ctx, temporaryDirectory, options.ShardCount,
		"fec.classic.natural-state.v1:"+string(spec.Dataset),
		"fec.classic.issue.v1:"+string(spec.Dataset),
		"publisher natural key occurs more than once in this dataset snapshot partition",
	)
	if err != nil {
		return ClassicManifest{}, err
	}
	defer shards.Remove()

	counts, err := streamClassicOccurrences(ctx, stagedPath, output, sourceArtifactSHA256, spec, cycle, occurrences, issues, shards, options.Progress)
	if err != nil {
		return ClassicManifest{}, err
	}
	if err := shards.BuildIndex(naturalIndex, issues, &counts); err != nil {
		return ClassicManifest{}, err
	}
	occurrenceArtifact, err := occurrences.Finalize()
	if err != nil {
		return ClassicManifest{}, err
	}
	issueArtifact, err := issues.Finalize()
	if err != nil {
		return ClassicManifest{}, err
	}
	indexArtifact, err := naturalIndex.Finalize()
	if err != nil {
		return ClassicManifest{}, err
	}

	changesWriter, err := newArtifactWriterAt(ctx, options.StorageRoot, temporaryDirectory, basePath, "changes")
	if err != nil {
		return ClassicManifest{}, err
	}
	defer changesWriter.Abort()
	var priorIndex *Artifact
	if prior != nil {
		priorIndex = &prior.Artifacts.NaturalIndex
	}
	changes, err := compareNaturalIndexes(ctx, options.StorageRoot, indexArtifact, priorIndex, changesWriter)
	if err != nil {
		return ClassicManifest{}, err
	}
	changeArtifact, err := changesWriter.Finalize()
	if err != nil {
		return ClassicManifest{}, err
	}

	manifest := ClassicManifest{
		Schema: "manifest.schema.json", SchemaVersion: ClassicManifestSchemaVersion,
		OccurrenceSetID: setID, PriorOccurrenceSetID: priorID,
		Dataset: string(spec.Dataset), SourceContract: spec.SourceContract,
		SourceID: output.SourceID, SourceReleaseID: releaseManifest.ReleaseID,
		SourceReleaseManifestSHA256: releaseManifestSHA256,
		SourceArtifactSHA256:        sourceArtifactSHA256, StagedOutputSHA256: output.CompressedSHA256,
		Member: output.Selection, Cycle: cycle, RunID: runID, State: "published",
		ParserVersion: classic.ParserVersion, SemanticSchemaVersion: ClassicSemanticSchemaVersion,
		PublishedAt: options.Clock().UTC(), Counts: counts, Changes: changes,
		Artifacts: ArtifactSet{Occurrences: occurrenceArtifact, Issues: issueArtifact, NaturalIndex: indexArtifact, Changes: changeArtifact},
	}
	manifest.Checks = classicOccurrenceChecks(manifest)
	return manifest, nil
}

func streamClassicOccurrences(
	ctx context.Context,
	stagedPath string,
	output fecrelease.StagedOutput,
	sourceArtifactSHA256 string,
	spec classic.Spec,
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
	rowDecoder := classic.NewDecoder(uncompressed)
	var counts Counts
	var offset uint64
	for rowDecoder.Scan() {
		row := rowDecoder.Row()
		counts.Total++
		raw := row.Raw()
		rawDigestBytes := sha256.Sum256(raw)
		rawDigest := hex.EncodeToString(rawDigestBytes[:])
		occurrenceIdentifier := digestParts("fec.classic.occurrence.v1", string(spec.Dataset), sourceArtifactSHA256, output.Selection, cycle, fmt.Sprintf("%d", row.Number()))
		rawKey, hasRawKey := row.NaturalKeyValue(spec)
		hasKey := hasRawKey && spec.ValidNaturalKey(rawKey)
		publisherReference := "unkeyed:" + occurrenceIdentifier
		var key string
		var keyPointer *string
		if hasKey {
			key = fmt.Sprintf("fec:%s:%s:%s", spec.Dataset, cycle, rawKey)
			publisherReference = key
			keyPointer = &key
			counts.Keyed++
		}
		recordVersion := digestParts("fec.classic.record-version.v1", string(spec.Dataset), cycle, publisherReference, rawDigest)
		validationIssues := row.Validate(spec, cycle)
		occurrence := Occurrence{
			OccurrenceID: occurrenceIdentifier, RowOrdinal: row.Number(), RawByteOffset: offset,
			RawByteLength: uint64(len(raw)), RawContentSHA256: rawDigest, NaturalKey: keyPointer,
			PublisherRecordReference: publisherReference, RecordVersionID: recordVersion,
			State: "invalid", IssueCodes: []string{},
		}
		if len(validationIssues) == 0 {
			values, ok := row.Values(spec)
			if !ok {
				return counts, fmt.Errorf("validated classic row has no canonical values")
			}
			semantic := digestParts(append([]string{ClassicSemanticSchemaVersion, string(spec.Dataset)}, values...)...)
			occurrence.State = "valid"
			occurrence.SemanticDigest = &semantic
			counts.Valid++
			if err := shards.Add(shardEntry{NaturalKey: key, OccurrenceID: occurrenceIdentifier, RecordVersionID: recordVersion, SemanticDigest: semantic, RowOrdinal: row.Number(), Valid: true}); err != nil {
				return counts, err
			}
		} else {
			counts.Invalid++
			if hasKey {
				if err := shards.Add(shardEntry{NaturalKey: key, OccurrenceID: occurrenceIdentifier, RecordVersionID: recordVersion, RowOrdinal: row.Number(), Valid: false}); err != nil {
					return counts, err
				}
			}
			grouped := make(map[string][]string)
			for _, issue := range validationIssues {
				grouped[issue.Code] = append(grouped[issue.Code], issue.Message)
			}
			codes := make([]string, 0, len(grouped))
			for code := range grouped {
				codes = append(codes, code)
			}
			sort.Strings(codes)
			occurrence.IssueCodes = append(occurrence.IssueCodes, codes...)
			for _, code := range codes {
				issue := Issue{
					IssueID:      digestParts("fec.classic.issue.v1:"+string(spec.Dataset), occurrenceIdentifier, code),
					OccurrenceID: occurrenceIdentifier, RowOrdinal: row.Number(), NaturalKey: keyPointer,
					Code: code, Severity: "block", Message: strings.Join(grouped[code], "; "),
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
		if progress != nil && counts.Total%100_000 == 0 {
			progress(fmt.Sprintf("indexed %d %s occurrences for %s", counts.Total, spec.Dataset, cycle))
		}
		if counts.Total&0x3fff == 0 {
			if err := ctx.Err(); err != nil {
				return counts, err
			}
		}
	}
	if err := rowDecoder.Err(); err != nil {
		return counts, err
	}
	if err := ctx.Err(); err != nil {
		return counts, err
	}
	if compressed.bytes != output.CompressedByteCount || hex.EncodeToString(compressed.hash.Sum(nil)) != output.CompressedSHA256 {
		return counts, fmt.Errorf("staged %s compressed identity mismatch", spec.Dataset)
	}
	if uncompressed.bytes != output.UncompressedByteCount || hex.EncodeToString(uncompressed.hash.Sum(nil)) != output.UncompressedSHA256 {
		return counts, fmt.Errorf("staged %s uncompressed identity mismatch", spec.Dataset)
	}
	return counts, nil
}

func selectedClassicOutput(manifest fecrelease.ReleaseManifest, spec classic.Spec, cycle string) (fecrelease.StagedOutput, string, error) {
	sourceID := fmt.Sprintf("fec:%s:%s", spec.Code, cycle)
	sourceSHA := ""
	for _, artifact := range manifest.Artifacts {
		if artifact.SourceID == sourceID {
			sourceSHA = artifact.SHA256
			break
		}
	}
	if sourceSHA == "" {
		return fecrelease.StagedOutput{}, "", fmt.Errorf("published release has no %s source artifact for %s", spec.Dataset, cycle)
	}
	var selected *fecrelease.StagedOutput
	for index := range manifest.StagedOutputs {
		output := &manifest.StagedOutputs[index]
		if output.SourceID == sourceID && output.SelectionKind == "member" && output.Period == cycle {
			if selected != nil {
				return fecrelease.StagedOutput{}, "", fmt.Errorf("published release has multiple %s outputs for %s", spec.Dataset, cycle)
			}
			selected = output
		}
	}
	if selected == nil {
		return fecrelease.StagedOutput{}, "", fmt.Errorf("published release has no %s output for %s", spec.Dataset, cycle)
	}
	if selected.SourceArtifactSHA256 != sourceSHA {
		return fecrelease.StagedOutput{}, "", fmt.Errorf("%s staged output belongs to another source artifact", spec.Dataset)
	}
	if selected.Selection != classicMember(spec, cycle) {
		return fecrelease.StagedOutput{}, "", fmt.Errorf("%s staged output member is %q; want %q", spec.Dataset, selected.Selection, classicMember(spec, cycle))
	}
	return *selected, sourceSHA, nil
}

func classicMember(spec classic.Spec, cycle string) string {
	switch spec.Dataset {
	case classic.AllCandidatesSummary, classic.CurrentCampaignsSummary:
		return spec.Code + cycle[2:] + ".txt"
	default:
		return spec.Code + ".txt"
	}
}

func classicEvidenceBase(spec classic.Spec) string {
	return filepath.Join("evidence", "fec", "classic", string(spec.Dataset))
}

func classicOccurrenceChecks(manifest ClassicManifest) []Check {
	return []Check{
		{ID: "source_release_lineage", Passed: true, Severity: "block", Detail: "occurrence set names the exact published source release and selected member"},
		{ID: "staged_output_integrity", Passed: true, Severity: "block", Detail: "complete staged compressed and uncompressed identities matched"},
		{ID: "occurrence_conservation", Passed: manifest.Artifacts.Occurrences.RecordCount == manifest.Counts.Total, Severity: "block", Detail: fmt.Sprintf("published %d occurrences from the complete selected member", manifest.Counts.Total)},
		{ID: "row_state_conservation", Passed: manifest.Counts.Total == manifest.Counts.Valid+manifest.Counts.Invalid, Severity: "block", Detail: "every occurrence is valid or explicitly invalid"},
		{ID: "natural_key_index", Passed: manifest.Artifacts.NaturalIndex.RecordCount == manifest.Counts.UniqueKeys+manifest.Counts.InvalidKeys+manifest.Counts.DuplicateKeys && manifest.Counts.Keyed == manifest.Counts.UniqueKeys+manifest.Counts.InvalidKeys+manifest.Counts.DuplicateOccurrences, Severity: "block", Detail: "all valid publisher keys were sorted and conserved"},
		{ID: "semantic_change_set", Passed: manifest.Artifacts.Changes.RecordCount == manifest.Changes.Added+manifest.Changes.Changed+manifest.Changes.Absent+manifest.Changes.Invalid, Severity: "block", Detail: "all non-unchanged natural-key transitions are materialized"},
		{ID: "projection_readiness", Passed: manifest.Counts.Invalid == 0 && manifest.Counts.DuplicateKeys == 0, Severity: "warning", Detail: fmt.Sprintf("%d invalid rows and %d duplicate natural keys require downstream isolation", manifest.Counts.Invalid, manifest.Counts.DuplicateKeys)},
	}
}

func validateClassicManifest(manifest ClassicManifest) error {
	if manifest.Schema != "manifest.schema.json" || manifest.SchemaVersion != ClassicManifestSchemaVersion {
		return fmt.Errorf("unexpected classic occurrence manifest schema")
	}
	spec, err := classic.Lookup(manifest.Dataset)
	if err != nil {
		return err
	}
	if manifest.SourceContract != spec.SourceContract || manifest.SourceID != fmt.Sprintf("fec:%s:%s", spec.Code, manifest.Cycle) || manifest.Member != classicMember(spec, manifest.Cycle) {
		return fmt.Errorf("classic occurrence source identity is inconsistent")
	}
	if !releaseIDPattern.MatchString(manifest.SourceReleaseID) {
		return fmt.Errorf("classic occurrence source release ID is invalid")
	}
	for _, digest := range []string{manifest.OccurrenceSetID, manifest.SourceReleaseManifestSHA256, manifest.SourceArtifactSHA256, manifest.StagedOutputSHA256} {
		if !digestPattern.MatchString(digest) {
			return fmt.Errorf("classic occurrence manifest contains an invalid digest")
		}
	}
	if manifest.PriorOccurrenceSetID != "" && !digestPattern.MatchString(manifest.PriorOccurrenceSetID) {
		return fmt.Errorf("prior classic occurrence-set ID is invalid")
	}
	if !validCycle(manifest.Cycle) || manifest.State != "published" || manifest.PublishedAt.IsZero() {
		return fmt.Errorf("classic occurrence manifest identity or state is incomplete")
	}
	if manifest.ParserVersion != classic.ParserVersion || manifest.SemanticSchemaVersion != ClassicSemanticSchemaVersion {
		return fmt.Errorf("classic occurrence parser contract is unsupported")
	}
	expectedSetID := digestParts("fec.classic.occurrence-set.v1", manifest.Dataset, manifest.SourceArtifactSHA256, manifest.StagedOutputSHA256, manifest.Member, manifest.Cycle, manifest.ParserVersion, manifest.SemanticSchemaVersion, manifest.PriorOccurrenceSetID)
	if manifest.OccurrenceSetID != expectedSetID {
		return fmt.Errorf("classic occurrence-set ID does not match its canonical inputs")
	}
	if !fecrelease.ValidAcquisitionRunID(manifest.RunID) {
		return fmt.Errorf("classic occurrence run ID is invalid")
	}
	basePath := classicEvidenceBase(spec)
	for kind, artifact := range map[string]Artifact{"occurrences": manifest.Artifacts.Occurrences, "issues": manifest.Artifacts.Issues, "natural-index": manifest.Artifacts.NaturalIndex, "changes": manifest.Artifacts.Changes} {
		if artifact.Compression != "zstd" || !digestPattern.MatchString(artifact.CompressedSHA256) || !digestPattern.MatchString(artifact.UncompressedSHA256) || artifact.CompressedBytes == 0 {
			return fmt.Errorf("%s artifact identity is invalid", kind)
		}
		prefix := filepath.ToSlash(filepath.Join(basePath, kind, "sha256", artifact.CompressedSHA256[:2])) + "/"
		if !strings.HasPrefix(artifact.StorageKey, prefix) {
			return fmt.Errorf("%s artifact storage key is not canonical", kind)
		}
	}
	if manifest.Counts.Total != manifest.Counts.Valid+manifest.Counts.Invalid || manifest.Artifacts.Occurrences.RecordCount != manifest.Counts.Total {
		return fmt.Errorf("classic occurrence counts are not conserved")
	}
	if manifest.Artifacts.NaturalIndex.RecordCount != manifest.Counts.UniqueKeys+manifest.Counts.InvalidKeys+manifest.Counts.DuplicateKeys || manifest.Counts.Keyed != manifest.Counts.UniqueKeys+manifest.Counts.InvalidKeys+manifest.Counts.DuplicateOccurrences {
		return fmt.Errorf("classic natural-key counts are not conserved")
	}
	if manifest.Artifacts.Issues.RecordCount < manifest.Counts.Invalid+manifest.Counts.DuplicateOccurrences {
		return fmt.Errorf("classic issue counts do not cover invalid and duplicate occurrences")
	}
	if manifest.Artifacts.Changes.RecordCount != manifest.Changes.Added+manifest.Changes.Changed+manifest.Changes.Absent+manifest.Changes.Invalid || manifest.Artifacts.NaturalIndex.RecordCount != manifest.Changes.Added+manifest.Changes.Changed+manifest.Changes.Unchanged+manifest.Changes.Invalid {
		return fmt.Errorf("classic semantic change counts are not conserved")
	}
	if len(manifest.Checks) < 6 {
		return fmt.Errorf("classic occurrence manifest is missing required checks")
	}
	for _, check := range manifest.Checks {
		if check.Severity == "block" && !check.Passed {
			return fmt.Errorf("blocking classic occurrence check %s failed", check.ID)
		}
	}
	return nil
}

func readClassicManifestIfPresent(path string) (*ClassicManifest, error) {
	content, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var manifest ClassicManifest
	decoder := json.NewDecoder(strings.NewReader(string(content)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&manifest); err != nil {
		return nil, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return nil, fmt.Errorf("multiple JSON values in classic occurrence manifest")
		}
		return nil, err
	}
	return &manifest, nil
}

func validateClassicManifestBacking(storageRoot string, current ClassicManifest) error {
	spec, err := classic.Lookup(current.Dataset)
	if err != nil {
		return err
	}
	immutablePath := filepath.Join(storageRoot, classicEvidenceBase(spec), "manifests", current.OccurrenceSetID+".json")
	immutable, err := readClassicManifestIfPresent(immutablePath)
	if err != nil {
		return err
	}
	if immutable == nil {
		return fmt.Errorf("immutable classic occurrence manifest is missing")
	}
	if err := validateClassicManifest(*immutable); err != nil {
		return fmt.Errorf("immutable classic occurrence manifest is invalid: %w", err)
	}
	if !reflect.DeepEqual(current, *immutable) {
		return fmt.Errorf("active classic occurrence pointer differs from its immutable manifest")
	}
	return validateArtifactFilesAt(storageRoot, classicEvidenceBase(spec), current.Artifacts)
}

func validateArtifactFilesAt(storageRoot, _ string, artifacts ArtifactSet) error {
	for kind, artifact := range map[string]Artifact{"occurrences": artifacts.Occurrences, "issues": artifacts.Issues, "natural-index": artifacts.NaturalIndex, "changes": artifacts.Changes} {
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
