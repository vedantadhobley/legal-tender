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
	"github.com/vedantadhobley/legal-tender/internal/source/fec/classic"
	fecrelease "github.com/vedantadhobley/legal-tender/internal/source/fec/release"
)

const (
	ClassicFactSetSchemaVersion = "legal-tender.fec.classic-fact-set.v1"
	ClassicFactSchemaVersion    = "legal-tender.fec.classic-fact.v1"
	ClassicNormalizerVersion    = "legal-tender.fec.classic-normalizer.v1"
)

type ClassicFactManifest struct {
	Schema                      string            `json:"$schema"`
	SchemaVersion               string            `json:"schema_version"`
	FactSetID                   string            `json:"fact_set_id"`
	Dataset                     string            `json:"dataset"`
	FactType                    string            `json:"fact_type"`
	Cycle                       string            `json:"cycle"`
	SourceContract              string            `json:"source_contract"`
	SourceReleaseID             string            `json:"source_release_id"`
	SourceReleaseManifestSHA256 string            `json:"source_release_manifest_sha256"`
	OccurrenceSetID             string            `json:"occurrence_set_id"`
	OccurrenceManifestSHA256    string            `json:"occurrence_manifest_sha256"`
	RunID                       string            `json:"run_id"`
	State                       string            `json:"state"`
	NormalizerVersion           string            `json:"normalizer_version"`
	FactSchemaVersion           string            `json:"fact_schema_version"`
	PublishedAt                 time.Time         `json:"published_at"`
	Counts                      ClassicFactCounts `json:"counts"`
	Facts                       Artifact          `json:"facts"`
	Checks                      []Check           `json:"checks"`
}

type ClassicFactCounts struct {
	SourceOccurrences   uint64 `json:"source_occurrences"`
	Facts               uint64 `json:"facts"`
	ValidFacts          uint64 `json:"valid_facts"`
	InvalidFacts        uint64 `json:"invalid_facts"`
	ExcludedOccurrences uint64 `json:"excluded_occurrences"`
	SourceInvalid       uint64 `json:"source_invalid_occurrences"`
	SourceDuplicates    uint64 `json:"source_duplicate_occurrences"`
}

type ClassicFact struct {
	SchemaVersion   string            `json:"schema_version"`
	FactID          string            `json:"fact_id"`
	FactType        string            `json:"fact_type"`
	Dataset         string            `json:"dataset"`
	Cycle           string            `json:"cycle"`
	NaturalKey      string            `json:"natural_key"`
	OccurrenceSetID string            `json:"occurrence_set_id"`
	OccurrenceID    string            `json:"occurrence_id"`
	RecordVersionID string            `json:"record_version_id"`
	SourceReleaseID string            `json:"source_release_id"`
	SourceContract  string            `json:"source_contract"`
	State           string            `json:"state"`
	IssueCodes      []string          `json:"issue_codes"`
	SourceFields    map[string]string `json:"source_fields"`
	TypedFields     any               `json:"typed_fields"`
}

type CandidateTypedFields struct {
	CandidateID                  string  `json:"candidate_id"`
	Name                         string  `json:"name"`
	PartyAffiliation             string  `json:"party_affiliation"`
	CandidateElectionYear        int     `json:"candidate_election_year"`
	SourceCycle                  int     `json:"source_cycle"`
	OfficeState                  string  `json:"office_state"`
	Office                       string  `json:"office"`
	OfficeDistrict               string  `json:"office_district"`
	IncumbentChallengerOpen      string  `json:"incumbent_challenger_open"`
	CandidateStatus              string  `json:"candidate_status"`
	PrincipalCampaignCommitteeID *string `json:"principal_campaign_committee_id"`
	Street1                      string  `json:"street_1"`
	Street2                      string  `json:"street_2"`
	City                         string  `json:"city"`
	State                        string  `json:"state"`
	ZIP                          string  `json:"zip"`
}

type CommitteeTypedFields struct {
	CommitteeID           string  `json:"committee_id"`
	Name                  string  `json:"name"`
	TreasurerName         string  `json:"treasurer_name"`
	Street1               string  `json:"street_1"`
	Street2               string  `json:"street_2"`
	City                  string  `json:"city"`
	State                 string  `json:"state"`
	ZIP                   string  `json:"zip"`
	DesignationCode       string  `json:"designation_code"`
	CommitteeTypeCode     string  `json:"committee_type_code"`
	PartyAffiliation      string  `json:"party_affiliation"`
	FilingFrequencyCode   string  `json:"filing_frequency_code"`
	OrganizationTypeCode  string  `json:"organization_type_code"`
	ConnectedOrganization string  `json:"connected_organization"`
	CandidateID           *string `json:"candidate_id"`
	SourceCycle           int     `json:"source_cycle"`
}

type LinkageTypedFields struct {
	CandidateID           string `json:"candidate_id"`
	CandidateElectionYear int    `json:"candidate_election_year"`
	FECElectionYear       int    `json:"fec_election_year"`
	SourceCycle           int    `json:"source_cycle"`
	CommitteeID           string `json:"committee_id"`
	CommitteeTypeCode     string `json:"committee_type_code"`
	DesignationCode       string `json:"designation_code"`
	LinkageID             string `json:"linkage_id"`
}

type SummaryTypedFields struct {
	CandidateID      string                             `json:"candidate_id"`
	Name             string                             `json:"name"`
	IncumbentCode    string                             `json:"incumbent_code"`
	PartyCode        string                             `json:"party_code"`
	PartyAffiliation string                             `json:"party_affiliation"`
	OfficeState      string                             `json:"office_state"`
	OfficeDistrict   string                             `json:"office_district"`
	CoverageThrough  *string                            `json:"coverage_through"`
	SourceCycle      int                                `json:"source_cycle"`
	Money            map[string]SummaryMoneyObservation `json:"money"`
}

type SummaryMoneyObservation struct {
	RawValue           string  `json:"raw_value"`
	ReportedMinorUnits *string `json:"reported_minor_units"`
	ObservationState   string  `json:"observation_state"`
	MeasurementKind    string  `json:"measurement_kind"`
	Currency           string  `json:"currency"`
}

// PublishClassicFacts normalizes the unique valid rows selected by one exact
// classic occurrence set. Source-invalid and duplicate occurrences remain in
// evidence and are explicitly excluded from this singleton fact projection.
func PublishClassicFacts(
	ctx context.Context,
	releaseManifest fecrelease.ReleaseManifest,
	releaseManifestSHA256 string,
	occurrenceManifestPath string,
	runID string,
	options Options,
) (ClassicFactManifest, error) {
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
		return ClassicFactManifest{}, fmt.Errorf("storage root and occurrence manifest path are required")
	}
	if !fecrelease.ValidAcquisitionRunID(runID) {
		return ClassicFactManifest{}, fmt.Errorf("run ID contains unsupported characters")
	}
	if !digestPattern.MatchString(releaseManifestSHA256) {
		return ClassicFactManifest{}, fmt.Errorf("source release manifest SHA-256 is invalid")
	}
	if issues := fecrelease.ValidateKnownManifest(releaseManifest); len(issues) != 0 {
		return ClassicFactManifest{}, fmt.Errorf("invalid source release: %s", issues[0].Message)
	}

	occurrenceManifest, occurrenceManifestSHA256, err := readClassicManifestWithSHA256(occurrenceManifestPath)
	if err != nil {
		return ClassicFactManifest{}, err
	}
	if err := validateClassicManifest(occurrenceManifest); err != nil {
		return ClassicFactManifest{}, fmt.Errorf("invalid classic occurrence input: %w", err)
	}
	if err := validateClassicManifestBacking(options.StorageRoot, occurrenceManifest); err != nil {
		return ClassicFactManifest{}, fmt.Errorf("validate classic occurrence input: %w", err)
	}
	spec, err := classic.Lookup(occurrenceManifest.Dataset)
	if err != nil {
		return ClassicFactManifest{}, err
	}
	descends, err := releaseDescendsFrom(ctx, options.StorageRoot, releaseManifest, occurrenceManifest.SourceReleaseID)
	if err != nil {
		return ClassicFactManifest{}, err
	}
	if !descends {
		return ClassicFactManifest{}, fmt.Errorf("source release %s does not descend from occurrence release %s", releaseManifest.ReleaseID, occurrenceManifest.SourceReleaseID)
	}
	output, sourceSHA, err := selectedClassicOutput(releaseManifest, spec, occurrenceManifest.Cycle)
	if err != nil {
		return ClassicFactManifest{}, err
	}
	if sourceSHA != occurrenceManifest.SourceArtifactSHA256 || output.CompressedSHA256 != occurrenceManifest.StagedOutputSHA256 {
		return ClassicFactManifest{}, fmt.Errorf("classic occurrence input does not describe the selected source bytes")
	}

	basePath := classicFactBase(spec)
	currentPath := options.CurrentManifestPath
	if currentPath == "" {
		currentPath = filepath.Join(options.StorageRoot, basePath, "current", occurrenceManifest.Cycle+".json")
	}
	if err := requirePathInside(options.StorageRoot, currentPath); err != nil {
		return ClassicFactManifest{}, err
	}
	lockPath := filepath.Join(options.StorageRoot, basePath, ".publish-"+occurrenceManifest.Cycle+".lock")
	unlock, err := lockContext(ctx, lockPath)
	if err != nil {
		return ClassicFactManifest{}, err
	}
	defer unlock()

	current, err := readClassicFactManifestIfPresent(currentPath)
	if err != nil {
		return ClassicFactManifest{}, err
	}
	if current != nil {
		if err := validateClassicFactManifest(*current); err != nil {
			return ClassicFactManifest{}, fmt.Errorf("invalid current classic fact manifest: %w", err)
		}
		if err := validateClassicFactManifestBacking(options.StorageRoot, *current); err != nil {
			return ClassicFactManifest{}, fmt.Errorf("validate current classic fact publication: %w", err)
		}
		if current.Dataset != occurrenceManifest.Dataset || current.Cycle != occurrenceManifest.Cycle {
			return ClassicFactManifest{}, fmt.Errorf("current classic fact manifest belongs to %s/%s", current.Dataset, current.Cycle)
		}
		if current.OccurrenceSetID == occurrenceManifest.OccurrenceSetID && current.NormalizerVersion == ClassicNormalizerVersion && current.FactSchemaVersion == ClassicFactSchemaVersion {
			return *current, nil
		}
	}

	factSetID := digestParts("fec.classic.fact-set.v1", occurrenceManifest.Dataset, occurrenceManifest.OccurrenceSetID, ClassicNormalizerVersion, ClassicFactSchemaVersion)
	manifestPath := filepath.Join(options.StorageRoot, basePath, "manifests", factSetID+".json")
	if existing, err := readClassicFactManifestIfPresent(manifestPath); err != nil {
		return ClassicFactManifest{}, err
	} else if existing != nil {
		if err := validateClassicFactManifest(*existing); err != nil {
			return ClassicFactManifest{}, fmt.Errorf("invalid immutable classic fact manifest: %w", err)
		}
		if existing.FactSetID != factSetID || existing.OccurrenceSetID != occurrenceManifest.OccurrenceSetID {
			return ClassicFactManifest{}, fmt.Errorf("immutable classic fact manifest collision at %s", manifestPath)
		}
		if err := validateClassicFactArtifact(options.StorageRoot, spec, existing.Facts); err != nil {
			return ClassicFactManifest{}, err
		}
		if err := writeAtomicJSON(currentPath, *existing); err != nil {
			return ClassicFactManifest{}, err
		}
		return *existing, nil
	}
	if err := requireOccurrenceStorage(options, output.UncompressedByteCount+options.WorkingMarginBytes); err != nil {
		return ClassicFactManifest{}, err
	}
	if options.Progress != nil {
		options.Progress(fmt.Sprintf("normalizing %s facts for %s", occurrenceManifest.Dataset, occurrenceManifest.Cycle))
	}
	manifest, err := buildClassicFactSet(ctx, occurrenceManifest, occurrenceManifestSHA256, output, spec, runID, factSetID, options)
	if err != nil {
		return ClassicFactManifest{}, err
	}
	if err := validateClassicFactManifest(manifest); err != nil {
		return ClassicFactManifest{}, err
	}
	if err := requireOccurrenceStorage(options, 0); err != nil {
		return ClassicFactManifest{}, err
	}
	if err := writeAtomicJSON(manifestPath, manifest); err != nil {
		return ClassicFactManifest{}, err
	}
	if err := writeAtomicJSON(currentPath, manifest); err != nil {
		return ClassicFactManifest{}, err
	}
	return manifest, nil
}

func buildClassicFactSet(ctx context.Context, occurrence ClassicManifest, occurrenceSHA string, output fecrelease.StagedOutput, spec classic.Spec, runID, factSetID string, options Options) (ClassicFactManifest, error) {
	basePath := classicFactBase(spec)
	temporaryDirectory := filepath.Join(options.StorageRoot, basePath, "staging", factSetID, runID)
	if err := os.MkdirAll(temporaryDirectory, 0o750); err != nil {
		return ClassicFactManifest{}, err
	}
	defer func() { _ = os.RemoveAll(temporaryDirectory) }()
	factsWriter, err := newArtifactWriterAt(ctx, options.StorageRoot, temporaryDirectory, basePath, "facts")
	if err != nil {
		return ClassicFactManifest{}, err
	}
	defer factsWriter.Abort()
	counts, err := streamClassicFacts(ctx, occurrence, output, spec, options.StorageRoot, factsWriter, options.Progress)
	if err != nil {
		return ClassicFactManifest{}, err
	}
	factsArtifact, err := factsWriter.Finalize()
	if err != nil {
		return ClassicFactManifest{}, err
	}
	manifest := ClassicFactManifest{
		Schema: "manifest.schema.json", SchemaVersion: ClassicFactSetSchemaVersion,
		FactSetID: factSetID, Dataset: occurrence.Dataset, FactType: spec.FactType, Cycle: occurrence.Cycle,
		SourceContract: spec.SourceContract, SourceReleaseID: occurrence.SourceReleaseID,
		SourceReleaseManifestSHA256: occurrence.SourceReleaseManifestSHA256, OccurrenceSetID: occurrence.OccurrenceSetID,
		OccurrenceManifestSHA256: occurrenceSHA, RunID: runID, State: "published",
		NormalizerVersion: ClassicNormalizerVersion, FactSchemaVersion: ClassicFactSchemaVersion,
		PublishedAt: options.Clock().UTC(), Counts: counts, Facts: factsArtifact,
	}
	manifest.Checks = []Check{
		{ID: "occurrence_lineage", Passed: true, Severity: "block", Detail: "fact set names an immutable validated occurrence set"},
		{ID: "selected_output_integrity", Passed: true, Severity: "block", Detail: "normalization replay matched the exact staged source bytes"},
		{ID: "unique_projection", Passed: counts.Facts == occurrence.Counts.UniqueKeys, Severity: "block", Detail: "each unique valid publisher key produced one fact"},
		{ID: "occurrence_conservation", Passed: counts.SourceOccurrences == counts.Facts+counts.ExcludedOccurrences, Severity: "block", Detail: "source occurrences are partitioned into facts and explicit exclusions"},
		{ID: "fact_state_conservation", Passed: counts.Facts == counts.ValidFacts+counts.InvalidFacts, Severity: "block", Detail: "every normalized fact has an explicit parse state"},
	}
	return manifest, nil
}

func streamClassicFacts(ctx context.Context, occurrence ClassicManifest, output fecrelease.StagedOutput, spec classic.Spec, storageRoot string, writer *artifactWriter, progress func(string)) (ClassicFactCounts, error) {
	return scanClassicFacts(ctx, occurrence, output, spec, storageRoot, func(f ClassicFact) error { return writer.WriteJSON(f) }, progress)
}

// Publication and reference-equivalence verification share the same pinned
// normalizer. Consumers can replay complete facts without writing new artifacts.
func scanClassicFacts(ctx context.Context, occurrence ClassicManifest, output fecrelease.StagedOutput, spec classic.Spec, storageRoot string, consume func(ClassicFact) error, progress func(string)) (ClassicFactCounts, error) {
	unique := make(map[string]NaturalIndexEntry, occurrence.Counts.UniqueKeys)
	indexReader, err := openArtifactJSON(ctx, storageRoot, occurrence.Artifacts.NaturalIndex)
	if err != nil {
		return ClassicFactCounts{}, err
	}
	for {
		entry, ok, err := indexReader.Next()
		if err != nil {
			_ = indexReader.Close()
			return ClassicFactCounts{}, err
		}
		if !ok {
			break
		}
		if entry.State == "unique" {
			unique[entry.NaturalKey] = entry
		}
	}
	if err := indexReader.Close(); err != nil {
		return ClassicFactCounts{}, err
	}

	stagedPath, err := resolveStorageKey(storageRoot, output.StorageKey)
	if err != nil {
		return ClassicFactCounts{}, err
	}
	file, err := os.Open(stagedPath)
	if err != nil {
		return ClassicFactCounts{}, err
	}
	defer func() { _ = file.Close() }()
	compressed := &countingHashReader{reader: &contextReader{ctx: ctx, reader: file}, hash: sha256.New()}
	zstdDecoder, err := zstd.NewReader(compressed, zstd.WithDecoderConcurrency(2), zstd.WithDecoderLowmem(true), zstd.WithDecoderMaxMemory(64<<20))
	if err != nil {
		return ClassicFactCounts{}, err
	}
	defer zstdDecoder.Close()
	uncompressed := &countingHashReader{reader: zstdDecoder, hash: sha256.New()}
	rowDecoder := classic.NewDecoder(uncompressed)
	seen := make(map[string]struct{}, len(unique))
	counts := ClassicFactCounts{
		SourceOccurrences:   occurrence.Counts.Total,
		ExcludedOccurrences: occurrence.Counts.Total - occurrence.Counts.UniqueKeys,
		SourceInvalid:       occurrence.Counts.Invalid,
		SourceDuplicates:    occurrence.Counts.DuplicateOccurrences,
	}
	var physicalRows uint64
	for rowDecoder.Scan() {
		physicalRows++
		row := rowDecoder.Row()
		rawKey, hasKey := row.NaturalKeyValue(spec)
		if !hasKey || !spec.ValidNaturalKey(rawKey) {
			continue
		}
		naturalKey := fmt.Sprintf("fec:%s:%s:%s", spec.Dataset, occurrence.Cycle, rawKey)
		entry, selected := unique[naturalKey]
		if !selected {
			continue
		}
		rawDigestBytes := sha256.Sum256(row.Raw())
		rawDigest := hex.EncodeToString(rawDigestBytes[:])
		occurrenceID := digestParts("fec.classic.occurrence.v1", string(spec.Dataset), occurrence.SourceArtifactSHA256, output.Selection, occurrence.Cycle, fmt.Sprintf("%d", row.Number()))
		recordVersionID := digestParts("fec.classic.record-version.v1", string(spec.Dataset), occurrence.Cycle, naturalKey, rawDigest)
		if entry.OccurrenceID != occurrenceID || entry.RecordVersionID != recordVersionID {
			return counts, fmt.Errorf("natural index does not match %s row %d", spec.Dataset, row.Number())
		}
		if _, duplicate := seen[naturalKey]; duplicate {
			return counts, fmt.Errorf("unique natural key %s was selected more than once", naturalKey)
		}
		seen[naturalKey] = struct{}{}
		sourceFields, ok := row.CanonicalMap(spec)
		if !ok {
			return counts, fmt.Errorf("unique classic row has no canonical field mapping")
		}
		typed, issueCodes, err := normalizeClassicFields(spec, occurrence.Cycle, sourceFields)
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
		fact := ClassicFact{
			SchemaVersion: ClassicFactSchemaVersion,
			FactID:        digestParts("fec.classic.fact.v1", spec.FactType, recordVersionID, ClassicNormalizerVersion),
			FactType:      spec.FactType, Dataset: string(spec.Dataset), Cycle: occurrence.Cycle,
			NaturalKey: naturalKey, OccurrenceSetID: occurrence.OccurrenceSetID,
			OccurrenceID: occurrenceID, RecordVersionID: recordVersionID,
			SourceReleaseID: occurrence.SourceReleaseID, SourceContract: spec.SourceContract,
			State: state, IssueCodes: issueCodes, SourceFields: sourceFields, TypedFields: typed,
		}
		if err := consume(fact); err != nil {
			return counts, err
		}
		counts.Facts++
		if progress != nil && counts.Facts%100_000 == 0 {
			progress(fmt.Sprintf("normalized %d %s facts for %s", counts.Facts, spec.Dataset, occurrence.Cycle))
		}
	}
	if err := rowDecoder.Err(); err != nil {
		return counts, err
	}
	if physicalRows != occurrence.Counts.Total {
		return counts, fmt.Errorf("classic physical row census mismatch")
	}
	if uint64(len(seen)) != occurrence.Counts.UniqueKeys || counts.Facts != occurrence.Counts.UniqueKeys {
		return counts, fmt.Errorf("classic fact projection selected %d unique rows; want %d", counts.Facts, occurrence.Counts.UniqueKeys)
	}
	if compressed.bytes != output.CompressedByteCount || hex.EncodeToString(compressed.hash.Sum(nil)) != output.CompressedSHA256 {
		return counts, fmt.Errorf("classic fact replay compressed identity mismatch")
	}
	if uncompressed.bytes != output.UncompressedByteCount || hex.EncodeToString(uncompressed.hash.Sum(nil)) != output.UncompressedSHA256 {
		return counts, fmt.Errorf("classic fact replay uncompressed identity mismatch")
	}
	return counts, nil
}

func normalizeClassicFields(spec classic.Spec, cycle string, fields map[string]string) (any, []string, error) {
	sourceCycle, err := strconv.Atoi(cycle)
	if err != nil {
		return nil, nil, err
	}
	optional := func(value string) *string {
		if value == "" {
			return nil
		}
		copy := value
		return &copy
	}
	switch spec.Dataset {
	case classic.CandidateMaster:
		year, err := strconv.Atoi(fields["CAND_ELECTION_YR"])
		if err != nil {
			return nil, nil, err
		}
		return CandidateTypedFields{
			CandidateID: fields["CAND_ID"], Name: fields["CAND_NAME"], PartyAffiliation: fields["CAND_PTY_AFFILIATION"],
			CandidateElectionYear: year, SourceCycle: sourceCycle, OfficeState: fields["CAND_OFFICE_ST"], Office: fields["CAND_OFFICE"],
			OfficeDistrict: fields["CAND_OFFICE_DISTRICT"], IncumbentChallengerOpen: fields["CAND_ICI"], CandidateStatus: fields["CAND_STATUS"],
			PrincipalCampaignCommitteeID: optional(fields["CAND_PCC"]), Street1: fields["CAND_ST1"], Street2: fields["CAND_ST2"],
			City: fields["CAND_CITY"], State: fields["CAND_ST"], ZIP: fields["CAND_ZIP"],
		}, []string{}, nil
	case classic.CommitteeMaster:
		return CommitteeTypedFields{
			CommitteeID: fields["CMTE_ID"], Name: fields["CMTE_NM"], TreasurerName: fields["TRES_NM"], Street1: fields["CMTE_ST1"],
			Street2: fields["CMTE_ST2"], City: fields["CMTE_CITY"], State: fields["CMTE_ST"], ZIP: fields["CMTE_ZIP"],
			DesignationCode: fields["CMTE_DSGN"], CommitteeTypeCode: fields["CMTE_TP"], PartyAffiliation: fields["CMTE_PTY_AFFILIATION"],
			FilingFrequencyCode: fields["CMTE_FILING_FREQ"], OrganizationTypeCode: fields["ORG_TP"], ConnectedOrganization: fields["CONNECTED_ORG_NM"],
			CandidateID: optional(fields["CAND_ID"]), SourceCycle: sourceCycle,
		}, []string{}, nil
	case classic.CandidateCommitteeLinkage:
		candidateYear, err := strconv.Atoi(fields["CAND_ELECTION_YR"])
		if err != nil {
			return nil, nil, err
		}
		fecYear, err := strconv.Atoi(fields["FEC_ELECTION_YR"])
		if err != nil {
			return nil, nil, err
		}
		return LinkageTypedFields{
			CandidateID: fields["CAND_ID"], CandidateElectionYear: candidateYear, FECElectionYear: fecYear, SourceCycle: sourceCycle,
			CommitteeID: fields["CMTE_ID"], CommitteeTypeCode: fields["CMTE_TP"], DesignationCode: fields["CMTE_DSGN"], LinkageID: fields["LINKAGE_ID"],
		}, []string{}, nil
	case classic.AllCandidatesSummary, classic.CurrentCampaignsSummary:
		var coverage *string
		if raw := fields["CVG_END_DT"]; raw != "" {
			parsed, err := time.Parse("01/02/2006", raw)
			if err != nil {
				return nil, nil, err
			}
			iso := parsed.Format("2006-01-02")
			coverage = &iso
		}
		money := make(map[string]SummaryMoneyObservation, len(summaryMoneyFields))
		issueCodes := make([]string, 0)
		for _, field := range summaryMoneyFields {
			observation, issue := normalizeSummaryMoney(fields[field])
			money[field] = observation
			if issue != "" {
				issueCodes = append(issueCodes, strings.ToLower(field)+"_"+issue)
			}
		}
		return SummaryTypedFields{
			CandidateID: fields["CAND_ID"], Name: fields["CAND_NAME"], IncumbentCode: fields["CAND_ICI"], PartyCode: fields["PTY_CD"],
			PartyAffiliation: fields["CAND_PTY_AFFILIATION"], OfficeState: fields["CAND_OFFICE_ST"], OfficeDistrict: fields["CAND_OFFICE_DISTRICT"],
			CoverageThrough: coverage, SourceCycle: sourceCycle, Money: money,
		}, issueCodes, nil
	default:
		return nil, nil, fmt.Errorf("unsupported classic fact dataset %s", spec.Dataset)
	}
}

var summaryMoneyFields = []string{
	"TTL_RECEIPTS", "TRANS_FROM_AUTH", "TTL_DISB", "TRANS_TO_AUTH", "COH_BOP", "COH_COP", "CAND_CONTRIB", "CAND_LOANS", "OTHER_LOANS", "CAND_LOAN_REPAY", "OTHER_LOAN_REPAY", "DEBTS_OWED_BY", "TTL_INDIV_CONTRIB", "OTHER_POL_CMTE_CONTRIB", "POL_PTY_CONTRIB", "INDIV_REFUNDS", "CMTE_REFUNDS",
}

// SummaryMoneyFields returns the reviewed financial fields shared by the two
// candidate-summary normalizers. Callers cannot change normalization policy.
func SummaryMoneyFields() []string { return append([]string(nil), summaryMoneyFields...) }

func normalizeSummaryMoney(raw string) (SummaryMoneyObservation, string) {
	result := SummaryMoneyObservation{RawValue: raw, MeasurementKind: "summary_value", Currency: "USD"}
	if raw == "" {
		result.ObservationState = "source_blank"
		return result, ""
	}
	result.ObservationState = "reported_value"
	minor, _, issue := parseUSDMinorUnits(raw)
	if issue != "" {
		result.ObservationState = "invalid"
		return result, issue
	}
	result.ReportedMinorUnits = &minor
	return result, ""
}

func classicFactBase(spec classic.Spec) string {
	return filepath.Join("facts", "fec", "classic", string(spec.Dataset))
}

func readClassicManifestWithSHA256(path string) (ClassicManifest, string, error) {
	content, err := os.ReadFile(path)
	if err != nil {
		return ClassicManifest{}, "", err
	}
	var manifest ClassicManifest
	decoder := json.NewDecoder(strings.NewReader(string(content)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&manifest); err != nil {
		return ClassicManifest{}, "", err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return ClassicManifest{}, "", fmt.Errorf("multiple JSON values in classic occurrence manifest")
		}
		return ClassicManifest{}, "", err
	}
	digest := sha256.Sum256(content)
	return manifest, hex.EncodeToString(digest[:]), nil
}

func readClassicFactManifestIfPresent(path string) (*ClassicFactManifest, error) {
	content, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var manifest ClassicFactManifest
	decoder := json.NewDecoder(strings.NewReader(string(content)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&manifest); err != nil {
		return nil, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return nil, fmt.Errorf("multiple JSON values in classic fact manifest")
		}
		return nil, err
	}
	return &manifest, nil
}

func validateClassicFactManifest(manifest ClassicFactManifest) error {
	if manifest.Schema != "manifest.schema.json" || manifest.SchemaVersion != ClassicFactSetSchemaVersion {
		return fmt.Errorf("unexpected classic fact manifest schema")
	}
	spec, err := classic.Lookup(manifest.Dataset)
	if err != nil {
		return err
	}
	if manifest.FactType != spec.FactType || manifest.SourceContract != spec.SourceContract || !validCycle(manifest.Cycle) {
		return fmt.Errorf("classic fact identity is inconsistent")
	}
	for _, digest := range []string{manifest.FactSetID, manifest.SourceReleaseManifestSHA256, manifest.OccurrenceSetID, manifest.OccurrenceManifestSHA256} {
		if !digestPattern.MatchString(digest) {
			return fmt.Errorf("classic fact manifest contains an invalid digest")
		}
	}
	if !releaseIDPattern.MatchString(manifest.SourceReleaseID) || !fecrelease.ValidAcquisitionRunID(manifest.RunID) || manifest.State != "published" || manifest.PublishedAt.IsZero() {
		return fmt.Errorf("classic fact publication identity is incomplete")
	}
	if manifest.NormalizerVersion != ClassicNormalizerVersion || manifest.FactSchemaVersion != ClassicFactSchemaVersion {
		return fmt.Errorf("classic fact parser contract is unsupported")
	}
	expected := digestParts("fec.classic.fact-set.v1", manifest.Dataset, manifest.OccurrenceSetID, manifest.NormalizerVersion, manifest.FactSchemaVersion)
	if manifest.FactSetID != expected {
		return fmt.Errorf("classic fact-set ID does not match canonical inputs")
	}
	if manifest.Facts.Compression != "zstd" || !digestPattern.MatchString(manifest.Facts.CompressedSHA256) || !digestPattern.MatchString(manifest.Facts.UncompressedSHA256) || manifest.Facts.CompressedBytes == 0 || manifest.Facts.RecordCount != manifest.Counts.Facts {
		return fmt.Errorf("classic fact artifact identity is invalid")
	}
	prefix := filepath.ToSlash(filepath.Join(classicFactBase(spec), "facts", "sha256", manifest.Facts.CompressedSHA256[:2])) + "/"
	if !strings.HasPrefix(manifest.Facts.StorageKey, prefix) {
		return fmt.Errorf("classic fact artifact storage key is not canonical")
	}
	if manifest.Counts.SourceOccurrences != manifest.Counts.Facts+manifest.Counts.ExcludedOccurrences || manifest.Counts.Facts != manifest.Counts.ValidFacts+manifest.Counts.InvalidFacts {
		return fmt.Errorf("classic fact counts are not conserved")
	}
	if len(manifest.Checks) < 5 {
		return fmt.Errorf("classic fact manifest is missing required checks")
	}
	for _, check := range manifest.Checks {
		if check.Severity == "block" && !check.Passed {
			return fmt.Errorf("blocking classic fact check %s failed", check.ID)
		}
	}
	return nil
}

func validateClassicFactManifestBacking(storageRoot string, current ClassicFactManifest) error {
	spec, err := classic.Lookup(current.Dataset)
	if err != nil {
		return err
	}
	immutablePath := filepath.Join(storageRoot, classicFactBase(spec), "manifests", current.FactSetID+".json")
	immutable, err := readClassicFactManifestIfPresent(immutablePath)
	if err != nil {
		return err
	}
	if immutable == nil {
		return fmt.Errorf("immutable classic fact manifest is missing")
	}
	if err := validateClassicFactManifest(*immutable); err != nil {
		return err
	}
	if !reflect.DeepEqual(current, *immutable) {
		return fmt.Errorf("active classic fact pointer differs from its immutable manifest")
	}
	return validateClassicFactArtifact(storageRoot, spec, current.Facts)
}

func validateClassicFactArtifact(storageRoot string, _ classic.Spec, artifact Artifact) error {
	path, err := resolveStorageKey(storageRoot, artifact.StorageKey)
	if err != nil {
		return err
	}
	info, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("facts artifact: %w", err)
	}
	if !info.Mode().IsRegular() || info.Size() < 0 || uint64(info.Size()) != artifact.CompressedBytes {
		return fmt.Errorf("facts artifact does not match its published size")
	}
	return nil
}
