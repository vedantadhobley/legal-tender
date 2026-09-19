package receipts

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
	"syscall"
	"time"

	fecoccurrence "github.com/vedantadhobley/legal-tender/internal/source/fec/occurrence"
	fecrelease "github.com/vedantadhobley/legal-tender/internal/source/fec/release"
	storageartifact "github.com/vedantadhobley/legal-tender/internal/storage/artifact"
)

const (
	ManifestSchemaVersion = "legal-tender.fec.candidate-itemized-individual-receipts-set.v1"
	ResultSchemaVersion   = SchemaVersion
)

type PublishInput struct {
	ScheduleAFactManifestPath     string
	LinkageFactManifestPath       string
	AllCandidatesFactManifestPath string
	CurrentCampaignsManifestPath  string
}

type PublishOptions struct {
	StorageRoot         string
	CurrentManifestPath string
	Clock               func() time.Time
	Progress            func(string)
}

type Manifest struct {
	Schema                string                     `json:"$schema"`
	SchemaVersion         string                     `json:"schema_version"`
	CalculationSetID      string                     `json:"calculation_set_id"`
	Calculation           string                     `json:"calculation"`
	CalculationVersion    string                     `json:"calculation_version"`
	DecisionSchemaVersion string                     `json:"decision_schema_version"`
	ResultSchemaVersion   string                     `json:"result_schema_version"`
	Cycle                 string                     `json:"cycle"`
	SourceReleaseID       string                     `json:"source_release_id"`
	InputFactSets         []FactSetReference         `json:"input_fact_sets"`
	RunID                 string                     `json:"run_id"`
	State                 string                     `json:"state"`
	PublishedAt           time.Time                  `json:"published_at"`
	Counts                PublicationCounts          `json:"counts"`
	Decisions             storageartifact.Descriptor `json:"decisions"`
	Results               storageartifact.Descriptor `json:"results"`
	Checks                []Check                    `json:"checks"`
}

type FactSetReference struct {
	Role           string `json:"role"`
	Dataset        string `json:"dataset"`
	FactType       string `json:"fact_type"`
	FactSetID      string `json:"fact_set_id"`
	ManifestSHA256 string `json:"manifest_sha256"`
}

type PublicationCounts struct {
	ReceiptFacts          uint64 `json:"receipt_facts"`
	ReceiptDecisions      uint64 `json:"receipt_decisions"`
	LinkageFacts          uint64 `json:"linkage_facts"`
	SummaryFacts          uint64 `json:"summary_facts"`
	Candidates            uint64 `json:"candidates"`
	Complete              uint64 `json:"complete"`
	Partial               uint64 `json:"partial"`
	NotComparable         uint64 `json:"not_comparable"`
	Reconciliations       uint64 `json:"reconciliations"`
	DateBounded           uint64 `json:"date_bounded_reconciliations"`
	UncomparableSummaries uint64 `json:"not_comparable_reconciliations"`
}

type Check struct {
	ID       string `json:"id"`
	Passed   bool   `json:"passed"`
	Severity string `json:"severity"`
	Detail   string `json:"detail"`
}

type classicFactEnvelope struct {
	SchemaVersion   string          `json:"schema_version"`
	FactID          string          `json:"fact_id"`
	FactType        string          `json:"fact_type"`
	Dataset         string          `json:"dataset"`
	Cycle           string          `json:"cycle"`
	NaturalKey      string          `json:"natural_key"`
	OccurrenceSetID string          `json:"occurrence_set_id"`
	OccurrenceID    string          `json:"occurrence_id"`
	RecordVersionID string          `json:"record_version_id"`
	SourceReleaseID string          `json:"source_release_id"`
	SourceContract  string          `json:"source_contract"`
	State           string          `json:"state"`
	IssueCodes      []string        `json:"issue_codes"`
	SourceFields    json.RawMessage `json:"source_fields"`
	TypedFields     json.RawMessage `json:"typed_fields"`
}

func Publish(ctx context.Context, input PublishInput, runID string, options PublishOptions) (Manifest, error) {
	if options.Clock == nil {
		options.Clock = time.Now
	}
	if options.StorageRoot == "" {
		return Manifest{}, fmt.Errorf("storage root is required")
	}
	if !fecrelease.ValidAcquisitionRunID(runID) {
		return Manifest{}, fmt.Errorf("run ID contains unsupported characters")
	}
	scheduleManifest, scheduleDigest, err := loadScheduleAFactManifest(options.StorageRoot, input.ScheduleAFactManifestPath)
	if err != nil {
		return Manifest{}, fmt.Errorf("load Schedule A facts: %w", err)
	}
	classicInputs := []struct {
		role, dataset, path string
	}{
		{"candidate_committee_linkage", "candidate-committee-linkage", input.LinkageFactManifestPath},
		{"all_candidates_summary", "all-candidates-summary", input.AllCandidatesFactManifestPath},
		{"current_campaigns_summary", "current-campaigns-summary", input.CurrentCampaignsManifestPath},
	}
	classicManifests := make(map[string]fecoccurrence.ClassicFactManifest, len(classicInputs))
	references := []FactSetReference{{
		Role: "schedule_a_receipts", Dataset: "schedule-a", FactType: scheduleManifest.FactType,
		FactSetID: scheduleManifest.FactSetID, ManifestSHA256: scheduleDigest,
	}}
	for _, selected := range classicInputs {
		manifest, digest, loadErr := loadClassicFactManifest(options.StorageRoot, selected.path, selected.dataset)
		if loadErr != nil {
			return Manifest{}, fmt.Errorf("load %s facts: %w", selected.dataset, loadErr)
		}
		if manifest.Cycle != scheduleManifest.Cycle || manifest.SourceReleaseID != scheduleManifest.SourceReleaseID {
			return Manifest{}, fmt.Errorf("%s facts do not share Schedule A cycle and source release", selected.dataset)
		}
		classicManifests[selected.dataset] = manifest
		references = append(references, FactSetReference{
			Role: selected.role, Dataset: selected.dataset, FactType: manifest.FactType,
			FactSetID: manifest.FactSetID, ManifestSHA256: digest,
		})
	}
	sort.Slice(references, func(left, right int) bool { return references[left].Role < references[right].Role })
	identityParts := []string{"fec.candidate-itemized-individual-receipts-set.v1", ContractVersion, DecisionSchemaVersion, ResultSchemaVersion}
	for _, reference := range references {
		identityParts = append(identityParts, reference.Role, reference.FactSetID, reference.ManifestSHA256)
	}
	calculationSetID := digestParts(identityParts...)
	basePath := calculationBase()
	currentPath := options.CurrentManifestPath
	if currentPath == "" {
		currentPath = filepath.Join(options.StorageRoot, basePath, "current", scheduleManifest.Cycle+".json")
	}
	if err := requirePathInside(options.StorageRoot, currentPath); err != nil {
		return Manifest{}, err
	}
	unlock, err := lockContext(ctx, filepath.Join(options.StorageRoot, basePath, ".publish-"+scheduleManifest.Cycle+".lock"))
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
			return Manifest{}, fmt.Errorf("invalid current calculation manifest: %w", err)
		}
		if err := validateManifestBacking(ctx, options.StorageRoot, *current); err != nil {
			return Manifest{}, err
		}
		if current.Cycle != scheduleManifest.Cycle {
			return Manifest{}, fmt.Errorf("current calculation manifest belongs to cycle %s", current.Cycle)
		}
		if current.CalculationSetID == calculationSetID {
			return *current, nil
		}
	}
	manifestPath := filepath.Join(options.StorageRoot, basePath, "manifests", calculationSetID+".json")
	if existing, readErr := readManifestIfPresent(manifestPath); readErr != nil {
		return Manifest{}, readErr
	} else if existing != nil {
		if err := validateManifest(*existing); err != nil {
			return Manifest{}, err
		}
		if !reflect.DeepEqual(existing.InputFactSets, references) {
			return Manifest{}, fmt.Errorf("immutable calculation manifest collision")
		}
		if err := validateManifestBacking(ctx, options.StorageRoot, *existing); err != nil {
			return Manifest{}, err
		}
		if err := writeAtomicJSON(currentPath, *existing); err != nil {
			return Manifest{}, err
		}
		return *existing, nil
	}
	linkages, err := loadLinkages(ctx, options.StorageRoot, classicManifests["candidate-committee-linkage"])
	if err != nil {
		return Manifest{}, err
	}
	summaries, err := loadSummaries(ctx, options.StorageRoot, classicManifests["all-candidates-summary"])
	if err != nil {
		return Manifest{}, err
	}
	currentSummaries, err := loadSummaries(ctx, options.StorageRoot, classicManifests["current-campaigns-summary"])
	if err != nil {
		return Manifest{}, err
	}
	summaries = append(summaries, currentSummaries...)
	calculator, err := NewCycleCalculator(scheduleManifest.Cycle, nil, linkages, summaries)
	if err != nil {
		return Manifest{}, err
	}
	temporaryDirectory := filepath.Join(options.StorageRoot, basePath, "staging", calculationSetID, runID)
	if err := os.MkdirAll(temporaryDirectory, 0o750); err != nil {
		return Manifest{}, err
	}
	defer func() { _ = os.RemoveAll(temporaryDirectory) }()
	decisionWriter, err := storageartifact.NewWriter(ctx, options.StorageRoot, temporaryDirectory, basePath, "decisions")
	if err != nil {
		return Manifest{}, err
	}
	defer decisionWriter.Abort()
	receiptReader, err := storageartifact.Open[fecoccurrence.ScheduleAFact](ctx, options.StorageRoot, descriptor(scheduleManifest.Facts))
	if err != nil {
		return Manifest{}, err
	}
	var receiptFacts uint64
	for {
		fact, ok, readErr := receiptReader.Next()
		if readErr != nil {
			receiptReader.Abort()
			return Manifest{}, readErr
		}
		if !ok {
			break
		}
		if fact.SchemaVersion != fecoccurrence.ScheduleAFactSchemaVersion || fact.FactType != fecoccurrence.ScheduleAFactType || fact.SourceReleaseID != scheduleManifest.SourceReleaseID {
			receiptReader.Abort()
			return Manifest{}, fmt.Errorf("Schedule A fact %s has incompatible identity", fact.FactID)
		}
		decision, err := calculator.AddReceiptWithDecision(fact)
		if err != nil {
			receiptReader.Abort()
			return Manifest{}, err
		}
		if err := decisionWriter.WriteJSON(decision); err != nil {
			receiptReader.Abort()
			return Manifest{}, err
		}
		receiptFacts++
		if options.Progress != nil && receiptFacts%1_000_000 == 0 {
			options.Progress(fmt.Sprintf("calculated receipt decisions for %d Schedule A facts in %s", receiptFacts, scheduleManifest.Cycle))
		}
	}
	if err := receiptReader.Close(); err != nil {
		return Manifest{}, err
	}
	results, err := calculator.Results()
	if err != nil {
		return Manifest{}, err
	}
	decisionArtifact, err := decisionWriter.Finalize()
	if err != nil {
		return Manifest{}, err
	}
	writer, err := storageartifact.NewWriter(ctx, options.StorageRoot, temporaryDirectory, basePath, "results")
	if err != nil {
		return Manifest{}, err
	}
	counts := PublicationCounts{
		ReceiptFacts: receiptFacts, ReceiptDecisions: decisionArtifact.RecordCount,
		LinkageFacts: uint64(len(linkages)), SummaryFacts: uint64(len(summaries)), Candidates: uint64(len(results)),
	}
	for _, result := range results {
		if err := writer.WriteJSON(result); err != nil {
			writer.Abort()
			return Manifest{}, err
		}
		switch result.State {
		case "complete":
			counts.Complete++
		case "partial":
			counts.Partial++
		case "not_comparable":
			counts.NotComparable++
		default:
			writer.Abort()
			return Manifest{}, fmt.Errorf("candidate %s has unsupported result state %q", result.CandidateID, result.State)
		}
		for _, reconciliation := range result.Reconciliations {
			counts.Reconciliations++
			switch reconciliation.CoverageState {
			case "date_bounded":
				counts.DateBounded++
			case "not_comparable":
				counts.UncomparableSummaries++
			case "source_aligned":
			default:
				writer.Abort()
				return Manifest{}, fmt.Errorf("candidate %s has unsupported reconciliation state %q", result.CandidateID, reconciliation.CoverageState)
			}
		}
	}
	resultArtifact, err := writer.Finalize()
	if err != nil {
		return Manifest{}, err
	}
	manifest := Manifest{
		Schema: "manifest.schema.json", SchemaVersion: ManifestSchemaVersion,
		CalculationSetID: calculationSetID, Calculation: ContractID, CalculationVersion: ContractVersion,
		DecisionSchemaVersion: DecisionSchemaVersion, ResultSchemaVersion: ResultSchemaVersion,
		Cycle: scheduleManifest.Cycle, SourceReleaseID: scheduleManifest.SourceReleaseID,
		InputFactSets: references, RunID: runID, State: "published", PublishedAt: options.Clock().UTC(), Counts: counts,
		Decisions: decisionArtifact, Results: resultArtifact,
		Checks: []Check{
			{ID: "input_lineage", Passed: true, Severity: "block", Detail: "all four fact sets share one cycle and source release"},
			{ID: "artifact_integrity", Passed: receiptFacts == scheduleManifest.Facts.RecordCount, Severity: "block", Detail: "every Schedule A fact was replayed from a verified artifact"},
			{ID: "decision_conservation", Passed: decisionArtifact.RecordCount == receiptFacts, Severity: "block", Detail: "every Schedule A fact has one immutable calculation decision"},
			{ID: "candidate_conservation", Passed: counts.Candidates == counts.Complete+counts.Partial+counts.NotComparable, Severity: "block", Detail: "every candidate has one explicit calculation state"},
			{ID: "summary_independence", Passed: counts.SummaryFacts == counts.Reconciliations, Severity: "block", Detail: "each source summary produced one separate reconciliation"},
			{ID: "result_conservation", Passed: resultArtifact.RecordCount == counts.Candidates, Severity: "block", Detail: "one immutable result exists per calculated candidate"},
		},
	}
	if err := validateManifest(manifest); err != nil {
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

func loadLinkages(ctx context.Context, storageRoot string, manifest fecoccurrence.ClassicFactManifest) ([]LinkageFact, error) {
	reader, err := storageartifact.Open[classicFactEnvelope](ctx, storageRoot, descriptor(manifest.Facts))
	if err != nil {
		return nil, err
	}
	result := make([]LinkageFact, 0, manifest.Facts.RecordCount)
	for {
		fact, ok, readErr := reader.Next()
		if readErr != nil {
			reader.Abort()
			return nil, readErr
		}
		if !ok {
			break
		}
		if err := validateClassicEnvelope(fact, manifest); err != nil {
			reader.Abort()
			return nil, err
		}
		var typed fecoccurrence.LinkageTypedFields
		if err := decodeRawStrict(fact.TypedFields, &typed); err != nil {
			reader.Abort()
			return nil, err
		}
		result = append(result, LinkageFact{FactID: fact.FactID, State: fact.State, CandidateID: typed.CandidateID, CommitteeID: typed.CommitteeID, DesignationCode: typed.DesignationCode})
	}
	if err := reader.Close(); err != nil {
		return nil, err
	}
	return result, nil
}

func loadSummaries(ctx context.Context, storageRoot string, manifest fecoccurrence.ClassicFactManifest) ([]SummaryFact, error) {
	reader, err := storageartifact.Open[classicFactEnvelope](ctx, storageRoot, descriptor(manifest.Facts))
	if err != nil {
		return nil, err
	}
	result := make([]SummaryFact, 0, manifest.Facts.RecordCount)
	seen := make(map[string]struct{}, manifest.Facts.RecordCount)
	for {
		fact, ok, readErr := reader.Next()
		if readErr != nil {
			reader.Abort()
			return nil, readErr
		}
		if !ok {
			break
		}
		if err := validateClassicEnvelope(fact, manifest); err != nil {
			reader.Abort()
			return nil, err
		}
		var typed fecoccurrence.SummaryTypedFields
		if err := decodeRawStrict(fact.TypedFields, &typed); err != nil {
			reader.Abort()
			return nil, err
		}
		if _, duplicate := seen[typed.CandidateID]; duplicate {
			reader.Abort()
			return nil, fmt.Errorf("%s contains duplicate candidate %s", manifest.Dataset, typed.CandidateID)
		}
		seen[typed.CandidateID] = struct{}{}
		individual, ok := typed.Money["TTL_INDIV_CONTRIB"]
		if !ok {
			reader.Abort()
			return nil, fmt.Errorf("summary fact %s has no TTL_INDIV_CONTRIB", fact.FactID)
		}
		total, ok := typed.Money["TTL_RECEIPTS"]
		if !ok {
			reader.Abort()
			return nil, fmt.Errorf("summary fact %s has no TTL_RECEIPTS", fact.FactID)
		}
		result = append(result, SummaryFact{
			FactID: fact.FactID, FactType: fact.FactType, Dataset: fact.Dataset, CandidateID: typed.CandidateID, CoverageThrough: typed.CoverageThrough,
			TotalIndividualContributions: summaryAmount(individual), TotalReceipts: summaryAmount(total),
		})
	}
	if err := reader.Close(); err != nil {
		return nil, err
	}
	return result, nil
}

func summaryAmount(input fecoccurrence.SummaryMoneyObservation) SummaryAmount {
	return SummaryAmount{RawValue: input.RawValue, ReportedMinorUnits: input.ReportedMinorUnits, ObservationState: input.ObservationState}
}

func validateClassicEnvelope(fact classicFactEnvelope, manifest fecoccurrence.ClassicFactManifest) error {
	if fact.SchemaVersion != fecoccurrence.ClassicFactSchemaVersion || fact.Dataset != manifest.Dataset || fact.FactType != manifest.FactType || fact.Cycle != manifest.Cycle || fact.SourceReleaseID != manifest.SourceReleaseID {
		return fmt.Errorf("classic fact %s has incompatible identity", fact.FactID)
	}
	return nil
}

func loadScheduleAFactManifest(storageRoot, path string) (fecoccurrence.ScheduleAFactManifest, string, error) {
	manifest, _, err := readStrictJSON[fecoccurrence.ScheduleAFactManifest](path)
	if err != nil {
		return manifest, "", err
	}
	if manifest.Schema != "manifest.schema.json" || manifest.SchemaVersion != fecoccurrence.ScheduleAFactSetSchemaVersion || manifest.FactSchemaVersion != fecoccurrence.ScheduleAFactSchemaVersion || manifest.FactType != fecoccurrence.ScheduleAFactType || manifest.State != "published" {
		return manifest, "", fmt.Errorf("unsupported Schedule A fact manifest")
	}
	immutablePath := filepath.Join(storageRoot, "facts", "fec", "schedule-a", "manifests", manifest.FactSetID+".json")
	immutable, digest, err := readStrictJSON[fecoccurrence.ScheduleAFactManifest](immutablePath)
	if err != nil {
		return manifest, "", err
	}
	if !reflect.DeepEqual(manifest, immutable) {
		return manifest, "", fmt.Errorf("Schedule A fact pointer differs from immutable manifest")
	}
	if manifest.Facts.RecordCount != manifest.Counts.Facts || manifest.Cycle == "" || manifest.SourceReleaseID == "" {
		return manifest, "", fmt.Errorf("Schedule A fact counts or identity are invalid")
	}
	if manifest.Counts.SourceDuplicates != 0 {
		return manifest, "", fmt.Errorf("Schedule A fact set excludes %d duplicate source occurrences", manifest.Counts.SourceDuplicates)
	}
	return manifest, digest, nil
}

func loadClassicFactManifest(storageRoot, path, dataset string) (fecoccurrence.ClassicFactManifest, string, error) {
	manifest, _, err := readStrictJSON[fecoccurrence.ClassicFactManifest](path)
	if err != nil {
		return manifest, "", err
	}
	expectedFactTypes := map[string]string{
		"candidate-committee-linkage": "fec.candidate_committee_linkage.v1",
		"all-candidates-summary":      "fec.candidate_summary_all.v1",
		"current-campaigns-summary":   "fec.campaign_summary.v1",
	}
	if manifest.Schema != "manifest.schema.json" || manifest.SchemaVersion != fecoccurrence.ClassicFactSetSchemaVersion || manifest.FactSchemaVersion != fecoccurrence.ClassicFactSchemaVersion || manifest.Dataset != dataset || manifest.FactType != expectedFactTypes[dataset] || manifest.State != "published" {
		return manifest, "", fmt.Errorf("unsupported %s fact manifest", dataset)
	}
	immutablePath := filepath.Join(storageRoot, "facts", "fec", "classic", dataset, "manifests", manifest.FactSetID+".json")
	immutable, digest, err := readStrictJSON[fecoccurrence.ClassicFactManifest](immutablePath)
	if err != nil {
		return manifest, "", err
	}
	if !reflect.DeepEqual(manifest, immutable) {
		return manifest, "", fmt.Errorf("%s fact pointer differs from immutable manifest", dataset)
	}
	if manifest.Facts.RecordCount != manifest.Counts.Facts || manifest.Cycle == "" || manifest.SourceReleaseID == "" {
		return manifest, "", fmt.Errorf("%s fact counts or identity are invalid", dataset)
	}
	return manifest, digest, nil
}

func descriptor(input fecoccurrence.Artifact) storageartifact.Descriptor {
	return storageartifact.Descriptor{
		RecordCount: input.RecordCount, UncompressedBytes: input.UncompressedBytes, UncompressedSHA256: input.UncompressedSHA256,
		CompressedBytes: input.CompressedBytes, CompressedSHA256: input.CompressedSHA256, Compression: input.Compression, StorageKey: input.StorageKey,
	}
}

func validateManifest(manifest Manifest) error {
	if manifest.Schema != "manifest.schema.json" || manifest.SchemaVersion != ManifestSchemaVersion || manifest.Calculation != ContractID || manifest.CalculationVersion != ContractVersion || manifest.DecisionSchemaVersion != DecisionSchemaVersion || manifest.ResultSchemaVersion != ResultSchemaVersion || manifest.State != "published" {
		return fmt.Errorf("unsupported calculation manifest")
	}
	if !validDigest(manifest.CalculationSetID) || manifest.Cycle == "" || !strings.HasPrefix(manifest.SourceReleaseID, "fec-") || !validDigest(strings.TrimPrefix(manifest.SourceReleaseID, "fec-")) || manifest.RunID == "" || manifest.PublishedAt.IsZero() || len(manifest.InputFactSets) != 4 {
		return fmt.Errorf("calculation manifest identity is incomplete")
	}
	expectedRoles := []string{"all_candidates_summary", "candidate_committee_linkage", "current_campaigns_summary", "schedule_a_receipts"}
	identityParts := []string{"fec.candidate-itemized-individual-receipts-set.v1", manifest.CalculationVersion, manifest.DecisionSchemaVersion, manifest.ResultSchemaVersion}
	for index, reference := range manifest.InputFactSets {
		if reference.Role != expectedRoles[index] || !validDigest(reference.FactSetID) || !validDigest(reference.ManifestSHA256) {
			return fmt.Errorf("calculation input fact-set identity is invalid")
		}
		identityParts = append(identityParts, reference.Role, reference.FactSetID, reference.ManifestSHA256)
	}
	if manifest.CalculationSetID != digestParts(identityParts...) {
		return fmt.Errorf("calculation-set ID does not match canonical inputs")
	}
	if manifest.Decisions.Compression != "zstd" || manifest.Decisions.RecordCount != manifest.Counts.ReceiptFacts || manifest.Decisions.RecordCount != manifest.Counts.ReceiptDecisions || manifest.Decisions.CompressedBytes == 0 {
		return fmt.Errorf("calculation decision artifact is invalid")
	}
	if manifest.Results.Compression != "zstd" || manifest.Results.RecordCount != manifest.Counts.Candidates || manifest.Results.CompressedBytes == 0 {
		return fmt.Errorf("calculation result artifact is invalid")
	}
	if !validDigest(manifest.Decisions.CompressedSHA256) || !validDigest(manifest.Decisions.UncompressedSHA256) || !validDigest(manifest.Results.CompressedSHA256) || !validDigest(manifest.Results.UncompressedSHA256) {
		return fmt.Errorf("calculation artifact digest is invalid")
	}
	expectedDecisionPrefix := filepath.ToSlash(filepath.Join(calculationBase(), "decisions", "sha256", manifest.Decisions.CompressedSHA256[:2])) + "/"
	if !strings.HasPrefix(manifest.Decisions.StorageKey, expectedDecisionPrefix) {
		return fmt.Errorf("calculation decision storage identity is invalid")
	}
	expectedPrefix := filepath.ToSlash(filepath.Join(calculationBase(), "results", "sha256", manifest.Results.CompressedSHA256[:2])) + "/"
	if !strings.HasPrefix(manifest.Results.StorageKey, expectedPrefix) {
		return fmt.Errorf("calculation result storage identity is invalid")
	}
	if manifest.Counts.Candidates != manifest.Counts.Complete+manifest.Counts.Partial+manifest.Counts.NotComparable || manifest.Counts.SummaryFacts != manifest.Counts.Reconciliations || len(manifest.Checks) < 6 {
		return fmt.Errorf("calculation counts are not conserved")
	}
	for _, check := range manifest.Checks {
		if check.Severity == "block" && !check.Passed {
			return fmt.Errorf("blocking calculation check %s failed", check.ID)
		}
	}
	return nil
}

func validateManifestBacking(ctx context.Context, storageRoot string, manifest Manifest) error {
	immutablePath := filepath.Join(storageRoot, calculationBase(), "manifests", manifest.CalculationSetID+".json")
	immutable, _, err := readStrictJSON[Manifest](immutablePath)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(manifest, immutable) {
		return fmt.Errorf("calculation pointer differs from immutable manifest")
	}
	decisionPath, err := storageartifact.Resolve(storageRoot, manifest.Decisions.StorageKey)
	if err != nil {
		return err
	}
	if err := storageartifact.Verify(ctx, decisionPath, manifest.Decisions); err != nil {
		return err
	}
	resultPath, err := storageartifact.Resolve(storageRoot, manifest.Results.StorageKey)
	if err != nil {
		return err
	}
	return storageartifact.Verify(ctx, resultPath, manifest.Results)
}

func readManifestIfPresent(path string) (*Manifest, error) {
	manifest, _, err := readStrictJSON[Manifest](path)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &manifest, nil
}

func readStrictJSON[T any](path string) (T, string, error) {
	var value T
	content, err := os.ReadFile(path)
	if err != nil {
		return value, "", err
	}
	decoder := json.NewDecoder(strings.NewReader(string(content)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&value); err != nil {
		return value, "", err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return value, "", fmt.Errorf("multiple JSON values in %s", path)
		}
		return value, "", err
	}
	digest := sha256.Sum256(content)
	return value, hex.EncodeToString(digest[:]), nil
}

func decodeRawStrict(content json.RawMessage, target any) error {
	decoder := json.NewDecoder(strings.NewReader(string(content)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return fmt.Errorf("multiple typed-field JSON values")
		}
		return err
	}
	return nil
}

func digestParts(parts ...string) string {
	hash := sha256.New()
	for _, part := range parts {
		_, _ = hash.Write([]byte{0})
		_, _ = hash.Write([]byte(part))
	}
	return hex.EncodeToString(hash.Sum(nil))
}

func validDigest(value string) bool {
	if len(value) != 64 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil && value == strings.ToLower(value)
}

func calculationBase() string {
	return filepath.Join("calculations", "fec", "candidate-itemized-individual-receipts")
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
	directory, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer func() { _ = directory.Close() }()
	return directory.Sync()
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
		return fmt.Errorf("calculation current-manifest path escapes storage root")
	}
	return nil
}
