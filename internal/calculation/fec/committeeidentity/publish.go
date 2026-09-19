package committeeidentity

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

	fecflowmastergaps "github.com/vedantadhobley/legal-tender/internal/audit/fecflowmastergaps"
	fecrelease "github.com/vedantadhobley/legal-tender/internal/source/fec/release"
	storageartifact "github.com/vedantadhobley/legal-tender/internal/storage/artifact"
)

// Publish classifies every graph endpoint absent from the selected cycle
// committee master. Exact historical evidence is preserved as evidence; it is
// never copied into the selected cycle master or treated as terminal identity.
func Publish(ctx context.Context, input PublishInput, runID string, options PublishOptions) (Manifest, error) {
	if options.Clock == nil {
		options.Clock = time.Now
	}
	if options.StorageRoot == "" || options.Cycle == "" || input.ReadinessBundlePath == "" {
		return Manifest{}, fmt.Errorf("storage root, cycle, and receiver-flow readiness bundle are required")
	}
	if !fecrelease.ValidAcquisitionRunID(runID) {
		return Manifest{}, fmt.Errorf("run ID contains unsupported characters")
	}
	if len(input.CommitteeComparisonManifestPaths) == 0 ||
		len(input.CommitteeHistoryManifestPaths)+len(input.RawHistoryArchives) == 0 ||
		input.LinkageManifestPath == "" || len(input.SummaryManifestPaths) == 0 {
		return Manifest{}, fmt.Errorf("comparison, history, linkage, and summary evidence are required")
	}

	report, err := fecflowmastergaps.Audit(ctx, fecflowmastergaps.Options{
		StorageRoot: options.StorageRoot, Cycle: options.Cycle,
		ReadinessBundlePath:              input.ReadinessBundlePath,
		CommitteeComparisonManifestPaths: append([]string(nil), input.CommitteeComparisonManifestPaths...),
		CommitteeHistoryManifestPaths:    append([]string(nil), input.CommitteeHistoryManifestPaths...),
		RawHistoryArchives:               append([]fecflowmastergaps.RawHistoryArchiveInput(nil), input.RawHistoryArchives...),
		LinkageManifestPath:              input.LinkageManifestPath,
		SummaryManifestPaths:             append([]string(nil), input.SummaryManifestPaths...),
		TraceSourceReceipts:              false,
		Clock:                            options.Clock,
		Progress:                         options.Progress,
	})
	if err != nil {
		return Manifest{}, fmt.Errorf("audit receiver-flow committee identity: %w", err)
	}
	evidenceInputs := identityInputs(report.Inputs)
	inputJSON, err := json.Marshal(evidenceInputs)
	if err != nil {
		return Manifest{}, err
	}
	calculationSetID := calculationIdentity(report.Cycle, report.SourceReleaseID, inputJSON)
	basePath := calculationBase()
	currentPath := options.CurrentManifestPath
	if currentPath == "" {
		currentPath = filepath.Join(options.StorageRoot, basePath, "current", report.Cycle+".json")
	}
	if err := requirePathInside(options.StorageRoot, currentPath); err != nil {
		return Manifest{}, err
	}

	unlock, err := lockContext(ctx, filepath.Join(options.StorageRoot, basePath, ".publish-"+report.Cycle+".lock"))
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
			return Manifest{}, fmt.Errorf("invalid current committee-identity calculation: %w", err)
		}
		if err := validateManifestBacking(ctx, options.StorageRoot, *current); err != nil {
			return Manifest{}, err
		}
		if current.Cycle != report.Cycle {
			return Manifest{}, fmt.Errorf("current committee-identity calculation belongs to cycle %s", current.Cycle)
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
		if !reflect.DeepEqual(existing.Inputs, evidenceInputs) {
			return Manifest{}, fmt.Errorf("immutable committee-identity manifest collision")
		}
		if err := validateManifestBacking(ctx, options.StorageRoot, *existing); err != nil {
			return Manifest{}, err
		}
		if err := writeAtomicJSON(currentPath, *existing); err != nil {
			return Manifest{}, err
		}
		return *existing, nil
	}

	temporaryDirectory := filepath.Join(options.StorageRoot, basePath, "staging", calculationSetID, runID)
	if err := os.MkdirAll(temporaryDirectory, 0o750); err != nil {
		return Manifest{}, err
	}
	defer func() { _ = os.RemoveAll(temporaryDirectory) }()
	writer, err := storageartifact.NewWriter(ctx, options.StorageRoot, temporaryDirectory, basePath, "decisions")
	if err != nil {
		return Manifest{}, err
	}
	defer writer.Abort()
	counts := Counts{
		ReferencedCommittees:       report.Counts.ReferencedCommittees,
		CurrentCycleMasters:        report.Counts.ReferencedCommittees - report.Counts.CommitteesMissingCurrentMaster,
		TerminalIdentityEligible:   report.Counts.ReferencedCommittees - report.Counts.CommitteesMissingCurrentMaster,
		TerminalIdentityIneligible: report.Counts.CommitteesMissingCurrentMaster,
	}
	for _, gap := range report.Committees {
		decision, decisionErr := decisionFromGap(calculationSetID, report.Cycle, gap)
		if decisionErr != nil {
			return Manifest{}, decisionErr
		}
		accumulateDecision(&counts, decision)
		if err := writer.WriteJSON(decision); err != nil {
			return Manifest{}, err
		}
	}
	artifact, err := writer.Finalize()
	if err != nil {
		return Manifest{}, err
	}
	if artifact.RecordCount != counts.IdentityCoverageDecisions ||
		counts.IdentityCoverageDecisions != report.Counts.CommitteesMissingCurrentMaster {
		return Manifest{}, fmt.Errorf("committee-identity decision membership is not conserved")
	}

	manifest := Manifest{
		Schema: "manifest.schema.json", SchemaVersion: ManifestSchemaVersion,
		CalculationSetID: calculationSetID, Calculation: ContractID,
		CalculationVersion: ContractVersion, PublisherVersion: PublisherVersion,
		DecisionSchemaVersion: DecisionSchemaVersion, MethodVersion: MethodVersion,
		Cycle: report.Cycle, SourceReleaseID: report.SourceReleaseID, Inputs: evidenceInputs,
		RunID: runID, State: "published", PublishedAt: options.Clock().UTC(),
		Counts: counts, Decisions: artifact,
	}
	manifest.Checks = []Check{
		{ID: "input_lineage", Passed: true, Severity: "block", Detail: "the exact flow bundle, calculation, selected master, comparison masters, and historical archives passed complete verification"},
		{ID: "gap_membership", Passed: counts.CurrentCycleMasters+counts.IdentityCoverageDecisions == counts.ReferencedCommittees, Severity: "block", Detail: "every referenced committee has either a selected-cycle master or one explicit coverage decision"},
		{ID: "decision_conservation", Passed: decisionCount(counts) == counts.IdentityCoverageDecisions && artifact.RecordCount == counts.IdentityCoverageDecisions, Severity: "block", Detail: "every selected-master gap has exactly one identity state"},
		{ID: "terminal_identity_guard", Passed: counts.TerminalIdentityEligible == counts.CurrentCycleMasters && counts.TerminalIdentityIneligible == counts.IdentityCoverageDecisions, Severity: "block", Detail: "historical, alternate-release, and unresolved IDs are ineligible for terminal-source classification"},
		{ID: "exact_id_only", Passed: true, Severity: "block", Detail: "registration evidence is joined only by exact reported committee ID; names never repair an ID"},
		{ID: "source_grain_preserved", Passed: true, Severity: "block", Detail: "each official registration assertion retains cycle, immutable source identity, row identity, fields, and source issues"},
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

func identityInputs(inputs fecflowmastergaps.Inputs) EvidenceInputs {
	return EvidenceInputs{
		ReadinessBundle: inputs.ReadinessBundle, Calculation: inputs.Calculation,
		CurrentCommitteeMaster: inputs.CurrentCommitteeMaster,
		SameCycleComparisons:   append([]fecflowmastergaps.FactSetReference(nil), inputs.SameCycleComparisons...),
		NormalizedHistory:      append([]fecflowmastergaps.FactSetReference(nil), inputs.NormalizedHistory...),
		RawHistory:             append([]fecflowmastergaps.RawHistoryArchive(nil), inputs.RawHistory...),
	}
}

func decisionFromGap(calculationSetID, cycle string, gap fecflowmastergaps.CommitteeGap) (Decision, error) {
	decision := Decision{
		SchemaVersion: DecisionSchemaVersion, CalculationSetID: calculationSetID,
		Cycle: cycle, CommitteeID: gap.CommitteeID,
		EndpointRoles:            append([]string(nil), gap.EndpointRoles...),
		TerminalIdentityEligible: false,
		SameCycleAssertions:      append([]fecflowmastergaps.CommitteeHistoricalAssertion(nil), gap.SameCycleComparisonMasters...),
		HistoricalAssertions:     append([]fecflowmastergaps.CommitteeHistoricalAssertion(nil), gap.HistoricalMasters...),
	}
	switch gap.State {
	case "found_only_in_historical_master", "found_in_same_cycle_and_historical_masters":
		decision.State = StateHistoricalRegistration
		decision.EvidenceCodes = []string{"exact_reported_id_in_official_historical_master"}
		if len(decision.SameCycleAssertions) != 0 {
			decision.EvidenceCodes = append(decision.EvidenceCodes, "exact_reported_id_in_alternate_same_cycle_release")
		}
	case "found_in_different_same_cycle_master":
		decision.State = StateAlternateReleaseRegistration
		decision.EvidenceCodes = []string{"exact_reported_id_in_alternate_same_cycle_release"}
	case "absent_from_all_audited_masters":
		decision.State = StateUnresolvedReportedID
		decision.EvidenceCodes = []string{"reported_id_absent_from_all_audited_committee_masters"}
	default:
		return Decision{}, fmt.Errorf("committee %s has unsupported audit state %q", gap.CommitteeID, gap.State)
	}
	decision.DecisionID = decisionIdentity(calculationSetID, cycle, decision.CommitteeID, decision.State)
	if err := validateDecision(decision); err != nil {
		return Decision{}, err
	}
	return decision, nil
}

func accumulateDecision(counts *Counts, decision Decision) {
	counts.IdentityCoverageDecisions++
	switch decision.State {
	case StateHistoricalRegistration:
		counts.HistoricalRegistrations++
	case StateAlternateReleaseRegistration:
		counts.AlternateReleaseRegistrations++
	case StateUnresolvedReportedID:
		counts.UnresolvedReportedIDs++
	}
	for _, role := range decision.EndpointRoles {
		switch role {
		case "source":
			counts.SourceEndpointDecisions++
		case "recipient":
			counts.RecipientEndpointDecisions++
		}
	}
	if len(decision.EndpointRoles) == 2 {
		counts.BothEndpointRoleDecisions++
	}
	counts.HistoricalRegistrationAssertions += uint64(len(decision.HistoricalAssertions))
	counts.AlternateRegistrationAssertions += uint64(len(decision.SameCycleAssertions))
}

func decisionCount(counts Counts) uint64 {
	return counts.HistoricalRegistrations + counts.AlternateReleaseRegistrations + counts.UnresolvedReportedIDs
}

func validateDecision(decision Decision) error {
	if decision.SchemaVersion != DecisionSchemaVersion || !validDigest(decision.DecisionID) ||
		!validDigest(decision.CalculationSetID) || decision.Cycle == "" || decision.CommitteeID == "" ||
		decision.TerminalIdentityEligible || len(decision.EndpointRoles) == 0 || len(decision.EvidenceCodes) == 0 {
		return fmt.Errorf("invalid committee-identity decision for %s", decision.CommitteeID)
	}
	if decision.DecisionID != decisionIdentity(decision.CalculationSetID, decision.Cycle, decision.CommitteeID, decision.State) {
		return fmt.Errorf("committee %s decision ID does not match its identity", decision.CommitteeID)
	}
	roles := append([]string(nil), decision.EndpointRoles...)
	sort.Strings(roles)
	if !reflect.DeepEqual(roles, decision.EndpointRoles) || len(roles) > 2 {
		return fmt.Errorf("committee %s has invalid endpoint roles", decision.CommitteeID)
	}
	for index, role := range roles {
		if role != "source" && role != "recipient" || index > 0 && role == roles[index-1] {
			return fmt.Errorf("committee %s has invalid endpoint role %q", decision.CommitteeID, role)
		}
	}
	switch decision.State {
	case StateHistoricalRegistration:
		if len(decision.HistoricalAssertions) == 0 {
			return fmt.Errorf("historical registration %s has no historical assertion", decision.CommitteeID)
		}
	case StateAlternateReleaseRegistration:
		if len(decision.SameCycleAssertions) == 0 || len(decision.HistoricalAssertions) != 0 {
			return fmt.Errorf("alternate registration %s has invalid assertions", decision.CommitteeID)
		}
	case StateUnresolvedReportedID:
		if len(decision.SameCycleAssertions) != 0 || len(decision.HistoricalAssertions) != 0 {
			return fmt.Errorf("unresolved reported ID %s contains registration assertions", decision.CommitteeID)
		}
	default:
		return fmt.Errorf("unsupported committee-identity state %q", decision.State)
	}
	return nil
}

// LoadPublishedManifest verifies an exact immutable calculation pointer and
// reads every decision to enforce membership and state conservation.
func LoadPublishedManifest(ctx context.Context, storageRoot, path string) (Manifest, string, error) {
	if storageRoot == "" || path == "" {
		return Manifest{}, "", fmt.Errorf("storage root and committee-identity manifest path are required")
	}
	if err := requirePathInside(storageRoot, path); err != nil {
		return Manifest{}, "", err
	}
	pointer, _, err := readStrictJSON[Manifest](path)
	if err != nil {
		return Manifest{}, "", err
	}
	if err := validateManifest(pointer); err != nil {
		return Manifest{}, "", err
	}
	immutablePath := filepath.Join(storageRoot, calculationBase(), "manifests", pointer.CalculationSetID+".json")
	immutable, digest, err := readStrictJSON[Manifest](immutablePath)
	if err != nil {
		return Manifest{}, "", err
	}
	if !reflect.DeepEqual(pointer, immutable) {
		return Manifest{}, "", fmt.Errorf("committee-identity pointer differs from immutable manifest")
	}
	if err := validateManifestBacking(ctx, storageRoot, immutable); err != nil {
		return Manifest{}, "", err
	}
	if err := validateEvidenceBacking(ctx, storageRoot, immutable); err != nil {
		return Manifest{}, "", err
	}
	return immutable, digest, nil
}

func ReadDecisions(ctx context.Context, storageRoot string, manifest Manifest) ([]Decision, error) {
	reader, err := storageartifact.Open[Decision](ctx, storageRoot, manifest.Decisions)
	if err != nil {
		return nil, err
	}
	defer reader.Abort()
	decisions := make([]Decision, 0, manifest.Decisions.RecordCount)
	for {
		decision, ok, readErr := reader.Next()
		if readErr != nil {
			return nil, readErr
		}
		if !ok {
			break
		}
		if decision.CalculationSetID != manifest.CalculationSetID || decision.Cycle != manifest.Cycle {
			return nil, fmt.Errorf("committee-identity decision does not match its manifest")
		}
		if err := validateDecision(decision); err != nil {
			return nil, err
		}
		decisions = append(decisions, decision)
	}
	if err := reader.Close(); err != nil {
		return nil, err
	}
	return decisions, nil
}

func validateManifestBacking(ctx context.Context, storageRoot string, manifest Manifest) error {
	decisions, err := ReadDecisions(ctx, storageRoot, manifest)
	if err != nil {
		return fmt.Errorf("verify committee-identity decisions: %w", err)
	}
	counts := Counts{
		ReferencedCommittees:       manifest.Counts.ReferencedCommittees,
		CurrentCycleMasters:        manifest.Counts.CurrentCycleMasters,
		TerminalIdentityEligible:   manifest.Counts.CurrentCycleMasters,
		TerminalIdentityIneligible: uint64(len(decisions)),
	}
	seen := make(map[string]struct{}, len(decisions))
	for _, decision := range decisions {
		if _, duplicate := seen[decision.CommitteeID]; duplicate {
			return fmt.Errorf("duplicate committee-identity decision for %s", decision.CommitteeID)
		}
		seen[decision.CommitteeID] = struct{}{}
		accumulateDecision(&counts, decision)
	}
	if counts != manifest.Counts || uint64(len(decisions)) != manifest.Decisions.RecordCount {
		return fmt.Errorf("committee-identity decision counts differ from manifest")
	}
	return nil
}

func validateManifest(manifest Manifest) error {
	if manifest.Schema != "manifest.schema.json" || manifest.SchemaVersion != ManifestSchemaVersion ||
		manifest.Calculation != ContractID || manifest.CalculationVersion != ContractVersion ||
		manifest.PublisherVersion != PublisherVersion || manifest.DecisionSchemaVersion != DecisionSchemaVersion ||
		manifest.MethodVersion != MethodVersion || manifest.State != "published" ||
		!validDigest(manifest.CalculationSetID) || manifest.Cycle == "" || manifest.SourceReleaseID == "" ||
		manifest.RunID == "" || manifest.PublishedAt.IsZero() || !validDigest(manifest.Inputs.ReadinessBundle.BundleID) ||
		!validDigest(manifest.Inputs.ReadinessBundle.ManifestSHA256) ||
		!validDigest(manifest.Inputs.Calculation.CalculationSetID) || !validDigest(manifest.Inputs.Calculation.ManifestSHA256) ||
		!validDigest(manifest.Inputs.Calculation.ResultsSHA256) || !validDigest(manifest.Inputs.CurrentCommitteeMaster.FactSetID) ||
		!validDigest(manifest.Inputs.CurrentCommitteeMaster.ManifestSHA256) || !validDigest(manifest.Inputs.CurrentCommitteeMaster.FactsSHA256) {
		return fmt.Errorf("invalid committee-identity manifest")
	}
	if manifest.Counts.CurrentCycleMasters+manifest.Counts.IdentityCoverageDecisions != manifest.Counts.ReferencedCommittees ||
		decisionCount(manifest.Counts) != manifest.Counts.IdentityCoverageDecisions ||
		manifest.Counts.TerminalIdentityEligible != manifest.Counts.CurrentCycleMasters ||
		manifest.Counts.TerminalIdentityIneligible != manifest.Counts.IdentityCoverageDecisions ||
		manifest.Decisions.RecordCount != manifest.Counts.IdentityCoverageDecisions || manifest.Decisions.Compression != "zstd" ||
		!validDigest(manifest.Decisions.UncompressedSHA256) || !validDigest(manifest.Decisions.CompressedSHA256) || manifest.Decisions.StorageKey == "" {
		return fmt.Errorf("committee-identity manifest counts or artifact are invalid")
	}
	inputJSON, err := json.Marshal(manifest.Inputs)
	if err != nil {
		return err
	}
	if manifest.CalculationSetID != calculationIdentity(manifest.Cycle, manifest.SourceReleaseID, inputJSON) {
		return fmt.Errorf("committee-identity calculation ID does not match its inputs")
	}
	for _, check := range manifest.Checks {
		if check.ID == "" || check.Severity != "block" || !check.Passed {
			return fmt.Errorf("committee-identity manifest contains a failed or invalid check")
		}
	}
	if len(manifest.Checks) == 0 {
		return fmt.Errorf("committee-identity manifest contains no checks")
	}
	return nil
}

func calculationBase() string {
	return filepath.Join("calculations", "fec", "receiver-flow-committee-identity-coverage")
}

func calculationIdentity(cycle, sourceReleaseID string, inputJSON []byte) string {
	return digestParts(
		ManifestSchemaVersion, ContractVersion, DecisionSchemaVersion,
		MethodVersion, PublisherVersion, cycle, sourceReleaseID, string(inputJSON),
	)
}

func decisionIdentity(calculationSetID, cycle, committeeID, state string) string {
	return digestParts(DecisionSchemaVersion, calculationSetID, cycle, committeeID, state)
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
	if len(value) != 64 || value != strings.ToLower(value) {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func readManifestIfPresent(path string) (*Manifest, error) {
	manifest, _, err := readStrictJSON[Manifest](path)
	if err == nil {
		return &manifest, nil
	}
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	return nil, err
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
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			err = fmt.Errorf("multiple JSON values")
		}
		return value, "", err
	}
	digest := sha256.Sum256(content)
	return value, hex.EncodeToString(digest[:]), nil
}

func writeAtomicJSON(path string, value any) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return err
	}
	content, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	content = append(content, '\n')
	temporary, err := os.CreateTemp(filepath.Dir(path), ".manifest-*.json")
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
	return os.Rename(temporaryPath, path)
}

func requirePathInside(root, path string) error {
	absoluteRoot, err := filepath.Abs(root)
	if err != nil {
		return err
	}
	absolutePath, err := filepath.Abs(path)
	if err != nil {
		return err
	}
	relative, err := filepath.Rel(absoluteRoot, absolutePath)
	if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return fmt.Errorf("path %q is outside storage root", path)
	}
	return nil
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
		err = syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
		if err == nil {
			return func() {
				_ = syscall.Flock(int(file.Fd()), syscall.LOCK_UN)
				_ = file.Close()
			}, nil
		}
		if !errors.Is(err, syscall.EWOULDBLOCK) && !errors.Is(err, syscall.EAGAIN) {
			_ = file.Close()
			return nil, err
		}
		select {
		case <-ctx.Done():
			_ = file.Close()
			return nil, ctx.Err()
		case <-time.After(25 * time.Millisecond):
		}
	}
}
