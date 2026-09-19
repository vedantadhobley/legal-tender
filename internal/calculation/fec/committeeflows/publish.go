package committeeflows

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"syscall"
	"time"

	fecoccurrence "github.com/vedantadhobley/legal-tender/internal/source/fec/occurrence"
	fecrelease "github.com/vedantadhobley/legal-tender/internal/source/fec/release"
	storageartifact "github.com/vedantadhobley/legal-tender/internal/storage/artifact"
)

// Publish calculates one immutable receiver-reported committee-flow set from
// one exact Schedule A columnar fact set.
func Publish(ctx context.Context, input PublishInput, runID string, options PublishOptions) (Manifest, error) {
	if options.Clock == nil {
		options.Clock = time.Now
	}
	if options.StorageRoot == "" || input.ColumnarFactManifestPath == "" {
		return Manifest{}, fmt.Errorf("storage root and Schedule A columnar fact manifest path are required")
	}
	if !fecrelease.ValidAcquisitionRunID(runID) {
		return Manifest{}, fmt.Errorf("run ID contains unsupported characters")
	}
	if options.Workers <= 0 {
		options.Workers = runtime.GOMAXPROCS(0)
		if options.Workers > 16 {
			options.Workers = 16
		}
	}

	factManifest, factManifestDigest, err := fecoccurrence.LoadPublishedScheduleAColumnarManifest(ctx, options.StorageRoot, input.ColumnarFactManifestPath)
	if err != nil {
		return Manifest{}, fmt.Errorf("load Schedule A columnar facts: %w", err)
	}
	if len(factManifest.Shards) == 0 || factManifest.Counts.Facts == 0 {
		return Manifest{}, fmt.Errorf("receiver-reported committee flow requires a nonempty Schedule A fact set")
	}
	if options.Workers > len(factManifest.Shards) {
		options.Workers = len(factManifest.Shards)
	}
	reference := FactSetReference{
		Role: "schedule_a_receipts", Dataset: "schedule-a", FactType: factManifest.FactType,
		FactSetID: factManifest.FactSetID, ManifestSHA256: factManifestDigest,
		PhysicalSchemaVersion: factManifest.PhysicalSchemaVersion,
	}
	calculationSetID := digestParts(
		ManifestSchemaVersion, ContractVersion, PolicyVersion, PublisherVersion,
		ResultSchemaVersion, ExceptionSchemaVersion, reference.Role,
		reference.FactSetID, reference.ManifestSHA256, reference.PhysicalSchemaVersion,
	)
	basePath := calculationBase()
	currentPath := options.CurrentManifestPath
	if currentPath == "" {
		currentPath = filepath.Join(options.StorageRoot, basePath, "current", factManifest.Cycle+".json")
	}
	if err := requirePathInside(options.StorageRoot, currentPath); err != nil {
		return Manifest{}, err
	}

	unlock, err := lockContext(ctx, filepath.Join(options.StorageRoot, basePath, ".publish-"+factManifest.Cycle+".lock"))
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
			return Manifest{}, fmt.Errorf("invalid current receiver-flow calculation: %w", err)
		}
		if err := validateManifestBacking(ctx, options.StorageRoot, *current); err != nil {
			return Manifest{}, err
		}
		if current.Cycle != factManifest.Cycle {
			return Manifest{}, fmt.Errorf("current receiver-flow calculation belongs to cycle %s", current.Cycle)
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
		if !reflect.DeepEqual(existing.InputFactSet, reference) {
			return Manifest{}, fmt.Errorf("immutable receiver-flow calculation manifest collision")
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
	if options.Progress != nil {
		options.Progress(fmt.Sprintf("calculating receiver-reported committee flows for %s with %d workers", factManifest.Cycle, options.Workers))
	}
	scan, err := scanColumnarFacts(ctx, options.StorageRoot, factManifest, calculationSetID, options.Workers, options.Progress)
	if err != nil {
		return Manifest{}, err
	}
	if scan.decisions.SourceFacts != factManifest.Counts.Facts || decisionTotal(scan.decisions) != scan.decisions.SourceFacts {
		return Manifest{}, fmt.Errorf("receiver-flow decisions do not conserve Schedule A facts")
	}
	if scan.counts.KnownAmountRows+scan.counts.UnknownAmountRows != scan.decisions.SourceFacts {
		return Manifest{}, fmt.Errorf("receiver-flow amount observations do not conserve Schedule A facts")
	}
	if !amountConserves(&scan.amounts.known, &scan.amounts.included, &scan.amounts.excluded, &scan.amounts.unresolved) {
		return Manifest{}, fmt.Errorf("receiver-flow signed amount dispositions do not conserve")
	}

	exceptionWriter, err := storageartifact.NewWriter(ctx, options.StorageRoot, temporaryDirectory, basePath, "exceptions")
	if err != nil {
		return Manifest{}, err
	}
	defer exceptionWriter.Abort()
	for _, exception := range scan.exceptions {
		if err := exceptionWriter.WriteJSON(exception); err != nil {
			return Manifest{}, err
		}
	}
	exceptionArtifact, err := exceptionWriter.Finalize()
	if err != nil {
		return Manifest{}, err
	}
	if exceptionArtifact.RecordCount != unresolvedTotal(scan.decisions) {
		return Manifest{}, fmt.Errorf("receiver-flow exceptions are not conserved")
	}

	resultWriter, err := storageartifact.NewWriter(ctx, options.StorageRoot, temporaryDirectory, basePath, "results")
	if err != nil {
		return Manifest{}, err
	}
	defer resultWriter.Abort()
	var resultAmount big.Int
	var resultRows uint64
	for _, key := range sortedGroupKeys(scan.groups) {
		accumulator := scan.groups[key]
		if accumulator.count != accumulator.positive+accumulator.negative+accumulator.zero {
			return Manifest{}, fmt.Errorf("receiver-flow result sign counts are not conserved")
		}
		result := Result{
			SchemaVersion:    ResultSchemaVersion,
			ResultID:         digestParts("fec.receiver-reported-committee-flow.result.v1", calculationSetID, factManifest.Cycle, key.source, key.recipient, key.role),
			CalculationSetID: calculationSetID, Cycle: factManifest.Cycle,
			SourceCommitteeID: key.source, RecipientCommitteeID: key.recipient, ReceiptRole: key.role,
			SignedAmountMinorUnits: accumulator.amount.String(), ReceiptCount: accumulator.count,
			PositiveCount: accumulator.positive, NegativeCount: accumulator.negative, ZeroCount: accumulator.zero,
		}
		if err := resultWriter.WriteJSON(result); err != nil {
			return Manifest{}, err
		}
		resultAmount.Add(&resultAmount, &accumulator.amount)
		resultRows += accumulator.count
	}
	resultArtifact, err := resultWriter.Finalize()
	if err != nil {
		return Manifest{}, err
	}
	if resultArtifact.RecordCount != scan.counts.ResultGroups || resultRows != scan.counts.IncludedRows || resultAmount.Cmp(&scan.amounts.included) != 0 {
		return Manifest{}, fmt.Errorf("receiver-flow grouped results are not conserved")
	}

	manifest := Manifest{
		Schema: "manifest.schema.json", SchemaVersion: ManifestSchemaVersion,
		CalculationSetID: calculationSetID, Calculation: ContractID, CalculationVersion: ContractVersion,
		PolicyVersion: PolicyVersion, PublisherVersion: PublisherVersion,
		ResultSchemaVersion: ResultSchemaVersion, ExceptionSchemaVersion: ExceptionSchemaVersion,
		Cycle: factManifest.Cycle, SourceReleaseID: factManifest.SourceReleaseID, InputFactSet: reference,
		RunID: runID, State: "published", PublishedAt: options.Clock().UTC(),
		Predicate: membershipPredicate(factManifest.PhysicalSchemaVersion), DecisionCounts: scan.decisions,
		ResultCounts: scan.counts,
		Amounts: AmountTotals{
			KnownSourceMinorUnits: scan.amounts.known.String(), IncludedMinorUnits: scan.amounts.included.String(),
			ExcludedMinorUnits: scan.amounts.excluded.String(), UnresolvedMinorUnits: scan.amounts.unresolved.String(),
		},
		Exceptions: exceptionArtifact, Results: resultArtifact,
	}
	manifest.Checks = []Check{
		{ID: "input_lineage", Passed: true, Severity: "block", Detail: "the immutable Schedule A manifest and every Parquet shard digest define one exact cycle input"},
		{ID: "decision_conservation", Passed: decisionTotal(scan.decisions) == scan.decisions.SourceFacts, Severity: "block", Detail: "every Schedule A fact has exactly one ordered terminal decision"},
		{ID: "amount_observation_conservation", Passed: scan.counts.KnownAmountRows+scan.counts.UnknownAmountRows == scan.decisions.SourceFacts, Severity: "block", Detail: "every fact has one known or unknown amount observation"},
		{ID: "signed_conservation", Passed: amountConserves(&scan.amounts.known, &scan.amounts.included, &scan.amounts.excluded, &scan.amounts.unresolved) && resultAmount.Cmp(&scan.amounts.included) == 0, Severity: "block", Detail: "known signed cents conserve through included, excluded, unresolved, and grouped-result totals"},
		{ID: "exception_conservation", Passed: exceptionArtifact.RecordCount == unresolvedTotal(scan.decisions), Severity: "block", Detail: "every unresolved decision has one sparse exception"},
		{ID: "result_conservation", Passed: resultArtifact.RecordCount == scan.counts.ResultGroups && resultRows == scan.counts.IncludedRows, Severity: "block", Detail: "every included fact contributes once to one source-recipient-role result"},
		{ID: "result_sign_conservation", Passed: scan.counts.IncludedRows == scan.counts.IncludedPositiveRows+scan.counts.IncludedNegativeRows+scan.counts.IncludedZeroRows, Severity: "block", Detail: "included positive, negative, and zero counts conserve"},
		{ID: "helper_field_neutrality", Passed: true, Severity: "observe", Detail: "entity type, is_individual, names, and committee masters do not alter membership"},
		{ID: "no_dense_decision_artifact", Passed: true, Severity: "block", Detail: "ordinary decisions are reconstructed from the exact fact set and predicate; only unresolved states are materialized"},
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

// LoadPublishedManifest verifies one current or immutable calculation manifest
// and both content-addressed artifacts before returning it downstream.
func LoadPublishedManifest(ctx context.Context, storageRoot, path string) (Manifest, string, error) {
	if storageRoot == "" || path == "" {
		return Manifest{}, "", fmt.Errorf("storage root and receiver-flow calculation manifest path are required")
	}
	if err := requirePathInside(storageRoot, path); err != nil {
		return Manifest{}, "", err
	}
	manifest, _, err := readStrictJSON[Manifest](path)
	if err != nil {
		return Manifest{}, "", err
	}
	if err := validateManifest(manifest); err != nil {
		return Manifest{}, "", err
	}
	immutablePath := filepath.Join(storageRoot, calculationBase(), "manifests", manifest.CalculationSetID+".json")
	immutable, digest, err := readStrictJSON[Manifest](immutablePath)
	if err != nil {
		return Manifest{}, "", err
	}
	if !reflect.DeepEqual(manifest, immutable) {
		return Manifest{}, "", fmt.Errorf("receiver-flow calculation pointer differs from immutable manifest")
	}
	if err := validateManifestBacking(ctx, storageRoot, immutable); err != nil {
		return Manifest{}, "", err
	}
	return immutable, digest, nil
}

func membershipPredicate(physicalSchema string) MembershipPredicate {
	return MembershipPredicate{
		Version: PolicyVersion, InputPhysicalSchemaVersion: physicalSchema,
		MembershipIdentity: "Schedule A fact-set ID plus one-based source row ordinal",
		RequiredColumns: []string{
			"lt_source_row_ordinal", "lt_normalization_state", "cmte_id", "contbr_id", "clean_contbr_id",
			"lt_memoed_subtotal", "lt_receipt_amount_state", "lt_receipt_amount_minor_units", "receipt_tp",
		},
		DecisionOrder: []PredicateRule{
			{State: DecisionInvalidNormalization, All: []string{"lt_normalization_state != valid"}},
			{State: DecisionUnresolvedRecipient, All: []string{"recipient committee ID is not exact C plus eight digits"}},
			{State: DecisionExcludedNoSource, All: []string{"neither contributor ID is an exact committee ID"}},
			{State: DecisionUnresolvedOneSided, All: []string{"only one contributor ID is an exact committee ID"}},
			{State: DecisionUnresolvedConflict, All: []string{"raw and cleaned contributor committee IDs disagree"}},
			{State: DecisionExcludedMemo, All: []string{"lt_memoed_subtotal is true"}},
			{State: DecisionUnresolvedAmount, All: []string{"exact reported signed minor units are absent"}},
			{State: "receipt_type_role_decision", All: []string{"classify exact receipt_tp through receipt_type_rules"}},
		},
		ReceiptTypeRules: ReceiptTypeRules(),
		ExceptionalStates: []string{
			DecisionInvalidNormalization, DecisionUnresolvedRecipient, DecisionUnresolvedOneSided,
			DecisionUnresolvedConflict, DecisionUnresolvedAmount, DecisionUnresolvedRole,
		},
		ExcludedStates: []string{
			DecisionExcludedNoSource, DecisionExcludedMemo, DecisionExcludedOutbound,
			DecisionExcludedSemanticMemo, DecisionExcludedEarmarked, DecisionExcludedNoncommittee,
		},
	}
}

func calculationBase() string {
	return filepath.Join("calculations", "fec", "receiver-reported-committee-flows")
}

func validateManifest(manifest Manifest) error {
	if manifest.Schema != "manifest.schema.json" || manifest.SchemaVersion != ManifestSchemaVersion ||
		manifest.Calculation != ContractID || manifest.CalculationVersion != ContractVersion || manifest.PolicyVersion != PolicyVersion ||
		manifest.PublisherVersion != PublisherVersion || manifest.ResultSchemaVersion != ResultSchemaVersion ||
		manifest.ExceptionSchemaVersion != ExceptionSchemaVersion || manifest.State != "published" ||
		!reflect.DeepEqual(manifest.Predicate, membershipPredicate(manifest.InputFactSet.PhysicalSchemaVersion)) {
		return fmt.Errorf("unsupported receiver-flow calculation manifest")
	}
	if !validDigest(manifest.CalculationSetID) || !validCycle(manifest.Cycle) || !validSourceReleaseID(manifest.SourceReleaseID) ||
		!fecrelease.ValidAcquisitionRunID(manifest.RunID) || manifest.PublishedAt.IsZero() {
		return fmt.Errorf("receiver-flow calculation identity is incomplete")
	}
	reference := manifest.InputFactSet
	if reference.Role != "schedule_a_receipts" || reference.Dataset != "schedule-a" || reference.FactType != fecoccurrence.ScheduleAFactType ||
		!validDigest(reference.FactSetID) || !validDigest(reference.ManifestSHA256) || reference.PhysicalSchemaVersion == "" {
		return fmt.Errorf("receiver-flow input fact-set identity is invalid")
	}
	expectedID := digestParts(
		ManifestSchemaVersion, ContractVersion, PolicyVersion, PublisherVersion,
		ResultSchemaVersion, ExceptionSchemaVersion, reference.Role,
		reference.FactSetID, reference.ManifestSHA256, reference.PhysicalSchemaVersion,
	)
	if manifest.CalculationSetID != expectedID {
		return fmt.Errorf("receiver-flow calculation-set ID does not match canonical inputs")
	}
	if decisionTotal(manifest.DecisionCounts) != manifest.DecisionCounts.SourceFacts ||
		manifest.ResultCounts.KnownAmountRows+manifest.ResultCounts.UnknownAmountRows != manifest.DecisionCounts.SourceFacts ||
		manifest.ResultCounts.IncludedRows != manifest.DecisionCounts.IncludedReceiverReportedCommitteeFlow ||
		manifest.ResultCounts.IncludedRows != manifest.ResultCounts.IncludedPositiveRows+manifest.ResultCounts.IncludedNegativeRows+manifest.ResultCounts.IncludedZeroRows ||
		manifest.Results.RecordCount != manifest.ResultCounts.ResultGroups || manifest.Exceptions.RecordCount != unresolvedTotal(manifest.DecisionCounts) {
		return fmt.Errorf("receiver-flow calculation counts are not conserved")
	}
	known, included, excluded, unresolved, err := parseAmounts(manifest.Amounts)
	if err != nil || !amountConserves(known, included, excluded, unresolved) {
		return fmt.Errorf("receiver-flow calculation amounts are invalid or do not conserve")
	}
	if !validDescriptor(manifest.Exceptions) || !validDescriptor(manifest.Results) {
		return fmt.Errorf("receiver-flow artifact descriptors are invalid")
	}
	return validateChecks(manifest.Checks)
}

func validateChecks(checks []Check) error {
	expectedChecks := expectedCheckDefinitions()
	if len(checks) != len(expectedChecks) {
		return fmt.Errorf("receiver-flow calculation has %d checks; want %d", len(checks), len(expectedChecks))
	}
	for index, check := range checks {
		expected := expectedChecks[index]
		if check.ID != expected.id || check.Severity != expected.severity || check.Detail == "" {
			return fmt.Errorf("receiver-flow check %d does not match the contract", index)
		}
		if check.Severity == "block" && !check.Passed {
			return fmt.Errorf("receiver-flow blocking check %s failed", check.ID)
		}
	}
	return nil
}

func expectedCheckDefinitions() []struct {
	id       string
	severity string
} {
	return []struct {
		id       string
		severity string
	}{
		{"input_lineage", "block"},
		{"decision_conservation", "block"},
		{"amount_observation_conservation", "block"},
		{"signed_conservation", "block"},
		{"exception_conservation", "block"},
		{"result_conservation", "block"},
		{"result_sign_conservation", "block"},
		{"helper_field_neutrality", "observe"},
		{"no_dense_decision_artifact", "block"},
	}
}

func validateManifestBacking(ctx context.Context, storageRoot string, manifest Manifest) error {
	for kind, descriptor := range map[string]storageartifact.Descriptor{"exceptions": manifest.Exceptions, "results": manifest.Results} {
		path, err := storageartifact.Resolve(storageRoot, descriptor.StorageKey)
		if err != nil {
			return err
		}
		if err := storageartifact.Verify(ctx, path, descriptor); err != nil {
			return fmt.Errorf("verify receiver-flow %s artifact: %w", kind, err)
		}
	}
	return nil
}

func validDescriptor(value storageartifact.Descriptor) bool {
	return value.Compression == "zstd" && validDigest(value.UncompressedSHA256) && validDigest(value.CompressedSHA256) &&
		value.CompressedBytes > 0 && value.StorageKey != ""
}

func parseAmounts(value AmountTotals) (known, included, excluded, unresolved *big.Int, err error) {
	values := []*big.Int{new(big.Int), new(big.Int), new(big.Int), new(big.Int)}
	texts := []string{value.KnownSourceMinorUnits, value.IncludedMinorUnits, value.ExcludedMinorUnits, value.UnresolvedMinorUnits}
	for index, text := range texts {
		if _, ok := values[index].SetString(text, 10); !ok || values[index].String() != text {
			return nil, nil, nil, nil, fmt.Errorf("invalid exact minor-unit value %q", text)
		}
	}
	return values[0], values[1], values[2], values[3], nil
}

func amountConserves(total *big.Int, parts ...*big.Int) bool {
	var sum big.Int
	for _, part := range parts {
		sum.Add(&sum, part)
	}
	return total.Cmp(&sum) == 0
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

func validSourceReleaseID(value string) bool {
	return strings.HasPrefix(value, "fec-") && validDigest(strings.TrimPrefix(value, "fec-"))
}

func validCycle(value string) bool {
	if len(value) != 4 {
		return false
	}
	for _, character := range value {
		if character < '0' || character > '9' {
			return false
		}
	}
	return true
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
		return fmt.Errorf("receiver-flow manifest path escapes storage root")
	}
	return nil
}
