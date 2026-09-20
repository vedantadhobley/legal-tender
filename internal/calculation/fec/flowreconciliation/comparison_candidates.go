package flowreconciliation

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"time"

	fecrelease "github.com/vedantadhobley/legal-tender/internal/source/fec/release"
	artifact "github.com/vedantadhobley/legal-tender/internal/storage/artifact"
)

const (
	ComparisonPublicationBase        = "calculations/fec/committee-flow-comparison-candidates/v1"
	ComparisonContractID             = "fec/committee-flow-comparison-candidates"
	ComparisonContractVersion        = "1.0.0"
	ComparisonManifestSchemaVersion  = "legal-tender.fec.committee-flow-comparison-candidate-set.v1"
	ComparisonCandidateSchemaVersion = "legal-tender.fec.committee-flow-comparison-candidate.v1"
	ComparisonMethodVersion          = "legal-tender.fec.committee-flow-pair-comparison-method.v1"
	ComparisonPublisherVersion       = "legal-tender.fec.committee-flow-comparison-candidate-publisher.v1"

	ComparisonExactSignature   = "exact_signature"
	ComparisonDateDisagreement = "role_amount_date_disagreement"
	ComparisonMissingDate      = "role_amount_missing_date"
	ComparisonAmountConflict   = "role_date_amount_conflict"
	ComparisonRoleConflict     = "amount_date_role_conflict"

	ComparisonMutualOneToOne      = "mutual_one_to_one"
	ComparisonCompetingCandidates = "competing_candidates"
	ComparisonFinancialEffectNone = "none"

	defaultMaxComparisonCandidates = 10_000_000
)

type ComparisonPublishOptions struct {
	StorageRoot         string
	ReconciliationPath  string
	CurrentManifestPath string
	MaxCandidatePairs   uint64
	Clock               func() time.Time
	Progress            func(string)
}

type ComparisonInputReference struct {
	Role                        string `json:"role"`
	Calculation                 string `json:"calculation"`
	CalculationVersion          string `json:"calculation_version"`
	CalculationSetID            string `json:"calculation_set_id"`
	ManifestSHA256              string `json:"manifest_sha256"`
	MatchPolicy                 string `json:"match_policy"`
	ScheduleAObservationsSHA256 string `json:"schedule_a_observations_sha256"`
	ScheduleBObservationsSHA256 string `json:"schedule_b_observations_sha256"`
}

type ComparisonMethod struct {
	Version          string   `json:"version"`
	CandidateSignals []string `json:"candidate_signals"`
	DateGapBands     []string `json:"date_gap_bands"`
	Pairing          string   `json:"pairing"`
	FinancialEffect  string   `json:"financial_effect"`
}

type ComparisonCounts struct {
	ScheduleAObservations      uint64 `json:"schedule_a_observations"`
	ScheduleBObservations      uint64 `json:"schedule_b_observations"`
	ScheduleAWithCandidates    uint64 `json:"schedule_a_with_candidates"`
	ScheduleAWithoutCandidates uint64 `json:"schedule_a_without_candidates"`
	ScheduleBWithCandidates    uint64 `json:"schedule_b_with_candidates"`
	ScheduleBWithoutCandidates uint64 `json:"schedule_b_without_candidates"`
	CandidatePairs             uint64 `json:"candidate_pairs"`
	MutualOneToOne             uint64 `json:"mutual_one_to_one"`
	CompetingCandidates        uint64 `json:"competing_candidates"`
	ExactSignature             uint64 `json:"exact_signature"`
	RoleAmountDateDisagreement uint64 `json:"role_amount_date_disagreement"`
	RoleAmountMissingDate      uint64 `json:"role_amount_missing_date"`
	RoleDateAmountConflict     uint64 `json:"role_date_amount_conflict"`
	AmountDateRoleConflict     uint64 `json:"amount_date_role_conflict"`
}

type ComparisonSignals struct {
	SameRole   bool `json:"same_role"`
	SameAmount bool `json:"same_amount"`
	SameDate   bool `json:"same_date"`
}

type ComparisonCandidate struct {
	SchemaVersion                     string            `json:"schema_version"`
	CandidateID                       string            `json:"candidate_id"`
	CalculationSetID                  string            `json:"calculation_set_id"`
	Cycle                             string            `json:"cycle"`
	SenderCommitteeID                 string            `json:"sender_committee_id"`
	RecipientCommitteeID              string            `json:"recipient_committee_id"`
	ScheduleAOrdinal                  uint64            `json:"schedule_a_ordinal"`
	ScheduleBOrdinal                  uint64            `json:"schedule_b_ordinal"`
	ScheduleASubID                    string            `json:"schedule_a_sub_id"`
	ScheduleBSubID                    string            `json:"schedule_b_sub_id"`
	ScheduleARole                     string            `json:"schedule_a_role"`
	ScheduleBRole                     string            `json:"schedule_b_role"`
	ScheduleAType                     string            `json:"schedule_a_type"`
	ScheduleBType                     string            `json:"schedule_b_type"`
	ScheduleADateDays                 *int32            `json:"schedule_a_date_days"`
	ScheduleBDateDays                 *int32            `json:"schedule_b_date_days"`
	ScheduleAReportedAmountMinorUnits int64             `json:"schedule_a_reported_amount_minor_units,string"`
	ScheduleBReportedAmountMinorUnits int64             `json:"schedule_b_reported_amount_minor_units,string"`
	Signals                           ComparisonSignals `json:"signals"`
	State                             string            `json:"state"`
	DateGapDays                       *int64            `json:"date_gap_days"`
	AbsoluteDateGapDays               *uint64           `json:"absolute_date_gap_days"`
	DateGapBand                       string            `json:"date_gap_band"`
	ScheduleACandidateCount           uint64            `json:"schedule_a_candidate_count"`
	ScheduleBCandidateCount           uint64            `json:"schedule_b_candidate_count"`
	Ambiguity                         string            `json:"ambiguity"`
	FinancialEffect                   string            `json:"financial_effect"`
}

type ComparisonManifest struct {
	Schema                 string                   `json:"$schema"`
	SchemaVersion          string                   `json:"schema_version"`
	CalculationSetID       string                   `json:"calculation_set_id"`
	Calculation            string                   `json:"calculation"`
	CalculationVersion     string                   `json:"calculation_version"`
	PublisherVersion       string                   `json:"publisher_version"`
	CandidateSchemaVersion string                   `json:"candidate_schema_version"`
	Cycle                  string                   `json:"cycle"`
	SourceReleaseID        string                   `json:"source_release_id"`
	InputReconciliation    ComparisonInputReference `json:"input_reconciliation"`
	RunID                  string                   `json:"run_id"`
	State                  string                   `json:"state"`
	PublishedAt            time.Time                `json:"published_at"`
	Method                 ComparisonMethod         `json:"method"`
	Counts                 ComparisonCounts         `json:"counts"`
	Candidates             artifact.Descriptor      `json:"candidates"`
	Checks                 []ComparisonCheck        `json:"checks"`
}

type ComparisonCheck struct {
	ID       string `json:"id"`
	Passed   bool   `json:"passed"`
	Severity string `json:"severity"`
	Detail   string `json:"detail"`
}

type comparisonPair struct{ a, b int }

type comparisonRoleAmountKey struct {
	sender, recipient, role string
	amount                  int64
}

type comparisonRoleDateKey struct {
	sender, recipient, role string
	date                    int32
}

type comparisonAmountDateKey struct {
	sender, recipient string
	amount            int64
	date              int32
}

// PublishComparisonCandidates replaces transitive A/B components with direct
// pair evidence. It verifies and retains the v1 reconciliation as input but
// never treats a comparison as another payment or deduplicates either ledger.
func PublishComparisonCandidates(ctx context.Context, runID string, options ComparisonPublishOptions) (ComparisonManifest, error) {
	if options.StorageRoot == "" || options.ReconciliationPath == "" {
		return ComparisonManifest{}, fmt.Errorf("storage root and reconciliation path are required")
	}
	if !fecrelease.ValidAcquisitionRunID(runID) {
		return ComparisonManifest{}, fmt.Errorf("run ID contains unsupported characters")
	}
	if options.Clock == nil {
		options.Clock = time.Now
	}
	if options.MaxCandidatePairs == 0 {
		options.MaxCandidatePairs = defaultMaxComparisonCandidates
	}
	if options.Progress == nil {
		options.Progress = func(string) {}
	}
	result, digest, err := LoadPublished(ctx, options.StorageRoot, options.ReconciliationPath)
	if err != nil {
		return ComparisonManifest{}, fmt.Errorf("load published reconciliation: %w", err)
	}
	immutablePath := filepath.Join(options.StorageRoot, PublicationBase, "manifests", result.CalculationSetID+".json")
	verified, a, b, _, replayDigest, err := readReviewEvidence(ctx, ReviewOptions{
		ResultPath: immutablePath, EvidenceRoot: filepath.Join(options.StorageRoot, PublicationBase),
	})
	if err != nil {
		return ComparisonManifest{}, fmt.Errorf("replay reconciliation evidence: %w", err)
	}
	if replayDigest != digest || !reflect.DeepEqual(verified, result) {
		return ComparisonManifest{}, fmt.Errorf("reconciliation changed between source verification and evidence replay")
	}
	input := ComparisonInputReference{
		Role: "committee_flow_reconciliation_evidence", Calculation: "fec/committee-flow-reconciliation",
		CalculationVersion: "1.0.0", CalculationSetID: result.CalculationSetID,
		ManifestSHA256: digest, MatchPolicy: result.Policy.Matcher,
		ScheduleAObservationsSHA256: result.A.Observations.UncompressedSHA256,
		ScheduleBObservationsSHA256: result.B.Observations.UncompressedSHA256,
	}
	calculationSetID := hashJSON(struct {
		Schema, Contract, Version, CandidateSchema, Method, Publisher string
		Input                                                         ComparisonInputReference
	}{ComparisonManifestSchemaVersion, ComparisonContractID, ComparisonContractVersion, ComparisonCandidateSchemaVersion, ComparisonMethodVersion, ComparisonPublisherVersion, input})
	currentPath := options.CurrentManifestPath
	if currentPath == "" {
		currentPath = filepath.Join(options.StorageRoot, ComparisonPublicationBase, "current", result.Cycle+".json")
	}
	if _, err := inside(options.StorageRoot, currentPath); err != nil {
		return ComparisonManifest{}, err
	}

	unlock, err := publicationLock(ctx, options.StorageRoot, ComparisonPublicationBase, result.Cycle)
	if err != nil {
		return ComparisonManifest{}, err
	}
	defer unlock()
	if existing, _, err := loadComparisonManifest(ctx, options.StorageRoot, currentPath); err == nil {
		if existing.CalculationSetID == calculationSetID {
			return existing, nil
		}
	} else if !os.IsNotExist(err) {
		return ComparisonManifest{}, err
	}
	manifestPath := filepath.Join(options.StorageRoot, ComparisonPublicationBase, "manifests", calculationSetID+".json")
	if existing, _, err := loadComparisonManifest(ctx, options.StorageRoot, manifestPath); err == nil {
		if existing.InputReconciliation != input {
			return ComparisonManifest{}, fmt.Errorf("immutable comparison-candidate manifest collision")
		}
		content, err := jsonBytes(existing)
		if err != nil {
			return ComparisonManifest{}, err
		}
		if err := writePublicationJSON(ctx, currentPath, content, false); err != nil {
			return ComparisonManifest{}, err
		}
		return existing, nil
	} else if !os.IsNotExist(err) {
		return ComparisonManifest{}, err
	}

	options.Progress(fmt.Sprintf("building direct comparison candidates from %d Schedule A and %d Schedule B observations", len(a), len(b)))
	candidates, counts, err := buildComparisonCandidates(ctx, result.Cycle, calculationSetID, a, b, options.MaxCandidatePairs)
	if err != nil {
		return ComparisonManifest{}, err
	}
	staging := filepath.Join(options.StorageRoot, ComparisonPublicationBase, "staging", calculationSetID, runID)
	if err := os.MkdirAll(staging, 0o750); err != nil {
		return ComparisonManifest{}, err
	}
	defer func() { _ = os.RemoveAll(staging) }()
	writer, err := artifact.NewWriter(ctx, options.StorageRoot, staging, ComparisonPublicationBase, "candidates")
	if err != nil {
		return ComparisonManifest{}, err
	}
	defer writer.Abort()
	for index, candidate := range candidates {
		if err := writer.WriteJSON(candidate); err != nil {
			return ComparisonManifest{}, err
		}
		if index != 0 && index%100_000 == 0 {
			options.Progress(fmt.Sprintf("wrote %d direct comparison candidates for %s", index, result.Cycle))
		}
	}
	descriptor, err := writer.Finalize()
	if err != nil {
		return ComparisonManifest{}, err
	}
	manifest := ComparisonManifest{
		Schema: "manifest.schema.json", SchemaVersion: ComparisonManifestSchemaVersion,
		CalculationSetID: calculationSetID, Calculation: ComparisonContractID,
		CalculationVersion: ComparisonContractVersion, PublisherVersion: ComparisonPublisherVersion,
		CandidateSchemaVersion: ComparisonCandidateSchemaVersion, Cycle: result.Cycle,
		SourceReleaseID: result.Input.ReleaseID, InputReconciliation: input, RunID: runID,
		State: "published", PublishedAt: options.Clock().UTC(), Method: comparisonMethod(),
		Counts: counts, Candidates: descriptor,
		Checks: []ComparisonCheck{
			{ID: "input_lineage", Passed: true, Severity: "block", Detail: "the immutable reconciliation, both observation artifacts, full candidate replay, and source ancestry passed verification"},
			{ID: "source_conservation", Passed: counts.ScheduleAObservations == counts.ScheduleAWithCandidates+counts.ScheduleAWithoutCandidates && counts.ScheduleBObservations == counts.ScheduleBWithCandidates+counts.ScheduleBWithoutCandidates, Severity: "block", Detail: "every selected source observation is classified by candidate presence without changing either ledger"},
			{ID: "candidate_conservation", Passed: descriptor.RecordCount == counts.CandidatePairs && comparisonStateCount(counts) == counts.CandidatePairs, Severity: "block", Detail: "every direct candidate pair appears exactly once in the artifact and one evidence state"},
			{ID: "ambiguity_conservation", Passed: counts.MutualOneToOne+counts.CompetingCandidates == counts.CandidatePairs, Severity: "block", Detail: "every pair exposes whether either source observation has competing candidates"},
			{ID: "direct_pairing", Passed: true, Severity: "block", Detail: "pairs are emitted from direct shared signals before transitive component union"},
			{ID: "date_gap_preservation", Passed: true, Severity: "block", Detail: "known reported dates retain signed and absolute gaps plus an evidence band; missing dates remain explicit"},
			{ID: "no_financial_effect", Passed: true, Severity: "block", Detail: "comparison candidates carry no money, deduplication, allocation, or graph eligibility"},
		},
	}
	if err := validateComparisonManifest(manifest); err != nil {
		return ComparisonManifest{}, err
	}
	if err := validateComparisonManifestBacking(ctx, options.StorageRoot, manifest); err != nil {
		return ComparisonManifest{}, err
	}
	content, err := jsonBytes(manifest)
	if err != nil {
		return ComparisonManifest{}, err
	}
	if err := writePublicationJSON(ctx, manifestPath, content, true); err != nil {
		return ComparisonManifest{}, err
	}
	if _, _, err := loadComparisonManifest(ctx, options.StorageRoot, manifestPath); err != nil {
		return ComparisonManifest{}, err
	}
	if err := writePublicationJSON(ctx, currentPath, content, false); err != nil {
		return ComparisonManifest{}, err
	}
	return manifest, nil
}

func buildComparisonCandidates(ctx context.Context, cycle, calculationSetID string, a, b []Observation, maxPairs uint64) ([]ComparisonCandidate, ComparisonCounts, error) {
	if !validCycle(cycle) || !validDigest(calculationSetID) || maxPairs == 0 {
		return nil, ComparisonCounts{}, fmt.Errorf("cycle, calculation ID, and positive candidate capacity are required")
	}
	roleAmount := make(map[comparisonRoleAmountKey][]int)
	roleDate := make(map[comparisonRoleDateKey][]int)
	amountDate := make(map[comparisonAmountDateKey][]int)
	for index, observation := range b {
		if err := validateComparisonObservation(observation); err != nil {
			return nil, ComparisonCounts{}, fmt.Errorf("Schedule B observation %d: %w", index+1, err)
		}
		roleAmount[comparisonRoleAmountKey{observation.Sender, observation.Recipient, observation.Role, observation.Amount}] = append(roleAmount[comparisonRoleAmountKey{observation.Sender, observation.Recipient, observation.Role, observation.Amount}], index)
		if observation.Date != nil {
			roleDate[comparisonRoleDateKey{observation.Sender, observation.Recipient, observation.Role, *observation.Date}] = append(roleDate[comparisonRoleDateKey{observation.Sender, observation.Recipient, observation.Role, *observation.Date}], index)
			amountDate[comparisonAmountDateKey{observation.Sender, observation.Recipient, observation.Amount, *observation.Date}] = append(amountDate[comparisonAmountDateKey{observation.Sender, observation.Recipient, observation.Amount, *observation.Date}], index)
		}
	}
	pairs := make(map[comparisonPair]struct{})
	add := func(ai int, matches []int) error {
		for _, bi := range matches {
			pair := comparisonPair{ai, bi}
			if _, exists := pairs[pair]; exists {
				continue
			}
			if uint64(len(pairs)) >= maxPairs {
				return fmt.Errorf("comparison candidate capacity %d exceeded; no partial publication was written", maxPairs)
			}
			pairs[pair] = struct{}{}
		}
		return nil
	}
	for index, observation := range a {
		if index%4096 == 0 && ctx.Err() != nil {
			return nil, ComparisonCounts{}, ctx.Err()
		}
		if err := validateComparisonObservation(observation); err != nil {
			return nil, ComparisonCounts{}, fmt.Errorf("Schedule A observation %d: %w", index+1, err)
		}
		if err := add(index, roleAmount[comparisonRoleAmountKey{observation.Sender, observation.Recipient, observation.Role, observation.Amount}]); err != nil {
			return nil, ComparisonCounts{}, err
		}
		if observation.Date != nil {
			if err := add(index, roleDate[comparisonRoleDateKey{observation.Sender, observation.Recipient, observation.Role, *observation.Date}]); err != nil {
				return nil, ComparisonCounts{}, err
			}
			if err := add(index, amountDate[comparisonAmountDateKey{observation.Sender, observation.Recipient, observation.Amount, *observation.Date}]); err != nil {
				return nil, ComparisonCounts{}, err
			}
		}
	}
	ordered := make([]comparisonPair, 0, len(pairs))
	aDegree := make([]uint64, len(a))
	bDegree := make([]uint64, len(b))
	for pair := range pairs {
		ordered = append(ordered, pair)
		aDegree[pair.a]++
		bDegree[pair.b]++
	}
	sort.Slice(ordered, func(i, j int) bool {
		left, right := ordered[i], ordered[j]
		if a[left.a].Ordinal != a[right.a].Ordinal {
			return a[left.a].Ordinal < a[right.a].Ordinal
		}
		return b[left.b].Ordinal < b[right.b].Ordinal
	})
	counts := ComparisonCounts{ScheduleAObservations: uint64(len(a)), ScheduleBObservations: uint64(len(b)), CandidatePairs: uint64(len(ordered))}
	for _, degree := range aDegree {
		if degree == 0 {
			counts.ScheduleAWithoutCandidates++
		} else {
			counts.ScheduleAWithCandidates++
		}
	}
	for _, degree := range bDegree {
		if degree == 0 {
			counts.ScheduleBWithoutCandidates++
		} else {
			counts.ScheduleBWithCandidates++
		}
	}
	candidates := make([]ComparisonCandidate, 0, len(ordered))
	for _, pair := range ordered {
		candidate, err := newComparisonCandidate(cycle, calculationSetID, a[pair.a], b[pair.b], aDegree[pair.a], bDegree[pair.b])
		if err != nil {
			return nil, ComparisonCounts{}, err
		}
		addComparisonCount(&counts, candidate)
		candidates = append(candidates, candidate)
	}
	return candidates, counts, nil
}

func newComparisonCandidate(cycle, calculationSetID string, a, b Observation, aDegree, bDegree uint64) (ComparisonCandidate, error) {
	if a.Sender != b.Sender || a.Recipient != b.Recipient || aDegree == 0 || bDegree == 0 {
		return ComparisonCandidate{}, fmt.Errorf("comparison candidate endpoints or degree differ")
	}
	signals := ComparisonSignals{SameRole: a.Role == b.Role, SameAmount: a.Amount == b.Amount, SameDate: a.Date != nil && b.Date != nil && *a.Date == *b.Date}
	if !(signals.SameRole && signals.SameAmount || signals.SameRole && signals.SameDate || signals.SameAmount && signals.SameDate) {
		return ComparisonCandidate{}, fmt.Errorf("comparison candidate has no shared qualifying signal")
	}
	value := ComparisonCandidate{
		SchemaVersion: ComparisonCandidateSchemaVersion, CalculationSetID: calculationSetID, Cycle: cycle,
		SenderCommitteeID: a.Sender, RecipientCommitteeID: a.Recipient,
		ScheduleAOrdinal: a.Ordinal, ScheduleBOrdinal: b.Ordinal, ScheduleASubID: a.SubID, ScheduleBSubID: b.SubID,
		ScheduleARole: a.Role, ScheduleBRole: b.Role, ScheduleAType: a.Type, ScheduleBType: b.Type,
		ScheduleADateDays: copyInt32Pointer(a.Date), ScheduleBDateDays: copyInt32Pointer(b.Date),
		ScheduleAReportedAmountMinorUnits: a.Amount, ScheduleBReportedAmountMinorUnits: b.Amount,
		Signals: signals, ScheduleACandidateCount: aDegree, ScheduleBCandidateCount: bDegree,
		FinancialEffect: ComparisonFinancialEffectNone,
	}
	if a.Date == nil || b.Date == nil {
		value.DateGapBand = "unknown"
	} else {
		gap := int64(*a.Date) - int64(*b.Date)
		absolute := absReview(gap)
		value.DateGapDays, value.AbsoluteDateGapDays = &gap, &absolute
		value.DateGapBand = comparisonDateGapBand(absolute)
	}
	switch {
	case signals.SameRole && signals.SameAmount && signals.SameDate:
		value.State = ComparisonExactSignature
	case signals.SameRole && signals.SameAmount && a.Date != nil && b.Date != nil:
		value.State = ComparisonDateDisagreement
	case signals.SameRole && signals.SameAmount:
		value.State = ComparisonMissingDate
	case signals.SameRole && signals.SameDate:
		value.State = ComparisonAmountConflict
	case signals.SameAmount && signals.SameDate:
		value.State = ComparisonRoleConflict
	default:
		return ComparisonCandidate{}, fmt.Errorf("comparison candidate state is unsupported")
	}
	if aDegree == 1 && bDegree == 1 {
		value.Ambiguity = ComparisonMutualOneToOne
	} else {
		value.Ambiguity = ComparisonCompetingCandidates
	}
	value.CandidateID = hashJSON(struct {
		Version, Calculation string
		A, B                 uint64
	}{ComparisonCandidateSchemaVersion, calculationSetID, a.Ordinal, b.Ordinal})
	if err := validateComparisonCandidate(value); err != nil {
		return ComparisonCandidate{}, err
	}
	return value, nil
}

func validateComparisonCandidate(value ComparisonCandidate) error {
	if value.SchemaVersion != ComparisonCandidateSchemaVersion || !validDigest(value.CandidateID) || !validDigest(value.CalculationSetID) ||
		!validCycle(value.Cycle) || !validCommittee(value.SenderCommitteeID) || !validCommittee(value.RecipientCommitteeID) ||
		value.ScheduleAOrdinal == 0 || value.ScheduleBOrdinal == 0 || value.ScheduleASubID == "" || value.ScheduleBSubID == "" ||
		!validRole(value.ScheduleARole) || !validRole(value.ScheduleBRole) || value.ScheduleACandidateCount == 0 || value.ScheduleBCandidateCount == 0 ||
		value.FinancialEffect != ComparisonFinancialEffectNone {
		return fmt.Errorf("comparison candidate identity is incomplete")
	}
	if value.Signals.SameRole != (value.ScheduleARole == value.ScheduleBRole) ||
		value.Signals.SameAmount != (value.ScheduleAReportedAmountMinorUnits == value.ScheduleBReportedAmountMinorUnits) ||
		value.Signals.SameDate != (value.ScheduleADateDays != nil && value.ScheduleBDateDays != nil && *value.ScheduleADateDays == *value.ScheduleBDateDays) {
		return fmt.Errorf("comparison candidate signals differ from source values")
	}
	expectedGapBand := "unknown"
	if value.ScheduleADateDays == nil || value.ScheduleBDateDays == nil {
		if value.DateGapDays != nil || value.AbsoluteDateGapDays != nil {
			return fmt.Errorf("comparison candidate has a date gap without two dates")
		}
	} else {
		gap := int64(*value.ScheduleADateDays) - int64(*value.ScheduleBDateDays)
		absolute := absReview(gap)
		expectedGapBand = comparisonDateGapBand(absolute)
		if value.DateGapDays == nil || value.AbsoluteDateGapDays == nil || *value.DateGapDays != gap || *value.AbsoluteDateGapDays != absolute {
			return fmt.Errorf("comparison candidate date gap differs from source values")
		}
	}
	if value.DateGapBand != expectedGapBand {
		return fmt.Errorf("comparison candidate date-gap band is inconsistent")
	}
	expectedState := ComparisonMissingDate
	switch {
	case value.Signals.SameRole && value.Signals.SameAmount && value.Signals.SameDate:
		expectedState = ComparisonExactSignature
	case value.Signals.SameRole && value.Signals.SameAmount && value.ScheduleADateDays != nil && value.ScheduleBDateDays != nil:
		expectedState = ComparisonDateDisagreement
	case value.Signals.SameRole && value.Signals.SameAmount:
		expectedState = ComparisonMissingDate
	case value.Signals.SameRole && value.Signals.SameDate:
		expectedState = ComparisonAmountConflict
	case value.Signals.SameAmount && value.Signals.SameDate:
		expectedState = ComparisonRoleConflict
	default:
		return fmt.Errorf("comparison candidate has no qualifying signal")
	}
	if value.State != expectedState {
		return fmt.Errorf("comparison candidate state differs from signals")
	}
	expectedAmbiguity := ComparisonCompetingCandidates
	if value.ScheduleACandidateCount == 1 && value.ScheduleBCandidateCount == 1 {
		expectedAmbiguity = ComparisonMutualOneToOne
	}
	if value.Ambiguity != expectedAmbiguity {
		return fmt.Errorf("comparison candidate ambiguity differs from degrees")
	}
	expectedID := hashJSON(struct {
		Version, Calculation string
		A, B                 uint64
	}{ComparisonCandidateSchemaVersion, value.CalculationSetID, value.ScheduleAOrdinal, value.ScheduleBOrdinal})
	if value.CandidateID != expectedID {
		return fmt.Errorf("comparison candidate ID differs from canonical identity")
	}
	return nil
}

func validateComparisonObservation(value Observation) error {
	if value.Ordinal == 0 || value.SubID == "" || !validCommittee(value.Sender) || !validCommittee(value.Recipient) || !validRole(value.Role) {
		return fmt.Errorf("observation identity is incomplete")
	}
	return nil
}

func addComparisonCount(counts *ComparisonCounts, value ComparisonCandidate) {
	if value.Ambiguity == ComparisonMutualOneToOne {
		counts.MutualOneToOne++
	} else {
		counts.CompetingCandidates++
	}
	switch value.State {
	case ComparisonExactSignature:
		counts.ExactSignature++
	case ComparisonDateDisagreement:
		counts.RoleAmountDateDisagreement++
	case ComparisonMissingDate:
		counts.RoleAmountMissingDate++
	case ComparisonAmountConflict:
		counts.RoleDateAmountConflict++
	case ComparisonRoleConflict:
		counts.AmountDateRoleConflict++
	}
}

func comparisonStateCount(counts ComparisonCounts) uint64 {
	return counts.ExactSignature + counts.RoleAmountDateDisagreement + counts.RoleAmountMissingDate + counts.RoleDateAmountConflict + counts.AmountDateRoleConflict
}

func comparisonDateGapBand(absolute uint64) string {
	switch {
	case absolute == 0:
		return "same_day"
	case absolute <= 3:
		return "1_to_3_days"
	case absolute <= 10:
		return "4_to_10_days"
	case absolute <= 30:
		return "11_to_30_days"
	case absolute <= 90:
		return "31_to_90_days"
	default:
		return "over_90_days"
	}
}

func comparisonMethod() ComparisonMethod {
	return ComparisonMethod{
		Version: ComparisonMethodVersion,
		CandidateSignals: []string{
			"same directed committee endpoints, role, and reported amount",
			"same directed committee endpoints, role, and reported date",
			"same directed committee endpoints, reported amount, and reported date",
		},
		DateGapBands:    []string{"same_day", "1_to_3_days", "4_to_10_days", "11_to_30_days", "31_to_90_days", "over_90_days", "unknown"},
		Pairing:         "emit each direct cross-ledger candidate once before transitive component union; preserve per-observation candidate degree",
		FinancialEffect: ComparisonFinancialEffectNone,
	}
}

func validateComparisonManifest(value ComparisonManifest) error {
	if value.Schema != "manifest.schema.json" || value.SchemaVersion != ComparisonManifestSchemaVersion || value.Calculation != ComparisonContractID ||
		value.CalculationVersion != ComparisonContractVersion || value.PublisherVersion != ComparisonPublisherVersion ||
		value.CandidateSchemaVersion != ComparisonCandidateSchemaVersion || value.State != "published" || !reflect.DeepEqual(value.Method, comparisonMethod()) {
		return fmt.Errorf("unsupported comparison-candidate manifest")
	}
	if !validDigest(value.CalculationSetID) || !validCycle(value.Cycle) || !strings.HasPrefix(value.SourceReleaseID, "fec-") ||
		!validDigest(strings.TrimPrefix(value.SourceReleaseID, "fec-")) || !fecrelease.ValidAcquisitionRunID(value.RunID) || value.PublishedAt.IsZero() {
		return fmt.Errorf("comparison-candidate manifest identity is incomplete")
	}
	input := value.InputReconciliation
	if input.Role != "committee_flow_reconciliation_evidence" || input.Calculation != "fec/committee-flow-reconciliation" || input.CalculationVersion != "1.0.0" ||
		input.MatchPolicy != MatchPolicy || !validDigest(input.CalculationSetID) || !validDigest(input.ManifestSHA256) ||
		!validDigest(input.ScheduleAObservationsSHA256) || !validDigest(input.ScheduleBObservationsSHA256) {
		return fmt.Errorf("comparison-candidate input is invalid")
	}
	expectedID := hashJSON(struct {
		Schema, Contract, Version, CandidateSchema, Method, Publisher string
		Input                                                         ComparisonInputReference
	}{ComparisonManifestSchemaVersion, ComparisonContractID, ComparisonContractVersion, ComparisonCandidateSchemaVersion, ComparisonMethodVersion, ComparisonPublisherVersion, input})
	if value.CalculationSetID != expectedID || value.Counts.ScheduleAObservations != value.Counts.ScheduleAWithCandidates+value.Counts.ScheduleAWithoutCandidates ||
		value.Counts.ScheduleBObservations != value.Counts.ScheduleBWithCandidates+value.Counts.ScheduleBWithoutCandidates ||
		value.Counts.CandidatePairs != comparisonStateCount(value.Counts) || value.Counts.CandidatePairs != value.Counts.MutualOneToOne+value.Counts.CompetingCandidates ||
		value.Candidates.RecordCount != value.Counts.CandidatePairs {
		return fmt.Errorf("comparison-candidate identity or counts are inconsistent")
	}
	if value.Candidates.Compression != "zstd" || value.Candidates.CompressedBytes == 0 || !validDigest(value.Candidates.CompressedSHA256) ||
		!validDigest(value.Candidates.UncompressedSHA256) || !strings.HasPrefix(value.Candidates.StorageKey, ComparisonPublicationBase+"/candidates/sha256/") || len(value.Checks) < 7 {
		return fmt.Errorf("comparison-candidate artifact or checks are incomplete")
	}
	for _, check := range value.Checks {
		if check.Severity == "block" && !check.Passed {
			return fmt.Errorf("comparison-candidate blocking check %s failed", check.ID)
		}
	}
	return nil
}

func loadComparisonManifest(ctx context.Context, storageRoot, path string) (ComparisonManifest, []byte, error) {
	var value ComparisonManifest
	resolved, err := inside(storageRoot, path)
	if err != nil {
		return value, nil, err
	}
	content, err := os.ReadFile(resolved)
	if err != nil {
		return value, nil, err
	}
	if err := strictJSON(content, &value); err != nil {
		return value, nil, err
	}
	if err := validateComparisonManifest(value); err != nil {
		return value, nil, err
	}
	if err := verifyPinnedBytes(storageRoot, ComparisonPublicationBase, value.CalculationSetID, content); err != nil {
		return value, nil, err
	}
	if err := validateComparisonManifestBacking(ctx, storageRoot, value); err != nil {
		return value, nil, err
	}
	return value, content, nil
}

func validateComparisonManifestBacking(ctx context.Context, storageRoot string, value ComparisonManifest) error {
	artifactPath, err := artifact.Resolve(storageRoot, value.Candidates.StorageKey)
	if err != nil {
		return err
	}
	if err := artifact.Verify(ctx, artifactPath, value.Candidates); err != nil {
		return err
	}
	reader, err := artifact.Open[ComparisonCandidate](ctx, storageRoot, value.Candidates)
	if err != nil {
		return err
	}
	defer reader.Abort()
	counts := ComparisonCounts{ScheduleAObservations: value.Counts.ScheduleAObservations, ScheduleBObservations: value.Counts.ScheduleBObservations}
	aSeen := make(map[uint64]struct{}, value.Counts.ScheduleAWithCandidates)
	bSeen := make(map[uint64]struct{}, value.Counts.ScheduleBWithCandidates)
	pairs := make(map[[2]uint64]struct{}, value.Candidates.RecordCount)
	var previousA, previousB uint64
	for {
		candidate, ok, err := reader.Next()
		if err != nil {
			return err
		}
		if !ok {
			break
		}
		if err := validateComparisonCandidate(candidate); err != nil {
			return err
		}
		if candidate.CalculationSetID != value.CalculationSetID || candidate.Cycle != value.Cycle {
			return fmt.Errorf("comparison candidate does not match its manifest")
		}
		if counts.CandidatePairs != 0 && (candidate.ScheduleAOrdinal < previousA || candidate.ScheduleAOrdinal == previousA && candidate.ScheduleBOrdinal <= previousB) {
			return fmt.Errorf("comparison candidates are duplicated or unordered")
		}
		previousA, previousB = candidate.ScheduleAOrdinal, candidate.ScheduleBOrdinal
		pair := [2]uint64{candidate.ScheduleAOrdinal, candidate.ScheduleBOrdinal}
		if _, duplicate := pairs[pair]; duplicate {
			return fmt.Errorf("duplicate comparison candidate pair")
		}
		pairs[pair] = struct{}{}
		aSeen[candidate.ScheduleAOrdinal] = struct{}{}
		bSeen[candidate.ScheduleBOrdinal] = struct{}{}
		counts.CandidatePairs++
		addComparisonCount(&counts, candidate)
	}
	if err := reader.Close(); err != nil {
		return err
	}
	counts.ScheduleAWithCandidates = uint64(len(aSeen))
	counts.ScheduleBWithCandidates = uint64(len(bSeen))
	if counts.ScheduleAWithCandidates > counts.ScheduleAObservations || counts.ScheduleBWithCandidates > counts.ScheduleBObservations {
		return fmt.Errorf("comparison candidates reference more observations than their source")
	}
	counts.ScheduleAWithoutCandidates = counts.ScheduleAObservations - counts.ScheduleAWithCandidates
	counts.ScheduleBWithoutCandidates = counts.ScheduleBObservations - counts.ScheduleBWithCandidates
	if uint64(len(pairs)) != value.Candidates.RecordCount || counts != value.Counts {
		return fmt.Errorf("comparison-candidate readback differs from manifest")
	}
	return nil
}

func copyInt32Pointer(value *int32) *int32 {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}
