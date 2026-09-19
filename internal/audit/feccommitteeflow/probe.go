package feccommitteeflow

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"runtime"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/parquet-go/parquet-go"
	committeeflows "github.com/vedantadhobley/legal-tender/internal/calculation/fec/committeeflows"
	fecoccurrence "github.com/vedantadhobley/legal-tender/internal/source/fec/occurrence"
	storageartifact "github.com/vedantadhobley/legal-tender/internal/storage/artifact"
)

const maxIdentitySamples = 32

var decisionOrder = []string{
	"invalid_normalization",
	"unresolved_recipient_committee_id",
	"excluded_no_source_committee_id",
	"unresolved_one_sided_source_committee_id",
	"unresolved_conflicting_source_committee_ids",
	"excluded_memo_subtotal",
	"unresolved_amount",
	"excluded_outbound_receipt_role",
	"excluded_semantic_memo_receipt_role",
	"excluded_earmarked_receipt_role",
	"excluded_noncommittee_receipt_role",
	"unresolved_receipt_role",
	"included_receiver_reported_committee_flow",
}

type probeRow struct {
	SourceRowOrdinal       int64   `parquet:"lt_source_row_ordinal"`
	Normalization          string  `parquet:"lt_normalization_state"`
	RecipientID            *string `parquet:"cmte_id,optional"`
	ContributorID          *string `parquet:"contbr_id,optional"`
	CleanContributorID     *string `parquet:"clean_contbr_id,optional"`
	EntityTypeCode         *string `parquet:"entity_tp,optional"`
	Individual             *bool   `parquet:"is_individual,optional"`
	MemoCode               *string `parquet:"memo_cd,optional"`
	MemoedSubtotal         bool    `parquet:"lt_memoed_subtotal"`
	ReceiptTypeCode        *string `parquet:"receipt_tp,optional"`
	ReceiptTypeDescription *string `parquet:"receipt_tp_desc,optional"`
	ActionCode             *string `parquet:"action_cd,optional"`
	ConduitCommitteeID     *string `parquet:"conduit_cmte_id,optional"`
	BackReferenceID        *string `parquet:"back_ref_tran_id,optional"`
	AmountMinorUnits       *int64  `parquet:"lt_receipt_amount_minor_units,optional"`
	AmountState            string  `parquet:"lt_receipt_amount_state"`
}

type moneyStat struct {
	rows, positive, negative, zero, unknown uint64
	amount                                  int64
}

type edgeKey struct {
	source, recipient, role string
}

type shardResult struct {
	rows                       uint64
	decisions                  map[string]moneyStat
	identities                 map[string]moneyStat
	entityTypes                map[string]moneyStat
	individualClasses          map[string]moneyStat
	receiptTypes               map[string]moneyStat
	receiptRoles               map[string]moneyStat
	receiptEntityShapes        map[string]moneyStat
	exactIdentityReceiptShapes map[string]moneyStat
	actionCodes                map[string]moneyStat
	edges                      map[edgeKey]moneyStat
	sources                    map[string]struct{}
	recipients                 map[string]struct{}
	selfEdges                  map[edgeKey]struct{}
	selfRows                   uint64
	conduitRows                uint64
	backRefRows                uint64
	samples                    []IdentitySample
	err                        error
}

// Probe scans every immutable Schedule A Parquet row and measures a narrow,
// conservative committee-ID cohort. The result is diagnostic evidence, not a
// published flow calculation.
func Probe(ctx context.Context, options Options) (Result, error) {
	if options.StorageRoot == "" || options.ManifestPath == "" || options.Cycle == "" {
		return Result{}, fmt.Errorf("storage root, manifest path, and cycle are required")
	}
	if options.Clock == nil {
		options.Clock = time.Now
	}
	if options.Progress == nil {
		options.Progress = func(string) {}
	}
	if options.Workers <= 0 {
		options.Workers = runtime.GOMAXPROCS(0)
		if options.Workers > 16 {
			options.Workers = 16
		}
	}
	if options.TopEdges <= 0 {
		options.TopEdges = 50
	}
	started := options.Clock().UTC()
	manifest, manifestDigest, err := fecoccurrence.LoadPublishedScheduleAColumnarManifest(ctx, options.StorageRoot, options.ManifestPath)
	if err != nil {
		return Result{}, fmt.Errorf("load Schedule A columnar facts: %w", err)
	}
	if manifest.Cycle != options.Cycle {
		return Result{}, fmt.Errorf("Schedule A facts belong to cycle %s, expected %s", manifest.Cycle, options.Cycle)
	}
	if options.Workers > len(manifest.Shards) {
		options.Workers = len(manifest.Shards)
	}
	options.Progress(fmt.Sprintf("scanning %d Schedule A Parquet shards with %d workers", len(manifest.Shards), options.Workers))

	combined, err := scanShards(ctx, options.StorageRoot, manifest.Shards, options.Workers, options.Progress)
	completed := options.Clock().UTC()
	result := buildResult(manifest, manifestDigest, options, started, completed, combined)
	if err != nil {
		return result, err
	}
	for _, check := range result.Checks {
		if !check.Passed {
			return result, fmt.Errorf("probe check %s failed: %s", check.Name, check.Details)
		}
	}
	return result, nil
}

func scanShards(ctx context.Context, storageRoot string, shards []fecoccurrence.ScheduleAColumnarShard, workers int, progress func(string)) (shardResult, error) {
	workerContext, cancel := context.WithCancel(ctx)
	defer cancel()
	jobs := make(chan fecoccurrence.ScheduleAColumnarShard)
	results := make(chan shardResult, workers)
	var group sync.WaitGroup
	group.Add(workers)
	for range workers {
		go func() {
			defer group.Done()
			for shard := range jobs {
				result := scanShard(workerContext, storageRoot, shard)
				select {
				case results <- result:
				case <-workerContext.Done():
					return
				}
				if result.err != nil {
					cancel()
					return
				}
			}
		}()
	}
	go func() {
		defer close(jobs)
		for _, shard := range shards {
			select {
			case jobs <- shard:
			case <-workerContext.Done():
				return
			}
		}
	}()
	go func() {
		group.Wait()
		close(results)
	}()

	combined := newShardResult()
	completed := 0
	var firstError error
	for result := range results {
		if result.err != nil {
			if firstError == nil {
				firstError = result.err
			}
			continue
		}
		if err := mergeShardResult(&combined, result); err != nil && firstError == nil {
			firstError = err
			cancel()
		}
		completed++
		if completed%16 == 0 || completed == len(shards) {
			progress(fmt.Sprintf("scanned %d of %d Schedule A Parquet shards", completed, len(shards)))
		}
	}
	if firstError != nil {
		return combined, firstError
	}
	if completed != len(shards) {
		return combined, fmt.Errorf("scanned %d of %d shards", completed, len(shards))
	}
	return combined, nil
}

func scanShard(ctx context.Context, storageRoot string, shard fecoccurrence.ScheduleAColumnarShard) shardResult {
	result := newShardResult()
	path, err := storageartifact.Resolve(storageRoot, shard.StorageKey)
	if err != nil {
		result.err = err
		return result
	}
	file, err := os.Open(path)
	if err != nil {
		result.err = err
		return result
	}
	reader := parquet.NewGenericReader[probeRow](file)
	buffer := make([]probeRow, 8192)
	for {
		count, readErr := reader.Read(buffer)
		for index := range count {
			if err := addRow(&result, buffer[index]); err != nil {
				result.err = fmt.Errorf("shard %d row %d: %w", shard.Index, buffer[index].SourceRowOrdinal, err)
				_ = reader.Close()
				_ = file.Close()
				return result
			}
			if result.rows&0x3fff == 0 {
				if err := ctx.Err(); err != nil {
					result.err = err
					_ = reader.Close()
					_ = file.Close()
					return result
				}
			}
		}
		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				break
			}
			result.err = readErr
			_ = reader.Close()
			_ = file.Close()
			return result
		}
	}
	if err := reader.Close(); err != nil {
		result.err = err
		_ = file.Close()
		return result
	}
	if err := file.Close(); err != nil {
		result.err = err
		return result
	}
	if result.rows != shard.Facts {
		result.err = fmt.Errorf("shard %d yielded %d rows, expected %d", shard.Index, result.rows, shard.Facts)
	}
	return result
}

func newShardResult() shardResult {
	return shardResult{
		decisions: make(map[string]moneyStat), identities: make(map[string]moneyStat),
		entityTypes: make(map[string]moneyStat), individualClasses: make(map[string]moneyStat), receiptTypes: make(map[string]moneyStat), receiptRoles: make(map[string]moneyStat), actionCodes: make(map[string]moneyStat),
		receiptEntityShapes:        make(map[string]moneyStat),
		exactIdentityReceiptShapes: make(map[string]moneyStat),
		edges:                      make(map[edgeKey]moneyStat), sources: make(map[string]struct{}), recipients: make(map[string]struct{}), selfEdges: make(map[edgeKey]struct{}),
	}
}

func addRow(result *shardResult, row probeRow) error {
	result.rows++
	identity, _ := committeeflows.ClassifySourceIdentity(row.ContributorID, row.CleanContributorID)
	if err := addNamed(result.identities, identity, row.AmountMinorUnits); err != nil {
		return err
	}
	if identity != "exact_matching_committee_id" && identity != "no_valid_committee_id" && len(result.samples) < maxIdentitySamples {
		result.samples = append(result.samples, IdentitySample{
			State: identity, SourceRowOrdinal: row.SourceRowOrdinal, RecipientCommitteeID: row.RecipientID,
			ContributorID: row.ContributorID, CleanContributorID: row.CleanContributorID,
			EntityTypeCode: row.EntityTypeCode, Individual: row.Individual, MemoCode: row.MemoCode,
			ReceiptTypeCode: row.ReceiptTypeCode, AmountMinorUnits: row.AmountMinorUnits,
		})
	}
	if identity == committeeflows.IdentityExactMatching && committeeflows.ValidCommitteeID(row.RecipientID) && !row.MemoedSubtotal && row.AmountState == "reported_value" && row.AmountMinorUnits != nil {
		shape := nullableCategory(row.ReceiptTypeCode) + " | " + nullableCategory(row.ReceiptTypeDescription) + " | individual=" + nullableBoolean(row.Individual) + " | entity=" + nullableCategory(row.EntityTypeCode)
		if err := addNamed(result.exactIdentityReceiptShapes, shape, row.AmountMinorUnits); err != nil {
			return err
		}
	}
	evaluation := committeeflows.Evaluate(committeeflows.EvaluationInput{
		NormalizationState: row.Normalization, RecipientCommitteeID: row.RecipientID,
		ContributorID: row.ContributorID, CleanContributorID: row.CleanContributorID,
		MemoedSubtotal: row.MemoedSubtotal, AmountObservationState: row.AmountState,
		AmountMinorUnits: row.AmountMinorUnits, ReceiptTypeCode: row.ReceiptTypeCode,
	})
	decision := evaluation.Decision
	if err := addNamed(result.decisions, decision, row.AmountMinorUnits); err != nil {
		return err
	}
	if decision != "included_receiver_reported_committee_flow" {
		return nil
	}
	role := evaluation.ReceiptRole
	key := edgeKey{source: evaluation.SourceCommitteeID, recipient: *row.RecipientID, role: role}
	if err := addNamedEdge(result.edges, key, row.AmountMinorUnits); err != nil {
		return err
	}
	result.sources[evaluation.SourceCommitteeID] = struct{}{}
	result.recipients[*row.RecipientID] = struct{}{}
	if evaluation.SourceCommitteeID == *row.RecipientID {
		result.selfRows++
		result.selfEdges[key] = struct{}{}
	}
	if row.ConduitCommitteeID != nil && *row.ConduitCommitteeID != "" {
		result.conduitRows++
	}
	if row.BackReferenceID != nil && *row.BackReferenceID != "" {
		result.backRefRows++
	}
	if err := addNamed(result.entityTypes, nullableCategory(row.EntityTypeCode), row.AmountMinorUnits); err != nil {
		return err
	}
	if err := addNamed(result.individualClasses, nullableBoolean(row.Individual), row.AmountMinorUnits); err != nil {
		return err
	}
	if err := addNamed(result.receiptTypes, nullableCategory(row.ReceiptTypeCode), row.AmountMinorUnits); err != nil {
		return err
	}
	if err := addNamed(result.receiptRoles, role, row.AmountMinorUnits); err != nil {
		return err
	}
	shape := nullableCategory(row.EntityTypeCode) + " | " + nullableCategory(row.ReceiptTypeCode) + " | " + nullableCategory(row.ReceiptTypeDescription)
	if err := addNamed(result.receiptEntityShapes, shape, row.AmountMinorUnits); err != nil {
		return err
	}
	return addNamed(result.actionCodes, nullableCategory(row.ActionCode), row.AmountMinorUnits)
}

func addNamed(values map[string]moneyStat, name string, amount *int64) error {
	value := values[name]
	if err := value.add(amount); err != nil {
		return err
	}
	values[name] = value
	return nil
}

func addNamedEdge(values map[edgeKey]moneyStat, key edgeKey, amount *int64) error {
	value := values[key]
	if err := value.add(amount); err != nil {
		return err
	}
	values[key] = value
	return nil
}

func (value *moneyStat) add(amount *int64) error {
	value.rows++
	if amount == nil {
		value.unknown++
		return nil
	}
	switch {
	case *amount > 0:
		value.positive++
	case *amount < 0:
		value.negative++
	default:
		value.zero++
	}
	if (*amount > 0 && value.amount > math.MaxInt64-*amount) || (*amount < 0 && value.amount < math.MinInt64-*amount) {
		return fmt.Errorf("signed amount sum exceeds int64")
	}
	value.amount += *amount
	return nil
}

func mergeShardResult(target *shardResult, source shardResult) error {
	target.rows += source.rows
	for name, value := range source.decisions {
		if err := mergeMoneyStat(target.decisions, name, value); err != nil {
			return err
		}
	}
	for name, value := range source.identities {
		if err := mergeMoneyStat(target.identities, name, value); err != nil {
			return err
		}
	}
	for name, value := range source.entityTypes {
		if err := mergeMoneyStat(target.entityTypes, name, value); err != nil {
			return err
		}
	}
	for name, value := range source.individualClasses {
		if err := mergeMoneyStat(target.individualClasses, name, value); err != nil {
			return err
		}
	}
	for name, value := range source.receiptTypes {
		if err := mergeMoneyStat(target.receiptTypes, name, value); err != nil {
			return err
		}
	}
	for name, value := range source.receiptRoles {
		if err := mergeMoneyStat(target.receiptRoles, name, value); err != nil {
			return err
		}
	}
	for name, value := range source.receiptEntityShapes {
		if err := mergeMoneyStat(target.receiptEntityShapes, name, value); err != nil {
			return err
		}
	}
	for name, value := range source.exactIdentityReceiptShapes {
		if err := mergeMoneyStat(target.exactIdentityReceiptShapes, name, value); err != nil {
			return err
		}
	}
	for name, value := range source.actionCodes {
		if err := mergeMoneyStat(target.actionCodes, name, value); err != nil {
			return err
		}
	}
	for key, value := range source.edges {
		prior := target.edges[key]
		merged, err := combineMoneyStat(prior, value)
		if err != nil {
			return err
		}
		target.edges[key] = merged
	}
	for value := range source.sources {
		target.sources[value] = struct{}{}
	}
	for value := range source.recipients {
		target.recipients[value] = struct{}{}
	}
	for value := range source.selfEdges {
		target.selfEdges[value] = struct{}{}
	}
	target.selfRows += source.selfRows
	target.conduitRows += source.conduitRows
	target.backRefRows += source.backRefRows
	target.samples = append(target.samples, source.samples...)
	return nil
}

func mergeMoneyStat(target map[string]moneyStat, name string, value moneyStat) error {
	merged, err := combineMoneyStat(target[name], value)
	if err != nil {
		return err
	}
	target[name] = merged
	return nil
}

func combineMoneyStat(left, right moneyStat) (moneyStat, error) {
	if (right.amount > 0 && left.amount > math.MaxInt64-right.amount) || (right.amount < 0 && left.amount < math.MinInt64-right.amount) {
		return moneyStat{}, fmt.Errorf("signed amount sum exceeds int64")
	}
	left.rows += right.rows
	left.positive += right.positive
	left.negative += right.negative
	left.zero += right.zero
	left.unknown += right.unknown
	left.amount += right.amount
	return left, nil
}

func buildResult(manifest fecoccurrence.ScheduleAColumnarManifest, digest string, options Options, started, completed time.Time, scan shardResult) Result {
	included := scan.decisions["included_receiver_reported_committee_flow"]
	observed := uint64(0)
	unknown := uint64(0)
	for _, value := range scan.decisions {
		observed += value.rows - value.unknown
		unknown += value.unknown
	}
	result := Result{
		SchemaVersion: SchemaVersion, ProbeVersion: ProbeVersion, Status: "diagnostic_not_a_flow_calculation",
		Cycle: manifest.Cycle, StartedAt: started, CompletedAt: completed, ElapsedSeconds: completed.Sub(started).Seconds(),
		Input:         InputReference{FactSetID: manifest.FactSetID, SourceReleaseID: manifest.SourceReleaseID, ManifestSHA256: digest, PhysicalSchema: manifest.PhysicalSchemaVersion, Rows: manifest.Counts.Facts, Shards: uint64(len(manifest.Shards))},
		Configuration: Configuration{Workers: options.Workers, SourceIdentityFields: []string{"contbr_id", "clean_contbr_id"}, DiagnosticDecisionOrder: append([]string(nil), decisionOrder...)},
		Counts: Counts{
			Rows: scan.rows, ObservedAmountRows: observed, UnknownAmountRows: unknown,
			IncludedRows: included.rows, IncludedEdges: uint64(len(scan.edges)), IncludedSourceCommittees: uint64(len(scan.sources)), IncludedRecipients: uint64(len(scan.recipients)),
			IncludedSelfEdgeRows: scan.selfRows, IncludedSelfEdges: uint64(len(scan.selfEdges)), IncludedConduitRows: scan.conduitRows, IncludedBackReferenceRows: scan.backRefRows,
		},
		Decisions: namedStats(scan.decisions, decisionOrder), SourceIdentity: namedStats(scan.identities, nil),
		EntityTypes: namedStats(scan.entityTypes, nil), IndividualClasses: namedStats(scan.individualClasses, nil),
		ReceiptTypes: namedStats(scan.receiptTypes, nil), ReceiptRoles: namedStats(scan.receiptRoles, nil),
		ReceiptEntityShapes: namedStats(scan.receiptEntityShapes, nil), ActionCodes: namedStats(scan.actionCodes, nil),
		ExactIdentityReceiptShapes: namedStats(scan.exactIdentityReceiptShapes, nil),
		TopEdges:                   topEdges(scan.edges, options.TopEdges), Samples: selectedIdentitySamples(scan.samples),
	}
	decisionRows := uint64(0)
	for _, value := range result.Decisions {
		decisionRows += value.Rows
	}
	result.Checks = []Check{
		{Name: "row_conservation", Passed: result.Counts.Rows == manifest.Counts.Facts && decisionRows == result.Counts.Rows, Details: fmt.Sprintf("manifest=%d scanned=%d decisions=%d", manifest.Counts.Facts, result.Counts.Rows, decisionRows)},
		{Name: "amount_observation_conservation", Passed: result.Counts.ObservedAmountRows+result.Counts.UnknownAmountRows == result.Counts.Rows, Details: fmt.Sprintf("observed=%d unknown=%d rows=%d", result.Counts.ObservedAmountRows, result.Counts.UnknownAmountRows, result.Counts.Rows)},
		{Name: "diagnostic_only", Passed: true, Details: "the exact-match cohort measures a candidate rule; it does not publish flow money or graph edges"},
	}
	return result
}

func namedStats(values map[string]moneyStat, preferred []string) []NamedMoneyStat {
	names := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, name := range preferred {
		if _, exists := values[name]; exists {
			names = append(names, name)
			seen[name] = struct{}{}
		}
	}
	rest := make([]string, 0, len(values))
	for name := range values {
		if _, exists := seen[name]; !exists {
			rest = append(rest, name)
		}
	}
	sort.Strings(rest)
	names = append(names, rest...)
	result := make([]NamedMoneyStat, 0, len(names))
	for _, name := range names {
		value := values[name]
		result = append(result, NamedMoneyStat{Name: name, Rows: value.rows, PositiveRows: value.positive, NegativeRows: value.negative, ZeroRows: value.zero, UnknownAmountRows: value.unknown, AmountMinorUnits: strconv.FormatInt(value.amount, 10)})
	}
	return result
}

func topEdges(values map[edgeKey]moneyStat, limit int) []EdgeGroup {
	result := make([]EdgeGroup, 0, len(values))
	for key, value := range values {
		result = append(result, EdgeGroup{SourceCommitteeID: key.source, RecipientCommitteeID: key.recipient, ReceiptRole: key.role, Rows: value.rows, PositiveRows: value.positive, NegativeRows: value.negative, ZeroRows: value.zero, AmountMinorUnits: strconv.FormatInt(value.amount, 10)})
	}
	sort.Slice(result, func(left, right int) bool {
		leftAmount, _ := strconv.ParseInt(result[left].AmountMinorUnits, 10, 64)
		rightAmount, _ := strconv.ParseInt(result[right].AmountMinorUnits, 10, 64)
		leftAbs, rightAbs := absolute(leftAmount), absolute(rightAmount)
		if leftAbs != rightAbs {
			return leftAbs > rightAbs
		}
		if result[left].SourceCommitteeID != result[right].SourceCommitteeID {
			return result[left].SourceCommitteeID < result[right].SourceCommitteeID
		}
		if result[left].RecipientCommitteeID != result[right].RecipientCommitteeID {
			return result[left].RecipientCommitteeID < result[right].RecipientCommitteeID
		}
		return result[left].ReceiptRole < result[right].ReceiptRole
	})
	if len(result) > limit {
		result = result[:limit]
	}
	return result
}

func absolute(value int64) uint64 {
	if value >= 0 {
		return uint64(value)
	}
	return uint64(-(value + 1)) + 1
}

func nullableCategory(value *string) string {
	if value == nil || *value == "" {
		return "<null>"
	}
	return *value
}

func nullableBoolean(value *bool) string {
	if value == nil {
		return "<null>"
	}
	return strconv.FormatBool(*value)
}

func nonNilSamples(values []IdentitySample) []IdentitySample {
	if values == nil {
		return []IdentitySample{}
	}
	return values
}

func selectedIdentitySamples(values []IdentitySample) []IdentitySample {
	sort.Slice(values, func(left, right int) bool {
		if values[left].State != values[right].State {
			return values[left].State < values[right].State
		}
		return values[left].SourceRowOrdinal < values[right].SourceRowOrdinal
	})
	perState := make(map[string]int)
	result := make([]IdentitySample, 0, maxIdentitySamples)
	for _, value := range values {
		if perState[value.State] >= 8 {
			continue
		}
		result = append(result, value)
		perState[value.State]++
		if len(result) == maxIdentitySamples {
			break
		}
	}
	return nonNilSamples(result)
}
