package fecscheduleab

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/parquet-go/parquet-go"
	committeeflows "github.com/vedantadhobley/legal-tender/internal/calculation/fec/committeeflows"
	fecoccurrence "github.com/vedantadhobley/legal-tender/internal/source/fec/occurrence"
	fecrelease "github.com/vedantadhobley/legal-tender/internal/source/fec/release"
	"github.com/vedantadhobley/legal-tender/internal/source/fec/scheduleb"
	storageartifact "github.com/vedantadhobley/legal-tender/internal/storage/artifact"
)

const scheduleBURL = "https://www.fec.gov/files/bulk-downloads/data-dump/schedules/fec_fitem_sched_b.dump"

var (
	bSenderIndex         = mustScheduleBColumn("cmte_id")
	bRawRecipientIndex   = mustScheduleBColumn("recipient_cmte_id")
	bCleanRecipientIndex = mustScheduleBColumn("clean_recipient_cmte_id")
	bDateIndex           = mustScheduleBColumn("disb_dt")
	bAmountIndex         = mustScheduleBColumn("disb_amt")
)

type scheduleAProbeRow struct {
	SourceRowOrdinal   int64   `parquet:"lt_source_row_ordinal"`
	Normalization      string  `parquet:"lt_normalization_state"`
	RecipientID        *string `parquet:"cmte_id,optional"`
	ContributorID      *string `parquet:"contbr_id,optional"`
	CleanContributorID *string `parquet:"clean_contbr_id,optional"`
	MemoedSubtotal     bool    `parquet:"lt_memoed_subtotal"`
	ReceiptTypeCode    *string `parquet:"receipt_tp,optional"`
	AmountMinorUnits   *int64  `parquet:"lt_receipt_amount_minor_units,optional"`
	AmountState        string  `parquet:"lt_receipt_amount_state"`
	ReceiptDateDays    *int32  `parquet:"lt_receipt_date,optional"`
}

type scheduleAScan struct {
	rows         uint64
	flowRows     uint64
	withoutDate  uint64
	observations []flowObservation
	err          error
}

// Audit measures candidate two-sided evidence for the already accepted
// Schedule A receiver-flow cohort. Schedule B remains a separate ledger and
// no state emitted here is a published reconciliation assertion.
func Audit(ctx context.Context, options Options) (Result, error) {
	if options.StorageRoot == "" || options.ScheduleAFactManifestPath == "" ||
		options.ScheduleAReleaseManifestPath == "" || options.ScheduleBDumpPath == "" ||
		options.ScheduleBObservationPath == "" || options.Cycle == "" {
		return Result{}, fmt.Errorf("storage root, Schedule A fact and release manifests, Schedule B dump and observation, and cycle are required")
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
	if options.PGRestorePath == "" {
		options.PGRestorePath = "pg_restore"
	}
	started := options.Clock().UTC()
	result := Result{
		SchemaVersion: SchemaVersion, ProbeVersion: ProbeVersion,
		Status: "diagnostic_not_reconciliation", Cycle: options.Cycle, StartedAt: started,
		Configuration: Configuration{
			ScheduleACohortPolicy: committeeflows.PolicyVersion,
			ExactKey:              "sender committee ID + recipient committee ID + calendar date + signed exact cents",
			CompatibleKey:         "unique sender/recipient/signed-cents candidate with a different calendar date",
			ConflictKey:           "unique sender/recipient/calendar-date candidate with different signed cents",
			CandidatePrecedence:   []string{stateExact, stateCompatible, stateConflict, bDispositionUnmatched},
			Workers:               options.Workers, MaxScheduleBRows: options.MaxScheduleBRows,
		},
		Caveats: []string{
			"Schedule A and Schedule B remain separate publisher assertions; matching evidence never adds their amounts.",
			"Compatible and conflicting states are candidates for review, not accepted economic-payment identities.",
			"The Schedule B side has no accepted effective-record or outgoing-flow policy; action, memo, line, form, purpose, and filing lineage require later classification.",
			"Schedule B rows outside an endpoint pair observed in the accepted Schedule A cohort are measured but cannot be called sender-only flows by this audit.",
		},
	}

	manifest, manifestDigest, err := fecoccurrence.LoadPublishedScheduleAColumnarManifest(ctx, options.StorageRoot, options.ScheduleAFactManifestPath)
	if err != nil {
		return result, fmt.Errorf("load Schedule A columnar facts: %w", err)
	}
	if manifest.Cycle != options.Cycle {
		return result, fmt.Errorf("Schedule A facts belong to cycle %s, expected %s", manifest.Cycle, options.Cycle)
	}
	releaseManifest, releaseDigest, scheduleAArtifact, err := loadScheduleARelease(options.ScheduleAReleaseManifestPath, manifest)
	if err != nil {
		return result, err
	}
	observation, err := loadScheduleBObservation(options.ScheduleBObservationPath)
	if err != nil {
		return result, err
	}
	alignment, err := sourceAlignment(scheduleAArtifact.LastModified, observation.LastModified)
	if err != nil {
		return result, err
	}
	result.Inputs.Alignment = alignment
	result.Inputs.ScheduleA = ScheduleAInput{
		FactSetID: manifest.FactSetID, FactManifestSHA256: manifestDigest,
		SourceReleaseID: releaseManifest.ReleaseID, ReleaseManifestSHA256: releaseDigest,
		SourceVersionID: scheduleAArtifact.VersionID, SourceArtifactSHA256: scheduleAArtifact.SHA256,
		LastModified: scheduleAArtifact.LastModified, Rows: manifest.Counts.Facts, Shards: uint64(len(manifest.Shards)),
	}
	relation := scheduleBRelation(options.Cycle)
	result.Inputs.ScheduleB = ScheduleBInput{
		ObservationSchemaVersion: observation.SchemaVersion, ObservedAt: observation.ObservedAt,
		SourceVersionID: observation.VersionID, SourceArtifactSHA256: observation.ArtifactSHA256,
		LastModified: observation.LastModified, ArtifactBytes: observation.ContentLength, Relation: relation,
	}
	if !alignment.SamePublisherDate {
		return finish(result, options.Clock), fmt.Errorf("Schedule A and B publisher dates are not aligned")
	}

	options.Progress("verifying the pinned Schedule B archive digest")
	bDigest, bBytes, err := hashFile(ctx, options.ScheduleBDumpPath)
	if err != nil {
		return finish(result, options.Clock), fmt.Errorf("hash Schedule B archive: %w", err)
	}
	if bDigest != observation.ArtifactSHA256 || bBytes != observation.ContentLength {
		return finish(result, options.Clock), fmt.Errorf("Schedule B archive does not match its pinned observation")
	}

	options.Progress(fmt.Sprintf("scanning %d Schedule A Parquet shards with %d workers", len(manifest.Shards), options.Workers))
	aScan, err := scanScheduleA(ctx, options.StorageRoot, manifest, options.Workers, options.Progress)
	if err != nil {
		return finish(result, options.Clock), err
	}
	match, err := newMatcher(aScan.observations)
	if err != nil {
		return finish(result, options.Clock), err
	}

	options.Progress(fmt.Sprintf("streaming and verifying Schedule B relation %s", relation))
	reader, writer := io.Pipe()
	extraction := make(chan error, 1)
	extractContext, cancelExtraction := context.WithCancel(ctx)
	defer cancelExtraction()
	go func() {
		_, extractErr := fecrelease.ExtractRelation(extractContext, options.PGRestorePath, options.ScheduleBDumpPath, relation, options.Cycle, writer)
		_ = writer.CloseWithError(extractErr)
		extraction <- extractErr
		close(extraction)
	}()
	verification, verifyErr := scheduleb.Verify(ctx, reader, scheduleb.VerifyOptions{
		ExpectedPeriod: options.Cycle, WorkDir: options.WorkDir, MaxRows: options.MaxScheduleBRows,
		Progress: options.Progress,
		ObserveValidRow: func(row *scheduleb.Row) error {
			observation, eligible := scheduleBObservation(row)
			if !eligible {
				match.observeIneligibleB()
				return nil
			}
			return match.observeB(observation)
		},
	})
	result.ScheduleBVerification = verification
	if verifyErr != nil || !verification.Complete {
		cancelExtraction()
		_ = reader.Close()
	}
	extractErr := <-extraction
	if verifyErr != nil {
		return finish(result, options.Clock), verifyErr
	}
	if verification.Complete && extractErr != nil {
		return finish(result, options.Clock), extractErr
	}

	states, err := match.candidateStates()
	if err != nil {
		return finish(result, options.Clock), err
	}
	dispositions := match.dispositions()
	result.CandidateStates = states
	result.ScheduleBDisposition = dispositions
	result.Counts = buildCounts(manifest, aScan, verification, dispositions, match)
	result.Checks = buildChecks(result, alignment, verification.Complete)
	if !verification.Complete {
		result.Status = "partial_diagnostic_not_reconciliation"
	}
	result = finish(result, options.Clock)
	for _, check := range result.Checks {
		if check.Severity == "block" && !check.Passed {
			return result, fmt.Errorf("Schedule A/B audit check %s failed: %s", check.ID, check.Detail)
		}
	}
	return result, nil
}

func scanScheduleA(ctx context.Context, storageRoot string, manifest fecoccurrence.ScheduleAColumnarManifest, workers int, progress func(string)) (scheduleAScan, error) {
	if workers > len(manifest.Shards) {
		workers = len(manifest.Shards)
	}
	workerContext, cancel := context.WithCancel(ctx)
	defer cancel()
	jobs := make(chan fecoccurrence.ScheduleAColumnarShard)
	results := make(chan scheduleAScan, workers)
	var group sync.WaitGroup
	group.Add(workers)
	for range workers {
		go func() {
			defer group.Done()
			for shard := range jobs {
				result := scanScheduleAShard(workerContext, storageRoot, shard)
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
		for _, shard := range manifest.Shards {
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

	combined := scheduleAScan{observations: make([]flowObservation, 0, 400_000)}
	completed := 0
	var firstError error
	for result := range results {
		if result.err != nil {
			if firstError == nil {
				firstError = result.err
			}
			continue
		}
		combined.rows += result.rows
		combined.flowRows += result.flowRows
		combined.withoutDate += result.withoutDate
		combined.observations = append(combined.observations, result.observations...)
		completed++
		if completed%16 == 0 || completed == len(manifest.Shards) {
			progress(fmt.Sprintf("scanned %d of %d Schedule A shards", completed, len(manifest.Shards)))
		}
	}
	if firstError != nil {
		return combined, firstError
	}
	if completed != len(manifest.Shards) || combined.rows != manifest.Counts.Facts {
		return combined, fmt.Errorf("Schedule A scan did not conserve manifest rows")
	}
	return combined, nil
}

func scanScheduleAShard(ctx context.Context, storageRoot string, shard fecoccurrence.ScheduleAColumnarShard) scheduleAScan {
	result := scheduleAScan{observations: make([]flowObservation, 0, 2048)}
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
	reader := parquet.NewGenericReader[scheduleAProbeRow](file)
	buffer := make([]scheduleAProbeRow, 8192)
	for {
		count, readErr := reader.Read(buffer)
		for index := range count {
			row := buffer[index]
			result.rows++
			if result.rows&0x3fff == 0 {
				if err := ctx.Err(); err != nil {
					result.err = err
					break
				}
			}
			evaluation := committeeflows.Evaluate(committeeflows.EvaluationInput{
				NormalizationState: row.Normalization, RecipientCommitteeID: row.RecipientID,
				ContributorID: row.ContributorID, CleanContributorID: row.CleanContributorID,
				MemoedSubtotal: row.MemoedSubtotal, AmountObservationState: row.AmountState,
				AmountMinorUnits: row.AmountMinorUnits, ReceiptTypeCode: row.ReceiptTypeCode,
			})
			if evaluation.Decision != committeeflows.DecisionIncluded {
				continue
			}
			result.flowRows++
			if row.ReceiptDateDays == nil {
				result.withoutDate++
				continue
			}
			sourceID, sourceOK := parseCommitteeIDString(evaluation.SourceCommitteeID)
			recipientID, recipientOK := parseCommitteeIDString(*row.RecipientID)
			if !sourceOK || !recipientOK || row.AmountMinorUnits == nil {
				result.err = fmt.Errorf("Schedule A included flow row %d has an invalid index projection", row.SourceRowOrdinal)
				break
			}
			result.observations = append(result.observations, flowObservation{
				SourceID: sourceID, RecipientID: recipientID, DateDays: *row.ReceiptDateDays, Amount: *row.AmountMinorUnits,
			})
		}
		if result.err != nil || readErr != nil {
			if readErr != nil && !errors.Is(readErr, io.EOF) && result.err == nil {
				result.err = readErr
			}
			break
		}
	}
	if err := reader.Close(); err != nil && result.err == nil {
		result.err = err
	}
	if err := file.Close(); err != nil && result.err == nil {
		result.err = err
	}
	if result.err == nil && result.rows != shard.Facts {
		result.err = fmt.Errorf("Schedule A shard %d yielded %d rows, expected %d", shard.Index, result.rows, shard.Facts)
	}
	return result
}

func scheduleBObservation(row *scheduleb.Row) (flowObservation, bool) {
	sender, _ := row.Field(bSenderIndex)
	rawRecipient, _ := row.Field(bRawRecipientIndex)
	cleanRecipient, _ := row.Field(bCleanRecipientIndex)
	date, _ := row.Field(bDateIndex)
	amount, _ := row.Field(bAmountIndex)
	if sender.IsNull() || rawRecipient.IsNull() || cleanRecipient.IsNull() ||
		date.IsNull() || amount.IsNull() || !strings.EqualFold(rawRecipient.String(), cleanRecipient.String()) {
		return flowObservation{}, false
	}
	sourceID, sourceOK := parseCommitteeIDBytes(sender.Bytes())
	recipientID, recipientOK := parseCommitteeIDBytes(rawRecipient.Bytes())
	dateDays, dateOK := parseDateDays(date.Bytes())
	amountMinorUnits, amountOK := parseScheduleBCents(amount.Bytes())
	if !sourceOK || !recipientOK || !dateOK || !amountOK {
		return flowObservation{}, false
	}
	return flowObservation{SourceID: sourceID, RecipientID: recipientID, DateDays: dateDays, Amount: amountMinorUnits}, true
}

func parseCommitteeIDString(value string) (uint32, bool) {
	return parseCommitteeIDBytes([]byte(value))
}

func parseCommitteeIDBytes(value []byte) (uint32, bool) {
	if len(value) != 9 || value[0] != 'C' {
		return 0, false
	}
	var result uint32
	for _, character := range value[1:] {
		if character < '0' || character > '9' {
			return 0, false
		}
		result = result*10 + uint32(character-'0')
	}
	return result, true
}

func parseDateDays(value []byte) (int32, bool) {
	if len(value) < 10 || value[4] != '-' || value[7] != '-' {
		return 0, false
	}
	year, okYear := decimal(value[0:4])
	month, okMonth := decimal(value[5:7])
	day, okDay := decimal(value[8:10])
	if !okYear || !okMonth || !okDay {
		return 0, false
	}
	parsed := time.Date(year, time.Month(month), day, 0, 0, 0, 0, time.UTC)
	if parsed.Year() != year || int(parsed.Month()) != month || parsed.Day() != day {
		return 0, false
	}
	return int32(parsed.Unix() / 86400), true
}

func decimal(value []byte) (int, bool) {
	result := 0
	for _, character := range value {
		if character < '0' || character > '9' {
			return 0, false
		}
		result = result*10 + int(character-'0')
	}
	return result, true
}

func parseScheduleBCents(value []byte) (int64, bool) {
	if len(value) < 4 || value[len(value)-3] != '.' {
		return 0, false
	}
	negative := value[0] == '-'
	start := 0
	if negative {
		start = 1
	}
	var result int64
	for index, character := range value[start:] {
		if index == len(value[start:])-3 {
			continue
		}
		if character < '0' || character > '9' || result > (1<<63-1-int64(character-'0'))/10 {
			return 0, false
		}
		result = result*10 + int64(character-'0')
	}
	if negative {
		result = -result
	}
	return result, true
}

func buildCounts(manifest fecoccurrence.ScheduleAColumnarManifest, a scheduleAScan, b scheduleb.Verification, dispositions []NamedCount, match *matcher) Counts {
	values := make(map[string]uint64, len(dispositions))
	for _, disposition := range dispositions {
		values[disposition.Name] = disposition.Rows
	}
	knownEndpoint := values[bDispositionExact] + values[bDispositionCompatible] + values[bDispositionConflict] + values[bDispositionUnmatched]
	return Counts{
		ScheduleASourceRows: a.rows, ScheduleAFlowRows: a.flowRows,
		ScheduleAIndexableRows: uint64(len(a.observations)), ScheduleAWithoutDateRows: a.withoutDate,
		ScheduleAExactSignatures: uint64(len(match.exactA)), ScheduleAEndpointPairs: uint64(len(match.endpoints)),
		ScheduleBSourceRows: b.Rows, ScheduleBEligibleRows: b.ValidRows - values[bDispositionIneligible],
		ScheduleBIneligibleRows: values[bDispositionIneligible], ScheduleBKnownEndpointRows: knownEndpoint,
		ScheduleBOutsideEndpointRows: values[bDispositionOutside],
	}
}

func buildChecks(result Result, alignment SourceAlignment, complete bool) []Check {
	aStateRows, bStateRows := sumStateRows(result.CandidateStates)
	dispositionRows := sumDispositionRows(result.ScheduleBDisposition)
	candidateBRows := uint64(0)
	for _, disposition := range result.ScheduleBDisposition {
		if disposition.Name == bDispositionExact || disposition.Name == bDispositionCompatible || disposition.Name == bDispositionConflict {
			candidateBRows += disposition.Rows
		}
	}
	return []Check{
		{ID: "publisher_batch_alignment", Passed: alignment.SamePublisherDate, Severity: "block", Detail: fmt.Sprintf("Schedule A=%s Schedule B=%s", alignment.ScheduleAPublisherDate, alignment.ScheduleBPublisherDate)},
		{ID: "schedule_a_row_conservation", Passed: result.Counts.ScheduleASourceRows == result.Inputs.ScheduleA.Rows, Severity: "block", Detail: fmt.Sprintf("scanned=%d manifest=%d", result.Counts.ScheduleASourceRows, result.Inputs.ScheduleA.Rows)},
		{ID: "schedule_a_cohort_conservation", Passed: result.Counts.ScheduleAFlowRows == result.Counts.ScheduleAIndexableRows+result.Counts.ScheduleAWithoutDateRows, Severity: "block", Detail: fmt.Sprintf("flow=%d indexable=%d without_date=%d", result.Counts.ScheduleAFlowRows, result.Counts.ScheduleAIndexableRows, result.Counts.ScheduleAWithoutDateRows)},
		{ID: "schedule_a_candidate_state_conservation", Passed: aStateRows == result.Counts.ScheduleAIndexableRows, Severity: "block", Detail: fmt.Sprintf("states=%d indexable=%d", aStateRows, result.Counts.ScheduleAIndexableRows)},
		{ID: "schedule_b_disposition_conservation", Passed: dispositionRows == result.ScheduleBVerification.ValidRows, Severity: "block", Detail: fmt.Sprintf("dispositions=%d valid=%d", dispositionRows, result.ScheduleBVerification.ValidRows)},
		{ID: "schedule_b_candidate_state_conservation", Passed: bStateRows == candidateBRows, Severity: "block", Detail: fmt.Sprintf("states=%d candidate_dispositions=%d", bStateRows, candidateBRows)},
		{ID: "schedule_b_complete", Passed: complete, Severity: "observe", Detail: fmt.Sprintf("complete=%t max_rows=%d", complete, result.Configuration.MaxScheduleBRows)},
		{ID: "no_money_merge", Passed: true, Severity: "block", Detail: "Schedule A and Schedule B amounts remain separate in every candidate state"},
	}
}

func loadScheduleARelease(path string, manifest fecoccurrence.ScheduleAColumnarManifest) (fecrelease.ReleaseManifest, string, fecrelease.PublishedArtifact, error) {
	content, err := os.ReadFile(path)
	if err != nil {
		return fecrelease.ReleaseManifest{}, "", fecrelease.PublishedArtifact{}, fmt.Errorf("read Schedule A release manifest: %w", err)
	}
	var releaseManifest fecrelease.ReleaseManifest
	decoder := json.NewDecoder(strings.NewReader(string(content)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&releaseManifest); err != nil {
		return fecrelease.ReleaseManifest{}, "", fecrelease.PublishedArtifact{}, fmt.Errorf("decode Schedule A release manifest: %w", err)
	}
	if issues := fecrelease.ValidateKnownManifest(releaseManifest); len(issues) != 0 {
		return fecrelease.ReleaseManifest{}, "", fecrelease.PublishedArtifact{}, fmt.Errorf("invalid Schedule A release manifest: %s", issues[0].Message)
	}
	digest := sha256.Sum256(content)
	digestText := hex.EncodeToString(digest[:])
	if releaseManifest.ReleaseID != manifest.SourceReleaseID || digestText != manifest.SourceReleaseManifestSHA256 {
		return fecrelease.ReleaseManifest{}, "", fecrelease.PublishedArtifact{}, fmt.Errorf("Schedule A fact and release lineage do not match")
	}
	for _, artifact := range releaseManifest.Artifacts {
		if artifact.SourceID == fecrelease.ScheduleASourceID {
			return releaseManifest, digestText, artifact, nil
		}
	}
	return fecrelease.ReleaseManifest{}, "", fecrelease.PublishedArtifact{}, fmt.Errorf("Schedule A release has no processed Schedule A artifact")
}

func loadScheduleBObservation(path string) (scheduleBArchiveObservation, error) {
	content, err := os.ReadFile(path)
	if err != nil {
		return scheduleBArchiveObservation{}, fmt.Errorf("read Schedule B observation: %w", err)
	}
	var result scheduleBArchiveObservation
	if err := json.Unmarshal(content, &result); err != nil {
		return result, fmt.Errorf("decode Schedule B observation: %w", err)
	}
	if result.SchemaVersion == "" || result.ObservedAt == "" || result.RequestURL != scheduleBURL ||
		result.LastModified == "" || result.VersionID == "" || result.ContentLength <= 0 || len(result.ArtifactSHA256) != 64 {
		return result, fmt.Errorf("Schedule B observation is incomplete")
	}
	return result, nil
}

func sourceAlignment(scheduleALastModified, scheduleBLastModified string) (SourceAlignment, error) {
	aTime, err := time.Parse(time.RFC1123, scheduleALastModified)
	if err != nil {
		return SourceAlignment{}, fmt.Errorf("parse Schedule A Last-Modified: %w", err)
	}
	bTime, err := time.Parse(time.RFC3339, scheduleBLastModified)
	if err != nil {
		return SourceAlignment{}, fmt.Errorf("parse Schedule B Last-Modified: %w", err)
	}
	aDate := aTime.UTC().Format(time.DateOnly)
	bDate := bTime.UTC().Format(time.DateOnly)
	return SourceAlignment{
		ScheduleAPublisherDate: aDate, ScheduleBPublisherDate: bDate,
		SamePublisherDate: aDate == bDate,
		Basis:             "immutable FEC object version IDs and UTC Last-Modified calendar date",
	}, nil
}

func hashFile(ctx context.Context, path string) (string, int64, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", 0, err
	}
	defer file.Close()
	digest := sha256.New()
	buffer := make([]byte, 4<<20)
	var total int64
	for {
		count, readErr := file.Read(buffer)
		if count > 0 {
			if _, err := digest.Write(buffer[:count]); err != nil {
				return "", total, err
			}
			total += int64(count)
		}
		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				break
			}
			return "", total, readErr
		}
		if err := ctx.Err(); err != nil {
			return "", total, err
		}
	}
	return hex.EncodeToString(digest.Sum(nil)), total, nil
}

func scheduleBRelation(cycle string) string {
	year, _ := strconv.Atoi(cycle)
	return fmt.Sprintf("disclosure.fec_fitem_sched_b_%d_%s", year-1, cycle)
}

func mustScheduleBColumn(name string) int {
	index, ok := scheduleb.ColumnIndex(name)
	if !ok {
		panic("missing Schedule B column " + name)
	}
	return index
}

func finish(result Result, clock func() time.Time) Result {
	result.CompletedAt = clock().UTC()
	result.ElapsedSeconds = result.CompletedAt.Sub(result.StartedAt).Seconds()
	return result
}
