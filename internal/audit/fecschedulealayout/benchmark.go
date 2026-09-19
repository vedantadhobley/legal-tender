package fecschedulealayout

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/vedantadhobley/legal-tender/internal/source/fec/schedulea"
)

const (
	parquetLibraryVersion = "github.com/parquet-go/parquet-go@v0.32.0"
	minimumDecisionRows   = 10_000_000
	maximumSizeRatio      = 1.50
	minimumWriteRowsPS    = 100_000
	maximumProjectedWrite = 45 * time.Minute
	minimumScanSpeedup    = 1.50
	maximumProcessRSS     = 2 << 30
)

var projectionFieldNames = []string{
	"cmte_id",
	"contb_receipt_dt",
	"contb_receipt_amt",
	"memo_cd",
	"is_individual",
}

// Options bound one real-corpus benchmark. OutputDirectory must not exist;
// benchmark evidence is never silently overwritten.
type Options struct {
	SourcePath           string
	Cycle                string
	Rows                 uint64
	ExpectedTotalRows    uint64
	ExpectedSourceSHA256 string
	OutputDirectory      string
	RowsPerFile          uint64
	RowsPerRowGroup      uint64
	Clock                func() time.Time
	Progress             func(string)
}

// Run creates equal-row zstd COPY and Parquet candidates, proves a complete
// logical-value round trip, and compares the accepted five-column receipt
// projection through both readers.
func Run(ctx context.Context, options Options) (Result, error) {
	var result Result
	if err := validateOptions(options); err != nil {
		return result, err
	}
	if options.Clock == nil {
		options.Clock = time.Now
	}
	if options.RowsPerFile == 0 {
		options.RowsPerFile = 1_000_000
	}
	if options.RowsPerRowGroup == 0 {
		options.RowsPerRowGroup = 128_000
	}
	if err := os.Mkdir(options.OutputDirectory, 0o750); err != nil {
		return result, fmt.Errorf("create benchmark output directory: %w", err)
	}

	sourceInfo, err := os.Stat(options.SourcePath)
	if err != nil {
		return result, fmt.Errorf("stat Schedule A source: %w", err)
	}
	result = Result{
		SchemaVersion: SchemaVersion,
		State:         "complete",
		Cycle:         options.Cycle,
		SampleRows:    options.Rows,
		Source: Source{
			Path:                  filepath.Base(options.SourcePath),
			CompressedBytes:       sourceInfo.Size(),
			ExpectedTotalRows:     options.ExpectedTotalRows,
			ExpectedSHA256:        options.ExpectedSourceSHA256,
			SampleFirstRowOrdinal: 1,
			SampleLastRowOrdinal:  options.Rows,
		},
		Configuration: Configuration{
			ColumnCount:      schedulea.FieldCount,
			Representation:   "all source lexemes; is_individual typed boolean; source row ordinal implicit from ordered shard range",
			ParquetLibrary:   parquetLibraryVersion,
			Compression:      "zstd default level; one encoder worker",
			RowsPerFile:      options.RowsPerFile,
			RowsPerRowGroup:  options.RowsPerRowGroup,
			ProjectionFields: append([]string(nil), projectionFieldNames...),
		},
		Diagnostics: []string{
			"This is a bounded sequential-prefix benchmark, not a production fact publication.",
			"The immutable COPY source remains the exact-byte authority; the Parquet candidate preserves decoded source values and resolves exact bytes through shard row ordinals.",
			"Projected Parquet bytes apply the measured equal-row size ratio to the complete source compressed size; prefix bytes per row are diagnostic only.",
			"A result below ten million rows is provisional and cannot accept the layout.",
		},
	}

	report(options.Progress, "writing equal-row zstd COPY baseline")
	result.ZstdCOPY, err = writeZstdBaseline(ctx, options)
	if err != nil {
		return result, err
	}
	report(options.Progress, "writing bounded Parquet candidate")
	result.Parquet, err = writeParquet(ctx, options)
	if err != nil {
		return result, err
	}
	if result.ZstdCOPY.SemanticSHA256 != result.Parquet.SemanticSHA256 {
		return result, errors.New("Parquet writer input semantic digest differs from equal-row zstd input")
	}

	report(options.Progress, "reading all Parquet columns for round-trip proof")
	fullScanStarted := options.Clock()
	fullDigest, fullRows, err := scanParquetFull(ctx, options.OutputDirectory, result.Parquet.Files)
	fullScanDuration := options.Clock().Sub(fullScanStarted)
	if err != nil {
		return result, err
	}
	if fullRows != options.Rows {
		return result, fmt.Errorf("full Parquet scan read %d rows; want %d", fullRows, options.Rows)
	}
	result.RoundTrip.FullSemanticDigestEqual = fullDigest == result.ZstdCOPY.SemanticSHA256
	result.RoundTrip.FullScanDurationMilliseconds = fullScanDuration.Milliseconds()
	result.RoundTrip.FullScanRowsPerSecond = rate(fullRows, fullScanDuration)

	report(options.Progress, "scanning the source receipt projection")
	result.SourceScan, err = scanSourceProjection(ctx, options)
	if err != nil {
		return result, err
	}
	report(options.Progress, "scanning the Parquet receipt projection")
	result.ParquetScan, err = scanParquetProjection(ctx, options.OutputDirectory, result.Parquet.Files)
	if err != nil {
		return result, err
	}
	result.RoundTrip.ProjectionDigestEqual = result.SourceScan.ProjectionSHA256 == result.ParquetScan.ProjectionSHA256
	result.RoundTrip.DecisionCountsEqual = equalDecisions(result.SourceScan.Decisions, result.ParquetScan.Decisions)

	result.Resources = readResources()
	result.Extrapolation = extrapolate(result)
	result.Gates = evaluateGates(result)
	result.Verdict = verdict(result.Gates)
	if !result.RoundTrip.FullSemanticDigestEqual || !result.RoundTrip.ProjectionDigestEqual || !result.RoundTrip.DecisionCountsEqual {
		result.State = "invalid"
		result.Verdict = "rejected"
	}
	if err := writeResult(options.OutputDirectory, result); err != nil {
		return result, err
	}
	return result, nil
}

func validateOptions(options Options) error {
	switch {
	case options.SourcePath == "":
		return errors.New("source path is required")
	case options.Cycle == "":
		return errors.New("cycle is required")
	case options.Rows == 0:
		return errors.New("rows must be positive")
	case options.ExpectedTotalRows < options.Rows:
		return errors.New("expected total rows must be at least the sample row count")
	case options.OutputDirectory == "":
		return errors.New("output directory is required")
	}
	if _, err := os.Stat(options.OutputDirectory); err == nil {
		return fmt.Errorf("output directory already exists: %s", options.OutputDirectory)
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("inspect output directory: %w", err)
	}
	return nil
}

func writeZstdBaseline(ctx context.Context, options Options) (LayoutMeasurement, error) {
	var result LayoutMeasurement
	started := options.Clock()
	path := filepath.Join(options.OutputDirectory, "sample.copy.zst")
	output, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o640)
	if err != nil {
		return result, err
	}
	compressedHash := sha256.New()
	countedOutput := &countingWriter{writer: io.MultiWriter(output, compressedHash)}
	encoder, err := zstd.NewWriter(countedOutput, zstd.WithEncoderConcurrency(1), zstd.WithEncoderLevel(zstd.SpeedDefault))
	if err != nil {
		_ = output.Close()
		return result, err
	}
	semantic := newSemanticHasher()
	rawBytes, rows, scanErr := scanSource(ctx, options.SourcePath, options.Cycle, options.Rows, func(row *schedulea.Row) error {
		if _, err := encoder.Write(row.Raw()); err != nil {
			return err
		}
		return semantic.AddScheduleARow(row)
	})
	closeErr := encoder.Close()
	fileErr := output.Close()
	if scanErr != nil {
		return result, scanErr
	}
	if closeErr != nil {
		return result, closeErr
	}
	if fileErr != nil {
		return result, fileErr
	}
	if rows != options.Rows {
		return result, fmt.Errorf("zstd baseline read %d rows; want %d", rows, options.Rows)
	}
	duration := options.Clock().Sub(started)
	result = layoutMeasurement(rows, countedOutput.bytes, duration)
	result.UncompressedBytes = rawBytes
	result.SHA256 = hex.EncodeToString(compressedHash.Sum(nil))
	result.SemanticSHA256 = semantic.Sum()
	return result, nil
}

func scanSourceProjection(ctx context.Context, options Options) (ScanMeasurement, error) {
	started := options.Clock()
	indexes, err := projectionIndexes()
	if err != nil {
		return ScanMeasurement{}, err
	}
	projectionHash := newProjectionHasher()
	var decisions decisionAccumulator
	_, rows, err := scanSource(ctx, options.SourcePath, options.Cycle, options.Rows, func(row *schedulea.Row) error {
		values, err := projectionFromScheduleARow(row, indexes)
		if err != nil {
			return err
		}
		projectionHash.Add(values)
		return decisions.Add(values)
	})
	if err != nil {
		return ScanMeasurement{}, err
	}
	if rows != options.Rows {
		return ScanMeasurement{}, fmt.Errorf("source projection read %d rows; want %d", rows, options.Rows)
	}
	return scanMeasurement(rows, options.Clock().Sub(started), projectionHash.Sum(), decisions.Result()), nil
}

func scanSource(ctx context.Context, path, cycle string, limit uint64, consume func(*schedulea.Row) error) (uint64, uint64, error) {
	file, err := os.Open(path)
	if err != nil {
		return 0, 0, err
	}
	defer func() { _ = file.Close() }()
	decoder, err := zstd.NewReader(file, zstd.WithDecoderConcurrency(2), zstd.WithDecoderLowmem(true), zstd.WithDecoderMaxMemory(64<<20))
	if err != nil {
		return 0, 0, err
	}
	defer decoder.Close()
	rows := schedulea.NewDecoder(decoder)
	var count, rawBytes uint64
	for count < limit && rows.Scan() {
		row := rows.Row()
		if err := schedulea.Validate(row, cycle); err != nil {
			return rawBytes, count, fmt.Errorf("source row %d: %w", row.Number(), err)
		}
		if err := consume(row); err != nil {
			return rawBytes, count, fmt.Errorf("consume source row %d: %w", row.Number(), err)
		}
		count++
		rawBytes += uint64(len(row.Raw()))
		if count&0x3fff == 0 {
			if err := ctx.Err(); err != nil {
				return rawBytes, count, err
			}
		}
	}
	if err := rows.Err(); err != nil {
		return rawBytes, count, err
	}
	return rawBytes, count, nil
}

type projectionIndexSet [lenProjectionFields]int

const lenProjectionFields = 5

func projectionIndexes() (projectionIndexSet, error) {
	var indexes projectionIndexSet
	for index, name := range projectionFieldNames {
		resolved, ok := schedulea.ColumnIndex(name)
		if !ok {
			return indexes, fmt.Errorf("Schedule A schema does not contain %s", name)
		}
		indexes[index] = resolved
	}
	return indexes, nil
}

type logicalValue struct {
	bytes []byte
	null  bool
}

type projectionValues [lenProjectionFields]logicalValue

func projectionFromScheduleARow(row *schedulea.Row, indexes projectionIndexSet) (projectionValues, error) {
	var result projectionValues
	for index, sourceIndex := range indexes {
		field, ok := row.Field(sourceIndex)
		if !ok {
			return result, fmt.Errorf("missing projection field %s", projectionFieldNames[index])
		}
		result[index] = logicalValue{bytes: field.Bytes(), null: field.IsNull()}
	}
	return result, nil
}

type decisionAccumulator struct {
	counts DecisionCounts
	amount int64
}

func (a *decisionAccumulator) Add(values projectionValues) error {
	individual := values[4]
	if individual.null {
		a.counts.UnresolvedIndividualClass++
		return nil
	}
	if len(individual.bytes) != 1 || individual.bytes[0] != 't' {
		a.counts.ExcludedNonIndividual++
		return nil
	}
	memo := values[3]
	if !memo.null && len(memo.bytes) == 1 && memo.bytes[0] == 'X' {
		a.counts.ExcludedMemoSubtotal++
		return nil
	}
	amount := values[2]
	if amount.null {
		a.counts.UnresolvedAmount++
		return nil
	}
	minorUnits, _, issue := schedulea.ParseUSDMinorUnits(string(amount.bytes))
	if issue != "" {
		a.counts.UnresolvedAmount++
		return nil
	}
	parsed, err := strconv.ParseInt(minorUnits, 10, 64)
	if err != nil {
		a.counts.UnresolvedAmount++
		return nil
	}
	if (parsed > 0 && a.amount > math.MaxInt64-parsed) || (parsed < 0 && a.amount < math.MinInt64-parsed) {
		return errors.New("included receipt amount overflow")
	}
	a.amount += parsed
	a.counts.Included++
	return nil
}

func (a decisionAccumulator) Result() DecisionCounts {
	result := a.counts
	result.IncludedAmountMinorUnits = strconv.FormatInt(a.amount, 10)
	return result
}

type semanticHasher struct {
	hash hash.Hash
}

func newSemanticHasher() *semanticHasher {
	h := sha256.New()
	_, _ = h.Write([]byte("legal-tender.schedule-a-layout-semantic.v1"))
	return &semanticHasher{hash: h}
}

func (h *semanticHasher) AddScheduleARow(row *schedulea.Row) error {
	for index := range schedulea.FieldCount {
		field, ok := row.Field(index)
		if !ok {
			return fmt.Errorf("missing semantic field %d", index)
		}
		h.add(field.IsNull(), field.Bytes())
	}
	return nil
}

func (h *semanticHasher) add(null bool, value []byte) {
	if null {
		_, _ = h.hash.Write([]byte{0})
		return
	}
	_, _ = h.hash.Write([]byte{1})
	writeLength(h.hash, uint64(len(value)))
	_, _ = h.hash.Write(value)
}

func (h *semanticHasher) Sum() string {
	return hex.EncodeToString(h.hash.Sum(nil))
}

type projectionHasher struct {
	hash hash.Hash
}

func newProjectionHasher() *projectionHasher {
	h := sha256.New()
	_, _ = h.Write([]byte("legal-tender.schedule-a-layout-projection.v1"))
	return &projectionHasher{hash: h}
}

func (h *projectionHasher) Add(values projectionValues) {
	for _, value := range values {
		if value.null {
			_, _ = h.hash.Write([]byte{0})
			continue
		}
		_, _ = h.hash.Write([]byte{1})
		writeLength(h.hash, uint64(len(value.bytes)))
		_, _ = h.hash.Write(value.bytes)
	}
}

func (h *projectionHasher) Sum() string {
	return hex.EncodeToString(h.hash.Sum(nil))
}

func writeLength(destination hash.Hash, value uint64) {
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], value)
	_, _ = destination.Write(encoded[:])
}

type countingWriter struct {
	writer io.Writer
	bytes  int64
}

func (w *countingWriter) Write(content []byte) (int, error) {
	n, err := w.writer.Write(content)
	w.bytes += int64(n)
	return n, err
}

func layoutMeasurement(rows uint64, bytes int64, duration time.Duration) LayoutMeasurement {
	return LayoutMeasurement{
		Rows: rows, CompressedBytes: bytes, BytesPerRow: float64(bytes) / float64(rows),
		DurationMilliseconds: duration.Milliseconds(), RowsPerSecond: rate(rows, duration),
	}
}

func scanMeasurement(rows uint64, duration time.Duration, digest string, decisions DecisionCounts) ScanMeasurement {
	return ScanMeasurement{
		Rows: rows, DurationMilliseconds: duration.Milliseconds(), RowsPerSecond: rate(rows, duration),
		ProjectionSHA256: digest, Decisions: decisions,
	}
}

func rate(rows uint64, duration time.Duration) float64 {
	if duration <= 0 {
		return 0
	}
	return float64(rows) / duration.Seconds()
}

func extrapolate(result Result) Extrapolation {
	var extrapolation Extrapolation
	if result.ZstdCOPY.CompressedBytes > 0 {
		extrapolation.ParquetToEqualRowZstdRatio = float64(result.Parquet.CompressedBytes) / float64(result.ZstdCOPY.CompressedBytes)
	}
	if result.SampleRows > 0 {
		extrapolation.ProjectedParquetBytes = uint64(math.Round(float64(result.Source.CompressedBytes) * extrapolation.ParquetToEqualRowZstdRatio))
		extrapolation.ProjectedParquetWriteMS = int64(float64(result.Parquet.DurationMilliseconds) * float64(result.Source.ExpectedTotalRows) / float64(result.SampleRows))
	}
	if result.SourceScan.RowsPerSecond > 0 {
		extrapolation.ProjectionScanSpeedup = result.ParquetScan.RowsPerSecond / result.SourceScan.RowsPerSecond
	}
	return extrapolation
}

func evaluateGates(result Result) Gates {
	roundTrip := result.RoundTrip.FullSemanticDigestEqual && result.RoundTrip.ProjectionDigestEqual && result.RoundTrip.DecisionCountsEqual
	return Gates{
		MinimumDecisionRows: minimumDecisionRows, MaximumSizeRatio: maximumSizeRatio,
		MinimumWriteRowsPerSecond: minimumWriteRowsPS, MaximumProjectedWriteMS: maximumProjectedWrite.Milliseconds(),
		MinimumScanSpeedup:     minimumScanSpeedup,
		MaximumProcessPeakRSS:  maximumProcessRSS,
		EnoughRows:             result.SampleRows >= minimumDecisionRows,
		Size:                   result.Extrapolation.ParquetToEqualRowZstdRatio <= maximumSizeRatio,
		WriteThroughput:        result.Parquet.RowsPerSecond >= minimumWriteRowsPS,
		ProjectedWriteDuration: result.Extrapolation.ProjectedParquetWriteMS <= maximumProjectedWrite.Milliseconds(),
		ColumnPruning:          result.Extrapolation.ProjectionScanSpeedup >= minimumScanSpeedup,
		Memory:                 result.Resources.ProcessPeakRSSBytes <= maximumProcessRSS,
		RoundTrip:              roundTrip,
	}
}

func verdict(gates Gates) string {
	if !gates.EnoughRows {
		return "provisional"
	}
	if gates.Size && gates.WriteThroughput && gates.ProjectedWriteDuration && gates.ColumnPruning && gates.Memory && gates.RoundTrip {
		return "accepted_for_full_corpus_probe"
	}
	return "rejected"
}

func equalDecisions(left, right DecisionCounts) bool {
	return left == right
}

func readResources() Resources {
	var memory runtime.MemStats
	runtime.ReadMemStats(&memory)
	return Resources{ProcessPeakRSSBytes: readPeakRSS(), GoMemorySysBytes: memory.Sys}
}

func readPeakRSS() uint64 {
	file, err := os.Open("/proc/self/status")
	if err != nil {
		return 0
	}
	defer func() { _ = file.Close() }()
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) == 3 && fields[0] == "VmHWM:" && fields[2] == "kB" {
			value, err := strconv.ParseUint(fields[1], 10, 64)
			if err == nil {
				return value * 1024
			}
		}
	}
	return 0
}

func writeResult(directory string, result Result) error {
	content, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return err
	}
	content = append(content, '\n')
	path := filepath.Join(directory, "benchmark.json")
	if err := os.WriteFile(path, content, 0o640); err != nil {
		return fmt.Errorf("write benchmark result: %w", err)
	}
	return nil
}

func report(progress func(string), message string) {
	if progress != nil {
		progress(message)
	}
}
