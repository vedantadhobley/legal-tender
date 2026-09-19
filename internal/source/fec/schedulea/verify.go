// Zstd verification joins compressed transport checks with COPY row checks.
package schedulea

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"time"

	"github.com/klauspost/compress/zstd"
)

const (
	verificationSchemaVersion = "legal-tender.schedule-a-verification.v1"
	maxReportedIssues         = 20
	decoderMemoryLimit        = 64 << 20
)

// VerifyOptions defines the selected partition and optional exact conservation
// expectations. MaxRows creates a partial smoke pass and disables whole-stream
// digest and size claims.
type VerifyOptions struct {
	ExpectedPeriod             string
	ExpectedRows               uint64
	ExpectedUncompressedBytes  uint64
	ExpectedUncompressedSHA256 string
	ExpectedCompressedBytes    uint64
	ExpectedCompressedSHA256   string
	MaxRows                    uint64
}

// Check is one stable, machine-readable verification result.
type Check struct {
	ID       string `json:"id"`
	Passed   bool   `json:"passed"`
	Expected string `json:"expected,omitempty"`
	Actual   string `json:"actual,omitempty"`
}

// Verification is the bounded-memory result emitted by the maintenance CLI.
type Verification struct {
	SchemaVersion       string   `json:"schema_version"`
	Complete            bool     `json:"complete"`
	ExpectedPeriod      string   `json:"expected_period,omitempty"`
	Rows                uint64   `json:"rows"`
	ValidRows           uint64   `json:"valid_rows"`
	InvalidRows         uint64   `json:"invalid_rows"`
	CompressedBytes     uint64   `json:"compressed_bytes,omitempty"`
	CompressedSHA256    string   `json:"compressed_sha256,omitempty"`
	UncompressedBytes   uint64   `json:"uncompressed_bytes,omitempty"`
	UncompressedSHA256  string   `json:"uncompressed_sha256,omitempty"`
	ElapsedMilliseconds int64    `json:"elapsed_milliseconds"`
	Checks              []Check  `json:"checks"`
	Issues              []string `json:"issues,omitempty"`
}

// VerifyError reports that the stream was readable but failed one or more
// source-contract checks.
type VerifyError struct {
	FailedChecks int
	InvalidRows  uint64
}

func (e *VerifyError) Error() string {
	return fmt.Sprintf("Schedule A verification failed: %d failed checks, %d invalid rows", e.FailedChecks, e.InvalidRows)
}

// VerifyZstd streams one zstd-compressed COPY extract. It bounds decompressor
// memory, validates every consumed row, and computes both physical digests on
// a complete pass.
func VerifyZstd(ctx context.Context, compressed io.Reader, options VerifyOptions) (Verification, error) {
	return VerifyZstdRows(ctx, compressed, options, nil)
}

// VerifyZstdRows adds a synchronous observer of source-valid rows. Row storage
// is borrowed until the callback returns. Observations are provisional: callers
// must discard them unless verification succeeds and Complete is true. This
// verifies physical occurrences, not publisher-key uniqueness or amendments.
func VerifyZstdRows(ctx context.Context, compressed io.Reader, options VerifyOptions, observe func(*Row) error) (Verification, error) {
	started := time.Now()
	result := Verification{
		SchemaVersion:  verificationSchemaVersion,
		ExpectedPeriod: options.ExpectedPeriod,
		Checks:         make([]Check, 0, 7),
	}
	if err := ctx.Err(); err != nil {
		return finishVerification(result, started), err
	}

	compressedCounter := &countingHashReader{reader: compressed, hash: sha256.New()}
	decoder, err := zstd.NewReader(
		compressedCounter,
		zstd.WithDecoderConcurrency(2),
		zstd.WithDecoderLowmem(true),
		zstd.WithDecoderMaxMemory(decoderMemoryLimit),
	)
	if err != nil {
		return finishVerification(result, started), fmt.Errorf("open zstd stream: %w", err)
	}
	defer decoder.Close()

	uncompressedCounter := &countingHashReader{reader: decoder, hash: sha256.New()}
	copyDecoder := NewDecoder(uncompressedCounter)
	partial := false
	for copyDecoder.Scan() {
		result.Rows++
		if err := Validate(copyDecoder.Row(), options.ExpectedPeriod); err != nil {
			result.InvalidRows++
			if len(result.Issues) < maxReportedIssues {
				result.Issues = append(result.Issues, err.Error())
			}
		} else {
			result.ValidRows++
			if observe != nil {
				if err := observe(copyDecoder.Row()); err != nil {
					return finishVerification(result, started), err
				}
			}
		}

		if result.Rows&0x3fff == 0 {
			select {
			case <-ctx.Done():
				return finishVerification(result, started), ctx.Err()
			default:
			}
		}
		if options.MaxRows > 0 && result.Rows >= options.MaxRows {
			partial = true
			break
		}
	}
	if err := copyDecoder.Err(); err != nil {
		return finishVerification(result, started), fmt.Errorf("read COPY stream: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return finishVerification(result, started), err
	}

	result.Complete = !partial
	if result.Complete {
		result.CompressedBytes = compressedCounter.bytes
		result.CompressedSHA256 = hex.EncodeToString(compressedCounter.hash.Sum(nil))
		result.UncompressedBytes = uncompressedCounter.bytes
		result.UncompressedSHA256 = hex.EncodeToString(uncompressedCounter.hash.Sum(nil))
	}

	result.Checks = append(result.Checks, Check{
		ID:       "row_validity",
		Passed:   result.InvalidRows == 0,
		Expected: "0 invalid rows",
		Actual:   fmt.Sprintf("%d invalid rows", result.InvalidRows),
	})
	if options.ExpectedRows > 0 && result.Complete {
		result.Checks = append(result.Checks, equalityCheck("row_count", options.ExpectedRows, result.Rows))
	}
	if options.ExpectedUncompressedBytes > 0 && result.Complete {
		result.Checks = append(result.Checks, equalityCheck("uncompressed_byte_count", options.ExpectedUncompressedBytes, result.UncompressedBytes))
	}
	if options.ExpectedUncompressedSHA256 != "" && result.Complete {
		result.Checks = append(result.Checks, stringCheck("uncompressed_sha256", options.ExpectedUncompressedSHA256, result.UncompressedSHA256))
	}
	if options.ExpectedCompressedBytes > 0 && result.Complete {
		result.Checks = append(result.Checks, equalityCheck("compressed_byte_count", options.ExpectedCompressedBytes, result.CompressedBytes))
	}
	if options.ExpectedCompressedSHA256 != "" && result.Complete {
		result.Checks = append(result.Checks, stringCheck("compressed_sha256", options.ExpectedCompressedSHA256, result.CompressedSHA256))
	}

	result = finishVerification(result, started)
	failed := 0
	for _, check := range result.Checks {
		if !check.Passed {
			failed++
		}
	}
	if failed > 0 {
		return result, &VerifyError{FailedChecks: failed, InvalidRows: result.InvalidRows}
	}
	return result, nil
}

func finishVerification(result Verification, started time.Time) Verification {
	result.ElapsedMilliseconds = time.Since(started).Milliseconds()
	return result
}

func equalityCheck(id string, expected, actual uint64) Check {
	return Check{
		ID:       id,
		Passed:   expected == actual,
		Expected: fmt.Sprintf("%d", expected),
		Actual:   fmt.Sprintf("%d", actual),
	}
}

func stringCheck(id, expected, actual string) Check {
	return Check{ID: id, Passed: expected == actual, Expected: expected, Actual: actual}
}

type countingHashReader struct {
	reader io.Reader
	hash   hash.Hash
	bytes  uint64
}

func (r *countingHashReader) Read(destination []byte) (int, error) {
	read, err := r.reader.Read(destination)
	if read > 0 {
		r.bytes += uint64(read)
		_, _ = r.hash.Write(destination[:read])
	}
	return read, err
}

// IsVerifyError reports whether an error represents completed contract checks
// rather than a transport, decompression, or cancellation failure.
func IsVerifyError(err error) bool {
	var verificationError *VerifyError
	return errors.As(err, &verificationError)
}
