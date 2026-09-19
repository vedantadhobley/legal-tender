package committeesummary

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/csv"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"unicode/utf8"
)

// Expected binds already-captured bytes. Acquisition and release publication
// are outside this reader. Bytes and SHA256 must not be learned from this scan.
type Expected struct {
	Cycle  string `json:"cycle"`
	Bytes  int64  `json:"bytes"`
	SHA256 string `json:"sha256"`
}

type Reader struct {
	raw      []byte
	csv      *csv.Reader
	expected Expected
	ordinal  uint64
	failure  error
}

// PhysicalError identifies retained evidence without logging source values.
// The original artifact survives; no successful verification is emitted.
type PhysicalError struct {
	Code      string
	Ordinal   uint64
	Offset    int64
	Length    int64
	RawSHA256 string
}

func (e *PhysicalError) Error() string {
	return fmt.Sprintf("committee summary %s at record %d, byte %d, length %d (sha256 %s)", e.Code, e.Ordinal, e.Offset, e.Length, e.RawSHA256)
}

func digest(raw []byte) string {
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

func ValidateExpected(expected Expected) error {
	year, ok := cycleYear(expected.Cycle)
	if !ok || year%2 != 0 || expected.Bytes <= 0 || expected.Bytes > MaxArtifactBytes {
		return errors.New("require an even four-digit cycle and expected bytes within the 16 MiB limit")
	}
	decoded, err := hex.DecodeString(expected.SHA256)
	if err != nil || len(decoded) != sha256.Size || strings.ToLower(expected.SHA256) != expected.SHA256 {
		return errors.New("require the captured artifact's lowercase SHA-256")
	}
	return nil
}

// NewReader owns a bounded copy of the small summary artifact. Hashing and
// size verification precede record delivery. It never closes the caller's input.
// Only the observed LF format is accepted, so encoding/csv cannot silently
// normalize CRLF inside a field. A changed physical format needs review.
func NewReader(ctx context.Context, input io.Reader, expected Expected) (*Reader, error) {
	if err := ValidateExpected(expected); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	raw, err := io.ReadAll(io.LimitReader(contextReader{ctx, input}, expected.Bytes+1))
	if err != nil {
		return nil, fmt.Errorf("read committee summary: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if int64(len(raw)) != expected.Bytes || digest(raw) != expected.SHA256 {
		return nil, errors.New("committee summary artifact size or SHA-256 mismatch")
	}
	if !utf8.Valid(raw) {
		return nil, errors.New("committee summary invalid UTF-8; raw artifact retained")
	}
	if bytes.ContainsRune(raw, '\r') {
		return nil, errors.New("committee summary unreviewed CR encoding; require LF without CR")
	}
	if raw[len(raw)-1] != '\n' {
		return nil, errors.New("committee summary missing final LF")
	}
	r := &Reader{raw: raw, csv: csv.NewReader(bytes.NewReader(raw)), expected: expected}
	r.csv.FieldsPerRecord = len(columns)
	header, err := r.csv.Read()
	if err != nil || !slices.Equal(header, Fields()) || digest(raw[:r.csv.InputOffset()]) != HeaderSHA256 {
		return nil, errors.New("committee summary header does not match pinned schema")
	}
	return r, nil
}

// Next returns every well-framed row, including invalid typed values and
// repeated identities. A physical error is sticky; callers cannot resume past it.
// Record maps and values are owned by the record and may be retained by callers.
func (r *Reader) Next(ctx context.Context) (*Record, error) {
	if r.failure != nil {
		return nil, r.failure
	}
	if err := ctx.Err(); err != nil {
		r.failure = err
		return nil, err
	}
	start := r.csv.InputOffset()
	if start == int64(len(r.raw)) {
		return nil, io.EOF
	}
	if r.ordinal >= MaxRows {
		return nil, r.fail("row_limit", start, start)
	}
	// encoding/csv intentionally skips blank lines. Reject before Read so it
	// cannot silently consume a blank occurrence along with the following row.
	if r.raw[start] == '\n' {
		return nil, r.fail("blank_record", start, start+1)
	}
	fields, err := r.csv.Read()
	end := r.csv.InputOffset()
	if err != nil {
		return nil, r.fail("invalid_csv_or_width", start, end)
	}
	if end-start > MaxRecordBytes {
		return nil, r.fail("record_limit", start, end)
	}
	if err := ctx.Err(); err != nil {
		r.failure = err
		return nil, err
	}
	r.ordinal++
	row := &Record{
		Ordinal: r.ordinal, Offset: start, Length: end - start, RawSHA256: digest(r.raw[start:end]),
		SourceFields: make(map[string]string, len(columns)),
		Money:        make(map[string]MoneyValue, len(moneyFields)), Dates: make(map[string]Value, len(dateFields)),
		Identifiers: make(map[string]Value, len(identityFields)), Issues: []Issue{},
	}
	for i, c := range columns {
		row.SourceFields[c.name] = fields[i]
	}
	row.normalize(r.expected.Cycle)
	return row, nil
}

func (r *Reader) fail(code string, start, end int64) error {
	r.failure = &PhysicalError{Code: code, Ordinal: r.ordinal + 1, Offset: start, Length: end - start, RawSHA256: digest(r.raw[start:end])}
	return r.failure
}

type contextReader struct {
	ctx    context.Context
	reader io.Reader
}

func (r contextReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.reader.Read(p)
}
