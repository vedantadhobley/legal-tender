// Package copytext decodes PostgreSQL COPY text rows without normalizing
// source values.
package copytext

import (
	"bufio"
	"errors"
	"fmt"
	"io"
)

const copyBufferSize = 256 * 1024

var (
	// ErrFieldCount marks a physical row whose width differs from the contract.
	ErrFieldCount = errors.New("unexpected COPY field count")
	// ErrInvalidEscape marks an incomplete or invalid COPY text escape.
	ErrInvalidEscape = errors.New("invalid COPY escape")
	// ErrMissingLineFeed marks a final physical row without its required LF.
	ErrMissingLineFeed = errors.New("COPY row is not LF terminated")
)

// Field is one decoded COPY value. Bytes remain valid until the decoder's next
// Scan call. Null is distinct from empty text.
type Field struct {
	bytes  []byte
	isNull bool
}

// Bytes returns the decoded source lexeme. It returns nil for SQL null.
func (f Field) Bytes() []byte {
	if f.isNull {
		return nil
	}
	return f.bytes
}

// String returns the decoded source lexeme. Call IsNull to distinguish null
// from an empty string.
func (f Field) String() string { return string(f.Bytes()) }

// IsNull reports whether the physical field was the COPY null marker, \N.
func (f Field) IsNull() bool { return f.isNull }

// Row is one physical source occurrence. Its slices remain valid until the
// decoder's next Scan call.
type Row struct {
	number uint64
	raw    []byte
	fields []Field
	issue  error
}

// Number is the one-based physical row ordinal in this stream.
func (r *Row) Number() uint64 { return r.number }

// Raw returns the exact physical bytes, including the terminating LF when it
// was present. The bytes remain valid until the next Scan call.
func (r *Row) Raw() []byte { return r.raw }

// FieldCount reports the width observed before contract validation.
func (r *Row) FieldCount() int { return len(r.fields) }

// Field returns one decoded field by zero-based relation index.
func (r *Row) Field(index int) (Field, bool) {
	if index < 0 || index >= len(r.fields) {
		return Field{}, false
	}
	return r.fields[index], true
}

// Issue reports a physical COPY framing, width, or escape failure. The row is
// still yielded so callers can conserve it as explicit issue evidence.
func (r *Row) Issue() error { return r.issue }

// Decoder streams PostgreSQL COPY text rows without imposing Scanner's token
// limit or retaining prior rows in memory.
type Decoder struct {
	reader         *bufio.Reader
	expectedFields int
	row            Row
	raw            []byte
	decoded        []byte
	err            error
	done           bool
}

// NewDecoder creates a decoder for data-row-only COPY text with tab fields and
// LF row endings.
func NewDecoder(reader io.Reader, expectedFields int) *Decoder {
	return &Decoder{
		reader:         bufio.NewReaderSize(reader, copyBufferSize),
		expectedFields: expectedFields,
		row:            Row{fields: make([]Field, 0, expectedFields)},
	}
}

// Scan consumes and exposes the next physical row. It returns true for a
// malformed row so the caller can preserve Row.Issue and continue.
func (d *Decoder) Scan() bool {
	if d.done || d.err != nil {
		return false
	}

	physical, readErr := d.readPhysicalRow()
	if errors.Is(readErr, io.EOF) && len(physical) == 0 {
		d.done = true
		return false
	}
	if readErr != nil && !errors.Is(readErr, io.EOF) {
		d.err = readErr
		return false
	}

	d.row.number++
	d.row.raw = physical
	d.row.fields = d.row.fields[:0]
	d.row.issue = nil
	d.decoded = d.decoded[:0]

	terminated := len(physical) > 0 && physical[len(physical)-1] == '\n'
	data := physical
	if terminated {
		data = physical[:len(physical)-1]
	} else {
		d.row.issue = ErrMissingLineFeed
		d.done = true
	}

	if err := d.parseFields(data); err != nil {
		d.row.issue = errors.Join(d.row.issue, err)
	}
	return true
}

// Row returns the current physical row. Its contents expire on the next Scan.
func (d *Decoder) Row() *Row { return &d.row }

// Err returns a terminal stream read error. Row-level parse issues are exposed
// through Row.Issue instead.
func (d *Decoder) Err() error { return d.err }

func (d *Decoder) readPhysicalRow() ([]byte, error) {
	d.raw = d.raw[:0]
	for {
		fragment, err := d.reader.ReadSlice('\n')
		if len(d.raw) == 0 && !errors.Is(err, bufio.ErrBufferFull) {
			return fragment, err
		}
		d.raw = append(d.raw, fragment...)
		if errors.Is(err, bufio.ErrBufferFull) {
			continue
		}
		return d.raw, err
	}
}

func (d *Decoder) parseFields(data []byte) error {
	if cap(d.decoded) < len(data) {
		d.decoded = make([]byte, 0, len(data))
	}

	fieldStart := 0
	for index := 0; index < len(data); index++ {
		switch data[index] {
		case '\\':
			if index+1 < len(data) {
				index++
			}
		case '\t':
			if err := d.appendField(data[fieldStart:index]); err != nil {
				return err
			}
			fieldStart = index + 1
		}
	}
	if err := d.appendField(data[fieldStart:]); err != nil {
		return err
	}
	if len(d.row.fields) != d.expectedFields {
		return fmt.Errorf("%w: row %d has %d fields; want %d", ErrFieldCount, d.row.number, len(d.row.fields), d.expectedFields)
	}
	return nil
}

func (d *Decoder) appendField(raw []byte) error {
	fieldIndex := len(d.row.fields)
	if fieldIndex >= d.expectedFields {
		d.row.fields = append(d.row.fields, Field{})
		return nil
	}

	if len(raw) == 2 && raw[0] == '\\' && raw[1] == 'N' {
		d.row.fields = append(d.row.fields, Field{isNull: true})
		return nil
	}
	if !containsBackslash(raw) {
		d.row.fields = append(d.row.fields, Field{bytes: raw})
		return nil
	}

	start := len(d.decoded)
	for index := 0; index < len(raw); index++ {
		if raw[index] != '\\' {
			d.decoded = append(d.decoded, raw[index])
			continue
		}
		index++
		if index >= len(raw) {
			return fmt.Errorf("%w at row %d field %d", ErrInvalidEscape, d.row.number, fieldIndex+1)
		}

		switch escaped := raw[index]; escaped {
		case 'b':
			d.decoded = append(d.decoded, '\b')
		case 'f':
			d.decoded = append(d.decoded, '\f')
		case 'n':
			d.decoded = append(d.decoded, '\n')
		case 'r':
			d.decoded = append(d.decoded, '\r')
		case 't':
			d.decoded = append(d.decoded, '\t')
		case 'v':
			d.decoded = append(d.decoded, '\v')
		case 'x':
			value, consumed, ok := parseHex(raw[index+1:])
			if !ok {
				return fmt.Errorf("%w at row %d field %d: hex escape has no digits", ErrInvalidEscape, d.row.number, fieldIndex+1)
			}
			d.decoded = append(d.decoded, value)
			index += consumed
		case '0', '1', '2', '3', '4', '5', '6', '7':
			value, consumed := parseOctal(raw[index:])
			d.decoded = append(d.decoded, value)
			index += consumed - 1
		default:
			d.decoded = append(d.decoded, escaped)
		}
	}

	d.row.fields = append(d.row.fields, Field{bytes: d.decoded[start:]})
	return nil
}

func containsBackslash(value []byte) bool {
	for _, character := range value {
		if character == '\\' {
			return true
		}
	}
	return false
}

func parseHex(value []byte) (byte, int, bool) {
	var decoded byte
	consumed := 0
	for consumed < len(value) && consumed < 2 {
		digit, ok := hexDigit(value[consumed])
		if !ok {
			break
		}
		decoded = decoded*16 + digit
		consumed++
	}
	return decoded, consumed, consumed > 0
}

func hexDigit(value byte) (byte, bool) {
	switch {
	case value >= '0' && value <= '9':
		return value - '0', true
	case value >= 'a' && value <= 'f':
		return value - 'a' + 10, true
	case value >= 'A' && value <= 'F':
		return value - 'A' + 10, true
	default:
		return 0, false
	}
}

func parseOctal(value []byte) (byte, int) {
	var decoded byte
	consumed := 0
	for consumed < len(value) && consumed < 3 {
		character := value[consumed]
		if character < '0' || character > '7' {
			break
		}
		decoded = decoded*8 + character - '0'
		consumed++
	}
	return decoded, consumed
}
