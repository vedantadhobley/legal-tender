package classic

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"unicode/utf8"
)

type ValidationIssue struct {
	Code    string
	Message string
}

type Row struct {
	number uint64
	raw    []byte
	fields [][]byte
	lineLF bool
}

func (row *Row) Number() uint64 { return row.number }
func (row *Row) Raw() []byte    { return row.raw }

func (row *Row) NaturalKeyValue(spec Spec) (string, bool) {
	if spec.NaturalKey < 0 || spec.NaturalKey >= len(row.fields) {
		return "", false
	}
	value := string(row.fields[spec.NaturalKey])
	return value, value != ""
}

func (row *Row) Values(spec Spec) ([]string, bool) {
	if len(row.fields) != len(spec.Fields) {
		return nil, false
	}
	values := make([]string, len(row.fields))
	for index := range row.fields {
		values[index] = string(row.fields[index])
	}
	return values, true
}

func (row *Row) CanonicalMap(spec Spec) (map[string]string, bool) {
	values, ok := row.Values(spec)
	if !ok {
		return nil, false
	}
	result := make(map[string]string, len(values))
	for index, value := range values {
		result[spec.Fields[index]] = value
	}
	return result, true
}

func (row *Row) Validate(spec Spec, cycle string) []ValidationIssue {
	issues := make([]ValidationIssue, 0, 2)
	if !row.lineLF {
		issues = append(issues, ValidationIssue{Code: "missing_line_feed", Message: fmt.Sprintf("row %d is not LF terminated", row.number)})
	}
	if len(row.raw) == 1 && row.raw[0] == '\n' || len(row.raw) == 0 {
		issues = append(issues, ValidationIssue{Code: "empty_record", Message: fmt.Sprintf("row %d is empty", row.number)})
	}
	if !utf8.Valid(row.raw) || bytes.IndexByte(row.raw, 0) >= 0 {
		issues = append(issues, ValidationIssue{Code: "invalid_utf8", Message: fmt.Sprintf("row %d is not valid NUL-free UTF-8", row.number)})
	}
	if len(row.fields) != len(spec.Fields) {
		issues = append(issues, ValidationIssue{Code: "field_count", Message: fmt.Sprintf("row %d has %d fields; want %d", row.number, len(row.fields), len(spec.Fields))})
		return issues
	}
	values, _ := row.Values(spec)
	return append(issues, spec.validateFields(values, cycle)...)
}

type Decoder struct {
	reader *bufio.Reader
	row    Row
	err    error
	done   bool
}

func NewDecoder(reader io.Reader) *Decoder {
	return &Decoder{reader: bufio.NewReaderSize(reader, 256<<10)}
}

func (decoder *Decoder) Scan() bool {
	if decoder.done || decoder.err != nil {
		return false
	}
	raw, err := decoder.reader.ReadBytes('\n')
	if len(raw) == 0 {
		decoder.done = true
		if err != nil && !errors.Is(err, io.EOF) {
			decoder.err = err
		}
		return false
	}
	decoder.row.number++
	decoder.row.raw = raw
	decoder.row.lineLF = raw[len(raw)-1] == '\n'
	content := raw
	if decoder.row.lineLF {
		content = content[:len(content)-1]
	}
	decoder.row.fields = bytes.Split(content, []byte{'|'})
	if err != nil {
		decoder.done = true
		if !errors.Is(err, io.EOF) {
			decoder.err = err
		}
	}
	return true
}

func (decoder *Decoder) Row() *Row  { return &decoder.row }
func (decoder *Decoder) Err() error { return decoder.err }
