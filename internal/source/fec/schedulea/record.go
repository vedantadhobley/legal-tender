// Record validation maps decoded COPY values to the source contract.
package schedulea

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"unicode/utf8"
)

var (
	// ErrNullRequired marks SQL null in a NOT NULL source column.
	ErrNullRequired = errors.New("required source value is null")
	// ErrInvalidLexeme marks a value outside its contracted source syntax.
	ErrInvalidLexeme = errors.New("invalid source lexeme")
	// ErrPeriodMismatch marks a row outside the selected physical partition.
	ErrPeriodMismatch = errors.New("transaction period does not match partition")
)

// Value is an owned, exact decoded source lexeme. Null remains distinct from
// empty text, false, zero, and negative values.
type Value struct {
	Lexeme string
	Null   bool
}

// Record owns all 81 source values and can outlive the streaming decoder row.
type Record struct {
	Values [FieldCount]Value
}

// ValueByName resolves an owned source value by contracted column name.
func (r Record) ValueByName(name string) (Value, bool) {
	index, ok := ColumnIndex(name)
	if !ok {
		return Value{}, false
	}
	return r.Values[index], true
}

// CanonicalMap returns the contract's JSON representation: null remains null,
// the PostgreSQL boolean lexeme becomes a JSON boolean, and all other values
// remain exact strings.
func (r Record) CanonicalMap() map[string]any {
	result := make(map[string]any, FieldCount)
	for index, column := range columns {
		value := r.Values[index]
		switch {
		case value.Null:
			result[column.Name] = nil
		case column.Kind == KindBoolean:
			result[column.Name] = value.Lexeme == "t"
		default:
			result[column.Name] = value.Lexeme
		}
	}
	return result
}

// CanonicalJSON emits the exact record-schema object in relation-column order.
// It avoids allocating and sorting an 81-entry map for every large-corpus fact.
func (r Record) CanonicalJSON() json.RawMessage {
	result := make([]byte, 0, 2048)
	result = append(result, '{')
	for index, column := range columns {
		if index > 0 {
			result = append(result, ',')
		}
		result = appendJSONString(result, column.Name)
		result = append(result, ':')
		value := r.Values[index]
		switch {
		case value.Null:
			result = append(result, "null"...)
		case column.Kind == KindBoolean:
			if value.Lexeme == "t" {
				result = append(result, "true"...)
			} else {
				result = append(result, "false"...)
			}
		default:
			result = appendJSONString(result, value.Lexeme)
		}
	}
	result = append(result, '}')
	return result
}

func appendJSONString(destination []byte, value string) []byte {
	const hexadecimal = "0123456789abcdef"
	destination = append(destination, '"')
	start := 0
	for index := 0; index < len(value); index++ {
		character := value[index]
		if character >= 0x20 && character != '"' && character != '\\' {
			continue
		}
		destination = append(destination, value[start:index]...)
		switch character {
		case '"', '\\':
			destination = append(destination, '\\', character)
		case '\b':
			destination = append(destination, '\\', 'b')
		case '\f':
			destination = append(destination, '\\', 'f')
		case '\n':
			destination = append(destination, '\\', 'n')
		case '\r':
			destination = append(destination, '\\', 'r')
		case '\t':
			destination = append(destination, '\\', 't')
		default:
			destination = append(destination, '\\', 'u', '0', '0', hexadecimal[character>>4], hexadecimal[character&0x0f])
		}
		start = index + 1
	}
	destination = append(destination, value[start:]...)
	destination = append(destination, '"')
	return destination
}

// Validate checks physical parsing, source lexemes, required fields, and an
// optional selected two-year transaction period.
func Validate(row *Row, expectedPeriod string) error {
	if row == nil {
		return errors.New("nil Schedule A row")
	}
	if row.Issue() != nil {
		return row.Issue()
	}
	if row.FieldCount() != FieldCount {
		return fmt.Errorf("%w: row %d has %d fields; want %d", ErrFieldCount, row.Number(), row.FieldCount(), FieldCount)
	}

	for index, column := range columns {
		field, _ := row.Field(index)
		if field.IsNull() {
			if !column.Nullable {
				return fieldError(row, index, ErrNullRequired)
			}
			continue
		}
		value := field.Bytes()
		if !utf8.Valid(value) || bytes.IndexByte(value, 0) >= 0 {
			return fieldError(row, index, fmt.Errorf("%w: value is not valid PostgreSQL UTF-8 text", ErrInvalidLexeme))
		}
		if !validKind(column.Kind, value) {
			return fieldError(row, index, fmt.Errorf("%w for %s", ErrInvalidLexeme, kindName(column.Kind)))
		}
	}

	subID, _ := row.Field(subIDIndex)
	if len(subID.Bytes()) == 0 || subID.Bytes()[0] == '-' {
		return fieldError(row, subIDIndex, fmt.Errorf("%w: sub_id must contain only decimal digits", ErrInvalidLexeme))
	}
	filingForm, _ := row.Field(filingFormIndex)
	if len(filingForm.Bytes()) == 0 {
		return fieldError(row, filingFormIndex, fmt.Errorf("%w: filing_form must not be empty", ErrInvalidLexeme))
	}

	if expectedPeriod != "" {
		period, _ := row.Field(twoYearTransactionPeriodIndex)
		if period.IsNull() || !bytes.Equal(period.Bytes(), []byte(expectedPeriod)) {
			return fieldError(row, twoYearTransactionPeriodIndex, fmt.Errorf("%w: got %q; want %q", ErrPeriodMismatch, period.String(), expectedPeriod))
		}
	}
	return nil
}

// Freeze validates a row and copies its exact decoded lexemes into an owned
// record. expectedPeriod may be empty when no partition constraint applies.
func Freeze(row *Row, expectedPeriod string) (Record, error) {
	if err := Validate(row, expectedPeriod); err != nil {
		return Record{}, err
	}

	var record Record
	for index := range record.Values {
		field, _ := row.Field(index)
		record.Values[index] = Value{Null: field.IsNull()}
		if !field.IsNull() {
			record.Values[index].Lexeme = string(field.Bytes())
		}
	}
	return record, nil
}

func fieldError(row *Row, index int, err error) error {
	return fmt.Errorf("row %d field %d (%s): %w", row.Number(), index+1, columns[index].Name, err)
}

func validKind(kind Kind, value []byte) bool {
	switch kind {
	case KindText:
		return true
	case KindDecimal:
		return validDecimal(value)
	case KindInteger:
		return validInteger(value)
	case KindTimestamp:
		return validTimestamp(value)
	case KindBoolean:
		return len(value) == 1 && (value[0] == 't' || value[0] == 'f')
	default:
		return false
	}
}

func kindName(kind Kind) string {
	switch kind {
	case KindText:
		return "text"
	case KindDecimal:
		return "decimal"
	case KindInteger:
		return "integer"
	case KindTimestamp:
		return "timestamp"
	case KindBoolean:
		return "boolean"
	default:
		return "unknown"
	}
}

func validInteger(value []byte) bool {
	if len(value) == 0 {
		return false
	}
	start := 0
	if value[0] == '-' {
		start = 1
		if len(value) == 1 {
			return false
		}
	}
	for _, character := range value[start:] {
		if character < '0' || character > '9' {
			return false
		}
	}
	return true
}

func validDecimal(value []byte) bool {
	if len(value) == 0 {
		return false
	}
	start := 0
	if value[0] == '-' {
		start = 1
		if len(value) == 1 {
			return false
		}
	}

	dot := -1
	for index, character := range value[start:] {
		switch {
		case character >= '0' && character <= '9':
		case character == '.' && dot == -1:
			dot = start + index
		default:
			return false
		}
	}
	if dot == start || dot == len(value)-1 {
		return false
	}
	integerEnd := len(value)
	if dot >= 0 {
		integerEnd = dot
	}
	if integerEnd-start > 1 && value[start] == '0' {
		return false
	}
	return true
}

func validTimestamp(value []byte) bool {
	if len(value) < len("2006-01-02 15:04:05") {
		return false
	}
	for index, expected := range []byte("0000-00-00 00:00:00") {
		switch expected {
		case '-', ' ', ':':
			if value[index] != expected {
				return false
			}
		default:
			if value[index] < '0' || value[index] > '9' {
				return false
			}
		}
	}
	if len(value) == len("2006-01-02 15:04:05") {
		return true
	}
	if value[len("2006-01-02 15:04:05")] != '.' || len(value) == len("2006-01-02 15:04:05")+1 {
		return false
	}
	for _, character := range value[len("2006-01-02 15:04:05")+1:] {
		if character < '0' || character > '9' {
			return false
		}
	}
	return true
}
