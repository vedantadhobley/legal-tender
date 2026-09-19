// Record validation maps decoded COPY values to the Schedule E contract.
package schedulee

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"time"
	"unicode/utf8"
)

var (
	// ErrNullRequired marks SQL null in a NOT NULL source column.
	ErrNullRequired = errors.New("required source value is null")
	// ErrInvalidLexeme marks a value outside its contracted source syntax.
	ErrInvalidLexeme = errors.New("invalid source lexeme")
	// ErrCycleMismatch marks a row outside an explicitly selected cycle.
	ErrCycleMismatch = errors.New("election cycle does not match selection")
)

// Value is an owned, exact decoded source lexeme. Null remains distinct from
// empty text and zero.
type Value struct {
	Lexeme string
	Null   bool
}

// Record owns all 80 source values and can outlive the streaming decoder row.
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

// CanonicalMap returns the record-schema representation. PostgreSQL null
// remains null and every non-null source lexeme remains an exact string.
func (r Record) CanonicalMap() map[string]any {
	result := make(map[string]any, FieldCount)
	for index, column := range columns {
		value := r.Values[index]
		if value.Null {
			result[column.Name] = nil
		} else {
			result[column.Name] = value.Lexeme
		}
	}
	return result
}

// CanonicalJSON emits the exact record-schema object in relation-column order.
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
		if value.Null {
			result = append(result, "null"...)
		} else {
			result = appendJSONString(result, value.Lexeme)
		}
	}
	result = append(result, '}')
	return result
}

func appendJSONString(destination []byte, value string) []byte {
	encoded, _ := json.Marshal(value)
	return append(destination, encoded...)
}

// Validate checks physical parsing, exact source lexemes, required fields, and
// an optional selected election cycle.
func Validate(row *Row, expectedCycle string) error {
	if row == nil {
		return errors.New("nil Schedule E row")
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
		if !validColumn(column, value) {
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

	if expectedCycle != "" {
		cycle, _ := row.Field(electionCycleIndex)
		if cycle.IsNull() || !bytes.Equal(cycle.Bytes(), []byte(expectedCycle)) {
			return fieldError(row, electionCycleIndex, fmt.Errorf("%w: got %q; want %q", ErrCycleMismatch, cycle.String(), expectedCycle))
		}
	}
	return nil
}

// Freeze validates a row and copies its exact decoded lexemes into an owned
// record. expectedCycle may be empty for the all-history relation.
func Freeze(row *Row, expectedCycle string) (Record, error) {
	if err := Validate(row, expectedCycle); err != nil {
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

func validColumn(column Column, value []byte) bool {
	switch column.Kind {
	case KindText:
		return true
	case KindDecimal:
		return validNumeric(value, column.Precision, column.Scale, true)
	case KindInteger:
		return validNumeric(value, column.Precision, 0, false)
	case KindTimestamp:
		_, err := time.Parse("2006-01-02 15:04:05.999999999", string(value))
		return err == nil
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
	default:
		return "unknown"
	}
}

func validNumeric(value []byte, precision, scale int, requireScale bool) bool {
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
	digits := 0
	fractional := 0
	for index, character := range value[start:] {
		switch {
		case character >= '0' && character <= '9':
			digits++
			if dot >= 0 {
				fractional++
			}
		case character == '.' && dot == -1 && scale > 0:
			dot = start + index
		default:
			return false
		}
	}
	if digits == 0 || digits > precision || fractional > scale {
		return false
	}
	if requireScale && fractional != scale {
		return false
	}
	if dot == start || dot == len(value)-1 {
		return false
	}
	integerEnd := len(value)
	if dot >= 0 {
		integerEnd = dot
	}
	return integerEnd-start <= 1 || value[start] != '0'
}
