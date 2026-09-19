package scheduleb

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"time"
	"unicode/utf8"
)

var (
	ErrNullRequired   = errors.New("required source value is null")
	ErrInvalidLexeme  = errors.New("invalid source lexeme")
	ErrPeriodMismatch = errors.New("transaction period does not match selection")
)

// Value is an owned, exact decoded source lexeme.
type Value struct {
	Lexeme string
	Null   bool
}

// Record owns all source values and can outlive the streaming decoder row.
type Record struct{ Values [FieldCount]Value }

func (r Record) ValueByName(name string) (Value, bool) {
	index, ok := ColumnIndex(name)
	if !ok {
		return Value{}, false
	}
	return r.Values[index], true
}

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
	return append(result, '}')
}

func appendJSONString(destination []byte, value string) []byte {
	encoded, _ := json.Marshal(value)
	return append(destination, encoded...)
}

// Validate checks exact field width, source lexemes, required fields, and an
// optional selected two-year transaction period.
func Validate(row *Row, expectedPeriod string) error {
	if row == nil {
		return errors.New("nil Schedule B row")
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
	positive := false
	for _, digit := range subID.Bytes() {
		if digit != '0' {
			positive = true
			break
		}
	}
	if len(subID.Bytes()) == 0 || subID.Bytes()[0] == '-' || !positive {
		return fieldError(row, subIDIndex, fmt.Errorf("%w: sub_id must be a positive decimal integer", ErrInvalidLexeme))
	}
	filingForm, _ := row.Field(filingFormIndex)
	if len(filingForm.Bytes()) == 0 {
		return fieldError(row, filingFormIndex, fmt.Errorf("%w: filing_form must not be empty", ErrInvalidLexeme))
	}
	if expectedPeriod != "" {
		period, _ := row.Field(transactionPeriodIndex)
		if period.IsNull() || !bytes.Equal(period.Bytes(), []byte(expectedPeriod)) {
			return fieldError(row, transactionPeriodIndex, fmt.Errorf("%w: got %q; want %q", ErrPeriodMismatch, period.String(), expectedPeriod))
		}
	}
	return nil
}

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
	if digits == 0 || digits > precision || fractional > scale || requireScale && fractional != scale {
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
