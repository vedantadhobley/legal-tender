// COPY decoder tests cover exact archive fixtures and malformed row evidence.
package schedulea

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"
)

func TestExactCOPYFixturesMatchCanonicalRecords(t *testing.T) {
	t.Parallel()
	fixtureDir := fixtureDirectory(t)
	copyFixtures, err := filepath.Glob(filepath.Join(fixtureDir, "*.copy"))
	if err != nil {
		t.Fatalf("glob COPY fixtures: %v", err)
	}
	if len(copyFixtures) != 8 {
		t.Fatalf("found %d COPY fixtures; want 8", len(copyFixtures))
	}

	for _, copyPath := range copyFixtures {
		copyPath := copyPath
		t.Run(strings.TrimSuffix(filepath.Base(copyPath), ".copy"), func(t *testing.T) {
			t.Parallel()
			physical, err := os.ReadFile(copyPath)
			if err != nil {
				t.Fatalf("read COPY fixture: %v", err)
			}
			decoder := NewDecoder(bytes.NewReader(physical))
			if !decoder.Scan() {
				t.Fatalf("first Scan returned false: %v", decoder.Err())
			}
			row := decoder.Row()
			if row.Number() != 1 || row.FieldCount() != FieldCount {
				t.Fatalf("row identity = (%d, %d fields); want (1, %d fields)", row.Number(), row.FieldCount(), FieldCount)
			}
			if !bytes.Equal(row.Raw(), physical) {
				t.Fatal("row did not preserve exact physical bytes")
			}
			record, err := Freeze(row, "2026")
			if err != nil {
				t.Fatalf("freeze exact fixture: %v", err)
			}

			expectedBytes, err := os.ReadFile(strings.TrimSuffix(copyPath, ".copy") + ".json")
			if err != nil {
				t.Fatalf("read canonical fixture: %v", err)
			}
			var expected map[string]any
			if err := json.Unmarshal(expectedBytes, &expected); err != nil {
				t.Fatalf("decode canonical fixture: %v", err)
			}
			if actual := record.CanonicalMap(); !reflect.DeepEqual(actual, expected) {
				actualJSON, _ := json.MarshalIndent(actual, "", "  ")
				t.Fatalf("canonical record mismatch\nactual: %s\nexpected: %s", actualJSON, expectedBytes)
			}
			if decoder.Scan() {
				t.Fatal("fixture produced a second row")
			}
			if err := decoder.Err(); err != nil {
				t.Fatalf("decoder error after fixture: %v", err)
			}
		})
	}
}

func TestCOPYNullAndEmptyTextStayDistinct(t *testing.T) {
	t.Parallel()
	values := validRowValues()
	values[0] = `\N`
	values[1] = ""
	decoder := NewDecoder(strings.NewReader(strings.Join(values, "\t") + "\n"))
	if !decoder.Scan() {
		t.Fatalf("Scan returned false: %v", decoder.Err())
	}
	nullField, _ := decoder.Row().Field(0)
	emptyField, _ := decoder.Row().Field(1)
	if !nullField.IsNull() {
		t.Fatal("COPY \\N was not preserved as null")
	}
	if emptyField.IsNull() || emptyField.String() != "" {
		t.Fatal("empty text was not preserved separately from null")
	}
}

func TestCOPYEscapes(t *testing.T) {
	t.Parallel()
	values := validRowValues()
	values[0] = `line\ncol\tbackslash\\octal\101hex\x42unknown\q`
	decoder := NewDecoder(strings.NewReader(strings.Join(values, "\t") + "\n"))
	if !decoder.Scan() {
		t.Fatalf("Scan returned false: %v", decoder.Err())
	}
	field, _ := decoder.Row().Field(0)
	if got, want := field.String(), "line\ncol\tbackslash\\octalAhexBunknownq"; got != want {
		t.Fatalf("decoded escape value %q; want %q", got, want)
	}
}

func TestCanonicalJSONEscapesDecodedPostgreSQLControlBytes(t *testing.T) {
	t.Parallel()
	values := validRowValues()
	memoTextIndex, ok := ColumnIndex("memo_text")
	if !ok {
		t.Fatal("memo_text column is absent")
	}
	values[memoTextIndex] = `bell\x07vertical\x0bunit\x1f`
	decoder := NewDecoder(strings.NewReader(strings.Join(values, "\t") + "\n"))
	if !decoder.Scan() {
		t.Fatalf("Scan returned false: %v", decoder.Err())
	}
	record, err := Freeze(decoder.Row(), "2026")
	if err != nil {
		t.Fatal(err)
	}
	canonical := record.CanonicalJSON()
	if !json.Valid(canonical) {
		t.Fatalf("canonical record is not valid JSON: %q", canonical)
	}
	var decoded map[string]any
	if err := json.Unmarshal(canonical, &decoded); err != nil {
		t.Fatal(err)
	}
	if got, want := decoded["memo_text"], "bell\x07vertical\x0bunit\x1f"; got != want {
		t.Fatalf("decoded control text = %q; want %q", got, want)
	}
}

func TestMalformedRowsRemainObservable(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		input string
		want  error
	}{
		{name: "short width", input: "one\ttwo\n", want: ErrFieldCount},
		{name: "missing LF", input: strings.Join(validRowValues(), "\t"), want: ErrMissingLineFeed},
		{name: "trailing escape", input: "value\\\n", want: ErrInvalidEscape},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			decoder := NewDecoder(strings.NewReader(test.input))
			if !decoder.Scan() {
				t.Fatalf("malformed physical row was not yielded: %v", decoder.Err())
			}
			if !errors.Is(decoder.Row().Issue(), test.want) {
				t.Fatalf("row issue = %v; want %v", decoder.Row().Issue(), test.want)
			}
		})
	}
}

func TestSourceLexemeValidation(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		column string
		value  string
		period string
		want   error
	}{
		{name: "negative money accepted", column: "contb_receipt_amt", value: "-42.50", period: "2026"},
		{name: "bad money", column: "contb_receipt_amt", value: "$42", period: "2026", want: ErrInvalidLexeme},
		{name: "bad boolean", column: "is_individual", value: "true", period: "2026", want: ErrInvalidLexeme},
		{name: "negative sub id", column: "sub_id", value: "-1", period: "2026", want: ErrInvalidLexeme},
		{name: "empty filing form", column: "filing_form", value: "", period: "2026", want: ErrInvalidLexeme},
		{name: "wrong period", column: "two_year_transaction_period", value: "2024", period: "2026", want: ErrPeriodMismatch},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			values := validRowValues()
			index, ok := ColumnIndex(test.column)
			if !ok {
				t.Fatalf("unknown test column %q", test.column)
			}
			values[index] = test.value
			decoder := NewDecoder(strings.NewReader(strings.Join(values, "\t") + "\n"))
			if !decoder.Scan() {
				t.Fatalf("Scan returned false: %v", decoder.Err())
			}
			err := Validate(decoder.Row(), test.period)
			if test.want == nil && err != nil {
				t.Fatalf("Validate returned %v", err)
			}
			if test.want != nil && !errors.Is(err, test.want) {
				t.Fatalf("Validate error = %v; want %v", err, test.want)
			}
		})
	}
}

func validRowValues() []string {
	values := make([]string, FieldCount)
	for index := range values {
		values[index] = `\N`
	}
	set := func(name, value string) {
		index, _ := ColumnIndex(name)
		values[index] = value
	}
	set("sub_id", "123")
	set("filing_form", "F3X")
	set("two_year_transaction_period", "2026")
	return values
}

func fixtureDirectory(t *testing.T) string {
	t.Helper()
	_, sourceFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve test source path")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(sourceFile), "../../../../contracts/sources/fec/schedule-a/v1/fixtures/dump-2026-08-23"))
}
