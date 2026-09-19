package schedulee

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestExactCOPYFixtures(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		cycle string
		want  map[string]*string
	}{
		{name: "negative-amount", cycle: "2012", want: map[string]*string{"exp_amt": pointer("-18892.30"), "exp_tp": pointer("24A"), "action_cd": pointer("A")}},
		{name: "missing-expenditure-type", cycle: "2026", want: map[string]*string{"exp_amt": pointer("5.58"), "exp_tp": nil, "dissem_dt": nil}},
		{name: "memo-x", cycle: "2024", want: map[string]*string{"memo_cd": pointer("X"), "exp_tp": pointer("24E"), "exp_dt": nil}},
		{name: "fractional-amount", cycle: "2026", want: map[string]*string{"exp_amt": pointer("60.99"), "s_o_ind": pointer("S"), "exp_tp": pointer("24E")}},
		{name: "escaped-copy-text", cycle: "2024", want: map[string]*string{"exp_desc": pointer("PMT FOR EST FROM 7/22/2023. \t PAYMENT OF 7/22/2023 DIGITAL ADVERTISING"), "action_cd": pointer("C")}},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			physical, err := os.ReadFile(filepath.Join(fixtureDirectory(t), test.name+".copy"))
			if err != nil {
				t.Fatal(err)
			}
			decoder := NewDecoder(bytes.NewReader(physical))
			if !decoder.Scan() {
				t.Fatalf("Scan returned false: %v", decoder.Err())
			}
			if got := decoder.Row().FieldCount(); got != FieldCount {
				t.Fatalf("field count = %d; want %d", got, FieldCount)
			}
			if !bytes.Equal(decoder.Row().Raw(), physical) {
				t.Fatal("decoder did not preserve the exact physical row")
			}
			record, err := Freeze(decoder.Row(), test.cycle)
			if err != nil {
				t.Fatal(err)
			}
			for name, expected := range test.want {
				value, ok := record.ValueByName(name)
				if !ok {
					t.Fatalf("compiled schema lacks %s", name)
				}
				if expected == nil {
					if !value.Null {
						t.Fatalf("%s = %q; want null", name, value.Lexeme)
					}
				} else if value.Null || value.Lexeme != *expected {
					t.Fatalf("%s = (null=%t, %q); want %q", name, value.Null, value.Lexeme, *expected)
				}
			}
			if !jsonValid(record.CanonicalJSON()) {
				t.Fatal("canonical record is not valid JSON")
			}
			if decoder.Scan() {
				t.Fatal("fixture produced a second row")
			}
			if err := decoder.Err(); err != nil {
				t.Fatal(err)
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
		cycle  string
		want   error
	}{
		{name: "negative exact cents", column: "exp_amt", value: "-42.50", cycle: "2026"},
		{name: "missing cents", column: "exp_amt", value: "42", cycle: "2026", want: ErrInvalidLexeme},
		{name: "too much scale", column: "exp_amt", value: "42.501", cycle: "2026", want: ErrInvalidLexeme},
		{name: "too much precision", column: "exp_amt", value: "1234567890123.45", cycle: "2026", want: ErrInvalidLexeme},
		{name: "bad timestamp", column: "exp_dt", value: "2026-02-30 00:00:00", cycle: "2026", want: ErrInvalidLexeme},
		{name: "negative sub id", column: "sub_id", value: "-1", cycle: "2026", want: ErrInvalidLexeme},
		{name: "empty filing form", column: "filing_form", value: "", cycle: "2026", want: ErrInvalidLexeme},
		{name: "wrong cycle", column: "election_cycle", value: "2024", cycle: "2026", want: ErrCycleMismatch},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			values := validRowValues()
			index, ok := ColumnIndex(test.column)
			if !ok {
				t.Fatalf("unknown column %q", test.column)
			}
			values[index] = test.value
			decoder := NewDecoder(strings.NewReader(strings.Join(values, "\t") + "\n"))
			if !decoder.Scan() {
				t.Fatalf("Scan returned false: %v", decoder.Err())
			}
			err := Validate(decoder.Row(), test.cycle)
			if test.want == nil && err != nil {
				t.Fatal(err)
			}
			if test.want != nil && !errors.Is(err, test.want) {
				t.Fatalf("Validate error = %v; want %v", err, test.want)
			}
		})
	}
}

func TestMalformedRowsRemainObservable(t *testing.T) {
	t.Parallel()
	decoder := NewDecoder(strings.NewReader("one\ttwo\n"))
	if !decoder.Scan() {
		t.Fatalf("malformed row was not yielded: %v", decoder.Err())
	}
	if !errors.Is(decoder.Row().Issue(), ErrFieldCount) {
		t.Fatalf("row issue = %v; want field-count error", decoder.Row().Issue())
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
	set("election_cycle", "2026")
	return values
}

func pointer(value string) *string { return &value }

func jsonValid(value []byte) bool {
	var destination any
	return json.Unmarshal(value, &destination) == nil
}

func fixtureDirectory(t *testing.T) string {
	t.Helper()
	_, sourceFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve test source path")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(sourceFile), "../../../../contracts/sources/fec/schedule-e/v1/fixtures/dump-2026-08-30"))
}
