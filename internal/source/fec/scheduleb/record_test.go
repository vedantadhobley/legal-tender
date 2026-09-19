package scheduleb

import (
	"errors"
	"strings"
	"testing"
)

func TestCompiledSchemaShape(t *testing.T) {
	if len(Columns()) != FieldCount {
		t.Fatalf("compiled width = %d; want %d", len(Columns()), FieldCount)
	}
	want := []string{"cmte_id", "recipient_cmte_id", "disb_dt", "disb_amt", "disb_tp", "sub_id", "two_year_transaction_period", "clean_recipient_cmte_id", "cmte_dsgn"}
	for _, name := range want {
		if _, ok := ColumnIndex(name); !ok {
			t.Fatalf("compiled schema lacks %s", name)
		}
	}
}

func TestValidateExactLexemesAndPeriod(t *testing.T) {
	values := validRowValues()
	setValue(values, "disb_amt", "42.50")
	setValue(values, "semi_an_bundled_refund", "-0.01")
	setValue(values, "disb_dt", "2024-10-31 00:00:00")
	setValue(values, "comm_dt", "2024-10-30 12:34:56.123456")
	decoder := NewDecoder(strings.NewReader(strings.Join(values, "\t") + "\n"))
	if !decoder.Scan() {
		t.Fatal("missing row")
	}
	if err := Validate(decoder.Row(), "2024"); err != nil {
		t.Fatal(err)
	}
	record, err := Freeze(decoder.Row(), "2024")
	if err != nil {
		t.Fatal(err)
	}
	amount, _ := record.ValueByName("disb_amt")
	if amount.Null || amount.Lexeme != "42.50" {
		t.Fatalf("amount = %+v", amount)
	}
}

func TestValidateRejectsBadRows(t *testing.T) {
	tests := []struct {
		name   string
		column string
		value  string
		period string
		want   error
	}{
		{name: "missing cents", column: "disb_amt", value: "42", period: "2024", want: ErrInvalidLexeme},
		{name: "bad timestamp", column: "disb_dt", value: "2024-02-30 00:00:00", period: "2024", want: ErrInvalidLexeme},
		{name: "negative sub id", column: "sub_id", value: "-1", period: "2024", want: ErrInvalidLexeme},
		{name: "zero sub id", column: "sub_id", value: "0", period: "2024", want: ErrInvalidLexeme},
		{name: "wrong period", column: "two_year_transaction_period", value: "2022", period: "2024", want: ErrPeriodMismatch},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			values := validRowValues()
			setValue(values, test.column, test.value)
			decoder := NewDecoder(strings.NewReader(strings.Join(values, "\t") + "\n"))
			if !decoder.Scan() {
				t.Fatal("missing row")
			}
			if err := Validate(decoder.Row(), test.period); !errors.Is(err, test.want) {
				t.Fatalf("error = %v; want %v", err, test.want)
			}
		})
	}
}

func validRowValues() []string {
	values := make([]string, FieldCount)
	for index := range values {
		values[index] = `\N`
	}
	setValue(values, "sub_id", "123")
	setValue(values, "filing_form", "F3X")
	setValue(values, "two_year_transaction_period", "2024")
	return values
}

func setValue(values []string, name, value string) {
	index, ok := ColumnIndex(name)
	if !ok {
		panic("unknown Schedule B column " + name)
	}
	values[index] = value
}
