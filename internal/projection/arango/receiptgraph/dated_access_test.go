package receiptgraph

import (
	"encoding/json"
	"testing"
)

func TestEntryDatePreservesTypedNullAndRejectsMissingOrInvalid(t *testing.T) {
	for _, value := range []string{"null", "0", "-365", "20000"} {
		source := json.RawMessage(`{"source":{"fields":{"lt_receipt_date":` + value + `}}}`)
		date, err := entryDate(source)
		if err != nil {
			t.Fatal(err)
		}
		b, _ := json.Marshal(date)
		if string(b) != value {
			t.Fatal("date changed", string(b), value)
		}
	}
	for _, value := range []string{`{}`, `{"source":{"fields":{}}}`, `{"source":{"fields":{"lt_receipt_date":1.5}}}`, `{"source":{"fields":{"lt_receipt_date":"20000"}}}`, `{"source":{"fields":{"lt_receipt_date":2147483648}}}`} {
		if _, err := entryDate(json.RawMessage(value)); err == nil {
			t.Fatal("accepted invalid typed date", value)
		}
	}
}
