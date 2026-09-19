package scheduleb

import (
	"context"
	"strings"
	"testing"
)

func TestVerifyProfilesRowsAndUniqueness(t *testing.T) {
	first := validRowValues()
	setValue(first, "sub_id", "100")
	setValue(first, "cmte_id", "C00000001")
	setValue(first, "recipient_cmte_id", "C00000002")
	setValue(first, "clean_recipient_cmte_id", "C00000002")
	setValue(first, "disb_amt", "60.99")
	setValue(first, "disb_tp", "24K")
	setValue(first, "disb_dt", "2024-01-02 00:00:00")
	setValue(first, "tran_id", "T1")
	second := validRowValues()
	setValue(second, "sub_id", "101")
	setValue(second, "disb_amt", "-1.00")
	setValue(second, "memo_cd", "X")
	stream := strings.Join(first, "\t") + "\n" + strings.Join(second, "\t") + "\n"
	result, err := Verify(context.Background(), strings.NewReader(stream), VerifyOptions{ExpectedPeriod: "2024", WorkDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	if !result.Complete || result.Rows != 2 || result.ValidRows != 2 || result.UniqueSubIDs != 2 || result.DuplicateSubIDRows != 0 {
		t.Fatalf("unexpected verification: %+v", result)
	}
	if result.FractionalAmounts != 1 || result.NegativeAmounts != 1 || result.Coverage.MatchingRecipientIDs != 1 || result.Coverage.TransactionID != 1 {
		t.Fatalf("unexpected profile: %+v", result)
	}
}

func TestVerifyRejectsDuplicateSubID(t *testing.T) {
	values := validRowValues()
	row := strings.Join(values, "\t") + "\n"
	result, err := Verify(context.Background(), strings.NewReader(row+row), VerifyOptions{ExpectedPeriod: "2024", WorkDir: t.TempDir()})
	if !IsVerifyError(err) || result.DuplicateSubIDRows != 1 || result.UniqueSubIDs != 1 {
		t.Fatalf("unexpected duplicate result: %+v, %v", result, err)
	}
}
