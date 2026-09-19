package schedulee

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestVerifyProfilesExactFixtures(t *testing.T) {
	t.Parallel()
	names := []string{"fractional-amount.copy", "missing-expenditure-type.copy"}
	var stream bytes.Buffer
	for _, name := range names {
		content, err := os.ReadFile(filepath.Join(fixtureDirectory(t), name))
		if err != nil {
			t.Fatal(err)
		}
		stream.Write(content)
	}

	result, err := Verify(context.Background(), bytes.NewReader(stream.Bytes()), VerifyOptions{ExpectedCycle: "2026", ExpectedRows: 2})
	if err != nil {
		t.Fatal(err)
	}
	if result.Rows != 2 || result.ValidRows != 2 || result.FractionalAmounts != 2 || result.NullExpenseDates != 0 || result.NullDisseminationDates != 1 {
		t.Fatalf("unexpected verification profile: %+v", result)
	}
	if countValue(result.ExpenditureTypes, "24E") != 1 || countValue(result.ExpenditureTypes, "<null>") != 1 {
		t.Fatalf("unexpected expenditure types: %+v", result.ExpenditureTypes)
	}
}

func TestVerifyReportsContractFailure(t *testing.T) {
	t.Parallel()
	result, err := Verify(context.Background(), bytes.NewBufferString("short\n"), VerifyOptions{})
	if !IsVerifyError(err) {
		t.Fatalf("error = %v; want VerifyError", err)
	}
	var verifyError *VerifyError
	if !errors.As(err, &verifyError) || result.InvalidRows != 1 || verifyError.InvalidRows != 1 {
		t.Fatalf("unexpected failed result: %+v, %v", result, err)
	}
}

func TestVerifyRejectsDuplicateSubID(t *testing.T) {
	t.Parallel()
	content, err := os.ReadFile(filepath.Join(fixtureDirectory(t), "fractional-amount.copy"))
	if err != nil {
		t.Fatal(err)
	}
	stream := append(append([]byte(nil), content...), content...)
	result, err := Verify(context.Background(), bytes.NewReader(stream), VerifyOptions{ExpectedCycle: "2026"})
	if !IsVerifyError(err) || result.DuplicateSubIDs != 1 || result.InvalidRows != 0 {
		t.Fatalf("unexpected duplicate result: %+v, %v", result, err)
	}
}

func countValue(counts []ValueCount, value string) uint64 {
	for _, count := range counts {
		if count.Value == value {
			return count.Rows
		}
	}
	return 0
}
