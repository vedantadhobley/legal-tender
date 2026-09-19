package cli

import (
	"bytes"
	"testing"
)

func TestFundingBasisCommandOptions(t *testing.T) {
	for _, command := range []string{"calculate-committee-funding-basis", "list-funding-receipts", "assess-candidate-funding-basis", "review-funding-component", "inspect-funding-receipts"} {
		var out, errout bytes.Buffer
		if code := Run([]string{"pipeline", "fec", command, "--help"}, &out, &errout); code != 0 {
			t.Fatal(command, code)
		}
		if code := Run([]string{"pipeline", "fec", command}, &out, &errout); code != 2 {
			t.Fatal(command, code)
		}
		if out.Len() != 0 {
			t.Fatal("options wrote result bytes")
		}
	}
}
