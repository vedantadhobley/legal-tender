package summaryassertion

// This is a pinned, opt-in source investigation, not a production report parser
// or amendment selector. Named examples and P3.4 offsets are audit witnesses
// only. Paper-transcription field meaning was checked against original images;
// the assertions below do not claim to automate that visual review.

import (
	"bufio"
	"bytes"
	"encoding/json"
	"io"
	"math/big"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

func TestSummaryReportReviewCorpus(t *testing.T) {
	root := os.Getenv("LT_SUMMARY_REPORT_AUDIT")
	if root == "" {
		t.Skip("requires pinned summary report investigation artifacts")
	}
	witnesses, err := os.ReadFile("../../../../docs/audit/fixtures/summary-report-review-2026-09-10.sha256")
	if err != nil {
		t.Fatal(err)
	}
	source := map[string][]byte{}
	s := bufio.NewScanner(bytes.NewReader(witnesses))
	for s.Scan() {
		parts := strings.Fields(s.Text())
		if len(parts) != 2 || filepath.Base(parts[1]) != parts[1] || len(parts[0]) != 64 {
			t.Fatalf("invalid audit witness %q", s.Text())
		}
		f, err := os.Open(filepath.Join(root, parts[1]))
		if err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(io.LimitReader(f, (10<<20)+1))
		closeErr := f.Close()
		if err != nil || closeErr != nil || len(body) > 10<<20 || digest(body) != parts[0] {
			t.Fatalf("changed/oversized/unreadable artifact: %s", parts[1])
		}
		if _, exists := source[parts[1]]; exists {
			t.Fatalf("duplicate witness %s", parts[1])
		}
		source[parts[1]] = body
	}
	if err := s.Err(); err != nil {
		t.Fatal(err)
	}
	var profile map[string]struct {
		Largest map[string][]struct {
			Committee string            `json:"committee"`
			Delta     string            `json:"delta"`
			Operands  map[string]string `json:"operands"`
		} `json:"largest_differences"`
	}
	if err := json.Unmarshal(source["independent-profile.json"], &profile); err != nil {
		t.Fatal(err)
	}
	operands := func(equation, committee string) map[string]string {
		for _, row := range profile["2024"].Largest[equation] {
			if row.Committee == committee {
				return row.Operands
			}
		}
		t.Fatalf("absent published summary witness %s/%s", equation, committee)
		return nil
	}
	// Offsets below are one-based and scoped to these exact artifact hashes.
	record := func(name, header, committee string, width int) []string {
		rows := bytes.SplitN(source[name], []byte{'\n'}, 3)
		if len(rows) < 2 || !strings.HasPrefix(string(rows[0]), header) {
			t.Fatalf("unexpected captured header: %s", name)
		}
		fields := strings.Split(strings.TrimSuffix(string(rows[1]), "\r"), "\x1c")
		if len(fields) != width || fields[1] != committee {
			t.Fatalf("wrong observed report identity/width: %s (%d)", name, len(fields))
		}
		return append([]string{"one-based"}, fields...)
	}

	t.Run("filer_cash_discontinuity", func(t *testing.T) {
		selected := []string{"1714573", "1766866", "1743911", "1780310", "1780346"}
		var listing struct {
			Results []struct {
				Number     json.Number `json:"file_number"`
				MostRecent bool        `json:"most_recent"`
			} `json:"results"`
		}
		if err := json.Unmarshal(source["C00843367-filings.json"], &listing); err != nil {
			t.Fatal(err)
		}
		latest := map[string]bool{}
		for _, row := range listing.Results {
			latest[string(row.Number)] = row.MostRecent
		}
		receipts, disbursements := new(big.Int), new(big.Int)
		for _, name := range selected {
			if !latest[name] {
				t.Fatalf("captured listing does not mark witness %s most recent", name)
			}
			f := record(name+".fec", "HDR\x1cFEC\x1c8.4\x1c", "C00843367", 93)
			reviewDelta(t, "report cash equation", "0", []string{f[58], f[46]}, []string{f[57], f[62]})
			receipts.Add(receipts, reviewCents(t, f[46]))
			disbursements.Add(disbursements, reviewCents(t, f[57]))
		}
		p := operands("cash", "C00843367")
		if receipts.Cmp(reviewCents(t, p["TTL_RECEIPTS"])) != 0 || disbursements.Cmp(reviewCents(t, p["TTL_DISB"])) != 0 {
			t.Fatal("period totals do not reproduce the captured cycle summary")
		}
		prior := record("1780310.fec", "HDR\x1cFEC\x1c8.4\x1c", "C00843367", 93)
		last := record("1780346.fec", "HDR\x1cFEC\x1c8.4\x1c", "C00843367", 93)
		if prior[17] != "20240331" || last[16] != "20240401" {
			t.Fatal("not adjacent reporting periods")
		}
		reviewDelta(t, "cash continuity gap", "150000000", []string{prior[62]}, []string{last[58]})
		reviewDelta(t, "cumulative vs period disbursements", "150000000", []string{last[93]}, []string{p["TTL_DISB"]})
		reviewDelta(t, "cumulative vs period loan repayment", "150000000", []string{last[85]}, []string{last[49]})
		reviewDelta(t, "matching ending cash", "0", []string{last[62]}, []string{p["COH_COP"]})
		t.Log("Five report-period cash equations balance; a $1,500,000 inter-report gap remains. Period totals reproduce the cycle CSV. F99 is a filer explanation, not a correction.")
	})

	t.Run("paper_attachment_amendment_scope", func(t *testing.T) {
		for _, name := range []string{"1876290.fec", "1882886.fec"} {
			f := record(name, "HDR\x1cP3.4\x1c", "C00075820", 125)
			if f[1] != "F3XA" || f[13] != "20240901" || f[14] != "20240930" {
				t.Fatal("wrong paper cover identity")
			}
			for i := 21; i <= 122; i++ {
				if f[i] != "" {
					t.Fatalf("expected blank financial slot %s/%d", name, i)
				}
			}
		}
		if len(source["1833804-prefix.fec"]) != 16384 || !bytes.Contains(source["1833804-prefix.fec.headers"], []byte("content-range: bytes 0-16383/19020241")) {
			t.Fatal("missing explicit partial-response scope")
		}
		original := record("1833804-prefix.fec", "HDR\x1cFEC\x1c8.4\x1c", "C00075820", 123)
		reviewDelta(t, "original report cash equation", "0", []string{original[23], original[24]}, []string{original[26], original[27]})
		var reports struct {
			Pagination struct{ Count, Pages int } `json:"pagination"`
			Results    []struct {
				Number   int         `json:"file_number"`
				Amended  bool        `json:"is_amended"`
				Receipts json.Number `json:"total_receipts_period"`
				Disb     json.Number `json:"total_disbursements_period"`
				Start    string      `json:"coverage_start_date"`
				End      string      `json:"coverage_end_date"`
			} `json:"results"`
		}
		if err := json.Unmarshal(source["C00075820-reports.json"], &reports); err != nil {
			t.Fatal(err)
		}
		if reports.Pagination.Pages != 1 || reports.Pagination.Count != len(reports.Results) {
			t.Fatal("incomplete captured report listing")
		}
		receipts, disbursements := new(big.Int), new(big.Int)
		seen := map[string]bool{}
		selected := 0
		paperSelected, originalExcluded := false, false
		for _, row := range reports.Results {
			if row.Number == 1833804 {
				originalExcluded = row.Amended
				reviewDelta(t, "API/original receipts", "0", []string{string(row.Receipts)}, []string{original[24]})
				reviewDelta(t, "API/original disbursements", "0", []string{string(row.Disb)}, []string{original[26]})
			}
			if row.Amended {
				continue
			}
			if row.Number == 1882886 {
				paperSelected = true
				reviewDelta(t, "processed paper receipts", "0", []string{string(row.Receipts)}, nil)
				reviewDelta(t, "processed paper disbursements", "0", []string{string(row.Disb)}, nil)
			}
			key := row.Start + "/" + row.End
			if seen[key] {
				t.Fatal("multiple unamended reports in one sampled interval")
			}
			seen[key] = true
			selected++
			receipts.Add(receipts, reviewCents(t, string(row.Receipts)))
			disbursements.Add(disbursements, reviewCents(t, string(row.Disb)))
		}
		p := operands("cash", "C00075820")
		if selected != 24 || !paperSelected || !originalExcluded || receipts.Cmp(reviewCents(t, p["TTL_RECEIPTS"])) != 0 || disbursements.Cmp(reviewCents(t, p["TTL_DISB"])) != 0 {
			t.Fatal("captured amendment selection does not reproduce cycle totals")
		}
		reviewDelta(t, "excluded original net equals cycle residual", "2187612494", []string{original[26]}, []string{original[24]})
		reviewDelta(t, "counterfactual balance only, not a repair", "0", []string{p["COH_BOP"], p["TTL_RECEIPTS"], original[24]}, []string{p["TTL_DISB"], original[26], p["COH_COP"]})
		t.Log("The unamended-report filter reproduces the cycle CSV exactly but selects a blank paper cover for September. The original report net accounts for the $21,876,124.94 residual; no report is promoted.")
	})

	t.Run("paper_transcription_vs_original_image", func(t *testing.T) {
		f := record("1813890.fec", "HDR\x1cP3.4\x1c", "C00249581", 125)
		if f[13] != "20240401" || f[14] != "20240630" || f[29] != "699033.00" || f[30] != "0.00" || f[31] != "699.00" {
			t.Fatal("changed paper-transcription witness")
		}
		p := operands("individual", "C00249581")
		for column, position := range map[string]int{"INDV_ITEM_CONTB": 29, "INDV_UNITEM_CONTB": 30, "INDV_CONTB": 31} {
			reviewDelta(t, "paper transcription/cycle column", "0", []string{p[column]}, []string{f[position]})
		}
		reviewDelta(t, "transcribed individual subtotal", "69833400", []string{f[29], f[30]}, []string{f[31]})
		t.Log("Original image page 3 was visually reviewed: line 11(a)(i) is $699.00, not transcribed $699,033.00. The test pins image bytes; it does not perform OCR or correct source facts.")
	})
}

func reviewCents(t *testing.T, value string) *big.Int {
	t.Helper()
	if !regexp.MustCompile(`^-?[0-9]+(\.[0-9]{1,2})?$`).MatchString(value) {
		t.Fatalf("not an exact present decimal: %q", value)
	}
	n, ok := new(big.Rat).SetString(value)
	if !ok {
		t.Fatal(value)
	}
	n.Mul(n, big.NewRat(100, 1))
	if !n.IsInt() {
		t.Fatal("non-cent amount")
	}
	return new(big.Int).Set(n.Num())
}

func reviewDelta(t *testing.T, label, want string, positive, negative []string) {
	t.Helper()
	n := new(big.Int)
	for _, raw := range positive {
		n.Add(n, reviewCents(t, raw))
	}
	for _, raw := range negative {
		n.Sub(n, reviewCents(t, raw))
	}
	if n.String() != want {
		t.Fatalf("%s: got %s cents, want %s", label, n, want)
	}
}
