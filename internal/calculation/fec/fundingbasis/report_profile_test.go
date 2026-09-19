package fundingbasis

import (
	"bytes"
	"context"
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/vedantadhobley/legal-tender/internal/source/fec/schedulea"
)

func profileFixtureRow(t *testing.T, changes map[string]string) []byte {
	t.Helper()
	path := filepath.Join("..", "..", "..", "..", "contracts", "sources", "fec", "schedule-a", "v1", "fixtures", "dump-2026-08-23", "targeted-individual.copy")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	fields := strings.Split(strings.TrimSuffix(string(raw), "\n"), "\t")
	for name, value := range changes {
		i, ok := schedulea.ColumnIndex(name)
		if !ok {
			t.Fatal(name)
		}
		fields[i] = value
	}
	return []byte(strings.Join(fields, "\t") + "\n")
}

func observeProfileFixture(t *testing.T, b *reportProfiler, raw []byte) error {
	t.Helper()
	d := schedulea.NewDecoder(bytes.NewReader(raw))
	for d.Scan() {
		if err := schedulea.Validate(d.Row(), b.out.Cycle); err != nil {
			return err
		}
		if err := b.observe(d.Row()); err != nil {
			return err
		}
	}
	return d.Err()
}

func TestReportProfilePreservesOccurrencesAndScope(t *testing.T) {
	var body []byte
	for _, changes := range []map[string]string{
		{"contb_receipt_amt": "10.25", "file_num": `\N`, "contb_receipt_dt": `\N`},
		{"contb_receipt_amt": "-2.25", "contb_receipt_dt": "2024-12-31 00:00:00"},
		{"contb_receipt_amt": "0", "line_num": "", "contb_receipt_dt": "2027-01-01 00:00:00"},
		{"contb_receipt_amt": "0", "line_num": `\N`, "contb_receipt_dt": "2026-02-30 00:00:00"},
		{"contb_receipt_amt": `\N`},
		{"memo_cd": "X"},
		{"is_individual": "f"},
		{"is_individual": `\N`},
		{"contb_receipt_amt": "10.25", "file_num": `\N`, "contb_receipt_dt": `\N`}, // Duplicate is another physical occurrence.
	} {
		body = append(body, profileFixtureRow(t, changes)...)
	}
	build := func() ReportProfile {
		b, err := newReportProfiler("2026")
		if err != nil {
			t.Fatal(err)
		}
		if err := observeProfileFixture(t, b, body); err != nil {
			t.Fatal(err)
		}
		r, err := b.finish()
		if err != nil {
			t.Fatal(err)
		}
		return r
	}
	a, c := build(), build()
	if !reflect.DeepEqual(a, c) || a.Total.Rows != 9 || a.Individual.Rows != 5 || a.Individual.Signed != 1825 || a.Total.Unknown != 1 || len(a.Reports) != 4 || a.ComparisonReady || a.TerminalEligible {
		t.Fatalf("invalid profile: %+v", a)
	}
	var missing, invalid, before, after uint64
	for _, g := range a.Reports {
		missing += g.Dates.Missing
		invalid += g.Dates.Invalid
		before += g.Dates.BeforeCycle
		after += g.Dates.AfterCycle
	}
	if missing != 2 || invalid != 1 || before != 1 || after != 1 {
		t.Fatal("dates clipped or lost")
	}
	if path := os.Getenv("LT_REPORT_PROFILE_FIXTURE"); path != "" {
		raw, _ := json.MarshalIndent(a, "", "  ")
		if err := os.WriteFile(path, raw, 0o640); err != nil {
			t.Fatal(err)
		}
	}
}

func TestReportProfileOverflowAndCycle(t *testing.T) {
	for _, cycle := range []string{"", "2025", "../2024", "0000"} {
		if _, err := newReportProfiler(cycle); err == nil {
			t.Fatal(cycle)
		}
	}
	b, _ := newReportProfiler("2026")
	if err := observeProfileFixture(t, b, profileFixtureRow(t, map[string]string{"contb_receipt_amt": "92233720368547758.08"})); err == nil {
		t.Fatal("amount overflow accepted")
	}
	b, _ = newReportProfiler("2026")
	b.out.Total.Signed = math.MaxInt64
	if err := observeProfileFixture(t, b, profileFixtureRow(t, nil)); err == nil {
		t.Fatal("sum overflow accepted")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := ProfileReports(ctx, t.TempDir(), "missing", "2026", nil); err == nil {
		t.Fatal("missing source accepted")
	}
}

func TestReportProfileBounds(t *testing.T) {
	b, _ := newReportProfiler("2026")
	for i := 0; i < maxReportProfileGroups; i++ {
		b.dates[Cell{true, strconv.Itoa(i) + "date"}] = ""
	}
	if err := observeProfileFixture(t, b, profileFixtureRow(t, nil)); err == nil {
		t.Fatal("unbounded date cache")
	}
}
