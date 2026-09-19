package fundingbasis

import (
	"bytes"
	"context"
	"encoding/json"
	"math"
	"os"
	"reflect"
	"testing"

	"github.com/vedantadhobley/legal-tender/internal/source/fec/schedulea"
)

func lineProfileFixture(t *testing.T) []byte {
	t.Helper()
	var body []byte
	for _, edit := range []map[string]string{
		{},
		{"is_individual": "f", "contb_receipt_amt": "-2.25"},
		{"is_individual": "f", "memo_cd": "X", "contb_receipt_dt": "2027-01-01 00:00:00"},
		{"memo_cd": "X"},
		{"is_individual": `\N`},
		{"contb_receipt_amt": `\N`},
		{"memo_cd": "Y"},
		{"filing_form": "F3P", "line_num": "17A"},
		{"line_num": "", "contb_receipt_dt": "2026-02-30 00:00:00"},
		{"line_num": `\N`, "contb_receipt_dt": "2024-12-31 00:00:00"},
		{}, // An identical physical occurrence stays in the profile.
		{"is_individual": "f", "memo_cd": "", "contb_receipt_amt": "0", "contb_receipt_dt": `\N`, "file_num": `\N`},
	} {
		fields := map[string]string{"filing_form": "F3", "schedule_type": "SA", "line_num": "11AI", "is_individual": "t", "memo_cd": `\N`, "contb_receipt_amt": "10.25", "contb_receipt_dt": "2026-01-01 00:00:00"}
		for k, v := range edit {
			fields[k] = v
		}
		body = append(body, profileFixtureRow(t, fields)...)
	}
	return body
}

func observeLineProfileFixture(b *reportLineProfiler, body []byte) error {
	d := schedulea.NewDecoder(bytes.NewReader(body))
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

func TestReportProfileV2AllAxesConservationAndV1Equivalence(t *testing.T) {
	body := lineProfileFixture(t)
	build := func() ReportLineProfile {
		b, err := newReportLineProfiler("2026")
		if err != nil {
			t.Fatal(err)
		}
		if err := observeLineProfileFixture(b, body); err != nil {
			t.Fatal(err)
		}
		out, err := b.finish(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		return out
	}
	a, replay := build(), build()
	if !reflect.DeepEqual(a, replay) || a.Total.Rows != 12 || a.Total.Unknown != 1 || a.Individual.Rows != 6 || a.Individual.Signed != 6150 || a.LinePopulation.Rows != 5 || a.LinePopulation.Signed != 2850 || a.ComparisonReady || a.TerminalEligible {
		t.Fatal(a)
	}
	v1, _ := newReportProfiler("2026")
	if err := observeProfileFixture(t, v1, body); err != nil {
		t.Fatal(err)
	}
	old, err := v1.finish()
	if err != nil || old.Total != a.Total || old.Individual != a.Individual {
		t.Fatal("old predicate changed", err)
	}
	forms := map[ReportFormKey]Measures{}
	for _, g := range a.Forms {
		k := ReportFormKey{g.Key.Form, g.Key.Schedule, g.Key.Line, g.Key.Decision}
		m := forms[k]
		if err := m.merge(g.Measures); err != nil {
			t.Fatal(err)
		}
		forms[k] = m
	}
	for _, g := range old.Forms {
		if forms[g.Key] != g.Measures {
			t.Fatal("v1 form membership changed")
		}
		delete(forms, g.Key)
	}
	if len(forms) != 0 {
		t.Fatal("unmatched v2 forms")
	}
	var missing, invalid, before, after uint64
	for _, g := range a.Reports {
		missing += g.Dates.Missing
		invalid += g.Dates.Invalid
		before += g.Dates.BeforeCycle
		after += g.Dates.AfterCycle
		if g.Dates.AfterCycle > 0 && (g.Key.Individual != "false" || g.Key.Memo.Value != "X") {
			t.Fatal("excluded memo date lost")
		}
	}
	if missing != 1 || invalid != 1 || before != 1 || after != 1 {
		t.Fatal("date axes lost")
	}
	if path := os.Getenv("LT_REPORT_PROFILE_V2_FIXTURE"); path != "" {
		raw, _ := json.MarshalIndent(a, "", "  ")
		if err := os.WriteFile(path, raw, 0o640); err != nil {
			t.Fatal(err)
		}
	}
}

func TestReportProfileV2BoundsAndFailures(t *testing.T) {
	for _, body := range [][]byte{
		append(profileFixtureRow(t, nil), profileFixtureRow(t, map[string]string{"filing_form": "F3P"})...),
		append(profileFixtureRow(t, nil), profileFixtureRow(t, map[string]string{"file_num": "123"})...),
	} {
		b, _ := newReportLineProfiler("2026")
		b.limit = 1
		if err := observeLineProfileFixture(b, body); err == nil {
			t.Fatal("unbounded group map")
		}
	}
	for _, edit := range []func(*reportLineProfiler){
		func(b *reportLineProfiler) { b.out.Total.Signed++ },
		func(b *reportLineProfiler) { b.out.LinePopulation.Rows++ },
		func(b *reportLineProfiler) {
			for _, g := range b.reports {
				g.Dates.Missing++
				break
			}
		},
		func(b *reportLineProfiler) {
			for _, g := range b.reports {
				g.First = 0
				break
			}
		},
	} {
		b, _ := newReportLineProfiler("2026")
		if err := observeLineProfileFixture(b, lineProfileFixture(t)); err != nil {
			t.Fatal(err)
		}
		edit(b)
		if out, err := b.finish(context.Background()); err == nil || out.ProfileID != "" {
			t.Fatal("invalid profile published")
		}
	}
	b, _ := newReportLineProfiler("2026")
	if err := observeLineProfileFixture(b, profileFixtureRow(t, map[string]string{"contb_receipt_amt": "92233720368547758.08"})); err == nil {
		t.Fatal("amount overflow accepted")
	}
	b, _ = newReportLineProfiler("2026")
	b.out.Total.Positive = math.MaxInt64
	if err := observeLineProfileFixture(b, profileFixtureRow(t, nil)); err == nil {
		t.Fatal("sum overflow accepted")
	}
	for _, cycle := range []string{"", "2025", "../2024", "0000"} {
		if _, err := newReportLineProfiler(cycle); err == nil {
			t.Fatal("invalid cycle")
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := b.finish(ctx); err == nil {
		t.Fatal("cancelled profile accepted")
	}
	if _, err := ProfileReportLines(ctx, t.TempDir(), "absent", "2026", nil); err == nil {
		t.Fatal("missing backing accepted")
	}
}
