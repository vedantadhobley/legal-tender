package fundingbasis

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/vedantadhobley/legal-tender/internal/calculation/fec/reportperiod"
	"github.com/vedantadhobley/legal-tender/internal/calculation/fec/summaryassertion"
	"github.com/vedantadhobley/legal-tender/internal/source/fec/reportscope"
	"github.com/vedantadhobley/legal-tender/internal/source/fec/schedulea"
)

func savedProfileFixture(t *testing.T) ReportLineProfile {
	t.Helper()
	b, _ := newReportLineProfiler("2026")
	if err := observeLineProfileFixture(b, lineProfileFixture(t)); err != nil {
		t.Fatal(err)
	}
	rows := b.out.Total.Rows
	b.out.Source.RowCount = &rows
	b.out.Verification = schedulea.Verification{SchemaVersion: "legal-tender.schedule-a-verification.v1", Complete: true, ExpectedPeriod: "2026", Rows: rows, ValidRows: rows}
	for _, name := range []string{"row_validity", "row_count", "compressed_byte_count", "compressed_sha256", "uncompressed_byte_count", "uncompressed_sha256"} {
		b.out.Verification.Checks = append(b.out.Verification.Checks, schedulea.Check{ID: name, Passed: true})
	}
	p, err := b.finish(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func TestSavedProfileRevalidation(t *testing.T) {
	p := savedProfileFixture(t)
	if err := validateSavedLineProfile(context.Background(), p); err != nil {
		t.Fatal(err)
	}
	body, _ := json.Marshal(p)
	for _, tc := range []struct {
		name   string
		mutate func(*ReportLineProfile)
	}{
		{"v1", func(p *ReportLineProfile) { p.SchemaVersion = "v1" }},
		{"ready", func(p *ReportLineProfile) { p.ComparisonReady = true }},
		{"terminal", func(p *ReportLineProfile) { p.TerminalEligible = true }},
		{"lost_limits", func(p *ReportLineProfile) { p.NotEstablished = nil }},
		{"source", func(p *ReportLineProfile) { p.Source.CompressedSHA256 = "different" }},
		{"incomplete", func(p *ReportLineProfile) { p.Verification.Complete = false }},
		{"failed_check", func(p *ReportLineProfile) { p.Verification.Checks[0].Passed = false }},
		{"missing_check", func(p *ReportLineProfile) { p.Verification.Checks = p.Verification.Checks[1:] }},
		{"duplicate_form", func(p *ReportLineProfile) { p.Forms = append(p.Forms, p.Forms[0]) }},
		{"duplicate_report", func(p *ReportLineProfile) { p.Reports = append(p.Reports, p.Reports[0]) }},
		{"omitted_group", func(p *ReportLineProfile) { p.Reports = p.Reports[1:] }},
		{"changed_amount", func(p *ReportLineProfile) { p.Reports[0].Measures.Signed++ }},
		{"changed_date", func(p *ReportLineProfile) { p.Reports[0].Dates.Missing++ }},
		{"changed_ordinal", func(p *ReportLineProfile) { p.Reports[0].First = 0 }},
		{"changed_identity", func(p *ReportLineProfile) { p.ProfileID = strings.Repeat("a", 64) }},
		{"noncanonical_null", func(p *ReportLineProfile) { p.Reports[0].Key.Memo = Cell{false, "X"} }},
		{"overflow_count", func(p *ReportLineProfile) { p.Reports[0].Measures.Rows = ^uint64(0) }},
		{"memo_rule", func(p *ReportLineProfile) {
			p.Reports[0].Key.Disposition = "reviewed_nonmemo_line"
			p.Reports[0].Key.Memo = Cell{true, "Y"}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var changed ReportLineProfile
			if err := json.Unmarshal(body, &changed); err != nil {
				t.Fatal(err)
			}
			tc.mutate(&changed)
			if err := validateSavedLineProfile(context.Background(), changed); err == nil {
				t.Fatal("accepted invalid saved profile")
			}
		})
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := validateSavedLineProfile(ctx, p); err == nil {
		t.Fatal("ignored cancellation")
	}
}

func TestSavedProfileReaderRejectsBeforeSourceAccess(t *testing.T) {
	p := savedProfileFixture(t)
	body, _ := json.Marshal(p)
	for _, tc := range []struct {
		name string
		body []byte
		pin  string
		want string
	}{
		{"pin", body, "invalid", "SHA-256 pin"},
		{"tamper", body, strings.Repeat("0", 64), "identity mismatch"},
		{"trailing", append(slices.Clone(body), []byte(" {}")...), "", "trailing"},
		{"unknown", []byte(`{"new_field":1}`), "", "unknown field"},
		{"lineage", body, "", "lineage mismatch"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "profile.json")
			if err := os.WriteFile(path, tc.body, 0600); err != nil {
				t.Fatal(err)
			}
			pin := tc.pin
			if pin == "" {
				d := sha256.Sum256(tc.body)
				pin = hex.EncodeToString(d[:])
			}
			_, _, err := readLineProfile(context.Background(), path, pin, "missing", summaryassertion.WindowComparison{})
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatal(err)
			}
		})
	}
}

func receiptReportFixture() (reportperiod.Observation, reportperiod.WindowBinding, []ReportLineProfileGroup) {
	v := "100"
	o := reportperiod.Observation{FileNumber: "1", ReportForm: "Form 3", ReportType: "Q1", ReportYear: 2026, WindowRelation: "inside"}
	b := reportperiod.WindowBinding{ScopeBound: true, ObservationIndex: new(int), Fields: []reportperiod.FieldBinding{{Name: itemizedPeriodField, ReportedValueBound: true, Metadata: reportperiod.MetadataAmount{State: "valid", MinorUnits: &v}}},
		Document: reportscope.ElectronicAssessment{Assessment: reportscope.Assessment{FileNumber: "1", CaptureExtent: "complete_response", Representation: "electronic_8.4", Disposition: "electronic_cover_parsed", Records: []reportscope.Record{{Complete: true, Raw: []byte("HDR\x1cFEC\n")}, {Complete: true, Raw: []byte("F3N\x1cC00000001\n")}}}}}
	g := ReportLineProfileGroup{Key: ReportLineProfileKey{Committee: Cell{true, "C00000001"}, File: Cell{true, "1"}, ReportType: Cell{true, "Q1"}, ReportYear: Cell{true, "2026"}, ReportLineProfileFormKey: ReportLineProfileFormKey{ReportLineKey: ReportLineKey{Form: Cell{true, "F3"}, Schedule: Cell{true, "SA"}, Line: Cell{true, "11AI"}, Individual: "false", Disposition: "reviewed_nonmemo_line"}}}, Measures: Measures{Rows: 1, Known: 1, PositiveRows: 1, Signed: 100, Positive: 100}}
	return o, b, []ReportLineProfileGroup{g}
}

func TestReceiptReportPopulationAndEmptyEvidence(t *testing.T) {
	for _, tc := range []struct {
		name, state, blocker string
		mutate               func(*reportperiod.Observation, *reportperiod.WindowBinding, *[]ReportLineProfileGroup)
	}{
		{"equal_false_individual", "equal", "", func(_ *reportperiod.Observation, _ *reportperiod.WindowBinding, _ *[]ReportLineProfileGroup) {}},
		{"mismatch", "different", "", func(_ *reportperiod.Observation, b *reportperiod.WindowBinding, _ *[]ReportLineProfileGroup) {
			v := "101"
			b.Fields[0].Metadata.MinorUnits = &v
		}},
		{"no_date_clipping", "equal", "", func(_ *reportperiod.Observation, _ *reportperiod.WindowBinding, g *[]ReportLineProfileGroup) {
			(*g)[0].Dates.Last = "2029-01-01"
		}},
		{"duplicate_occurrences_not_deduped", "different", "", func(_ *reportperiod.Observation, _ *reportperiod.WindowBinding, g *[]ReportLineProfileGroup) {
			(*g)[0].Measures = Measures{Rows: 2, Known: 2, PositiveRows: 2, Signed: 200, Positive: 200}
		}},
		{"memo_preserved_not_added", "equal", "", func(_ *reportperiod.Observation, _ *reportperiod.WindowBinding, g *[]ReportLineProfileGroup) {
			x := (*g)[0]
			x.Key.Memo = Cell{true, "X"}
			x.Key.Disposition = "excluded_memo_subtotal"
			*g = append(*g, x)
		}},
		{"unknown_memo", "blocked", "unresolved_memo_code", func(_ *reportperiod.Observation, _ *reportperiod.WindowBinding, g *[]ReportLineProfileGroup) {
			(*g)[0].Key.Disposition = "unresolved_memo_code"
		}},
		{"unknown_amount", "blocked", "unresolved_line_amount", func(_ *reportperiod.Observation, _ *reportperiod.WindowBinding, g *[]ReportLineProfileGroup) {
			(*g)[0].Key.Disposition = "unresolved_line_amount"
		}},
		{"wrong_form", "blocked", "profile_report_scope_mismatch", func(o *reportperiod.Observation, _ *reportperiod.WindowBinding, _ *[]ReportLineProfileGroup) {
			o.ReportForm = "Form 3X"
		}},
		{"wrong_type", "blocked", "profile_report_scope_mismatch", func(o *reportperiod.Observation, _ *reportperiod.WindowBinding, _ *[]ReportLineProfileGroup) {
			o.ReportType = "YE"
		}},
		{"wrong_year", "blocked", "profile_report_scope_mismatch", func(o *reportperiod.Observation, _ *reportperiod.WindowBinding, _ *[]ReportLineProfileGroup) {
			o.ReportYear--
		}},
		{"empty_positive_cover", "blocked", "no_qualified_detail_value", func(_ *reportperiod.Observation, _ *reportperiod.WindowBinding, g *[]ReportLineProfileGroup) {
			*g = nil
		}},
		{"empty_explicit_zero", "equal", "", func(_ *reportperiod.Observation, b *reportperiod.WindowBinding, g *[]ReportLineProfileGroup) {
			*g = nil
			v := "0"
			b.Fields[0].Metadata.MinorUnits = &v
		}},
		{"empty_prefix", "blocked", "no_qualified_detail_value", func(_ *reportperiod.Observation, b *reportperiod.WindowBinding, g *[]ReportLineProfileGroup) {
			*g = nil
			v := "0"
			b.Fields[0].Metadata.MinorUnits = &v
			b.Document.CaptureExtent = "prefix"
		}},
		{"empty_original_has_rows", "blocked", "no_qualified_detail_value", func(_ *reportperiod.Observation, b *reportperiod.WindowBinding, g *[]ReportLineProfileGroup) {
			*g = nil
			v := "0"
			b.Fields[0].Metadata.MinorUnits = &v
			b.Document.Records = append(b.Document.Records, reportscope.Record{Complete: true, Raw: []byte("SA11AI\x1canything\n")})
		}},
		{"empty_unbound_zero", "blocked", "report_itemized_field_unbound", func(_ *reportperiod.Observation, b *reportperiod.WindowBinding, g *[]ReportLineProfileGroup) {
			*g = nil
			v := "0"
			b.Fields[0].Metadata.MinorUnits = &v
			b.Fields[0].ReportedValueBound = false
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			o, b, g := receiptReportFixture()
			tc.mutate(&o, &b, &g)
			before, _ := json.Marshal(g)
			r, err := compareReceiptReport(o, b, g)
			if err != nil || r.State != tc.state || (tc.blocker != "" && !slices.Contains(r.Blockers, tc.blocker)) {
				t.Fatal(r, err)
			}
			after, _ := json.Marshal(g)
			if string(before) != string(after) {
				t.Fatal("mutated groups")
			}
			if r.State == "blocked" && (r.DetailMinorUnits != nil || r.DeltaMinorUnits != nil) {
				t.Fatal("manufactured blocked value")
			}
		})
	}
}

func TestReceiptWindowScopeIsolationAndExactDifference(t *testing.T) {
	o, b, g := receiptReportFixture()
	x := "100"
	f := summaryassertion.WindowFieldComparison{SummaryField: "INDV_ITEM_CONTB", WindowField: itemizedPeriodField, SummaryMinorUnits: &x, WindowMinorUnits: &x, State: "equal", ReportedComparisonReady: true, Blockers: []string{}}
	s := summaryassertion.WindowComparison{CommitteeID: "C00000001", Window: reportperiod.WindowReview{Membership: reportperiod.Review{Observations: []reportperiod.Observation{o}}, Bindings: []reportperiod.WindowBinding{b}, Fields: []reportperiod.WindowField{{Name: itemizedPeriodField, MemberBindingIndexes: []int{0}, WindowValueMinorUnits: &x, ReportedWindowReady: true}}}, Comparisons: []summaryassertion.AssertionWindowComparison{{AssertionID: "one", Fields: []summaryassertion.WindowFieldComparison{f}}}}
	outside := g[0]
	outside.Key.File = Cell{true, "superseded"}
	other := g[0]
	other.Key.Committee = Cell{true, "C00000002"}
	p := ReportLineProfile{Reports: append(g, outside, other)}
	before, _ := json.Marshal(s)
	r, err := compareReceiptWindow(p, s)
	if err != nil || r.State != "equal" || !r.OccurrenceComparisonReady || r.Line.Rows != 1 || len(r.Reports) != 1 || r.SummaryComparisons[0].State != "equal" || r.SourceBodyRescanned || r.UniqueTransactionMembershipProven || r.FinancialUseEligible || r.TerminalEligible {
		t.Fatal(r, err)
	}
	after, _ := json.Marshal(s)
	if !reflect.DeepEqual(before, after) {
		t.Fatal("mutated prior evidence")
	}
	s.Window.Fields[0].ReportedWindowReady = false
	r, err = compareReceiptWindow(p, s)
	if err != nil || r.State != "blocked" || r.DetailMinorUnits != nil || r.SummaryComparisons[0].ReportedComparisonReady {
		t.Fatal(r, err)
	}
	s.Window.Fields[0].ReportedWindowReady = true
	s.Comparisons[0].Fields[0].Blockers = []string{"conflicting_reported_field"}
	r, err = compareReceiptWindow(p, s)
	if err != nil || r.State != "equal" || r.SummaryComparisons[0].State != "blocked" {
		t.Fatal("conflict isolation", r, err)
	}
	a, c := "9223372036854775808", "-9223372036854775808"
	d, state, err := receiptDifference(&a, &c)
	if err != nil || *d != "18446744073709551616" || state != "different" {
		t.Fatal(d, state)
	}
	if _, _, err := receiptDifference(nil, &c); err == nil {
		t.Fatal("accepted missing amount")
	}
	x = "invalid"
	if _, _, err := receiptDifference(&x, &c); err == nil {
		t.Fatal("accepted invalid amount")
	}
}

func TestReceiptWindowPolicy(t *testing.T) {
	raw, err := os.ReadFile("../../../../contracts/calculations/fec/receipt-reported-window/v1/policy.json")
	if err != nil {
		t.Fatal(err)
	}
	var policy struct {
		Version    string          `json:"version"`
		LinePolicy string          `json:"line_policy"`
		Field      string          `json:"reported_field"`
		Max        int             `json:"profile_max_bytes"`
		Guards     map[string]bool `json:"guards"`
	}
	if err := json.Unmarshal(raw, &policy); err != nil {
		t.Fatal(err)
	}
	if policy.Version != ReceiptWindowVersion || policy.LinePolicy != ReportLinePolicy || policy.Field != itemizedPeriodField || policy.Max != maxSavedProfileBytes || len(policy.Guards) != 4 {
		t.Fatal(policy)
	}
	for _, value := range policy.Guards {
		if value {
			t.Fatal("promoted eligibility")
		}
	}
}
