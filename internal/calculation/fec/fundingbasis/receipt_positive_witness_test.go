package fundingbasis

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/vedantadhobley/legal-tender/internal/calculation/fec/summaryassertion"
	"github.com/vedantadhobley/legal-tender/internal/source/fec/reportscope"
)

// Source gate only: no fabricated metadata or financial report-scope binding.
// Candidate/file identities and amounts belong in the pinned audit fixture,
// never in runtime dispatch or financial selection rules.
func TestPositiveReceiptFamilyOriginals(t *testing.T) {
	root, audit := os.Getenv("LT_POSITIVE_FAMILY_STORAGE"), os.Getenv("LT_POSITIVE_FAMILY_AUDIT")
	testPositiveReceiptFamilyOriginals(t, root, audit, "positive-receipt-families-2026-09-11.json", false)
}

func TestPositiveReceiptFamilyV2Originals(t *testing.T) {
	root, audit := os.Getenv("LT_POSITIVE_FAMILY_V2_STORAGE"), os.Getenv("LT_POSITIVE_FAMILY_V2_AUDIT")
	testPositiveReceiptFamilyOriginals(t, root, audit, "positive-receipt-families-v2-2026-09-11.json", true)
}

func testPositiveReceiptFamilyOriginals(t *testing.T, root, audit, fixtureName string, v2 bool) {
	t.Helper()
	if root == "" || audit == "" {
		t.Skip("requires retained originals, summary publication and saved profile")
	}
	var fixture struct {
		Cycle             string
		ProfileStorageKey string `json:"profile_storage_key"`
		ProfileSHA256     string `json:"profile_sha256"`
		SummaryFactSet    string `json:"summary_fact_set"`
		Cases             []struct {
			ExpectedUnreviewedRecords            map[string]int `json:"expected_unreviewed_records"`
			File, Committee, Form, Body, Headers string
			Retained                             bool
			BodySHA256                           string `json:"body_sha256"`
			HeadersSHA256                        string `json:"headers_sha256"`
			Targets                              map[string]string
			DetailTargets                        map[string]string `json:"detail_targets"`
		}
	}
	raw, err := os.ReadFile(filepath.Join("../../../../docs/audit/fixtures", fixtureName))
	if err != nil || json.Unmarshal(raw, &fixture) != nil {
		t.Fatal("invalid source fixture", err)
	}
	ctx := context.Background()
	assess, fields := reportscope.AssessReceiptFamilies, reportscope.ReceiptFamilyFields
	version := "legal-tender.audit.positive-receipt-family-source.v1"
	if v2 {
		assess, fields = reportscope.AssessReceiptFamiliesV2, reportscope.ReceiptFamilyFieldsV2
		version = "legal-tender.audit.positive-receipt-family-source.v2"
	}
	s, err := summaryassertion.Run(ctx, root, filepath.Join(root, "facts/fec/committee-summary/v1/manifests", fixture.SummaryFactSet+".json"), fixture.Cycle)
	if err != nil {
		t.Fatal(err)
	}
	p, identity, err := readLineProfile(ctx, filepath.Join(root, fixture.ProfileStorageKey), fixture.ProfileSHA256, root, summaryassertion.WindowComparison{Cycle: s.Cycle, SummaryInput: s.Input, SummaryCalculationID: s.CalculationID})
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range fixture.Cases {
		t.Run(c.File, func(t *testing.T) {
			base := audit
			if c.Retained {
				base = root
			}
			q := reportscope.Request{SourceURL: "https://docquery.fec.gov/dcdev/posted/" + c.File + ".fec", BodyPath: filepath.Join(base, c.Body), BodySHA256: c.BodySHA256, HeadersPath: filepath.Join(base, c.Headers), HeadersSHA256: c.HeadersSHA256}
			a, err := assess(ctx, q)
			if err != nil {
				t.Fatal(err)
			}
			if a.Cover == nil || a.Cover.CommitteeID != c.Committee || reportscope.ReceiptFamilyCoverForm(a.Cover.Form) != c.Form || a.CaptureExtent != "complete_response" || a.Disposition != "electronic_cover_parsed" || len(a.Issues) != 0 || a.HistoryComplete || a.FinancialSelectionReady || len(a.Metadata) != 0 {
				t.Fatal("unexpected original scope", a.Issues)
			}
			replay, err := assess(ctx, q)
			if err != nil || !reflect.DeepEqual(a, replay) {
				t.Fatal("unstable original replay", err)
			}
			census, err := reportscope.InventoryScheduleALines(ctx, q)
			if err != nil || len(census.ScopeIssues) != 0 || census.Complete != (len(c.ExpectedUnreviewedRecords) == 0) {
				t.Fatal("unexpected layout census", census.Issues, err)
			}
			unreviewed := map[string]int{}
			for _, issue := range census.Issues {
				if issue.Code != "unreviewed_record_family" || issue.RecordOrdinal < 3 || issue.RecordOrdinal > len(a.Records) {
					t.Fatal("unexpected census issue", issue)
				}
				tag := string(bytes.SplitN(a.Records[issue.RecordOrdinal-1].Raw, []byte{0x1c}, 2)[0])
				unreviewed[tag]++
			}
			if len(unreviewed) != len(c.ExpectedUnreviewedRecords) {
				t.Fatal("changed unreviewed record population", unreviewed)
			}
			for tag, count := range unreviewed {
				if c.ExpectedUnreviewedRecords[tag] != count {
					t.Fatal("changed unreviewed record count", tag, count)
				}
			}
			groups := []ReportLineProfileGroup{}
			var rows uint64
			for _, g := range p.Reports {
				if g.Key.Committee == (Cell{true, c.Committee}) && g.Key.File == (Cell{true, c.File}) {
					groups = append(groups, g)
					rows += g.Measures.Rows
				}
			}
			var originalRows uint64
			for _, line := range census.Lines {
				originalRows += uint64(len(line.RecordOrdinals))
			}
			if rows == 0 || rows != originalRows {
				t.Fatal("original/profile row conservation", rows, originalRows)
			}
			matched := 0
			for _, spec := range fields(c.Form) {
				expected, selected := c.Targets[spec.ID]
				if !selected {
					continue
				}
				cover := ""
				for _, field := range a.PeriodFields {
					if field.Name == spec.MetadataField {
						want := 1
						if spec.CorroboratingSequence != 0 {
							want = 2
						}
						if len(field.Amounts) != want {
							t.Fatal("missing original amount", spec.ID)
						}
						for _, value := range field.Amounts {
							if value.State != "valid" || value.MinorUnits != expected {
								t.Fatal("changed cover assertion", spec.ID, value)
							}
						}
						cover = expected
					}
				}
				m := Measures{}
				for _, g := range groups {
					if g.Key.Form != (Cell{true, c.Form}) || g.Key.Schedule != (Cell{true, "SA"}) || g.Key.Line != (Cell{true, spec.Line}) || g.Key.Memo.Value != "" {
						continue
					}
					if err := m.merge(g.Measures); err != nil {
						t.Fatal(err)
					}
				}
				detail := expected
				if v2 {
					var ok bool
					detail, ok = c.DetailTargets[spec.ID]
					if !ok {
						t.Fatal("missing separate detail expectation", spec.ID)
					}
				}
				if cover != expected || m.Rows == 0 || m.Unknown != 0 || m.Signed <= 0 || strconv.FormatInt(m.Signed, 10) != detail {
					t.Fatal("positive original/profile witness changed", spec.ID, cover, m)
				}
				matched++
			}
			if matched != len(c.Targets) || matched == 0 {
				t.Fatal("missing target family")
			}
			bad := q
			bad.BodySHA256 = strings.Repeat("0", 64)
			if _, err := assess(ctx, bad); err == nil {
				t.Fatal("accepted changed original pin")
			}
			// Persist only the complete bounded original assessment/census and
			// exact selected groups. The full profile remains in its prior audit.
			result := map[string]any{"version": version, "assessment": a, "census": census, "profile": identity, "profile_id": p.ProfileID, "summary_input": p.SummaryInput, "schedule_a_source": p.Source, "profile_groups": groups, "target_values_minor_units": c.Targets, "metadata_binding_proven": false, "financial_use_eligible": false, "terminal_attribution_eligible": false}
			if v2 {
				result["detail_target_values_minor_units"] = c.DetailTargets
			}
			body, err := json.MarshalIndent(result, "", "  ")
			if err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(audit, c.File+"-source.json")
			if prior, err := os.ReadFile(path); err == nil {
				if string(prior) != string(append(body, '\n')) {
					t.Fatal("retained source result changed")
				}
			} else if !os.IsNotExist(err) {
				t.Fatal(err)
			} else if err := os.WriteFile(path, append(body, '\n'), 0o640); err != nil {
				t.Fatal(err)
			}
			t.Logf("file=%s form=%s records=%d SA=%d positive_families=%d", c.File, c.Form, len(a.Records), rows, matched)
		})
	}
}
