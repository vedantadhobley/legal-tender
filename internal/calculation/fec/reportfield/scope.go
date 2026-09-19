package reportfield

import (
	"bytes"
	"encoding/json"
	"strings"
	"time"

	"github.com/vedantadhobley/legal-tender/internal/source/fec/reportscope"
)

// Shared by the two concrete report-field reviews. Scope checks do not choose
// financial versions or require unrelated amount fields to be valid.
func paperCoverScope(a reportscope.Assessment) (*Period, []string) {
	blockers := []string{}
	if a.Representation != "paper_transcription_p3.4" || a.SchemaSHA256 != reportscope.PaperSchemaSHA256 || a.Cover == nil {
		return nil, append(blockers, "unqualified_cover_layout")
	}
	if a.CaptureExtent != "complete_response" {
		blockers = append(blockers, "incomplete_document")
	}
	// Detect extra covers even if an unrelated invalid amount stopped the
	// source reader's later whole-document classification.
	coverCount := 0
	for _, record := range a.Records {
		form, _, _ := bytes.Cut(record.Raw, []byte{0x1c})
		if bytes.HasPrefix(form, []byte("F3X")) {
			coverCount++
		}
	}
	if coverCount != 1 {
		blockers = append(blockers, "multiple_financial_covers")
	}
	for _, issue := range a.Issues {
		switch issue {
		case "invalid_cover_identity_or_period", "multiple_financial_covers":
			blockers = append(blockers, issue)
		}
	}
	start, e1 := time.Parse("20060102", a.Cover.CoverageStart)
	end, e2 := time.Parse("20060102", a.Cover.CoverageEnd)
	if e1 == nil && e2 == nil && !end.Before(start) {
		return &Period{start.Format("2006-01-02"), end.Format("2006-01-02")}, blockers
	}
	return nil, blockers
}

func metadataMatchesCover(a reportscope.Assessment, fields map[string]json.RawMessage, formField, formValue string) []string {
	blockers := []string{}
	if a.Cover == nil {
		return blockers
	}
	for field, want := range map[string]string{"committee_id": a.Cover.CommitteeID, formField: formValue,
		"report_type": a.Cover.ReportCode, "means_filed": "paper", "amendment_indicator": a.Cover.Form[3:]} {
		if stringValue(fields[field]) != want {
			blockers = append(blockers, "metadata_scope:"+field)
		}
	}
	for field, want := range map[string]string{"coverage_start_date": a.Cover.CoverageStart, "coverage_end_date": a.Cover.CoverageEnd} {
		date := metadataDate(fields[field])
		if date == "" || strings.ReplaceAll(date, "-", "") != want {
			blockers = append(blockers, "metadata_scope:"+field)
		}
	}
	// A conflicting URL matters; null cannot override the exact file/filer join.
	if raw := fields["fec_url"]; len(raw) > 0 && string(raw) != "null" && stringValue(raw) != a.SourceURL {
		blockers = append(blockers, "metadata_scope:fec_url")
	}
	return blockers
}

func metadataDate(raw json.RawMessage) string {
	s := strings.TrimSuffix(stringValue(raw), "T00:00:00")
	d, err := time.Parse("2006-01-02", s)
	if err != nil || d.Format("2006-01-02") != s {
		return ""
	}
	return s
}
