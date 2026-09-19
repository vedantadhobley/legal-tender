package reportscope

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/vedantadhobley/legal-tender/internal/source/fec/reportmetadata"
)

// Differences are literal source assertions, including null versus false and
// number versus string. They are not all semantic conflicts. Raw complete records
// remain attached, with page/body/record ancestry and no status-field precedence.
var comparedFields = []string{"committee_id", "form_type", "report_form", "report_type", "coverage_start_date", "coverage_end_date",
	"amendment_indicator", "is_amended", "most_recent", "previous_file_number", "amendment_chain"}

func (a *Assessment) metadata(ctx context.Context, paths []string) error {
	seen := map[string]bool{}
	for _, path := range paths {
		r, err := reportmetadata.ReadCapture(ctx, path)
		if err != nil {
			return err
		}
		if r.State != "validated_observations" {
			return errors.New("metadata capture has blocking issues")
		}
		if seen[r.CaptureSHA256] {
			return errors.New("repeated metadata capture")
		}
		seen[r.CaptureSHA256] = true
		input := MetadataInput{CaptureSHA256: r.CaptureSHA256, Endpoint: r.Endpoint, PaginationState: r.PaginationState, Rows: r.Rows}
		for _, page := range r.Pages {
			for _, record := range page.Records {
				if record.FileNumber != a.FileNumber {
					continue
				}
				a.Metadata = append(a.Metadata, Assertion{r.CaptureSHA256, r.Endpoint, page.Capture, r.PaginationState, record})
				input.MatchingRows++
			}
		}
		a.MetadataInputs = append(a.MetadataInputs, input)
	}
	values := make([]map[string]json.RawMessage, len(a.Metadata))
	for i, assertion := range a.Metadata {
		if err := json.Unmarshal(assertion.Record.Raw, &values[i]); err != nil {
			return errors.New("invalid verified metadata record")
		}
		if a.Cover == nil {
			continue
		}
		for field, want := range map[string]string{"committee_id": a.Cover.CommitteeID, "form_type": "F3X", "report_form": "Form 3X", "report_type": a.Cover.ReportCode,
			"means_filed": "paper", "fec_url": a.SourceURL, "amendment_indicator": a.Cover.Form[3:],
			"coverage_start_date": a.Cover.CoverageStart, "coverage_end_date": a.Cover.CoverageEnd} {
			raw, present := values[i][field]
			if !present || string(raw) == "null" {
				continue
			}
			var got string
			if err := json.Unmarshal(raw, &got); err != nil {
				return errors.New("invalid verified metadata field")
			}
			if field == "coverage_start_date" || field == "coverage_end_date" {
				if d, err := time.Parse("20060102", want); err == nil {
					want = d.Format("2006-01-02")
				}
				// The qualified endpoint representation is a date or midnight
				// without a zone. Other timestamps stay different, never truncated.
				got = strings.TrimSuffix(got, "T00:00:00")
			}
			if got != want {
				a.Issues = append(a.Issues, "metadata_cover_difference:"+field)
			}
		}
	}
	for _, field := range comparedFields {
		d := Difference{Field: field, AssertionIndexes: []int{}, Values: []json.RawMessage{}}
		unique := map[string]bool{}
		for i, fields := range values {
			if raw, ok := fields[field]; ok {
				d.AssertionIndexes = append(d.AssertionIndexes, i)
				d.Values = append(d.Values, raw)
				unique[string(raw)] = true
			}
		}
		if len(unique) > 1 {
			a.Differences = append(a.Differences, d)
		}
	}
	if len(a.Metadata) == 0 {
		a.Issues = append(a.Issues, "no_matching_metadata_observation")
	}
	return nil
}
