package reportscope

import (
	"context"
	"regexp"
	"slices"
	"strings"

	"github.com/vedantadhobley/legal-tender/internal/source/fec/reportmetadata"
)

const LineCensusVersion = "legal-tender.fec.electronic-sa-line-census.v1"

type CensusLine struct {
	Tag            string `json:"tag"`
	RecordOrdinals []int  `json:"record_ordinals"`
}

type CensusIssue struct {
	RecordOrdinal int    `json:"record_ordinal"`
	Code          string `json:"code"`
}

type LineCensus struct {
	Version      string                  `json:"version"`
	SchemaSHA256 string                  `json:"schema_sha256"`
	Body         reportmetadata.Artifact `json:"body"`
	Headers      reportmetadata.Artifact `json:"headers"`
	Form         string                  `json:"form"`
	Lines        []CensusLine            `json:"schedule_a_lines"`
	OtherRecords []CensusLine            `json:"other_records"`
	Issues       []CensusIssue           `json:"issues"`
	ScopeIssues  []string                `json:"scope_issues"`
	Complete     bool                    `json:"layout_census_complete"`
}

// Known SALine meanings come from the reviewed form-specific receipt map.
func ScheduleALines(form string) []string {
	switch form {
	case "F3":
		return []string{"11AI", "11B", "11C", "11D", "12", "13A", "13B", "14", "15"}
	case "F3X":
		return []string{"11AI", "11B", "11C", "12", "13", "14", "15", "16", "17"}
	default:
		return nil
	}
}

// These only identify non-SA records. Their monetary fields are not normalized
// or accepted here. Other schedules need a reviewed census extension.
var censusSB = regexp.MustCompile(`^SB[0-9][0-9A-Z]{0,5}$`)
var censusSC = regexp.MustCompile(`^SC/[0-9]{1,2}$`)

// InventoryScheduleALines re-verifies the original; it accepts no saved census
// or caller-supplied completeness decision. Existing cover output is unchanged.
func InventoryScheduleALines(ctx context.Context, request Request) (LineCensus, error) {
	d, err := AssessElectronic(ctx, request)
	if err != nil {
		return LineCensus{}, err
	}
	return censusScheduleA(ctx, d)
}

func censusScheduleA(ctx context.Context, d ElectronicAssessment) (LineCensus, error) {
	r := LineCensus{Version: LineCensusVersion, SchemaSHA256: d.SchemaSHA256, Body: d.Body, Headers: d.Headers,
		Lines: []CensusLine{}, OtherRecords: []CensusLine{}, Issues: []CensusIssue{}, ScopeIssues: slices.Clone(d.Issues)}
	if d.Cover != nil {
		r.Form = ReceiptFamilyCoverForm(d.Cover.Form)
	}
	if r.Form == "" || d.SchemaSHA256 != ElectronicSchemaSHA256 || d.Representation != "electronic_8.4" || d.Disposition != "electronic_cover_parsed" || d.CaptureExtent != "complete_response" || len(d.Records) < 2 {
		r.ScopeIssues = append(r.ScopeIssues, "unqualified_complete_electronic_original")
	}
	lines, others := map[string][]int{}, map[string][]int{}
	for i, row := range d.Records {
		if err := ctx.Err(); err != nil {
			return LineCensus{}, err
		}
		if i < 2 {
			continue
		} // Already checked by the cover reader.
		f, ok := recordFields(row)
		if !ok || len(f) < 2 {
			r.Issues = append(r.Issues, CensusIssue{row.Ordinal, "unreadable_record_structure"})
			continue
		}
		if d.Cover == nil || f[1] != d.Cover.CommitteeID {
			r.Issues = append(r.Issues, CensusIssue{row.Ordinal, "record_filer_mismatch"})
		}
		tag, width := f[0], 0
		if strings.HasPrefix(tag, "SA") {
			lines[tag] = append(lines[tag], row.Ordinal)
			width = 45
			if !slices.Contains(ScheduleALines(r.Form), strings.TrimPrefix(tag, "SA")) {
				r.Issues = append(r.Issues, CensusIssue{row.Ordinal, "unreviewed_sa_line"})
			}
		} else {
			others[tag] = append(others[tag], row.Ordinal)
			switch {
			case censusSB.MatchString(tag):
				width = 44
			case censusSC.MatchString(tag):
				width = 38
			case tag == "TEXT":
				width = 6
			default:
				r.Issues = append(r.Issues, CensusIssue{row.Ordinal, "unreviewed_record_family"})
			}
		}
		if width != 0 && len(f) != width {
			r.Issues = append(r.Issues, CensusIssue{row.Ordinal, "record_width_mismatch"})
		}
	}
	for tag, ordinals := range lines {
		r.Lines = append(r.Lines, CensusLine{tag, ordinals})
	}
	for tag, ordinals := range others {
		r.OtherRecords = append(r.OtherRecords, CensusLine{tag, ordinals})
	}
	slices.SortFunc(r.Lines, func(a, b CensusLine) int { return strings.Compare(a.Tag, b.Tag) })
	slices.SortFunc(r.OtherRecords, func(a, b CensusLine) int { return strings.Compare(a.Tag, b.Tag) })
	slices.Sort(r.ScopeIssues)
	r.ScopeIssues = slices.Compact(r.ScopeIssues)
	r.Complete = len(r.ScopeIssues) == 0 && len(r.Issues) == 0
	return r, nil
}
