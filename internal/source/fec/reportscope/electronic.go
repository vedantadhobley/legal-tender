package reportscope

import (
	"bytes"
	"context"
	"errors"
	"slices"
	"strings"
	"time"

	"github.com/vedantadhobley/legal-tender/internal/source/fec/money"
)

const ElectronicVersion = "legal-tender.fec.electronic-cover.v1"
const ElectronicSchemaSHA256 = "9d3775d73e9398144b0e0267415ba53e1b5c6a326110b327ce2cd58d233bf3d6"

// Repeated positions are independent source assertions of the same period field.
// Preserve each one; a consumer must not silently choose between conflicting values.
type ElectronicField struct {
	Name    string        `json:"name"`
	Amounts []AmountField `json:"amounts"`
}

type ElectronicAssessment struct {
	Assessment
	Header       []string          `json:"header_fields"`
	PeriodFields []ElectronicField `json:"period_fields"`
}

// AssessElectronic accepts only the pinned 8.4 F3/F3X cover layouts. Other
// records remain byte evidence, not parsed schedules or a complete filing model.
func AssessElectronic(ctx context.Context, request Request) (ElectronicAssessment, error) {
	if len(request.MetadataCaptures) != 0 {
		return ElectronicAssessment{}, errors.New("electronic cover reader does not accept metadata captures")
	}
	a, err := readDocument(ctx, request)
	if err != nil {
		return ElectronicAssessment{}, err
	}
	a.Version = ElectronicVersion
	out := ElectronicAssessment{Assessment: a, Header: []string{}, PeriodFields: []ElectronicField{}}
	out.assess84(documentURL.FindStringSubmatch(request.SourceURL)[1])
	slices.Sort(out.Issues)
	out.Issues = slices.Compact(out.Issues)
	return out, nil
}

func electronicForm(s string) bool {
	return slices.Contains([]string{"F3N", "F3A", "F3T", "F3XN", "F3XA", "F3XT"}, s)
}

func (a *ElectronicAssessment) assess84(pathKind string) {
	if len(a.Records) < 2 {
		a.Issues = append(a.Issues, "missing_header_or_cover")
		return
	}
	h, ok := recordFields(a.Records[0])
	// Column 8 is the optional HDR comment. Preserve a physically omitted
	// trailing comment distinctly; no general short-row padding is allowed.
	if !ok || (len(h) != 7 && len(h) != 8) || h[0] != "HDR" || h[1] != "FEC" || h[2] != "8.4" || pathKind != "dcdev" {
		a.Issues = append(a.Issues, "unsupported_electronic_header")
		return
	}
	a.Header = h
	a.Representation = "electronic_8.4"
	a.SchemaSHA256 = ElectronicSchemaSHA256
	f, ok := recordFields(a.Records[1])
	if !ok || len(f) == 0 || !electronicForm(f[0]) {
		a.Issues = append(a.Issues, "unsupported_or_incomplete_electronic_cover")
		return
	}
	f3 := strings.HasPrefix(f[0], "F3") && !strings.HasPrefix(f[0], "F3X")
	width, report, start, end, first := 123, 10, 14, 15, 23
	if f3 {
		width, report, start, end, first = 93, 12, 16, 17, 24
	}
	if len(f) != width {
		a.Issues = append(a.Issues, "electronic_cover_width_mismatch")
		return
	}
	c := &Cover{RecordOrdinal: 2, Form: f[0], CommitteeID: f[1], ReportCode: f[report-1], CoverageStart: f[start-1], CoverageEnd: f[end-1], Fields: f, Amounts: []AmountField{}, AmountPresence: "all_blank"}
	a.Cover = c
	lo, e1 := time.Parse("20060102", c.CoverageStart)
	hi, e2 := time.Parse("20060102", c.CoverageEnd)
	if !committeePattern.MatchString(c.CommitteeID) || c.ReportCode == "" || e1 != nil || e2 != nil || hi.Before(lo) {
		a.Issues = append(a.Issues, "invalid_cover_identity_or_period")
	}
	for seq := first; seq <= width; seq++ {
		if !f3 && seq == 75 {
			continue
		} // Column B year, not AMT-12.
		v := AmountField{Sequence: seq, Raw: f[seq-1], State: "blank"}
		if v.Raw != "" {
			var issue string
			v.MinorUnits, _, issue = money.ParseUSDMinorUnits(v.Raw)
			v.State = "valid"
			if c.AmountPresence == "all_blank" {
				c.AmountPresence = "populated"
			}
			if issue != "" {
				v.State, v.MinorUnits, c.AmountPresence = "invalid", "", "invalid"
			}
		}
		c.Amounts = append(c.Amounts, v)
	}
	// Check the record tag even if the remaining fields cannot be decoded.
	// Invalid amounts or text must not conceal a second cover.
	for _, r := range a.Records[2:] {
		tag := bytes.SplitN(r.Raw, []byte{0x1c}, 2)[0]
		name := strings.TrimRight(string(tag), "\r\n")
		if electronicForm(name) {
			a.Issues = append(a.Issues, "multiple_financial_covers")
		} else if name == "HDR" || strings.HasPrefix(name, "F") {
			a.Issues = append(a.Issues, "additional_unqualified_form_or_header")
		}
	}
	names := []string{"individual_itemized_contributions_period", "individual_unitemized_contributions_period", "total_individual_contributions_period", "total_receipts_period", "total_disbursements_period", "cash_on_hand_beginning_period", "cash_on_hand_end_period"}
	positions := [][]int{{30}, {31}, {32}, {24, 45}, {26, 66}, {23}, {27}}
	if f3 {
		positions = [][]int{{33}, {34}, {35}, {46, 59}, {57, 61}, {58}, {30, 62}}
	}
	for i, name := range names {
		field := ElectronicField{Name: name, Amounts: []AmountField{}}
		for _, seq := range positions[i] {
			for _, amount := range c.Amounts {
				if amount.Sequence == seq {
					field.Amounts = append(field.Amounts, amount)
				}
			}
		}
		a.PeriodFields = append(a.PeriodFields, field)
	}
	// Invalid money remains local to its field. Scope issues block all bindings.
	if len(a.Issues) == 0 {
		a.Disposition = "electronic_cover_parsed"
	}
}
