package reportscope

import (
	"bytes"
	"regexp"
	"time"

	"github.com/vedantadhobley/legal-tender/internal/source/fec/money"
)

var committeePattern = regexp.MustCompile(`^C[0-9]{8}$`)

func recordFields(r Record) ([]string, bool) {
	if !r.Complete {
		return nil, false
	}
	b := r.Raw
	if bytes.HasSuffix(b, []byte{'\n'}) {
		b = bytes.TrimSuffix(bytes.TrimSuffix(b, []byte{'\n'}), []byte{'\r'})
	}
	// No unreviewed single-byte mapping or replacement decoding. Raw always survives.
	for _, v := range b {
		if (v < 32 && v != 0x1c) || v > 126 {
			return nil, false
		}
	}
	fields := bytes.Split(b, []byte{0x1c})
	out := make([]string, len(fields))
	for i, f := range fields {
		out[i] = string(f)
	}
	return out, true
}

func (a *Assessment) assessPaper(pathKind string) {
	if len(a.Records) < 2 {
		a.Issues = append(a.Issues, "missing_header_or_cover")
		return
	}
	hdr, ok := recordFields(a.Records[0])
	if !ok || len(hdr) < 2 || hdr[0] != "HDR" || hdr[1] != "P3.4" {
		a.Issues = append(a.Issues, "unsupported_header_layout")
		return
	}
	a.Representation = "paper_transcription_p3.4"
	a.SchemaSHA256 = PaperSchemaSHA256
	// Workbook HDR has six logical fields. The retained dialect also emits one
	// empty delimiter field. No nonempty seventh field is silently discarded.
	if pathKind != "paper" || (len(hdr) != 6 && !(len(hdr) == 7 && hdr[6] == "")) {
		a.Issues = append(a.Issues, "paper_header_shape_or_url_mismatch")
		return
	}
	fields, ok := recordFields(a.Records[1])
	if !ok || len(fields) != 125 || (fields[0] != "F3XN" && fields[0] != "F3XA" && fields[0] != "F3XT") {
		a.Issues = append(a.Issues, "unsupported_or_incomplete_paper_cover")
		return
	}
	c := &Cover{RecordOrdinal: 2, Form: fields[0], CommitteeID: fields[1], ReportCode: fields[9],
		CoverageStart: fields[12], CoverageEnd: fields[13], Fields: fields,
		AmountPresence: "all_blank", Amounts: []AmountField{}}
	a.Cover = c
	start, e1 := time.Parse("20060102", c.CoverageStart)
	end, e2 := time.Parse("20060102", c.CoverageEnd)
	scopeOK := committeePattern.MatchString(c.CommitteeID) && c.ReportCode != "" && e1 == nil && e2 == nil && !end.Before(start)
	if !scopeOK {
		a.Issues = append(a.Issues, "invalid_cover_identity_or_period")
	}
	// Exact AMT-12 columns in the pinned P3.4 F3X sheet. 28 is a checkbox,
	// 73 is a year, and 123..125 are image/date fields, NOT money.
	for seq := 21; seq <= 122; seq++ {
		if seq == 28 || seq == 73 {
			continue
		}
		f := AmountField{Sequence: seq, Raw: fields[seq-1], State: "blank"}
		if f.Raw != "" {
			minor, _, issue := money.ParseUSDMinorUnits(f.Raw)
			f.State = "valid"
			f.MinorUnits = minor
			if c.AmountPresence == "all_blank" {
				c.AmountPresence = "populated"
			}
			if issue != "" {
				f.State = "invalid"
				f.MinorUnits = ""
				c.AmountPresence = "invalid"
			}
		}
		c.Amounts = append(c.Amounts, f)
	}
	if c.AmountPresence == "invalid" {
		a.Issues = append(a.Issues, "invalid_cover_amount")
		return
	}
	for _, r := range a.Records[2:] {
		f, valid := recordFields(r)
		if valid && len(f) > 0 && (f[0] == "F3XN" || f[0] == "F3XA" || f[0] == "F3XT") {
			a.Issues = append(a.Issues, "multiple_financial_covers")
			return
		}
	}
	// Presence is not a complete, arithmetically valid, or effective financial report.
	if scopeOK && c.AmountPresence == "populated" {
		a.Disposition = "financial_cover_present"
	}
	if a.CaptureExtent != "complete_response" || !scopeOK || c.AmountPresence != "all_blank" {
		return
	}
	if len(a.Records) < 3 {
		a.Issues = append(a.Issues, "blank_cover_without_supplement")
		return
	}
	for _, r := range a.Records[2:] {
		f, valid := recordFields(r)
		if !valid || len(f) != 47 || f[0] != "SC1" || f[1] != c.CommitteeID || f[2] == "" {
			a.Issues = append(a.Issues, "unqualified_supplement_record")
			return
		}
		// Require a substantive loan field, not an empty SC1 placeholder. Do not
		// reinterpret that loan as receipts or add it to any transaction ledger.
		if f[8] == "" {
			a.Issues = append(a.Issues, "missing_supplement_loan_amount")
			return
		}
		if _, _, issue := money.ParseUSDMinorUnits(f[8]); issue != "" {
			a.Issues = append(a.Issues, "invalid_supplement_loan_amount")
			return
		}
	}
	a.Disposition = "supplemental_attachment_shape"
	a.Basis = "complete_paper_transcription_blank_f3x_amounts_and_sc1_only"
}
