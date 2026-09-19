package committeesummary

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"hash"
	"io"
	"strconv"
	"strings"
)

const VerificationSchema = "legal-tender.committee-summary-verification.v1"
const maxIssueExamples = 10

type ValueProfile struct {
	Valid   uint64 `json:"valid"`
	Blank   uint64 `json:"blank"`
	Invalid uint64 `json:"invalid"`
}

type MoneyProfile struct {
	ValueProfile
	Positive       uint64 `json:"positive"`
	Negative       uint64 `json:"negative"`
	Zero           uint64 `json:"zero"`
	LeadingDecimal uint64 `json:"leading_decimal"`
}

type IntervalProfile struct {
	ValidEndpoints       uint64 `json:"valid_endpoints"`
	UnavailableEndpoints uint64 `json:"unavailable_endpoints"`
	Reversed             uint64 `json:"reversed"`
	StartBeforeCycle     uint64 `json:"start_before_cycle"`
	EndAfterCycle        uint64 `json:"end_after_cycle"`
	StartAfterCycleStart uint64 `json:"start_after_cycle_start"`
}

type LocatedIssue struct {
	Ordinal   uint64 `json:"ordinal"`
	Offset    int64  `json:"offset"`
	RawSHA256 string `json:"raw_sha256"`
	Issue
}

type Verification struct {
	SchemaVersion               string                      `json:"schema_version"`
	SourceContract              string                      `json:"source_contract"`
	ParserVersion               string                      `json:"parser_version"`
	Complete                    bool                        `json:"complete"`
	Expected                    Expected                    `json:"expected"`
	HeaderSHA256                string                      `json:"header_sha256"`
	HeaderBytes                 int64                       `json:"header_bytes"`
	RecordBytes                 int64                       `json:"record_bytes"`
	Rows                        uint64                      `json:"rows"`
	RowsWithIssues              uint64                      `json:"rows_with_issues"`
	FieldsSHA256                string                      `json:"fields_sha256"`
	TypedValuesSHA256           string                      `json:"typed_values_sha256"`
	Money                       map[string]*MoneyProfile    `json:"money"`
	Dates                       map[string]*ValueProfile    `json:"dates"`
	Identifiers                 map[string]*ValueProfile    `json:"identifiers"`
	Intervals                   IntervalProfile             `json:"intervals"`
	Multiplicity                Multiplicity                `json:"multiplicity"`
	Equations                   map[string]*EquationProfile `json:"diagnostic_equations"`
	IssueCounts                 map[string]uint64           `json:"issue_counts"`
	IssueExamples               map[string][]LocatedIssue   `json:"issue_examples"`
	MaxIssueExamplesPerCode     int                         `json:"max_issue_examples_per_code"`
	CodeInterpretation          string                      `json:"code_interpretation"`
	TerminalAttributionEligible bool                        `json:"terminal_attribution_eligible"`
}

// Verify delivers transient records only to an optional evidence observer.
// Any physical, identity-partition, read, callback, or cancellation failure
// returns no successful result. No mutable publication or financial total exists.
func Verify(ctx context.Context, input io.Reader, expected Expected, observe func(*Record) error) (Verification, error) {
	reader, err := NewReader(ctx, input, expected)
	if err != nil {
		return Verification{}, err
	}
	out := Verification{
		SchemaVersion: VerificationSchema, SourceContract: SourceContract, ParserVersion: ParserVersion,
		Expected: expected, HeaderSHA256: HeaderSHA256, HeaderBytes: reader.csv.InputOffset(),
		Money: map[string]*MoneyProfile{}, Dates: map[string]*ValueProfile{}, Identifiers: map[string]*ValueProfile{},
		IssueCounts: map[string]uint64{}, IssueExamples: map[string][]LocatedIssue{}, MaxIssueExamplesPerCode: maxIssueExamples,
		Equations: map[string]*EquationProfile{"cash": {}, "individual": {}}, CodeInterpretation: "preserved_not_interpreted",
	}
	for _, name := range moneyFields {
		out.Money[name] = &MoneyProfile{}
	}
	for _, name := range dateFields {
		out.Dates[name] = &ValueProfile{}
	}
	for _, name := range identityFields {
		out.Identifiers[name] = &ValueProfile{}
	}
	fieldsHash, typedHash := sha256.New(), sha256.New()
	identities := newIdentityIndex()
	for {
		row, err := reader.Next(ctx)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return Verification{}, err
		}
		out.Rows++
		out.RecordBytes += row.Length
		if len(row.Issues) > 0 {
			out.RowsWithIssues++
		}
		for _, issue := range row.Issues {
			out.IssueCounts[issue.Code]++
			if len(out.IssueExamples[issue.Code]) < maxIssueExamples {
				out.IssueExamples[issue.Code] = append(out.IssueExamples[issue.Code], LocatedIssue{Ordinal: row.Ordinal, Offset: row.Offset, RawSHA256: row.RawSHA256, Issue: issue})
			}
		}
		for _, c := range columns {
			hashString(fieldsHash, row.SourceFields[c.name])
		}
		for _, name := range moneyFields {
			value := row.Money[name]
			profile := out.Money[name]
			profile.ValueProfile.add(value.State)
			if strings.HasPrefix(row.SourceFields[name], ".") || strings.HasPrefix(row.SourceFields[name], "-.") {
				profile.LeadingDecimal++
			}
			if value.State == Valid {
				switch {
				case *value.MinorUnits == "0":
					profile.Zero++
				case strings.HasPrefix(*value.MinorUnits, "-"):
					profile.Negative++
				default:
					profile.Positive++
				}
			}
			hashString(typedHash, value.State)
			hashString(typedHash, stringValue(value.MinorUnits))
			hashString(typedHash, strconv.Itoa(value.SourceScale))
		}
		for _, name := range dateFields {
			value := row.Dates[name]
			out.Dates[name].add(value.State)
			hashString(typedHash, value.State)
			hashString(typedHash, stringValue(value.Value))
		}
		for _, name := range identityFields {
			value := row.Identifiers[name]
			out.Identifiers[name].add(value.State)
			hashString(typedHash, value.State)
			hashString(typedHash, stringValue(value.Value))
		}
		out.Intervals.add(row, expected.Cycle)
		identities.add(row)
		out.Equations["cash"].add(row, []string{"COH_BOP", "TTL_RECEIPTS", "TTL_DISB", "COH_COP"}, true)
		out.Equations["individual"].add(row, []string{"INDV_ITEM_CONTB", "INDV_UNITEM_CONTB", "INDV_CONTB"}, false)
		if observe != nil {
			if err := observe(row); err != nil {
				return Verification{}, err
			}
		}
	}
	if err := ctx.Err(); err != nil {
		return Verification{}, err
	}
	if out.Rows == 0 {
		return Verification{}, errors.New("committee summary has no data records")
	}
	if out.HeaderBytes+out.RecordBytes != expected.Bytes {
		return Verification{}, errors.New("committee summary byte conservation failed")
	}
	if out.IssueCounts["cycle_mismatch"] != 0 || out.Identifiers["FEC_ELECTION_YR"].Valid != out.Rows {
		return Verification{}, errors.New("committee summary does not prove the expected cycle for every record")
	}
	out.Multiplicity = identities.profile()
	out.FieldsSHA256, out.TypedValuesSHA256 = hex.EncodeToString(fieldsHash.Sum(nil)), hex.EncodeToString(typedHash.Sum(nil))
	out.Complete = true
	return out, nil
}

func (p *ValueProfile) add(state string) {
	switch state {
	case Valid:
		p.Valid++
	case Blank:
		p.Blank++
	case Invalid:
		p.Invalid++
	}
}

func (p *IntervalProfile) add(row *Record, cycle string) {
	start, end := row.Dates["CVG_START_DT"], row.Dates["CVG_END_DT"]
	if start.State != Valid || end.State != Valid {
		p.UnavailableEndpoints++
		return
	}
	p.ValidEndpoints++
	year, _ := cycleYear(cycle)
	first, last := strconv.Itoa(year-1)+"-01-01", cycle+"-12-31"
	// Pad early four-digit source cycles as well; no hardcoded target window.
	if len(first) < 10 {
		first = strings.Repeat("0", 10-len(first)) + first
	}
	if *start.Value > *end.Value {
		p.Reversed++
	}
	if *start.Value < first {
		p.StartBeforeCycle++
	}
	if *start.Value > first {
		p.StartAfterCycleStart++
	}
	if *end.Value > last {
		p.EndAfterCycle++
	}
}

// Hash contracts encode UTF-8 byte length as uint64 little endian, then text.
// Fixed column order and row order avoid delimiter collisions and map order.
func hashString(h hash.Hash, value string) {
	var size [8]byte
	binary.LittleEndian.PutUint64(size[:], uint64(len(value)))
	_, _ = h.Write(size[:])
	_, _ = io.WriteString(h, value)
}

func stringValue(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}
