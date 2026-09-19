package committeesummary

import (
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/vedantadhobley/legal-tender/internal/source/fec/money"
)

const (
	Valid   = "valid"
	Blank   = "source_blank"
	Invalid = "invalid"
)

type Value struct {
	State string  `json:"state"`
	Value *string `json:"value"`
}

type MoneyValue struct {
	State       string  `json:"state"`
	MinorUnits  *string `json:"minor_units"`
	SourceScale int     `json:"source_scale"`
}

type Issue struct {
	Code  string `json:"code"`
	Field string `json:"field"`
}

type Record struct {
	Ordinal      uint64                `json:"ordinal"`
	Offset       int64                 `json:"offset"`
	Length       int64                 `json:"length"`
	RawSHA256    string                `json:"raw_sha256"`
	SourceFields map[string]string     `json:"source_fields"`
	Money        map[string]MoneyValue `json:"money"`
	Dates        map[string]Value      `json:"dates"`
	Identifiers  map[string]Value      `json:"identifiers"`
	Issues       []Issue               `json:"issues"`
}

var (
	decimalPattern   = regexp.MustCompile(`^-?(?:[0-9]+(?:\.[0-9]{1,2})?|\.[0-9]{1,2})$`)
	committeePattern = regexp.MustCompile(`^C[0-9]{8}$`)
	candidatePattern = regexp.MustCompile(`^[HPS][A-Z0-9]{8}$`)
	yearPattern      = regexp.MustCompile(`^[0-9]{4}$`)
	datePattern      = regexp.MustCompile(`^[0-9]{8}$`)
)

func cycleYear(value string) (int, bool) {
	if !yearPattern.MatchString(value) {
		return 0, false
	}
	year, err := strconv.Atoi(value)
	return year, err == nil && year >= 2
}

func parseMoney(raw string) (MoneyValue, string) {
	if raw == "" {
		return MoneyValue{State: Blank}, ""
	}
	if !decimalPattern.MatchString(raw) {
		return MoneyValue{State: Invalid}, "invalid_money_syntax"
	}
	// This source explicitly permits leading-decimal lexemes. Add the integer
	// zero only for conversion; SourceFields retains the exact publisher text.
	lexeme := raw
	if strings.HasPrefix(lexeme, ".") {
		lexeme = "0" + lexeme
	}
	if strings.HasPrefix(lexeme, "-.") {
		lexeme = "-0" + lexeme[1:]
	}
	minor, scale, issue := money.ParseUSDMinorUnits(lexeme)
	if issue != "" {
		return MoneyValue{State: Invalid, SourceScale: scale}, "money_" + issue
	}
	return MoneyValue{State: Valid, MinorUnits: &minor, SourceScale: scale}, ""
}

func parseDate(raw string) Value {
	if raw == "" {
		return Value{State: Blank}
	}
	date, err := time.Parse("20060102", raw)
	if !datePattern.MatchString(raw) || err != nil || date.Year() == 0 || date.Format("20060102") != raw {
		return Value{State: Invalid}
	}
	value := date.Format("2006-01-02")
	return Value{State: Valid, Value: &value}
}

func (r *Record) normalize(cycle string) {
	for _, name := range moneyFields {
		value, issue := parseMoney(r.SourceFields[name])
		r.Money[name] = value
		if issue != "" {
			r.Issues = append(r.Issues, Issue{Code: issue, Field: name})
		}
	}
	for _, name := range dateFields {
		value := parseDate(r.SourceFields[name])
		r.Dates[name] = value
		if value.State == Invalid {
			r.Issues = append(r.Issues, Issue{Code: "invalid_date", Field: name})
		}
	}
	for _, name := range identityFields {
		raw := r.SourceFields[name]
		value := Value{State: Valid, Value: &raw}
		valid := false
		switch name {
		case "CMTE_ID":
			valid = committeePattern.MatchString(raw)
		case "CAND_ID":
			valid = candidatePattern.MatchString(raw)
		case "FEC_ELECTION_YR":
			year, ok := cycleYear(raw)
			valid = ok && year%2 == 0
		}
		if !valid {
			value = Value{State: Invalid}
			if raw == "" {
				value.State = Blank
			}
			if raw != "" || name != "CAND_ID" {
				r.Issues = append(r.Issues, Issue{Code: "invalid_identity", Field: name})
			}
		}
		r.Identifiers[name] = value
	}
	if r.SourceFields["FEC_ELECTION_YR"] != cycle {
		r.Issues = append(r.Issues, Issue{Code: "cycle_mismatch", Field: "FEC_ELECTION_YR"})
	}
	start, end := r.Dates["CVG_START_DT"], r.Dates["CVG_END_DT"]
	if start.State == Valid && end.State == Valid && *start.Value > *end.Value {
		r.Issues = append(r.Issues, Issue{Code: "reversed_interval", Field: "CVG_START_DT"})
	}
}
