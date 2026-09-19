// Package sec preserves SEC issuer-directory observations. A directory name is
// candidate evidence, not an independent binding to a FEC organization string.
package sec

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/vedantadhobley/legal-tender/internal/strictjson"
)

const Contract = "sec/company-tickers@1.0.0"
const DirectoryURL = "https://www.sec.gov/files/company_tickers.json"
const MaxBody = 16 << 20
const MaxRows = 100_000

// SourceRow retains the three publisher fields without rewriting the name or
// ticker. The numeric CIK and the derived zero-padded identifier stay separate.
type SourceRow struct {
	CIK    int64  `json:"cik_str"`
	Ticker string `json:"ticker"`
	Title  string `json:"title"`
}

type Row struct {
	Key    string    `json:"source_row_key"`
	CIK    string    `json:"cik"`
	Source SourceRow `json:"source"`
}

func Parse(raw []byte) ([]Row, error) {
	if len(raw) > MaxBody {
		return nil, fmt.Errorf("SEC directory byte budget exceeded")
	}
	var source map[string]json.RawMessage
	if err := strictjson.Decode(raw, &source); err != nil {
		return nil, err
	}
	if len(source) == 0 || len(source) > MaxRows {
		return nil, fmt.Errorf("SEC directory row budget or empty source")
	}
	rows := make([]Row, 0, len(source))
	for key, rawRow := range source {
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(rawRow, &fields); err != nil || len(fields) != 3 || fields["cik_str"] == nil || fields["ticker"] == nil || fields["title"] == nil {
			return nil, fmt.Errorf("SEC directory exact field names required")
		}
		var s SourceRow
		if err := strictjson.Decode(rawRow, &s); err != nil {
			return nil, err
		}
		if !rowKey(key) || s.CIK < 1 || s.CIK > 9_999_999_999 || strings.TrimSpace(s.Ticker) == "" || strings.TrimSpace(s.Title) == "" {
			return nil, fmt.Errorf("SEC directory row does not match contract")
		}
		rows = append(rows, Row{Key: key, CIK: fmt.Sprintf("%010d", s.CIK), Source: s})
	}
	// Numeric key order without imposing contiguous indices or integer width.
	sort.Slice(rows, func(i, j int) bool {
		a, b := rows[i].Key, rows[j].Key
		if len(a) != len(b) {
			return len(a) < len(b)
		}
		return a < b
	})
	return rows, nil
}

func rowKey(s string) bool {
	if s == "" || (len(s) > 1 && s[0] == '0') {
		return false
	}
	for _, c := range s {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}
