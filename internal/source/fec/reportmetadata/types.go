// Package reportmetadata reviews retained OpenFEC metadata pages. It performs
// no HTTP requests, financial selection, or source/fact publication.
package reportmetadata

import "encoding/json"

const (
	MaxPageBytes    = 4 << 20
	MaxCaptureBytes = 16 << 20
	MaxPages        = 16
)

type Artifact struct {
	Path   string `json:"path"`
	SHA256 string `json:"sha256"`
	Bytes  int64  `json:"bytes"`
}

// Query admits no credentials, arbitrary URLs, or hidden latest/amended filter.
// FileNumbers is only supported by /filings/. Otherwise both committee and
// cycle are required. Acquisition windows are not reporting-period assertions.
type Query struct {
	FileNumbers []int64 `json:"file_numbers,omitempty"`
	CommitteeID string  `json:"committee_id,omitempty"`
	Cycle       int     `json:"cycle,omitempty"`
	PerPage     int     `json:"per_page"`
}

type PageCapture struct {
	Page       int      `json:"page"`
	ObservedAt string   `json:"observed_at"`
	TimeBasis  string   `json:"time_basis"` // client_clock or http_date; never conflated
	Body       Artifact `json:"body"`
	Headers    Artifact `json:"headers"`
}

type Capture struct {
	Contract     string        `json:"contract"`
	SchemaSHA256 string        `json:"schema_sha256"`
	Endpoint     string        `json:"endpoint"`
	Query        Query         `json:"query"`
	Pages        []PageCapture `json:"pages"`
}

type Pagination struct {
	Count        int64 `json:"count"`
	IsCountExact bool  `json:"is_count_exact"`
	Page         int   `json:"page"`
	Pages        int64 `json:"pages"`
	PerPage      int   `json:"per_page"`
}

type Issue struct {
	Page    int    `json:"page"`
	Ordinal int    `json:"ordinal"` // zero denotes page/capture-level evidence
	Field   string `json:"field"`
	Code    string `json:"code"`
}

type Record struct {
	Ordinal     int             `json:"ordinal"`      // one-based within the exact results array
	SHA256      string          `json:"sha256"`       // exact object bytes, not reconstructed JSON
	FileNumber  string          `json:"file_number"`  // empty if no valid positive integer
	CommitteeID string          `json:"committee_id"` // empty if invalid or source-null
	Raw         json.RawMessage `json:"raw"`          // all values and types; byte authority is Body
}

type PageReview struct {
	Capture    PageCapture `json:"capture"`
	Pagination Pagination  `json:"pagination"`
	Records    []Record    `json:"records"`
}

type Review struct {
	Version                 string       `json:"version"`
	Contract                string       `json:"contract"`
	SchemaSHA256            string       `json:"schema_sha256"`
	CaptureSHA256           string       `json:"capture_sha256"`
	Endpoint                string       `json:"endpoint"`
	Query                   Query        `json:"query"`
	State                   string       `json:"state"` // validated_observations or blocked
	PaginationState         string       `json:"pagination_state"`
	HistoryComplete         bool         `json:"history_complete"`          // deliberately always false
	FinancialSelectionReady bool         `json:"financial_selection_ready"` // always false
	Rows                    int          `json:"rows"`
	MissingRequestedFiles   []int64      `json:"missing_requested_files"`
	Pages                   []PageReview `json:"pages"`
	Issues                  []Issue      `json:"issues"`
}
