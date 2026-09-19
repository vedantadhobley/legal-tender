// Package reportscope assesses retained report representations, not effective
// financial versions. It performs no HTTP, publication, or transaction changes.
package reportscope

import (
	"encoding/json"

	"github.com/vedantadhobley/legal-tender/internal/source/fec/reportmetadata"
)

const (
	Version           = "legal-tender.fec.report-scope.v1"
	PaperSchemaSHA256 = "f9637d641d581d169cecc42cfb46c08e5a073f02803df8159e3ff40e3860f259"
	MaxBodyBytes      = 4 << 20
	MaxRecords        = 4096
)

// Request pins local bytes and the recorded public document URL. The URL is
// capture provenance supplied by the operator, not independently authenticated.
// No caller-supplied completeness, role, or financial-readiness flag is accepted.
type Request struct {
	SourceURL        string
	BodyPath         string
	BodySHA256       string
	HeadersPath      string
	HeadersSHA256    string
	MetadataCaptures []string
}

type Record struct {
	Ordinal  int    `json:"ordinal"`
	Offset   int    `json:"offset"` // zero-based byte offset
	Bytes    int    `json:"bytes"`  // includes the exact record terminator, if present
	SHA256   string `json:"sha256"`
	Complete bool   `json:"complete"`
	Raw      []byte `json:"raw_base64"` // no decoding loss, even for unsupported layouts
}

type AmountField struct {
	Sequence   int    `json:"sequence"` // one-based position in the identified source layout
	Raw        string `json:"raw"`
	State      string `json:"state"` // blank, valid, invalid; zero is valid, never blank
	MinorUnits string `json:"minor_units,omitempty"`
}

type Cover struct {
	RecordOrdinal  int           `json:"record_ordinal"`
	Form           string        `json:"form"`
	CommitteeID    string        `json:"committee_id"`
	ReportCode     string        `json:"report_code"`
	CoverageStart  string        `json:"coverage_start"` // raw source dates
	CoverageEnd    string        `json:"coverage_end"`
	AmountPresence string        `json:"amount_presence"`
	Fields         []string      `json:"fields"` // all source fields, including blanks
	Amounts        []AmountField `json:"amounts"`
}

type Assertion struct {
	CaptureSHA256   string                     `json:"capture_sha256"`
	Endpoint        string                     `json:"endpoint"`
	Page            reportmetadata.PageCapture `json:"page"`
	PaginationState string                     `json:"pagination_state"`
	Record          reportmetadata.Record      `json:"record"`
}

type Difference struct {
	Field            string            `json:"field"`
	AssertionIndexes []int             `json:"assertion_indexes"` // zero-based into Metadata
	Values           []json.RawMessage `json:"values"`            // exact types; no endpoint precedence
}

type MetadataInput struct {
	CaptureSHA256   string `json:"capture_sha256"`
	Endpoint        string `json:"endpoint"`
	PaginationState string `json:"pagination_state"`
	Rows            int    `json:"rows"`
	MatchingRows    int    `json:"matching_rows"`
}

type Assessment struct {
	Version                 string                  `json:"version"`
	SourceURL               string                  `json:"source_url"`
	FileNumber              string                  `json:"file_number"`
	Body                    reportmetadata.Artifact `json:"body"`
	Headers                 reportmetadata.Artifact `json:"headers"`
	HTTPDate                string                  `json:"http_date"`
	CaptureExtent           string                  `json:"capture_extent"` // complete_response or prefix
	PublisherBytes          int64                   `json:"publisher_bytes"`
	Representation          string                  `json:"representation"`
	SchemaSHA256            string                  `json:"schema_sha256,omitempty"`
	Records                 []Record                `json:"records"`
	Cover                   *Cover                  `json:"cover"`
	Disposition             string                  `json:"disposition"`
	Basis                   string                  `json:"basis"`
	OriginalImageVerified   bool                    `json:"original_image_verified"`   // always false
	HistoryComplete         bool                    `json:"history_complete"`          // always false
	FinancialSelectionReady bool                    `json:"financial_selection_ready"` // always false
	Metadata                []Assertion             `json:"metadata"`
	MetadataInputs          []MetadataInput         `json:"metadata_inputs"`
	Differences             []Difference            `json:"metadata_differences"`
	Issues                  []string                `json:"issues"`
}
