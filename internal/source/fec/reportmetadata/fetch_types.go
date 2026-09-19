package reportmetadata

const FetchVersion = "legal-tender.fec.report-metadata-capture.v1"

type FetchLimits struct {
	Pages           int   `json:"pages"`
	Requests        int   `json:"requests"`
	AttemptsPerPage int   `json:"attempts_per_page"`
	SourceBytes     int64 `json:"source_bytes"`
}

// FetchRequest has no arbitrary URL, authentication, or implicit report filter.
type FetchRequest struct {
	Contract     string      `json:"contract"`
	SchemaSHA256 string      `json:"schema_sha256"`
	Endpoint     string      `json:"endpoint"`
	Query        Query       `json:"query"`
	Limits       FetchLimits `json:"limits"`
}

type Attempt struct {
	Sequence          int       `json:"sequence"`
	Page              int       `json:"page"`
	URL               string    `json:"url"` // credential-free; token travels only in a header
	StartedAt         string    `json:"started_at"`
	FinishedAt        string    `json:"finished_at"`
	Status            int       `json:"status"` // zero if no response headers
	Outcome           string    `json:"outcome"`
	BytesRead         int64     `json:"bytes_read"`
	BodyComplete      bool      `json:"body_complete"`
	RetryAfterSeconds int64     `json:"retry_after_seconds"`
	OmittedHeaders    []string  `json:"omitted_headers"`
	Body              *Artifact `json:"body"`
	Headers           *Artifact `json:"headers"`
}

type FetchResult struct {
	Version                 string       `json:"version"`
	Request                 FetchRequest `json:"request"`
	StartedAt               string       `json:"started_at"`
	FinishedAt              string       `json:"finished_at"`
	State                   string       `json:"state"` // captured, incomplete, blocked, failed
	Reason                  string       `json:"reason"`
	HeaderRepresentation    string       `json:"header_representation"`
	Attempts                []Attempt    `json:"attempts"`
	BytesRead               int64        `json:"bytes_read"`
	Capture                 *Artifact    `json:"capture"`
	Review                  *Artifact    `json:"review"`
	HistoryComplete         bool         `json:"history_complete"`
	FinancialSelectionReady bool         `json:"financial_selection_ready"`
}
