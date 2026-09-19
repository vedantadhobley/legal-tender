// Package fundingbasis inventories reported Schedule A receipts. It does not
// resolve donor identities or allocate a committee's outgoing dollars.
package fundingbasis

const Version = "legal-tender.fec.committee-funding-basis.v1"
const Policy = "fec/committee-reported-receipt-inventory@1.0.0"

type Options struct {
	StorageRoot, ScheduleA, Cycle string
	Workers                       int
	Progress                      func(string)
}

type Input struct {
	FactSetID      string `json:"fact_set_id"`
	ManifestSHA256 string `json:"manifest_sha256"`
	Facts          uint64 `json:"facts"`
	Shards         int    `json:"shards"`
}

// Cell preserves source null separately from an empty or invalid ID.
type Cell struct {
	Present bool   `json:"present"`
	Value   string `json:"value"`
}

type Key struct {
	Recipient          Cell   `json:"recipient"`
	Component          string `json:"component"`
	IndividualDecision string `json:"individual_decision"`
	CommitteeDecision  string `json:"committee_decision"`
	ReceiptRole        string `json:"receipt_role"`
}

// Measures contains signed known-amount subtotals, not economic cash balances.
// Unknown signed amounts mean the known subtotal is not even a lower bound.
type Measures struct {
	Rows          uint64 `json:"rows"`
	Known         uint64 `json:"known_amount_rows"`
	Unknown       uint64 `json:"unknown_amount_rows"`
	PositiveRows  uint64 `json:"positive_rows"`
	NegativeRows  uint64 `json:"negative_rows"`
	ZeroRows      uint64 `json:"zero_rows"`
	Signed        int64  `json:"signed_minor_units,string"`
	Positive      int64  `json:"positive_minor_units,string"`
	Negative      int64  `json:"negative_minor_units,string"`
	ConduitIDRows uint64 `json:"nonempty_conduit_id_rows"`
}

type Bucket struct {
	Key      Key      `json:"key"`
	Measures Measures `json:"measures"`
	First    uint64   `json:"first_source_row_ordinal"`
	Last     uint64   `json:"last_source_row_ordinal"`
	Shards   []byte   `json:"shard_presence_bitmap"`
}

type Result struct {
	SchemaVersion    string   `json:"schema_version"`
	CalculationID    string   `json:"calculation_id"`
	Policy           string   `json:"policy"`
	IndividualPolicy string   `json:"individual_policy"`
	CommitteePolicy  string   `json:"committee_policy"`
	Cycle            string   `json:"cycle"`
	State            string   `json:"state"`
	TerminalEligible bool     `json:"terminal_attribution_eligible"`
	Input            Input    `json:"input"`
	Total            Measures `json:"total"`
	Buckets          []Bucket `json:"buckets"`
	NotCovered       []string `json:"not_covered"`
}

type Query struct {
	Committee, Component string
	After                uint64
	Limit                int
}

type Receipt struct {
	Ordinal     uint64         `json:"source_row_ordinal"`
	ShardSHA256 string         `json:"shard_sha256"`
	Key         Key            `json:"key"`
	Fields      map[string]any `json:"fields"`
}

type Page struct {
	SchemaVersion    string    `json:"schema_version"`
	CalculationID    string    `json:"calculation_id"`
	Input            Input     `json:"input"`
	Committee        string    `json:"committee_id"`
	Component        string    `json:"component"`
	After            uint64    `json:"after_source_row_ordinal"`
	Receipts         []Receipt `json:"receipts"`
	NextAfter        *uint64   `json:"next_after_source_row_ordinal"`
	HasMore          bool      `json:"has_more"`
	TerminalEligible bool      `json:"terminal_attribution_eligible"`
}
