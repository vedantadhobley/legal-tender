// Package terminalpolicycomparison compares candidate-receipt stopping and
// allocation policies without adopting one or changing underlying evidence.
package terminalpolicycomparison

import "github.com/vedantadhobley/legal-tender/internal/calculation/fec/fundingbasis"

const SchemaVersion = "legal-tender.fec.terminal-policy-comparison.v1"
const Policy = "fec/terminal-policy-comparison@1.0.0"

type InputReference struct {
	DossierID            string `json:"candidate_dossier_id"`
	DossierSHA256        string `json:"candidate_dossier_sha256"`
	ReceiptFactSetID     string `json:"receipt_fact_set_id"`
	ReceiptManifestSHA   string `json:"receipt_manifest_sha256"`
	ReceiptSourceRelease string `json:"receipt_fec_source_release_id"`
}

type CommitteeInput struct {
	CommitteeID   string                         `json:"committee_id"`
	Authorization string                         `json:"candidate_authorization"`
	Receipts      fundingbasis.ReceiptPopulation `json:"reported_receipt_population"`
}

type Input struct {
	CandidateID      string           `json:"candidate_id"`
	Cycle            string           `json:"cycle"`
	ExecutableSHA256 string           `json:"executable_sha256"`
	Reference        InputReference   `json:"reference"`
	Committees       []CommitteeInput `json:"committees"`
}

// Measures preserves signed exact cents and the row population. Unknown amount
// rows conserve their row count but cannot contribute a numeric value.
type Measures struct {
	Rows               uint64 `json:"rows"`
	KnownAmountRows    uint64 `json:"known_amount_rows"`
	UnknownAmountRows  uint64 `json:"unknown_amount_rows"`
	PositiveRows       uint64 `json:"positive_rows"`
	NegativeRows       uint64 `json:"negative_rows"`
	ZeroRows           uint64 `json:"zero_rows"`
	SignedMinorUnits   string `json:"signed_minor_units"`
	PositiveMinorUnits string `json:"positive_minor_units"`
	NegativeMinorUnits string `json:"negative_minor_units"`
}

type Scope struct {
	State            string   `json:"state"`
	Included         Measures `json:"included_nonmemo_population"`
	ExcludedMemo     Measures `json:"excluded_memo_population"`
	Limitations      []string `json:"limitations"`
	CompleteFunding  bool     `json:"complete_candidate_funding_basis"`
	IndependentSpend bool     `json:"includes_independent_expenditures"`
}

type TerminalDefinition struct {
	ID       string `json:"definition_id"`
	Boundary string `json:"boundary"`
	Status   string `json:"status"`
	Meaning  string `json:"meaning"`
	Blocker  string `json:"blocker,omitempty"`
}

type AllocationMethod struct {
	ID           string `json:"method_id"`
	Status       string `json:"status"`
	Rule         string `json:"rule"`
	Conservation string `json:"conservation"`
	Blocker      string `json:"blocker,omitempty"`
}

type Scenario struct {
	ID                    string   `json:"scenario_id"`
	Status                string   `json:"status"`
	TerminalDefinitionIDs []string `json:"terminal_definition_ids"`
	AllocationMethodID    string   `json:"allocation_method_id"`
	Direct                Measures `json:"direct"`
	Earmarked             Measures `json:"explicitly_earmarked"`
	Proportional          Measures `json:"proportional"`
	Unresolved            Measures `json:"unresolved"`
	Conserved             bool     `json:"exactly_conserved"`
	Selected              bool     `json:"selected"`
	Notes                 []string `json:"notes"`
}

type Result struct {
	SchemaVersion       string               `json:"schema_version"`
	ComparisonID        string               `json:"comparison_id"`
	Policy              string               `json:"policy"`
	ExecutableSHA256    string               `json:"executable_sha256"`
	CandidateID         string               `json:"candidate_id"`
	Cycle               string               `json:"cycle"`
	Input               InputReference       `json:"input"`
	Scope               Scope                `json:"scope"`
	TerminalDefinitions []TerminalDefinition `json:"terminal_definitions"`
	AllocationMethods   []AllocationMethod   `json:"allocation_methods"`
	Scenarios           []Scenario           `json:"scenarios"`
	TerminalPolicy      *string              `json:"terminal_policy"`
	AllocationPolicy    *string              `json:"allocation_policy"`
	TerminalEligible    bool                 `json:"terminal_attribution_eligible"`
}
