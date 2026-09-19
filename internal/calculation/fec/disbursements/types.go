package disbursements

const Version = "legal-tender.fec.processed-disbursement-reporting.v1"

type Options struct {
	StorageRoot, FactManifestPath, Cycle string
	Workers                              int
	Progress                             func(string)
}

type Source struct {
	FactSetID             string `json:"fact_set_id"`
	ManifestSHA256        string `json:"manifest_sha256"`
	SourceReleaseID       string `json:"source_release_id"`
	SourceArtifactSHA256  string `json:"source_artifact_sha256"`
	PhysicalSchemaVersion string `json:"physical_schema_version"`
	Facts                 uint64 `json:"facts"`
}

type Measures struct {
	Rows              uint64 `json:"rows"`
	AmountRows        uint64 `json:"amount_rows"`
	MissingAmountRows uint64 `json:"missing_amount_rows"`
	Amount            int64  `json:"signed_amount_minor_units,string"`
	PositiveRows      uint64 `json:"positive_rows"`
	NegativeRows      uint64 `json:"negative_rows"`
	ZeroRows          uint64 `json:"zero_rows"`
}

type Group struct {
	Key          Key      `json:"key"`
	Measures     Measures `json:"measures"`
	FirstOrdinal uint64   `json:"first_source_row_ordinal"`
}

// Result is deterministic: runtime metadata is logged separately. Membership
// is reconstructed from the exact source fact index and embedded policy.
type Result struct {
	SchemaVersion    string              `json:"schema_version"`
	CalculationSetID string              `json:"calculation_set_id"`
	PolicyVersion    string              `json:"policy_version"`
	PolicySHA256     string              `json:"policy_sha256"`
	State            string              `json:"state"`
	Cycle            string              `json:"cycle"`
	Input            Source              `json:"input"`
	LineRules        []LineRule          `json:"line_rules"`
	Total            Measures            `json:"total"`
	Decisions        map[string]Measures `json:"decisions"`
	Groups           []Group             `json:"groups"`
	GroupsSHA256     string              `json:"groups_sha256"`
	Membership       string              `json:"membership"`
	GraphEligible    bool                `json:"graph_eligible"`
}
