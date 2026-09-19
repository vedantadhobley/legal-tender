// Package fundinggeneration binds typed evidence families without constructing
// an undifferentiated money graph or substituting one ledger for another.
package fundinggeneration

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"

	flow "github.com/vedantadhobley/legal-tender/internal/projection/arango/flowevidence"
	ie "github.com/vedantadhobley/legal-tender/internal/projection/arango/independentexpenditures"
	receipts "github.com/vedantadhobley/legal-tender/internal/projection/arango/receiptgraph"
	occ "github.com/vedantadhobley/legal-tender/internal/source/fec/occurrence"
)

const Version = "legal-tender.funding-evidence-generation.v1"
const Policy = "fec/typed-funding-generation@1.0.0"

type Options struct {
	Receipt                            receipts.Options
	ReceiptManifest, ReceiptSHA256     string
	FlowBundle, FlowBundleSHA256       string
	OutsideBundle, OutsideBundleSHA256 string
	BuildSHA256, ExpectedGenerationID  string
}

type Family struct {
	Kind          string `json:"kind"`
	Database      string `json:"database"`
	Collection    string `json:"collection"`
	ProjectionID  string `json:"projection_id"`
	Ledger        string `json:"ledger"`
	FactSetID     string `json:"fact_set_id"`
	Predicate     string `json:"predicate"`
	Grain         string `json:"grain"`
	AmountMeaning string `json:"amount_meaning"`
	Membership    uint64 `json:"membership"`
	OverlapGroup  string `json:"overlap_group"`
}

type EndpointNamespace struct {
	ProjectionID      string `json:"projection_id"`
	Database          string `json:"database"`
	Collection        string `json:"collection"`
	Kind              string `json:"kind"`
	KeyPrefix         string `json:"key_prefix"`
	IdentifierPattern string `json:"identifier_pattern"`
	JoinScope         string `json:"join_scope"`
}

type Result struct {
	SchemaVersion        string                         `json:"schema_version"`
	GenerationID         string                         `json:"generation_id"`
	Policy               string                         `json:"policy"`
	BuildSHA256          string                         `json:"executable_sha256"`
	State                string                         `json:"state"`
	Cycle                string                         `json:"cycle"`
	Release              occ.ReferenceIdentity          `json:"coordinated_source_release"`
	Receipts             receipts.CycleView             `json:"receipts"`
	CommitteeFlow        flow.View                      `json:"committee_flow"`
	OutsideSpending      ie.ResolvedView                `json:"outside_spending"`
	ReferenceProofs      []occ.ClassicReferenceProof    `json:"reference_content_proofs"`
	OutsideMembership    occ.ScheduleEReleaseMembership `json:"outside_source_membership"`
	Families             []Family                       `json:"relationship_families"`
	EndpointNamespaces   []EndpointNamespace            `json:"endpoint_namespaces"`
	Checks               []string                       `json:"checks"`
	Limitations          []string                       `json:"limitations"`
	FinancialEligibility bool                           `json:"financial_eligibility"`
	TerminalEligible     bool                           `json:"terminal_attribution_eligible"`
}

func identity(v Result) string {
	v.GenerationID = ""
	b, _ := json.Marshal(v)
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}
func validDigest(s string) bool {
	b, e := hex.DecodeString(s)
	return e == nil && len(b) == 32 && hex.EncodeToString(b) == s
}
