// Package receiptgraph benchmarks a source-grain, bounded Arango publication.
// A sample is never promoted to complete-cycle coverage or financial eligibility.
package receiptgraph

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"regexp"

	c "github.com/vedantadhobley/legal-tender/internal/calculation/fec/receiptconduits"
	p "github.com/vedantadhobley/legal-tender/internal/calculation/fec/receiptparticipants"
)

const Version = "legal-tender.arango.receipt-participant-sample.v1"
const appearances = "contributor_appearances"
const receipts = "reported_receipts"
const conduits = "reported_conduit_associations"
const entities = "entities"
const authorizations = "candidate_authorization_context"
const metadata = "projection_metadata"

var collections = []string{appearances, receipts, conduits, entities, authorizations, metadata}
var committeePattern = regexp.MustCompile(`^C[0-9]{8}$`)
var candidatePattern = regexp.MustCompile(`^[HSP][A-Z0-9]{8}$`)

type Options struct {
	StorageRoot, Participants, ParticipantID, Conduits, ConduitID, Facts, Committees, Candidates, Linkages string
	Endpoint, Username, Password, BuildSHA256, LockDirectory                                               string
	First, Rows                                                                                            uint64
	Workers, BatchSize                                                                                     int
	Progress                                                                                               func(string)
	Layout, CompareResult, CompareSHA256                                                                   string
	FullCycle                                                                                              bool
	PublicationDirectory, ArangoDataDirectory                                                              string
	ReserveFreeBytes, MaxFilesystemGrowthBytes, MaxEncodedBytes                                            uint64
}
type Reference struct {
	ID     string `json:"id"`
	SHA256 string `json:"manifest_sha256"`
}
type Inputs struct {
	Participants  Reference `json:"participants"`
	Conduits      Reference `json:"conduits"`
	Facts         Reference `json:"facts"`
	Committees    Reference `json:"committees"`
	Candidates    Reference `json:"candidates"`
	Linkages      Reference `json:"linkages"`
	SourceRelease string    `json:"source_release_id"`
	Cycle         string    `json:"cycle"`
}
type definition struct {
	Key                  string `json:"_key"`
	Version              string `json:"schema_version"`
	State                string `json:"state"`
	Inputs               Inputs `json:"inputs"`
	Build                string `json:"executable_sha256"`
	First                uint64 `json:"first_source_row_ordinal"`
	Rows                 uint64 `json:"selected_rows"`
	SourceRows           uint64 `json:"source_rows"`
	FinancialEligibility bool   `json:"financial_eligibility"`
	IdentityResolved     bool   `json:"contributor_identity_resolved"`
}
type Result struct {
	Definition           definition                 `json:"definition"`
	Database             string                     `json:"database"`
	Reused               bool                       `json:"reused"`
	Counts               map[string]uint64          `json:"counts"`
	Unrouted             uint64                     `json:"unrouted_receipts"`
	MissingMasters       uint64                     `json:"missing_masters"`
	ConduitStates        map[string]uint64          `json:"conduit_states"`
	PayloadSHA256        map[string]string          `json:"ordered_document_values_sha256"`
	PayloadBytes         map[string]uint64          `json:"encoded_document_bytes"`
	Storage              map[string]json.RawMessage `json:"collection_figures"`
	Queries              []json.RawMessage          `json:"candidate_path_witnesses"`
	ConduitWitness       json.RawMessage            `json:"conduit_witness"`
	SourceChecks         []p.Inspection             `json:"source_checks"`
	ElapsedMS            int64                      `json:"elapsed_ms"`
	ImportReadbackMS     int64                      `json:"stream_import_readback_ms"`
	PeakRSSBytes         uint64                     `json:"peak_rss_bytes"`
	Workers              int                        `json:"workers"`
	BatchSize            int                        `json:"batch_rows"`
	SourceEvidenceSHA256 map[string]string          `json:"source_evidence_document_values_sha256,omitempty"`
	Comparison           *Comparison                `json:"expanded_graph_comparison,omitempty"`
	Publication          *Publication               `json:"cycle_publication,omitempty"`
}
type appearance struct {
	Key              string      `json:"_key"`
	FactSet          string      `json:"fact_set_id"`
	Row              p.Row       `json:"participant"`
	Conduit          *c.Decision `json:"conduit_decision"`
	ConduitState     string      `json:"conduit_disposition"`
	RecipientState   string      `json:"recipient_connection_state"`
	IdentityResolved bool        `json:"identity_resolved"`
}
type receipt struct {
	Key                  string `json:"_key"`
	From                 string `json:"_from"`
	To                   string `json:"_to"`
	FactSet              string `json:"fact_set_id"`
	Ordinal              uint64 `json:"source_row_ordinal"`
	Amount               *int64 `json:"reported_amount_minor_units,string"`
	Memo                 bool   `json:"memoed_subtotal"`
	FinancialEligibility bool   `json:"financial_eligibility"`
}
type conduit struct {
	Key                  string     `json:"_key"`
	From                 string     `json:"_from"`
	To                   string     `json:"_to"`
	FactSet              string     `json:"fact_set_id"`
	Decision             c.Decision `json:"decision"`
	AdditionalAmount     string     `json:"additional_amount_minor_units"`
	FinancialEligibility bool       `json:"financial_eligibility"`
}
type entity struct {
	Key     string  `json:"_key"`
	Kind    string  `json:"kind"`
	State   string  `json:"identity_state"`
	FactSet string  `json:"master_fact_set_id"`
	FactID  *string `json:"master_fact_id"`
	Name    *string `json:"reported_name"`
}
type authorization struct {
	Key                  string   `json:"_key"`
	From                 string   `json:"_from"`
	To                   string   `json:"_to"`
	State                string   `json:"authorization_state"`
	FactSet              string   `json:"linkage_fact_set_id"`
	SupportingFacts      []string `json:"supporting_fact_ids"`
	Designations         []string `json:"designation_codes"`
	FinancialEligibility bool     `json:"financial_eligibility"`
}

func digest(b []byte) string { h := sha256.Sum256(b); return hex.EncodeToString(h[:]) }
func validDigest(s string) bool {
	b, e := hex.DecodeString(s)
	return e == nil && len(b) == 32 && hex.EncodeToString(b) == s
}
func project(fact string, r p.Row, d *c.Decision) (appearance, *receipt, *conduit, error) {
	key, e := p.AppearanceID(fact, uint64(r.Ordinal))
	if e != nil {
		return appearance{}, nil, nil, e
	}
	a := appearance{Key: key, FactSet: fact, Row: r, Conduit: d, ConduitState: "not_a_non_memo_reviewed_earmark", RecipientState: "unresolved_reported_recipient"}
	var edge *receipt
	var association *conduit
	if r.Recipient != nil && committeePattern.MatchString(*r.Recipient) {
		a.RecipientState = "reported_committee_id"
		edge = &receipt{Key: key, From: appearances + "/" + key, To: entities + "/" + *r.Recipient, FactSet: fact, Ordinal: uint64(r.Ordinal), Amount: r.Amount, Memo: r.Memo}
	}
	if d != nil {
		if d.Ordinal != uint64(r.Ordinal) {
			return a, nil, nil, fmt.Errorf("conduit/source occurrence mismatch")
		}
		a.ConduitState = d.State
		if d.ConduitID != nil {
			if edge == nil || d.State != "reported_earmark_memo_association" || d.Related == 0 || !committeePattern.MatchString(*d.ConduitID) {
				return a, nil, nil, fmt.Errorf("invalid qualified conduit endpoint")
			}
			association = &conduit{Key: key, From: appearances + "/" + key, To: entities + "/" + *d.ConduitID, FactSet: fact, Decision: *d, AdditionalAmount: "0"}
		}
	}
	return a, edge, association, nil
}
