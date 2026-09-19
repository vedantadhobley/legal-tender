// Package receiptparticipants publishes source-grain reported appearances.
// Appearances are not resolved people, effective payments or terminal sources.
package receiptparticipants

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/vedantadhobley/legal-tender/internal/calculation/fec/fundingbasis"
)

const Version = "legal-tender.fec.receipt-participants.v1"
const Policy = "fec/reported-receipt-participant-appearance@1.0.0"
const ContributorRole = "reported_contributor"

// Row is a compact access record for one exact source occurrence. Raw role/ID
// fields remain separate from accepted source routing. Names and employer text
// stay in the full fact, reachable through the manifest and source ordinal.
type Row struct {
	Ordinal            int64   `parquet:"source_row_ordinal" json:"source_row_ordinal"`
	Recipient          *string `parquet:"recipient_committee_id" json:"recipient_committee_id"`
	Component          string  `parquet:"inventory_component,dict" json:"inventory_component"`
	IndividualDecision string  `parquet:"individual_decision,dict" json:"individual_decision"`
	CommitteeDecision  string  `parquet:"committee_decision,dict" json:"committee_decision"`
	ReceiptRole        string  `parquet:"receipt_role,dict" json:"receipt_role"`
	SourceRoute        string  `parquet:"source_route,dict" json:"source_route"`
	ReportedSourceID   *string `parquet:"reported_source_committee_id" json:"reported_source_committee_id"`
	IndividualOverlap  bool    `parquet:"publisher_individual_overlap" json:"publisher_individual_overlap"`
	EntityConflict     bool    `parquet:"individual_entity_type_conflict" json:"individual_entity_type_conflict"`
	EarmarkState       string  `parquet:"earmark_state,dict" json:"earmark_state"`
	ConduitState       string  `parquet:"structured_conduit_state,dict" json:"structured_conduit_state"`
	ReportedConduitID  *string `parquet:"reported_conduit_committee_id" json:"reported_conduit_committee_id"`
	ReferenceState     string  `parquet:"reported_reference_state,dict" json:"reported_reference_state"`
	MemoTextPresent    bool    `parquet:"memo_text_present" json:"memo_text_present"`
	ConduitNamePresent bool    `parquet:"conduit_name_present" json:"conduit_name_present"`
	Amount             *int64  `parquet:"reported_amount_minor_units" json:"reported_amount_minor_units,string"`
	AmountState        string  `parquet:"reported_amount_state,dict" json:"reported_amount_state"`
	Memo               bool    `parquet:"memoed_subtotal" json:"memoed_subtotal"`
	Entity             *string `parquet:"reported_entity_type" json:"reported_entity_type"`
	Contributor        *string `parquet:"raw_contributor_id" json:"raw_contributor_id"`
	CleanContributor   *string `parquet:"clean_contributor_id" json:"clean_contributor_id"`
	ConduitID          *string `parquet:"raw_conduit_committee_id" json:"raw_conduit_committee_id"`
	ReceiptType        *string `parquet:"reported_receipt_type" json:"reported_receipt_type"`
}

func project(s fundingbasis.SourceEvidenceRow) Row {
	k, d := s.Classify()
	return Row{Ordinal: s.Ordinal, Recipient: s.Recipient, Component: k.Component, IndividualDecision: k.IndividualDecision, CommitteeDecision: k.CommitteeDecision, ReceiptRole: k.ReceiptRole,
		SourceRoute: d.SourceRoute, ReportedSourceID: d.ReportedSourceCommitteeID, IndividualOverlap: d.PublisherIndividualOverlap, EntityConflict: d.IndividualEntityConflict,
		EarmarkState: d.EarmarkState, ConduitState: d.ConduitState, ReportedConduitID: d.ReportedConduitID, ReferenceState: d.ReportReferenceState,
		MemoTextPresent: d.MemoTextPresent, ConduitNamePresent: d.ConduitNamePresent, Amount: s.Amount, AmountState: s.AmountState, Memo: s.Memo,
		Entity: s.Entity, Contributor: s.Contributor, CleanContributor: s.CleanContributor, ConduitID: s.ConduitID, ReceiptType: s.ReceiptType}
}

// Canonical values use struct field order, big-endian integers, and tagged
// length-framed nullable strings. Reuse the caller's buffer, not per-row JSON.
func (r Row) canonical(b []byte) []byte {
	b = binary.BigEndian.AppendUint64(b[:0], uint64(r.Ordinal))
	str := func(s string) { b = binary.BigEndian.AppendUint64(b, uint64(len(s))); b = append(b, s...) }
	optional := func(s *string) {
		if s == nil {
			b = append(b, 0)
		} else {
			b = append(b, 1)
			str(*s)
		}
	}
	boolean := func(v bool) {
		if v {
			b = append(b, 1)
		} else {
			b = append(b, 0)
		}
	}
	optional(r.Recipient)
	str(r.Component)
	str(r.IndividualDecision)
	str(r.CommitteeDecision)
	str(r.ReceiptRole)
	str(r.SourceRoute)
	optional(r.ReportedSourceID)
	boolean(r.IndividualOverlap)
	boolean(r.EntityConflict)
	str(r.EarmarkState)
	str(r.ConduitState)
	optional(r.ReportedConduitID)
	str(r.ReferenceState)
	boolean(r.MemoTextPresent)
	boolean(r.ConduitNamePresent)
	if r.Amount == nil {
		b = append(b, 0)
	} else {
		b = append(b, 1)
		b = binary.BigEndian.AppendUint64(b, uint64(*r.Amount))
	}
	str(r.AmountState)
	boolean(r.Memo)
	optional(r.Entity)
	optional(r.Contributor)
	optional(r.CleanContributor)
	optional(r.ConduitID)
	optional(r.ReceiptType)
	return b
}

type Census struct {
	keys            map[string]string
	Rows            uint64            `json:"rows"`
	MemoRows        uint64            `json:"memo_rows"`
	UnknownAmounts  uint64            `json:"unknown_amount_rows"`
	PositiveAmounts uint64            `json:"positive_amount_rows"`
	NegativeAmounts uint64            `json:"negative_amount_rows"`
	ZeroAmounts     uint64            `json:"zero_amount_rows"`
	Components      map[string]uint64 `json:"inventory_components"`
	Routes          map[string]uint64 `json:"source_routes"`
	Conduits        map[string]uint64 `json:"structured_conduit_states"`
	Earmarks        map[string]uint64 `json:"earmark_states"`
}

func newCensus() Census {
	return Census{keys: map[string]string{}, Components: map[string]uint64{}, Routes: map[string]uint64{}, Conduits: map[string]uint64{}, Earmarks: map[string]uint64{}}
}
func (c *Census) observe(r Row) {
	c.Rows++
	if r.Memo {
		c.MemoRows++
	}
	switch {
	case r.Amount == nil:
		c.UnknownAmounts++
	case *r.Amount > 0:
		c.PositiveAmounts++
	case *r.Amount < 0:
		c.NegativeAmounts++
	default:
		c.ZeroAmounts++
	}
	// Keep owned state keys even when a Parquet reader reuses its buffers.
	intern := func(s string) string {
		if own, ok := c.keys[s]; ok {
			return own
		}
		own := strings.Clone(s)
		c.keys[own] = own
		return own
	}
	c.Components[intern(r.Component)]++
	c.Routes[intern(r.SourceRoute)]++
	c.Conduits[intern(r.ConduitState)]++
	c.Earmarks[intern(r.EarmarkState)]++
}
func (c *Census) merge(v Census) {
	c.Rows += v.Rows
	c.MemoRows += v.MemoRows
	c.UnknownAmounts += v.UnknownAmounts
	c.PositiveAmounts += v.PositiveAmounts
	c.NegativeAmounts += v.NegativeAmounts
	c.ZeroAmounts += v.ZeroAmounts
	for i, m := range []map[string]uint64{v.Components, v.Routes, v.Conduits, v.Earmarks} {
		target := []map[string]uint64{c.Components, c.Routes, c.Conduits, c.Earmarks}[i]
		for k, n := range m {
			target[k] += n
		}
	}
}

type File struct {
	Name         string `json:"name"`
	Rows         uint64 `json:"rows"`
	First        uint64 `json:"first_source_row_ordinal"`
	Last         uint64 `json:"last_source_row_ordinal"`
	SourceSHA256 string `json:"source_shard_sha256"`
	Bytes        uint64 `json:"bytes"`
	SHA256       string `json:"sha256"`
	ValuesSHA256 string `json:"values_sha256"`
}
type Result struct {
	SchemaVersion           string `json:"schema_version"`
	State                   string `json:"state"`
	CalculationID           string `json:"calculation_id"`
	BuildSHA256             string `json:"executable_sha256"`
	Policy                  string `json:"appearance_policy"`
	InventoryPolicy         string `json:"inventory_policy"`
	SourceRolePolicy        string `json:"source_role_policy"`
	IndividualPolicy        string `json:"individual_policy"`
	CommitteePolicy         string `json:"committee_policy"`
	FactSetID               string `json:"fact_set_id"`
	ManifestSHA256          string `json:"fact_manifest_sha256"`
	Cycle                   string `json:"cycle"`
	SourceRows              uint64 `json:"source_rows"`
	Scope                   string `json:"scope"`
	AppearanceRole          string `json:"appearance_role"`
	Census                  Census `json:"census"`
	Files                   []File `json:"files"`
	Workers                 int    `json:"workers"`
	OutputBytes             uint64 `json:"output_bytes"`
	ElapsedMS               int64  `json:"elapsed_ms"`
	PeakRSSBytes            uint64 `json:"peak_rss_bytes"`
	IdentityResolved        bool   `json:"contributor_identity_resolved"`
	ReferenceQualification  bool   `json:"reference_conduit_qualification_evaluated"`
	FinancialEligibility    bool   `json:"financial_eligibility"`
	AdditionalConduitAmount string `json:"additional_conduit_amount_minor_units"`
}

type Options struct {
	StorageRoot, Manifest, Cycle, OutputDirectory, BuildSHA256 string
	Workers                                                    int
	MaxOutputBytes                                             uint64
	Progress                                                   func(string)
}

func digest(s string) bool {
	b, e := hex.DecodeString(s)
	return e == nil && len(b) == 32 && hex.EncodeToString(b) == s
}
func AppearanceID(fact string, ordinal uint64) (string, error) {
	if !digest(fact) || ordinal == 0 {
		return "", fmt.Errorf("exact fact digest and positive occurrence ordinal required")
	}
	b := append([]byte(ContributorRole+":"+fact+":"), binary.BigEndian.AppendUint64(nil, ordinal)...)
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:]), nil
}
func logicalID(r Result) string {
	r.CalculationID = ""
	r.Workers = 0
	r.ElapsedMS = 0
	r.PeakRSSBytes = 0
	b, _ := json.Marshal(r)
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}
