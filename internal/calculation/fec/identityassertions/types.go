// Package identityassertions exposes reported text without resolving identities.
// Its source-backed view retains one row per published fact, not per person.
package identityassertions

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"

	occ "github.com/vedantadhobley/legal-tender/internal/source/fec/occurrence"
)

const Version = "legal-tender.fec.reported-identity-assertions.v1"
const Policy = "fec/reported-identity-field-projection@1.0.0"
const maxTextBytes = 1 << 20
const maxCommitteeFacts = 1_000_000

// Receipt uses original Parquet column names. Null pointers are source nulls;
// neither empty strings nor whitespace are normalized. Cycle is source scope,
// not a claimed employment/organization validity period.
type Receipt struct {
	Ordinal       int64   `parquet:"lt_source_row_ordinal" json:"source_row_ordinal"`
	Cycle         int64   `parquet:"lt_two_year_transaction_period" json:"source_cycle"`
	Normalization string  `parquet:"lt_normalization_state" json:"normalization_state"`
	Recipient     *string `parquet:"cmte_id" json:"reported_recipient_id"`
	Entity        *string `parquet:"entity_tp" json:"reported_entity_type"`
	Contributor   *string `parquet:"contbr_id" json:"reported_contributor_id"`
	CleanID       *string `parquet:"clean_contbr_id" json:"publisher_clean_contributor_id"`
	Name          *string `parquet:"contbr_nm" json:"reported_name"`
	First         *string `parquet:"contbr_nm_first" json:"reported_first_name"`
	Middle        *string `parquet:"contbr_m_nm" json:"reported_middle_name"`
	Last          *string `parquet:"contbr_nm_last" json:"reported_last_name"`
	Prefix        *string `parquet:"contbr_prefix" json:"reported_prefix"`
	Suffix        *string `parquet:"contbr_suffix" json:"reported_suffix"`
	Street1       *string `parquet:"contbr_st1" json:"reported_street_1"`
	Street2       *string `parquet:"contbr_st2" json:"reported_street_2"`
	City          *string `parquet:"contbr_city" json:"reported_city"`
	State         *string `parquet:"contbr_st" json:"reported_state"`
	ZIP           *string `parquet:"contbr_zip" json:"reported_zip"`
	Employer      *string `parquet:"contbr_employer" json:"reported_employer"`
	Occupation    *string `parquet:"contbr_occupation" json:"reported_occupation"`
	ReceiptDate   *string `parquet:"contb_receipt_dt" json:"reported_receipt_date"`
}

func (r Receipt) fields() [18]*string {
	return [18]*string{r.Recipient, r.Entity, r.Contributor, r.CleanID, r.Name, r.First, r.Middle, r.Last, r.Prefix, r.Suffix, r.Street1, r.Street2, r.City, r.State, r.ZIP, r.Employer, r.Occupation, r.ReceiptDate}
}

// ReceiptColumns is the ordered raw field mapping used by values digests and
// per-field state counts. A returned copy cannot change the accepted mapping.
func ReceiptColumns() [18]string {
	return [18]string{"cmte_id", "entity_tp", "contbr_id", "clean_contbr_id", "contbr_nm", "contbr_nm_first", "contbr_m_nm", "contbr_nm_last", "contbr_prefix", "contbr_suffix", "contbr_st1", "contbr_st2", "contbr_city", "contbr_st", "contbr_zip", "contbr_employer", "contbr_occupation", "contb_receipt_dt"}
}

type Committee struct {
	FactID                string `json:"fact_id"`
	OccurrenceID          string `json:"occurrence_id"`
	CommitteeID           string `json:"reported_committee_id"`
	Name                  string `json:"reported_committee_name"`
	OrganizationType      string `json:"reported_organization_type"`
	ConnectedOrganization string `json:"reported_connected_organization"`
}

func (r Committee) fields() [4]*string {
	return [4]*string{&r.CommitteeID, &r.Name, &r.OrganizationType, &r.ConnectedOrganization}
}

func CommitteeColumns() [4]string {
	return [4]string{"CMTE_ID", "CMTE_NM", "ORG_TP", "CONNECTED_ORG_NM"}
}

type FieldCounts struct {
	Null     uint64 `json:"null"`
	Empty    uint64 `json:"empty"`
	Nonempty uint64 `json:"nonempty"`
}

func (c *FieldCounts) observe(p *string) {
	switch {
	case p == nil:
		c.Null++
	case *p == "":
		c.Empty++
	default:
		c.Nonempty++
	}
}

func (c FieldCounts) valid(rows uint64) bool {
	return c.Null <= rows && c.Empty <= rows-c.Null && c.Nonempty == rows-c.Null-c.Empty
}

type ReceiptProof struct {
	Source       occ.ScheduleAColumnarShard `json:"source_shard"`
	Rows         uint64                     `json:"rows"`
	ValuesSHA256 string                     `json:"ordered_values_sha256"`
	Fields       [18]FieldCounts            `json:"field_counts"`
}

type CommitteeProof struct {
	Source       occ.Artifact   `json:"source_artifact"`
	Rows         uint64         `json:"rows"`
	ValuesSHA256 string         `json:"ordered_values_sha256"`
	Fields       [4]FieldCounts `json:"field_counts"`
}

type Source struct {
	FactSetID           string `json:"fact_set_id"`
	ManifestSHA256      string `json:"manifest_sha256"`
	SourceReleaseID     string `json:"source_release_id"`
	SourceReleaseSHA256 string `json:"source_release_manifest_sha256"`
	PublishedFacts      uint64 `json:"published_facts"`
	SourceOccurrences   uint64 `json:"source_occurrences"`
	ExcludedOccurrences uint64 `json:"excluded_occurrences"`
}

type Result struct {
	SchemaVersion         string          `json:"schema_version"`
	ViewID                string          `json:"view_id"`
	Policy                string          `json:"policy"`
	BuildSHA256           string          `json:"executable_sha256"`
	Cycle                 string          `json:"cycle"`
	State                 string          `json:"state"`
	Storage               string          `json:"storage_mode"`
	ReceiptSource         Source          `json:"receipt_source"`
	CommitteeSource       Source          `json:"committee_source"`
	ReceiptColumns        [18]string      `json:"receipt_columns"`
	CommitteeColumns      [4]string       `json:"committee_columns"`
	Receipts              []ReceiptProof  `json:"receipt_shards"`
	Committee             CommitteeProof  `json:"committee_proof"`
	ReceiptFields         [18]FieldCounts `json:"receipt_field_counts"`
	IdentityResolved      bool            `json:"person_corporation_identity_resolved"`
	EmploymentVerified    bool            `json:"employment_verified"`
	OwnershipVerified     bool            `json:"ownership_verified"`
	TerminalPolicyAdopted bool            `json:"terminal_policy_adopted"`
	FinancialAttribution  bool            `json:"financial_attribution"`
}

// Operational timing/worker choices do not enter the identity or output. All
// declared source ancestry, mappings, evidence limits and values do enter it.
func logicalID(r Result) string {
	r.ViewID = ""
	b, _ := json.Marshal(r)
	return hashBytes(b)
}

func digest(s string) bool {
	b, err := hex.DecodeString(s)
	return err == nil && len(b) == 32 && strings.ToLower(s) == s
}

func hashBytes(b []byte) string {
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

func appendText(b []byte, p *string) []byte {
	if p == nil {
		return append(b, 0)
	}
	b = append(b, 1)
	b = binary.BigEndian.AppendUint64(b, uint64(len(*p)))
	return append(b, *p...)
}

func (r Receipt) canonical(b []byte) []byte {
	b = binary.BigEndian.AppendUint64(b[:0], uint64(r.Ordinal))
	b = binary.BigEndian.AppendUint64(b, uint64(r.Cycle))
	b = appendText(b, &r.Normalization)
	for _, p := range r.fields() {
		b = appendText(b, p)
	}
	return b
}

func (r Receipt) validate(ordinal uint64, cycle int64) error {
	if r.Ordinal <= 0 || uint64(r.Ordinal) != ordinal || r.Cycle != cycle || r.Normalization != "valid" {
		return fmt.Errorf("reported identity source ordinal/cycle/state mismatch")
	}
	var size int
	for _, p := range r.fields() {
		if p != nil {
			size += len(*p)
		}
		if size > maxTextBytes {
			return fmt.Errorf("reported identity text exceeds 1MiB row resource cap")
		}
	}
	return nil
}
