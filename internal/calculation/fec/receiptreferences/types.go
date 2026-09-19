// Package receiptreferences joins references over a complete published cycle.
// No reference is a payment, donor identity, or qualified conduit association.
package receiptreferences

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"

	"github.com/vedantadhobley/legal-tender/internal/calculation/fec/reportreference"
	xsort "github.com/vedantadhobley/legal-tender/internal/storage/externalsort"
)

const Version = "legal-tender.fec.cycle-receipt-references.v1"

type Row struct {
	Ordinal       int64   `parquet:"lt_source_row_ordinal" json:"source_row_ordinal"`
	Cycle         int64   `parquet:"lt_two_year_transaction_period" json:"cycle"`
	Normalization string  `parquet:"lt_normalization_state" json:"normalization_state"`
	Recipient     *string `parquet:"cmte_id" json:"committee_id"`
	File          *string `parquet:"file_num" json:"file_num"`
	Transaction   *string `parquet:"tran_id" json:"transaction_id"`
	BackReference *string `parquet:"back_ref_tran_id" json:"back_reference"`
	BackSchedule  *string `parquet:"back_ref_sched_nm" json:"back_schedule"`
	Schedule      *string `parquet:"schedule_type" json:"schedule"`
	Line          *string `parquet:"line_num" json:"line"`
}
type member struct {
	Ordinal        uint64 `json:"ordinal"`
	Schedule, Line *string
}
type lookup struct {
	Count uint64 `json:"count"`
	First member `json:"first_member"`
}
type Decision struct {
	Source             Row     `json:"source"`
	State              string  `json:"state"`
	Target             *uint64 `json:"target_source_row_ordinal"`
	SourceMultiplicity *uint64 `json:"source_key_multiplicity"`
	TargetMultiplicity *uint64 `json:"target_key_multiplicity"`
}
type Neighbors struct {
	Ordinal  uint64 `json:"source_row_ordinal"`
	Peers    uint64 `json:"distinct_exact_reference_peers"`
	Incoming uint64 `json:"incoming_exact_references"`
	Outgoing uint64 `json:"outgoing_exact_references"`
}
type Options struct {
	StorageRoot, Manifest, Cycle, OutputDirectory, BuildSHA256 string
	RunRows, FanIn, FilterBytes, ScanWorkers, Workers          int
	MaxWorkspaceBytes                                          uint64
	Progress                                                   func(string)
}
type Result struct {
	SchemaVersion               string            `json:"schema_version"`
	State                       string            `json:"state"`
	CalculationID               string            `json:"calculation_id"`
	BuildSHA256                 string            `json:"executable_sha256"`
	FactSetID                   string            `json:"fact_set_id"`
	ManifestSHA256              string            `json:"manifest_sha256"`
	Cycle                       string            `json:"cycle"`
	Policy                      string            `json:"reference_policy"`
	SourceRows                  uint64            `json:"source_rows"`
	ReferenceRows               uint64            `json:"reference_rows"`
	LookupMemberRows            uint64            `json:"lookup_member_rows_including_filter_false_positives"`
	States                      map[string]uint64 `json:"states"`
	LookupEvidence              xsort.File        `json:"lookup_evidence"`
	Decisions                   xsort.File        `json:"decisions"`
	ExactIncidences             xsort.File        `json:"exact_reference_incidences"`
	Neighbors                   xsort.File        `json:"exact_reference_neighbors"`
	RunRows                     int               `json:"sort_run_rows"`
	FanIn                       int               `json:"merge_fan_in"`
	FilterBytes                 int               `json:"filter_bytes"`
	ScanWorkers                 int               `json:"scan_workers"`
	Workers                     int               `json:"processing_workers,omitempty"`
	WorkerFanIn                 int               `json:"worker_merge_fan_in,omitempty"`
	Partitions                  []PartitionStats  `json:"processing_partitions,omitempty"`
	StageMS                     map[string]int64  `json:"stage_ms,omitempty"`
	PeakWorkspaceBytes          uint64            `json:"peak_workspace_bytes"`
	RetainedBytes               uint64            `json:"retained_data_bytes"`
	ElapsedMS                   int64             `json:"elapsed_ms"`
	PeakRSSBytes                uint64            `json:"peak_rss_bytes"`
	ConduitEligibilityEvaluated bool              `json:"conduit_eligibility_evaluated"`
	FinancialEligibility        bool              `json:"financial_eligibility"`
	Scope                       string            `json:"scope"`
}

type PartitionStats struct {
	Index         int              `json:"index"`
	SourceRows    uint64           `json:"source_rows"`
	ReferenceRows uint64           `json:"reference_rows"`
	StageMS       map[string]int64 `json:"stage_ms"`
}

// Length framing prevents delimiter collisions. This key is only an exact
// equality/ordering key; it is never a resolved entity identity.
func key(a, b, c *string) string {
	buf := make([]byte, 0, 64)
	for _, s := range []*string{a, b, c} {
		if s == nil {
			buf = append(buf, 0)
			continue
		}
		buf = append(buf, 1)
		var n [4]byte
		binary.BigEndian.PutUint32(n[:], uint32(len(*s)))
		buf = append(buf, n[:]...)
		buf = append(buf, (*s)...)
	}
	return string(buf)
}
func ordinalKey(n uint64) string {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], n)
	return string(b[:])
}
func marshal(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}                            // closed structs only
func present(s *string) bool { return reportreference.Present(s) }
func scope(r Row) bool       { return reportreference.ValidScope(r.Recipient, r.File) }
func validateRow(r Row, ordinal uint64, cycle int64) error {
	if r.Ordinal <= 0 || uint64(r.Ordinal) != ordinal || r.Cycle != cycle || r.Normalization != "valid" {
		return fmt.Errorf("receipt reference source identity mismatch at %d", ordinal)
	}
	// Fail explicitly, never truncate an identifier or allow unbounded sort records.
	for _, s := range []*string{r.Recipient, r.File, r.Transaction, r.BackReference, r.BackSchedule, r.Schedule, r.Line} {
		if s != nil && len(*s) > 4096 {
			return fmt.Errorf("source identifier exceeds 4096-byte contract at %d", ordinal)
		}
	}
	return nil
}

type filter []byte

func (f filter) visit(k string, add bool) bool {
	d := sha256.Sum256([]byte(k))
	a, b := binary.LittleEndian.Uint64(d[:8]), binary.LittleEndian.Uint64(d[8:16])|1
	for i := uint64(0); i < 4; i++ {
		n := (a + i*b) % uint64(len(f)*8)
		mask := byte(1 << uint(n%8))
		if add {
			f[n/8] |= mask
		} else if f[n/8]&mask == 0 {
			return false
		}
	}
	return true
}
