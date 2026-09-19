package fundingbasis

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sort"

	"github.com/vedantadhobley/legal-tender/internal/calculation/fec/committeeflows"
	"github.com/vedantadhobley/legal-tender/internal/calculation/fec/receipts"
	occ "github.com/vedantadhobley/legal-tender/internal/source/fec/occurrence"
)

func limitations() []string {
	return []string{"resolved_person_or_organization_identity", "employer_is_not_donor_identity", "conduit_is_not_original_contributor",
		"unitemized_receipts", "opening_cash_and_prior_cycle_funds", "complete_cash_funding_denominator",
		"chronological_availability", "cash_vs_in_kind_valuation", "terminal_dollar_allocation"}
}

func Run(ctx context.Context, o Options) (Result, error) {
	if o.Workers < 1 || o.Workers > 4 || o.StorageRoot == "" || o.ScheduleA == "" || o.Cycle == "" {
		return Result{}, fmt.Errorf("storage root, Schedule A manifest, cycle, and 1..4 workers required")
	}
	if o.Progress != nil {
		o.Progress("verifying exact Schedule A manifest and backing bytes")
	}
	m, digest, err := occ.LoadPublishedScheduleAColumnarManifest(ctx, o.StorageRoot, o.ScheduleA)
	if err != nil {
		return Result{}, err
	}
	if m.Cycle != o.Cycle {
		return Result{}, fmt.Errorf("Schedule A cycle mismatch")
	}
	if err := dense(m); err != nil {
		return Result{}, err
	}
	buckets, err := scan(ctx, o.StorageRoot, m, o.Workers, o.Progress)
	if err != nil {
		return Result{}, err
	}
	return assemble(m, digest, buckets)
}

func dense(m occ.ScheduleAColumnarManifest) error {
	c := m.Counts
	if c.Facts == 0 || c.Facts != c.SourceOccurrences || c.Facts != c.ValidFacts || c.InvalidFacts != 0 || c.ExcludedOccurrences != 0 {
		return fmt.Errorf("receipt inventory requires a complete dense valid fact set")
	}
	var rows uint64
	for i, s := range m.Shards {
		if s.Index != uint64(i) || s.Facts == 0 || s.Facts != s.SourceRows || s.Facts != s.ValidFacts || s.InvalidFacts != 0 ||
			s.FirstSourceRowOrdinal != rows+1 || s.LastSourceRowOrdinal != rows+s.Facts {
			return fmt.Errorf("receipt inventory requires contiguous valid shards")
		}
		rows += s.Facts
	}
	if rows != c.Facts {
		return fmt.Errorf("Schedule A shard conservation failed")
	}
	return nil
}

func assemble(m occ.ScheduleAColumnarManifest, digest string, buckets []Bucket) (Result, error) {
	r := Result{SchemaVersion: Version, Policy: Policy, IndividualPolicy: receipts.ContractID + "@" + receipts.ContractVersion,
		CommitteePolicy: committeeflows.ContractID + "@" + committeeflows.ContractVersion, Cycle: m.Cycle,
		State: "complete_reported_receipt_inventory", Input: Input{m.FactSetID, digest, m.Counts.Facts, len(m.Shards)}, Buckets: buckets, NotCovered: limitations()}
	sort.Slice(r.Buckets, func(i, j int) bool { return keyText(r.Buckets[i].Key) < keyText(r.Buckets[j].Key) })
	for _, b := range r.Buckets {
		if err := r.Total.merge(b.Measures); err != nil {
			return Result{}, err
		}
	}
	if err := validate(r); err != nil {
		return Result{}, err
	}
	id, err := identity(r)
	r.CalculationID = id
	return r, err
}

func keyText(k Key) string { b, _ := json.Marshal(k); return string(b) }
func identity(r Result) (string, error) {
	r.CalculationID = ""
	b, err := json.Marshal(r)
	if err != nil {
		return "", err
	}
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:]), nil
}

func validate(r Result) error {
	if r.SchemaVersion != Version || r.Policy != Policy || r.IndividualPolicy != receipts.ContractID+"@"+receipts.ContractVersion ||
		r.CommitteePolicy != committeeflows.ContractID+"@"+committeeflows.ContractVersion || r.TerminalEligible ||
		r.State != "complete_reported_receipt_inventory" || !reflect.DeepEqual(r.NotCovered, limitations()) ||
		r.Input.Facts == 0 || r.Input.Shards < 1 || len(r.Buckets) == 0 || len(r.Buckets) > maxBuckets || !r.Total.valid() {
		return fmt.Errorf("invalid receipt inventory contract")
	}
	var total Measures
	previous := ""
	for _, b := range r.Buckets {
		key := keyText(b.Key)
		if key <= previous || !b.Measures.valid() || b.Measures.Rows == 0 || b.First == 0 || b.Last < b.First || b.Last > r.Input.Facts ||
			len(b.Shards) != (r.Input.Shards+7)/8 || !validComponent(b.Key.Component) || (!b.Key.Recipient.Present && b.Key.Recipient.Value != "") {
			return fmt.Errorf("invalid or duplicate receipt inventory bucket")
		}
		present := false
		for i, v := range b.Shards {
			for bit := 0; bit < 8; bit++ {
				if v&(1<<uint(bit)) != 0 {
					present = true
					if i*8+bit >= r.Input.Shards {
						return fmt.Errorf("invalid shard bitmap")
					}
				}
			}
		}
		if !present {
			return fmt.Errorf("empty shard bitmap")
		}
		previous = key
		if err := total.merge(b.Measures); err != nil {
			return err
		}
	}
	if total != r.Total || total.Rows != r.Input.Facts {
		return fmt.Errorf("receipt inventory conservation failed")
	}
	return nil
}

func validComponent(s string) bool {
	switch s {
	case "unresolved_recipient", "memo_subtotal", "overlapping_individual_and_committee", "itemized_individual_only",
		"committee_flow_only", "unknown_amount", "unresolved_individual_class", "other_reported_receipt":
		return true
	}
	return false
}

// Reader pins a checked result and its published backing. Construction verifies
// all shard hashes; each row query rechecks the opened shard to reject changes.
type Reader struct {
	root     string
	result   Result
	manifest occ.ScheduleAColumnarManifest
}

func Open(ctx context.Context, root, path string) (*Reader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if info.Size() > 128<<20 {
		return nil, fmt.Errorf("receipt inventory exceeds 128 MiB limit")
	}
	d := json.NewDecoder(f)
	d.DisallowUnknownFields()
	var r Result
	if err := d.Decode(&r); err != nil {
		return nil, err
	}
	var trailing any
	if err := d.Decode(&trailing); err != io.EOF {
		return nil, fmt.Errorf("trailing inventory data")
	}
	if err := validate(r); err != nil {
		return nil, err
	}
	id, err := identity(r)
	if err != nil || id != r.CalculationID {
		return nil, fmt.Errorf("receipt inventory identity mismatch")
	}
	if b, err := hex.DecodeString(r.Input.FactSetID); err != nil || len(b) != 32 {
		return nil, fmt.Errorf("invalid fact set ID")
	}
	m, digest, err := occ.LoadPublishedScheduleAColumnarManifest(ctx, root, filepath.Join(root, "facts/fec/schedule-a/columnar/manifests", r.Input.FactSetID+".json"))
	if err != nil {
		return nil, err
	}
	if err := dense(m); err != nil {
		return nil, err
	}
	if digest != r.Input.ManifestSHA256 || m.FactSetID != r.Input.FactSetID || m.Cycle != r.Cycle || m.Counts.Facts != r.Input.Facts || len(m.Shards) != r.Input.Shards {
		return nil, fmt.Errorf("receipt inventory source identity mismatch")
	}
	return &Reader{root, r, m}, nil
}
