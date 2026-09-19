package fundingbasis

import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"sync"

	"github.com/vedantadhobley/legal-tender/internal/calculation/fec/committeeflows"
	"github.com/vedantadhobley/legal-tender/internal/calculation/fec/earmarkassociation"
	"github.com/vedantadhobley/legal-tender/internal/calculation/fec/reportreference"
)

const ReportPolicy = earmarkassociation.Policy
const maxReportReviewRows = 10000

var reportNumberPattern = regexp.MustCompile(`^[1-9][0-9]*$`)

// ReportEvidence exhausts one report's occurrences in the selected published
// cycle. It does not assert completeness of the original filing or other cycles.
type ReportEvidence struct {
	SchemaVersion    string               `json:"schema_version"`
	Policy           string               `json:"policy"`
	InventoryID      string               `json:"inventory_calculation_id"`
	Cycle            string               `json:"cycle"`
	Input            Input                `json:"input"`
	Committee        string               `json:"committee_id"`
	File             string               `json:"file_num"`
	Scope            string               `json:"scope"`
	Receipts         []Receipt            `json:"receipts"`
	References       []ReportReference    `json:"references"`
	Associations     []ConduitAssociation `json:"associations"`
	TerminalEligible bool                 `json:"terminal_attribution_eligible"`
}

type ReportReference struct {
	Ordinal uint64  `json:"source_row_ordinal"`
	State   string  `json:"state"`
	Target  *uint64 `json:"target_source_row_ordinal"`
}

type ConduitAssociation struct {
	Ordinal          uint64   `json:"earmark_source_row_ordinal"`
	State            string   `json:"state"`
	Related          []uint64 `json:"related_source_row_ordinals"`
	ConduitID        *string  `json:"reported_conduit_committee_id"`
	AmountComparison string   `json:"related_amount_comparison"`
	AdditionalAmount string   `json:"additional_amount_minor_units"`
	TerminalEligible bool     `json:"terminal_attribution_eligible"`
}

// ReviewReport is deliberately bounded. A large report fails, never truncates.
// The inventory bitmap includes every component, including memo/unknown rows.
func (r *Reader) ReviewReport(ctx context.Context, committee, file string, progress func(string)) (ReportEvidence, error) {
	return r.reviewReport(ctx, committee, file, maxReportReviewRows, progress)
}

func (r *Reader) reviewReport(ctx context.Context, committee, file string, limit int, progress func(string)) (ReportEvidence, error) {
	if !committeeflows.ValidCommitteeID(&committee) || !reportNumberPattern.MatchString(file) {
		return ReportEvidence{}, fmt.Errorf("exact committee ID and positive report file number required")
	}
	out := ReportEvidence{SchemaVersion: "legal-tender.fec.receipt-report-evidence.v1", Policy: ReportPolicy, InventoryID: r.result.CalculationID, Cycle: r.result.Cycle, Input: r.result.Input, Committee: committee, File: file, Scope: "published_schedule_a_cycle_report", Receipts: []Receipt{}}
	bits := make([]byte, (len(r.manifest.Shards)+7)/8)
	for _, b := range r.result.Buckets {
		if b.Key.Recipient.Present && b.Key.Recipient.Value == committee {
			for i, v := range b.Shards {
				bits[i] |= v
			}
		}
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	jobs := make(chan int)
	type result struct {
		rows []Receipt
		err  error
	}
	results := make(chan result, 4)
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			for i := range jobs {
				rows, err := r.queryShardFile(ctx, i, Query{Committee: committee}, limit+1, file)
				select {
				case results <- result{rows, err}:
				case <-ctx.Done():
					return
				}
				if err != nil {
					return
				}
			}
		})
	}
	go func() {
		defer close(jobs)
		for i := range r.manifest.Shards {
			if bits[i/8]&(1<<uint(i%8)) != 0 {
				select {
				case jobs <- i:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	go func() { wg.Wait(); close(results) }()
	var firstError error
	completed := 0
	for got := range results {
		if firstError != nil {
			continue
		}
		if got.err != nil {
			firstError = got.err
			cancel()
			continue
		}
		if len(out.Receipts)+len(got.rows) > limit {
			firstError = fmt.Errorf("report review exceeds %d rows; use a streaming calculation", limit)
			cancel()
			continue
		}
		out.Receipts = append(out.Receipts, got.rows...)
		completed++
		if progress != nil && completed%16 == 0 {
			progress(fmt.Sprintf("reviewed %d report-bearing committee shards", completed))
		}
	}
	if firstError != nil {
		return ReportEvidence{}, firstError
	}
	if err := ctx.Err(); err != nil {
		return ReportEvidence{}, err
	}
	sort.Slice(out.Receipts, func(i, j int) bool { return out.Receipts[i].Ordinal < out.Receipts[j].Ordinal })
	refs, associations, err := resolveReport(out.Receipts, committee, file)
	if err != nil {
		return ReportEvidence{}, err
	}
	out.References = refs
	out.Associations = associations
	return out, nil
}

func resolveReport(rows []Receipt, committee, file string) ([]ReportReference, []ConduitAssociation, error) {
	inputs := make([]evidenceInput, len(rows))
	transactions := make(map[string][]int)
	for i, row := range rows {
		for _, key := range []string{"schedule_type", "line_num"} {
			value, exists := row.Fields[key]
			if !exists {
				return nil, nil, fmt.Errorf("missing source field %s", key)
			}
			if value != nil {
				if _, ok := value.(string); !ok {
					return nil, nil, fmt.Errorf("invalid string source field %s", key)
				}
			}
		}
		if i > 0 && row.Ordinal <= rows[i-1].Ordinal {
			return nil, nil, fmt.Errorf("non-unique report occurrence order")
		}
		v, err := decodeEvidence(row)
		if err != nil {
			return nil, nil, err
		}
		if v.row.Recipient == nil || *v.row.Recipient != committee || v.file == nil || *v.file != file {
			return nil, nil, fmt.Errorf("report scope mismatch")
		}
		inputs[i] = v
		if present(v.transaction) {
			transactions[*v.transaction] = append(transactions[*v.transaction], i)
		}
	}
	refs := make([]ReportReference, len(rows))
	adjacent := make([][]int, len(rows))
	unsafe := make([]bool, len(rows))
	for i, v := range inputs {
		targets := transactions[valueOrEmpty(v.backReference)]
		input := reportreference.Input{Ordinal: rows[i].Ordinal, ScopeValid: true, Transaction: v.transaction,
			BackReference: v.backReference, BackSchedule: v.backSchedule, SourceCount: uint64(len(transactions[valueOrEmpty(v.transaction)])), TargetCount: uint64(len(targets))}
		if len(targets) == 1 {
			target := rows[targets[0]]
			input.TargetOrdinal = target.Ordinal
			if s, ok := target.Fields["schedule_type"].(string); ok {
				input.TargetSchedule = &s
			}
			if s, ok := target.Fields["line_num"].(string); ok {
				input.TargetLine = &s
			}
		}
		state, targetOrdinal := reportreference.Decide(input)
		d := ReportReference{Ordinal: rows[i].Ordinal, State: state}
		if present(v.backReference) || present(v.backSchedule) {
			if targetOrdinal != 0 {
				d.Target = &targetOrdinal
				target := targets[0]
				adjacent[i] = append(adjacent[i], target)
				adjacent[target] = append(adjacent[target], i)
			}
			if d.State != "exact_same_report_reference" {
				unsafe[i] = true
				if present(v.backReference) {
					for _, target := range transactions[*v.backReference] {
						unsafe[target] = true
					}
				}
			}
		}
		if present(v.transaction) && len(transactions[*v.transaction]) != 1 {
			unsafe[i] = true
		}
		refs[i] = d
	}
	associations := []ConduitAssociation{}
	for i, v := range inputs {
		original := associationEvidence(v)
		if !earmarkassociation.Applies(original) {
			continue
		}
		d := ConduitAssociation{Ordinal: rows[i].Ordinal, State: "no_exact_related_memo", Related: []uint64{}, AdditionalAmount: "0", AmountComparison: "not_assessed"}
		unique := make(map[int]bool)
		for _, j := range adjacent[i] {
			unique[j] = true
		}
		indexes := make([]int, 0, len(unique))
		for j := range unique {
			indexes = append(indexes, j)
		}
		sort.Ints(indexes)
		for _, j := range indexes {
			d.Related = append(d.Related, rows[j].Ordinal)
		}
		var related *earmarkassociation.Related
		if len(indexes) == 1 {
			j := indexes[0]
			peerSet := make(map[int]bool)
			for _, peer := range adjacent[j] {
				peerSet[peer] = true
			}
			related = &earmarkassociation.Related{Evidence: associationEvidence(inputs[j]), Topology: earmarkassociation.Topology{Unsafe: unsafe[j], Peers: uint64(len(peerSet))}}
		}
		decision, err := earmarkassociation.Decide(original, earmarkassociation.Topology{Unsafe: unsafe[i], Peers: uint64(len(indexes))}, related)
		if err != nil {
			return nil, nil, err
		}
		d.State, d.ConduitID, d.AmountComparison = decision.State, decision.ConduitID, decision.AmountComparison
		d.AdditionalAmount, d.TerminalEligible = decision.AdditionalAmount, decision.TerminalEligible
		associations = append(associations, d)
	}
	return refs, associations, nil
}

func associationEvidence(v evidenceInput) earmarkassociation.Evidence {
	r := v.row
	return earmarkassociation.Evidence{Memo: r.Memo, ReceiptType: r.ReceiptType, Entity: v.entity, Contributor: r.Contributor, CleanContributor: r.CleanContributor, ConduitID: r.ConduitID, Amount: r.Amount}
}

// Empty IDs are absent from the transaction index, as in the shared policy.
func valueOrEmpty(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
