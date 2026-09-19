package flowreconciliation

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"sort"
	"strings"
	"sync"

	"github.com/parquet-go/parquet-go"
	receiver "github.com/vedantadhobley/legal-tender/internal/calculation/fec/committeeflows"
	"github.com/vedantadhobley/legal-tender/internal/calculation/fec/disbursements"
	artifact "github.com/vedantadhobley/legal-tender/internal/storage/artifact"
)

const maxSelected = 1_000_000

type shard struct {
	key                      string
	first, last, rows, bytes uint64
}
type scanned struct {
	total        Measures
	decisions    map[DecisionKey]Measures
	observations []Observation
	err          error
}

type aRow struct {
	Ordinal       int64   `parquet:"lt_source_row_ordinal"`
	Period        int64   `parquet:"lt_two_year_transaction_period"`
	Normalization string  `parquet:"lt_normalization_state"`
	SubID         string  `parquet:"sub_id"`
	Recipient     *string `parquet:"cmte_id,optional"`
	Raw           *string `parquet:"contbr_id,optional"`
	Clean         *string `parquet:"clean_contbr_id,optional"`
	Memo          bool    `parquet:"lt_memoed_subtotal"`
	Type          *string `parquet:"receipt_tp,optional"`
	Amount        *int64  `parquet:"lt_receipt_amount_minor_units,optional"`
	AmountState   string  `parquet:"lt_receipt_amount_state"`
	Date          *int32  `parquet:"lt_receipt_date,optional"`
}

func selectA(r aRow) (DecisionKey, *Observation, error) {
	if r.Normalization != "valid" || (r.AmountState == "reported_value") != (r.Amount != nil) || (r.AmountState != "reported_value" && r.AmountState != "source_null") {
		return DecisionKey{}, nil, fmt.Errorf("invalid Schedule A normalization")
	}
	e := receiver.Evaluate(receiver.EvaluationInput{NormalizationState: r.Normalization, RecipientCommitteeID: r.Recipient, ContributorID: r.Raw, CleanContributorID: r.Clean, MemoedSubtotal: r.Memo, AmountObservationState: r.AmountState, AmountMinorUnits: r.Amount, ReceiptTypeCode: r.Type})
	d := DecisionKey{e.Decision, e.ReceiptRole, cell(r.Type)}
	if e.Decision != receiver.DecisionIncluded {
		return d, nil, nil
	}
	role, ok := ReceiverFlowRole(e.ReceiptRole)
	if !ok {
		return d, nil, fmt.Errorf("unsupported receiver flow role")
	}
	return d, newObservation(r.Ordinal, r.SubID, e.SourceCommitteeID, *r.Recipient, role, *r.Type, e.ReceiptRole, r.Date, *r.Amount), nil
}

// ReceiverFlowRole is the shared mapping from the accepted receipt reporting
// role to the observation vocabulary. It does not expand source membership.
func ReceiverFlowRole(reportingRole string) (string, bool) {
	switch reportingRole {
	case receiver.RoleRegisteredFilerContribution:
		return "contribution", true
	case receiver.RoleRegisteredFilerInKind:
		return "in_kind", true
	case receiver.RoleAffiliatedTransferIn:
		return "affiliated_transfer", true
	case receiver.RoleRefundOrRepaymentReceived:
		return "refund_or_repayment", true
	default:
		return "", false
	}
}

type bRow struct {
	Ordinal     int64   `parquet:"lt_source_row_ordinal"`
	Period      int64   `parquet:"lt_two_year_transaction_period"`
	SubID       string  `parquet:"sub_id"`
	Sender      *string `parquet:"cmte_id,optional"`
	Raw         *string `parquet:"recipient_cmte_id,optional"`
	Clean       *string `parquet:"clean_recipient_cmte_id,optional"`
	Form        string  `parquet:"filing_form"`
	Line        *string `parquet:"line_num,optional"`
	Schedule    *string `parquet:"schedule_type,optional"`
	MemoCode    *string `parquet:"memo_cd,optional"`
	Memo        bool    `parquet:"lt_memoed_subtotal"`
	Type        *string `parquet:"disb_tp,optional"`
	Beneficiary *string `parquet:"benef_cmte_nm,optional"`
	Conduit     *string `parquet:"conduit_cmte_nm,optional"`
	Amount      *int64  `parquet:"lt_disbursement_amount_minor_units,optional"`
	AmountState string  `parquet:"lt_disbursement_amount_state"`
	Date        *int32  `parquet:"lt_disbursement_date,optional"`
}

func selectB(r bRow) (DecisionKey, *Observation, error) {
	d, role, err := EvaluateSender(disbursements.Input{Sender: r.Sender, RawRecipient: r.Raw, CleanRecipient: r.Clean, Form: r.Form, Line: r.Line, Schedule: r.Schedule, MemoCode: r.MemoCode, Memoed: r.Memo, DisbursementType: r.Type, BeneficiaryName: r.Beneficiary, ConduitName: r.Conduit, Amount: r.Amount, AmountState: r.AmountState})
	if err != nil || d.State != Included {
		return d, nil, err
	}
	return d, newObservation(r.Ordinal, r.SubID, *r.Sender, *r.Clean, role, *r.Type, d.ReportingRole, r.Date, *r.Amount), nil
}
func cell(s *string) disbursements.Cell {
	if s == nil {
		return disbursements.Cell{}
	}
	return disbursements.Cell{Present: true, Value: *s}
}
func newObservation(ordinal int64, sub, sender, recipient, role, code, reporting string, date *int32, amount int64) *Observation {
	var day *int32
	if date != nil {
		value := *date
		day = &value
	}
	return &Observation{uint64(ordinal), strings.Clone(sub), strings.Clone(sender), strings.Clone(recipient), role, strings.Clone(code), reporting, day, amount}
}

func scan[T any](ctx context.Context, root string, shards []shard, schema *parquet.Schema, period int64, workers int, selectRow func(T) (DecisionKey, *Observation, error), coordinates func(T) (int64, int64, *int64), progress func(string)) (scanned, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	jobs := make(chan shard)
	results := make(chan scanned, workers)
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for s := range jobs {
				if ctx.Err() != nil {
					return
				}
				r := scanShard(ctx, root, s, schema, period, selectRow, coordinates)
				results <- r
				if r.err != nil {
					cancel()
					return
				}
			}
		}()
	}
	go func() {
		defer close(jobs)
		for _, s := range shards {
			select {
			case jobs <- s:
			case <-ctx.Done():
				return
			}
		}
	}()
	go func() { wg.Wait(); close(results) }()
	out := scanned{decisions: map[DecisionKey]Measures{}}
	var firstErr error
	completed := 0
	for r := range results {
		if r.err != nil && firstErr == nil {
			firstErr = r.err
			cancel()
		}
		if firstErr != nil {
			continue
		}
		if err := merge(&out.total, r.total); err != nil {
			firstErr = err
			cancel()
			continue
		}
		for k, m := range r.decisions {
			v := out.decisions[k]
			if err := merge(&v, m); err != nil {
				firstErr = err
				cancel()
				break
			}
			out.decisions[k] = v
		}
		if len(out.decisions) > 10000 {
			firstErr = fmt.Errorf("decision shape limit exceeded")
			cancel()
			continue
		}
		if len(out.observations)+len(r.observations) > maxSelected {
			firstErr = fmt.Errorf("selected cohort exceeds bounded capacity; review before expansion")
			cancel()
			continue
		}
		out.observations = append(out.observations, r.observations...)
		completed++
		if completed%32 == 0 || completed == len(shards) {
			progress(fmt.Sprintf("scanned %d/%d shards, %d facts, %d selected observations", completed, len(shards), out.total.Rows, len(out.observations)))
		}
	}
	if firstErr != nil {
		return scanned{}, firstErr
	}
	if ctx.Err() != nil {
		return scanned{}, ctx.Err()
	}
	if completed != len(shards) {
		return scanned{}, fmt.Errorf("incomplete source scan")
	}
	sort.Slice(out.observations, func(i, j int) bool { return out.observations[i].Ordinal < out.observations[j].Ordinal })
	var total Measures
	for _, m := range out.decisions {
		if err := merge(&total, m); err != nil {
			return scanned{}, err
		}
	}
	if total != out.total {
		return scanned{}, fmt.Errorf("decision totals not conserved")
	}
	return out, nil
}

func scanShard[T any](ctx context.Context, root string, s shard, schema *parquet.Schema, period int64, selectRow func(T) (DecisionKey, *Observation, error), coordinates func(T) (int64, int64, *int64)) (out scanned) {
	out.decisions = map[DecisionKey]Measures{}
	path, err := artifact.Resolve(root, s.key)
	if err != nil {
		out.err = err
		return
	}
	f, err := os.Open(path)
	if err != nil {
		out.err = err
		return
	}
	defer f.Close()
	physical, err := parquet.OpenFile(f, int64(s.bytes))
	if err != nil {
		out.err = err
		return
	}
	if physical.Schema().String() != schema.String() || physical.NumRows() != int64(s.rows) {
		out.err = fmt.Errorf("source physical schema or row count mismatch")
		return
	}
	reader := parquet.NewGenericReader[T](physical)
	defer reader.Close()
	buf := make([]T, 8192)
	for {
		if ctx.Err() != nil {
			out.err = ctx.Err()
			return
		}
		n, readErr := reader.Read(buf)
		for i := range n {
			ordinal, cycle, amount := coordinates(buf[i])
			if ordinal <= 0 || uint64(ordinal) != s.first+out.total.Rows || uint64(ordinal) > s.last || cycle != period {
				out.err = fmt.Errorf("source locator or cycle mismatch")
				return
			}
			d, o, err := selectRow(buf[i])
			if err != nil {
				out.err = fmt.Errorf("ordinal %d: %w", ordinal, err)
				return
			}
			m := Measures{Rows: 1}
			if amount != nil {
				m.Known = 1
				m.Amount = *amount
			}
			v := out.decisions[d]
			if err := merge(&v, m); err != nil {
				out.err = err
				return
			}
			out.decisions[d] = v
			if err := merge(&out.total, m); err != nil {
				out.err = err
				return
			}
			if len(out.decisions) > 10000 {
				out.err = fmt.Errorf("decision shape limit exceeded")
				return
			}
			if o != nil {
				if len(out.observations) >= maxSelected {
					out.err = fmt.Errorf("cohort capacity exceeded")
					return
				}
				out.observations = append(out.observations, *o)
			}
		}
		if readErr != nil {
			if !errors.Is(readErr, io.EOF) {
				out.err = readErr
			}
			break
		}
		if n == 0 {
			out.err = io.ErrNoProgress
			return
		}
	}
	if out.err == nil && out.total.Rows != s.rows {
		out.err = fmt.Errorf("source shard not conserved")
	}
	return
}
func addMoney(dst *int64, v int64) error {
	if (v > 0 && *dst > math.MaxInt64-v) || (v < 0 && *dst < math.MinInt64-v) {
		return fmt.Errorf("signed amount overflow")
	}
	*dst += v
	return nil
}
func merge(dst *Measures, v Measures) error {
	if math.MaxUint64-dst.Rows < v.Rows || math.MaxUint64-dst.Known < v.Known {
		return fmt.Errorf("count overflow")
	}
	if err := addMoney(&dst.Amount, v.Amount); err != nil {
		return err
	}
	dst.Rows += v.Rows
	dst.Known += v.Known
	return nil
}
