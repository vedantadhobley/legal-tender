package fundingbasis

import (
	"fmt"
	"math"

	"github.com/vedantadhobley/legal-tender/internal/calculation/fec/committeeflows"
	"github.com/vedantadhobley/legal-tender/internal/calculation/fec/receipts"
)

type receiptRow struct {
	Ordinal          int64   `parquet:"lt_source_row_ordinal"`
	Cycle            int64   `parquet:"lt_two_year_transaction_period"`
	Normalization    string  `parquet:"lt_normalization_state"`
	Recipient        *string `parquet:"cmte_id,optional"`
	Contributor      *string `parquet:"contbr_id,optional"`
	CleanContributor *string `parquet:"clean_contbr_id,optional"`
	Individual       *bool   `parquet:"is_individual,optional"`
	Memo             bool    `parquet:"lt_memoed_subtotal"`
	ReceiptType      *string `parquet:"receipt_tp,optional"`
	Amount           *int64  `parquet:"lt_receipt_amount_minor_units,optional"`
	AmountState      string  `parquet:"lt_receipt_amount_state"`
	ConduitID        *string `parquet:"conduit_cmte_id,optional"`
}

func classify(r receiptRow) Key {
	k := Key{IndividualDecision: receipts.ItemizedIndividualDecision(r.Individual, r.Memo, r.AmountState, r.Amount)}
	if r.Recipient != nil {
		k.Recipient = Cell{true, *r.Recipient}
	}
	f := committeeflows.Evaluate(committeeflows.EvaluationInput{
		NormalizationState: r.Normalization, RecipientCommitteeID: r.Recipient,
		ContributorID: r.Contributor, CleanContributorID: r.CleanContributor,
		MemoedSubtotal: r.Memo, AmountObservationState: r.AmountState,
		AmountMinorUnits: r.Amount, ReceiptTypeCode: r.ReceiptType,
	})
	k.CommitteeDecision = f.Decision
	// Classify the role independently so it remains visible for excluded rows.
	k.ReceiptRole, _ = committeeflows.ClassifyReceiptRole(r.ReceiptType)
	switch {
	case !committeeflows.ValidCommitteeID(r.Recipient):
		k.Component = "unresolved_recipient"
	case r.Memo:
		k.Component = "memo_subtotal"
	case k.IndividualDecision == "included" && f.Decision == committeeflows.DecisionIncluded:
		k.Component = "overlapping_individual_and_committee"
	case k.IndividualDecision == "included":
		k.Component = "itemized_individual_only"
	case f.Decision == committeeflows.DecisionIncluded:
		k.Component = "committee_flow_only"
	case r.Amount == nil:
		k.Component = "unknown_amount"
	case r.Individual == nil:
		k.Component = "unresolved_individual_class"
	default:
		k.Component = "other_reported_receipt"
	}
	return k
}

func add(a, b int64) (int64, error) {
	if b > 0 && a > math.MaxInt64-b || b < 0 && a < math.MinInt64-b {
		return 0, fmt.Errorf("receipt subtotal exceeds exact int64 range")
	}
	return a + b, nil
}

func (m *Measures) observe(r receiptRow) error {
	m.Rows++
	if r.ConduitID != nil && *r.ConduitID != "" {
		m.ConduitIDRows++
	}
	if r.Amount == nil {
		m.Unknown++
		return nil
	}
	m.Known++
	var err error
	m.Signed, err = add(m.Signed, *r.Amount)
	if err != nil {
		return err
	}
	switch {
	case *r.Amount > 0:
		m.PositiveRows++
		m.Positive, err = add(m.Positive, *r.Amount)
	case *r.Amount < 0:
		m.NegativeRows++
		m.Negative, err = add(m.Negative, *r.Amount)
	default:
		m.ZeroRows++
	}
	return err
}

func (m *Measures) merge(b Measures) error {
	var err error
	m.Signed, err = add(m.Signed, b.Signed)
	if err != nil {
		return err
	}
	m.Positive, err = add(m.Positive, b.Positive)
	if err != nil {
		return err
	}
	m.Negative, err = add(m.Negative, b.Negative)
	if err != nil {
		return err
	}
	m.Rows += b.Rows
	m.Known += b.Known
	m.Unknown += b.Unknown
	m.PositiveRows += b.PositiveRows
	m.NegativeRows += b.NegativeRows
	m.ZeroRows += b.ZeroRows
	m.ConduitIDRows += b.ConduitIDRows
	return nil
}

func (m Measures) valid() bool {
	net, err := add(m.Positive, m.Negative)
	return err == nil && net == m.Signed && m.Rows == m.Known+m.Unknown &&
		m.Known == m.PositiveRows+m.NegativeRows+m.ZeroRows && m.ConduitIDRows <= m.Rows &&
		m.Positive >= 0 && m.Negative <= 0 && (m.PositiveRows == 0) == (m.Positive == 0) && (m.NegativeRows == 0) == (m.Negative == 0)
}
