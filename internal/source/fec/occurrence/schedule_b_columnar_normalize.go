package occurrence

import (
	"fmt"
	"strconv"
	"time"

	fecmoney "github.com/vedantadhobley/legal-tender/internal/source/fec/money"
	"github.com/vedantadhobley/legal-tender/internal/source/fec/scheduleb"
	"github.com/vedantadhobley/legal-tender/internal/source/fec/schedulebparquet"
)

func scheduleBColumnarDerived(record scheduleb.Record) (schedulebparquet.Derived, error) {
	value := func(name string) *string {
		source, ok := record.ValueByName(name)
		if !ok || source.Null {
			return nil
		}
		copy := source.Lexeme
		return &copy
	}
	integer := func(name string, required bool) (*int64, error) {
		raw := value(name)
		if raw == nil {
			if required {
				return nil, fmt.Errorf("validated Schedule B record has no %s", name)
			}
			return nil, nil
		}
		parsed, err := strconv.ParseInt(*raw, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("parse Schedule B %s: %w", name, err)
		}
		return &parsed, nil
	}
	money := func(name string) (*int64, *int32, string, error) {
		raw := value(name)
		if raw == nil {
			return nil, nil, "source_null", nil
		}
		minor, sourceScale, issue := fecmoney.ParseUSDMinorUnits(*raw)
		if issue != "" {
			return nil, nil, "", fmt.Errorf("validated Schedule B %s has %s", name, issue)
		}
		parsed, err := strconv.ParseInt(minor, 10, 64)
		if err != nil {
			return nil, nil, "", fmt.Errorf("parse Schedule B %s minor units: %w", name, err)
		}
		scale := int32(sourceScale)
		return &parsed, &scale, "reported_value", nil
	}
	timestamp := func(name string) (*int64, *int32, error) {
		raw := value(name)
		if raw == nil {
			return nil, nil, nil
		}
		parsed, err := time.Parse("2006-01-02 15:04:05.999999999", *raw)
		if err != nil {
			return nil, nil, fmt.Errorf("parse validated Schedule B %s: %w", name, err)
		}
		nanos := parsed.UnixNano()
		days := int32(parsed.Unix() / 86400)
		return &nanos, &days, nil
	}

	var result schedulebparquet.Derived
	var err error
	result.DisbursementAmountMinorUnits, result.DisbursementAmountSourceScale, result.DisbursementAmountState, err = money("disb_amt")
	if err != nil {
		return result, err
	}
	result.BundledRefundMinorUnits, result.BundledRefundSourceScale, result.BundledRefundState, err = money("semi_an_bundled_refund")
	if err != nil {
		return result, err
	}
	result.DisbursementAtLocalNanos, result.DisbursementDateDays, err = timestamp("disb_dt")
	if err != nil {
		return result, err
	}
	result.CommunicationAtLocalNanos, result.CommunicationDateDays, err = timestamp("comm_dt")
	if err != nil {
		return result, err
	}
	result.PublisherLoadedAtNanos, _, err = timestamp("pg_date")
	if err != nil {
		return result, err
	}
	result.ReportYear, err = integer("rpt_yr", false)
	if err != nil {
		return result, err
	}
	period, err := integer("two_year_transaction_period", true)
	if err != nil {
		return result, err
	}
	result.TwoYearTransactionPeriod = *period
	memo := value("memo_cd")
	result.MemoedSubtotal = memo != nil && *memo == "X"
	return result, nil
}
