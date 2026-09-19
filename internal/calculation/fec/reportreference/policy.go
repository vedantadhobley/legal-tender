// Package reportreference owns same-report reference decisions, independently
// of in-memory review or external-memory execution.
package reportreference

import (
	"github.com/vedantadhobley/legal-tender/internal/calculation/fec/committeeflows"
	"regexp"
)

const Policy = "fec/same-report-reference@1.0.0"

var number = regexp.MustCompile(`^[1-9][0-9]*$`)

func Present(p *string) bool { return p != nil && *p != "" }
func ValidScope(committee, file *string) bool {
	return committeeflows.ValidCommitteeID(committee) && file != nil && number.MatchString(*file)
}

type Input struct {
	Ordinal                                  uint64
	ScopeValid                               bool
	Transaction, BackReference, BackSchedule *string
	SourceCount, TargetCount, TargetOrdinal  uint64
	TargetSchedule, TargetLine               *string
}

// Decide preserves the precedence of the accepted bounded reviewer. The
// additional invalid-scope state handles rows that reviewer cannot select.
func Decide(v Input) (string, uint64) {
	if !Present(v.BackReference) && !Present(v.BackSchedule) {
		return "no_report_reference", 0
	}
	if !v.ScopeValid {
		return "invalid_report_scope", 0
	}
	if !Present(v.Transaction) || !Present(v.BackReference) {
		return "incomplete_report_reference", 0
	}
	switch {
	case v.SourceCount != 1:
		return "duplicate_source_transaction_id", 0
	case v.TargetCount > 1:
		return "ambiguous_target_transaction_id", 0
	case !Present(v.BackSchedule):
		return "missing_reference_schedule", 0
	case v.TargetCount == 0:
		return "target_absent_from_cycle_report", 0
	case v.TargetOrdinal == v.Ordinal:
		return "self_reference", 0
	case v.TargetSchedule == nil || *v.TargetSchedule != "SA" ||
		(*v.BackSchedule != "SA" && (v.TargetLine == nil || *v.BackSchedule != "SA"+*v.TargetLine)):
		return "reference_schedule_mismatch", 0
	default:
		return "exact_same_report_reference", v.TargetOrdinal
	}
}
