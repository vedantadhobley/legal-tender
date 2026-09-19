package personaffiliation

import (
	"strings"
	"time"

	"github.com/vedantadhobley/legal-tender/internal/source/wikimedia"
)

// Compare at the publisher's granularity, without emitting invented day bounds.
// Exact day endpoints follow the existing inclusive screening convention. For
// coarse bounds, equality means an uncertain boundary, not full-year/month service.
// This compares qualifiers on a claim, not its truth or all its other constraints.
func compareRoleDate(raw *string, s wikimedia.RoleStatement) string {
	if raw == nil || *raw == "" {
		return "receipt_date_unknown"
	}
	day, ok := receiptDay(*raw)
	if !ok {
		return "receipt_date_unusable"
	}
	times := map[string]wikimedia.RoleTime{}
	for _, tm := range s.Times {
		if tm.Property != "P580" && tm.Property != "P582" && tm.Property != "P585" {
			return "unsupported_time_qualifier"
		}
		if _, duplicate := times[tm.Property]; duplicate {
			return "multiple_time_values_unassessed"
		}
		layout := ""
		switch tm.State {
		case "year_precision":
			layout = "2006"
		case "month_precision":
			layout = "2006-01"
		case "day_precision":
			layout = time.DateOnly
		default:
			return "unsupported_or_unknown_time_value"
		}
		if _, err := time.Parse(layout, tm.Text); err != nil || len(tm.Text) != len(layout) {
			return "unsupported_or_unknown_time_value"
		}
		times[tm.Property] = tm
	}
	if len(times) == 0 {
		return "role_time_unknown"
	}
	compare := func(tm wikimedia.RoleTime) int { return strings.Compare(day[:len(tm.Text)], tm.Text) }
	if asOf, found := times["P585"]; found {
		if len(times) != 1 {
			return "mixed_as_of_and_period_unassessed"
		}
		if compare(asOf) != 0 {
			return "different_as_of_period_not_role_exclusion"
		}
		if asOf.State != "day_precision" {
			return "within_as_of_precision_unconfirmed"
		}
		return "on_reported_as_of_day"
	}
	start, hasStart := times["P580"]
	end, hasEnd := times["P582"]
	if hasStart && hasEnd {
		n := min(len(start.Text), len(end.Text))
		if start.Text[:n] > end.Text[:n] {
			return "inconsistent_reported_bounds"
		}
	}
	if hasStart && compare(start) < 0 {
		return "before_reported_start_precision"
	}
	if hasEnd && compare(end) > 0 {
		return "after_reported_end_precision"
	}
	if !hasStart || !hasEnd {
		return "open_period_unconfirmed"
	}
	if (compare(start) == 0 && start.State != "day_precision") || (compare(end) == 0 && end.State != "day_precision") {
		return "within_boundary_precision_unconfirmed"
	}
	return "within_reported_bounds"
}
