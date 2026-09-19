package reportperiod

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strconv"

	"github.com/vedantadhobley/legal-tender/internal/source/fec/reportmetadata"
)

const Version = "legal-tender.fec.report-period-membership-review.v1"

type Request struct {
	CapturePath string
	Start       string
	End         string
}

type Observation struct {
	Page           int      `json:"page"`
	Ordinal        int      `json:"ordinal"`
	FileNumber     string   `json:"file_number"`
	Period         *Period  `json:"period"`
	ReportForm     string   `json:"report_form"`
	ReportType     string   `json:"report_type"`
	ReportYear     int      `json:"report_year"`
	PublisherState string   `json:"publisher_state"` // not_amended, amended, unknown; not financial membership
	WindowRelation string   `json:"window_relation"`
	Blockers       []string `json:"blockers"`
}

// A cohort is an exact reported form/type/year/interval bucket, not an inferred
// amendment family. Explicit chain evidence must justify its source membership.
type Cohort struct {
	Period
	ReportForm             string   `json:"report_form"`
	ReportType             string   `json:"report_type"`
	ReportYear             int      `json:"report_year"`
	ObservationIndexes     []int    `json:"observation_indexes"`
	PublisherMemberIndexes []int    `json:"publisher_member_indexes"`
	ChainCandidateIndex    *int     `json:"chain_candidate_index"`
	Blockers               []string `json:"blockers"`
}

type Review struct {
	Version                  string                `json:"version"`
	Window                   Period                `json:"window"`
	Evidence                 reportmetadata.Review `json:"evidence"`
	Observations             []Observation         `json:"observations"`
	Cohorts                  []Cohort              `json:"cohorts"`
	UngroupedIndexes         []int                 `json:"ungrouped_indexes"`
	PublisherMemberIndexes   []int                 `json:"publisher_member_indexes"`
	ChainCandidateIndexes    []int                 `json:"chain_candidate_indexes"`
	PublisherCoverage        Coverage              `json:"publisher_coverage"`
	ChainCoverage            Coverage              `json:"chain_coverage"`
	PartitionBlockers        []string              `json:"partition_blockers"`
	ObservedPartitionReady   bool                  `json:"observed_partition_ready"`
	FinancialMembershipReady bool                  `json:"financial_membership_ready"` // deliberately false
	CycleTotalReady          bool                  `json:"cycle_total_ready"`          // deliberately false
}

func Inspect(ctx context.Context, request Request) (Review, error) {
	window := period(request.Start, request.End)
	if window == nil || window.Start != request.Start || window.End != request.End {
		return Review{}, errors.New("require ordered YYYY-MM-DD window dates")
	}
	lo, hi := bounds(*window)
	if hi-lo > 36600 {
		return Review{}, errors.New("requested window exceeds 36600-day review limit")
	}
	evidence, err := reportmetadata.ReadCapture(ctx, request.CapturePath)
	if err != nil {
		return Review{}, err
	}
	form := map[string]string{"/v1/reports/pac-party/": "Form 3X", "/v1/reports/house-senate/": "Form 3"}[evidence.Endpoint]
	if evidence.State != "validated_observations" || form == "" || evidence.Query.CommitteeID == "" {
		return Review{}, errors.New("require validated single-committee report-endpoint capture")
	}
	r := Review{Version: Version, Window: *window, Evidence: evidence, Observations: []Observation{},
		Cohorts: []Cohort{}, UngroupedIndexes: []int{}, PublisherMemberIndexes: []int{}, ChainCandidateIndexes: []int{}, PartitionBlockers: []string{}}
	type key struct {
		Period
		Form, ReportType string
		Year             int
	}
	groups := map[key][]int{}
	raws := []map[string]json.RawMessage{}
	for _, page := range evidence.Pages {
		for _, record := range page.Records {
			if err := ctx.Err(); err != nil {
				return Review{}, err
			}
			var raw map[string]json.RawMessage
			if err := json.Unmarshal(record.Raw, &raw); err != nil {
				return Review{}, errors.New("invalid verified report record")
			}
			i := len(r.Observations)
			o := observation(page.Capture.Page, record, raw, form, *window)
			raws = append(raws, raw)
			r.Observations = append(r.Observations, o)
			if o.PublisherState == "not_amended" {
				r.PublisherMemberIndexes = append(r.PublisherMemberIndexes, i)
			}
			if len(o.Blockers) > 0 {
				r.UngroupedIndexes = append(r.UngroupedIndexes, i)
				if o.WindowRelation != "outside" {
					r.PartitionBlockers = append(r.PartitionBlockers, "ungrouped_report_scope")
				}
				continue
			}
			k := key{*o.Period, o.ReportForm, o.ReportType, o.ReportYear}
			groups[k] = append(groups[k], i)
		}
	}
	for k, members := range groups {
		if err := ctx.Err(); err != nil {
			return Review{}, err
		}
		c := Cohort{Period: k.Period, ReportForm: k.Form, ReportType: k.ReportType, ReportYear: k.Year,
			ObservationIndexes: members, PublisherMemberIndexes: []int{}, Blockers: []string{}}
		qualifyChain(&c, r.Observations, raws)
		if c.ChainCandidateIndex != nil {
			r.ChainCandidateIndexes = append(r.ChainCandidateIndexes, *c.ChainCandidateIndex)
		}
		if len(c.Blockers) > 0 && intersects(c.Period, *window) {
			r.PartitionBlockers = append(r.PartitionBlockers, "unresolved_report_cohort")
		}
		r.Cohorts = append(r.Cohorts, c)
	}
	slices.SortFunc(r.Cohorts, func(a, b Cohort) int {
		if c := comparePeriod(a.Period, b.Period); c != 0 {
			return c
		}
		if c := cmp.Compare(a.ReportForm, b.ReportForm); c != 0 {
			return c
		}
		if c := cmp.Compare(a.ReportYear, b.ReportYear); c != 0 {
			return c
		}
		return cmp.Compare(a.ReportType, b.ReportType)
	})
	slices.Sort(r.ChainCandidateIndexes)
	r.PublisherCoverage = coverage(*window, r.Observations, r.PublisherMemberIndexes)
	r.ChainCoverage = coverage(*window, r.Observations, r.ChainCandidateIndexes)
	if evidence.PaginationState != "exact_count_satisfied" && evidence.PaginationState != "empty_page_observed" {
		r.PartitionBlockers = append(r.PartitionBlockers, "partial_metadata_traversal")
	}
	if r.ChainCoverage.GapDays > 0 {
		r.PartitionBlockers = append(r.PartitionBlockers, "uncovered_days")
	}
	if r.ChainCoverage.OverlapDays > 0 {
		r.PartitionBlockers = append(r.PartitionBlockers, "overlapping_report_periods")
	}
	if len(r.ChainCoverage.CrossBoundaryIndexes) > 0 {
		r.PartitionBlockers = append(r.PartitionBlockers, "cross_boundary_report_amount_not_apportioned")
	}
	slices.Sort(r.PartitionBlockers)
	r.PartitionBlockers = slices.Compact(r.PartitionBlockers)
	r.ObservedPartitionReady = len(r.PartitionBlockers) == 0
	return r, nil
}

func observation(page int, record reportmetadata.Record, raw map[string]json.RawMessage, form string, window Period) Observation {
	o := Observation{Page: page, Ordinal: record.Ordinal, FileNumber: record.FileNumber, ReportForm: text(raw["report_form"]),
		ReportType: text(raw["report_type"]), PublisherState: "unknown", WindowRelation: "unknown", Blockers: []string{}}
	o.ReportYear, _ = strconv.Atoi(string(raw["report_year"]))
	o.Period = period(text(raw["coverage_start_date"]), text(raw["coverage_end_date"]))
	switch string(raw["is_amended"]) {
	case "false":
		o.PublisherState = "not_amended"
	case "true":
		o.PublisherState = "amended"
	}
	if o.ReportForm != form {
		o.Blockers = append(o.Blockers, "unsupported_report_form")
	}
	if o.ReportType == "" {
		o.Blockers = append(o.Blockers, "missing_report_type")
	}
	if o.ReportYear < 1 || o.ReportYear > 9999 {
		o.Blockers = append(o.Blockers, "invalid_report_year")
	}
	if o.Period == nil {
		o.Blockers = append(o.Blockers, "invalid_report_period")
	} else {
		o.WindowRelation = "inside"
		if !intersects(*o.Period, window) {
			o.WindowRelation = "outside"
		} else if o.Period.Start < window.Start || o.Period.End > window.End {
			o.WindowRelation = "crosses_boundary"
		}
	}
	return o
}

func text(raw json.RawMessage) string { var s string; _ = json.Unmarshal(raw, &s); return s }
