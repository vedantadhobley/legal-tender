// Package reportperiod reviews source-reported membership and calendar coverage.
// Window calculations aggregate bound reported fields, not financially effective cash.
package reportperiod

import (
	"cmp"
	"slices"
	"strings"
	"time"
)

type Period struct {
	Start string `json:"start"`
	End   string `json:"end"`
}

type Segment struct {
	Period
	MembershipCount int `json:"membership_count"`
}

type Coverage struct {
	Segments             []Segment `json:"segments"`
	WindowDays           int64     `json:"window_days"`
	CoveredDays          int64     `json:"covered_days"`
	GapDays              int64     `json:"gap_days"`
	OverlapDays          int64     `json:"overlap_days"`
	CrossBoundaryIndexes []int     `json:"cross_boundary_indexes"` // indexes into Review.Observations
}

// CoverageFor measures a subset of this verified review's observations. Callers
// must use indexes from this review; this does not qualify financial membership.
func (r Review) CoverageFor(indexes []int) Coverage {
	return coverage(r.Window, r.Observations, indexes)
}

func date(s string) (time.Time, bool) {
	s = strings.TrimSuffix(s, "T00:00:00")
	d, err := time.Parse("2006-01-02", s)
	return d, err == nil && d.Format("2006-01-02") == s
}

func period(start, end string) *Period {
	a, okA := date(start)
	b, okB := date(end)
	if !okA || !okB || b.Before(a) {
		return nil
	}
	return &Period{a.Format("2006-01-02"), b.Format("2006-01-02")}
}

func bounds(p Period) (int64, int64) {
	a, _ := date(p.Start)
	b, _ := date(p.End)
	return a.Unix() / 86400, b.Unix()/86400 + 1 // inclusive dates, half-open sweep
}

func intersects(a, b Period) bool { return a.Start <= b.End && b.Start <= a.End }

// Only dates are clipped, never lump-sum report amounts. An event sweep keeps
// nested overlaps exact without emitting a quadratic list of report pairs.
func coverage(window Period, observations []Observation, indexes []int) Coverage {
	lo, hi := bounds(window)
	r := Coverage{Segments: []Segment{}, WindowDays: hi - lo, CrossBoundaryIndexes: []int{}}
	events := map[int64]int{lo: 0, hi: 0}
	for _, i := range indexes {
		p := observations[i].Period
		if p == nil || !intersects(window, *p) {
			continue
		}
		a, b := bounds(*p)
		if a < lo || b > hi {
			r.CrossBoundaryIndexes = append(r.CrossBoundaryIndexes, i)
		}
		events[max(a, lo)]++
		events[min(b, hi)]--
	}
	keys := make([]int64, 0, len(events))
	for d := range events {
		keys = append(keys, d)
	}
	slices.Sort(keys)
	count := 0
	for i := 0; i+1 < len(keys); i++ {
		a, b := keys[i], keys[i+1]
		count += events[a]
		p := Period{time.Unix(a*86400, 0).UTC().Format("2006-01-02"), time.Unix((b-1)*86400, 0).UTC().Format("2006-01-02")}
		r.Segments = append(r.Segments, Segment{p, count})
		if count == 0 {
			r.GapDays += b - a
		} else {
			r.CoveredDays += b - a
		}
		if count > 1 {
			r.OverlapDays += b - a
		}
	}
	return r
}

func comparePeriod(a, b Period) int {
	if c := cmp.Compare(a.Start, b.Start); c != 0 {
		return c
	}
	return cmp.Compare(a.End, b.End)
}
