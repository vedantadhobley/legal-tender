package personaffiliation

import (
	"sort"
	"time"
)

func assessTimeline(raw *string, observations []RoleObservation, infer bool) RoleTimeline {
	r := RoleTimeline{State: "no_source_coverage_at_day", Members: []Reference{}, Support: []Reference{}, Contrary: []Reference{}}
	sort.Slice(observations, func(i, j int) bool { return referenceLess(observations[i].Claim.Source, observations[j].Claim.Source) })
	for _, o := range observations {
		r.Members = append(r.Members, o.Claim.Source)
		if o.Claim.Issue != "" || o.Claim.Role == UnknownRole || (o.Claim.AsOf == "" && (o.Claim.ValidFrom == "" || o.Claim.ValidThrough == "")) {
			r.HasUnassessedEvidence = true
		}
	}
	if raw == nil || *raw == "" {
		r.State = "receipt_date_unknown"
		return r
	}
	day, ok := receiptDay(*raw)
	if !ok {
		r.State = "receipt_date_unusable"
		return r
	}
	before, after := "", ""
	for _, o := range observations {
		c := o.Claim
		if c.Issue != "" || c.Role == UnknownRole {
			continue
		}
		state := roleTime(raw, c)
		if state == "within_reported_period" || state == "on_reported_as_of_date" {
			if o.Polarity == "asserted" {
				r.Support = append(r.Support, c.Source)
			} else {
				r.Contrary = append(r.Contrary, c.Source)
			}
		}
		if o.Polarity == "asserted" && c.AsOf != "" {
			if c.AsOf < day && c.AsOf > before {
				before = c.AsOf
			}
			if c.AsOf > day && (after == "" || c.AsOf < after) {
				after = c.AsOf
			}
		}
	}
	switch {
	case len(r.Support) > 0 && len(r.Contrary) > 0:
		r.State = "conflicting_source_assertions"
	case len(r.Contrary) > 0:
		r.State = "source_denies_at_day"
	case len(r.Support) > 0:
		r.State = "source_supported_at_day"
	default:
		if infer && before != "" && after != "" {
			assessContinuity(&r, observations, before, after)
		}
	}
	return r
}

func assessContinuity(r *RoleTimeline, observations []RoleObservation, before, after string) {
	// The executive category alone cannot equate CEO, CFO and other offices.
	if observations[0].RoleID == "" {
		r.State = "continuity_role_unspecified"
		return
	}
	leftOrigins, rightOrigins := map[Reference]bool{}, map[Reference]bool{}
	refs := []Reference{}
	unknownOrigin, reusedOrigin, unassessed, interrupted := false, false, false, false
	for _, o := range observations {
		c := o.Claim
		if c.Issue != "" || c.Role == UnknownRole {
			unassessed = true // Do not silently discard a possibly relevant constraint.
			continue
		}
		if o.Polarity == "denied" {
			if c.AsOf == "" && c.ValidFrom == "" && c.ValidThrough == "" {
				unassessed = true
			} else if c.AsOf != "" {
				interrupted = interrupted || (c.AsOf >= before && c.AsOf <= after)
			} else {
				// Open bounds conservatively block a bridge they could intersect;
				// they are not promoted to a confirmed interval of non-service.
				interrupted = interrupted || ((c.ValidFrom == "" || c.ValidFrom <= after) && (c.ValidThrough == "" || c.ValidThrough >= before))
			}
			continue
		}
		// A recorded end then possible reappointment blocks interpolation even
		// when an earlier and a later observation agree. Other organizations and
		// roles are already in different groups, not automatically conflicts.
		interrupted = interrupted || (c.ValidThrough >= before && c.ValidThrough < after && c.ValidThrough != "") || (c.ValidFrom > before && c.ValidFrom <= after)
		if c.AsOf != before && c.AsOf != after {
			continue
		}
		refs = append(refs, c.Source)
		if o.Origin == nil {
			unknownOrigin = true
			continue
		}
		if c.AsOf == before {
			leftOrigins[*o.Origin] = true
		} else {
			rightOrigins[*o.Origin] = true
		}
	}
	for origin := range leftOrigins {
		reusedOrigin = reusedOrigin || rightOrigins[origin]
	}
	switch {
	case interrupted:
		r.State = "continuity_blocked_by_source_constraint"
	case unassessed:
		r.State = "continuity_unassessed_evidence"
	case unknownOrigin:
		r.State = "continuity_origin_unknown"
	case reusedOrigin:
		r.State = "continuity_reuses_observation"
	default:
		left, _ := time.Parse(time.DateOnly, before)
		right, _ := time.Parse(time.DateOnly, after)
		r.State = "inferred_continuity"
		r.Continuity = &Continuity{Before: before, After: after, GapDays: (right.Unix() - left.Unix()) / 86400, References: refs}
	}
}
