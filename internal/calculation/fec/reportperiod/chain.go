package reportperiod

import (
	"encoding/json"
	"regexp"
	"slices"
	"strings"
)

var chainID = regexp.MustCompile(`^[1-9][0-9]*(\.0+)?$`)

func chain(raw json.RawMessage) ([]string, bool) {
	var values []json.RawMessage
	if json.Unmarshal(raw, &values) != nil || len(values) == 0 {
		return nil, false
	}
	ids := make([]string, 0, len(values))
	seen := map[string]bool{}
	for _, value := range values {
		s := string(value)
		if len(value) > 0 && value[0] == '"' {
			s = text(value)
		}
		if !chainID.MatchString(s) {
			return nil, false
		}
		id, _, _ := strings.Cut(s, ".")
		if seen[id] {
			return nil, false
		}
		seen[id] = true
		ids = append(ids, id)
	}
	return ids, true
}

func qualifyChain(c *Cohort, observations []Observation, raws []map[string]json.RawMessage) {
	chains := map[int][]string{}
	byFile := map[string]int{}
	for _, i := range c.ObservationIndexes {
		o := observations[i]
		byFile[o.FileNumber] = i
		switch o.PublisherState {
		case "not_amended":
			c.PublisherMemberIndexes = append(c.PublisherMemberIndexes, i)
		case "unknown":
			c.Blockers = append(c.Blockers, "unknown_amended_status")
		}
		// Paper amendments can be partial. Do not interpret missing financial
		// fields as replacements or use a nearby electronic version as a fallback.
		if text(raws[i]["means_filed"]) != "e-file" {
			c.Blockers = append(c.Blockers, "non_electronic_replacement_unqualified")
		}
		ids, valid := chain(raws[i]["amendment_chain"])
		if !valid || ids[len(ids)-1] != o.FileNumber {
			c.Blockers = append(c.Blockers, "invalid_or_missing_chain")
			continue
		}
		chains[i] = ids
	}
	if len(c.PublisherMemberIndexes) != 1 {
		c.Blockers = append(c.Blockers, "require_one_publisher_member")
	}
	if len(c.PublisherMemberIndexes) == 1 {
		selected := c.PublisherMemberIndexes[0]
		ids := chains[selected]
		if len(ids) != len(c.ObservationIndexes) {
			c.Blockers = append(c.Blockers, "chain_membership_not_closed")
		}
		for pos, id := range ids {
			i, found := byFile[id]
			if !found {
				c.Blockers = append(c.Blockers, "chain_reference_outside_cohort")
				continue
			}
			if !slices.Equal(chains[i], ids[:pos+1]) {
				c.Blockers = append(c.Blockers, "inconsistent_chain_prefix")
			}
		}
		if len(c.Blockers) == 0 {
			c.ChainCandidateIndex = &selected
		}
	}
	slices.Sort(c.Blockers)
	c.Blockers = slices.Compact(c.Blockers)
}
