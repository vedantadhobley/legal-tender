package committeesummary

import (
	"crypto/sha256"
	"math/big"
)

type EquationProfile struct {
	Equal     uint64 `json:"equal"`
	Different uint64 `json:"different"`
	Missing   uint64 `json:"missing"`
	Invalid   uint64 `json:"invalid"`
}

func (p *EquationProfile) add(row *Record, names []string, cash bool) {
	missing, invalid := false, false
	for _, name := range names {
		missing = missing || row.Money[name].State == Blank
		invalid = invalid || row.Money[name].State == Invalid
	}
	if invalid {
		p.Invalid++
		return
	}
	if missing {
		p.Missing++
		return
	}
	values := make([]*big.Int, len(names))
	for i, name := range names {
		values[i], _ = new(big.Int).SetString(*row.Money[name].MinorUnits, 10)
	}
	left := new(big.Int).Add(values[0], values[1])
	if cash {
		left.Sub(left, values[2])
	}
	if left.Cmp(values[len(values)-1]) == 0 {
		p.Equal++
	} else {
		p.Different++
	}
}

type Multiplicity struct {
	CommitteeIDs                  uint64 `json:"committee_ids"`
	UnindexedCommitteeRows        uint64 `json:"unindexed_committee_rows"`
	RepeatedCommitteeIDs          uint64 `json:"repeated_committee_ids"`
	RepeatedCommitteeOccurrences  uint64 `json:"repeated_committee_occurrences"`
	RepeatedCommitteeExtraRows    uint64 `json:"repeated_committee_extra_rows"`
	EqualNonCandidateGroups       uint64 `json:"equal_non_candidate_groups"`
	ConflictingNonCandidateGroups uint64 `json:"conflicting_non_candidate_groups"`
	CompositeKeys                 uint64 `json:"composite_keys"`
	UnindexedCompositeRows        uint64 `json:"unindexed_composite_rows"`
	DuplicateCompositeExtraRows   uint64 `json:"duplicate_composite_extra_rows"`
	ConflictingCompositeKeys      uint64 `json:"conflicting_composite_keys"`
	ExactDuplicateExtraRows       uint64 `json:"exact_duplicate_extra_rows"`
}

type identityGroup struct {
	rows     uint64
	digest   [32]byte
	conflict bool
}
type compositeKey struct{ committee, candidate, cycle string }
type identityIndex struct {
	committees                             map[string]*identityGroup
	composites                             map[compositeKey]*identityGroup
	raw                                    map[string]uint64
	unindexedCommittee, unindexedComposite uint64
}

func newIdentityIndex() *identityIndex {
	return &identityIndex{committees: map[string]*identityGroup{}, composites: map[compositeKey]*identityGroup{}, raw: map[string]uint64{}}
}

func (idx *identityIndex) add(row *Record) {
	idx.raw[row.RawSHA256]++
	if row.Identifiers["CMTE_ID"].State != Valid {
		idx.unindexedCommittee++
		idx.unindexedComposite++
		return
	}
	full, nonCandidate := sha256.New(), sha256.New()
	for _, c := range columns {
		hashString(full, row.SourceFields[c.name])
		if c.name != "CAND_ID" {
			hashString(nonCandidate, row.SourceFields[c.name])
		}
	}
	addGroup(idx.committees, row.SourceFields["CMTE_ID"], [32]byte(nonCandidate.Sum(nil)))
	if row.Identifiers["FEC_ELECTION_YR"].State != Valid || row.Identifiers["CAND_ID"].State == Invalid {
		idx.unindexedComposite++
		return
	}
	key := compositeKey{row.SourceFields["CMTE_ID"], row.SourceFields["CAND_ID"], row.SourceFields["FEC_ELECTION_YR"]}
	addGroup(idx.composites, key, [32]byte(full.Sum(nil)))
}

func addGroup[K comparable](groups map[K]*identityGroup, key K, digest [32]byte) {
	group := groups[key]
	if group == nil {
		groups[key] = &identityGroup{rows: 1, digest: digest}
		return
	}
	group.rows++
	group.conflict = group.conflict || group.digest != digest
}

func (idx *identityIndex) profile() Multiplicity {
	p := Multiplicity{CommitteeIDs: uint64(len(idx.committees)), CompositeKeys: uint64(len(idx.composites)), UnindexedCommitteeRows: idx.unindexedCommittee, UnindexedCompositeRows: idx.unindexedComposite}
	for _, group := range idx.committees {
		if group.rows == 1 {
			continue
		}
		p.RepeatedCommitteeIDs++
		p.RepeatedCommitteeOccurrences += group.rows
		p.RepeatedCommitteeExtraRows += group.rows - 1
		if group.conflict {
			p.ConflictingNonCandidateGroups++
		} else {
			p.EqualNonCandidateGroups++
		}
	}
	for _, group := range idx.composites {
		p.DuplicateCompositeExtraRows += group.rows - 1
		if group.conflict {
			p.ConflictingCompositeKeys++
		}
	}
	for _, rows := range idx.raw {
		p.ExactDuplicateExtraRows += rows - 1
	}
	return p
}
