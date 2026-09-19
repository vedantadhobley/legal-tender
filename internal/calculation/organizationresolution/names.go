package organizationresolution

import (
	"strings"
	"unicode"

	"github.com/vedantadhobley/legal-tender/internal/nameform"
)

// MatchName exposes the existing bounded organization-name proposal rule. A
// match is text correspondence, not registry identity or employment evidence.
func MatchName(query, name string) (NameMatch, bool) { return matchName(query, name) }

// matchName tries exact, boundary, suffix and combined forms in that order.
// It neither removes arbitrary spaces nor treats semantic words as suffixes.
func matchName(query, name string) (NameMatch, bool) {
	q, n := Normalize(query), Normalize(name)
	if q == "" || n == "" {
		return NameMatch{}, false
	}
	match := func(rule, a, b, qs, ns string) NameMatch {
		return NameMatch{Rule: rule,
			Query:     NameForm{Normalized: q, Comparison: a, RemovedLegalSuffix: qs},
			Candidate: NameForm{Normalized: n, Comparison: b, RemovedLegalSuffix: ns}}
	}
	if q == n {
		return match("normalized_exact", q, n, "", ""), true
	}
	qb, nb := nameform.DigitLetterBoundaries(q), nameform.DigitLetterBoundaries(n)
	if qb == nb {
		m := match("digit_letter_boundaries", qb, nb, "", "")
		m.Query.DigitLetterBoundaries, m.Candidate.DigitLetterBoundaries = qb != q, nb != n
		return m, true
	}
	for _, withBoundaries := range []bool{false, true} {
		a, b := q, n
		if withBoundaries {
			a, b = qb, nb
		}
		queryStem, qs, qkind := nameform.SplitLegalSuffix(a)
		nameStem, ns, nkind := nameform.SplitLegalSuffix(b)
		// Never equate two explicitly different legal designator families. Empty
		// or number-only stems cannot acquire a match by suffix removal.
		if (qs == "" && ns == "") || (qs != "" && ns != "" && qkind != nkind) || queryStem != nameStem || !strings.ContainsFunc(queryStem, unicode.IsLetter) {
			continue
		}
		m := match("legal_suffix_variant", queryStem, nameStem, qs, ns)
		if withBoundaries {
			m.Rule = "digit_letter_boundaries_and_legal_suffix_variant"
			m.Query.DigitLetterBoundaries, m.Candidate.DigitLetterBoundaries = a != q, b != n
		}
		return m, true
	}
	return NameMatch{}, false
}
