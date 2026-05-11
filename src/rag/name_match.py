"""Name-matching signals for low-confidence reconci.link candidates.

Reconci.link returns candidates with a single relevance score (0-100).
We trust the score at the high end (≥70) and reject below 40. The
middle band (40-69) is the productive question: is this a real match or
noise?

Reconci's score is generic. We have more specific knowledge about how
FEC employer strings vary from canonical company names:

  - **Acronyms** ("NEA" → "National Education Association")
  - **Contractions / portmanteaus** ("WILMERHALE" → "Wilmer Cutler
    Pickering Hale and Dorr" — concatenation of a subset of tokens)
  - **Typos** ("UIINE" → "Uline" — single-character edit)
  - **Suffix variants** ("BAUPOST GROUP" → "Baupost Inc." — shared
    distinctive token, different generic suffix)

Each of these is a *general* property of name variation, not a list of
specific cases. The functions below compute each signal separately so
they can be combined with explicit weights / thresholds in
`wikidata_resolver`.

All functions are pure, deterministic, and microsecond-fast. No
network calls, no LLM, no caching needed.
"""

from __future__ import annotations

import re
from typing import Iterable

# Tokens that don't carry distinguishing information when comparing
# company names. The acronym signal in particular needs to skip these —
# "Wilmer Cutler Pickering Hale and Dorr" has initials W-C-P-H-and-D
# but "and" shouldn't count toward the acronym.
_STOPWORDS = frozenset({
    "and", "&", "the", "of", "for", "in", "at",
    "co", "company", "inc", "llc", "lp", "llp", "ltd", "limited",
    "corp", "corporation", "plc", "gmbh", "ag", "sa", "nv", "bv",
    "group", "holdings", "holding", "partners", "associates",
    "enterprises", "industries", "international", "intl",
})

_NON_ALPHANUM_RE = re.compile(r"[^a-z0-9]+")


def _tokens(s: str, drop_stopwords: bool = True) -> list[str]:
    """Tokenize: lowercase, split on non-alphanum, optionally drop stopwords."""
    raw = _NON_ALPHANUM_RE.split(s.lower())
    toks = [t for t in raw if t]
    if drop_stopwords:
        toks = [t for t in toks if t not in _STOPWORDS]
    return toks


def _collapsed(s: str) -> str:
    """Lowercase string with all non-alphanum removed.
    Used for substring/typo comparisons that ignore punctuation/spacing."""
    return _NON_ALPHANUM_RE.sub("", s.lower())


def acronym_match(input_str: str, candidate_label: str) -> bool:
    """True if `input_str` equals the initial-letter acronym of
    `candidate_label`'s tokens (with or without stopwords).

    Try both forms because canonical acronyms sometimes incorporate words
    we treat as stopwords for other signals:

        "NEA" matches "National Education Association" (N-E-A, no stopwords involved)
        "BCG" matches "Boston Consulting Group" (B-C-G — "Group" is a stopword
            in other contexts but part of the acronym here)
        "AIG" matches "American International Group" (A-I-G — both "International"
            and "Group" are stopwords but the acronym keeps them)
        "WH" matches "Wilmer and Hale" (W-H — "and" stopword dropped)
        "WILMERHALE" does NOT match (it's a portmanteau, not an acronym;
            token_subset_concat_match handles that case)
    """
    inp = _collapsed(input_str)
    if not inp or len(inp) > 6:
        # Acronyms are ≤6 letters in practice; longer "all-caps" inputs
        # are usually contractions, not acronyms.
        return False
    # Try both with and without stopwords. Without-stopwords catches
    # "WH" for "Wilmer and Hale"; with-stopwords catches "BCG" for
    # "Boston Consulting Group" where "Group" is part of the acronym.
    for drop in (True, False):
        cand_tokens = _tokens(candidate_label, drop_stopwords=drop)
        if not cand_tokens:
            continue
        acronym = "".join(t[0] for t in cand_tokens)
        if acronym == inp:
            return True
    return False


def token_subset_concat_match(input_str: str, candidate_label: str) -> bool:
    """True if `input_str` (lowercased, collapsed) equals the
    concatenation of some IN-ORDER subset of `candidate_label`'s
    non-stopword tokens.

    Catches portmanteau-style FEC contractions:
        "WILMERHALE" matches "Wilmer Cutler Pickering Hale and Dorr"
            (Wilmer + Hale, an in-order subset)
        "DEUTSCHEBANK" matches "Deutsche Bank AG"

    The "in-order" constraint avoids false positives like
    "BANKAMERICA" → "American Bank" (out-of-order). Out-of-order
    permutations would over-match.
    """
    inp = _collapsed(input_str)
    if not inp:
        return False
    cand_tokens = _tokens(candidate_label, drop_stopwords=True)
    if not cand_tokens:
        return False
    # Bitmask iteration over subsets of cand_tokens. n ≤ ~15 in
    # practice; 2^15 = 32K which is fine for ms-scale matching.
    n = len(cand_tokens)
    if n > 12:
        # Long candidate names — restrict to contiguous subsets to keep
        # the search bounded.
        for start in range(n):
            concat = ""
            for end in range(start, n):
                concat += cand_tokens[end]
                if concat == inp:
                    return True
                if len(concat) > len(inp):
                    break
        return False
    # Short candidate — try all in-order subsets.
    for mask in range(1, 1 << n):
        concat = "".join(cand_tokens[i] for i in range(n) if mask & (1 << i))
        if concat == inp:
            return True
    return False


def edit_distance(a: str, b: str) -> int:
    """Standard Levenshtein distance (insertions, deletions, substitutions
    each cost 1). Compares collapsed lowercase versions of the inputs."""
    a, b = _collapsed(a), _collapsed(b)
    if a == b:
        return 0
    if not a:
        return len(b)
    if not b:
        return len(a)
    # Two-row DP to keep memory O(min(len)).
    if len(a) > len(b):
        a, b = b, a
    prev = list(range(len(a) + 1))
    for j, cb in enumerate(b, 1):
        curr = [j]
        for i, ca in enumerate(a, 1):
            cost = 0 if ca == cb else 1
            curr.append(min(
                curr[-1] + 1,       # insert
                prev[i] + 1,        # delete
                prev[i - 1] + cost, # substitute
            ))
        prev = curr
    return prev[-1]


def edit_distance_close(input_str: str, candidate_label: str, max_distance: int = 2) -> bool:
    """True if input and candidate are within `max_distance` Levenshtein
    edits of each other (after collapsing). Captures typos and small
    spelling variants.

    Default threshold 2 catches:
        "UIINE" → "Uline" (1 edit)
        "MICROSFT" → "Microsoft" (1 edit)
        "DOWN JONES" → "Dow Jones" (1 edit)
    """
    a, b = _collapsed(input_str), _collapsed(candidate_label)
    if abs(len(a) - len(b)) > max_distance:
        # Cheap pre-filter — strings differing in length by more than
        # max_distance can't possibly be within max_distance edits.
        return False
    return edit_distance(a, b) <= max_distance


def token_containment_score(input_str: str, candidate_label: str) -> float:
    """Bidirectional token-overlap score in [0, 1].

    Counts how many INPUT tokens appear as substrings of any CANDIDATE
    token, OR how many CANDIDATE tokens appear as substrings of any
    INPUT token. Returns the higher of the two coverage ratios.

    Catches:
        "BAUPOST GROUP" vs "Baupost Inc." — "baupost" matches → 0.5
        "GREYLOCK" vs "Greylock Partners" — "greylock" matches → 1.0
        "WILMERHALE" vs "Wilmer Cutler..." — "wilmer" is substring of
            "wilmerhale" + "hale" is substring of "wilmerhale" → 2/1 in
            one direction, 0/N in the other; we return the max ratio
            (capped at 1.0).

    Stopword-aware: doesn't count generic suffix tokens (GROUP / INC /
    LLC) toward matches, so "BAUPOST GROUP" vs "Baupost Inc." doesn't
    spuriously match on the trivial "X GROUP" / "Y INC" suffixes.
    """
    inp_toks = _tokens(input_str, drop_stopwords=True)
    cand_toks = _tokens(candidate_label, drop_stopwords=True)
    if not inp_toks or not cand_toks:
        return 0.0

    def coverage(needles: list[str], haystacks: list[str]) -> float:
        if not needles:
            return 0.0
        hits = sum(
            1 for n in needles
            if any(n in h or h in n for h in haystacks if h)
        )
        return hits / len(needles)

    return max(
        coverage(inp_toks, cand_toks),
        coverage(cand_toks, inp_toks),
    )


def any_corroborating_signal(
    input_str: str,
    candidate_label: str,
    *,
    edit_max: int = 2,
    token_min: float = 0.5,
) -> bool:
    """OR of the per-signal checks. Used to corroborate a low-confidence
    reconci match — if any signal fires, we trust the match even though
    its raw reconci score is in the 40-69 fuzzy band."""
    if acronym_match(input_str, candidate_label):
        return True
    if token_subset_concat_match(input_str, candidate_label):
        return True
    if edit_distance_close(input_str, candidate_label, max_distance=edit_max):
        return True
    if token_containment_score(input_str, candidate_label) >= token_min:
        return True
    return False


def is_short_input(input_str: str) -> bool:
    """Heuristic: inputs ≤ 4 characters (alphanumeric, collapsed) are
    likely to fuzz-match unrelated entities at high reconci scores.
    "RDV" matches "North Vietnam" at score 100. Demand corroboration
    even at high reconci scores for short inputs."""
    return len(_collapsed(input_str)) <= 4
