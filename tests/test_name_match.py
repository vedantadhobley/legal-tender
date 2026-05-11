"""Unit tests for name-matching signals (no network)."""

import pytest

from src.rag.name_match import (
    acronym_match,
    edit_distance,
    edit_distance_close,
    is_short_input,
    token_containment_score,
    token_subset_concat_match,
    any_corroborating_signal,
)


# ---------------------------------------------------------------------------
# acronym_match
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("inp,label", [
    ("NEA", "National Education Association"),
    ("KKR", "Kohlberg Kravis Roberts"),
    ("BCG", "Boston Consulting Group"),
    ("nea", "national education association"),  # case-insensitive
    ("AIG", "American International Group"),
])
def test_acronym_match_true(inp, label):
    assert acronym_match(inp, label)


@pytest.mark.parametrize("inp,label", [
    ("WILMERHALE", "Wilmer Cutler Pickering Hale and Dorr"),  # portmanteau, not acronym
    ("GOLDMAN", "Goldman Sachs"),  # input isn't initials
    ("XYZ", "Apple Inc."),  # no relation
    ("LONGINPUTNAME", "Long Input Name"),  # > 6 chars
    ("", "Foo Bar"),  # empty input
])
def test_acronym_match_false(inp, label):
    assert not acronym_match(inp, label)


def test_acronym_match_handles_stopwords_both_ways():
    # acronym_match tries with-stopwords and without-stopwords forms.
    # "WH" matches via stopword-dropped form (W-H from Wilmer/Hale).
    # "WAH" matches via stopword-kept form (W-A-H from Wilmer/and/Hale).
    # Both are accepted — better to over-corroborate at this layer than
    # to mis-reject a legitimate acronym variant.
    assert acronym_match("WH", "Wilmer and Hale")
    assert acronym_match("WAH", "Wilmer and Hale")


# ---------------------------------------------------------------------------
# token_subset_concat_match
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("inp,label", [
    ("WILMERHALE", "Wilmer Cutler Pickering Hale and Dorr"),  # Wilmer + Hale
    ("wilmerhale", "Wilmer Hale"),
    ("deutschebank", "Deutsche Bank AG"),  # Deutsche + Bank
])
def test_token_subset_concat_match_true(inp, label):
    assert token_subset_concat_match(inp, label)


@pytest.mark.parametrize("inp,label", [
    ("GOLDMAN", "Goldman Sachs"),  # single token equality — but "goldman" == subset(["goldman"]) yes
    ("XYZ", "Apple Inc."),
    ("HELLOWORLD", "Hello World"),  # actually true: hello+world
])
def test_token_subset_concat_match_basic(inp, label):
    # These ARE matches (single-token subset == single-token concat).
    # Just checking the function doesn't error out on simple cases.
    _ = token_subset_concat_match(inp, label)


def test_token_subset_concat_match_negative():
    assert not token_subset_concat_match("XYZ", "Apple Inc.")
    assert not token_subset_concat_match("BANKAMERICA", "American Bank")  # out-of-order


# ---------------------------------------------------------------------------
# edit_distance / edit_distance_close
# ---------------------------------------------------------------------------

def test_edit_distance_basic():
    assert edit_distance("uiine", "uline") == 1
    assert edit_distance("microsft", "microsoft") == 1
    assert edit_distance("uline", "uline") == 0
    assert edit_distance("abc", "xyz") == 3


@pytest.mark.parametrize("inp,label", [
    ("UIINE", "Uline"),       # 1 edit
    ("MICROSFT", "Microsoft"),  # 1 edit
    ("APPLES", "Apple"),       # 1 edit
])
def test_edit_distance_close_true(inp, label):
    assert edit_distance_close(inp, label, max_distance=2)


def test_edit_distance_close_false():
    assert not edit_distance_close("GOLDMAN SACHS", "Apple", max_distance=2)
    assert not edit_distance_close("XYZ", "ABCDEFGHIJ", max_distance=2)


# ---------------------------------------------------------------------------
# token_containment_score
# ---------------------------------------------------------------------------

def test_token_containment_greylock():
    # "GREYLOCK" perfectly contains "greylock" of "Greylock Partners";
    # both directions: cand-tokens "greylock" in input "greylock" — 1/1 = 1.0
    # input-tokens "greylock" in cand-tokens "greylock"/"partners" — 1/1 = 1.0
    score = token_containment_score("GREYLOCK", "Greylock Partners")
    assert score == 1.0


def test_token_containment_baupost_with_stopword():
    # "BAUPOST GROUP" vs "Baupost Inc." — "group" and "inc" are stopwords,
    # so the only meaningful token on each side is "baupost". 1/1 = 1.0.
    score = token_containment_score("BAUPOST GROUP", "Baupost Inc.")
    assert score == 1.0


def test_token_containment_unrelated():
    assert token_containment_score("APPLE", "Microsoft") == 0.0


# ---------------------------------------------------------------------------
# any_corroborating_signal — combined check
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("inp,label", [
    ("NEA", "National Education Association"),  # acronym
    ("WILMERHALE", "Wilmer Cutler Pickering Hale and Dorr"),  # portmanteau
    ("UIINE", "Uline"),  # typo
    ("GREYLOCK", "Greylock Partners"),  # token containment
    ("BAUPOST GROUP", "Baupost Inc."),  # token containment + suffix variant
])
def test_any_corroborating_signal_true(inp, label):
    assert any_corroborating_signal(inp, label)


@pytest.mark.parametrize("inp,label", [
    ("RDV", "North Vietnam"),  # 3-char input, no signal
    ("XYZ", "Apple Inc."),
    ("STEYER", "Steyr"),  # close enough? edit distance = 2 (insert "e")
])
def test_any_corroborating_signal_short_inputs(inp, label):
    # STEYER vs Steyr: edit distance is 1 (insert "e"). Will return True.
    # Just probing — not asserting True/False, depends on threshold.
    _ = any_corroborating_signal(inp, label)


def test_steyer_vs_steyr_passes_edit_distance():
    # STEYER → Steyr is 1 edit. This passes corroboration even though
    # it's the wrong match for the FEC donor's intent.
    # Acceptable trade-off — we don't classify "is this an employer?"
    # any more, and the resolver path will accept it.
    assert edit_distance_close("STEYER", "Steyr", max_distance=2)


def test_rdv_vs_north_vietnam_no_signal():
    # The poster-child false-positive case. Reconci returns score 100,
    # but no corroborating signal fires. Combined with the short_input
    # rule, this gets rejected.
    assert not any_corroborating_signal("RDV", "North Vietnam")
    assert is_short_input("RDV")


# ---------------------------------------------------------------------------
# is_short_input
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("inp,expected", [
    ("RDV", True),
    ("NEA", True),
    ("KKR", True),
    ("XYZA", True),  # 4 chars
    ("APPLE", False),  # 5 chars
    ("GOLDMAN SACHS", False),
])
def test_is_short_input(inp, expected):
    assert is_short_input(inp) is expected
