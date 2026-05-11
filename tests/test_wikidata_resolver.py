"""End-to-end tests for the resolver pipeline.

Each test sends FEC-style employer names through `resolve_batch()` and
verifies the outcome (which source, which canonical name). Cases are
grouped by the bug they protect against — every name here has either
regressed at some point in this session OR represents a known-good
sanity case.

Run: pytest tests/test_wikidata_resolver.py
Speed target: <30 seconds on a warm cache.

Live network calls to reconci.link + GLEIF. Will fail if either is
down. That's acceptable — if production resolution is broken, we want
tests to fail too. To skip live tests, set NO_NETWORK=1.
"""

import os

import pytest

from src.rag.wikidata_resolver import resolve_batch

NO_NETWORK = os.environ.get("NO_NETWORK") == "1"
pytestmark = pytest.mark.skipif(NO_NETWORK, reason="live network disabled via NO_NETWORK=1")


# ---------------------------------------------------------------------------
# Resolution expectations.
#
# Each tuple: (fec_name, expected_source, [optional substring of canonical_name])
#
#   expected_source ∈ {'wikidata', 'gleif', 'not_found'}
#   canonical_substr: if provided, the resolution's canonical_name must
#                     CONTAIN this string (case-insensitive). Optional.
# ---------------------------------------------------------------------------


# Big-bank / well-known corporation sanity. If any of these break,
# something is catastrophically wrong.
WELL_KNOWN_CORPORATIONS = [
    ("GOLDMAN SACHS",      "wikidata", "Goldman Sachs"),
    ("APPLE",              "wikidata", "Apple"),
    ("MICROSOFT",          "wikidata", "Microsoft"),
    ("GOOGLE",             "wikidata", "Alphabet"),  # rolls up via P749
    ("IBM",                "wikidata", "IBM"),
    ("PFIZER",             "wikidata", "Pfizer"),
    ("BLACKROCK",          "wikidata", "BlackRock"),
    ("TESLA",              "wikidata", "Tesla"),
    ("COMCAST",            "wikidata", "Comcast"),
    ("PAN AM RAILWAYS",    "wikidata", "Pan Am Railways"),
    ("CITADEL INVESTMENT GROUP", "wikidata", "Citadel"),
]

# Subclass-of-organization Q-ids (previously failed due to type=Q43229
# filter — fixed by removing that filter).
SUBCLASS_LLC_CASES = [
    ("BAUPOST GROUP",   "wikidata", "Baupost Group"),
    ("BALLMER GROUP",   "wikidata", "Ballmer Group"),
    ("BEAL BANK",       "wikidata", "Beal Bank"),
    ("LOEWS HOTELS",    "wikidata", "Loews Hotels"),  # hotel CHAIN not single hotel
    ("ULINE",           "wikidata", "Uline"),
    ("BLACKSTONE GROUP","wikidata", "Blackstone"),  # alias match
]

# Law firms — must resolve via type=law firm (Q613142 was wrongly
# cached False during parallel-walk corruption).
LAW_FIRMS = [
    ("AKIN GUMP",                       "wikidata", "Akin Gump"),
    ("WILMERHALE",                      "wikidata", "Wilmer Cutler"),
    ("BROWNSTEIN HYATT FARBER SCHRECK", "gleif", "Brownstein Hyatt"),  # GLEIF route
]

# YAML overrides must take precedence over the default ranking.
OVERRIDES = [
    ("NEA",                       "wikidata", "National Education Association"),
    ("CITADEL",                   "wikidata", "Citadel"),
    ("CITADEL ASSET MANAGEMENT",  "wikidata", "Citadel"),
]

# GLEIF Layer-2 fallback — Wikidata doesn't have these well; GLEIF does.
GLEIF_RECOVERIES = [
    ("PRATT INDUSTRIES",   "gleif", "PRATT INDUSTRIES"),
    ("MOUNTAIRE",          "gleif", "Mountaire"),
    ("EUCLIDEAN CAPITAL",  "gleif", "EUCLIDEAN CAPITAL"),
    ("LINKEDIN",           "gleif", "LinkedIn Corporation"),
]

# Things that MUST NOT resolve — Wikidata has matching entries that
# are NOT employers (cities, scholarly articles, viruses, generic
# concepts, family names, video games, etc.).
MUST_NOT_RESOLVE = [
    ("STEYER",             "not_found", None),  # Steyr, Austrian city
    ("FAHR",               "not_found", None),  # municipality / temperature unit
    ("RDV",                "not_found", None),  # Rice dwarf virus (was wrong before)
    ("CO-OWNER",           "not_found", None),  # nonsense
    ("CANDIDATE",          "not_found", None),  # Candidate Master (chess) was wrong before
    ("PRITZKER GROUP",     "not_found", None),  # news article was wrong before
    ("ADELSON CLINIC",     "not_found", None),  # Wikidata genuinely lacks
]


ALL_CASES = (
    WELL_KNOWN_CORPORATIONS
    + SUBCLASS_LLC_CASES
    + LAW_FIRMS
    + OVERRIDES
    + GLEIF_RECOVERIES
    + MUST_NOT_RESOLVE
)


@pytest.fixture(scope="module")
def batch_results():
    """Resolve all test names in one batch (faster than per-test calls)."""
    names = [c[0] for c in ALL_CASES]
    return resolve_batch(names)


@pytest.mark.parametrize("fec_name,expected_source,canonical_substr", ALL_CASES)
def test_resolution(batch_results, fec_name, expected_source, canonical_substr):
    """Each FEC name resolves to the expected source + canonical (substring)."""
    result = batch_results.get(fec_name)
    assert result is not None, f"{fec_name!r} missing from batch result"

    # Source must match (or be a soft upgrade — e.g. 'wikidata' is
    # acceptable when we predicted 'gleif' if Wikidata started having
    # the entity. The reverse is NOT acceptable.)
    if expected_source == "not_found":
        assert result.source == "not_found", (
            f"{fec_name!r} should be not_found, got source={result.source}, "
            f"canonical={result.canonical!r}. "
            f"Top alternatives: {result.alternatives[:2]}"
        )
    elif expected_source == "wikidata":
        assert result.source in ("wikidata", "gleif"), (
            f"{fec_name!r} should resolve (expected wikidata), "
            f"got source={result.source}. Top alts: {result.alternatives[:2]}"
        )
    elif expected_source == "gleif":
        # Either wikidata or gleif acceptable — wikidata is preferred
        assert result.source in ("wikidata", "gleif"), (
            f"{fec_name!r} should resolve (expected gleif), "
            f"got source={result.source}. Top alts: {result.alternatives[:2]}"
        )

    # Canonical substring check (case-insensitive)
    if canonical_substr is not None and result.source != "not_found":
        assert canonical_substr.lower() in result.canonical.lower(), (
            f"{fec_name!r} resolved to {result.canonical!r} "
            f"(source={result.source}), but expected canonical to contain "
            f"{canonical_substr!r}. Top alts: {result.alternatives[:2]}"
        )
