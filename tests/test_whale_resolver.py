"""End-to-end tests for the whale (person → company) resolver.

Live network calls. Set NO_NETWORK=1 to skip.
"""

import os
import pytest

from src.rag.whale_resolver import resolve_people_batch

NO_NETWORK = os.environ.get("NO_NETWORK") == "1"
pytestmark = pytest.mark.skipif(NO_NETWORK, reason="live network disabled via NO_NETWORK=1")


# Known whales whose Wikidata entries store corporate links directly
# on the person side (P108 employer, P1830 owner of, or P39 position
# held with P642 'of' qualifier). Each tuple: (name, expected_company_substring).
RESOLVABLE_WHALES = [
    ("ELON MUSK",         "Tesla"),         # P1830 → Tesla
    ("SHELDON ADELSON",   "Las Vegas Sands"),
    ("KENNETH GRIFFIN",   "Citadel"),
    ("TIMOTHY MELLON",    "Pan Am"),
    ("CHARLES KOCH",      "Koch"),
]

# Whales that resolve but the Wikidata page lacks corporate relationships
# or reconci picks the wrong person. Documented coverage gap — pending
# OpenCorporates Layer 3 or richer disambiguation.
KNOWN_NOT_FOUNDS = [
    # "JOHN ARNOLD"  — reconci picks historical figure; real hedge-fund
    #                  founder is at a less-notable Q-id.
    # "PAUL SINGER"  — same disambiguation issue.
]


@pytest.fixture(scope="module")
def batch():
    return resolve_people_batch([w[0] for w in RESOLVABLE_WHALES])


@pytest.mark.parametrize("name,company_substr", RESOLVABLE_WHALES)
def test_whale_resolves(batch, name, company_substr):
    r = batch.get(name)
    assert r is not None, f"{name!r} missing from batch"
    assert r["source"] == "wikidata", (
        f"{name!r} did not resolve: source={r['source']}, "
        f"method={r.get('method')}, alts={r.get('alternatives', [])[:2]}"
    )
    assert r["companies"], (
        f"{name!r} resolved as person but no companies extracted. "
        f"person_qid={r.get('person_qid')}"
    )
    # At least one company contains the expected substring.
    company_names = [c["name"] for c in r["companies"]]
    matched = any(company_substr.lower() in cn.lower() for cn in company_names)
    assert matched, (
        f"{name!r}: expected a company containing {company_substr!r}; "
        f"got {company_names!r}"
    )


def test_output_shape_matches_legacy():
    """The asset consumes `source`, `companies`, and each company's
    `name` / `wikidata_id` / `relationship`. Verify those keys exist."""
    r = resolve_people_batch(["ELON MUSK"])["ELON MUSK"]
    assert "source" in r
    assert "companies" in r
    assert "primary_company" in r
    assert r["source"] == "wikidata"
    c0 = r["companies"][0]
    assert "name" in c0
    assert "wikidata_id" in c0
    assert "relationship" in c0


def test_empty_input():
    assert resolve_people_batch([]) == {}


def test_short_input_no_corroboration_rejected():
    """Short inputs without a name-match signal should be rejected
    (same rule as employer resolver)."""
    # "XQ" — too short, no real Wikidata person matches it well.
    r = resolve_people_batch(["XQ"])["XQ"]
    # Either no candidates, or rejected via short_input rule.
    assert r["source"] == "not_found"
