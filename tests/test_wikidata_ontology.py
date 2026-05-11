"""Tests for the Wikidata ontology classifier.

These verify the `wikidata_ontology.is_org_subclass()` function returns
the right True/False for representative Q-ids. They protect against
the parallel-walk corruption bugs (Q22687=False, Q613142=False) we've
hit multiple times in this session.

Run: pytest tests/test_wikidata_ontology.py

Speed: <5s if cache is warm; first run touches Wikidata REST for cold
Q-ids (a few seconds per cold Q-id). Subsequent runs are cache hits
and finish in milliseconds.
"""

import pytest

from src.rag.wikidata_ontology import (
    ORG_ROOTS,
    NON_EMPLOYER_ROOTS,
    is_org_subclass,
)


# ---------------------------------------------------------------------------
# Critical Q-ids — these have all been wrong at some point this session.
# Every one of these failing means a whole class of FEC employer fails.
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("qid,label", [
    ("Q43229", "organization"),
    ("Q4830453", "business"),
    ("Q22687", "bank"),
    ("Q319845", "investment bank"),
    ("Q1331793", "media company / financial institution"),
    ("Q740752", "limited liability company"),  # subclass — was failing pre-type-filter-removal
    ("Q167037", "corporation"),
    ("Q891723", "public company"),
    ("Q3918", "university"),
    ("Q38723", "higher education institution"),
    ("Q16917", "hospital"),
    ("Q4287745", "medical organization"),
    ("Q15911314", "association"),
    ("Q163740", "nonprofit organization"),
    ("Q178790", "trade union"),
    ("Q613142", "law firm"),  # was wrongly False after parallel-walk corruption
    ("Q2089936", "consulting company"),  # was Q1361353 (test typo; Q1361353 has empty P279)
    ("Q122229703", "management consulting company"),  # BCG-class regression case
    ("Q249556", "railway company"),  # Pan Am Railways case
    ("Q46970", "airline"),
    ("Q11691", "stock exchange"),
    ("Q22687", "bank"),  # listed again for emphasis — most critical Q-id
])
def test_org_subclass_is_True(qid, label):
    """Every Wikidata Q-id below MUST classify as an employer-shaped
    organization. If any of these regresses to False or None, an entire
    category of FEC employers (banks, law firms, universities, etc.)
    silently fails to resolve."""
    result = is_org_subclass(qid)
    assert result is True, (
        f"Q-id {qid} ({label!r}) should classify as ORG (True), "
        f"got {result}. This is a wikidata_ontology regression that "
        f"would silently reject every employer in this category."
    )


@pytest.mark.parametrize("qid,label", [
    # People
    ("Q5", "human"),
    # Geography / territory — must reject Steyr-class cases
    ("Q6256", "country"),
    ("Q3624078", "sovereign state"),
    ("Q15642541", "administrative territorial entity"),
    ("Q486972", "human settlement"),
    ("Q116457956", "German municipality without town privileges"),
    ("Q262166", "municipality in Germany"),
    ("Q532", "village"),
    # Creative works — must reject RDV-class cases
    ("Q7889", "video game"),
    ("Q11424", "film"),
    ("Q571", "book"),
    ("Q482994", "album"),
    ("Q7366", "song"),
    # Family / social — must reject Goldman-Sachs-family case
    ("Q8436", "family"),
    ("Q721790", "extended family"),
    # Sport teams — must reject Kolkata-Knight-Riders case
    ("Q12973014", "cricket team"),
    # Biology — must reject Rice-dwarf-virus case
    ("Q11173", "chemical compound"),
    ("Q7187", "gene"),
    # Names
    ("Q11879003", "given name"),
    ("Q101352", "family name"),
])
def test_org_subclass_is_False(qid, label):
    """Every Q-id below MUST classify as NOT employer-shaped. If any
    regresses to True, the resolver will accept candidates of that
    class as employers (Steyr-the-city, Rice-dwarf-virus, Goldman-Sachs-
    family-the-family-not-the-bank, etc.)."""
    result = is_org_subclass(qid)
    assert result is False, (
        f"Q-id {qid} ({label!r}) should classify as NON-EMPLOYER (False), "
        f"got {result}. Accepting it would let the resolver pick "
        f"{label!r}-class matches as corporate employers."
    )


def test_root_sets_consistent():
    """ORG_ROOTS and NON_EMPLOYER_ROOTS should not overlap."""
    overlap = ORG_ROOTS & NON_EMPLOYER_ROOTS
    assert not overlap, (
        f"ORG_ROOTS and NON_EMPLOYER_ROOTS overlap: {overlap}. "
        f"A Q-id in both sets makes classification non-deterministic."
    )


def test_org_roots_classify_True():
    """Every ORG_ROOT must self-classify as True."""
    for qid in ORG_ROOTS:
        assert is_org_subclass(qid) is True, f"{qid} in ORG_ROOTS but classified non-True"


def test_non_employer_roots_classify_False():
    """Every NON_EMPLOYER_ROOT must self-classify as False."""
    for qid in NON_EMPLOYER_ROOTS:
        assert is_org_subclass(qid) is False, f"{qid} in NON_EMPLOYER_ROOTS but classified non-False"
