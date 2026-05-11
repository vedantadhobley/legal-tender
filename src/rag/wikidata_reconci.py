"""Wikidata Reconciliation API client.

Thin wrapper around `https://wikidata.reconci.link/en/api`, an
ElasticSearch-backed reconciliation service over Wikidata maintained by
Antonin Delpeuch (independent of Wikimedia Foundation infrastructure).

Why this and not `wbsearchentities`:
- Type-aware ranking. Pass `type="Q43229"` (organization) to bias results
  toward the corporate space rather than letting random video games and
  given names rank highly. NOTE: this is a soft preference, not a hard
  filter — caller must still verify P31 client-side.
- Returns top-N candidates with a numeric score, type list, and
  description in one round trip (vs wbsearchentities' label-only result
  + separate entity-data fetch per candidate).
- Aliases (`skos:altLabel`) are searched natively, so "BLACKSTONE GROUP"
  matches "Blackstone Inc." which has alias "Blackstone Group" without
  any client-side suffix-stripping logic.
- Batched: up to ~50 queries per HTTP request, dropping per-name latency
  to ~150-400ms.

Trade-offs:
- Third-party hosted; if it goes down, our resolution layer breaks. The
  bulk-fetch + local-index plan in `decisions.md` (2026-05-10 entry) is
  the documented fallback architecture.
- No SLA. We rate-limit ourselves to avoid getting banned.

This module is purely a transport layer. Filtering, scoring, tiebreaks,
and confidence thresholds live one layer up in `wikidata_resolver.py`.
"""

from __future__ import annotations

import json
import logging
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional

import requests

logger = logging.getLogger(__name__)

RECONCI_ENDPOINT = "https://wikidata.reconci.link/en/api"
USER_AGENT = "LegalTender/1.0 (https://github.com/vedantadhobley/legal-tender)"

# A single batched POST can carry many queries. The service docs don't
# publish a hard limit, but 50 is a safe size that completes in <10s for
# our query shapes and avoids triggering any per-batch limit.
DEFAULT_BATCH_SIZE = 50

# We do NOT pass `type` to reconci.link. Empirically (2026-05-10) the
# `type` parameter REJECTS entities whose P31 doesn't directly include
# the target Q-id, even when P31 walks to it via P279 — so legitimate
# corporate matches disappear (BAUPOST GROUP returns 1 hit without
# filter, 0 with `type=Q43229`). Callers can opt back in by passing
# `type_qid="Q43229"` explicitly, but every caller in the codebase
# leaves it None.

REQUEST_TIMEOUT = 30
MAX_RETRIES = 3
INITIAL_BACKOFF = 1.0  # seconds; doubled per retry
MAX_BACKOFF = 30.0


@dataclass
class ReconciCandidate:
    """One candidate returned by the reconciliation API for one query."""

    qid: str
    name: str
    score: float
    description: Optional[str]
    types: List[Dict[str, str]]  # [{"id": "Q43229", "name": "organization"}, ...]
    is_match_flag: bool  # the API's own "match" boolean

    @classmethod
    def from_api(cls, raw: Dict[str, Any]) -> "ReconciCandidate":
        return cls(
            qid=raw.get("id", ""),
            name=raw.get("name", ""),
            score=float(raw.get("score", 0.0)),
            description=raw.get("description"),
            types=list(raw.get("type") or []),
            is_match_flag=bool(raw.get("match", False)),
        )


def _post_batch(queries: Dict[str, Dict[str, Any]]) -> Optional[Dict[str, Dict[str, Any]]]:
    """POST one batch to the reconciliation endpoint with retry+backoff.
    Returns the raw API response dict, or None on terminal failure."""
    last_exc: Optional[BaseException] = None
    backoff = INITIAL_BACKOFF
    for attempt in range(MAX_RETRIES):
        try:
            r = requests.post(
                RECONCI_ENDPOINT,
                data={"queries": json.dumps(queries)},
                headers={
                    "User-Agent": USER_AGENT,
                    "Accept": "application/json",
                },
                timeout=REQUEST_TIMEOUT,
            )
            if r.status_code == 429:
                # Rate-limited. Back off and retry; 429s aren't a service
                # outage, just the server asking us to slow down.
                logger.warning(
                    "reconci.link 429 (attempt %d/%d), backing off %.1fs",
                    attempt + 1, MAX_RETRIES, backoff,
                )
                time.sleep(backoff)
                backoff = min(backoff * 2, MAX_BACKOFF)
                last_exc = requests.HTTPError("429")
                continue
            r.raise_for_status()
            return r.json()
        except requests.RequestException as e:
            last_exc = e
            logger.warning(
                "reconci.link request failed (attempt %d/%d): %s",
                attempt + 1, MAX_RETRIES, e,
            )
            time.sleep(backoff)
            backoff = min(backoff * 2, MAX_BACKOFF)
    logger.error("reconci.link giving up after %d retries: %s", MAX_RETRIES, last_exc)
    return None


def _chunked(items: List[str], size: int) -> Iterable[List[str]]:
    for i in range(0, len(items), size):
        yield items[i:i + size]


def reconcile_batch(
    names: List[str],
    type_qid: Optional[str] = None,
    limit: int = 3,  # was 5 — reduced 2026-05-10. Each extra candidate
                     # adds ~3 type Q-ids to walk in the ontology cache,
                     # so 5→3 cuts per-chunk ontology workload by ~40%
                     # with negligible loss of resolution quality
                     # (we use top-1-2 in practice; the 4th-5th are
                     # provenance-only).
    batch_size: int = DEFAULT_BATCH_SIZE,
    parallel_batches: int = 2,
) -> Dict[str, List[ReconciCandidate]]:
    """Resolve `names` to ranked Wikidata candidates, batched.

    Returns a dict keyed by input name. Every input name appears in the
    output. An empty list value means either "no candidates" or "the
    request for this batch failed terminally" — caller can't easily
    distinguish, so caller should treat empty as "try again later" if
    fewer than expected names came back populated.

    Parameters:
        names: input strings (unmodified case/spacing — pass FEC names as-is)
        type_qid: type hint for ranking; default None (no type filter)
        limit: max candidates per query (the API caps lower than we'd
            ever want; 5 is plenty for typical disambiguation needs)
        batch_size: queries per HTTP request. Default 50.
        parallel_batches: HTTP requests in flight at once. Default 2 —
            polite to the third-party service. Tune up if needed.
    """
    if not names:
        return {}

    results: Dict[str, List[ReconciCandidate]] = {n: [] for n in names}

    chunks = list(_chunked(names, batch_size))

    def _process_chunk(chunk: List[str]) -> Dict[str, List[ReconciCandidate]]:
        # Build a queries dict keyed by index — we re-hydrate by position.
        # Only include `type` when caller explicitly opted in.
        def _build_query(name: str) -> Dict[str, Any]:
            q: Dict[str, Any] = {"query": name, "limit": limit}
            if type_qid:
                q["type"] = type_qid
            return q

        queries = {f"q{i}": _build_query(chunk[i]) for i in range(len(chunk))}
        response = _post_batch(queries)
        if not response:
            # Request terminally failed; return empty for this chunk.
            return {name: [] for name in chunk}
        out: Dict[str, List[ReconciCandidate]] = {}
        for i, name in enumerate(chunk):
            block = response.get(f"q{i}", {})
            raw_results = block.get("result") or []
            out[name] = [ReconciCandidate.from_api(r) for r in raw_results]
        return out

    # Parallelize across chunks. Threads share the requests connection
    # pool. Conservatively low concurrency to avoid 429s.
    if parallel_batches <= 1 or len(chunks) <= 1:
        for chunk in chunks:
            results.update(_process_chunk(chunk))
    else:
        with ThreadPoolExecutor(max_workers=parallel_batches) as ex:
            for chunk_result in ex.map(_process_chunk, chunks):
                results.update(chunk_result)

    return results


def reconcile_one(name: str, type_qid: Optional[str] = None, limit: int = 5) -> List[ReconciCandidate]:
    """Resolve one name. Convenience wrapper; batch via reconcile_batch
    for any non-trivial volume."""
    return reconcile_batch([name], type_qid=type_qid, limit=limit).get(name, [])


if __name__ == "__main__":
    # Smoke test. Run against the known-hard cases used in the Phase 0
    # reconciliation-API spike (see decisions.md 2026-05-10 entry).
    print("=" * 70)
    print("WIKIDATA RECONCILIATION API — SMOKE TEST")
    print("=" * 70)
    cases = [
        "CITADEL",
        "NEA",
        "BCG",
        "KKR",
        "FAHR",
        "ULINE",
        "BLACKSTONE GROUP",
        "PRATT INDUSTRIES",
        "ADELSON CLINIC",
        "GOLDMAN SACHS",
        "APPLE",
        "GOOGLE",
        "PAN AM RAILWAYS",
        "CITADEL INVESTMENT GROUP",
        "STEYER",
        "IBM",
    ]
    t = time.time()
    results = reconcile_batch(cases)
    dt = time.time() - t
    print(f"\nBatched {len(cases)} names in {dt:.2f}s")
    for name in cases:
        cands = results.get(name, [])
        print(f"\n{name!r}:")
        if not cands:
            print("  (no candidates)")
            continue
        for c in cands[:3]:
            mflag = "✓" if c.is_match_flag else " "
            types = ", ".join(t.get("name", "") for t in c.types)[:60]
            print(
                f"  {mflag} {c.qid:12} score={c.score:6.1f} "
                f"{c.name!r:35} [{types}]"
            )
