"""Validation harness — diff old vs new wikidata resolution on the full
canonical_employers set.

Compares a "before" snapshot of `corporate_families` and
`employer_canonical_mapping` against the current Arango state after
the resolver pivot. Categorizes per-employer changes into
improved / regressed / unchanged / new / lost, with concrete examples.

Acceptance metrics from `docs/decisions.md` 2026-05-10:
  1. Filter-shaped code lines: 250 → 0 in code (verified separately)
  2. Hardcoded Q-id mappings: 12 → ≤5 in YAML (Phase 6)
  3. No regressions on currently-resolved ~3,000 employers
  4. Hit-rate ≥ current 60%

This script reports specifically against (3) and (4).

Usage:
  # First snapshot (before the new run):
  python scripts/validate_wikidata_resolver.py snapshot \\
      --output /tmp/before.json
  # ... run wikidata_corporate_resolution with new resolver ...
  # Then diff:
  python scripts/validate_wikidata_resolver.py diff \\
      --before /tmp/before.json
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections import defaultdict
from typing import Any, Dict, List, Optional, Tuple

# Inject project root so `python scripts/...` works without install.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from arango import ArangoClient  # noqa: E402


def _connect(host: str, password: str):
    client = ArangoClient(hosts=host)
    return client.db("aggregation", username="root", password=password)


def snapshot(db) -> Dict[str, Any]:
    """Capture corp_families + employer_canonical_mapping as a single
    JSON dict. Used as 'before' state."""
    families = list(db.aql.execute("""
        FOR f IN corporate_families
            RETURN {
                canonical_name: f.canonical_name,
                wikidata_id: f.wikidata_id,
                total_influence: f.total_influence,
                member_employers: f.member_employers,
                linked_whales: f.linked_whales,
            }
    """))
    mappings = list(db.aql.execute("""
        FOR m IN employer_canonical_mapping
            RETURN {
                employer_name: m.employer_name,
                canonical_name: m.canonical_name,
                wikidata_id: m.wikidata_id,
                amount: m.amount,
                relationship: m.relationship,
            }
    """))
    return {"corporate_families": families, "employer_canonical_mapping": mappings}


def _index_mappings(records: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """Index by employer_name for O(1) lookup."""
    return {r["employer_name"]: r for r in records}


def _index_families(records: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """Index by canonical_name."""
    return {r["canonical_name"]: r for r in records}


def diff(before: Dict[str, Any], after: Dict[str, Any]) -> None:
    """Categorize per-employer changes and print a report."""
    b_map = _index_mappings(before["employer_canonical_mapping"])
    a_map = _index_mappings(after["employer_canonical_mapping"])

    b_emps = set(b_map)
    a_emps = set(a_map)

    only_before = b_emps - a_emps
    only_after = a_emps - b_emps
    common = b_emps & a_emps

    # Categorize the common set into change classes.
    unchanged: List[str] = []
    canonical_changed: List[Tuple[str, str, str]] = []  # (emp, before, after)
    qid_changed: List[Tuple[str, Optional[str], Optional[str]]] = []
    newly_resolved: List[Tuple[str, str]] = []  # (emp, new_canonical)
    newly_unresolved: List[Tuple[str, str]] = []  # (emp, old_canonical)

    for emp in common:
        b = b_map[emp]
        a = a_map[emp]
        b_qid = b.get("wikidata_id")
        a_qid = a.get("wikidata_id")
        b_can = b.get("canonical_name")
        a_can = a.get("canonical_name")
        was_resolved = bool(b_qid)
        is_resolved = bool(a_qid)

        if not was_resolved and is_resolved:
            newly_resolved.append((emp, a_can or ""))
        elif was_resolved and not is_resolved:
            newly_unresolved.append((emp, b_can or ""))
        elif b_qid != a_qid:
            qid_changed.append((emp, b_qid, a_qid))
        elif b_can != a_can:
            canonical_changed.append((emp, b_can or "", a_can or ""))
        else:
            unchanged.append(emp)

    # Hit-rate
    b_resolved = sum(1 for r in b_map.values() if r.get("wikidata_id"))
    a_resolved = sum(1 for r in a_map.values() if r.get("wikidata_id"))
    # In the new path, GLEIF results have wikidata_id=null but are still
    # "resolved" — count them via canonical != raw-name as a proxy.
    a_resolved_or_gleif = sum(
        1 for r in a_map.values()
        if r.get("wikidata_id") or (r.get("canonical_name") != r.get("employer_name"))
    )

    print("=" * 72)
    print("WIKIDATA RESOLVER VALIDATION — old vs new")
    print("=" * 72)
    print(f"Total employer mappings:")
    print(f"  before: {len(b_map):,}")
    print(f"  after:  {len(a_map):,}")
    print()
    print(f"Hit-rate (wikidata_id resolved):")
    print(f"  before: {b_resolved:,} / {len(b_map):,} = {100*b_resolved/max(1,len(b_map)):.1f}%")
    print(f"  after:  {a_resolved:,} / {len(a_map):,} = {100*a_resolved/max(1,len(a_map)):.1f}% (Wikidata-only)")
    print(f"  after:  {a_resolved_or_gleif:,} / {len(a_map):,} = {100*a_resolved_or_gleif/max(1,len(a_map)):.1f}% (Wikidata+GLEIF)")
    print()
    print(f"Per-employer change categories:")
    print(f"  unchanged (same canonical_name + wikidata_id): {len(unchanged):,}")
    print(f"  newly resolved (was raw-FEC, now Wikidata/GLEIF): {len(newly_resolved):,}")
    print(f"  newly unresolved (was Wikidata, now raw-FEC): {len(newly_unresolved):,}")
    print(f"  Q-id changed (different Wikidata entity picked): {len(qid_changed):,}")
    print(f"  canonical changed (same Q-id, different label): {len(canonical_changed):,}")
    print(f"  only in before (employer dropped from canonical_employers): {len(only_before):,}")
    print(f"  only in after (employer newly added): {len(only_after):,}")
    print()

    # Spot-check samples — sorted by donation amount where available.
    def _amt(emp_record):
        return emp_record.get("amount", 0) or 0

    print("=" * 72)
    print("REGRESSIONS (newly unresolved — was Wikidata, now raw)")
    print("These need manual spot-check. Sorted by amount.")
    print("=" * 72)
    samples = sorted(newly_unresolved, key=lambda x: -_amt(b_map[x[0]]))[:30]
    for emp, was in samples:
        amt = _amt(b_map[emp])
        print(f"  ${amt:>14,.0f}  {emp!r:35} was: {was!r}")
    print()

    print("=" * 72)
    print("Q-ID CHANGES (different entity selected for same employer)")
    print("Possibly improvements or regressions. Sorted by amount.")
    print("=" * 72)
    samples = sorted(qid_changed, key=lambda x: -_amt(b_map[x[0]]))[:30]
    for emp, b_qid, a_qid in samples:
        amt = _amt(b_map[emp])
        b_can = b_map[emp].get("canonical_name")
        a_can = a_map[emp].get("canonical_name")
        print(f"  ${amt:>14,.0f}  {emp!r:30}")
        print(f"      before: {b_qid} {b_can!r}")
        print(f"      after:  {a_qid} {a_can!r}")
    print()

    print("=" * 72)
    print("NEWLY RESOLVED (was raw-FEC, now Wikidata/GLEIF)")
    print("Mostly wins. Sorted by amount.")
    print("=" * 72)
    samples = sorted(newly_resolved, key=lambda x: -_amt(b_map[x[0]]))[:30]
    for emp, now in samples:
        amt = _amt(b_map[emp])
        a_qid = a_map[emp].get("wikidata_id") or "(GLEIF)"
        print(f"  ${amt:>14,.0f}  {emp!r:30} -> {now!r}  [{a_qid}]")


def main():
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="command", required=True)

    snap = sub.add_parser("snapshot", help="Capture current state to JSON")
    snap.add_argument("--host", default="http://legal-tender-dev-arango:8529")
    snap.add_argument("--password", default="ltpass")
    snap.add_argument("--output", required=True)

    df = sub.add_parser("diff", help="Diff before-snapshot against current state")
    df.add_argument("--before", required=True)
    df.add_argument("--host", default="http://legal-tender-dev-arango:8529")
    df.add_argument("--password", default="ltpass")

    args = p.parse_args()
    db = _connect(args.host, args.password)

    if args.command == "snapshot":
        data = snapshot(db)
        with open(args.output, "w") as f:
            json.dump(data, f)
        print(f"Wrote snapshot: {len(data['corporate_families']):,} families, "
              f"{len(data['employer_canonical_mapping']):,} mappings → {args.output}")
    elif args.command == "diff":
        with open(args.before) as f:
            before = json.load(f)
        after = snapshot(db)
        diff(before, after)


if __name__ == "__main__":
    main()
