"""Surface where a candidate's whale-donor pool also gives, without
imposing a pre-defined network label.

The motivating use case: instead of "show me how much AIPAC money this
candidate took," the principled question is "what committees do this
candidate's donors collectively fund?" The algorithm reports the top-K
co-funded committees in order of pool spend; a human reader identifies
any clusters they see in the names.

Why this matters: hard-coding "look for AIPAC" injects the analyst's
bias into the result. Surfacing destinations algorithmically and
letting them be named after the fact preserves the bias-removal
property the pipeline was built for.

USAGE:
    python scripts/donor_network_overlap.py <CAND_ID> [--top-k 30]
                                           [--by candidates|committees|both]
                                           [--min-dollars 1000]
                                           [--cycle 2026]

OUTPUT (default):
    Two tables, sorted by aggregate pool flow:
      1. Top-K committees the pool funds (excluding the target itself).
      2. Top-K candidates the pool funds (across all their principal
         committees, excluding the target).
    Plus a per-donor footprint table for the most-active pool members.

DESIGN NOTES:
  - Operates on the `donors` + `contributed_to` graph, so only
    max-out-tier donors are visible (FEC's per-election limits).
    Sub-tier donations are aggregate-only and not graph-traced.
  - "Pool" = donors with a contributed_to edge to any of the target's
    principal committees, in any cycle.
  - Aggregation crosses cycles. If you want a single-cycle view, pass
    --cycle.
  - No clustering yet — flat top-K. Pairwise donor-overlap clustering
    (Jaccard, DBSCAN) is a v2 if the flat report turns out
    insufficient.
"""

import argparse
import sys
from collections import defaultdict
from typing import Dict, List, Optional, Tuple

from arango import ArangoClient


def _connect_aggregation():
    """Connect to the aggregation DB. Connection details mirror the
    Dagster resource defaults; override via env if needed."""
    import os
    host = os.environ.get("LT_ARANGO_HOST", "legal-tender-dev-arango")
    port = os.environ.get("LT_ARANGO_PORT", "8529")
    client = ArangoClient(hosts=f"http://{host}:{port}")
    return client.db("aggregation", username="root", password="ltpass")


def get_principal_committees(db, cand_id: str) -> List[Tuple[str, str]]:
    """Return [(cmte_id, cmte_name), ...] affiliated with the candidate."""
    return list(db.aql.execute(
        """
        FOR v, e IN INBOUND @cand affiliated_with
          RETURN DISTINCT {id: v._key, name: v.CMTE_NM}
        """,
        bind_vars={"cand": f"candidates/{cand_id}"},
    ))


def get_pool(db, cmte_ids: List[str], cycle: Optional[str] = None) -> Dict[str, Dict]:
    """Get whale donors who funded any of the target's committees.

    Returns {donor_key: {name, employer, total_to_target}}.
    """
    extra_filter = "FILTER e.cycle == @cycle" if cycle else ""
    bind = {"cmtes": [f"committees/{c}" for c in cmte_ids]}
    if cycle:
        bind["cycle"] = cycle
    rows = list(db.aql.execute(
        f"""
        FOR e IN contributed_to
          FILTER e._to IN @cmtes
          {extra_filter}
          LET d = DOCUMENT(e._from)
          COLLECT key = d._key, name = d.canonical_name, employer = d.canonical_employer
          AGGREGATE total = SUM(e.total_amount)
          SORT total DESC
          RETURN {{key: key, name: name, employer: employer, total_to_target: total}}
        """,
        bind_vars=bind,
    ))
    return {r["key"]: r for r in rows}


def top_recipient_committees(
    db,
    pool_keys: List[str],
    exclude_cmtes: List[str],
    top_k: int,
    cycle: Optional[str],
) -> List[Dict]:
    """Top K committees the pool funds (excluding target's own cmtes)."""
    extra_filter = "FILTER e.cycle == @cycle" if cycle else ""
    bind = {
        "keys": pool_keys,
        "excl": [f"committees/{c}" for c in exclude_cmtes],
    }
    if cycle:
        bind["cycle"] = cycle
    return list(db.aql.execute(
        f"""
        FOR e IN contributed_to
          FILTER PARSE_IDENTIFIER(e._from).key IN @keys
          FILTER e._to NOT IN @excl
          {extra_filter}
          LET cmte = DOCUMENT(e._to)
          COLLECT cmte_id = cmte._key,
                  cmte_name = cmte.CMTE_NM,
                  cmte_type = cmte.terminal_type,
                  cmte_party = cmte.CMTE_PTY_AFFILIATION
          AGGREGATE pool_total = SUM(e.total_amount),
                    donor_count = COUNT_DISTINCT(e._from)
          SORT pool_total DESC
          LIMIT @k
          RETURN {{
            id: cmte_id, name: cmte_name, type: cmte_type, party: cmte_party,
            total: pool_total, donors: donor_count
          }}
        """,
        bind_vars={**bind, "k": top_k},
    ))


def top_recipient_candidates(
    db,
    pool_keys: List[str],
    exclude_cand: str,
    top_k: int,
    cycle: Optional[str],
) -> List[Dict]:
    """Top K candidates (by their principal committees) the pool funds.

    Skips the target candidate. Aggregates across all a candidate's
    principal committees and across all cycles (or one cycle if given).
    """
    extra_filter = "FILTER e.cycle == @cycle" if cycle else ""
    bind = {"keys": pool_keys, "excl_cand": f"candidates/{exclude_cand}"}
    if cycle:
        bind["cycle"] = cycle
    return list(db.aql.execute(
        f"""
        FOR e IN contributed_to
          FILTER PARSE_IDENTIFIER(e._from).key IN @keys
          {extra_filter}
          FOR cand IN OUTBOUND e._to affiliated_with
            FILTER cand._id != @excl_cand
            COLLECT cand_id = cand._key, cand_name = cand.CAND_NAME,
                    office = cand.CAND_OFFICE, st = cand.CAND_OFFICE_ST,
                    dist = cand.CAND_OFFICE_DISTRICT,
                    party = cand.CAND_PTY_AFFILIATION
            AGGREGATE pool_total = SUM(e.total_amount),
                      donor_count = COUNT_DISTINCT(e._from)
            SORT pool_total DESC
            LIMIT @k
            RETURN {{
              id: cand_id, name: cand_name, office: office, st: st,
              dist: dist, party: party, total: pool_total, donors: donor_count
            }}
        """,
        bind_vars={**bind, "k": top_k},
    ))


def deployment_by_committee_type(
    db, pool_keys: List[str], exclude_cmtes: List[str], cycle: Optional[str]
) -> List[Dict]:
    """Aggregate pool spending by destination terminal_type."""
    extra_filter = "FILTER e.cycle == @cycle" if cycle else ""
    bind = {
        "keys": pool_keys,
        "excl": [f"committees/{c}" for c in exclude_cmtes],
    }
    if cycle:
        bind["cycle"] = cycle
    return list(db.aql.execute(
        f"""
        FOR e IN contributed_to
          FILTER PARSE_IDENTIFIER(e._from).key IN @keys
          FILTER e._to NOT IN @excl
          {extra_filter}
          LET cmte = DOCUMENT(e._to)
          COLLECT t = cmte.terminal_type
          AGGREGATE total = SUM(e.total_amount),
                    edges = COUNT(1)
          SORT total DESC
          RETURN {{type: t, total: total, edges: edges}}
        """,
        bind_vars=bind,
    ))


def donor_footprints(
    db, pool: Dict[str, Dict], top_n: int
) -> List[Dict]:
    """For the top N pool donors by full-FEC giving, return their
    portfolio totals so the reader can see who's actually heavy."""
    out = []
    for key, d in pool.items():
        totals = list(db.aql.execute(
            """
            FOR e IN contributed_to FILTER e._from == @from
              COLLECT AGGREGATE total = SUM(e.total_amount),
                                cmtes = COUNT_DISTINCT(e._to)
              RETURN {total: total, cmtes: cmtes}
            """,
            bind_vars={"from": f"donors/{key}"},
        ))
        if totals and totals[0]:
            out.append({
                "name": d["name"],
                "employer": d.get("employer"),
                "to_target": d["total_to_target"],
                "all_giving": totals[0]["total"],
                "n_cmtes": totals[0]["cmtes"],
            })
    out.sort(key=lambda r: -r["all_giving"])
    return out[:top_n]


def fmt_money(x):
    return f"${x:>12,.0f}"


def fmt_row(*cells, widths):
    return "  ".join(str(c).ljust(w) if i == 0 else str(c).rjust(w)
                     for i, (c, w) in enumerate(zip(cells, widths)))


def main():
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("cand_id", help="FEC candidate ID (e.g. H6NJ12268)")
    p.add_argument("--top-k", type=int, default=30,
                   help="Number of co-funded committees/candidates to surface (default 30).")
    p.add_argument("--by", choices=("committees", "candidates", "both"), default="both",
                   help="Show top-K committees, candidates, or both. Default: both.")
    p.add_argument("--cycle", default=None,
                   help="Restrict to one cycle (e.g. 2026). Default: all cycles.")
    p.add_argument("--min-dollars", type=float, default=1000.0,
                   help="Hide rows below this $ threshold (default 1000).")
    p.add_argument("--donor-footprint-n", type=int, default=15,
                   help="How many of the most-active pool donors to surface (default 15).")
    args = p.parse_args()

    db = _connect_aggregation()
    cand = db.collection("candidates").get(args.cand_id)
    if not cand:
        print(f"Candidate {args.cand_id} not found", file=sys.stderr)
        sys.exit(1)

    cmtes = get_principal_committees(db, args.cand_id)
    if not cmtes:
        print(f"No principal committees for {args.cand_id}", file=sys.stderr)
        sys.exit(1)
    cmte_ids = [c["id"] for c in cmtes]

    pool = get_pool(db, cmte_ids, cycle=args.cycle)
    pool_total = sum(d["total_to_target"] for d in pool.values())

    print()
    print("=" * 92)
    print(f"DONOR-NETWORK OVERLAP — {cand.get('CAND_NAME')} ({args.cand_id})")
    print(f"  Office: {cand.get('CAND_OFFICE')}/{cand.get('CAND_OFFICE_ST')}"
          f"-{cand.get('CAND_OFFICE_DISTRICT', '00')}"
          f"  Party: {cand.get('CAND_PTY_AFFILIATION')}"
          f"  Election year: {cand.get('CAND_ELECTION_YR')}")
    print(f"  Principal committee(s): {', '.join(c['name'] for c in cmtes)}")
    print(f"  Cycle filter: {args.cycle or 'all'}")
    print(f"  Whale-donor pool: {len(pool):,} donors, ${pool_total:,.0f} total max-out money")
    print("=" * 92)

    if not pool:
        print()
        print("  No graph-tracked whale donors found. Either the candidate")
        print("  has no max-out donors yet, or our pipeline doesn't have")
        print("  itemized records for this committee (check the")
        print("  individuals.data_quality block on the candidate doc).")
        return

    pool_keys = list(pool.keys())

    if args.by in ("committees", "both"):
        committees = top_recipient_committees(db, pool_keys, cmte_ids,
                                              args.top_k, args.cycle)
        committees = [c for c in committees if c["total"] >= args.min_dollars]
        print()
        print(f"# Top {len(committees)} committees this pool also funds")
        print(f"  (ranked by aggregate pool spend, threshold ${args.min_dollars:,.0f})")
        print()
        widths = (55, 14, 9, 25)
        print("  " + fmt_row("Committee", "$ from pool", "# donors", "terminal_type",
                             widths=widths))
        print("  " + "-" * sum(widths) + "  " * (len(widths) - 1))
        for c in committees:
            name = (c["name"] or "")[:54]
            t = (c["type"] or "?")[:24]
            print("  " + fmt_row(name, fmt_money(c["total"]),
                                 str(c["donors"]), t,
                                 widths=widths))

        # Spending by terminal_type summary
        by_type = deployment_by_committee_type(db, pool_keys, cmte_ids, args.cycle)
        total_deployment = sum(r["total"] for r in by_type)
        print()
        print(f"# Pool deployment by recipient terminal_type")
        print(f"  Total (excl. target's committees): ${total_deployment:,.0f}")
        for r in by_type:
            pct = 100 * r["total"] / total_deployment if total_deployment else 0
            t = r["type"] or "?"
            print(f"    {t:<27} ${r['total']:>14,.0f}  ({pct:>5.1f}%)  edges={r['edges']:,}")

    if args.by in ("candidates", "both"):
        candidates = top_recipient_candidates(db, pool_keys, args.cand_id,
                                              args.top_k, args.cycle)
        candidates = [c for c in candidates if c["total"] >= args.min_dollars]
        print()
        print(f"# Top {len(candidates)} candidates this pool also funds")
        print()
        widths = (32, 10, 5, 14, 9)
        print("  " + fmt_row("Candidate", "office", "party", "$ from pool", "# donors",
                             widths=widths))
        print("  " + "-" * sum(widths) + "  " * (len(widths) - 1))
        for c in candidates:
            seat = (f"{c['office']}/{c['st']}-{c['dist']}"
                    if c["office"] == "H" else f"{c['office']}/{c['st']}")
            print("  " + fmt_row((c["name"] or "")[:31], seat,
                                 (c["party"] or "")[:4],
                                 fmt_money(c["total"]),
                                 str(c["donors"]),
                                 widths=widths))

    # Per-donor footprint
    print()
    print(f"# Top {args.donor_footprint_n} pool donors by total FEC giving footprint")
    print(f"  (so you can see who in the pool is a serial political donor vs one-off)")
    print()
    foot = donor_footprints(db, pool, args.donor_footprint_n)
    widths = (32, 26, 12, 12, 7)
    print("  " + fmt_row("Donor", "Employer", "→target", "→all FEC", "#cmtes",
                         widths=widths))
    print("  " + "-" * sum(widths) + "  " * (len(widths) - 1))
    for d in foot:
        print("  " + fmt_row((d["name"] or "")[:31],
                             (d.get("employer") or "")[:25],
                             fmt_money(d["to_target"]),
                             fmt_money(d["all_giving"]),
                             str(d["n_cmtes"]),
                             widths=widths))

    print()
    print("# How to read this")
    print("  - The committees table shows where this candidate's donors collectively")
    print("    deploy their money. Clusters of related committees (multiple PACs of")
    print("    the same network) indicate the donor pool is organized around an")
    print("    issue or affiliation. NAMING the cluster is your job, not the tool's.")
    print("  - The candidates table shows which OTHER candidates the pool also funds.")
    print("    A pool that heavily funds one party's incumbents has a partisan signal.")
    print("    A pool that funds candidates from both parties around a specific issue")
    print("    (e.g. foreign policy) has an issue-network signal.")
    print("  - Per-donor footprints surface bundlers — donors with high total FEC")
    print("    giving spread across many committees are not one-off contributors.")


if __name__ == "__main__":
    main()
