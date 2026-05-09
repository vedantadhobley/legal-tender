#!/usr/bin/env python3
"""Bulk validation: compare every candidate's funding_channels against FEC's weball.

Apples-to-apples comparison:
  Ours:  candidates[X].funding_channels.by_cycle[Y].direct_funding
  FEC:   fec_Y.weball[X].TTL_RECEIPTS

direct_funding is "money received by the candidate's principal committee" —
excludes IE (Super PAC spending FOR/AGAINST). weball.TTL_RECEIPTS is the same
scope. So they should match closely (within data-quality bounds).

Outputs:
  - Per-cycle distribution of delta percentages
  - Buckets: % of candidates within ±1%, ±5%, ±10%, ±25%, ±50%
  - Top 20 worst offenders per cycle (biggest abs delta)
  - Top 20 over-attributed and under-attributed
  - Aggregate quality score: median delta, p90 delta

Run:
  docker exec legal-tender-dev-webserver python3 /workspace/scripts/validation_report.py
"""

import os
import sys
from arango import ArangoClient


CYCLES = ["2020", "2022", "2024", "2026"]


def fmt_money(n):
    if n is None:
        return "—"
    if abs(n) >= 1e9:
        return f"${n/1e9:+.2f}B"
    if abs(n) >= 1e6:
        return f"${n/1e6:+.2f}M"
    if abs(n) >= 1e3:
        return f"${n/1e3:+.0f}K"
    return f"${n:+.0f}"


def main():
    client = ArangoClient(hosts=os.environ.get("ARANGO_URL", "http://legal-tender-dev-arango:8529"))
    agg_db = client.db("aggregation", username="root", password="ltpass")

    print("=" * 90)
    print("  BULK VALIDATION: our direct_funding vs FEC weball.TTL_RECEIPTS")
    print("=" * 90)

    # Collect all (cand_id, cycle, ours, fec) rows
    rows = []  # list of dicts
    for cycle in CYCLES:
        fec_db_name = f"fec_{cycle}"
        try:
            fec_db = client.db(fec_db_name, username="root", password="ltpass")
        except Exception:
            print(f"\n[skip] {fec_db_name} not accessible")
            continue

        # Pull weball for this cycle into memory (it's small — ~4K rows/cycle)
        fec_records = {}
        for r in fec_db.aql.execute("FOR c IN weball RETURN c"):
            fec_records[r["CAND_ID"]] = r

        # Pull every candidate that has funding_channels for this cycle
        ours = list(agg_db.aql.execute(f"""
            FOR c IN candidates
                FILTER c.funding_channels != null
                   AND c.funding_channels.by_cycle != null
                   AND c.funding_channels.by_cycle["{cycle}"] != null
                LET cyc = c.funding_channels.by_cycle["{cycle}"]
                RETURN {{
                    cand_id: c.CAND_ID,
                    name: c.CAND_NAME,
                    office: c.CAND_OFFICE,
                    st: c.CAND_OFFICE_ST,
                    direct_funding: cyc.direct_funding,
                    total_funding: cyc.total_funding,
                    ie_support: cyc.ie_support,
                    ie_oppose: cyc.ie_oppose
                }}
        """))

        for o in ours:
            fec = fec_records.get(o["cand_id"])
            if fec is None:
                continue
            fec_ttl = fec.get("TTL_RECEIPTS") or 0
            our_direct = o.get("direct_funding") or 0
            if fec_ttl == 0 and our_direct == 0:
                continue  # both zero, uninteresting
            rows.append({
                "cycle": cycle,
                "cand_id": o["cand_id"],
                "name": o["name"],
                "office": o["office"],
                "st": o["st"],
                "ours": our_direct,
                "fec": fec_ttl,
                "abs_delta": our_direct - fec_ttl,
                "pct_delta": (our_direct - fec_ttl) / fec_ttl * 100 if fec_ttl > 0 else None,
            })

    print(f"\n  Compared rows: {len(rows):,}")
    print(f"  Cycles covered: {sorted(set(r['cycle'] for r in rows))}")

    # ------------------------------------------------------------------------
    # Per-cycle stats
    # ------------------------------------------------------------------------
    for cycle in CYCLES:
        cycle_rows = [r for r in rows if r["cycle"] == cycle and r["pct_delta"] is not None]
        if not cycle_rows:
            continue

        deltas = sorted(abs(r["pct_delta"]) for r in cycle_rows)
        n = len(deltas)

        within_1 = sum(1 for d in deltas if d <= 1)
        within_5 = sum(1 for d in deltas if d <= 5)
        within_10 = sum(1 for d in deltas if d <= 10)
        within_25 = sum(1 for d in deltas if d <= 25)
        within_50 = sum(1 for d in deltas if d <= 50)

        median = deltas[n // 2]
        p90 = deltas[int(n * 0.9)]
        p99 = deltas[int(n * 0.99)] if n >= 100 else deltas[-1]

        print(f"\n[{cycle}] {n:,} candidates compared (have both our funding_channels and FEC weball)")
        print(f"  within ± 1%: {within_1:>5,}  ({100*within_1/n:.1f}%)")
        print(f"  within ± 5%: {within_5:>5,}  ({100*within_5/n:.1f}%)")
        print(f"  within ±10%: {within_10:>5,}  ({100*within_10/n:.1f}%)")
        print(f"  within ±25%: {within_25:>5,}  ({100*within_25/n:.1f}%)")
        print(f"  within ±50%: {within_50:>5,}  ({100*within_50/n:.1f}%)")
        print(f"  median |delta|: {median:.1f}%   p90: {p90:.1f}%   p99: {p99:.1f}%")

        # Sort by absolute delta size (biggest dollars first), not pct
        cycle_rows_sorted = sorted(cycle_rows, key=lambda r: abs(r["abs_delta"] or 0), reverse=True)

        print(f"\n  Top 5 worst offenders by abs($ delta) for {cycle}:")
        print(f"  {'name':<40s} {'office':<8s} {'ours':>12s} {'FEC':>12s} {'delta':>12s} {'%':>8s}")
        for r in cycle_rows_sorted[:5]:
            office_st = f"{r['office']}/{r['st'] or '--'}"
            print(f"  {r['name'][:40]:<40s} {office_st:<8s} "
                  f"{fmt_money(r['ours']):>12s} {fmt_money(r['fec']):>12s} "
                  f"{fmt_money(r['abs_delta']):>12s} {r['pct_delta']:>+7.0f}%")

    # ------------------------------------------------------------------------
    # BWC sanity check (one specific named candidate)
    # ------------------------------------------------------------------------
    print(f"\n[BWC sanity check across cycles]")
    bwc_rows = [r for r in rows if r["cand_id"] == "H4NJ12149"]
    print(f"  {'cycle':<6s} {'ours':>14s} {'FEC':>14s} {'delta':>12s} {'%':>8s}")
    for r in bwc_rows:
        d = "—" if r["pct_delta"] is None else f"{r['pct_delta']:+.1f}%"
        print(f"  {r['cycle']:<6s} {fmt_money(r['ours']):>14s} {fmt_money(r['fec']):>14s} "
              f"{fmt_money(r['abs_delta']):>12s} {d:>8s}")

    # ------------------------------------------------------------------------
    # Aggregate summary
    # ------------------------------------------------------------------------
    all_deltas = [abs(r["pct_delta"]) for r in rows if r["pct_delta"] is not None]
    n = len(all_deltas)
    if n:
        all_deltas.sort()
        within_5 = sum(1 for d in all_deltas if d <= 5)
        within_10 = sum(1 for d in all_deltas if d <= 10)
        within_25 = sum(1 for d in all_deltas if d <= 25)

        print(f"\n[overall]")
        print(f"  total comparisons:       {n:,}")
        print(f"  median |delta|:          {all_deltas[n//2]:.1f}%")
        print(f"  within ± 5% of FEC:      {100*within_5/n:.1f}%")
        print(f"  within ±10% of FEC:      {100*within_10/n:.1f}%")
        print(f"  within ±25% of FEC:      {100*within_25/n:.1f}%")

    print()
    print("=" * 90)


if __name__ == "__main__":
    main()
