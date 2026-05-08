#!/usr/bin/env python3
"""Sanity-check the funding_channels output in ArangoDB after candidate_funding runs.

Designed to surface obvious bugs in the trace algorithm in seconds without
manual inspection. Run with:

    docker exec legal-tender-dev-arango arangosh \\
        --server.password ltpass \\
        --server.database aggregation \\
        --javascript.execute-string "$(cat scripts/validate_funding_channels.py)"

…actually no, this needs to run as Python with arango client. Use:

    docker exec legal-tender-dev-webserver python3 /workspace/scripts/validate_funding_channels.py

Each check returns PASS / WARN / FAIL with a plain-text reason.
"""

import os
import sys
from arango import ArangoClient


def fmt_money(n):
    if n is None:
        return "—"
    if n >= 1e12:
        return f"${n/1e12:.2f}T"
    if n >= 1e9:
        return f"${n/1e9:.2f}B"
    if n >= 1e6:
        return f"${n/1e6:.2f}M"
    if n >= 1e3:
        return f"${n/1e3:.1f}K"
    return f"${n:,.0f}"


def main():
    client = ArangoClient(hosts=os.environ.get("ARANGO_URL", "http://legal-tender-dev-arango:8529"))
    db = client.db("aggregation", username="root", password="ltpass")

    print("=" * 70)
    print("  FUNDING CHANNELS VALIDATION")
    print("=" * 70)

    failures = []
    warnings = []

    # ------------------------------------------------------------------------
    # 1. Coverage: how many candidates have funding_channels populated?
    # ------------------------------------------------------------------------
    total = db.collection("candidates").count()
    with_funding = next(db.aql.execute(
        "FOR c IN candidates FILTER c.funding_channels != null COLLECT WITH COUNT INTO n RETURN n"
    ))
    pct = 100 * with_funding / total if total else 0
    print(f"\n[Coverage]")
    print(f"  candidates total:           {total:,}")
    print(f"  with funding_channels:      {with_funding:,} ({pct:.1f}%)")
    if pct < 30:
        warnings.append(f"Coverage low: only {pct:.1f}% of candidates have funding_channels")

    # ------------------------------------------------------------------------
    # 2. Magnitude check: top candidate should NOT be in trillions
    # ------------------------------------------------------------------------
    top = list(db.aql.execute("""
        FOR c IN candidates
            FILTER c.funding_channels != null
               AND c.funding_channels.`aggregate` != null
               AND c.funding_channels.`aggregate`.total_funding > 0
            SORT c.funding_channels.`aggregate`.total_funding DESC
            LIMIT 10
            RETURN {
                name: c.CAND_NAME,
                office: c.CAND_OFFICE,
                st: c.CAND_OFFICE_ST,
                total: c.funding_channels.`aggregate`.total_funding
            }
    """))
    print(f"\n[Top 10 by total_funding]")
    for c in top:
        print(f"  {fmt_money(c['total']):>10}   {c['office']}/{c['st'] or '--':<3}  {c['name']}")

    if top:
        max_total = top[0]['total']
        # Sanity: no campaign (even Trump 2024 across 4 cycles) raised >$5B aggregate
        if max_total > 5e9:
            failures.append(
                f"Top candidate has total_funding={fmt_money(max_total)} — implausible. "
                f"Real-world ceiling for federal campaigns is ~$1-2B per cycle."
            )
        elif max_total > 2e9:
            warnings.append(
                f"Top candidate has total_funding={fmt_money(max_total)} — borderline. "
                f"Verify against published totals."
            )

    # ------------------------------------------------------------------------
    # 3. BWC sanity check (NJ-12 incumbent, simple House race)
    # ------------------------------------------------------------------------
    bwc = list(db.aql.execute("""
        FOR c IN candidates
            FILTER c.CAND_OFFICE_ST == "NJ" AND c.CAND_OFFICE_DISTRICT == "12"
               AND c.CAND_NAME LIKE "%COLEMAN%"
            LIMIT 1
            RETURN {
                name: c.CAND_NAME,
                fc: c.funding_channels
            }
    """))
    print(f"\n[BWC sanity check (NJ-12 House incumbent)]")
    if bwc and bwc[0]['fc']:
        fc = bwc[0]['fc']
        agg = fc.get("aggregate", {})
        print(f"  candidate:                  {bwc[0]['name']}")
        print(f"  total_funding (aggregate):  {fmt_money(agg.get('total_funding'))}")
        print(f"  direct_funding:             {fmt_money(agg.get('direct_funding'))}")
        print(f"  ie_support:                 {fmt_money(agg.get('ie_support'))}")
        print(f"  ie_oppose:                  {fmt_money(agg.get('ie_oppose'))}")
        ind = agg.get("individuals") or {}
        print(f"  individuals.total:          {fmt_money(ind.get('total'))}")
        org = agg.get("organizational_direct") or {}
        print(f"  organizational_direct.total:{fmt_money(org.get('total'))}")
        unacc = agg.get("unaccounted") or {}
        print(f"  unaccounted.pct:            {unacc.get('pct', '?'):.1f}%" if isinstance(unacc.get('pct'), (int, float)) else f"  unaccounted.pct:            {unacc.get('pct', '?')}")

        print(f"\n  per-cycle:")
        for cycle, data in (fc.get("by_cycle") or {}).items():
            print(f"    {cycle}: {fmt_money(data.get('total_funding'))}")

        # Plausibility: BWC aggregate should be $1-15M (House incumbent, 4 cycles)
        total_funding = agg.get("total_funding") or 0
        if total_funding > 50e6:
            failures.append(
                f"BWC total_funding={fmt_money(total_funding)} — implausible. "
                f"House incumbents typically raise $1-5M/cycle."
            )
        elif total_funding > 20e6:
            warnings.append(
                f"BWC total_funding={fmt_money(total_funding)} — high but possibly valid. "
                f"Verify against FEC or OpenSecrets."
            )
        elif total_funding < 100e3:
            warnings.append(
                f"BWC total_funding={fmt_money(total_funding)} — surprisingly low. "
                f"Raw indiv contributions alone are ~$1.17M across 4 cycles."
            )

        # Per-cycle plausibility
        for cycle, data in (fc.get("by_cycle") or {}).items():
            tot = data.get("total_funding") or 0
            if tot > 50e6:
                failures.append(
                    f"BWC cycle {cycle} total_funding={fmt_money(tot)} — implausible for one House race."
                )
    else:
        warnings.append("BWC has no funding_channels — no candidates traced or query failed")

    # ------------------------------------------------------------------------
    # 4. Distribution: are there obvious outliers (orders of magnitude > median)?
    # ------------------------------------------------------------------------
    stats = list(db.aql.execute("""
        LET totals = (
            FOR c IN candidates
                FILTER c.funding_channels != null
                   AND c.funding_channels.`aggregate` != null
                   AND c.funding_channels.`aggregate`.total_funding > 1000
                RETURN c.funding_channels.`aggregate`.total_funding
        )
        RETURN {
            count: LENGTH(totals),
            min: MIN(totals),
            median: PERCENTILE(totals, 50),
            p95: PERCENTILE(totals, 95),
            max: MAX(totals),
            sum: SUM(totals)
        }
    """))[0]
    print(f"\n[Distribution of total_funding (candidates with > $1K)]")
    print(f"  count:    {stats['count']:,}")
    print(f"  min:      {fmt_money(stats['min'])}")
    print(f"  median:   {fmt_money(stats['median'])}")
    print(f"  p95:      {fmt_money(stats['p95'])}")
    print(f"  max:      {fmt_money(stats['max'])}")
    print(f"  sum:      {fmt_money(stats['sum'])}")

    if stats['median'] and stats['max'] / stats['median'] > 100000:
        warnings.append(
            f"Max/median ratio = {stats['max']/stats['median']:.0f}× — extreme spread. "
            f"Possible algorithm artifact for top candidates."
        )

    # ------------------------------------------------------------------------
    # 5. Channel sum check: do the channels add up to total_funding?
    # ------------------------------------------------------------------------
    print(f"\n[Channel-sum consistency (sample 100 candidates)]")
    sample = list(db.aql.execute("""
        FOR c IN candidates
            FILTER c.funding_channels != null
               AND c.funding_channels.`aggregate` != null
               AND c.funding_channels.`aggregate`.total_funding > 0
            LIMIT 100
            LET agg = c.funding_channels.`aggregate`
            RETURN {
                total: agg.total_funding,
                direct: agg.direct_funding,
                ie_s: agg.ie_support,
                ie_o: agg.ie_oppose,
                unacc: (agg.unaccounted ? agg.unaccounted.total : 0)
            }
    """))
    discrepancies = 0
    for s in sample:
        # Per the model: total_funding = direct + ie_support + ie_oppose
        # unaccounted is part of direct (it's the residual within direct)
        expected = (s['direct'] or 0) + (s['ie_s'] or 0) + (s['ie_o'] or 0)
        actual = s['total'] or 0
        if expected > 0 and abs(actual - expected) / expected > 0.05:
            discrepancies += 1
    print(f"  candidates checked:       {len(sample)}")
    print(f"  with channel-sum mismatch: {discrepancies}")
    if discrepancies > 5:
        warnings.append(
            f"{discrepancies}/{len(sample)} candidates have channel sums that don't match "
            f"total_funding (>5% off). Possible double-counting between channels."
        )

    # ------------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------------
    print()
    print("=" * 70)
    if failures:
        print(f"  ❌ {len(failures)} FAILURES:")
        for f in failures:
            print(f"     - {f}")
    if warnings:
        print(f"  ⚠️  {len(warnings)} WARNINGS:")
        for w in warnings:
            print(f"     - {w}")
    if not failures and not warnings:
        print(f"  ✅ All checks passed.")
    print("=" * 70)

    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    main()
