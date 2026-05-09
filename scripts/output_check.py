#!/usr/bin/env python3
"""Output spot-check: pretty-print a candidate's funding_channels for
visual inspection.

Different from validation_report.py — that compares aggregate magnitudes
against FEC weball. This script lets a human eyeball the actual data
shape: do the top organizations look right? Are the IE Super PACs
plausibly the actual ones? Does the whale corporate-connection list
contain real names? Catches issues that bulk validation can't.

Usage:
  docker exec legal-tender-dev-webserver python3 /workspace/scripts/output_check.py [name_pattern...]

If no arguments given, runs against a default set of well-known
candidates across office types and party.
"""
import os
import sys
from arango import ArangoClient


DEFAULT_TARGETS = [
    "CRUZ, RAFAEL",        # Senate, R, incumbent — well-trafficked
    "TRUMP, DONALD J.",     # Pres, R
    "HARRIS, KAMALA",       # Pres, D, 2024
    "SANDERS, BERNARD",     # Pres+Senate, D
    "COLEMAN, BONNIE WATSON",  # House, D, NJ-12 — BWC, our canonical small-cmte baseline
    "PELOSI, NANCY",        # House, D — leadership PAC heavy
    "SCALISE, STEVE",       # House, R — leadership PAC heavy
    "BLOOMBERG, MICHAEL",   # Pres, self-funded
]


def fmt_money(n):
    if n is None or n == 0:
        return "—"
    if abs(n) >= 1e9:
        return f"${n/1e9:.2f}B"
    if abs(n) >= 1e6:
        return f"${n/1e6:.2f}M"
    if abs(n) >= 1e3:
        return f"${n/1e3:.0f}K"
    return f"${n:.0f}"


def fmt_pct(p):
    if p is None or p == 0:
        return "—"
    return f"{p:.1f}%"


def print_card(c):
    fc = c.get('funding_channels')
    if not fc:
        print(f"  ❌ {c['_key']}  {c.get('CAND_NAME', '?')[:40]}  — no funding_channels")
        return

    name = c.get('CAND_NAME', '?')
    office = c.get('CAND_OFFICE', '?')
    st = c.get('CAND_OFFICE_ST', '--')
    party = c.get('CAND_PTY_AFFILIATION', '--')
    cycles = fc.get('cycles_available', [])
    agg = fc.get('aggregate', {}) or {}

    print()
    print("=" * 78)
    print(f"  {name}  ({c['_key']})  [{office}/{st}, {party}]   cycles: {cycles}")
    print("=" * 78)

    total = agg.get('total_funding') or 0
    direct = agg.get('direct_funding') or 0
    ie_s = agg.get('ie_support') or 0
    ie_o = agg.get('ie_oppose') or 0

    print(f"  Aggregate: total={fmt_money(total):>10}  direct={fmt_money(direct):>10}  IE_S={fmt_money(ie_s):>9}  IE_O={fmt_money(ie_o):>9}")

    # Channel %
    org = (agg.get('organizational_direct') or {})
    indiv = (agg.get('individuals') or {})
    unacc = (agg.get('unaccounted') or {})

    print(f"\n  Channel mix (% of total_funding):")
    print(f"    Org Direct:     {fmt_money(org.get('total')):>10}  {fmt_pct(org.get('pct')):>7}")
    print(f"    IE Support:     {fmt_money(ie_s):>10}  {fmt_pct(((agg.get('ie') or {}).get('support') or {}).get('pct')):>7}")
    print(f"    IE Oppose:      {fmt_money(ie_o):>10}  (separate; against opponents)")
    print(f"    Individuals:    {fmt_money(indiv.get('total')):>10}  {fmt_pct(indiv.get('pct')):>7}")
    whale = indiv.get('whale') or {}
    grass = indiv.get('grassroots') or {}
    self_f = indiv.get('self_funded') or {}
    print(f"      ├─ Whale:     {fmt_money(whale.get('total')):>10}  {fmt_pct(whale.get('pct')):>7}")
    print(f"      ├─ Grassroots:{fmt_money(grass.get('total')):>10}  {fmt_pct(grass.get('pct')):>7}")
    print(f"      └─ Self-fund: {fmt_money(self_f.get('total')):>10}  {fmt_pct(self_f.get('pct')):>7}")
    print(f"    Unaccounted:    {fmt_money(unacc.get('total')):>10}  {fmt_pct(unacc.get('pct')):>7}")

    # Top organizational direct (corp / trade / labor / ideo)
    by_type = (org.get('by_type') or {})
    for tname in ('corporation', 'trade_association', 'labor_union', 'ideological', 'cooperative'):
        tdata = by_type.get(tname) or {}
        top = tdata.get('top') or []
        ttotal = tdata.get('total') or 0
        if ttotal == 0:
            continue
        print(f"\n  Org Direct → {tname} ({fmt_money(ttotal)}):")
        for item in top[:5]:
            print(f"    {fmt_money(item.get('amount')):>10}  {item.get('name', '')[:60]}")

    # Whale corporate connections
    cc = (whale.get('corporate_connected') or {})
    by_company = cc.get('by_company') or []
    if by_company:
        print(f"\n  Whale corporate-connected ({fmt_money(cc.get('total'))}):")
        for item in by_company[:8]:
            company = item.get('company') or item.get('name', '')
            print(f"    {fmt_money(item.get('amount')):>10}  {company[:50]}")
            for d in (item.get('top_donors') or [])[:3]:
                print(f"      — {fmt_money(d.get('amount')):>8}  {d.get('name', '')[:45]}")

    # Whale independent (top names)
    ind = (whale.get('independent') or {})
    ind_top = ind.get('top') or []
    if ind_top:
        print(f"\n  Whale independent (top, {fmt_money(ind.get('total'))} total):")
        for item in ind_top[:8]:
            print(f"    {fmt_money(item.get('amount')):>10}  {item.get('name', '')[:60]}")

    # IE Support — top PACs, top corporations
    ie_data = agg.get('ie') or {}
    ie_support_data = ie_data.get('support') or {}
    if ie_support_data.get('total', 0) > 0:
        print(f"\n  IE Support (${fmt_money(ie_support_data.get('total'))}):")
        for item in (ie_support_data.get('top_pacs') or [])[:5]:
            print(f"    {fmt_money(item.get('amount')):>10}  {item.get('name', '')[:60]}  [PAC]")
        for item in (ie_support_data.get('by_corporation') or [])[:5]:
            print(f"    {fmt_money(item.get('amount')):>10}  {item.get('name', '')[:60]}  [corp upstream]")

    # IE Oppose — same shape
    ie_oppose_data = ie_data.get('oppose') or {}
    if ie_oppose_data.get('total', 0) > 0:
        print(f"\n  IE Oppose (${fmt_money(ie_oppose_data.get('total'))}):")
        for item in (ie_oppose_data.get('top_pacs') or [])[:5]:
            print(f"    {fmt_money(item.get('amount')):>10}  {item.get('name', '')[:60]}  [PAC]")

    # Unaccounted breakdown
    if (unacc.get('total') or 0) > 0:
        print(f"\n  Unaccounted: {fmt_money(unacc.get('total'))} ({fmt_pct(unacc.get('pct'))})")
        b = unacc.get('breakdown') or {}
        if b:
            print(f"    cmte_total_receipts: {fmt_money(unacc.get('cmte_total_receipts'))}")
            print(f"    total_accounted:     {fmt_money(unacc.get('total_accounted'))}")

    # by_organization cross-cut (top combined)
    by_org = fc.get('by_organization') or []
    if by_org:
        print(f"\n  by_organization cross-cut (top combined PAC + employees + IE):")
        print(f"    {'amount':>10}  {'org name':<40}  {'PAC':>8} {'emp':>8} {'IE+':>8} {'IE-':>8}")
        for item in by_org[:8]:
            print(f"    {fmt_money(item.get('total_pro') or item.get('total', 0)):>10}  "
                  f"{(item.get('name') or '')[:40]:<40}  "
                  f"{fmt_money(item.get('direct_pac')):>8} "
                  f"{fmt_money(item.get('direct_employees')):>8} "
                  f"{fmt_money(item.get('ie_support')):>8} "
                  f"{fmt_money(item.get('ie_oppose')):>8}")
            # When the org's totals come via founder/employee donations
            # (not corporate-PAC money), surface the donor names that
            # produced the attribution. Prevents readers from mistaking
            # "Pan Am Railways $20M IE+" as corporate spending when it's
            # actually Timothy Mellon's personal donations rolled up.
            for vd in (item.get('via_donors') or [])[:3]:
                bits = []
                if vd.get('ie_support', 0) >= 1000:
                    bits.append(f"IE+ {fmt_money(vd['ie_support'])}")
                if vd.get('ie_oppose', 0) >= 1000:
                    bits.append(f"IE- {fmt_money(vd['ie_oppose'])}")
                if vd.get('employees', 0) >= 1000:
                    bits.append(f"emp {fmt_money(vd['employees'])}")
                if bits:
                    print(f"    {'':>10}    └─ via {vd['name'][:35]:<35}  ({', '.join(bits)})")


def main():
    targets = sys.argv[1:] if len(sys.argv) > 1 else DEFAULT_TARGETS

    client = ArangoClient(hosts=os.environ.get("ARANGO_URL", "http://legal-tender-dev-arango:8529"))
    db = client.db("aggregation", username="root", password="ltpass")

    for pattern in targets:
        rows = list(db.aql.execute("""
            FOR c IN candidates
                FILTER UPPER(c.CAND_NAME) LIKE UPPER(@pat)
                FILTER c.funding_channels != null
                SORT c.funding_channels.total_funding DESC
                LIMIT 1
                RETURN c
        """, bind_vars={"pat": f"%{pattern}%"}))
        if not rows:
            print(f"\n  (no match for: {pattern!r})")
            continue
        print_card(rows[0])

    print()


if __name__ == "__main__":
    main()
