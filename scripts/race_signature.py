"""Surface donor-network signatures for an entire race at once.

Given a state, district, year, party — list every candidate in that
race with their funding totals, whale/grassroots mix, data-quality
coverage, and top-3 destinations of their whale donor pool.

Pairs with `donor_network_overlap.py` (which drills into one candidate
deeply). Use this script when you want a field-wide comparison; use
the other when you want a deep portfolio for a specific candidate.

USAGE:
    python scripts/race_signature.py --state NJ --district 12 --year 2026 --party DEM
    python scripts/race_signature.py --state TX --year 2026 --office S         # Senate
    python scripts/race_signature.py --state US --office P --year 2024         # Presidential

DESIGN NOTES:
  - "Office" follows FEC convention: H (House) / S (Senate) / P (Pres).
  - District field is "00" for Senate and Presidential; --district 12
    only makes sense for House races.
  - Skips candidates below `--min-total` dollars (default $1000) to
    keep the output focused.
  - Top-3 destinations of each candidate's whale pool are surfaced
    algorithmically — no pre-defined "network" lookup. Reader names
    the clusters. See `docs/data-quality.md` for caveats.
"""

import argparse
import os
import sys
from typing import Dict, List, Optional

from arango import ArangoClient


def _connect():
    host = os.environ.get("LT_ARANGO_HOST", "legal-tender-dev-arango")
    port = os.environ.get("LT_ARANGO_PORT", "8529")
    client = ArangoClient(hosts=f"http://{host}:{port}")
    return client.db("aggregation", username="root", password="ltpass")


def get_field(db, state: str, district: str, year: int, party: Optional[str],
              office: Optional[str]) -> List[Dict]:
    """Return all candidates in the race, with funding_channels fields
    flattened for easy formatting."""
    filters = ["c.CAND_OFFICE_ST == @state",
               "c.CAND_ELECTION_YR == @year",
               "c.funding_channels != null"]
    bind = {"state": state, "year": year}
    if district:
        filters.append("c.CAND_OFFICE_DISTRICT == @district")
        bind["district"] = district
    if office:
        filters.append("c.CAND_OFFICE == @office")
        bind["office"] = office
    if party:
        filters.append("c.CAND_PTY_AFFILIATION == @party")
        bind["party"] = party
    filter_clause = "\n    FILTER ".join([""] + filters)

    return list(db.aql.execute(f"""
      FOR c IN candidates{filter_clause}
        LET fc = c.funding_channels
        LET agg = fc["aggregate"]
        SORT agg.total_funding DESC
        RETURN {{
          id: c._key, name: c.CAND_NAME, ici: c.CAND_ICI,
          office: c.CAND_OFFICE, st: c.CAND_OFFICE_ST,
          district: c.CAND_OFFICE_DISTRICT, party: c.CAND_PTY_AFFILIATION,
          total: agg.total_funding,
          whale: agg.individuals.whale.total,
          grass: agg.individuals.grassroots.total,
          org: agg.organizational_direct.total,
          ie_sup: agg.ie_support, ie_opp: agg.ie_oppose,
          coverage: agg.individuals.data_quality.detail_coverage,
          source: agg.individuals.data_quality.primary_source
        }}
    """, bind_vars=bind))


def top_destinations_for(db, cand_id: str, top_k: int = 3) -> List[Dict]:
    """Top-K committees the candidate's whale donor pool also funds."""
    cmtes = list(db.aql.execute(
        """
        FOR v, e IN INBOUND CONCAT('candidates/', @cid) affiliated_with
          RETURN DISTINCT v._key
        """,
        bind_vars={"cid": cand_id},
    ))
    if not cmtes:
        return []
    pool = list(db.aql.execute(
        """
        FOR e IN contributed_to FILTER e._to IN @cmtes
          RETURN DISTINCT PARSE_IDENTIFIER(e._from).key
        """,
        bind_vars={"cmtes": [f"committees/{x}" for x in cmtes]},
    ))
    if not pool:
        return []
    return list(db.aql.execute(
        """
        FOR e IN contributed_to
          FILTER PARSE_IDENTIFIER(e._from).key IN @keys
          FILTER e._to NOT IN @excl
          LET cmte = DOCUMENT(e._to)
          COLLECT name = cmte.CMTE_NM, typ = cmte.terminal_type
          AGGREGATE tot = SUM(e.total_amount), donors_n = COUNT_DISTINCT(e._from)
          SORT tot DESC LIMIT @k
          RETURN {n: name, t: typ, tot: tot, d: donors_n}
        """,
        bind_vars={
            "keys": pool,
            "excl": [f"committees/{x}" for x in cmtes],
            "k": top_k,
        },
    )), len(pool)


def pct(num: float, denom: float) -> float:
    return (100 * num / denom) if denom else 0


def main():
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--state", required=True, help="FEC state code (e.g. NJ, TX, US)")
    p.add_argument("--district", default=None,
                   help='House district (e.g. "12"). Omit for Senate/Presidential.')
    p.add_argument("--year", type=int, required=True, help="Election year (e.g. 2026)")
    p.add_argument("--party", default=None, help="Filter by party (e.g. DEM, REP)")
    p.add_argument("--office", default=None,
                   help="Office (H/S/P). Inferred if --district is set (H).")
    p.add_argument("--min-total", type=float, default=1000.0,
                   help="Hide candidates raising less than this (default $1000).")
    p.add_argument("--top-destinations", type=int, default=3,
                   help="Top-K destinations of each candidate's pool to surface.")
    args = p.parse_args()

    if args.district and not args.office:
        args.office = "H"

    db = _connect()
    field = get_field(db, args.state, args.district, args.year, args.party, args.office)
    if not field:
        print(f"No candidates found matching the filters.", file=sys.stderr)
        sys.exit(1)

    # Race label for the header
    if args.office == "H":
        race = f"{args.state}-{args.district or '??'} House"
    elif args.office == "S":
        race = f"{args.state} Senate"
    elif args.office == "P":
        race = f"Presidential ({args.state})"
    else:
        race = f"{args.state} {args.district or ''}".strip()
    party_label = f" {args.party}" if args.party else ""

    print()
    print(f"{race} {args.year}{party_label} — donor signatures")
    print("=" * 110)
    print(f"  {'Candidate':<28} {'Total':>11} {'Whale%':>7} {'Grass%':>7} "
          f"{'PAC$':>8} {'IE+$':>8} {'Cov':>5}  {'Source':<13}")
    print("  " + "-" * 100)
    for c in field:
        total = c["total"] or 0
        if total < args.min_total:
            continue
        wp = pct(c.get("whale") or 0, total)
        gp = pct(c.get("grass") or 0, total)
        cov = c.get("coverage")
        cov_s = f"{cov*100:.0f}%" if cov is not None else "?"
        print(f"  {c['name'][:27]:<28} ${total:>10,.0f} "
              f"{wp:>6.0f}% {gp:>6.0f}% ${(c.get('org') or 0):>6,.0f} "
              f"${(c.get('ie_sup') or 0):>6,.0f}   {cov_s:>4}  "
              f"{(c.get('source') or '?'):<13}")

    print()
    print(f"Top-{args.top_destinations} destinations of each candidate's whale pool")
    print(f"(where their max-out donors collectively also give)")
    print("-" * 110)
    for c in field:
        if (c["total"] or 0) < args.min_total:
            continue
        result = top_destinations_for(db, c["id"], args.top_destinations)
        if not result:
            print(f"  {c['name'][:32]:<32}  (no graph donors — likely fec_summary only)")
            continue
        dests, pool_size = result
        if not dests:
            print(f"  {c['name'][:32]:<32}  pool={pool_size:>3}  (no destinations resolved)")
            continue
        summary = " | ".join(f"{(d['n'] or '?')[:32]} ${d['tot']:,.0f}" for d in dests)
        print(f"  {c['name'][:30]:<30}  pool={pool_size:>3}  {summary}")

    print()
    print("# Notes")
    print("  - Whale% + Grass% may not sum to 100; difference is org/IE/self-funded.")
    print("  - 'Cov' is donor_detail_coverage. When < 100%, the whale/grassroots split")
    print("    is partly inferred from FEC summary totals rather than itemized records.")
    print("    See docs/data-quality.md.")
    print("  - 'Top destinations' are surfaced algorithmically. Pattern-naming is the")
    print("    reader's job — see clusters of related committees as ideological signal.")


if __name__ == "__main__":
    main()
