#!/usr/bin/env python3
"""View a candidate's funding-channels breakdown from the terminal.

Usage:
    python scripts/view_candidate.py "CRUZ"                  # substring match
    python scripts/view_candidate.py S2TX00312               # exact FEC CAND_ID
    python scripts/view_candidate.py "HARRIS" --cycle 2024   # one cycle instead of aggregate
    python scripts/view_candidate.py "TRUMP" --top 10        # change top-N for each table

Reads from `aggregation.candidates.<doc>.funding_channels` in ArangoDB.
No Dagster dependency.

Run inside the dev container:
    docker exec -w /workspace legal-tender-dev-webserver \\
        python3 /workspace/scripts/view_candidate.py "CRUZ"
"""

from __future__ import annotations

import argparse
import os
import sys
from typing import Any, Dict, List, Optional

from arango import ArangoClient
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.text import Text


def _arango_url() -> str:
    """Build the ArangoDB URL from env vars.

    Container env sets `ARANGO_HOST=legal-tender-dev-arango` and
    `ARANGO_PORT=8529` separately; outside the container the user may
    set `ARANGO_URL=http://host:8529` directly.
    """
    explicit = os.environ.get("ARANGO_URL")
    if explicit:
        return explicit
    host = os.environ.get("ARANGO_HOST", "legal-tender-dev-arango")
    port = os.environ.get("ARANGO_PORT", "8529")
    if "://" in host:
        return host
    return f"http://{host}:{port}"


ARANGO_USER = os.environ.get("ARANGO_USER", "root")
ARANGO_PASSWORD = os.environ.get("ARANGO_PASSWORD", "ltpass")
ARANGO_DB = "aggregation"


# ---------------------------------------------------------------------------
# formatting helpers
# ---------------------------------------------------------------------------

def fmt_money(n: Optional[float]) -> str:
    if n is None:
        return "—"
    n = float(n)
    if abs(n) >= 1e9:
        return f"${n / 1e9:.2f}B"
    if abs(n) >= 1e6:
        return f"${n / 1e6:.2f}M"
    if abs(n) >= 1e3:
        return f"${n / 1e3:.0f}K"
    return f"${n:.0f}"


def fmt_pct(n: Optional[float]) -> str:
    if n is None:
        return ""
    return f"{n:.1f}%"


def truncate(s: str, n: int) -> str:
    if not s:
        return ""
    return s if len(s) <= n else s[: n - 1] + "…"


# ---------------------------------------------------------------------------
# candidate lookup
# ---------------------------------------------------------------------------

def find_candidates(db, query: str) -> List[Dict[str, Any]]:
    """Match `query` against CAND_ID (exact) or CAND_NAME (substring,
    case-insensitive). Returns candidates that have funding_channels."""
    aql = """
    FOR c IN candidates
        FILTER c.CAND_ID == @q OR CONTAINS(UPPER(c.CAND_NAME), UPPER(@q))
        FILTER c.funding_channels.`aggregate`.total_funding > 0
        SORT c.funding_channels.`aggregate`.total_funding DESC
        LIMIT 25
        RETURN {
            id: c.CAND_ID,
            name: c.CAND_NAME,
            office: c.CAND_OFFICE,
            state: c.CAND_OFFICE_ST,
            district: c.CAND_OFFICE_DISTRICT,
            party: c.CAND_PTY_AFFILIATION,
            election_yr: c.CAND_ELECTION_YR,
            total: c.funding_channels.`aggregate`.total_funding,
        }
    """
    return list(db.aql.execute(aql, bind_vars={"q": query}))


def get_candidate_doc(db, cand_id: str) -> Optional[Dict[str, Any]]:
    res = list(db.aql.execute(
        "FOR c IN candidates FILTER c.CAND_ID == @id LIMIT 1 RETURN c",
        bind_vars={"id": cand_id},
    ))
    return res[0] if res else None


# ---------------------------------------------------------------------------
# rendering
# ---------------------------------------------------------------------------

def office_label(office: str, state: str, district: str) -> str:
    if office == "P":
        return "Presidential"
    if office == "S":
        return f"Senate / {state}"
    if office == "H":
        return f"House / {state}-{district or '??'}"
    return f"{office} / {state}"


def render_header(console: Console, cand: Dict[str, Any], section: Dict[str, Any], cycle: Optional[str]) -> None:
    name = cand.get("CAND_NAME", "?")
    cand_id = cand.get("CAND_ID", "?")
    office = office_label(
        cand.get("CAND_OFFICE", ""),
        cand.get("CAND_OFFICE_ST", ""),
        cand.get("CAND_OFFICE_DISTRICT", ""),
    )
    party = cand.get("CAND_PTY_AFFILIATION") or "?"
    yr = cand.get("CAND_ELECTION_YR") or ""

    total = section.get("total_funding", 0) or 0
    direct = section.get("direct_funding", 0) or 0
    ie_sup = section.get("ie_support", 0) or 0
    ie_opp = section.get("ie_oppose", 0) or 0

    scope = f"cycle {cycle}" if cycle else "all cycles (aggregate)"

    header = (
        f"[bold]{name}[/]  [dim]({cand_id})[/]\n"
        f"{office}  ·  party {party}  ·  election {yr}  ·  scope: {scope}\n"
        f"\n"
        f"Total funding: [bold green]{fmt_money(total)}[/]   "
        f"Direct: {fmt_money(direct)}   "
        f"IE+: {fmt_money(ie_sup)}   "
        f"IE-: {fmt_money(ie_opp)}"
    )
    console.print(Panel(header, border_style="bright_blue", expand=False))


def render_channels_summary(console: Console, agg: Dict[str, Any]) -> None:
    total = agg.get("total_funding", 0) or 0
    org_d = agg.get("organizational_direct", {}) or {}
    ie_sup = agg.get("ie_support", 0) or 0
    ie_opp = agg.get("ie_oppose", 0) or 0
    indiv = agg.get("individuals", {}) or {}
    unacct = agg.get("unaccounted", {}) or {}

    def pct(n): return (n / total * 100) if total else 0

    t = Table(title="Funding Channels", show_header=True, header_style="bold")
    t.add_column("Channel")
    t.add_column("Total", justify="right")
    t.add_column("% of total", justify="right")
    t.add_column("Note", style="dim")

    t.add_row("Ch1 Organizational Direct", fmt_money(org_d.get("total", 0)), fmt_pct(pct(org_d.get("total", 0))),
              "Corp/trade/labor/ideo/coop PACs, traced upstream")
    t.add_row("Ch2 IE Support", fmt_money(ie_sup), fmt_pct(pct(ie_sup)),
              "Outside money spent FOR candidate")
    t.add_row("Ch3 IE Oppose", fmt_money(ie_opp), fmt_pct(pct(ie_opp)),
              "Outside money spent AGAINST opponents")
    t.add_row("Ch4 Individuals", fmt_money(indiv.get("total", 0)), fmt_pct(indiv.get("pct", 0)),
              "Whale + grassroots donors")
    t.add_row("Ch5 Unaccounted", fmt_money(unacct.get("total", 0)), fmt_pct(unacct.get("pct", 0)),
              "Trace loss + data gaps (target <5%)")
    console.print(t)


def render_org_direct(console: Console, agg: Dict[str, Any], top_n: int) -> None:
    org_d = agg.get("organizational_direct", {}) or {}
    by_type = org_d.get("by_type", {}) or {}
    total = org_d.get("total", 0) or 0

    console.print()
    console.print(f"[bold]Channel 1 — Organizational Direct[/]  ({fmt_money(total)})")

    for bucket in ("corporation", "trade_association", "labor_union", "ideological", "cooperative"):
        b = by_type.get(bucket, {}) or {}
        b_total = b.get("total", 0) or 0
        top = b.get("top", []) or []
        if b_total < 1 and not top:
            continue
        t = Table(title=f"  {bucket}: {fmt_money(b_total)} ({fmt_pct(b.get('pct', 0))})",
                  show_header=True, header_style="bold", title_justify="left", title_style="bold cyan")
        t.add_column("Source")
        t.add_column("Amount", justify="right")
        for entry in top[:top_n]:
            t.add_row(truncate(entry.get("name", ""), 60), fmt_money(entry.get("amount", 0)))
        if not top:
            t.add_row("[dim](no entries)[/]", "")
        console.print(t)


def render_ie(console: Console, agg: Dict[str, Any], side: str, top_n: int) -> None:
    """side: 'support' or 'oppose'."""
    ie = (agg.get("ie") or {}).get(side, {}) or {}
    total = ie.get("total", 0) or 0
    sign = "+" if side == "support" else "-"

    console.print()
    label = "Support" if side == "support" else "Oppose"
    console.print(f"[bold]Channel {2 if side == 'support' else 3} — IE {label}[/]  "
                  f"({fmt_money(total)}, IE{sign})")

    if total < 1:
        console.print("  [dim](no IE spending in this channel)[/]")
        return

    # top_pacs (immediate spenders)
    top_pacs = ie.get("top_pacs", []) or []
    if top_pacs:
        t = Table(title="  Top spenders (immediate IE PACs)",
                  show_header=True, header_style="bold", title_justify="left", title_style="bold cyan")
        t.add_column("Spender")
        t.add_column("Amount", justify="right")
        for p in top_pacs[:top_n]:
            t.add_row(truncate(p.get("name", ""), 60), fmt_money(p.get("amount", 0)))
        console.print(t)

    # by_corporation (recursively traced upstream donors → corporate identities)
    by_corp = ie.get("by_corporation", []) or []
    if by_corp:
        t = Table(title="  Attributed to corporations (via donor → corp resolution)",
                  show_header=True, header_style="bold", title_justify="left", title_style="bold cyan")
        t.add_column("Corporation")
        t.add_column("Amount", justify="right")
        for c in by_corp[:top_n]:
            t.add_row(truncate(c.get("name", ""), 60), fmt_money(c.get("amount", 0)))
        console.print(t)

    # by_individual (recursively traced individual donors without corp link)
    by_indiv = ie.get("by_individual", []) or []
    if by_indiv:
        t = Table(title="  Attributed to individuals (no corporate link)",
                  show_header=True, header_style="bold", title_justify="left", title_style="bold cyan")
        t.add_column("Donor")
        t.add_column("Employer", style="dim")
        t.add_column("Amount", justify="right")
        for d in by_indiv[:top_n]:
            t.add_row(
                truncate(d.get("name", ""), 40),
                truncate(d.get("employer", "") or "—", 30),
                fmt_money(d.get("amount", 0)),
            )
        console.print(t)

    # by_pac (trace stuck here — passthrough PAC with zero receipts)
    by_pac = ie.get("by_pac", []) or []
    if by_pac:
        any_meaningful = any((p.get("amount", 0) or 0) > 1000 for p in by_pac)
        if any_meaningful:
            t = Table(title="  [yellow]Trace stuck at passthrough PAC[/] (zero/missing receipts upstream)",
                      show_header=True, header_style="bold", title_justify="left", title_style="bold yellow")
            t.add_column("PAC")
            t.add_column("Amount", justify="right")
            for p in by_pac[:top_n]:
                t.add_row(truncate(p.get("name", ""), 60), fmt_money(p.get("amount", 0)))
            console.print(t)


def render_individuals(console: Console, agg: Dict[str, Any], top_n: int) -> None:
    indiv = agg.get("individuals", {}) or {}
    total = indiv.get("total", 0) or 0
    whale = indiv.get("whale", {}) or {}
    grass = indiv.get("grassroots", {}) or {}
    # self_funded can be a scalar (legacy) or {total, pct} (current)
    sf_raw = indiv.get("self_funded", 0) or 0
    self_funded = sf_raw.get("total", 0) if isinstance(sf_raw, dict) else sf_raw

    console.print()
    console.print(f"[bold]Channel 4 — Individuals[/]  ({fmt_money(total)})")

    summary = Table(show_header=True, header_style="bold")
    summary.add_column("Sub-channel")
    summary.add_column("Total", justify="right")
    summary.add_column("% of total funding", justify="right")
    summary.add_row("Whale (max-out donors)", fmt_money(whale.get("total", 0)), fmt_pct(whale.get("pct", 0)))
    cc = whale.get("corporate_connected", {}) or {}
    summary.add_row("  · corp-connected", fmt_money(cc.get("total", 0)), fmt_pct(cc.get("pct", 0)))
    ind_w = whale.get("independent", {}) or {}
    summary.add_row("  · independent (no corp link)", fmt_money(ind_w.get("total", 0)), fmt_pct(ind_w.get("pct", 0)))
    summary.add_row("Grassroots (sub-max donors)", fmt_money(grass.get("total", 0)), fmt_pct(grass.get("pct", 0)))
    summary.add_row("  · direct", fmt_money(grass.get("direct", 0)), "")
    summary.add_row("  · upstream (via passthroughs)", fmt_money(grass.get("upstream", 0)), "")
    if self_funded:
        summary.add_row("Self-funded", fmt_money(self_funded), "")
    console.print(summary)

    # Whale corp-connected detail
    by_company = cc.get("by_company", []) or []
    if by_company:
        t = Table(title="  Whale → corporate connections (top, with via-donor provenance)",
                  show_header=True, header_style="bold", title_justify="left", title_style="bold cyan")
        t.add_column("Company")
        t.add_column("Total", justify="right")
        t.add_column("Top donors (via Wikidata / employer)", style="dim")
        for c in by_company[:top_n]:
            donors = c.get("top_donors", []) or []
            donor_str = ", ".join(f"{d.get('name', '?')} ({fmt_money(d.get('amount', 0))})" for d in donors[:3])
            t.add_row(
                truncate(c.get("company", ""), 35),
                fmt_money(c.get("amount", 0)),
                truncate(donor_str, 70),
            )
        console.print(t)

    # Whale independent
    top_w = ind_w.get("top", []) or []
    if top_w:
        t = Table(title="  Top independent whales (no corporate link resolved)",
                  show_header=True, header_style="bold", title_justify="left", title_style="bold cyan")
        t.add_column("Donor")
        t.add_column("Amount", justify="right")
        for d in top_w[:top_n]:
            t.add_row(truncate(d.get("name", ""), 50), fmt_money(d.get("amount", 0)))
        console.print(t)


def render_unaccounted(console: Console, agg: Dict[str, Any]) -> None:
    u = agg.get("unaccounted", {}) or {}
    total = u.get("total", 0) or 0
    pct = u.get("pct", 0) or 0
    receipts = u.get("cmte_total_receipts", 0) or 0
    accounted = u.get("total_accounted", 0) or 0
    breakdown = u.get("breakdown", {}) or {}

    console.print()
    color = "green" if pct < 5 else "yellow" if pct < 15 else "red"
    console.print(f"[bold]Channel 5 — Unaccounted[/]  ([{color}]{fmt_money(total)}[/], {fmt_pct(pct)})")

    t = Table(show_header=True, header_style="bold")
    t.add_column("Detail")
    t.add_column("Amount", justify="right")
    t.add_row("Committee total receipts (FEC)", fmt_money(receipts))
    t.add_row("Total accounted by our trace", fmt_money(accounted))
    t.add_row("Residual gap", fmt_money(total))
    if breakdown:
        for k, v in breakdown.items():
            t.add_row(f"  · {k}", fmt_money(v))
    console.print(t)


def render_by_organization(console: Console, agg: Dict[str, Any], top_n: int) -> None:
    by_org = agg.get("by_organization", []) or []
    if not by_org:
        return

    console.print()
    console.print(f"[bold]by_organization cross-cut[/] (top {top_n} across all channels, sorted by total_pro)")

    t = Table(show_header=True, header_style="bold")
    t.add_column("#", justify="right", style="dim")
    t.add_column("Organization")
    t.add_column("Total pro", justify="right", style="bold")
    t.add_column("PAC direct", justify="right")
    t.add_column("Employees", justify="right")
    t.add_column("IE+", justify="right")
    t.add_column("IE-", justify="right", style="dim")
    t.add_column("Top via_donors", style="dim")

    for i, o in enumerate(by_org[:top_n], 1):
        vd = o.get("via_donors", []) or []
        vd_str = ", ".join(d.get("name", "?") for d in vd[:3]) if vd else ""
        t.add_row(
            str(i),
            truncate(o.get("name", ""), 38),
            fmt_money(o.get("total_pro", 0)),
            fmt_money(o.get("direct_pac", 0)),
            fmt_money(o.get("direct_employees", 0)),
            fmt_money(o.get("ie_support", 0)),
            fmt_money(o.get("ie_oppose", 0)),
            truncate(vd_str, 35),
        )
    console.print(t)


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------

def pick_section(fc: Dict[str, Any], cycle: Optional[str]) -> Optional[Dict[str, Any]]:
    if cycle is None:
        return fc.get("aggregate")
    by_cycle = fc.get("by_cycle", {}) or {}
    return by_cycle.get(cycle)


def main(argv: List[str]) -> int:
    ap = argparse.ArgumentParser(description="View one candidate's funding-channels output.")
    ap.add_argument("query", help="CAND_ID (exact) or substring of CAND_NAME (case-insensitive)")
    ap.add_argument("--cycle", default=None, help="Show per-cycle view instead of aggregate (e.g. 2024)")
    ap.add_argument("--top", type=int, default=10, help="Top-N rows per table (default 10)")
    args = ap.parse_args(argv)

    console = Console()

    client = ArangoClient(hosts=_arango_url())
    db = client.db(ARANGO_DB, username=ARANGO_USER, password=ARANGO_PASSWORD)

    matches = find_candidates(db, args.query)
    if not matches:
        console.print(f"[red]No candidate matched {args.query!r} (or no funding_channels yet).[/]")
        return 2
    if len(matches) > 1 and matches[0]["id"] != args.query:
        console.print(f"[yellow]{len(matches)} candidates match {args.query!r}. Disambiguate by CAND_ID:[/]")
        t = Table(show_header=True, header_style="bold")
        t.add_column("CAND_ID")
        t.add_column("Name")
        t.add_column("Office")
        t.add_column("Party")
        t.add_column("Yr")
        t.add_column("Total", justify="right")
        for m in matches:
            t.add_row(
                m["id"], truncate(m["name"], 38),
                office_label(m["office"], m["state"], m["district"]),
                m.get("party") or "?", str(m.get("election_yr") or ""),
                fmt_money(m["total"]),
            )
        console.print(t)
        return 0

    cand_id = matches[0]["id"]
    cand = get_candidate_doc(db, cand_id)
    if not cand:
        console.print(f"[red]Candidate {cand_id} not found.[/]")
        return 2

    fc = cand.get("funding_channels") or {}
    section = pick_section(fc, args.cycle)
    if not section:
        available = ", ".join(sorted((fc.get("by_cycle") or {}).keys()))
        console.print(f"[red]No data for cycle {args.cycle!r}. Available cycles: {available or '(none)'}[/]")
        return 2

    render_header(console, cand, section, args.cycle)
    render_channels_summary(console, section)
    render_org_direct(console, section, args.top)
    render_ie(console, section, "support", args.top)
    render_ie(console, section, "oppose", args.top)
    render_individuals(console, section, args.top)
    render_unaccounted(console, section)
    render_by_organization(console, section, args.top)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
