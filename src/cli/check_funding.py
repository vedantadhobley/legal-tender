"""Quick check of pre-computed funding channels for validation."""
import sys
from arango import ArangoClient

client = ArangoClient(hosts='http://legal-tender-dev-arango:8529')
db = client.db('aggregation', username='root', password='ltpass')

query = '''
FOR c IN candidates
    FILTER c.funding_channels != null
    LET fc = c.funding_channels.`aggregate`
    FILTER fc.total_funding > 0
    SORT fc.total_funding DESC
    LIMIT @limit
    RETURN {
        name: c.CAND_NAME,
        id: c.CAND_ID,
        party: c.CAND_PTY_AFFILIATION,
        office: c.CAND_OFFICE,
        total: fc.total_funding,
        direct: fc.direct_funding,
        receipts: fc.unaccounted.cmte_total_receipts,
        org: fc.organizational_direct.total,
        org_by_type: fc.organizational_direct.by_type,
        ie_sup: fc.ie.support.total,
        ie_opp: fc.ie.oppose.total,
        indiv: fc.individuals.total,
        whale: fc.individuals.whale.total,
        whale_corp: fc.individuals.whale.corporate_connected.total,
        whale_indep: fc.individuals.whale.independent.total,
        grass: fc.individuals.grassroots.total,
        grass_direct: fc.individuals.grassroots.direct,
        grass_upstream: fc.individuals.grassroots.upstream,
        unacc: fc.unaccounted.total,
        cycles: c.funding_channels.cycles_available
    }
'''

limit = int(sys.argv[1]) if len(sys.argv) > 1 else 15
results = list(db.aql.execute(query, bind_vars={'limit': limit}))

for d in results:
    u_pct = (d['unacc'] / d['receipts'] * 100) if d['receipts'] else 0
    i_pct = (d['indiv'] / d['total'] * 100) if d['total'] else 0
    o_pct = (d['org'] / d['total'] * 100) if d['total'] else 0
    ie_pct = (d['ie_sup'] / d['total'] * 100) if d['total'] else 0
    w_pct = (d['whale'] / d['total'] * 100) if d['total'] else 0
    g_pct = (d['grass'] / d['total'] * 100) if d['total'] else 0

    print(f"{'='*70}")
    print(f"  {d['name']} ({d['id']}) [{d['party']}] {d['office']}")
    print(f"  Cycles: {d['cycles']}")
    print(f"{'='*70}")
    print(f"  Total Funding:    ${d['total']:>15,.0f}")
    print(f"  Cmte Receipts:    ${d['receipts']:>15,.0f}")
    print(f"  {'─'*50}")
    print(f"  Ch1 Org Direct:   ${d['org']:>15,.0f}  ({o_pct:.1f}%)")
    if d.get('org_by_type'):
        for t in ['corporation','trade_association','labor_union','ideological','cooperative']:
            v = d['org_by_type'].get(t, {})
            amt = v.get('total', 0) if isinstance(v, dict) else 0
            if amt > 0:
                print(f"      {t:<20} ${amt:>12,.0f}")
    print(f"  Ch2 IE Support:   ${d['ie_sup']:>15,.0f}  ({ie_pct:.1f}%)")
    print(f"  Ch3 IE Oppose:    ${d['ie_opp']:>15,.0f}")
    print(f"  Ch4 Individuals:  ${d['indiv']:>15,.0f}  ({i_pct:.1f}%)")
    print(f"      Whale:        ${d['whale']:>15,.0f}  ({w_pct:.1f}%)")
    print(f"        Corp-conn:  ${d.get('whale_corp',0):>15,.0f}")
    print(f"        Independent:${d.get('whale_indep',0):>15,.0f}")
    print(f"      Grassroots:   ${d['grass']:>15,.0f}  ({g_pct:.1f}%)")
    print(f"        Direct:     ${d.get('grass_direct',0):>15,.0f}")
    print(f"        Upstream:   ${d.get('grass_upstream',0):>15,.0f}")
    print(f"  Ch5 Unaccounted:  ${d['unacc']:>15,.0f}  ({u_pct:.1f}% of receipts)")
    print()
