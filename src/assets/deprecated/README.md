"""Deprecated Assets - Summary Files

These parsers have been deprecated in favor of deriving summaries from raw transactional data.

REASON FOR DEPRECATION (November 2025):
- Summary files (weball, webl, webk) were cluttering the data pipeline
- They provide pre-aggregated data that can be calculated from raw transactions
- Focus shifted to 6 core files: cn, cm, ccl, pas2, oth, indiv
- Summaries can be derived in enrichment/aggregation layers when needed

FILES MOVED HERE:

FEC Raw Parsers (src/assets/fec/):
- weball.py (candidate summary files - weball.zip)
- webl.py (committee summary files - webl.zip)
- webk.py (PAC summary files - webk.zip)

Enrichment Assets (src/assets/enrichment/):
- enriched_weball.py (enriched candidate summaries)
- enriched_webl.py (enriched committee summaries)
- enriched_webk.py (enriched PAC summaries)

Aggregation Assets (src/assets/aggregation/):
- candidate_summaries.py (cross-cycle candidate summaries)
- committee_summaries.py (cross-cycle committee summaries)

These files are preserved for reference but should not be used in active pipelines.
If summaries are needed in the future, they should be derived from raw data:
- Candidate summaries: derive from cn + itpas2 + itoth + itcont
- Committee summaries: derive from cm + itpas2 + itoth + itcont
"""
