"""LT (Legal Tender) enriched data assets.

These assets process FEC data per-cycle and create enriched views
filtered to tracked members with full denormalized context.

Philosophy:
- Input: fec_{cycle} databases (raw FEC data)
- Output: lt_{cycle} databases (enriched, filtered to tracked members)
- Same pattern as FEC assets: no year suffix, use cycles config

Available Assets:
- lt_itpas2: Itemized transactions (ALL types including 24A/24E independent expenditures) for tracked members
- lt_oppexp: Operating expenditures for tracked member committees
- lt_candidate_financials: Per-cycle aggregation of ALL money TO each candidate
- lt_donor_financials: Per-cycle aggregation of ALL money FROM each committee
- lt_webl: FEC candidate summaries (cycle-specific) filtered to tracked members
- lt_weball: FEC all-candidate summaries (includes future cycles) filtered to tracked members
- lt_webk: FEC committee/PAC summaries filtered to committees linked to tracked members
"""

from src.assets.lt.lt_itpas2 import lt_itpas2_asset
from src.assets.lt.lt_oppexp import lt_oppexp_asset
from src.assets.lt.lt_candidate_financials import lt_candidate_financials_asset
from src.assets.lt.lt_donor_financials import lt_donor_financials_asset
from src.assets.lt.lt_webl import lt_webl_asset
from src.assets.lt.lt_weball import lt_weball_asset
from src.assets.lt.lt_webk import lt_webk_asset

__all__ = [
    "lt_itpas2_asset",
    "lt_oppexp_asset",
    "lt_candidate_financials_asset",
    "lt_donor_financials_asset",
    "lt_webl_asset",
    "lt_weball_asset",
    "lt_webk_asset",
]

