"""Enrichment assets - Per-cycle enriched data filtered to tracked members.

DEPRECATED (moved to src/assets/deprecated/):
- enriched_webl_asset (derive from raw committee data)
- enriched_weball_asset (derive from raw candidate data)
- enriched_webk_asset (derive from raw PAC data)
"""

from .enriched_pas2 import enriched_pas2_asset
from .enriched_candidate_financials import enriched_candidate_financials_asset
from .enriched_donor_financials import enriched_donor_financials_asset
from .enriched_committee_funding import enriched_committee_funding_asset

__all__ = [
    "enriched_pas2_asset",
    "enriched_candidate_financials_asset",
    "enriched_donor_financials_asset",
    "enriched_committee_funding_asset",
]
