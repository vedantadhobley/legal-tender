"""FEC Bulk Data Assets - Raw FEC file parsers (one file = one parser)"""

from .candidates import candidates_asset
from .committees import committees_asset
from .linkages import linkages_asset
from .candidate_summaries import candidate_summaries_asset
from .committee_summaries import committee_summaries_asset
from .pac_summaries import pac_summaries_asset
from .committee_transfers import committee_transfers_asset
from .independent_expenditures import independent_expenditures_asset

__all__ = [
    'candidates_asset',
    'committees_asset',
    'linkages_asset',
    'candidate_summaries_asset',
    'committee_summaries_asset',
    'pac_summaries_asset',
    'committee_transfers_asset',
    'independent_expenditures_asset',
]
