"""Graph Assets - Create vertices and edges for political money flow graph.

Vertex Collections:
- donors: Normalized individual donors (from indiv, filtered by threshold)
- employers: Unique employers (extracted from donors)

Edge Collections:
- contributed_to: donor → committee (individual donations)
- transferred_to: committee → committee (PAC-to-PAC transfers)
- affiliated_with: committee ↔ candidate (from ccl)
- employed_by: donor → employer (employment relationships)
- spent_on: committee → candidate (independent expenditures FOR/AGAINST)

All assets write to the 'aggregation' database.
"""

from src.assets.graph.donors import donors_asset
from src.assets.graph.employers import employers_asset
from src.assets.graph.contributed_to import contributed_to_asset
from src.assets.graph.transferred_to import transferred_to_asset
from src.assets.graph.affiliated_with import affiliated_with_asset
from src.assets.graph.employed_by import employed_by_asset
from src.assets.graph.spent_on import spent_on_asset
from src.assets.graph.political_money_graph import political_money_graph_asset

__all__ = [
    "donors_asset",
    "employers_asset",
    "contributed_to_asset",
    "transferred_to_asset",
    "affiliated_with_asset",
    "employed_by_asset",
    "spent_on_asset",
    "political_money_graph_asset",
]
