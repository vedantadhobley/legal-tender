"""Data models for Legal Tender graph database.

This module defines Pydantic models for all vertices and edges
in the political_money_flow graph.

Usage:
    from src.models import Donor, Committee, Candidate, ContributedTo
    
    # Create a donor vertex
    donor = Donor(
        key="abc123def456",
        canonical_name="MUSK, ELON",
        canonical_employer="TESLA INC",
        total_amount=55000.0,
        transaction_count=5,
        cycles=["2024"]
    )
    
    # Convert to ArangoDB document
    doc = donor.to_arango_doc()
"""

from .vertices import (
    Donor,
    Employer,
    Committee,
    Candidate,
)

from .edges import (
    ContributedTo,
    TransferredTo,
    AffiliatedWith,
    EmployedBy,
)

__all__ = [
    # Vertices (entities)
    "Donor",
    "Employer", 
    "Committee",
    "Candidate",
    
    # Edges (relationships)
    "ContributedTo",
    "TransferredTo",
    "AffiliatedWith",
    "EmployedBy",
]
