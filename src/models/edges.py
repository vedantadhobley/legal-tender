"""Edge (relationship) models for the political money flow graph.

In a graph database:
- **Vertices** are the entities (donors, committees, candidates)
- **Edges** are the relationships between entities

This module defines the EDGE models:
- ContributedTo: Donor → Committee (individual donations)
- TransferredTo: Committee → Committee (PAC-to-PAC transfers)
- AffiliatedWith: Committee → Candidate (official FEC linkages)
- EmployedBy: Donor → Employer (employment relationships)

ArangoDB Edge Requirements:
- Every edge needs `_from` and `_to` fields
- Format: "{collection_name}/{_key}" (e.g., "donors/abc123")
- Edges can have their own properties (amount, date, etc.)
- Edges are stored in EDGE collections (different from vertex collections)

The edge direction matters for traversal queries:
- OUTBOUND: Follow edges in the direction of `_from → _to`
- INBOUND: Follow edges backwards `_to → _from`
- ANY: Follow edges in both directions
"""

from datetime import datetime
from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field, computed_field
from hashlib import sha256


# =============================================================================
# CONTRIBUTED_TO EDGE (Donor → Committee)
# =============================================================================

class ContributedTo(BaseModel):
    """Individual donation from a donor to a committee.
    
    This is the PRIMARY ENTRY POINT for money into the political system.
    FEC requires itemization of individual contributions > $200.
    
    We aggregate donations by (donor, committee, cycle) to reduce edge count
    while preserving total amounts and transaction counts.
    
    Data Source: FEC indiv.txt (individual contributions)
    
    Edge Direction: donors/{donor_key} → committees/{CMTE_ID}
    
    Example:
        {
            "_from": "donors/a1b2c3d4e5f6g7h8",
            "_to": "committees/C00873398",
            "total_amount": 55000.0,
            "transaction_count": 5,
            "cycles": ["2024"],
            "first_date": "2024-01-15",
            "last_date": "2024-10-01"
        }
    
    Query Example (AQL):
        // Find all committees a donor contributed to
        FOR edge IN contributed_to
            FILTER edge._from == "donors/a1b2c3d4e5f6g7h8"
            RETURN edge
    """
    
    # Edge endpoints (required by ArangoDB)
    from_donor: str = Field(..., description="Source vertex: donors/{key}")
    to_committee: str = Field(..., description="Target vertex: committees/{CMTE_ID}")
    
    # Aggregated donation data
    total_amount: float = Field(..., description="Sum of all donations ($)")
    transaction_count: int = Field(default=1, description="Number of individual donations")
    
    # Temporal information
    cycles: List[str] = Field(default_factory=list, description="Election cycles")
    first_date: Optional[str] = Field(None, description="First donation date (YYYY-MM-DD)")
    last_date: Optional[str] = Field(None, description="Most recent donation date")
    
    # Metadata
    updated_at: datetime = Field(default_factory=datetime.now)
    
    @computed_field
    @property
    def _from(self) -> str:
        return self.from_donor
    
    @computed_field
    @property
    def _to(self) -> str:
        return self.to_committee
    
    @computed_field
    @property
    def _key(self) -> str:
        """Generate unique edge key from donor and committee."""
        combined = f"{self.from_donor}|{self.to_committee}"
        return sha256(combined.encode()).hexdigest()[:16]
    
    def to_arango_doc(self) -> Dict[str, Any]:
        return {
            "_key": self._key,
            "_from": self.from_donor,
            "_to": self.to_committee,
            "total_amount": self.total_amount,
            "transaction_count": self.transaction_count,
            "cycles": self.cycles,
            "first_date": self.first_date,
            "last_date": self.last_date,
            "updated_at": self.updated_at.isoformat(),
        }


# =============================================================================
# TRANSFERRED_TO EDGE (Committee → Committee)
# =============================================================================

class TransferredTo(BaseModel):
    """Committee-to-committee money transfer.
    
    THIS IS THE "DARK MONEY" PATH. Money flows between PACs, often through
    multiple intermediaries, obscuring the original source.
    
    Transfer Types (from FEC TRANSACTION_TP codes):
        - 22Y: Contribution refund to candidate committee
        - 24A: Independent expenditure opposing candidate
        - 24C: Coordinated party expenditure
        - 24E: Independent expenditure supporting candidate
        - 24F: Communication cost for candidate (corporation/union)
        - 24G: Transfer to affiliated committee
        - 24H: In-kind to registered party committee
        - 24I: Earmarked contribution (intermediary)
        - 24K: Direct contribution to candidate
        - 24N: Communication cost against candidate
        - 24T: Earmarked contribution (conduit)
        - 24Z: In-kind contribution (earmarked conduit)
    
    Data Sources:
        - FEC pas2.txt (PAC contributions to candidates/committees)
        - FEC oth.txt (other committee receipts)
    
    Edge Direction: committees/{from_CMTE_ID} → committees/{to_CMTE_ID}
    
    Example:
        {
            "_from": "committees/C00873398",
            "_to": "committees/C00543211",
            "total_amount": 500000.0,
            "transaction_type": "24G",
            "cycles": ["2024"]
        }
    """
    
    # Edge endpoints
    from_committee: str = Field(..., description="Source: committees/{CMTE_ID}")
    to_committee: str = Field(..., description="Target: committees/{CMTE_ID}")
    
    # Transfer details
    total_amount: float = Field(..., description="Total transferred ($)")
    transaction_count: int = Field(default=1, description="Number of transfers")
    transaction_types: List[str] = Field(default_factory=list, description="FEC transaction type codes")
    
    # Temporal information
    cycles: List[str] = Field(default_factory=list, description="Election cycles")
    first_date: Optional[str] = Field(None, description="First transfer date")
    last_date: Optional[str] = Field(None, description="Most recent transfer date")
    
    # Metadata
    updated_at: datetime = Field(default_factory=datetime.now)
    
    @computed_field
    @property
    def _from(self) -> str:
        return self.from_committee
    
    @computed_field
    @property
    def _to(self) -> str:
        return self.to_committee
    
    @computed_field
    @property
    def _key(self) -> str:
        combined = f"{self.from_committee}|{self.to_committee}"
        return sha256(combined.encode()).hexdigest()[:16]
    
    @computed_field
    @property
    def is_independent_expenditure(self) -> bool:
        """Check if this includes independent expenditures (24A/24E)."""
        ie_types = {'24A', '24E'}
        return bool(set(self.transaction_types) & ie_types)
    
    def to_arango_doc(self) -> Dict[str, Any]:
        return {
            "_key": self._key,
            "_from": self.from_committee,
            "_to": self.to_committee,
            "total_amount": self.total_amount,
            "transaction_count": self.transaction_count,
            "transaction_types": self.transaction_types,
            "cycles": self.cycles,
            "first_date": self.first_date,
            "last_date": self.last_date,
            "updated_at": self.updated_at.isoformat(),
        }


# =============================================================================
# AFFILIATED_WITH EDGE (Committee → Candidate)
# =============================================================================

class AffiliatedWith(BaseModel):
    """Official FEC linkage between a committee and a candidate.
    
    This represents the OFFICIAL relationship between a committee and
    the candidate it supports. This is how we trace money to its
    final destination.
    
    Linkage Types (from FEC CMTE_DSGN):
        - A: Authorized by candidate
        - B: Lobbyist/Registrant PAC
        - D: Leadership PAC
        - J: Joint fundraising committee
        - P: Principal campaign committee
        - U: Unauthorized (not authorized by candidate)
    
    Data Source: FEC ccl.txt (candidate-committee linkages)
    
    Edge Direction: committees/{CMTE_ID} → candidates/{CAND_ID}
    
    Example:
        {
            "_from": "committees/C00618389",
            "_to": "candidates/P00009423",
            "linkage_type": "P",
            "cycles": ["2024"]
        }
    """
    
    # Edge endpoints
    from_committee: str = Field(..., description="Source: committees/{CMTE_ID}")
    to_candidate: str = Field(..., description="Target: candidates/{CAND_ID}")
    
    # Linkage details
    linkage_type: Optional[str] = Field(None, description="FEC committee designation")
    
    # Temporal information
    cycles: List[str] = Field(default_factory=list, description="Election cycles")
    
    # Metadata
    updated_at: datetime = Field(default_factory=datetime.now)
    
    @computed_field
    @property
    def _from(self) -> str:
        return self.from_committee
    
    @computed_field
    @property
    def _to(self) -> str:
        return self.to_candidate
    
    @computed_field
    @property
    def _key(self) -> str:
        combined = f"{self.from_committee}|{self.to_candidate}"
        return sha256(combined.encode()).hexdigest()[:16]
    
    @computed_field
    @property
    def is_principal_committee(self) -> bool:
        """Check if this is the candidate's principal campaign committee."""
        return self.linkage_type == 'P'
    
    @computed_field
    @property
    def is_authorized(self) -> bool:
        """Check if committee is authorized by the candidate."""
        return self.linkage_type in ('A', 'P')
    
    def to_arango_doc(self) -> Dict[str, Any]:
        return {
            "_key": self._key,
            "_from": self.from_committee,
            "_to": self.to_candidate,
            "linkage_type": self.linkage_type,
            "cycles": self.cycles,
            "updated_at": self.updated_at.isoformat(),
        }


# =============================================================================
# EMPLOYED_BY EDGE (Donor → Employer)
# =============================================================================

class EmployedBy(BaseModel):
    """Employment relationship between a donor and their employer.
    
    This enables corporate influence analysis:
    "How much did Google employees donate to candidate X?"
    "Which companies' employees donated the most to Republicans?"
    
    Data Source: Extracted from indiv.txt EMPLOYER field
    
    Edge Direction: donors/{donor_key} → employers/{employer_key}
    
    Example:
        {
            "_from": "donors/a1b2c3d4e5f6g7h8",
            "_to": "employers/b2c3d4e5f6g7h8i9",
            "donation_amount": 55000.0
        }
    """
    
    # Edge endpoints
    from_donor: str = Field(..., description="Source: donors/{key}")
    to_employer: str = Field(..., description="Target: employers/{key}")
    
    # The donor's total donations (for weighting)
    donation_amount: float = Field(default=0.0, description="Donor's total donations ($)")
    
    # Metadata
    updated_at: datetime = Field(default_factory=datetime.now)
    
    @computed_field
    @property
    def _from(self) -> str:
        return self.from_donor
    
    @computed_field
    @property
    def _to(self) -> str:
        return self.to_employer
    
    @computed_field
    @property
    def _key(self) -> str:
        combined = f"{self.from_donor}|{self.to_employer}"
        return sha256(combined.encode()).hexdigest()[:16]
    
    def to_arango_doc(self) -> Dict[str, Any]:
        return {
            "_key": self._key,
            "_from": self.from_donor,
            "_to": self.to_employer,
            "donation_amount": self.donation_amount,
            "updated_at": self.updated_at.isoformat(),
        }


# =============================================================================
# SPENT_ON EDGE (Committee → Candidate) - Independent Expenditures
# =============================================================================

class SpentOn(BaseModel):
    """Independent expenditure by a committee FOR or AGAINST a candidate.
    
    This is DIFFERENT from affiliated_with:
    - affiliated_with = official FEC linkage (principal campaign committee)
    - spent_on = Super PAC spending ads FOR or AGAINST candidates
    
    Independent Expenditures (IE) are spending by PACs/Super PACs that is NOT
    coordinated with the candidate's campaign. This is the "dark money" path
    that Citizens United enabled.
    
    Transaction Types (from FEC pas2):
    - 24E: Independent expenditure advocating election (SUPPORT)
    - 24A: Independent expenditure against candidate (OPPOSE)
    
    Data Source: FEC pas2.txt filtered to TRANSACTION_TP IN ['24E', '24A']
    
    Edge Direction: committees/{CMTE_ID} → candidates/{CAND_ID}
    
    Example (Support):
        {
            "_from": "committees/C00873398",  # America PAC
            "_to": "candidates/P80001571",     # Trump
            "support_oppose": "S",
            "total_amount": 95000000.0,
            "transaction_count": 1500
        }
    
    Example (Oppose):
        {
            "_from": "committees/C00492785",  # Priorities USA
            "_to": "candidates/P80001571",     # Trump
            "support_oppose": "O",
            "total_amount": 45000000.0,
            "transaction_count": 800
        }
    
    Query Example (AQL):
        // Find all IE spending FOR a candidate
        FOR edge IN spent_on
            FILTER edge._to == "candidates/P80001571"
            FILTER edge.support_oppose == "S"
            RETURN edge
        
        // Find all IE spending AGAINST a candidate
        FOR edge IN spent_on
            FILTER edge._to == "candidates/P80001571"
            FILTER edge.support_oppose == "O"
            RETURN edge
    """
    
    # Edge endpoints
    from_committee: str = Field(..., description="Source: committees/{CMTE_ID}")
    to_candidate: str = Field(..., description="Target: candidates/{CAND_ID}")
    
    # Independent expenditure classification
    support_oppose: str = Field(..., description="'S' = Support, 'O' = Oppose")
    
    # Aggregated spending data
    total_amount: float = Field(..., description="Sum of all IE spending ($)")
    transaction_count: int = Field(default=1, description="Number of expenditure records")
    
    # Temporal information
    cycle: str = Field(..., description="Election cycle (e.g., '2024')")
    
    # Metadata
    updated_at: datetime = Field(default_factory=datetime.now)
    
    @computed_field
    @property
    def _from(self) -> str:
        return self.from_committee
    
    @computed_field
    @property
    def _to(self) -> str:
        return self.to_candidate
    
    @computed_field
    @property
    def _key(self) -> str:
        # Unique by committee + candidate + support/oppose + cycle
        cmte_id = self.from_committee.split("/")[-1]
        cand_id = self.to_candidate.split("/")[-1]
        return f"{cmte_id}_{cand_id}_{self.support_oppose}_{self.cycle}"
    
    def to_arango_doc(self) -> Dict[str, Any]:
        return {
            "_key": self._key,
            "_from": self.from_committee,
            "_to": self.to_candidate,
            "support_oppose": self.support_oppose,
            "total_amount": self.total_amount,
            "transaction_count": self.transaction_count,
            "cycle": self.cycle,
            "updated_at": self.updated_at.isoformat(),
        }
