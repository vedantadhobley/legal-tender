"""Vertex (node) models for the political money flow graph.

In a graph database:
- **Vertices** are the entities (things with properties)
- **Edges** are the relationships between entities

This module defines the VERTEX models:
- Donor: An individual who donated money
- Employer: A company whose employees donated
- Committee: A PAC, Super PAC, or campaign committee
- Candidate: A federal election candidate

ArangoDB Vertex Requirements:
- Every vertex needs a unique `_key` (like a primary key)
- The `_id` is auto-generated as "{collection_name}/{_key}"
- Properties can be any JSON-serializable data
"""

from datetime import datetime
from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field, computed_field
from hashlib import sha256


# =============================================================================
# DONOR VERTEX
# =============================================================================

class Donor(BaseModel):
    """An individual who donated $10K+ in any election cycle.
    
    Donors are aggregated from raw FEC individual contributions (indiv.txt).
    The $10K threshold filters out small donors to focus on major contributors.
    
    Key Generation:
        The _key is a SHA256 hash of "{NAME}|{EMPLOYER}" to handle:
        - Name variations: "MUSK, ELON" vs "MUSK ELON"
        - Employer variations: "TESLA INC" vs "TESLA"
        We normalize both before hashing for better deduplication.
    
    Example:
        {
            "_key": "a1b2c3d4e5f6g7h8",
            "canonical_name": "MUSK, ELON",
            "canonical_employer": "TESLA INC",
            "total_amount": 55000.0,
            "transaction_count": 5,
            "cycles": ["2024"],
            "updated_at": "2026-01-05T12:00:00"
        }
    """
    
    # Unique identifier (hash of normalized name + employer)
    key: str = Field(..., description="SHA256 hash of normalized name|employer (16 chars)")
    
    # Core identity
    canonical_name: str = Field(..., description="Normalized donor name (uppercase)")
    canonical_employer: str = Field(..., description="Normalized employer name")
    
    # Aggregated donation data
    total_amount: float = Field(..., description="Total donations across all cycles ($)")
    transaction_count: int = Field(..., description="Number of individual donations")
    cycles: List[str] = Field(default_factory=list, description="Election cycles with donations")
    
    # Metadata
    updated_at: datetime = Field(default_factory=datetime.now)
    
    @computed_field
    @property
    def _key(self) -> str:
        """ArangoDB document key."""
        return self.key
    
    def to_arango_doc(self) -> Dict[str, Any]:
        """Convert to ArangoDB document format."""
        return {
            "_key": self.key,
            "canonical_name": self.canonical_name,
            "canonical_employer": self.canonical_employer,
            "total_amount": self.total_amount,
            "transaction_count": self.transaction_count,
            "cycles": self.cycles,
            "updated_at": self.updated_at.isoformat(),
        }
    
    @staticmethod
    def make_key(name: str, employer: str) -> str:
        """Generate a unique key from name and employer.
        
        Args:
            name: Donor name (will be normalized)
            employer: Employer name (will be normalized)
            
        Returns:
            16-character hex string (first 16 chars of SHA256)
        """
        import re
        
        # Normalize name
        normalized_name = re.sub(r'\s*,\s*', ' ', name.upper().strip())
        normalized_name = re.sub(r'\s+', ' ', normalized_name)
        
        # Normalize employer
        normalized_employer = employer.upper().strip() if employer else ""
        normalized_employer = re.sub(r'[,.]', '', normalized_employer)
        normalized_employer = re.sub(r'\s+', ' ', normalized_employer)
        
        combined = f"{normalized_name}|{normalized_employer}"
        return sha256(combined.encode()).hexdigest()[:16]


# =============================================================================
# EMPLOYER VERTEX
# =============================================================================

class Employer(BaseModel):
    """A company whose employees donated $10K+ in any cycle.
    
    Employers are extracted from donor records to enable queries like:
    "Show all donations from Google employees" or 
    "Which companies had the most employees donate to candidate X?"
    
    Key Generation:
        The _key is a SHA256 hash of the normalized employer name.
    
    Example:
        {
            "_key": "b2c3d4e5f6g7h8i9",
            "name": "GOOGLE LLC",
            "donor_count": 1523,
            "total_amount": 45000000.0,
            "updated_at": "2026-01-05T12:00:00"
        }
    """
    
    key: str = Field(..., description="SHA256 hash of normalized employer name")
    name: str = Field(..., description="Normalized employer name (uppercase)")
    donor_count: int = Field(default=0, description="Number of employees who donated")
    total_amount: float = Field(default=0.0, description="Total donations from all employees")
    updated_at: datetime = Field(default_factory=datetime.now)
    
    @computed_field
    @property
    def _key(self) -> str:
        return self.key
    
    def to_arango_doc(self) -> Dict[str, Any]:
        return {
            "_key": self.key,
            "name": self.name,
            "donor_count": self.donor_count,
            "total_amount": self.total_amount,
            "updated_at": self.updated_at.isoformat(),
        }
    
    @staticmethod
    def make_key(name: str) -> str:
        """Generate key from employer name."""
        import re
        normalized = name.upper().strip() if name else ""
        normalized = re.sub(r'[,.]', '', normalized)
        normalized = re.sub(r'\s+', ' ', normalized)
        return sha256(normalized.encode()).hexdigest()[:16]


# =============================================================================
# COMMITTEE VERTEX
# =============================================================================

class Committee(BaseModel):
    """A PAC, Super PAC, or campaign committee registered with the FEC.
    
    Committees are the CENTRAL nodes in the money flow graph. Money flows:
    - INTO committees from donors (contributed_to edges)
    - BETWEEN committees (transferred_to edges) - the "dark money" path
    - OUT to candidates (affiliated_with edges)
    
    Committee Types (from FEC):
        - C: Communication cost
        - D: Delegate committee
        - E: Electioneering communication
        - H: House campaign
        - I: Independent expenditure (Super PAC)
        - N: PAC - nonqualified
        - O: Super PAC (Independent Expenditure Only)
        - P: Presidential campaign
        - Q: PAC - qualified
        - S: Senate campaign
        - U: Single candidate independent expenditure
        - V: PAC with non-contribution account
        - W: PAC with non-contribution account - nonqualified
        - X: Party - nonqualified
        - Y: Party - qualified
        - Z: National party nonfederal account
    
    Example:
        {
            "_key": "C00873398",
            "CMTE_ID": "C00873398",
            "CMTE_NM": "AMERICA PAC",
            "CMTE_TP": "O",
            "CMTE_DSGN": "U",
            "CMTE_PTY_AFFILIATION": "REP",
            "total_receipts": 150000000.0,
            "total_disbursements": 148000000.0
        }
    """
    
    # FEC identifiers (CMTE_ID is the _key)
    CMTE_ID: str = Field(..., description="FEC Committee ID (e.g., C00873398)")
    CMTE_NM: str = Field(..., description="Committee name")
    CMTE_TP: Optional[str] = Field(None, description="Committee type code")
    CMTE_DSGN: Optional[str] = Field(None, description="Committee designation")
    CMTE_PTY_AFFILIATION: Optional[str] = Field(None, description="Party affiliation")
    
    # Address (from FEC)
    CMTE_ST1: Optional[str] = Field(None, description="Street address line 1")
    CMTE_ST2: Optional[str] = Field(None, description="Street address line 2")
    CMTE_CITY: Optional[str] = Field(None, description="City")
    CMTE_ST: Optional[str] = Field(None, description="State")
    CMTE_ZIP: Optional[str] = Field(None, description="ZIP code")
    
    # Financials (aggregated)
    total_receipts: float = Field(default=0.0, description="Total money received")
    total_disbursements: float = Field(default=0.0, description="Total money spent")
    
    # Metadata
    cycles: List[str] = Field(default_factory=list, description="Active election cycles")
    updated_at: datetime = Field(default_factory=datetime.now)
    
    @computed_field
    @property
    def _key(self) -> str:
        return self.CMTE_ID
    
    @computed_field
    @property
    def is_super_pac(self) -> bool:
        """Check if this is a Super PAC (Independent Expenditure Only)."""
        return self.CMTE_TP in ('I', 'O', 'U')
    
    @computed_field
    @property
    def is_campaign(self) -> bool:
        """Check if this is a campaign committee."""
        return self.CMTE_TP in ('H', 'P', 'S')
    
    def to_arango_doc(self) -> Dict[str, Any]:
        return {
            "_key": self.CMTE_ID,
            "CMTE_ID": self.CMTE_ID,
            "CMTE_NM": self.CMTE_NM,
            "CMTE_TP": self.CMTE_TP,
            "CMTE_DSGN": self.CMTE_DSGN,
            "CMTE_PTY_AFFILIATION": self.CMTE_PTY_AFFILIATION,
            "CMTE_ST1": self.CMTE_ST1,
            "CMTE_ST2": self.CMTE_ST2,
            "CMTE_CITY": self.CMTE_CITY,
            "CMTE_ST": self.CMTE_ST,
            "CMTE_ZIP": self.CMTE_ZIP,
            "total_receipts": self.total_receipts,
            "total_disbursements": self.total_disbursements,
            "cycles": self.cycles,
            "updated_at": self.updated_at.isoformat(),
        }


# =============================================================================
# CANDIDATE VERTEX
# =============================================================================

class Candidate(BaseModel):
    """A federal election candidate registered with the FEC.
    
    Candidates are the END NODES for most money flow queries:
    "Where did donor X's money ultimately go?"
    
    Candidates are linked to committees via affiliated_with edges.
    A candidate may have multiple committees across cycles.
    
    Office Codes:
        - H: House of Representatives
        - S: Senate
        - P: President
    
    Example:
        {
            "_key": "P00009423",
            "CAND_ID": "P00009423",
            "CAND_NAME": "TRUMP, DONALD J.",
            "CAND_PTY_AFFILIATION": "REP",
            "CAND_OFFICE": "P",
            "CAND_OFFICE_ST": "US",
            "CAND_OFFICE_DISTRICT": "00"
        }
    """
    
    # FEC identifiers (CAND_ID is the _key)
    CAND_ID: str = Field(..., description="FEC Candidate ID (e.g., P00009423)")
    CAND_NAME: str = Field(..., description="Candidate name")
    CAND_PTY_AFFILIATION: Optional[str] = Field(None, description="Party affiliation code")
    
    # Office information
    CAND_OFFICE: Optional[str] = Field(None, description="Office code: H/S/P")
    CAND_OFFICE_ST: Optional[str] = Field(None, description="State (or US for President)")
    CAND_OFFICE_DISTRICT: Optional[str] = Field(None, description="District (House only)")
    
    # Status
    CAND_STATUS: Optional[str] = Field(None, description="Candidate status code")
    CAND_ICI: Optional[str] = Field(None, description="Incumbent/Challenger/Open")
    
    # Metadata
    cycles: List[str] = Field(default_factory=list, description="Election cycles")
    updated_at: datetime = Field(default_factory=datetime.now)
    
    @computed_field
    @property
    def _key(self) -> str:
        return self.CAND_ID
    
    @computed_field
    @property
    def office_name(self) -> str:
        """Human-readable office name."""
        offices = {'H': 'House', 'S': 'Senate', 'P': 'President'}
        return offices.get(self.CAND_OFFICE, 'Unknown')
    
    @computed_field
    @property
    def party_name(self) -> str:
        """Human-readable party name."""
        parties = {
            'REP': 'Republican',
            'DEM': 'Democrat',
            'LIB': 'Libertarian',
            'GRE': 'Green',
            'IND': 'Independent',
        }
        return parties.get(self.CAND_PTY_AFFILIATION, self.CAND_PTY_AFFILIATION or 'Unknown')
    
    def to_arango_doc(self) -> Dict[str, Any]:
        return {
            "_key": self.CAND_ID,
            "CAND_ID": self.CAND_ID,
            "CAND_NAME": self.CAND_NAME,
            "CAND_PTY_AFFILIATION": self.CAND_PTY_AFFILIATION,
            "CAND_OFFICE": self.CAND_OFFICE,
            "CAND_OFFICE_ST": self.CAND_OFFICE_ST,
            "CAND_OFFICE_DISTRICT": self.CAND_OFFICE_DISTRICT,
            "CAND_STATUS": self.CAND_STATUS,
            "CAND_ICI": self.CAND_ICI,
            "cycles": self.cycles,
            "updated_at": self.updated_at.isoformat(),
        }
