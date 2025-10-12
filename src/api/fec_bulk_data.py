"""FEC Bulk Data Downloads - No rate limits, weekly updates."""
import requests
import logging
from typing import Dict, List, Optional, Any
import csv
from io import StringIO, BytesIO
import zipfile
import tempfile
import os

logger = logging.getLogger("fec_bulk_data")

BASE_URL = "https://www.fec.gov/files/bulk-downloads"
CACHE_DIR = "/app/cache/fec_bulk"  # Persistent cache directory

# Create cache directory if it doesn't exist
os.makedirs(CACHE_DIR, exist_ok=True)


def download_bulk_file(cycle: int, filename: str, force_refresh: bool = False) -> Optional[str]:
    """
    Download a bulk data file from FEC with caching support.
    
    Args:
        cycle: Election cycle year (e.g., 2024, 2026)
        filename: File to download (e.g., 'cn.txt', 'cm.txt', 'ccl.txt')
        force_refresh: If True, download fresh even if cached. If False, use cache if available.
                      Can also be controlled via FEC_FORCE_REFRESH environment variable.
    
    Returns:
        Raw text content of the file, or None if error
    """
    # Check environment variable for force refresh
    env_force_refresh = os.getenv("FEC_FORCE_REFRESH", "false").lower() == "true"
    force_refresh = env_force_refresh or force_refresh
    
    cache_path = os.path.join(CACHE_DIR, f"{cycle}_{filename}")
    
    # Use cached version if exists and not forcing refresh
    if not force_refresh and os.path.exists(cache_path):
        logger.info(f"📂 Using cached {filename} from {cache_path}")
        try:
            with open(cache_path, 'r', encoding='utf-8') as f:
                content = f.read()
            logger.info(f"   Loaded {len(content)} chars from cache")
            return content
        except Exception as e:
            logger.warning(f"⚠️  Error reading cache {cache_path}: {e}, will download fresh")
            # Fall through to download
    
    # Download fresh from FEC
    url = f"{BASE_URL}/{cycle}/{filename}"
    logger.info(f"⬇️  Downloading {filename} from {url}...")
    
    try:
        resp = requests.get(url, timeout=60)
        resp.raise_for_status()
        content = resp.text
        logger.info(f"✅ Downloaded {filename} ({len(content)} chars)")
        
        # Save to cache for future use
        try:
            with open(cache_path, 'w', encoding='utf-8') as f:
                f.write(content)
            logger.info(f"💾 Cached to {cache_path}")
        except Exception as e:
            logger.warning(f"⚠️  Could not cache {filename}: {e}")
        
        return content
    except Exception as e:
        logger.error(f"❌ Error downloading {url}: {e}")
        return None


def parse_candidate_master(text: str) -> List[Dict[str, Any]]:
    """
    Parse cn.txt (candidate master file).
    
    Format: Pipe-delimited (|) with these key columns:
    - CAND_ID: FEC candidate ID
    - CAND_NAME: Full name (LAST, FIRST)
    - CAND_PTY_AFFILIATION: Party code (DEM, REP, etc.)
    - CAND_ELECTION_YR: Election year
    - CAND_OFFICE_ST: State code (CA, NY, etc.)
    - CAND_OFFICE: H, S, or P
    - CAND_OFFICE_DISTRICT: District number (for House)
    - CAND_ICI: Incumbent/Challenger/Open (I/C/O)
    - CAND_STATUS: Candidate status (C, F, N, P)
    - CAND_PCC: Principal campaign committee ID
    - CAND_ST1: Street address
    - CAND_ST2: Street address 2
    - CAND_CITY: City
    - CAND_ST: State
    - CAND_ZIP: ZIP code
    
    Returns:
        List of candidate dicts
    """
    candidates = []
    reader = csv.DictReader(StringIO(text), delimiter='|')
    
    for row in reader:
        candidates.append({
            "candidate_id": row.get("CAND_ID", "").strip(),
            "name": row.get("CAND_NAME", "").strip(),
            "party": row.get("CAND_PTY_AFFILIATION", "").strip(),
            "election_year": row.get("CAND_ELECTION_YR", "").strip(),
            "state": row.get("CAND_OFFICE_ST", "").strip(),
            "office": row.get("CAND_OFFICE", "").strip(),
            "district": row.get("CAND_OFFICE_DISTRICT", "").strip(),
            "incumbent_challenger": row.get("CAND_ICI", "").strip(),
            "status": row.get("CAND_STATUS", "").strip(),
            "pcc_id": row.get("CAND_PCC", "").strip(),
        })
    
    logger.info(f"Parsed {len(candidates)} candidates from cn.txt")
    return candidates


def parse_committee_master(text: str) -> List[Dict[str, Any]]:
    """
    Parse cm.txt (committee master file).
    
    Format: Pipe-delimited (|) with these key columns:
    - CMTE_ID: Committee ID
    - CMTE_NM: Committee name
    - TRES_NM: Treasurer name
    - CMTE_ST1: Street address
    - CMTE_ST2: Street address 2
    - CMTE_CITY: City
    - CMTE_ST: State
    - CMTE_ZIP: ZIP code
    - CMTE_DSGN: Committee designation (P, A, J, B, U, D)
    - CMTE_TP: Committee type
    - CMTE_PTY_AFFILIATION: Party affiliation
    - CMTE_FILING_FREQ: Filing frequency
    - ORG_TP: Organization type
    - CONNECTED_ORG_NM: Connected organization name
    
    Returns:
        List of committee dicts
    """
    committees = []
    reader = csv.DictReader(StringIO(text), delimiter='|')
    
    for row in reader:
        committees.append({
            "committee_id": row.get("CMTE_ID", "").strip(),
            "name": row.get("CMTE_NM", "").strip(),
            "treasurer": row.get("TRES_NM", "").strip(),
            "state": row.get("CMTE_ST", "").strip(),
            "designation": row.get("CMTE_DSGN", "").strip(),
            "type": row.get("CMTE_TP", "").strip(),
            "party": row.get("CMTE_PTY_AFFILIATION", "").strip(),
            "connected_org": row.get("CONNECTED_ORG_NM", "").strip(),
        })
    
    logger.info(f"Parsed {len(committees)} committees from cm.txt")
    return committees


def parse_candidate_committee_linkage(text: str) -> List[Dict[str, Any]]:
    """
    Parse ccl.txt (candidate-committee linkage file).
    
    Format: Pipe-delimited (|) with these key columns:
    - CAND_ID: Candidate ID
    - CAND_ELECTION_YR: Election year
    - FEC_ELECTION_YR: FEC election year
    - CMTE_ID: Committee ID
    - CMTE_TP: Committee type
    - CMTE_DSGN: Committee designation
    - LINKAGE_ID: Linkage ID
    
    Returns:
        List of linkage dicts
    """
    linkages = []
    reader = csv.DictReader(StringIO(text), delimiter='|')
    
    for row in reader:
        linkages.append({
            "candidate_id": row.get("CAND_ID", "").strip(),
            "election_year": row.get("CAND_ELECTION_YR", "").strip(),
            "committee_id": row.get("CMTE_ID", "").strip(),
            "committee_type": row.get("CMTE_TP", "").strip(),
            "committee_designation": row.get("CMTE_DSGN", "").strip(),
        })
    
    logger.info(f"Parsed {len(linkages)} candidate-committee linkages from ccl.txt")
    return linkages


def build_candidate_lookup(candidates: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    """
    Build a lookup dict for fast candidate searching.
    
    Key format: "{last_name}|{state}|{office}"
    
    Returns:
        Dict mapping lookup key -> list of matching candidates
    """
    lookup = {}
    
    for candidate in candidates:
        name = candidate.get("name", "")
        state = candidate.get("state", "")
        office = candidate.get("office", "")
        
        # Extract last name (format is "LAST, FIRST")
        if "," in name:
            last_name = name.split(",")[0].strip().upper()
        else:
            last_name = name.strip().upper()
        
        key = f"{last_name}|{state}|{office}"
        
        if key not in lookup:
            lookup[key] = []
        lookup[key].append(candidate)
    
    logger.info(f"Built lookup index with {len(lookup)} unique keys")
    return lookup


def search_candidate_in_bulk(
    last_name: str,
    state: str,
    office: str,
    lookup: Dict[str, List[Dict[str, Any]]]
) -> List[Dict[str, Any]]:
    """
    Search for a candidate in the bulk data lookup.
    
    Args:
        last_name: Candidate last name
        state: Two-letter state code
        office: H, S, or P
        lookup: Pre-built lookup dict from build_candidate_lookup()
    
    Returns:
        List of matching candidates (usually 0 or 1)
    """
    key = f"{last_name.upper()}|{state}|{office}"
    return lookup.get(key, [])


def get_committees_for_candidate(
    candidate_id: str,
    linkages: List[Dict[str, Any]],
    committees: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    """
    Get all committees linked to a candidate.
    
    Args:
        candidate_id: FEC candidate ID
        linkages: List from parse_candidate_committee_linkage()
        committees: List from parse_committee_master()
    
    Returns:
        List of committee dicts
    """
    # Build committee lookup for fast access
    committee_dict = {c["committee_id"]: c for c in committees}
    
    # Find all committees linked to this candidate
    linked_committees = []
    for link in linkages:
        if link["candidate_id"] == candidate_id:
            cmte_id = link["committee_id"]
            if cmte_id in committee_dict:
                linked_committees.append({
                    **committee_dict[cmte_id],
                    "linkage_type": link["committee_type"],
                    "linkage_designation": link["committee_designation"],
                })
    
    return linked_committees


class FECBulkDataCache:
    """Cache for FEC bulk data to avoid re-downloading."""
    
    def __init__(self):
        self.candidates = {}
        self.committees = {}
        self.linkages = {}
        self.lookups = {}
        self.candidate_summaries = {}  # Financial summaries by cycle
        self.committee_summaries = {}  # Committee financial summaries by cycle
        self.pac_summaries = {}  # PAC/party financial summaries by cycle
        self.contributions_by_committee = {}  # Individual contributions grouped by committee ID
        self.independent_expenditures_by_candidate = {}  # Super PAC spending grouped by candidate ID
        self.independent_expenditures_by_committee = {}  # Super PAC spending grouped by spending committee ID
        self.committee_transfers_from = {}  # Committee transfers grouped by donor committee
        self.committee_transfers_to = {}  # Committee transfers grouped by recipient committee
    
    def load_cycle(self, cycle: int, force_refresh: bool = False) -> bool:
        """
        Load all bulk data for a given cycle.
        
        Args:
            cycle: Election cycle year
            force_refresh: If True, redownload files even if cached
        
        Returns:
            True if successful, False if error
        """
        if cycle in self.candidates and not force_refresh:
            logger.info(f"Cycle {cycle} already loaded in memory")
            return True
        
        logger.info(f"Loading FEC bulk data for cycle {cycle}...")
        
        # Download files (will use cache if available unless force_refresh=True)
        cn_text = download_bulk_file(cycle, "cn.txt", force_refresh=force_refresh)
        cm_text = download_bulk_file(cycle, "cm.txt", force_refresh=force_refresh)
        ccl_text = download_bulk_file(cycle, "ccl.txt", force_refresh=force_refresh)
        
        if not cn_text or not cm_text or not ccl_text:
            logger.error(f"Failed to download one or more files for cycle {cycle}")
            return False
        
        # Parse files
        self.candidates[cycle] = parse_candidate_master(cn_text)
        self.committees[cycle] = parse_committee_master(cm_text)
        self.linkages[cycle] = parse_candidate_committee_linkage(ccl_text)
        
        # Build lookup index
        self.lookups[cycle] = build_candidate_lookup(self.candidates[cycle])
        
        logger.info(f"✅ Loaded cycle {cycle}: {len(self.candidates[cycle])} candidates, {len(self.committees[cycle])} committees")
        return True
    
    def search_candidate(
        self,
        last_name: str,
        state: str,
        office: str,
        cycles: List[int] = [2024, 2026]
    ) -> Optional[Dict[str, Any]]:
        """
        Search for a candidate across multiple cycles.
        
        Returns:
            Best matching candidate dict (from most recent cycle), or None
        """
        for cycle in sorted(cycles, reverse=True):  # Search newest first
            if cycle not in self.lookups:
                self.load_cycle(cycle)
            
            results = search_candidate_in_bulk(last_name, state, office, self.lookups[cycle])
            if results:
                # Return most recent match
                return results[0]
        
        return None
    
    def get_candidate_committees(
        self,
        candidate_id: str,
        cycle: int
    ) -> List[Dict[str, Any]]:
        """Get committees for a candidate in a specific cycle."""
        if cycle not in self.linkages:
            self.load_cycle(cycle)
        
        return get_committees_for_candidate(
            candidate_id,
            self.linkages[cycle],
            self.committees[cycle]
        )
    
    def load_candidate_summaries(self, cycle: int, force_refresh: bool = False) -> bool:
        """
        Load candidate financial summary data for a cycle.
        
        Downloads weball[YY].zip (~50MB) and parses financial totals.
        
        Args:
            cycle: Election cycle year
            force_refresh: If True, redownload files even if cached
        
        Returns:
            True if successful, False if error
        """
        if cycle in self.candidate_summaries and not force_refresh:
            logger.info(f"Candidate summaries for cycle {cycle} already loaded in memory")
            return True
        
        logger.info(f"Loading candidate financial summaries for cycle {cycle}...")
        
        # Construct filename: weball24.zip for 2024
        year_suffix = str(cycle)[-2:]  # Last 2 digits
        filename = f"weball{year_suffix}.zip"
        
        zip_bytes = download_bulk_zip(cycle, filename, force_refresh=force_refresh)
        if not zip_bytes:
            logger.error(f"Failed to download {filename}")
            return False
        
        summaries = parse_candidate_summary(zip_bytes)
        if not summaries:
            logger.error(f"Failed to parse {filename}")
            return False
        
        # Build lookup dict by candidate_id
        self.candidate_summaries[cycle] = {
            s["candidate_id"]: s for s in summaries
        }
        
        logger.info(f"✅ Loaded {len(summaries)} candidate financial summaries for cycle {cycle}")
        return True
    
    def get_candidate_summary(
        self,
        candidate_id: str,
        cycle: int
    ) -> Optional[Dict[str, Any]]:
        """
        Get financial summary for a candidate.
        
        Returns dict with total_receipts, total_disbursements, cash_on_hand, etc.
        """
        if cycle not in self.candidate_summaries:
            self.load_candidate_summaries(cycle)
        
        return self.candidate_summaries.get(cycle, {}).get(candidate_id)
    
    def load_individual_contributions(
        self,
        cycle: int,
        max_records: Optional[int] = None,
        force_refresh: bool = False
    ) -> bool:
        """
        Load individual contribution data for a cycle.
        
        WARNING: This downloads and parses a HUGE file (4+ GB, millions of records).
        Use max_records for testing, or None to load everything.
        
        This will take several minutes to download and parse.
        
        Args:
            cycle: Election cycle year
            max_records: Limit number of records (for testing)
            force_refresh: If True, redownload files even if cached
        
        Returns:
            True if successful, False if error
        """
        if cycle in self.contributions_by_committee and not force_refresh:
            logger.info(f"Individual contributions for cycle {cycle} already loaded in memory")
            return True
        
        logger.info(f"Loading individual contributions for cycle {cycle}...")
        if max_records:
            logger.info(f"  Limited to first {max_records:,} records")
        else:
            logger.warning("  Loading ALL records - this will take several minutes and use ~4GB RAM!")
        
        # Construct filename: indiv24.zip for 2024
        year_suffix = str(cycle)[-2:]
        filename = f"indiv{year_suffix}.zip"
        
        zip_bytes = download_bulk_zip(cycle, filename, force_refresh=force_refresh)
        if not zip_bytes:
            logger.error(f"Failed to download {filename}")
            return False
        
        contributions = parse_individual_contributions(zip_bytes, max_records=max_records)
        if not contributions:
            logger.error(f"Failed to parse {filename}")
            return False
        
        # Group by committee_id for fast lookup
        logger.info("Indexing contributions by committee...")
        by_committee = {}
        for contrib in contributions:
            cmte_id = contrib["committee_id"]
            if cmte_id not in by_committee:
                by_committee[cmte_id] = []
            by_committee[cmte_id].append(contrib)
        
        self.contributions_by_committee[cycle] = by_committee
        
        logger.info(f"✅ Loaded {len(contributions):,} contributions across {len(by_committee)} committees")
        return True
    
    def get_contributions_for_committee(
        self,
        committee_id: str,
        cycle: int
    ) -> List[Dict[str, Any]]:
        """
        Get all individual contributions for a committee.
        
        Note: Must call load_individual_contributions() first.
        """
        if cycle not in self.contributions_by_committee:
            logger.warning(f"Individual contributions for cycle {cycle} not loaded. Call load_individual_contributions() first.")
            return []
        
        return self.contributions_by_committee.get(cycle, {}).get(committee_id, [])
    
    def load_independent_expenditures(
        self,
        cycle: int,
        max_records: Optional[int] = None,
        force_refresh: bool = False
    ) -> bool:
        """
        Load independent expenditures (Super PAC spending) for a cycle.
        
        This is THE CRITICAL FILE for tracking AIPAC/UDP and other Super PAC spending.
        
        WARNING: This file is LARGE (~2-3 GB, millions of records).
        Use max_records for testing, or None to load everything.
        
        Args:
            cycle: Election cycle year
            max_records: Limit number of records (for testing)
            force_refresh: If True, redownload files even if cached
        
        Returns:
            True if successful, False if error
        """
        if cycle in self.independent_expenditures_by_candidate and not force_refresh:
            logger.info(f"Independent expenditures for cycle {cycle} already loaded")
            return True
        
        logger.info(f"Loading independent expenditures for cycle {cycle}...")
        if max_records:
            logger.info(f"  Limited to first {max_records:,} records")
        else:
            logger.warning("  Loading ALL records - this will take several minutes and use ~2-3GB RAM!")
        
        # Construct filename: oppexp24.zip for 2024
        year_suffix = str(cycle)[-2:]
        filename = f"oppexp{year_suffix}.zip"
        
        zip_bytes = download_bulk_zip(cycle, filename, force_refresh=force_refresh)
        if not zip_bytes:
            logger.error(f"Failed to download {filename}")
            return False
        
        expenditures = parse_independent_expenditures(zip_bytes, max_records=max_records)
        if not expenditures:
            logger.error(f"Failed to parse {filename}")
            return False
        
        # Group by candidate_id AND by spending committee_id
        logger.info("Indexing expenditures by candidate and committee...")
        by_candidate = {}
        by_committee = {}
        
        for exp in expenditures:
            cand_id = exp["candidate_id"]
            cmte_id = exp["committee_id"]
            
            # Index by candidate (who money was spent for/against)
            if cand_id:
                if cand_id not in by_candidate:
                    by_candidate[cand_id] = []
                by_candidate[cand_id].append(exp)
            
            # Index by committee (who spent the money - e.g., AIPAC)
            if cmte_id:
                if cmte_id not in by_committee:
                    by_committee[cmte_id] = []
                by_committee[cmte_id].append(exp)
        
        self.independent_expenditures_by_candidate[cycle] = by_candidate
        self.independent_expenditures_by_committee[cycle] = by_committee
        
        logger.info(f"✅ Loaded {len(expenditures):,} independent expenditures")
        logger.info(f"   Indexed {len(by_candidate)} candidates and {len(by_committee)} spending committees")
        return True
    
    def get_independent_expenditures_for_candidate(
        self,
        candidate_id: str,
        cycle: int
    ) -> List[Dict[str, Any]]:
        """
        Get all Super PAC spending FOR or AGAINST a candidate.
        
        Use this to see which Super PACs (like AIPAC) spent money on a candidate.
        
        Note: Must call load_independent_expenditures() first.
        """
        if cycle not in self.independent_expenditures_by_candidate:
            logger.warning(f"Independent expenditures for cycle {cycle} not loaded. Call load_independent_expenditures() first.")
            return []
        
        return self.independent_expenditures_by_candidate.get(cycle, {}).get(candidate_id, [])
    
    def get_spending_by_committee(
        self,
        committee_id: str,
        cycle: int
    ) -> List[Dict[str, Any]]:
        """
        Get all spending BY a Super PAC committee.
        
        Use this to see ALL candidates that AIPAC/UDP spent money on.
        
        Note: Must call load_independent_expenditures() first.
        """
        if cycle not in self.independent_expenditures_by_committee:
            logger.warning(f"Independent expenditures for cycle {cycle} not loaded. Call load_independent_expenditures() first.")
            return []
        
        return self.independent_expenditures_by_committee.get(cycle, {}).get(committee_id, [])
    
    def aggregate_expenditures_by_committee_for_candidate(
        self,
        candidate_id: str,
        cycle: int
    ) -> Dict[str, Dict[str, Any]]:
        """
        Aggregate Super PAC spending for a candidate, grouped by spending committee.
        
        Returns dict mapping committee_id -> {
            committee_name, support_amount, oppose_amount, net_support,
            support_count, oppose_count
        }
        """
        expenditures = self.get_independent_expenditures_for_candidate(candidate_id, cycle)
        
        aggregates = {}
        for exp in expenditures:
            cmte_id = exp["committee_id"]
            if cmte_id not in aggregates:
                aggregates[cmte_id] = {
                    "committee_id": cmte_id,
                    "committee_name": exp.get("committee_name", ""),
                    "support_amount": 0,
                    "oppose_amount": 0,
                    "support_count": 0,
                    "oppose_count": 0,
                    "net_support": 0,
                }
            
            amount = exp["amount"]
            support_oppose = exp["support_oppose"]
            
            if support_oppose == "S":
                aggregates[cmte_id]["support_amount"] += amount
                aggregates[cmte_id]["support_count"] += 1
            elif support_oppose == "O":
                aggregates[cmte_id]["oppose_amount"] += amount
                aggregates[cmte_id]["oppose_count"] += 1
            
            aggregates[cmte_id]["net_support"] = (
                aggregates[cmte_id]["support_amount"] - 
                aggregates[cmte_id]["oppose_amount"]
            )
        
        return aggregates
    
    def load_committee_summaries(self, cycle: int, force_refresh: bool = False) -> bool:
        """
        Load committee financial summary data for a cycle.
        
        Downloads webl[YY].zip (~100MB) and parses financial totals for ALL committees.
        
        Args:
            cycle: Election cycle year
            force_refresh: If True, redownload files even if cached
        
        Returns:
            True if successful, False if error
        """
        if cycle in self.committee_summaries and not force_refresh:
            logger.info(f"Committee summaries for cycle {cycle} already loaded in memory")
            return True
        
        logger.info(f"Loading committee financial summaries for cycle {cycle}...")
        
        year_suffix = str(cycle)[-2:]
        filename = f"webl{year_suffix}.zip"
        
        zip_bytes = download_bulk_zip(cycle, filename, force_refresh=force_refresh)
        if not zip_bytes:
            logger.error(f"Failed to download {filename}")
            return False
        
        summaries = parse_committee_summary(zip_bytes)
        if not summaries:
            logger.error(f"Failed to parse {filename}")
            return False
        
        # Build lookup dict by committee_id
        self.committee_summaries[cycle] = {
            s["committee_id"]: s for s in summaries
        }
        
        logger.info(f"✅ Loaded {len(summaries)} committee financial summaries for cycle {cycle}")
        return True
    
    def get_committee_summary(
        self,
        committee_id: str,
        cycle: int
    ) -> Optional[Dict[str, Any]]:
        """
        Get financial summary for a committee.
        
        Returns dict with total_receipts, total_disbursements, cash_on_hand, etc.
        """
        if cycle not in self.committee_summaries:
            self.load_committee_summaries(cycle)
        
        return self.committee_summaries.get(cycle, {}).get(committee_id)
    
    def load_pac_summaries(self, cycle: int, force_refresh: bool = False) -> bool:
        """
        Load PAC/party financial summary data for a cycle.
        
        Downloads webk[YY].zip (~50MB) and parses financial totals for PACs and parties.
        
        Args:
            cycle: Election cycle year
            force_refresh: If True, redownload files even if cached
        
        Returns:
            True if successful, False if error
        """
        if cycle in self.pac_summaries and not force_refresh:
            logger.info(f"PAC summaries for cycle {cycle} already loaded in memory")
            return True
        
        logger.info(f"Loading PAC/party financial summaries for cycle {cycle}...")
        
        year_suffix = str(cycle)[-2:]
        filename = f"webk{year_suffix}.zip"
        
        zip_bytes = download_bulk_zip(cycle, filename, force_refresh=force_refresh)
        if not zip_bytes:
            logger.error(f"Failed to download {filename}")
            return False
        
        summaries = parse_pac_summary(zip_bytes)
        if not summaries:
            logger.error(f"Failed to parse {filename}")
            return False
        
        # Build lookup dict by committee_id
        self.pac_summaries[cycle] = {
            s["committee_id"]: s for s in summaries
        }
        
        logger.info(f"✅ Loaded {len(summaries)} PAC/party financial summaries for cycle {cycle}")
        return True
    
    def get_pac_summary(
        self,
        committee_id: str,
        cycle: int
    ) -> Optional[Dict[str, Any]]:
        """Get financial summary for a PAC or party committee."""
        if cycle not in self.pac_summaries:
            self.load_pac_summaries(cycle)
        
        return self.pac_summaries.get(cycle, {}).get(committee_id)
    
    def load_committee_transfers(
        self,
        cycle: int,
        max_records: Optional[int] = None,
        force_refresh: bool = False
    ) -> bool:
        """
        Load committee-to-committee transfer data for a cycle.
        
        This is CRITICAL for tracking money flow networks.
        
        WARNING: This file is LARGE (~1-2 GB, hundreds of thousands of records).
        Use max_records for testing, or None to load everything.
        
        Args:
            cycle: Election cycle year
            max_records: Limit number of records (for testing)
            force_refresh: If True, redownload files even if cached
        
        Returns:
            True if successful, False if error
        """
        if cycle in self.committee_transfers_from and not force_refresh:
            logger.info(f"Committee transfers for cycle {cycle} already loaded in memory")
            return True
        
        logger.info(f"Loading committee-to-committee transfers for cycle {cycle}...")
        if max_records:
            logger.info(f"  Limited to first {max_records:,} records")
        else:
            logger.warning("  Loading ALL records - this will take several minutes!")
        
        year_suffix = str(cycle)[-2:]
        filename = f"pas2{year_suffix}.zip"
        
        zip_bytes = download_bulk_zip(cycle, filename, force_refresh=force_refresh)
        if not zip_bytes:
            logger.error(f"Failed to download {filename}")
            return False
        
        transfers = parse_committee_transfers(zip_bytes, max_records=max_records)
        if not transfers:
            logger.error(f"Failed to parse {filename}")
            return False
        
        # Group by donor (from) and recipient (to) committee
        logger.info("Indexing transfers by donor and recipient committees...")
        by_donor = {}
        by_recipient = {}
        
        for transfer in transfers:
            donor_id = transfer["donor_committee_id"]
            recipient_id = transfer["recipient_committee_id"]
            
            # Index by donor (who gave money)
            if donor_id:
                if donor_id not in by_donor:
                    by_donor[donor_id] = []
                by_donor[donor_id].append(transfer)
            
            # Index by recipient (who received money)
            if recipient_id:
                if recipient_id not in by_recipient:
                    by_recipient[recipient_id] = []
                by_recipient[recipient_id].append(transfer)
        
        self.committee_transfers_from[cycle] = by_donor
        self.committee_transfers_to[cycle] = by_recipient
        
        logger.info(f"✅ Loaded {len(transfers):,} committee transfers")
        logger.info(f"   Indexed {len(by_donor)} donor committees and {len(by_recipient)} recipient committees")
        return True
    
    def get_transfers_from_committee(
        self,
        committee_id: str,
        cycle: int
    ) -> List[Dict[str, Any]]:
        """
        Get all transfers FROM a committee (money they gave to others).
        
        Note: Must call load_committee_transfers() first.
        """
        if cycle not in self.committee_transfers_from:
            logger.warning(f"Committee transfers for cycle {cycle} not loaded. Call load_committee_transfers() first.")
            return []
        
        return self.committee_transfers_from.get(cycle, {}).get(committee_id, [])
    
    def get_transfers_to_committee(
        self,
        committee_id: str,
        cycle: int
    ) -> List[Dict[str, Any]]:
        """
        Get all transfers TO a committee (money they received from others).
        
        Note: Must call load_committee_transfers() first.
        """
        if cycle not in self.committee_transfers_to:
            logger.warning(f"Committee transfers for cycle {cycle} not loaded. Call load_committee_transfers() first.")
            return []
        
        return self.committee_transfers_to.get(cycle, {}).get(committee_id, [])


# ============================================================================
# FINANCIAL DATA FILES (ZIP format)
# ============================================================================

def download_bulk_zip(cycle: int, filename: str, force_refresh: bool = False) -> Optional[bytes]:
    """
    Download a bulk ZIP file from FEC with caching support.
    
    Args:
        cycle: Election cycle year (e.g., 2024, 2026)
        filename: File to download (e.g., 'weball24.zip', 'indiv24.zip')
        force_refresh: If True, download fresh even if cached. If False, use cache if available.
                      Can also be controlled via FEC_FORCE_REFRESH environment variable.
    
    Returns:
        Raw bytes of the ZIP file, or None if error
    """
    # Check environment variable for force refresh
    env_force_refresh = os.getenv("FEC_FORCE_REFRESH", "false").lower() == "true"
    force_refresh = env_force_refresh or force_refresh
    
    cache_path = os.path.join(CACHE_DIR, f"{cycle}_{filename}")
    
    # Use cached version if exists and not forcing refresh
    if not force_refresh and os.path.exists(cache_path):
        logger.info(f"📂 Using cached {filename} from {cache_path}")
        try:
            with open(cache_path, 'rb') as f:
                content = f.read()
            logger.info(f"   Loaded {len(content)/(1024*1024):.1f}MB from cache")
            return content
        except Exception as e:
            logger.warning(f"⚠️  Error reading cache {cache_path}: {e}, will download fresh")
            # Fall through to download
    
    # Download fresh from FEC
    url = f"{BASE_URL}/{cycle}/{filename}"
    logger.info(f"⬇️  Downloading {filename} from {url}...")
    
    try:
        resp = requests.get(url, timeout=300, stream=True)  # 5 min timeout for large files
        resp.raise_for_status()
        
        # Download in chunks to handle large files
        content = b""
        total_size = int(resp.headers.get('content-length', 0))
        downloaded = 0
        
        for chunk in resp.iter_content(chunk_size=1024*1024):  # 1MB chunks
            if chunk:
                content += chunk
                downloaded += len(chunk)
                if total_size > 0:
                    pct = (downloaded / total_size) * 100
                    if downloaded % (10 * 1024 * 1024) == 0:  # Log every 10MB
                        logger.info(f"  Downloaded {downloaded/(1024*1024):.1f}MB / {total_size/(1024*1024):.1f}MB ({pct:.1f}%)")
        
        logger.info(f"✅ Downloaded {filename} ({len(content)/(1024*1024):.1f}MB)")
        
        # Save to cache for future use
        try:
            with open(cache_path, 'wb') as f:
                f.write(content)
            logger.info(f"💾 Cached to {cache_path}")
        except Exception as e:
            logger.warning(f"⚠️  Could not cache {filename}: {e}")
        
        return content
    except Exception as e:
        logger.error(f"❌ Error downloading {url}: {e}")
        return None


def parse_committee_summary(zip_bytes: bytes) -> List[Dict[str, Any]]:
    """
    Parse webl[YY].zip (committee summary file).
    
    Contains aggregated financial totals for ALL committees (not just candidate committees):
    - CMTE_ID: Committee ID
    - CMTE_NM: Committee name
    - CMTE_TP: Committee type
    - CMTE_DSGN: Committee designation
    - CMTE_PTY_AFFILIATION: Party affiliation
    - TTL_RECEIPTS: Total receipts
    - TRANS_FROM_AFF: Transfers from affiliates
    - INDV_CONTRIB: Individual contributions
    - OTHER_POL_CMTE_CONTRIB: Other political committee contributions
    - CAND_CONTRIB: Candidate contributions
    - CAND_LOANS: Candidate loans
    - TTL_LOANS: Total loans
    - TTL_DISB: Total disbursements
    - TRANF_TO_AFF: Transfers to affiliates
    - INDV_REFUNDS: Individual refunds
    - OTHER_POL_CMTE_REFUNDS: Other political committee refunds
    - CAND_LOAN_REPAY: Candidate loan repayments
    - LOAN_REPAY: Loan repayments
    - COH_BOP: Cash on hand beginning of period
    - COH_COP: Cash on hand close of period
    - DEBTS_OWED_BY: Debts owed by committee
    - NONFED_TRANS_RECEIVED: Non-federal transfers received
    - CONTRIB_TO_OTHER_CMTE: Contributions to other committees
    - IND_EXP: Independent expenditures
    - PTY_COORD_EXP: Party coordinated expenditures
    - NONFED_SHARE_EXP: Non-federal share of expenditures
    - CVG_END_DT: Coverage end date
    
    Returns:
        List of committee summary dicts
    """
    summaries = []
    
    try:
        with zipfile.ZipFile(BytesIO(zip_bytes)) as zf:
            txt_files = [f for f in zf.namelist() if f.endswith('.txt')]
            if not txt_files:
                logger.error("No .txt file found in committee summary ZIP")
                return []
            
            with zf.open(txt_files[0]) as f:
                text = f.read().decode('utf-8', errors='ignore')
                reader = csv.DictReader(StringIO(text), delimiter='|')
                
                for row in reader:
                    summaries.append({
                        "committee_id": row.get("CMTE_ID", "").strip(),
                        "name": row.get("CMTE_NM", "").strip(),
                        "type": row.get("CMTE_TP", "").strip(),
                        "designation": row.get("CMTE_DSGN", "").strip(),
                        "party": row.get("CMTE_PTY_AFFILIATION", "").strip(),
                        "total_receipts": float(row.get("TTL_RECEIPTS", "0") or "0"),
                        "total_disbursements": float(row.get("TTL_DISB", "0") or "0"),
                        "cash_on_hand": float(row.get("COH_COP", "0") or "0"),
                        "debts_owed": float(row.get("DEBTS_OWED_BY", "0") or "0"),
                        "individual_contributions": float(row.get("INDV_CONTRIB", "0") or "0"),
                        "other_political_committee_contributions": float(row.get("OTHER_POL_CMTE_CONTRIB", "0") or "0"),
                        "candidate_contributions": float(row.get("CAND_CONTRIB", "0") or "0"),
                        "transfers_from_affiliates": float(row.get("TRANS_FROM_AFF", "0") or "0"),
                        "transfers_to_affiliates": float(row.get("TRANF_TO_AFF", "0") or "0"),
                        "contributions_to_other_committees": float(row.get("CONTRIB_TO_OTHER_CMTE", "0") or "0"),
                        "independent_expenditures": float(row.get("IND_EXP", "0") or "0"),
                        "coordinated_expenditures": float(row.get("PTY_COORD_EXP", "0") or "0"),
                        "coverage_end_date": row.get("CVG_END_DT", "").strip(),
                    })
        
        logger.info(f"Parsed {len(summaries)} committee financial summaries")
        return summaries
    except Exception as e:
        logger.error(f"Error parsing committee summary ZIP: {e}")
        return []


def parse_pac_summary(zip_bytes: bytes) -> List[Dict[str, Any]]:
    """
    Parse webk[YY].zip (PAC/Party summary file).
    
    Similar to committee summaries but specifically for PACs and party committees.
    Contains aggregated financial totals.
    
    Returns:
        List of PAC/party summary dicts
    """
    summaries = []
    
    try:
        with zipfile.ZipFile(BytesIO(zip_bytes)) as zf:
            txt_files = [f for f in zf.namelist() if f.endswith('.txt')]
            if not txt_files:
                logger.error("No .txt file found in PAC summary ZIP")
                return []
            
            with zf.open(txt_files[0]) as f:
                text = f.read().decode('utf-8', errors='ignore')
                reader = csv.DictReader(StringIO(text), delimiter='|')
                
                for row in reader:
                    summaries.append({
                        "committee_id": row.get("CMTE_ID", "").strip(),
                        "name": row.get("CMTE_NM", "").strip(),
                        "type": row.get("CMTE_TP", "").strip(),
                        "designation": row.get("CMTE_DSGN", "").strip(),
                        "party": row.get("CMTE_PTY_AFFILIATION", "").strip(),
                        "total_receipts": float(row.get("TTL_RECEIPTS", "0") or "0"),
                        "total_disbursements": float(row.get("TTL_DISB", "0") or "0"),
                        "cash_on_hand": float(row.get("COH_COP", "0") or "0"),
                        "debts_owed": float(row.get("DEBTS_OWED_BY", "0") or "0"),
                        "individual_contributions": float(row.get("INDV_CONTRIB", "0") or "0"),
                        "contributions_to_committees": float(row.get("CONTRIB_TO_OTHER_CMTE", "0") or "0"),
                        "independent_expenditures": float(row.get("IND_EXP", "0") or "0"),
                        "coverage_end_date": row.get("CVG_END_DT", "").strip(),
                    })
        
        logger.info(f"Parsed {len(summaries)} PAC/party financial summaries")
        return summaries
    except Exception as e:
        logger.error(f"Error parsing PAC summary ZIP: {e}")
        return []


def parse_committee_transfers(zip_bytes: bytes, max_records: Optional[int] = None) -> List[Dict[str, Any]]:
    """
    Parse pas2[YY].zip (committee-to-committee transfers file).
    
    This is CRITICAL for tracking money flow between committees.
    Shows how PACs fund each other, party committee networks, etc.
    
    WARNING: This file is LARGE (~1-2 GB, hundreds of thousands of records).
    Use max_records for testing, or None to load everything.
    
    Contains committee transfer transactions:
    - CMTE_ID: Filing committee ID (recipient)
    - AMNDT_IND: Amendment indicator
    - RPT_TP: Report type
    - TRANSACTION_PGI: Primary/General indicator
    - IMAGE_NUM: Image number
    - TRANSACTION_TP: Transaction type
    - ENTITY_TP: Entity type
    - NAME: Contributor/Transferor committee name
    - CITY: City
    - STATE: State
    - ZIP_CODE: ZIP code
    - EMPLOYER: Employer (usually blank for committees)
    - OCCUPATION: Occupation (usually blank for committees)
    - TRANSACTION_DT: Transaction date (MMDDYYYY)
    - TRANSACTION_AMT: Transaction amount
    - OTHER_ID: Other committee ID (transferor)
    - TRAN_ID: Transaction ID
    - FILE_NUM: File number
    - MEMO_CD: Memo code
    - MEMO_TEXT: Memo text
    - SUB_ID: Submission ID
    
    Returns:
        List of committee transfer dicts
    """
    transfers = []
    
    try:
        with zipfile.ZipFile(BytesIO(zip_bytes)) as zf:
            txt_files = [f for f in zf.namelist() if f.endswith('.txt')]
            if not txt_files:
                logger.error("No .txt file found in committee transfers ZIP")
                return []
            
            with zf.open(txt_files[0]) as f:
                text = f.read().decode('utf-8', errors='ignore')
                reader = csv.DictReader(StringIO(text), delimiter='|')
                
                for idx, row in enumerate(reader):
                    if max_records and idx >= max_records:
                        logger.info(f"Reached max_records limit ({max_records})")
                        break
                    
                    transfers.append({
                        "recipient_committee_id": row.get("CMTE_ID", "").strip(),
                        "donor_committee_id": row.get("OTHER_ID", "").strip(),
                        "donor_committee_name": row.get("NAME", "").strip(),
                        "transaction_type": row.get("TRANSACTION_TP", "").strip(),
                        "entity_type": row.get("ENTITY_TP", "").strip(),
                        "city": row.get("CITY", "").strip(),
                        "state": row.get("STATE", "").strip(),
                        "transaction_date": row.get("TRANSACTION_DT", "").strip(),
                        "amount": float(row.get("TRANSACTION_AMT", "0") or "0"),
                        "election_type": row.get("TRANSACTION_PGI", "").strip(),
                        "transaction_id": row.get("TRAN_ID", "").strip(),
                        "file_number": row.get("FILE_NUM", "").strip(),
                        "memo_text": row.get("MEMO_TEXT", "").strip(),
                    })
                    
                    if idx % 50000 == 0 and idx > 0:
                        logger.info(f"  Parsed {idx:,} committee transfer records...")
        
        logger.info(f"Parsed {len(transfers):,} committee transfers")
        return transfers
    except Exception as e:
        logger.error(f"Error parsing committee transfers ZIP: {e}")
        return []


def parse_candidate_summary(zip_bytes: bytes) -> List[Dict[str, Any]]:
    """
    Parse weball[YY].zip (candidate summary file).
    
    Contains aggregated financial totals for each candidate:
    - CAND_ID: Candidate ID
    - CAND_NAME: Candidate name
    - CAND_ICI: Incumbent/Challenger/Open
    - PTY_CD: Party code
    - CAND_PTY_AFFILIATION: Party affiliation
    - TTL_RECEIPTS: Total receipts
    - TRANS_FROM_AUTH: Transfers from authorized committees
    - TTL_DISB: Total disbursements
    - TRANS_TO_AUTH: Transfers to authorized committees
    - COH_BOP: Cash on hand beginning of period
    - COH_COP: Cash on hand close of period
    - CAND_CONTRIB: Candidate contributions
    - CAND_LOANS: Candidate loans
    - OTHER_LOANS: Other loans
    - CAND_LOAN_REPAY: Candidate loan repayments
    - OTHER_LOAN_REPAY: Other loan repayments
    - DEBTS_OWED_BY: Debts owed by candidate
    - TTL_INDIV_CONTRIB: Total individual contributions
    - CAND_OFFICE_ST: State
    - CAND_OFFICE_DISTRICT: District
    - SPEC_ELECTION: Special election status
    - PRIM_ELECTION: Primary election status
    - RUN_ELECTION: Runoff election status
    - GEN_ELECTION: General election status
    - GEN_ELECTION_PRECENT: General election percent
    - OTHER_POL_CMTE_CONTRIB: Contributions from other political committees
    - POL_PTY_CONTRIB: Political party contributions
    - CVG_END_DT: Coverage end date
    - INDIV_REFUNDS: Individual refunds
    - CMTE_REFUNDS: Committee refunds
    
    Returns:
        List of candidate summary dicts
    """
    summaries = []
    
    try:
        with zipfile.ZipFile(BytesIO(zip_bytes)) as zf:
            # Find the .txt file inside (usually weballYY.txt)
            txt_files = [f for f in zf.namelist() if f.endswith('.txt')]
            if not txt_files:
                logger.error("No .txt file found in candidate summary ZIP")
                return []
            
            with zf.open(txt_files[0]) as f:
                text = f.read().decode('utf-8', errors='ignore')
                reader = csv.DictReader(StringIO(text), delimiter='|')
                
                for row in reader:
                    summaries.append({
                        "candidate_id": row.get("CAND_ID", "").strip(),
                        "name": row.get("CAND_NAME", "").strip(),
                        "incumbent_challenger": row.get("CAND_ICI", "").strip(),
                        "party": row.get("PTY_CD", "").strip(),
                        "state": row.get("CAND_OFFICE_ST", "").strip(),
                        "district": row.get("CAND_OFFICE_DISTRICT", "").strip(),
                        "total_receipts": float(row.get("TTL_RECEIPTS", "0") or "0"),
                        "total_disbursements": float(row.get("TTL_DISB", "0") or "0"),
                        "cash_on_hand": float(row.get("COH_COP", "0") or "0"),
                        "debts_owed": float(row.get("DEBTS_OWED_BY", "0") or "0"),
                        "total_individual_contributions": float(row.get("TTL_INDIV_CONTRIB", "0") or "0"),
                        "candidate_contributions": float(row.get("CAND_CONTRIB", "0") or "0"),
                        "candidate_loans": float(row.get("CAND_LOANS", "0") or "0"),
                        "other_political_committee_contributions": float(row.get("OTHER_POL_CMTE_CONTRIB", "0") or "0"),
                        "political_party_contributions": float(row.get("POL_PTY_CONTRIB", "0") or "0"),
                        "coverage_end_date": row.get("CVG_END_DT", "").strip(),
                    })
        
        logger.info(f"Parsed {len(summaries)} candidate financial summaries")
        return summaries
    except Exception as e:
        logger.error(f"Error parsing candidate summary ZIP: {e}")
        return []


def parse_independent_expenditures(zip_bytes: bytes, max_records: Optional[int] = None) -> List[Dict[str, Any]]:
    """
    Parse oppexp[YY].zip (independent expenditures / operating expenditures file).
    
    This is THE KEY FILE for tracking Super PAC spending (like AIPAC/UDP).
    
    WARNING: This file is LARGE (~2-3 GB, millions of records).
    Use max_records to limit parsing for testing.
    
    Contains independent expenditure transactions (Schedule E):
    - CMTE_ID: Committee ID (spender - e.g., AIPAC's UDP)
    - AMNDT_IND: Amendment indicator
    - RPT_YR: Report year
    - RPT_TP: Report type
    - IMAGE_NUM: Image number
    - LINE_NUM: Line number
    - FORM_TP_CD: Form type
    - SCHED_TP_CD: Schedule type
    - NAME: Payee name (who got paid)
    - CITY: City
    - STATE: State
    - ZIP_CODE: ZIP code
    - TRANSACTION_DT: Transaction date (MMDDYYYY)
    - TRANSACTION_AMT: Transaction amount
    - TRANSACTION_PGI: Election type (P=Primary, G=General, etc.)
    - PURPOSE: Purpose/description of expenditure
    - CATEGORY: Category
    - CATEGORY_DESC: Category description
    - MEMO_CD: Memo code
    - MEMO_TEXT: Memo text
    - ENTITY_TP: Entity type
    - SUB_ID: Submission ID
    - FILE_NUM: File number
    - TRAN_ID: Transaction ID
    - BACK_REF_TRAN_ID: Back reference transaction ID
    - BACK_REF_SCHED_NM: Back reference schedule name
    - CAND_ID: Candidate ID (WHO THE MONEY IS FOR/AGAINST)
    - CAND_NAME: Candidate name
    - CAND_OFFICE: Candidate office (H/S/P)
    - CAND_OFFICE_ST: Candidate state
    - CAND_OFFICE_DISTRICT: Candidate district
    - CONDUIT_CMTE_ID: Conduit committee ID
    - CONDUIT_CMTE_NM: Conduit committee name
    - CONDUIT_CMTE_ST1: Conduit committee street
    - CONDUIT_CMTE_ST2: Conduit committee street 2
    - CONDUIT_CMTE_CITY: Conduit committee city
    - CONDUIT_CMTE_ST: Conduit committee state
    - CONDUIT_CMTE_ZIP: Conduit committee ZIP
    - SUPPORT_OPPOSE_INDICATOR: 'S' = Support, 'O' = Oppose (CRITICAL!)
    
    Returns:
        List of independent expenditure dicts
    """
    expenditures = []
    
    try:
        with zipfile.ZipFile(BytesIO(zip_bytes)) as zf:
            txt_files = [f for f in zf.namelist() if f.endswith('.txt')]
            if not txt_files:
                logger.error("No .txt file found in independent expenditures ZIP")
                return []
            
            with zf.open(txt_files[0]) as f:
                text = f.read().decode('utf-8', errors='ignore')
                reader = csv.DictReader(StringIO(text), delimiter='|')
                
                for idx, row in enumerate(reader):
                    if max_records and idx >= max_records:
                        logger.info(f"Reached max_records limit ({max_records})")
                        break
                    
                    expenditures.append({
                        "committee_id": row.get("CMTE_ID", "").strip(),
                        "committee_name": row.get("CMTE_NM", "").strip(),  # May not be in all versions
                        "payee_name": row.get("NAME", "").strip(),
                        "city": row.get("CITY", "").strip(),
                        "state": row.get("STATE", "").strip(),
                        "transaction_date": row.get("TRANSACTION_DT", "").strip(),
                        "amount": float(row.get("TRANSACTION_AMT", "0") or "0"),
                        "election_type": row.get("TRANSACTION_PGI", "").strip(),
                        "purpose": row.get("PURPOSE", "").strip(),
                        "candidate_id": row.get("CAND_ID", "").strip(),
                        "candidate_name": row.get("CAND_NAME", "").strip(),
                        "candidate_office": row.get("CAND_OFFICE", "").strip(),
                        "candidate_state": row.get("CAND_OFFICE_ST", "").strip(),
                        "candidate_district": row.get("CAND_OFFICE_DISTRICT", "").strip(),
                        "support_oppose": row.get("SUPPORT_OPPOSE_INDICATOR", "").strip(),  # S or O
                        "memo_text": row.get("MEMO_TEXT", "").strip(),
                    })
                    
                    if idx % 100000 == 0 and idx > 0:
                        logger.info(f"  Parsed {idx:,} independent expenditure records...")
        
        logger.info(f"Parsed {len(expenditures):,} independent expenditures")
        return expenditures
    except Exception as e:
        logger.error(f"Error parsing independent expenditures ZIP: {e}")
        return []


def parse_individual_contributions(zip_bytes: bytes, max_records: Optional[int] = None) -> List[Dict[str, Any]]:
    """
    Parse indiv[YY].zip (individual contributions file).
    
    WARNING: This file is HUGE (4+ GB, millions of records).
    Use max_records to limit parsing for testing.
    
    Contains individual contribution transactions:
    - CMTE_ID: Committee ID (recipient)
    - AMNDT_IND: Amendment indicator
    - RPT_TP: Report type
    - TRANSACTION_PGI: Primary/General indicator
    - IMAGE_NUM: Microfilm location
    - TRANSACTION_TP: Transaction type
    - ENTITY_TP: Entity type (IND, ORG, PAC, etc.)
    - NAME: Contributor name
    - CITY: City
    - STATE: State
    - ZIP_CODE: ZIP code
    - EMPLOYER: Employer
    - OCCUPATION: Occupation
    - TRANSACTION_DT: Transaction date (MMDDYYYY)
    - TRANSACTION_AMT: Transaction amount
    - OTHER_ID: Other ID (for PACs, candidate ID)
    - TRAN_ID: Transaction ID
    - FILE_NUM: File number
    - MEMO_CD: Memo code
    - MEMO_TEXT: Memo text
    - SUB_ID: Submission ID
    
    Returns:
        List of contribution dicts
    """
    contributions = []
    
    try:
        with zipfile.ZipFile(BytesIO(zip_bytes)) as zf:
            txt_files = [f for f in zf.namelist() if f.endswith('.txt')]
            if not txt_files:
                logger.error("No .txt file found in individual contributions ZIP")
                return []
            
            with zf.open(txt_files[0]) as f:
                text = f.read().decode('utf-8', errors='ignore')
                reader = csv.DictReader(StringIO(text), delimiter='|')
                
                for idx, row in enumerate(reader):
                    if max_records and idx >= max_records:
                        logger.info(f"Reached max_records limit ({max_records})")
                        break
                    
                    contributions.append({
                        "committee_id": row.get("CMTE_ID", "").strip(),
                        "transaction_type": row.get("TRANSACTION_TP", "").strip(),
                        "entity_type": row.get("ENTITY_TP", "").strip(),
                        "contributor_name": row.get("NAME", "").strip(),
                        "city": row.get("CITY", "").strip(),
                        "state": row.get("STATE", "").strip(),
                        "zip_code": row.get("ZIP_CODE", "").strip(),
                        "employer": row.get("EMPLOYER", "").strip(),
                        "occupation": row.get("OCCUPATION", "").strip(),
                        "transaction_date": row.get("TRANSACTION_DT", "").strip(),
                        "amount": float(row.get("TRANSACTION_AMT", "0") or "0"),
                        "transaction_id": row.get("TRAN_ID", "").strip(),
                        "file_number": row.get("FILE_NUM", "").strip(),
                        "memo_text": row.get("MEMO_TEXT", "").strip(),
                    })
                    
                    if idx % 100000 == 0 and idx > 0:
                        logger.info(f"  Parsed {idx:,} contribution records...")
        
        logger.info(f"Parsed {len(contributions):,} individual contributions")
        return contributions
    except Exception as e:
        logger.error(f"Error parsing individual contributions ZIP: {e}")
        return []


# Global cache instance
_cache = None

def get_cache() -> FECBulkDataCache:
    """Get or create the global bulk data cache."""
    global _cache
    if _cache is None:
        _cache = FECBulkDataCache()
    return _cache
