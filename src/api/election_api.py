
import requests
import logging
from typing import Optional, Dict, Any, List, Generator
import os
import time
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

ELECTION_API_KEY = os.getenv("ELECTION_API_KEY")

BASE_URL = "https://api.open.fec.gov/v1/"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("election_api")


def search_candidate_by_name(name: str, state: Optional[str] = None, office: Optional[str] = None) -> Optional[List[Dict[str, Any]]]:
    """
    Search for candidates by name to get their FEC candidate_id.
    
    Args:
        name: Candidate name (last name or full name)
        state: Two-letter state code (e.g., 'CA')
        office: Office type - 'H' (House), 'S' (Senate), 'P' (President)
    
    Returns:
        List of candidate records with candidate_id, or None if error
    """
    url = f"{BASE_URL}candidates/search/"
    params = {
        "api_key": ELECTION_API_KEY,
        "name": name,
        "is_active_candidate": "true",
        "per_page": 10
    }
    
    if state:
        params["state"] = state
    if office:
        params["office"] = office
    
    try:
        resp = requests.get(url, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        return data.get("results", [])
    except Exception as e:
        logger.error(f"Error searching candidate by name '{name}': {e}")
        return None


def get_candidate_committees(candidate_id: str, cycle: int = 2024) -> Optional[List[Dict[str, Any]]]:
    """
    Get all committees for a candidate (campaign committees, leadership PACs, etc).
    
    Args:
        candidate_id: FEC candidate ID string
        cycle: Election cycle year (e.g., 2024)
    
    Returns:
        List of committee records with committee_id and details, or None if error
    """
    url = f"{BASE_URL}candidate/{candidate_id}/committees/"
    params = {
        "api_key": ELECTION_API_KEY,
        "cycle": cycle,
        "per_page": 100
    }
    try:
        resp = requests.get(url, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        return data.get("results", [])
    except Exception as e:
        logger.error(f"Error fetching candidate committees for {candidate_id}: {e}")
        return None


def get_committee_contributions_paginated(
    committee_id: str,
    two_year_period: int = 2024,
    max_pages: Optional[int] = None,
    per_page: int = 100,
) -> Generator[Dict[str, Any], None, None]:
    """
    Get ALL contributions for a committee with automatic pagination.
    
    WARNING: This can be A LOT of data. Nancy Pelosi has 238k+ contributions.
    Use max_pages to limit for testing.
    
    Args:
        committee_id: FEC committee ID string
        two_year_period: Two-year election period (e.g., 2024 for 2023-2024)
        max_pages: Maximum pages to fetch (None = all pages)
        per_page: Results per page (max 100)
    
    Yields:
        Individual contribution records
    """
    url = f"{BASE_URL}schedules/schedule_a/"
    page = 1
    
    while True:
        if max_pages and page > max_pages:
            logger.info(f"Reached max_pages limit ({max_pages})")
            break
        
        params = {
            "api_key": ELECTION_API_KEY,
            "committee_id": committee_id,
            "two_year_transaction_period": two_year_period,
            "per_page": per_page,
            "page": page
        }
        
        try:
            resp = requests.get(url, params=params, timeout=15)
            resp.raise_for_status()
            data = resp.json()
            
            results = data.get("results", [])
            if not results:
                break
            
            for contribution in results:
                yield contribution
            
            pagination = data.get("pagination", {})
            total_pages = pagination.get("pages", 0)
            
            logger.info(f"Fetched page {page}/{total_pages} for committee {committee_id}")
            
            if page >= total_pages:
                break
            
            page += 1
            
            # Rate limiting - be nice to the API
            time.sleep(0.2)
            
        except Exception as e:
            logger.error(f"Error fetching contributions page {page} for {committee_id}: {e}")
            break


def aggregate_contributions_by_donor(contributions: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """
    Aggregate a list of contributions by donor name.
    
    Args:
        contributions: List of contribution records from Schedule A
    
    Returns:
        Dict mapping donor_name -> {total_amount, count, employer, type, etc}
    """
    donor_totals = {}
    
    for contrib in contributions:
        # Use contributor name as key (we'll normalize later)
        name = contrib.get("contributor_name", "UNKNOWN").upper().strip()
        amount = contrib.get("contribution_receipt_amount", 0)
        date = contrib.get("contribution_receipt_date")
        entity_type = contrib.get("entity_type", "UNK")  # IND, PAC, ORG, COM
        employer = contrib.get("contributor_employer", "")
        
        if name not in donor_totals:
            donor_totals[name] = {
                "donor_name": name,
                "total_amount": 0,
                "contribution_count": 0,
                "donor_type": entity_type,
                "employer": employer,
                "first_contribution_date": date,
                "last_contribution_date": date,
            }
        
        donor_totals[name]["total_amount"] += amount
        donor_totals[name]["contribution_count"] += 1
        
        # Update date range
        if date:
            if not donor_totals[name]["first_contribution_date"] or date < donor_totals[name]["first_contribution_date"]:
                donor_totals[name]["first_contribution_date"] = date
            if not donor_totals[name]["last_contribution_date"] or date > donor_totals[name]["last_contribution_date"]:
                donor_totals[name]["last_contribution_date"] = date
    
    return donor_totals


def get_independent_expenditures_by_committee(
    committee_id: str,
    two_year_period: int = 2024,
    max_pages: Optional[int] = None,
    per_page: int = 100
) -> Generator[Dict[str, Any], None, None]:
    """
    Get ALL independent expenditures (Super PAC spending) made by a committee.
    
    This is CRITICAL for tracking organizations like AIPAC (United Democracy Project)
    which operate as Super PACs and DON'T make direct contributions.
    
    Use this to get ALL politicians that AIPAC has supported/opposed.
    
    Args:
        committee_id: FEC committee ID (e.g., 'C00799031' for UDP/AIPAC)
        two_year_period: Two-year election period (e.g., 2024 for 2023-2024)
        max_pages: Maximum pages to fetch (None = all pages)
        per_page: Results per page (max 100)
    
    Yields:
        Individual independent expenditure records with:
        - candidate_id, candidate_name (who the money was for/against)
        - expenditure_amount
        - support_oppose_indicator ('S' = support, 'O' = oppose)
        - expenditure_description (what it was for)
    """
    url = f"{BASE_URL}schedules/schedule_e/"
    page = 1
    
    while True:
        if max_pages and page > max_pages:
            logger.info(f"Reached max_pages limit ({max_pages})")
            break
        
        params = {
            "api_key": ELECTION_API_KEY,
            "committee_id": committee_id,
            "two_year_transaction_period": two_year_period,
            "per_page": per_page,
            "page": page
        }
        
        try:
            resp = requests.get(url, params=params, timeout=15)
            resp.raise_for_status()
            data = resp.json()
            
            results = data.get("results", [])
            if not results:
                break
            
            for expenditure in results:
                yield expenditure
            
            pagination = data.get("pagination", {})
            total_pages = pagination.get("pages", 0)
            
            logger.info(f"Fetched page {page}/{total_pages} of independent expenditures for committee {committee_id}")
            
            if page >= total_pages:
                break
            
            page += 1
            time.sleep(0.2)  # Rate limiting
            
        except Exception as e:
            logger.error(f"Error fetching independent expenditures page {page} for committee {committee_id}: {e}")
            break


def search_super_pac_by_name(name: str) -> Optional[List[Dict[str, Any]]]:
    """
    Search for Super PACs / PACs by name.
    
    Args:
        name: Committee name or partial name (e.g., 'AIPAC', 'United Democracy')
    
    Returns:
        List of committee records with committee_id, name, type
    """
    url = f"{BASE_URL}committees/"
    params = {
        "api_key": ELECTION_API_KEY,
        "q": name,
        "per_page": 20
    }
    
    try:
        resp = requests.get(url, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        return data.get("results", [])
    except Exception as e:
        logger.error(f"Error searching for committee '{name}': {e}")
        return None


def aggregate_independent_expenditures_by_candidate(expenditures: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """
    Aggregate independent expenditures by candidate.
    
    Use this to see which candidates a Super PAC (like AIPAC) has supported/opposed.
    
    Args:
        expenditures: List of expenditure records from Schedule E (from a single committee)
    
    Returns:
        Dict mapping candidate_name -> {
            support_amount, oppose_amount, net_support,
            support_count, oppose_count, candidate_id, candidate_office
        }
    """
    candidate_totals = {}
    
    for exp in expenditures:
        candidate_name = exp.get("candidate_name", "UNKNOWN").upper().strip()
        candidate_id = exp.get("candidate_id", "")
        candidate_office = exp.get("candidate_office", "")
        amount = exp.get("expenditure_amount", 0)
        support_oppose = exp.get("support_oppose_indicator", "?")
        
        if candidate_name not in candidate_totals:
            candidate_totals[candidate_name] = {
                "candidate_name": candidate_name,
                "candidate_id": candidate_id,
                "candidate_office": candidate_office,
                "support_amount": 0,
                "oppose_amount": 0,
                "support_count": 0,
                "oppose_count": 0,
                "net_support": 0,
            }
        
        if support_oppose == "S":
            candidate_totals[candidate_name]["support_amount"] += amount
            candidate_totals[candidate_name]["support_count"] += 1
        elif support_oppose == "O":
            candidate_totals[candidate_name]["oppose_amount"] += amount
            candidate_totals[candidate_name]["oppose_count"] += 1
        
        # Net support = money spent supporting minus money spent opposing
        candidate_totals[candidate_name]["net_support"] = (
            candidate_totals[candidate_name]["support_amount"] - 
            candidate_totals[candidate_name]["oppose_amount"]
        )
    
    return candidate_totals


def get_candidate_financial_summary(candidate_id: str, cycle: int = 2024) -> Optional[Dict[str, Any]]:
    """
    Get financial summary for a candidate (total raised, spent, cash on hand, debt).
    
    Args:
        candidate_id: FEC candidate ID string
        cycle: Election cycle year (e.g., 2024)
    
    Returns:
        Dict with financial totals, or None if error
    """
    url = f"{BASE_URL}candidate/{candidate_id}/totals/"
    params = {
        "api_key": ELECTION_API_KEY,
        "cycle": cycle
    }
    
    try:
        resp = requests.get(url, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        results = data.get("results", [])
        return results[0] if results else None
    except Exception as e:
        logger.error(f"Error fetching financial summary for {candidate_id}: {e}")
        return None


def get_independent_expenditures_for_candidate(
    candidate_id: str,
    two_year_period: int = 2024,
    max_pages: Optional[int] = None,
    per_page: int = 100
) -> Generator[Dict[str, Any], None, None]:
    """
    Get ALL independent expenditures (Super PAC spending) FOR or AGAINST a specific candidate.
    
    This shows which Super PACs are trying to help or defeat this candidate.
    
    Args:
        candidate_id: FEC candidate ID string
        two_year_period: Two-year election period (e.g., 2024 for 2023-2024)
        max_pages: Maximum pages to fetch (None = all pages)
        per_page: Results per page (max 100)
    
    Yields:
        Individual independent expenditure records with:
        - committee_id, committee_name (who spent the money)
        - expenditure_amount
        - support_oppose_indicator ('S' = support, 'O' = oppose)
        - expenditure_description (what it was for)
    """
    url = f"{BASE_URL}schedules/schedule_e/"
    page = 1
    
    while True:
        if max_pages and page > max_pages:
            logger.info(f"Reached max_pages limit ({max_pages})")
            break
        
        params = {
            "api_key": ELECTION_API_KEY,
            "candidate_id": candidate_id,
            "two_year_transaction_period": two_year_period,
            "per_page": per_page,
            "page": page
        }
        
        try:
            resp = requests.get(url, params=params, timeout=15)
            resp.raise_for_status()
            data = resp.json()
            
            results = data.get("results", [])
            if not results:
                break
            
            for expenditure in results:
                yield expenditure
            
            pagination = data.get("pagination", {})
            total_pages = pagination.get("pages", 0)
            
            logger.info(f"Fetched page {page}/{total_pages} of independent expenditures for candidate {candidate_id}")
            
            if page >= total_pages:
                break
            
            page += 1
            time.sleep(0.2)  # Rate limiting
            
        except Exception as e:
            logger.error(f"Error fetching independent expenditures page {page} for candidate {candidate_id}: {e}")
            break


def aggregate_independent_expenditures_by_committee(expenditures: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """
    Aggregate independent expenditures by committee (Super PAC).
    
    Use this to see which Super PACs spent money on a candidate.
    
    Args:
        expenditures: List of expenditure records from Schedule E (for a single candidate)
    
    Returns:
        Dict mapping committee_name -> {
            support_amount, oppose_amount, net_support,
            support_count, oppose_count, committee_id
        }
    """
    committee_totals = {}
    
    for exp in expenditures:
        committee_name = exp.get("committee_name", "UNKNOWN").upper().strip()
        committee_id = exp.get("committee_id", "")
        amount = exp.get("expenditure_amount", 0)
        support_oppose = exp.get("support_oppose_indicator", "?")
        
        if committee_name not in committee_totals:
            committee_totals[committee_name] = {
                "committee_name": committee_name,
                "committee_id": committee_id,
                "support_amount": 0,
                "oppose_amount": 0,
                "support_count": 0,
                "oppose_count": 0,
                "net_support": 0,
            }
        
        if support_oppose == "S":
            committee_totals[committee_name]["support_amount"] += amount
            committee_totals[committee_name]["support_count"] += 1
        elif support_oppose == "O":
            committee_totals[committee_name]["oppose_amount"] += amount
            committee_totals[committee_name]["oppose_count"] += 1
        
        # Net support = money spent supporting minus money spent opposing
        committee_totals[committee_name]["net_support"] = (
            committee_totals[committee_name]["support_amount"] - 
            committee_totals[committee_name]["oppose_amount"]
        )
    
    return committee_totals
