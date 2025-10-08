
import requests
import logging
from typing import Optional, Dict, Any, List
from src.config import FEC_API_KEY

BASE_URL = "https://api.open.fec.gov/v1/"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("fec_api")

def get_candidate_committees(candidate_id: str, cycle: int = 2024) -> Optional[List[str]]:
    """
    Get committee IDs for a candidate by FEC candidate ID.
    Args:
        candidate_id: FEC candidate ID string.
        cycle: Election cycle year (e.g., 2024).
    Returns:
        List of committee IDs, or None if error.
    """
    url = f"{BASE_URL}candidate/{candidate_id}/committees/"
    params = {
        "api_key": FEC_API_KEY,
        "cycle": cycle,
        "per_page": 20
    }
    try:
        resp = requests.get(url, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        committees = [c["committee_id"] for c in data.get("results", [])]
        return committees
    except Exception as e:
        logger.error(f"Error fetching candidate committees: {e}")
        return None

def get_committee_contributions(committee_id: str, cycle: int = 2024, per_page: int = 20) -> Optional[Dict[str, Any]]:
    """
    Get contributions for a committee by FEC committee ID.
    Args:
        committee_id: FEC committee ID string.
        cycle: Election cycle year (e.g., 2024).
        per_page: Number of results per page.
    Returns:
        JSON response as dict, or None if error.
    """
    url = f"{BASE_URL}schedules/schedule_a/"
    params = {
        "api_key": FEC_API_KEY,
        "committee_id": committee_id,
        "cycle": cycle,
        "per_page": per_page
    }
    try:
        resp = requests.get(url, params=params, timeout=10)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        logger.error(f"Error fetching committee contributions: {e}")
        return None
