import requests
import logging
from typing import Optional, Dict, Any
from src.config import CONGRESS_API_KEY

BASE_URL = "https://api.congress.gov/v3"
headers = {"X-Api-Key": CONGRESS_API_KEY}

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("congress_api")

def get_members(congress: str = "118", chamber: str = "house") -> Optional[Dict[str, Any]]:
    """
    Get a list of members of Congress for a given session and chamber.
    Args:
        congress: Congress session number as string (e.g., "118").
        chamber: "house" or "senate".
    Returns:
        JSON response as dict, or None if error.
    """
    url = f"{BASE_URL}/member"
    params = {"congress": congress, "chamber": chamber, "limit": 250}
    try:
        resp = requests.get(url, headers=headers, params=params, timeout=10)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        logger.error(f"Error fetching members: {e}")
        return None

def get_bills(congress: str = "118", chamber: str = "house") -> Optional[Dict[str, Any]]:
    """
    Get a list of bills for a given session and chamber.
    Args:
        congress: Congress session number as string (e.g., "118").
        chamber: "house" or "senate".
    Returns:
        JSON response as dict, or None if error.
    """
    url = f"{BASE_URL}/bill"
    params = {"congress": congress, "chamber": chamber, "limit": 250}
    try:
        resp = requests.get(url, headers=headers, params=params, timeout=10)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        logger.error(f"Error fetching bills: {e}")
        return None

def get_bill_details(bill_id: str) -> Optional[Dict[str, Any]]:
    """
    Get details for a specific bill.
    Args:
        bill_id: Bill identifier string.
    Returns:
        JSON response as dict, or None if error.
    """
    url = f"{BASE_URL}/bill/{bill_id}"
    try:
        resp = requests.get(url, headers=headers, timeout=10)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        logger.error(f"Error fetching bill details: {e}")
        return None

def get_votes(congress: str = "118", chamber: str = "house") -> Optional[Dict[str, Any]]:
    """
    Get roll call votes for a given session and chamber.
    Args:
        congress: Congress session number as string (e.g., "118").
        chamber: "house" or "senate".
    Returns:
        JSON response as dict, or None if error.
    """
    url = f"{BASE_URL}/roll-call-vote"
    params = {"congress": congress, "chamber": chamber, "limit": 250}
    try:
        resp = requests.get(url, headers=headers, params=params, timeout=10)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        logger.error(f"Error fetching votes: {e}")
        return None
