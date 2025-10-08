import requests
import logging
from typing import Optional, Dict, Any

BASE_URL = "https://lda.senate.gov/api/v1/"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("lobbying_api")

def search_filings(
    client_name: Optional[str] = None,
    registrant_name: Optional[str] = None,
    year: Optional[int] = None,
    filing_type: Optional[str] = None,
    offset: int = 0,
    limit: int = 100
) -> Optional[Dict[str, Any]]:
    """
    Search lobbying filings. You can filter by client, registrant, year, or filing type.
    Args:
        client_name: Name of the client organization.
        registrant_name: Name of the lobbying registrant.
        year: Filing year.
        filing_type: Type of filing (e.g., 'LD-2').
        offset: Pagination offset.
        limit: Number of results to return.
    Returns:
        JSON response as dict, or None if error.
    """
    params = {
        "client_name": client_name,
        "registrant_name": registrant_name,
        "year": year,
        "filing_type": filing_type,
        "offset": offset,
        "limit": limit
    }
    params = {k: v for k, v in params.items() if v is not None}
    url = f"{BASE_URL}filings/"
    try:
        resp = requests.get(url, params=params, timeout=10)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        logger.error(f"Error searching lobbying filings: {e}")
        return None

def get_filing(filing_id: str) -> Optional[Dict[str, Any]]:
    """
    Get details for a specific lobbying filing by ID.
    Args:
        filing_id: Filing identifier string.
    Returns:
        JSON response as dict, or None if error.
    """
    url = f"{BASE_URL}filings/{filing_id}/"
    try:
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        logger.error(f"Error fetching lobbying filing: {e}")
        return None
