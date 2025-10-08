"""Shared preflight and API key validation utilities.

Provides lightweight presence checks (safe for setup) and an optional
online validation (performs minimal API calls with retries) used by the
test flow or the CLI test script.
"""
from typing import Tuple, List
import os


CONGRESS_API_KEY = os.getenv("CONGRESS_API_KEY")
ELECTION_API_KEY = os.getenv("ELECTION_API_KEY")
LOBBYING_API_KEY = os.getenv("LOBBYING_API_KEY")


def _request_with_retries(method, url, *, params=None, headers=None, timeout=10, retries=3, backoff_factor=1.0):
    import time
    import requests

    for attempt in range(1, retries + 1):
        try:
            resp = requests.request(method, url, params=params, headers=headers, timeout=timeout)
            return resp
        except Exception as exc:
            wait = backoff_factor * (2 ** (attempt - 1))
            print(f"[preflight] Request failed (attempt {attempt}/{retries}): {exc}. Retrying in {wait}s...")
            if attempt == retries:
                raise
            time.sleep(wait)


def check_keys_presence() -> Tuple[bool, List[str]]:
    """Check presence of required API keys.

    Returns (ok, errors).
    """
    errors: List[str] = []
    if not CONGRESS_API_KEY:
        errors.append("CONGRESS_API_KEY missing")
    if not ELECTION_API_KEY:
        errors.append("ELECTION_API_KEY missing")
    # Lobbying key may be optional in this project; warn but do not fail presence check
    if not LOBBYING_API_KEY:
        print("[preflight] Warning: LOBBYING_API_KEY not set; lobbying client may not require a key.")

    return (len(errors) == 0, errors)


def validate_api_keys(online: bool = False) -> Tuple[bool, List[str]]:
    """Validate API keys. If online is False, do presence-only checks.

    If online is True, performs minimal network calls to detect obvious auth errors.
    Returns (ok, errors).
    """
    ok, errors = check_keys_presence()
    if not online:
        return ok, errors

    # perform online checks
    if ok:
        # Congress
        try:
            url = "https://api.congress.gov/v3/member"
            headers = {"X-Api-Key": CONGRESS_API_KEY} if CONGRESS_API_KEY else None
            resp = _request_with_retries("GET", url, headers=headers, params={"limit": 1}, retries=2)
            if resp is None:
                errors.append("CONGRESS API check returned no response")
            else:
                status = getattr(resp, "status_code", None)
                if status in (401, 403):
                    errors.append(f"CONGRESS API returned {status}; check CONGRESS_API_KEY; text={resp.text[:200]}")
        except Exception as e:
            errors.append(f"CONGRESS API check failed: {e}")

        # FEC
        try:
            url = "https://api.open.fec.gov/v1/committee/search"
            params = {"api_key": ELECTION_API_KEY, "per_page": 1}
            resp = _request_with_retries("GET", url, params=params, retries=2)
            if resp is None:
                errors.append("Election API check returned no response")
            else:
                status = getattr(resp, "status_code", None)
                if status in (401, 403):
                    errors.append(f"Election API returned {status}; check ELECTION_API_KEY; text={resp.text[:200]}")
        except Exception as e:
            errors.append(f"Election API check failed: {e}")

        # Lobbying public endpoint
        try:
            url = "https://lda.senate.gov/api/v1/filings/"
            resp = _request_with_retries("GET", url, params={"limit": 1}, retries=2)
            if resp is None:
                errors.append("Lobbying API check returned no response")
            else:
                status = getattr(resp, "status_code", None)
                if status in (401, 403):
                    errors.append(f"Lobbying API returned {status}; check LOBBYING_API_KEY or access; text={resp.text[:200]}")
        except Exception as e:
            errors.append(f"Lobbying API check failed: {e}")

    return (len(errors) == 0, errors)


if __name__ == "__main__":
    ok, errs = validate_api_keys(online=True)
    if ok:
        print("Preflight: all API checks passed")
        raise SystemExit(0)
    print("Preflight failed:")
    for e in errs:
        print(f" - {e}")
    raise SystemExit(2)
