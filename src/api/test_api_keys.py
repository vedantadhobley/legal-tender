"""Script to test API keys and connectivity using the consolidated preflight utilities."""
from src.utils import preflight


if __name__ == "__main__":
    ok, errs = preflight.validate_api_keys(online=True)
    if ok:
        print("All API checks passed")
        raise SystemExit(0)
    print("Preflight failed:")
    for e in errs:
        print(f" - {e}")
    raise SystemExit(2)
