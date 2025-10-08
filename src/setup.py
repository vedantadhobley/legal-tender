#!/usr/bin/env python3
"""
legal_tender_setup.py: Register all Prefect deployments for the project using prefect.yaml.
This script should be run inside the setup container.
"""
import pathlib
import subprocess
import sys
import os
import importlib
from dotenv import load_dotenv

# Ensure the project root is on sys.path so dynamic import can find `src` when
# this script is executed from inside the container at /app/src/setup.py.
_here = pathlib.Path(__file__).resolve()
# parent[0] -> /app/src, parent[1] -> /app (project root)
project_root = _here.parents[1] if len(_here.parents) > 1 else _here.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

# Dynamically import preflight now that sys.path has been adjusted.
preflight = importlib.import_module("src.utils.preflight")

# Load environment variables from .env
load_dotenv()

CONGRESS_API_KEY = os.getenv("CONGRESS_API_KEY")
ELECTION_API_KEY = os.getenv("ELECTION_API_KEY")
LOBBYING_API_KEY = os.getenv("LOBBYING_API_KEY")

if not (CONGRESS_API_KEY and ELECTION_API_KEY):
    print("Warning: One or more API keys are missing (CONGRESS/ELECTION). Please check your .env file.")


def run(cmd, check=True):
    print(f"[setup] $ {' '.join(cmd)}")
    result = subprocess.run(cmd, check=check)
    return result.returncode

def print_prefect_version():
    try:
        subprocess.run(["prefect", "--version"], check=False)
    except Exception as e:
        print(f"[setup] Could not print Prefect version: {e}")


def ensure_work_pool(pool_name: str = "api-pool"):
    """Ensure a work pool with the given name exists. Idempotent.

    Uses the Prefect CLI to create the pool if it does not exist.
    """
    # Check existence
    print(f"[setup] Ensuring work pool '{pool_name}' exists...")
    try:
        res = subprocess.run(["prefect", "work-pool", "inspect", pool_name], capture_output=True)
        if res.returncode == 0:
            print(f"[setup] Work pool '{pool_name}' already exists.")
            return
    except Exception:
        # continue to create
        pass

    # Create pool
    print(f"[setup] Creating work pool '{pool_name}'...")
    run(["prefect", "work-pool", "create", pool_name, "--type", "process"], check=True)
    print(f"[setup] Created work pool '{pool_name}'.")


def main():
    # Preflight: print Prefect version and validate API keys before making any changes
    print_prefect_version()
    # Use presence-only preflight in setup to avoid network calls during image startup
    ok, errors = preflight.check_keys_presence()
    if not ok:
        print("[setup] Presence preflight failed:")
        for e in errors:
            print(f"  - {e}")
        print("[setup] Aborting setup due to missing API keys.")
        raise SystemExit(2)
    # Ensure api-pool exists
    ensure_work_pool("api-pool")

    # Set Prefect API URL to server
    prefect_api_url = os.environ.get("PREFECT_API_URL", "http://prefect-server:4200/api")
    os.environ["PREFECT_API_URL"] = prefect_api_url
    print(f"[setup] Using PREFECT_API_URL={prefect_api_url}")

    # Register all deployments using prefect.yaml
    prefect_yaml = os.path.join(os.path.dirname(os.path.dirname(__file__)), "prefect.yaml")
    if not os.path.isfile(prefect_yaml):
        print(f"[setup] No prefect.yaml found at {prefect_yaml}")
        sys.exit(1)

    print(f"[setup] Registering deployments from {prefect_yaml} ...")
    run(["prefect", "deploy", "--all"], check=True)
    print("[setup] All deployments registered from prefect.yaml.")

if __name__ == "__main__":
    main()
