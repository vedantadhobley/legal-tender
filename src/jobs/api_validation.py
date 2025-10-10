"""API validation job for testing Congress, Election, and Lobbying API connectivity."""
from dagster import job, op, OpExecutionContext
from src.utils import preflight


@op(
    tags={"kind": "validation"},
    description="Validates API keys by performing online connectivity checks"
)
def validate_api_keys(context: OpExecutionContext) -> dict:
    """Validate all API keys by performing online checks.
    
    Returns:
        dict: Validation results with status for each API
    """
    ok, errors = preflight.validate_api_keys(online=True)
    
    results = {
        "success": ok,
        "errors": errors
    }
    
    if ok:
        context.log.info("✓ Congress API: Success")
        context.log.info("✓ Election API: Success")
        context.log.info("✓ Lobbying API: Success")
    else:
        context.log.error("API validation failures:")
        for error in errors:
            context.log.error(f"  ✗ {error}")
        raise RuntimeError(f"API validation failed with {len(errors)} error(s)")
    
    return results


@job(
    name="api_test_job",
    description="Test all API keys and endpoints for Congress, Election, and Lobbying APIs",
    tags={"team": "data", "priority": "high"}
)
def api_test_job():
    """Job to validate all API keys and endpoints."""
    validate_api_keys()
