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
    context.log.info("=" * 60)
    context.log.info("🔍 API VALIDATION STARTED")
    context.log.info("=" * 60)
    context.log.info("Testing connectivity to all external APIs...")
    context.log.info("")
    
    ok, errors = preflight.validate_api_keys(online=True)
    
    results = {
        "success": ok,
        "errors": errors,
        "apis_tested": ["Congress.gov", "OpenFEC (Election)", "Senate LDA (Lobbying)"]
    }
    
    context.log.info("📊 VALIDATION RESULTS:")
    context.log.info("-" * 60)
    
    if ok:
        context.log.info("✅ Congress API: PASSED")
        context.log.info("✅ Election API: PASSED")
        context.log.info("✅ Lobbying API: PASSED")
        context.log.info("-" * 60)
        context.log.info("🎉 ALL API CHECKS PASSED!")
        context.log.info("=" * 60)
    else:
        context.log.error("❌ API VALIDATION FAILED")
        context.log.error("-" * 60)
        for error in errors:
            context.log.error(f"  ✗ {error}")
        context.log.error("-" * 60)
        context.log.error(f"💥 {len(errors)} error(s) detected")
        context.log.error("=" * 60)
        raise RuntimeError(f"API validation failed with {len(errors)} error(s)")
    
    return results


@job(
    description="Test all API keys and endpoints for Congress, Election, and Lobbying APIs",
    tags={"team": "data", "priority": "high"}
)
def api_test_job():
    """Job to validate all API keys and endpoints."""
    validate_api_keys()
