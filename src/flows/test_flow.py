from prefect import flow, task
from src.utils import preflight


@task
def run_online_preflight():
    ok, errors = preflight.validate_api_keys(online=True)
    if ok:
        print("Congress API: Success")
        print("FEC API: Success")
        print("Lobbying API: Success")
    else:
        print("Online preflight failures:")
        for e in errors:
            print(f" - {e}")
        # Fail the task to make the flow reflect the error in Prefect
        raise RuntimeError("Online preflight failed; see logs for details")


@flow(name="Test API Keys Flow")
def test_api_keys_flow():
    run_online_preflight()


if __name__ == "__main__":
    test_api_keys_flow()
