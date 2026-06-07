import os
import time
import boto3


def run_athena_query(
    query: str,
    database: str,
    output_location: str,
    workgroup: str = "primary",
    poll_interval_seconds: int = 5,
    timeout_seconds: int = 300,
) -> str:
    if not query:
        raise ValueError("query is required.")

    if not database:
        raise ValueError("database is required.")

    if not output_location:
        raise ValueError("output_location is required.")

    region_name = os.getenv("AWS_DEFAULT_REGION", "eu-west-1")

    athena_client = boto3.client(
        "athena",
        region_name=region_name,
    )

    response = athena_client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            "Database": database,
        },
        ResultConfiguration={
            "OutputLocation": output_location,
        },
        WorkGroup=workgroup,
    )

    query_execution_id = response["QueryExecutionId"]

    start_time = time.time()

    while True:
        query_status_response = athena_client.get_query_execution(
            QueryExecutionId=query_execution_id
        )

        status = query_status_response["QueryExecution"]["Status"]
        state = status["State"]

        if state == "SUCCEEDED":
            return query_execution_id

        if state in {"FAILED", "CANCELLED"}:
            reason = status.get("StateChangeReason", "No failure reason provided.")
            raise RuntimeError(
                f"Athena query {query_execution_id} ended with state={state}. Reason: {reason}"
            )

        if time.time() - start_time > timeout_seconds:
            raise TimeoutError(
                f"Athena query {query_execution_id} did not finish within {timeout_seconds} seconds."
            )

        time.sleep(poll_interval_seconds)