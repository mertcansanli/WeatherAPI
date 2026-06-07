import os
import time
import boto3


def start_glue_crawler_and_wait(
    crawler_name: str,
    poll_interval_seconds: int = 10,
    timeout_seconds: int = 600,
) -> str:
    if not crawler_name:
        raise ValueError("crawler_name is required.")

    region_name = os.getenv("AWS_DEFAULT_REGION", "eu-west-1")

    glue_client = boto3.client(
        "glue",
        region_name=region_name,
    )

    try:
        glue_client.start_crawler(Name=crawler_name)
    except glue_client.exceptions.CrawlerRunningException:
        pass

    start_time = time.time()

    while True:
        response = glue_client.get_crawler(Name=crawler_name)
        state = response["Crawler"]["State"]

        if state == "READY":
            return "Crawler finished"

        if time.time() - start_time > timeout_seconds:
            raise TimeoutError(
                f"Glue crawler {crawler_name} did not finish within {timeout_seconds} seconds."
            )

        time.sleep(poll_interval_seconds)