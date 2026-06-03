import os
import boto3


def upload_file_to_s3(local_file_path: str, s3_key: str) -> None:
    bucket_name = os.getenv("S3_BUCKET_NAME")

    if not bucket_name:
        raise ValueError("S3_BUCKET_NAME environment variable is missing.")

    if not os.path.exists(local_file_path):
        raise FileNotFoundError(f"Local file not found: {local_file_path}")

    s3_client = boto3.client("s3")

    s3_client.upload_file(
        Filename=local_file_path,
        Bucket=bucket_name,
        Key=s3_key
    )