import json
import os
from datetime import datetime, timezone

import pandas as pd
import boto3

from airflow.decorators import dag, task
from airflow.stats import Stats

from include.scripts.extract_weather import extract_weather
from include.scripts.transform_scripts import transform_weather
from include.scripts.validation_script import validate_weather
from include.scripts.upload_s3 import upload_file_to_s3
from include.scripts.postgres_sql import load_record_to_postgres

from include.scripts.athena_query import run_athena_query
from include.scripts.glue_crawler import start_glue_crawler_and_wait
CITIES = ["Lisbon", "Istanbul", "London"]

RAW_DIR = "/opt/airflow/data/raw"
PROCESSED_DIR = "/opt/airflow/data/processed"
MASTER_FILE_PATH = "/opt/airflow/data/processed/weather_master.csv"


def dag_success_callback(context):
    Stats.incr("weather_pipeline.dag_success")


def dag_failure_callback(context):
    Stats.incr("weather_pipeline.dag_failure")


def get_partition_parts() -> dict:
    now = datetime.now(timezone.utc)

    return {
        "year": now.strftime("%Y"),
        "month": now.strftime("%m"),
        "day": now.strftime("%d"),
        "hour": now.strftime("%H"),
    }


@dag(
    dag_id="weather_etl_pipeline",
    description="Extract weather data from OpenWeather, transform it, upload to S3, and load to PostgreSQL.",
    start_date=datetime(2026, 1, 1),
    schedule="@hourly",
    catchup=False,
    tags=["weather", "api", "s3", "postgres", "data-engineering"],
    on_success_callback=dag_success_callback,
    on_failure_callback=dag_failure_callback,
)
def weather_etl_pipeline():

    @task
    def extract_task() -> list[dict]:
        records = []

        for city in CITIES:
            payload = extract_weather(city)
            records.append(payload)

        Stats.incr("weather_pipeline.records_extracted", count=len(records))

        return records

    @task
    def save_raw_task(payloads: list[dict]) -> list[str]:
        os.makedirs(RAW_DIR, exist_ok=True)

        raw_file_paths = []
        run_timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")

        for payload in payloads:
            city = payload["city_requested"].lower().replace(" ", "_")
            file_path = f"{RAW_DIR}/{city}_{run_timestamp}.json"

            with open(file_path, "w", encoding="utf-8") as file:
                json.dump(payload, file, ensure_ascii=False, indent=2)

            raw_file_paths.append(file_path)

        Stats.incr("weather_pipeline.raw_files_created", count=len(raw_file_paths))

        return raw_file_paths

    @task
    def upload_raw_to_s3_task(raw_file_paths: list[str]) -> list[str]:
        uploaded_keys = []
        partition = get_partition_parts()

        for file_path in raw_file_paths:
            file_name = os.path.basename(file_path)

            s3_key = (
                f"weather/raw/"
                f"year={partition['year']}/"
                f"month={partition['month']}/"
                f"day={partition['day']}/"
                f"hour={partition['hour']}/"
                f"{file_name}"
            )

            try:
                upload_file_to_s3(
                    local_file_path=file_path,
                    s3_key=s3_key,
                )
            except Exception:
                Stats.incr("weather_pipeline.raw_upload_failure")
                raise

            uploaded_keys.append(s3_key)

        Stats.incr("weather_pipeline.raw_upload_success", count=len(uploaded_keys))

        return uploaded_keys

    @task
    def transform_task(payloads: list[dict]) -> list[dict]:
        transformed_records = []

        for payload in payloads:
            record = transform_weather(payload)
            transformed_records.append(record)

        Stats.incr("weather_pipeline.records_transformed", count=len(transformed_records))

        return transformed_records

    @task
    def validate_task(records: list[dict]) -> list[dict]:
        for record in records:
            try:
                validate_weather(record)
            except Exception:
                Stats.incr("weather_pipeline.validation_failure")
                raise

        Stats.incr("weather_pipeline.records_validated", count=len(records))

        return records

    @task
    def save_processed_task(records: list[dict]) -> str:
        os.makedirs(PROCESSED_DIR, exist_ok=True)

        run_timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
        processed_file_path = f"{PROCESSED_DIR}/weather_{run_timestamp}.csv"

        df = pd.DataFrame(records)
        df.to_csv(processed_file_path, index=False)

        Stats.incr("weather_pipeline.processed_csv_created")

        return processed_file_path

    @task
    def upload_processed_to_s3_task(processed_file_path: str) -> str:
        file_name = os.path.basename(processed_file_path)
        s3_key = f"weather/processed/{file_name}"

        try:
            upload_file_to_s3(
                local_file_path=processed_file_path,
                s3_key=s3_key,
            )
        except Exception:
            Stats.incr("weather_pipeline.s3_upload_failure")
            raise

        Stats.incr("weather_pipeline.s3_upload_success")

        return s3_key

    @task
    def update_master_csv_task(processed_file_path: str) -> str:
        new_df = pd.read_csv(processed_file_path)

        if os.path.exists(MASTER_FILE_PATH):
            master_df = pd.read_csv(MASTER_FILE_PATH)
            combined_df = pd.concat([master_df, new_df], ignore_index=True)
        else:
            combined_df = new_df

        if "collected_hour" in combined_df.columns:
            duplicate_subset = ["city", "collected_hour"]
        else:
            duplicate_subset = ["city", "collected_at"]

        combined_df = combined_df.drop_duplicates(
            subset=duplicate_subset,
            keep="last",
        )

        combined_df = combined_df.sort_values(by=duplicate_subset)

        os.makedirs(os.path.dirname(MASTER_FILE_PATH), exist_ok=True)
        combined_df.to_csv(MASTER_FILE_PATH, index=False)

        Stats.incr("weather_pipeline.master_csv_updated")
        Stats.incr("weather_pipeline.master_records_total", count=len(combined_df))

        return MASTER_FILE_PATH

    @task
    def upload_master_to_s3_task(master_file_path: str) -> str:
        s3_key = "weather/master/weather_master.csv"

        try:
            upload_file_to_s3(
                local_file_path=master_file_path,
                s3_key=s3_key,
            )
        except Exception:
            Stats.incr("weather_pipeline.master_upload_failure")
            raise

        Stats.incr("weather_pipeline.master_upload_success")

        return s3_key

    @task
    def save_processed_parquet_task(records: list[dict]) -> str:
        os.makedirs(PROCESSED_DIR, exist_ok=True)

        run_timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
        parquet_file_path = f"{PROCESSED_DIR}/weather_{run_timestamp}.parquet"

        df = pd.DataFrame(records)
        df.to_parquet(parquet_file_path, index=False)

        Stats.incr("weather_pipeline.parquet_created")

        return parquet_file_path

    @task
    def upload_parquet_to_s3_task(parquet_file_path: str) -> str:
        file_name = os.path.basename(parquet_file_path)
        partition = get_partition_parts()

        s3_key = (
            f"weather/processed_parquet/"
            f"year={partition['year']}/"
            f"month={partition['month']}/"
            f"day={partition['day']}/"
            f"hour={partition['hour']}/"
            f"{file_name}"
        )

        try:
            upload_file_to_s3(
                local_file_path=parquet_file_path,
                s3_key=s3_key,
            )
        except Exception:
            Stats.incr("weather_pipeline.parquet_upload_failure")
            raise

        Stats.incr("weather_pipeline.parquet_upload_success")

        return s3_key

    @task
    def load_postgres_task(records: list[dict]) -> int:
        try:
            for record in records:
                load_record_to_postgres(record)
        except Exception:
            Stats.incr("weather_pipeline.postgres_load_failure")
            raise

        Stats.incr("weather_pipeline.records_loaded", count=len(records))
        Stats.incr("weather_pipeline.postgres_load_success")

        return len(records)
    



    @task
    def run_glue_crawler_task() -> str:
        crawler_name = os.getenv("GLUE_CRAWLER_NAME")

        if not crawler_name:
            raise ValueError("GLUE_CRAWLER_NAME is missing.")

        try:
            result = start_glue_crawler_and_wait(crawler_name)
        except Exception:
            Stats.incr("weather_pipeline.glue_crawler_failure")
            raise

        Stats.incr("weather_pipeline.glue_crawler_success")

        return result
    

    @task
    def run_athena_summary_query_task() -> str:
        database = os.getenv("ATHENA_DATABASE", "weather_db")
        output_location = os.getenv("ATHENA_OUTPUT_LOCATION")
        workgroup = os.getenv("ATHENA_WORKGROUP", "primary")

        if not output_location:
            raise ValueError("ATHENA_OUTPUT_LOCATION is missing.")

        query = """
        SELECT
            city,
            COUNT(*) AS records,
            AVG(temperature_c) AS avg_temperature_c,
            AVG(humidity) AS avg_humidity,
            AVG(pressure) AS avg_pressure,
            MAX(collected_at) AS latest_collected_at
        FROM weather_db.year_2026

        GROUP BY city
        ORDER BY city
        """

        try:
            query_execution_id = run_athena_query(
                query=query,
                database=database,
                output_location=output_location,
                workgroup=workgroup,
            )
        except Exception:
            Stats.incr("weather_pipeline.athena_query_failure")
            raise

        Stats.incr("weather_pipeline.athena_query_success")

        return query_execution_id
    



    extracted_payloads = extract_task()

    raw_files = save_raw_task(extracted_payloads)
    raw_s3_keys = upload_raw_to_s3_task(raw_files)

    transformed_records = transform_task(extracted_payloads)
    validated_records = validate_task(transformed_records)

    processed_file = save_processed_task(validated_records)
    processed_s3_key = upload_processed_to_s3_task(processed_file)

    master_file = update_master_csv_task(processed_file)
    master_s3_key = upload_master_to_s3_task(master_file)

    parquet_file = save_processed_parquet_task(validated_records)
    parquet_s3_key = upload_parquet_to_s3_task(parquet_file)
    glue_crawler_result = run_glue_crawler_task()
    athena_query_id = run_athena_summary_query_task()

    postgres_count = load_postgres_task(validated_records)

    raw_files >> raw_s3_keys
    validated_records >> processed_file
    processed_file >> processed_s3_key
    processed_file >> master_file >> master_s3_key
    validated_records >> parquet_file >> parquet_s3_key
    validated_records >> postgres_count
    parquet_s3_key >> glue_crawler_result >> athena_query_id


weather_etl_pipeline()