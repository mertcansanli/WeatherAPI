import json
import os
from datetime import datetime, timezone

import pandas as pd

from airflow.decorators import dag, task
from airflow.stats import Stats

from include.scripts.extract_weather import extract_weather
from include.scripts.transform_scripts import transform_weather
from include.scripts.validation_script import validate_weather
from include.scripts.upload_s3 import upload_file_to_s3
from include.scripts.postgres_sql import load_record_to_postgres


CITIES = ["Lisbon", "Istanbul", "London"]


def dag_success_callback(context):
    Stats.incr("weather_pipeline.dag_success")


def dag_failure_callback(context):
    Stats.incr("weather_pipeline.dag_failure")


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
        raw_file_paths = []

        os.makedirs("/opt/airflow/data/raw", exist_ok=True)

        run_timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")

        for payload in payloads:
            city = payload["city_requested"].lower().replace(" ", "_")
            file_path = f"/opt/airflow/data/raw/{city}_{run_timestamp}.json"

            with open(file_path, "w", encoding="utf-8") as file:
                json.dump(payload, file, ensure_ascii=False, indent=2)

            raw_file_paths.append(file_path)

        return raw_file_paths

    @task
    def transform_task(payloads: list[dict]) -> list[dict]:
        transformed_records = []

        for payload in payloads:
            record = transform_weather(payload)
            transformed_records.append(record)

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
        os.makedirs("/opt/airflow/data/processed", exist_ok=True)

        run_timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
        processed_file_path = f"/opt/airflow/data/processed/weather_{run_timestamp}.csv"

        df = pd.DataFrame(records)
        df.to_csv(processed_file_path, index=False)

        return processed_file_path

    @task
    def upload_to_s3_task(processed_file_path: str) -> str:
        file_name = os.path.basename(processed_file_path)
        s3_key = f"weather/processed/{file_name}"

        try:
            upload_file_to_s3(
                local_file_path=processed_file_path,
                s3_key=s3_key
            )
        except Exception:
            Stats.incr("weather_pipeline.s3_upload_failure")
            raise

        Stats.incr("weather_pipeline.s3_upload_success")

        return s3_key

    @task
    def update_master_csv_task(processed_file_path: str) -> str:
        master_file_path = "/opt/airflow/data/processed/weather_master.csv"

        new_df = pd.read_csv(processed_file_path)

        if os.path.exists(master_file_path):
            master_df = pd.read_csv(master_file_path)
            combined_df = pd.concat([master_df, new_df], ignore_index=True)
        else:
            combined_df = new_df

        if "collected_hour" in combined_df.columns:
            duplicate_subset = ["city", "collected_hour"]
        else:
            duplicate_subset = ["city", "collected_at"]

        combined_df = combined_df.drop_duplicates(
            subset=duplicate_subset,
            keep="last"
        )

        combined_df = combined_df.sort_values(
            by=duplicate_subset
        )

        combined_df.to_csv(master_file_path, index=False)

        return master_file_path

    @task
    def upload_master_to_s3_task(master_file_path: str) -> str:
        s3_key = "weather/master/weather_master.csv"

        try:
            upload_file_to_s3(
                local_file_path=master_file_path,
                s3_key=s3_key
            )
        except Exception:
            Stats.incr("weather_pipeline.master_upload_failure")
            raise

        Stats.incr("weather_pipeline.master_upload_success")

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

    extracted_payloads = extract_task()
    raw_files = save_raw_task(extracted_payloads)

    transformed_records = transform_task(extracted_payloads)
    validated_records = validate_task(transformed_records)

    processed_file = save_processed_task(validated_records)

    run_s3_key = upload_to_s3_task(processed_file)

    master_file = update_master_csv_task(processed_file)
    master_s3_key = upload_master_to_s3_task(master_file)

    postgres_count = load_postgres_task(validated_records)

    raw_files >> transformed_records
    processed_file >> run_s3_key
    processed_file >> master_file >> master_s3_key
    validated_records >> postgres_count


weather_etl_pipeline()