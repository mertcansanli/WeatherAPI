#Day 1 :

# Weather Airflow S3 Pipeline

A beginner-friendly data engineering pipeline built with Apache Airflow, Docker, AWS S3, PostgreSQL, and OpenWeather API.

The project extracts current weather data from OpenWeather API, stores raw JSON files locally, transforms nested API responses into a clean tabular format, validates data quality rules, uploads processed files to Amazon S3, and loads curated records into PostgreSQL.

## Project Goal

The goal of this project is to practice core data engineering concepts in a local Dockerized environment while using AWS S3 as cloud object storage.

This project focuses on:

* Apache Airflow DAG development
* Docker Compose usage
* API data extraction
* Raw and processed data handling
* Data transformation with Python
* Basic data quality validation
* AWS S3 file upload
* PostgreSQL loading
* Container networking and environment variables

## Architecture

```text
OpenWeather API
      ↓
Apache Airflow DAG
      ↓
Extract raw weather data
      ↓
Save raw JSON locally
      ↓
Transform nested JSON into tabular records
      ↓
Validate required fields and value ranges
      ↓
Save processed CSV
      ↓
Upload processed file to AWS S3
      ↓
Load records into PostgreSQL
```

## Tech Stack

* Python
* Apache Airflow
* Docker
* Docker Compose
* AWS S3
* PostgreSQL
* Pandas
* Boto3
* Requests
* OpenWeather API

## Pipeline Steps

1. Extract weather data from OpenWeather API for selected cities
2. Save raw API responses as JSON files
3. Transform nested JSON into a flat analytical structure
4. Validate important fields such as city, temperature, humidity, pressure, and timestamp
5. Save the processed records as CSV
6. Upload processed files to an AWS S3 bucket
7. Load final records into PostgreSQL

## Project Structure

```text
weatherapi/
├── dags/
│   └── weather_etl_dag.py
├── include/
│   ├── scripts/
│   │   ├── extract_weather.py
│   │   ├── transform_weather.py
│   │   ├── validation_script.py
│   │   ├── upload_s3.py
│   │   └── postgres_sql.py
│   └── sql/
│       └── create_weather_table.sql
├── data/
│   ├── raw/
│   └── processed/
├── logs/
├── plugins/
├── docker-compose.yaml
├── requirements.txt
├── .env.example
└── README.md
```

## Environment Variables

Create a `.env` file in the project root.

```env
OPENWEATHER_API_KEY=
AWS_ACCESS_KEY_ID=
AWS_SECRET_ACCESS_KEY=
AWS_DEFAULT_REGION=eu-west-1
S3_BUCKET_NAME=
FERNET_KEY=
_PIP_ADDITIONAL_REQUIREMENTS=requests pandas boto3 psycopg2-binary
```

Do not commit the real `.env` file to GitHub.

## How to Run

Start Airflow with Docker Compose:

```bash
docker compose up -d
```

Open Airflow UI:

```text
http://localhost:8080
```

Default credentials:

```text
username: airflow
password: airflow
```

Trigger the DAG manually from the Airflow UI:

```text
weather_etl_pipeline
```

## PostgreSQL Check

Connect to the project PostgreSQL container:

```bash
docker exec -it weather-postgres psql -U weather_user -d weather_db
```

Query latest records:

```sql
SELECT
    city,
    temperature_c,
    humidity,
    weather_description,
    collected_at
FROM weather_observations
ORDER BY collected_at DESC;
```

## Example Data Quality Checks

The pipeline validates:

* city is not null
* temperature is not null
* humidity is between 0 and 100
* pressure is not null
* collected_at timestamp exists

## What I Learned

Through this project, I practiced:

* Building an Airflow DAG from scratch
* Running Airflow with Docker Compose
* Debugging DAG import errors and task failures
* Working with environment variables inside containers
* Extracting data from an external API
* Transforming semi-structured JSON into tabular data
* Uploading processed files to AWS S3
* Loading records into PostgreSQL
* Understanding container-to-container networking

## Future Improvements

Planned improvements:

* Store both raw and processed data in AWS S3
* Convert processed CSV files to Parquet
* Add partitioned S3 paths such as year/month/day
* Add AWS Glue Crawler and Athena for querying S3 data
* Move PostgreSQL from Docker to AWS RDS
* Deploy Airflow on an EC2 instance
* Replace AWS access keys with IAM roles
* Add CI checks with GitHub Actions
* Add better logging and retry handling

## Status

Current version: Local Dockerized Airflow pipeline with AWS S3 and PostgreSQL integration.
---------------------------------------------------------------------------------------------

Second update Status