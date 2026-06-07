# Weather Data Engineering Pipeline

A Dockerized data engineering project built with Apache Airflow, OpenWeather API, AWS S3, AWS Glue, Amazon Athena, PostgreSQL, Prometheus, and Grafana.

The pipeline extracts weather data from OpenWeather API, transforms and validates it, stores raw and processed files in S3, loads records into PostgreSQL, catalogs Parquet data with AWS Glue, runs Athena queries automatically, and monitors the pipeline with Prometheus and Grafana.

## Tech Stack

* Python
* Apache Airflow
* Docker Compose
* OpenWeather API
* AWS S3
* AWS Glue
* Amazon Athena
* PostgreSQL
* Pandas
* Boto3
* Prometheus
* Grafana

## Pipeline Flow

```text
OpenWeather API
    ↓
Airflow DAG
    ↓
Extract raw weather data
    ↓
Save raw JSON locally and upload to S3
    ↓
Transform nested JSON into tabular records
    ↓
Validate data quality rules
    ↓
Save processed CSV
    ↓
Update master CSV
    ↓
Upload CSV outputs to S3
    ↓
Save Parquet output
    ↓
Upload partitioned Parquet to S3
    ↓
Run AWS Glue Crawler
    ↓
Run Athena summary query
    ↓
Load validated records into PostgreSQL
    ↓
Monitor with Prometheus and Grafana
```

## S3 Structure

```text
weather/
├── raw/
│   └── year=YYYY/month=MM/day=DD/hour=HH/
├── processed/
│   └── weather_YYYYMMDDTHHMMSS.csv
├── master/
│   └── weather_master.csv
├── processed_parquet/
│   └── year=YYYY/month=MM/day=DD/hour=HH/
└── athena-results/
    └── weather/
```

## Main Features

* Hourly weather data extraction from OpenWeather API
* Raw JSON storage
* Data transformation with Python
* Data validation before loading
* Run-level processed CSV outputs
* Cumulative master CSV
* Partitioned Parquet output for analytics
* AWS S3 data lake structure
* AWS Glue Crawler integration
* Automated Athena query execution
* PostgreSQL loading
* Prometheus and Grafana monitoring
* Custom Airflow pipeline metrics

## Data Validation

The pipeline validates:

* Required fields
* Temperature range
* Humidity between 0 and 100
* Positive pressure values
* Timestamp availability

If validation fails, the pipeline stops before uploading processed data or loading PostgreSQL.

## Monitoring

The project includes Prometheus, StatsD Exporter, and Grafana.

Tracked metrics include:

* DAG success/failure
* Records extracted
* Records validated
* Records loaded
* Validation failures
* S3 upload success/failure
* Parquet upload success/failure
* Glue crawler success/failure
* Athena query success/failure
* PostgreSQL load success/failure

## Environment Variables

Create a `.env` file:

```env
OPENWEATHER_API_KEY=

AWS_ACCESS_KEY_ID=
AWS_SECRET_ACCESS_KEY=
AWS_DEFAULT_REGION=eu-west-1
S3_BUCKET_NAME=

ATHENA_DATABASE=weather_db
ATHENA_TABLE=<your_glue_table_name>
ATHENA_OUTPUT_LOCATION=s3://<bucket-name>/athena-results/weather/
ATHENA_WORKGROUP=primary
GLUE_CRAWLER_NAME=weather_processed_parquet_crawler

FERNET_KEY=

_PIP_ADDITIONAL_REQUIREMENTS=requests pandas boto3 psycopg2-binary apache-airflow[statsd] pyarrow
```

Do not commit the real `.env` file.

## Run Locally

Start the services:

```bash
docker compose up -d
```

Open Airflow:

```text
http://localhost:8080
```

Trigger the DAG:

```text
weather_etl_pipeline
```

Open monitoring tools:

```text
Prometheus: http://localhost:9090
Grafana:    http://localhost:3000
```

## Athena Example Query

```sql
SELECT
    city,
    COUNT(*) AS records,
    AVG(temperature_c) AS avg_temperature_c,
    AVG(humidity) AS avg_humidity,
    MAX(collected_at) AS latest_collected_at
FROM weather_db.<your_table_name>
GROUP BY city
ORDER BY city;
```

## PostgreSQL Check

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

## Project Structure

```text
weatherapi/
├── dags/
│   └── weather_etl_dag.py
├── include/
│   └── scripts/
│       ├── extract_weather.py
│       ├── transform_scripts.py
│       ├── validation_script.py
│       ├── upload_s3.py
│       ├── postgres_sql.py
│       ├── glue_crawler.py
│       └── athena_query.py
├── data/
├── monitoring/
├── docker-compose.yaml
├── .env.example
├── .gitignore
└── README.md
```

## Future Improvements

* Move PostgreSQL to AWS RDS
* Deploy Airflow on EC2
* Replace AWS keys with IAM roles
* Add Terraform
* Add dbt
* Add stronger validation with Great Expectations
* Add Grafana alert notifications

## Status

Current version:

```text
Dockerized Airflow weather pipeline with S3, PostgreSQL, partitioned Parquet, AWS Glue, Athena, Prometheus, and Grafana.
```
