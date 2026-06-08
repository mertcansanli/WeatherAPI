import os
import psycopg2


def get_redshift_connection():
    required_env_vars = [
        "REDSHIFT_HOST",
        "REDSHIFT_PORT",
        "REDSHIFT_DB",
        "REDSHIFT_USER",
        "REDSHIFT_PASSWORD",
    ]

    missing_vars = [
        var for var in required_env_vars
        if not os.getenv(var)
    ]

    if missing_vars:
        raise ValueError(f"Missing Redshift environment variables: {missing_vars}")

    return psycopg2.connect(
        host=os.getenv("REDSHIFT_HOST"),
        port=os.getenv("REDSHIFT_PORT", "5439"),
        database=os.getenv("REDSHIFT_DB"),
        user=os.getenv("REDSHIFT_USER"),
        password=os.getenv("REDSHIFT_PASSWORD"),
        connect_timeout=20,
    )


def create_redshift_weather_table_if_not_exists():
    conn = get_redshift_connection()
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS fact_weather_observations (
            city VARCHAR(100),
            country VARCHAR(10),
            temperature_c DOUBLE PRECISION,
            feels_like_c DOUBLE PRECISION,
            humidity INTEGER,
            pressure INTEGER,
            wind_speed DOUBLE PRECISION,
            weather_main VARCHAR(100),
            weather_description VARCHAR(255),
            collected_at TIMESTAMP,
            collected_hour TIMESTAMP,
            year VARCHAR(4),
            month VARCHAR(2),
            day VARCHAR(2),
            hour VARCHAR(2),
            loaded_at TIMESTAMP DEFAULT GETDATE()
        );
    """)

    conn.commit()
    cursor.close()
    conn.close()


def copy_parquet_from_s3_to_redshift(s3_path: str) -> str:
    if not s3_path:
        raise ValueError("s3_path is required.")

    iam_role_arn = os.getenv("REDSHIFT_IAM_ROLE_ARN")

    if not iam_role_arn:
        raise ValueError("REDSHIFT_IAM_ROLE_ARN is missing.")

    create_redshift_weather_table_if_not_exists()

    conn = get_redshift_connection()
    cursor = conn.cursor()

    copy_sql = f"""
        COPY fact_weather_observations
        FROM '{s3_path}'
        IAM_ROLE '{iam_role_arn}'
        FORMAT AS PARQUET;
    """

    cursor.execute(copy_sql)

    conn.commit()
    cursor.close()
    conn.close()

    return "Redshift COPY completed"


def create_redshift_daily_summary_table():
    conn = get_redshift_connection()
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS weather_city_daily_summary (
            city VARCHAR(100),
            observation_date DATE,
            avg_temperature_c DOUBLE PRECISION,
            avg_humidity DOUBLE PRECISION,
            avg_wind_speed DOUBLE PRECISION,
            record_count INTEGER,
            created_at TIMESTAMP DEFAULT GETDATE()
        );
    """)

    conn.commit()
    cursor.close()
    conn.close()


def refresh_redshift_daily_summary():
    create_redshift_daily_summary_table()

    conn = get_redshift_connection()
    cursor = conn.cursor()

    cursor.execute("""
        DELETE FROM weather_city_daily_summary;

        INSERT INTO weather_city_daily_summary (
            city,
            observation_date,
            avg_temperature_c,
            avg_humidity,
            avg_wind_speed,
            record_count
        )
        SELECT
            city,
            CAST(collected_at AS DATE) AS observation_date,
            AVG(temperature_c) AS avg_temperature_c,
            AVG(humidity) AS avg_humidity,
            AVG(wind_speed) AS avg_wind_speed,
            COUNT(*) AS record_count
        FROM fact_weather_observations
        GROUP BY
            city,
            CAST(collected_at AS DATE);
    """)

    conn.commit()
    cursor.close()
    conn.close()

    return "Redshift daily summary refreshed"