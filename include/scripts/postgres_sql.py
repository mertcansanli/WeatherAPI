import psycopg2


def create_table_if_not_exists():
    conn = psycopg2.connect(
        host="weather-postgres",
        port=5432,
        database="weather_db",
        user="weather_user",
        password="weather_pass"
    )

    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS weather_observations (
            id SERIAL PRIMARY KEY,
            city TEXT NOT NULL,
            country TEXT,
            temperature_c NUMERIC,
            feels_like_c NUMERIC,
            humidity INTEGER,
            pressure INTEGER,
            wind_speed NUMERIC,
            weather_main TEXT,
            weather_description TEXT,
            collected_at TIMESTAMP,
            inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)

    conn.commit()
    cursor.close()
    conn.close()


def load_record_to_postgres(record: dict):
    create_table_if_not_exists()

    conn = psycopg2.connect(
        host="weather-postgres",
        port=5432,
        database="weather_db",
        user="weather_user",
        password="weather_pass"
    )

    cursor = conn.cursor()

    insert_query = """
        INSERT INTO weather_observations (
            city,
            country,
            temperature_c,
            feels_like_c,
            humidity,
            pressure,
            wind_speed,
            weather_main,
            weather_description,
            collected_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    """

    cursor.execute(
        insert_query,
        (
            record["city"],
            record["country"],
            record["temperature_c"],
            record["feels_like_c"],
            record["humidity"],
            record["pressure"],
            record["wind_speed"],
            record["weather_main"],
            record["weather_description"],
            record["collected_at"],
        )
    )

    conn.commit()
    cursor.close()
    conn.close()